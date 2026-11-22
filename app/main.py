"""FastAPI operations-drill page and JSON API."""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from .gateway import (
    ClientPool,
    GatewayRuntime,
    Upstream,
    parse_ports,
)
from .gateway_core import GatewayCore

STATIC_DIR = Path(__file__).parent / "static"


def _build_config() -> dict:
    egress_ports = parse_ports(os.environ.get("EGRESS_PORTS", "17000,17001"))
    upstream_specs = [
        ("echo-a", "上游 A", os.environ.get("ECHO_A_HOST", "echo-a"), 16001, 15001),
        ("echo-b", "上游 B", os.environ.get("ECHO_B_HOST", "echo-b"), 16002, 15002),
    ]
    upstreams = [
        Upstream(key=key, label=label, host=host, port=port, ingress_port=ingress)
        for key, label, host, port, ingress in upstream_specs
    ]
    clients = [
        ("client-1", "客户端 1", os.environ.get("CLIENT1_BIND_IP", "127.0.0.11")),
        ("client-2", "客户端 2", os.environ.get("CLIENT2_BIND_IP", "127.0.0.12")),
        ("client-3", "客户端 3", os.environ.get("CLIENT3_BIND_IP", "127.0.0.13")),
    ]
    return {
        "egress_ports": egress_ports,
        "upstreams": upstreams,
        "clients": clients,
        "session_idle_seconds": float(os.environ.get("SESSION_IDLE_SECONDS", "1.0")),
        "quarantine_seconds": float(os.environ.get("QUARANTINE_SECONDS", "2.0")),
    }


class SendRequest(BaseModel):
    client_id: str = Field(..., min_length=1)
    upstream: str = Field(..., min_length=1)
    delay_ms: int = Field(0, ge=0, le=2000)
    body: str = Field("", max_length=500)


class BatchRequest(BaseModel):
    delay_ms: int = Field(0, ge=0, le=2000)
    body: str = Field("", max_length=500)
    sends: list[SendRequest] | None = None


class ResetRequest(BaseModel):
    confirm: bool = False


class Drill:
    def __init__(self) -> None:
        self.config = _build_config()
        self.core: GatewayCore | None = None
        self.runtime: GatewayRuntime | None = None
        self.clients: ClientPool | None = None

    async def start(self) -> None:
        self.core = GatewayCore(
            egress_ports=self.config["egress_ports"],
            upstreams={u.key: u.label for u in self.config["upstreams"]},
            session_idle_seconds=self.config["session_idle_seconds"],
            quarantine_seconds=self.config["quarantine_seconds"],
        )
        self.runtime = GatewayRuntime(
            core=self.core,
            upstreams=self.config["upstreams"],
            egress_ports=self.config["egress_ports"],
        )
        await self.runtime.start()
        self.clients = ClientPool(
            self.runtime,
            [(cid, label, ip) for cid, label, ip in self.config["clients"]],
        )
        await self.clients.start()

    async def stop(self) -> None:
        if self.clients is not None:
            await self.clients.stop()
        if self.runtime is not None:
            await self.runtime.stop()

    def require(self) -> tuple[GatewayCore, GatewayRuntime, ClientPool]:
        if self.core is None or self.runtime is None or self.clients is None:
            raise HTTPException(status_code=503, detail="网关尚未就绪")
        return self.core, self.runtime, self.clients


drill = Drill()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await drill.start()
    try:
        yield
    finally:
        await drill.stop()


app = FastAPI(title="UDP 双端口出口演练", lifespan=lifespan)


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/healthz")
async def healthz() -> dict:
    core, runtime, clients = drill.require()
    return {
        "ok": True,
        "ingress_ports": sorted(u.ingress_port for u in runtime.upstreams.values()),
        "egress_ports": core.egress_ports,
        "clients": [c.snapshot()["local_addr"] for c in clients.clients.values()],
        "upstreams": {
            u.key: f"{u.addr[0]}:{u.addr[1]}" if u.addr else None
            for u in runtime.upstreams.values()
        },
    }


@app.get("/api/state")
async def state() -> JSONResponse:
    core, runtime, clients = drill.require()
    payload = core.snapshot()
    payload["clients"] = clients.snapshot()
    payload["client_ingress"] = {
        key: f"{runtime.client_ingress_host}:{u.ingress_port}"
        for key, u in runtime.upstreams.items()
    }
    payload["server_time"] = core.wallclock()
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


@app.get("/api/events")
async def events(
    since: int = 0,
    client: str | None = None,
    upstream: str | None = None,
    limit: int = 100,
) -> dict:
    core, _, _ = drill.require()
    items = core.recent_events(since, client=client, upstream=upstream, limit=limit)
    return {"events": items, "last_seq": core._seq}


@app.post("/api/send")
async def send(payload: SendRequest) -> dict:
    _, runtime, clients = drill.require()
    client = clients.get(payload.client_id)
    if client is None:
        raise HTTPException(status_code=404, detail=f"未知客户端 {payload.client_id}")
    if payload.upstream not in runtime.upstreams:
        raise HTTPException(status_code=404, detail=f"未知上游 {payload.upstream}")
    try:
        message = client.send(payload.upstream, payload.delay_ms, payload.body)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    await _await_resolution(clients, {payload.client_id: message.request_id}, timeout=0.6)
    return {"sent": message.as_dict()}


@app.post("/api/send-batch")
async def send_batch(payload: BatchRequest) -> dict:
    _, runtime, clients = drill.require()
    specs = payload.sends
    if not specs:
        specs = [
            SendRequest(
                client_id="client-1",
                upstream="echo-a",
                delay_ms=payload.delay_ms,
                body=payload.body,
            ),
            SendRequest(
                client_id="client-2",
                upstream="echo-b",
                delay_ms=payload.delay_ms,
                body=payload.body,
            ),
            SendRequest(
                client_id="client-3",
                upstream="echo-a",
                delay_ms=payload.delay_ms,
                body=payload.body,
            ),
        ]
    pending: dict[str, str] = {}
    sent = []
    for spec in specs:
        client = clients.get(spec.client_id)
        if client is None:
            raise HTTPException(status_code=404, detail=f"未知客户端 {spec.client_id}")
        if spec.upstream not in runtime.upstreams:
            raise HTTPException(status_code=404, detail=f"未知上游 {spec.upstream}")
        message = client.send(spec.upstream, spec.delay_ms, spec.body)
        pending[spec.client_id] = message.request_id
        sent.append(message.as_dict())
    await _await_resolution(clients, pending, timeout=0.6)
    return {"sent": sent}


async def _await_resolution(
    clients: ClientPool, pending: dict[str, str], timeout: float
) -> None:
    """Give the local UDP round-trip a moment so the response is useful."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        waiting = False
        for client_id, request_id in pending.items():
            client = clients.get(client_id)
            if client is not None and request_id in client.pending:
                waiting = True
                break
        if not waiting:
            break
        await asyncio.sleep(0.02)


@app.get("/api/client/{client_id}")
async def client_messages(client_id: str) -> dict:
    _, _, clients = drill.require()
    client = clients.get(client_id)
    if client is None:
        raise HTTPException(status_code=404, detail=f"未知客户端 {client_id}")
    return client.snapshot()


@app.post("/api/reset")
async def reset(payload: ResetRequest = Body(default_factory=ResetRequest)) -> dict:  # noqa: B008
    if not payload.confirm:
        raise HTTPException(status_code=400, detail="需要 confirm=true")
    core, _, clients = drill.require()
    clients.clear()
    return core.reset()
