"""UDP plumbing for the drill: ingress endpoints, egress sockets, client agents.

Topology (all inside one gateway container):

    client 1 ─┐
    client 2 ─┼─ UDP ─▶ ingress :15001 (echo-a) ─┐
    client 3 ─┘                                   ├─ egress :17000 / :17001 ─▶ echo services
              └─ UDP ─▶ ingress :15002 (echo-b) ─┘

Each client is a real UDP socket with its own source address.  The gateway
opens one ingress socket per upstream and two egress sockets in total.
"""

from __future__ import annotations

import asyncio
import json
import socket
import time
import uuid
from collections import deque
from dataclasses import dataclass, field

from .gateway_core import ClientAddr, GatewayCore, format_addr


def encode(payload: dict) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def decode(data: bytes) -> dict | None:
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


@dataclass
class Upstream:
    key: str
    label: str
    host: str
    port: int
    ingress_port: int
    addr: tuple[str, int] | None = None


class GatewayRuntime:
    def __init__(
        self,
        core: GatewayCore,
        upstreams: list[Upstream],
        egress_ports: list[int],
        client_ingress_host: str = "127.0.0.1",
    ) -> None:
        self.core = core
        self.upstreams = {u.key: u for u in upstreams}
        self.egress_ports = list(egress_ports)
        self.client_ingress_host = client_ingress_host
        self.ingress: dict[str, asyncio.DatagramTransport] = {}
        self.egress: dict[int, asyncio.DatagramTransport] = {}
        self._upstream_by_source: dict[tuple[str, int], str] = {}
        self._tasks: list[asyncio.Task] = []

    # ------------------------------------------------------------------ setup
    async def resolve_upstreams(self, timeout: float = 30.0) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        for upstream in self.upstreams.values():
            while True:
                try:
                    infos = await loop.getaddrinfo(
                        upstream.host,
                        upstream.port,
                        type=socket.SOCK_DGRAM,
                        family=socket.AF_INET,
                    )
                    upstream.addr = infos[0][4]
                    break
                except OSError as exc:  # DNS may not be ready in compose yet
                    if loop.time() >= deadline:
                        raise RuntimeError(
                            f"无法解析上游 {upstream.key} ({upstream.host}:{upstream.port})"
                        ) from exc
                    await asyncio.sleep(0.25)
        self._upstream_by_source = {
            u.addr: u.key for u in self.upstreams.values() if u.addr is not None
        }

    async def start(self) -> None:
        await self.resolve_upstreams()
        loop = asyncio.get_running_loop()

        for upstream in self.upstreams.values():
            transport, _ = await loop.create_datagram_endpoint(
                lambda key=upstream.key: IngressProtocol(self, key),
                local_addr=("0.0.0.0", upstream.ingress_port),
            )
            self.ingress[upstream.key] = transport

        for port in self.egress_ports:
            transport, _ = await loop.create_datagram_endpoint(
                lambda p=port: EgressProtocol(self, p),
                local_addr=("0.0.0.0", port),
            )
            self.egress[port] = transport

        self._tasks.append(asyncio.create_task(self._expiry_loop(), name="session-expiry"))

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001 - log and keep shutting down
                self.core.log_event(
                    "error", f"后台任务异常退出：{exc}", at=self.core.now()
                )
        self._tasks.clear()
        for transport in [*self.ingress.values(), *self.egress.values()]:
            transport.close()
        self.ingress.clear()
        self.egress.clear()

    async def _expiry_loop(self) -> None:
        while True:
            self.core.expire()
            await asyncio.sleep(0.2)

    # -------------------------------------------------------------- routing
    def ingress_addr(self, upstream_key: str) -> tuple[str, int]:
        return (self.client_ingress_host, self.upstreams[upstream_key].ingress_port)

    def _send_to_client(self, upstream_key: str, addr: ClientAddr, data: bytes) -> None:
        transport = self.ingress.get(upstream_key)
        if transport is not None:
            transport.sendto(data, addr)

    def handle_ingress(
        self, upstream_key: str, data: bytes, addr: ClientAddr, transport: asyncio.DatagramTransport
    ) -> None:
        payload = decode(data)
        request_id = payload.get("request_id") if payload else None
        result = self.core.on_forward(
            addr[0], addr[1], upstream_key, len(data)
        )
        upstream = self.upstreams[upstream_key]
        if result.accepted:
            assert result.egress_port is not None and upstream.addr is not None
            try:
                self.egress[result.egress_port].sendto(data, upstream.addr)
            except OSError as exc:
                self.core.log_event(
                    "error",
                    f"向上游 {upstream.label} 转发失败：{exc}",
                    at=self.core.now(),
                    upstream=upstream_key,
                    egress_port=result.egress_port,
                )
            return

        reason = {
            "kind": "gateway_error",
            "request_id": request_id,
            "reason": result.message,
            "blocked": result.blocked,
            "upstream": upstream_key,
            "upstream_label": upstream.label,
            "at": time.time(),
        }
        transport.sendto(encode(reason), addr)

    def handle_egress(
        self, egress_port: int, data: bytes, source: ClientAddr
    ) -> None:
        upstream_key = self._upstream_by_source.get(source)
        if upstream_key is None:
            self.core.on_unknown_reply(egress_port, source, len(data))
            return
        result = self.core.on_reply(egress_port, upstream_key, len(data))
        if result.delivered and result.client_addr is not None:
            self._send_to_client(upstream_key, result.client_addr, data)


class IngressProtocol(asyncio.DatagramProtocol):
    def __init__(self, runtime: GatewayRuntime, upstream_key: str) -> None:
        self.runtime = runtime
        self.upstream_key = upstream_key
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr: ClientAddr) -> None:
        if self.transport is None:  # pragma: no cover - defensive
            return
        self.runtime.handle_ingress(self.upstream_key, data, addr, self.transport)

    def error_received(self, exc: Exception) -> None:  # pragma: no cover - OS level
        self.runtime.core.log_event(
            "error",
            f"入口 {self.upstream_key} UDP 错误：{exc}",
            at=self.runtime.core.now(),
            upstream=self.upstream_key,
        )


class EgressProtocol(asyncio.DatagramProtocol):
    def __init__(self, runtime: GatewayRuntime, egress_port: int) -> None:
        self.runtime = runtime
        self.egress_port = egress_port

    def datagram_received(self, data: bytes, addr: ClientAddr) -> None:
        self.runtime.handle_egress(self.egress_port, data, addr)

    def error_received(self, exc: Exception) -> None:  # pragma: no cover - OS level
        self.runtime.core.log_event(
            "error",
            f"出口端口 {self.egress_port} UDP 错误：{exc}",
            at=self.runtime.core.now(),
            egress_port=self.egress_port,
        )


# --------------------------------------------------------------------------
# Local clients
# --------------------------------------------------------------------------
@dataclass
class ClientMessage:
    request_id: str
    upstream: str
    upstream_label: str
    body: str
    delay_ms: int
    sent_at: float
    status: str = "waiting"  # waiting | received | rejected | timeout
    sent_mono: float = field(default_factory=time.monotonic)
    rtt_ms: int | None = None
    received: dict | None = None
    error: str | None = None
    blocked: list[dict] = field(default_factory=list)
    note: str | None = None

    def as_dict(self) -> dict:
        return {
            "request_id": self.request_id,
            "upstream": self.upstream,
            "upstream_label": self.upstream_label,
            "body": self.body,
            "delay_ms": self.delay_ms,
            "sent_at": self.sent_at,
            "status": self.status,
            "rtt_ms": self.rtt_ms,
            "received": self.received,
            "error": self.error,
            "blocked": self.blocked,
            "note": self.note,
        }


class ClientAgent(asyncio.DatagramProtocol):
    def __init__(
        self,
        client_id: str,
        label: str,
        runtime: GatewayRuntime,
        bind_ip: str,
        timeout_seconds: float = 2.5,
    ) -> None:
        self.client_id = client_id
        self.label = label
        self.runtime = runtime
        self.bind_ip = bind_ip
        self.timeout_seconds = timeout_seconds
        self.transport: asyncio.DatagramTransport | None = None
        self.local_addr: ClientAddr | None = None
        self.messages: deque[ClientMessage] = deque(maxlen=80)
        self.pending: dict[str, ClientMessage] = {}
        self._counter = 0
        self._task: asyncio.Task | None = None
        self._stopping = False

    async def start(self, loop: asyncio.AbstractEventLoop) -> None:
        try:
            await loop.create_datagram_endpoint(
                lambda: self, local_addr=(self.bind_ip, 0)
            )
        except OSError:
            # Some environments do not allow binding extra loopback aliases.
            await loop.create_datagram_endpoint(
                lambda: self, local_addr=("127.0.0.1", 0)
            )
        self._task = asyncio.create_task(self._sweep_loop(), name=f"client-{self.client_id}")

    async def stop(self) -> None:
        self._stopping = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self.transport is not None:
            self.transport.close()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]
        sockname = transport.get_extra_info("sockname")
        self.local_addr = (sockname[0], sockname[1])

    def send(
        self,
        upstream_key: str,
        delay_ms: int,
        body: str,
    ) -> ClientMessage:
        if self.transport is None or self.local_addr is None:
            raise RuntimeError("client not started")
        upstream = self.runtime.upstreams[upstream_key]
        self._counter += 1
        request_id = (
            f"{self.client_id}-{int(time.time() * 1000)}-{self._counter}-"
            f"{uuid.uuid4().hex[:4]}"
        )
        message = ClientMessage(
            request_id=request_id,
            upstream=upstream_key,
            upstream_label=upstream.label,
            body=body,
            delay_ms=delay_ms,
            sent_at=time.time(),
        )
        payload = {
            "kind": "drill_request",
            "request_id": request_id,
            "client_id": self.client_id,
            "client_label": self.label,
            "client_local": format_addr(self.local_addr),
            "delay_ms": delay_ms,
            "body": body,
            "sent_at": message.sent_at,
        }
        self.messages.append(message)
        self.pending[request_id] = message
        self.transport.sendto(
            encode(payload),
            self.runtime.ingress_addr(upstream_key),
        )
        return message

    def datagram_received(self, data: bytes, addr: ClientAddr) -> None:
        payload = decode(data)
        if payload is None:
            self._orphan(
                {
                    "kind": "raw",
                    "raw": data.decode("utf-8", "replace"),
                    "source": format_addr(addr),
                }
            )
            return

        if payload.get("kind") == "gateway_error":
            request_id = payload.get("request_id")
            message = self.pending.pop(request_id, None) if request_id else None
            if message is None:
                self._orphan(payload)
                return
            message.status = "rejected"
            message.error = payload.get("reason")
            message.blocked = payload.get("blocked") or []
            message.note = "网关没有分配出口端口，未向上游发送"
            return

        request_id = payload.get("request_id")
        message = self.pending.pop(request_id, None) if request_id else None
        if message is None:
            self._orphan(payload)
            return
        message.status = "received"
        message.received = payload
        message.rtt_ms = round((time.monotonic() - message.sent_mono) * 1000)

    def _orphan(self, payload: dict) -> None:
        message = ClientMessage(
            request_id=str(payload.get("request_id") or "-"),
            upstream=str(payload.get("upstream") or "-"),
            upstream_label=str(payload.get("upstream_label") or "未知"),
            body="",
            delay_ms=0,
            sent_at=time.time(),
            status="received",
            received=payload,
            note="收到无法匹配到本次请求的回包",
        )
        self.messages.append(message)

    async def _sweep_loop(self) -> None:
        while not self._stopping:
            await asyncio.sleep(0.2)
            now = time.monotonic()
            for request_id, message in list(self.pending.items()):
                if now - message.sent_mono < self.timeout_seconds:
                    continue
                self.pending.pop(request_id, None)
                message.status = "timeout"
                message.note = (
                    f"等待 {self.timeout_seconds:.1f} 秒仍未收到回包；"
                    "若回包延迟超过会话存活时间，迟到回包会被网关丢弃"
                )

    def snapshot(self) -> dict:
        return {
            "id": self.client_id,
            "label": self.label,
            "local_addr": format_addr(self.local_addr) if self.local_addr else None,
            "pending": len(self.pending),
            "messages": [m.as_dict() for m in list(self.messages)[-30:]][::-1],
        }


class ClientPool:
    def __init__(
        self,
        runtime: GatewayRuntime,
        specs: list[tuple[str, str, str]],
    ) -> None:
        self.clients: dict[str, ClientAgent] = {
            client_id: ClientAgent(client_id, label, runtime, bind_ip)
            for client_id, label, bind_ip in specs
        }

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        for client in self.clients.values():
            await client.start(loop)

    async def stop(self) -> None:
        for client in self.clients.values():
            await client.stop()

    def get(self, client_id: str) -> ClientAgent | None:
        return self.clients.get(client_id)

    def snapshot(self) -> list[dict]:
        return [client.snapshot() for client in self.clients.values()]

    def clear(self) -> None:
        for client in self.clients.values():
            client.messages.clear()
            client.pending.clear()


def parse_ports(value: str) -> list[int]:
    """Parse "17000,17001,17010-17019" into a list of unique ports.

    Ranges (``start-end``) expand in ascending order; comma-separated items
    keep their config order, which is also the allocation order.
    """
    ports: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part[1:]:  # range like 17000-17599 (a leading '-' is invalid input)
            start_text, _, end_text = part.partition("-")
            try:
                start, end = int(start_text), int(end_text)
            except ValueError as exc:
                raise ValueError(f"invalid port range {part!r}") from exc
            if end < start:
                raise ValueError(f"invalid port range {part!r}: end before start")
            ports.extend(range(start, end + 1))
        else:
            ports.append(int(part))
    if not ports:
        raise ValueError("no egress ports configured")
    if len(set(ports)) != len(ports):
        raise ValueError("duplicate ports in configuration")
    for port in ports:
        if not 1 <= port <= 65535:
            raise ValueError(f"port out of range: {port}")
    return ports
