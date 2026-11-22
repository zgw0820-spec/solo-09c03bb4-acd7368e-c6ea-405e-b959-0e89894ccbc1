"""UDP echo services with a configurable reply delay (0–2 s).

Each service answers only the payload it understands (kind=drill_request),
echoes the request back wrapped in a response envelope, and reports which
gateway egress port it saw.  The response is delayed by exactly the delay the
web page requested, which is what makes late replies (beyond the 1 s session
idle window) reproducible.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import time

MAX_DELAY_SECONDS = 2.0


def encode(payload: dict) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


class EchoService(asyncio.DatagramProtocol):
    def __init__(self, name: str, max_delay: float = MAX_DELAY_SECONDS) -> None:
        self.name = name
        self.max_delay = max_delay
        self.transport: asyncio.DatagramTransport | None = None
        self.received = 0
        self.replied = 0
        self.dropped_invalid = 0
        self._tasks: set[asyncio.Task] = set()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        self.received += 1
        try:
            payload = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            payload = None
        if not isinstance(payload, dict) or payload.get("kind") != "drill_request":
            self.dropped_invalid += 1
            return
        try:
            delay = float(payload.get("delay_ms", 0)) / 1000.0
        except (TypeError, ValueError):
            delay = 0.0
        delay = min(max(delay, 0.0), self.max_delay)
        task = asyncio.create_task(self._reply_later(data, addr, delay, payload))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _reply_later(
        self,
        data: bytes,
        addr: tuple[str, int],
        delay: float,
        payload: dict,
    ) -> None:
        requested = payload.get("delay_ms", 0)
        received_at = time.time()
        if delay > 0:
            await asyncio.sleep(delay)
        if self.transport is None:
            return
        sockname = self.transport.get_extra_info("sockname")
        response = {
            "kind": "drill_response",
            "echo_service": self.name,
            "echo_addr": f"{sockname[0]}:{sockname[1]}",
            "gateway_egress": f"{addr[0]}:{addr[1]}",
            "gateway_egress_port": addr[1],
            "request_id": payload.get("request_id"),
            "client_local": payload.get("client_local"),
            "client_id": payload.get("client_id"),
            "body": payload.get("body"),
            "requested_delay_ms": requested,
            "applied_delay_ms": round(delay * 1000),
            "request_sent_at": payload.get("sent_at"),
            "received_at": received_at,
            "responded_at": time.time(),
            "raw_bytes": len(data),
        }
        try:
            self.transport.sendto(encode(response), addr)
            self.replied += 1
        except OSError:
            pass


async def serve(name: str, port: int) -> None:
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: EchoService(name),
        local_addr=("0.0.0.0", port),
    )
    stop = asyncio.Event()

    def _stop(*_: object) -> None:
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:  # pragma: no cover
            pass
    print(f"echo service {name} listening on udp/{port}", flush=True)
    try:
        await stop.wait()
    finally:
        protocol._tasks.clear()
        transport.close()


def main() -> None:
    name = os.environ.get("ECHO_NAME", "echo")
    port = int(os.environ.get("ECHO_PORT", "16001"))
    asyncio.run(serve(name, port))


if __name__ == "__main__":
    main()
