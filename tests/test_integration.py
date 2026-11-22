"""End-to-end: real UDP sockets, real echo services, real gateway routing."""

from __future__ import annotations

import asyncio
import socket

import pytest

from app.echo_service import EchoService
from app.gateway import ClientPool, GatewayRuntime, Upstream
from app.gateway_core import GatewayCore


async def start_echo(name: str, port: int) -> asyncio.DatagramTransport:
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: EchoService(name), local_addr=("127.0.0.1", port)
    )
    return transport


async def wait_for(predicate, timeout: float = 3.0, interval: float = 0.02):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        value = predicate()
        if value:
            return value
        await asyncio.sleep(interval)
    return predicate()


@pytest.fixture
async def drill_env():
    echo_a = await start_echo("echo-a", 16601)
    echo_b = await start_echo("echo-b", 16602)
    upstreams = [
        Upstream("echo-a", "上游 A", "127.0.0.1", 16601, 15601),
        Upstream("echo-b", "上游 B", "127.0.0.1", 16602, 15602),
    ]
    core = GatewayCore(
        egress_ports=[17600, 17601],
        upstreams={"echo-a": "上游 A", "echo-b": "上游 B"},
    )
    runtime = GatewayRuntime(core, upstreams, [17600, 17601])
    await runtime.start()
    clients = ClientPool(
        runtime,
        [
            ("client-1", "客户端 1", "127.0.0.11"),
            ("client-2", "客户端 2", "127.0.0.12"),
            ("client-3", "客户端 3", "127.0.0.13"),
        ],
    )
    await clients.start()
    try:
        yield core, runtime, clients
    finally:
        await clients.stop()
        await runtime.stop()
        echo_a.close()
        echo_b.close()


async def test_three_clients_two_upstreams_round_trip(drill_env):
    core, _, clients = drill_env
    one = clients.get("client-1").send("echo-a", 0, "hello-a")
    two = clients.get("client-2").send("echo-b", 0, "hello-b")
    three = clients.get("client-3").send("echo-a", 0, "hello-a2")

    assert await wait_for(lambda: one.status != "waiting"), one
    assert await wait_for(lambda: two.status != "waiting"), two
    three_result = await wait_for(lambda: three.status != "waiting" and three)

    assert one.status == "received"
    assert two.status == "received"
    assert one.received["gateway_egress_port"] == 17600
    assert two.received["gateway_egress_port"] == 17600  # port shared, upstream distinct
    # third client also talks to echo-a; second echo-a session needs the other port.
    assert three_result.status == "received"
    assert three_result.received["gateway_egress_port"] == 17601
    assert three.received["echo_service"] == "echo-a"
    assert one.received["body"] == "hello-a"

    snapshot = core.snapshot()
    assert len(snapshot["sessions"]) == 3
    assert snapshot["counters"]["delivered"] == 3


async def test_no_available_port_is_reported_to_client(drill_env):
    core, _, clients = drill_env
    first = clients.get("client-1").send("echo-a", 0, "")
    second = clients.get("client-2").send("echo-a", 0, "")
    assert await wait_for(lambda: first.status == "received")
    assert await wait_for(lambda: second.status == "received")

    third = clients.get("client-3").send("echo-a", 0, "")
    assert await wait_for(lambda: third.status == "rejected", timeout=2.0)
    assert third.received is None
    assert "无可用出口端口" in third.error
    assert {item["port"] for item in third.blocked} == {17600, 17601}
    assert core.snapshot()["counters"]["rejected"] == 1


async def test_late_reply_is_dropped_and_not_delivered_elsewhere(drill_env):
    core, _, clients = drill_env
    # Echo-a answers after 1.2 s: session (1 s) has already expired.
    late = clients.get("client-1").send("echo-a", 1200, "late")
    assert await wait_for(lambda: late.status == "timeout", timeout=3.2)
    assert late.received is None
    assert core.snapshot()["counters"]["dropped_late"] >= 1

    # Reusing the port for the other upstream must not surface the late reply.
    other = clients.get("client-2").send("echo-b", 0, "fresh")
    assert await wait_for(lambda: other.status == "received")
    await asyncio.sleep(0.1)
    assert other.received["body"] == "fresh"
    assert other.received["gateway_egress_port"] == 17600


async def test_session_survives_within_one_second(drill_env):
    core, _, clients = drill_env
    client = clients.get("client-1")
    first = client.send("echo-a", 0, "one")
    assert await wait_for(lambda: first.status == "received")
    await asyncio.sleep(0.5)
    second = client.send("echo-a", 800, "two")
    # second forward refreshes the session; the 800 ms reply keeps it alive long
    # enough at send time but expires 1 s after the forward.
    assert await wait_for(lambda: second.received is not None, timeout=1.0)
    sessions = core.snapshot()["sessions"]
    assert len(sessions) == 1 and sessions[0]["forward_count"] == 2


async def test_unknown_reply_is_dropped(drill_env):
    core, _, _ = drill_env
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)
    sock.sendto(b"rogue", ("127.0.0.1", 17601))
    await asyncio.sleep(0.1)
    sock.close()
    assert core.snapshot()["counters"]["dropped_unknown"] >= 1


async def test_hundreds_of_egress_ports_round_trip():
    """Real sockets at drill scale: 260 egress ports, first-fit in config order."""
    port_count = 260
    egress_ports = list(range(18000, 18000 + port_count))
    echo_a = await start_echo("echo-a", 16801)
    echo_b = await start_echo("echo-b", 16802)
    upstreams = [
        Upstream("echo-a", "上游 A", "127.0.0.1", 16801, 15801),
        Upstream("echo-b", "上游 B", "127.0.0.1", 16802, 15802),
    ]
    core = GatewayCore(
        egress_ports=egress_ports,
        upstreams={"echo-a": "上游 A", "echo-b": "上游 B"},
    )
    runtime = GatewayRuntime(core, upstreams, egress_ports)
    await runtime.start()
    clients = ClientPool(
        runtime,
        [
            ("client-1", "客户端 1", "127.0.0.11"),
            ("client-2", "客户端 2", "127.0.0.12"),
            ("client-3", "客户端 3", "127.0.0.13"),
        ],
    )
    await clients.start()
    try:
        assert len(runtime.egress) == port_count  # one real socket per port
        one = clients.get("client-1").send("echo-a", 0, "scale-a")
        two = clients.get("client-2").send("echo-b", 0, "scale-b")
        three = clients.get("client-3").send("echo-a", 0, "scale-a2")

        assert await wait_for(lambda: one.status != "waiting"), one
        assert await wait_for(lambda: two.status != "waiting"), two
        assert await wait_for(lambda: three.status != "waiting"), three

        assert one.status == two.status == three.status == "received"
        # Different upstreams share the first port; the second echo-a session
        # takes the next port in config order.
        assert one.received["gateway_egress_port"] == 18000
        assert two.received["gateway_egress_port"] == 18000
        assert three.received["gateway_egress_port"] == 18001

        snapshot = core.snapshot()
        assert len(snapshot["ports"]) == port_count
        assert snapshot["counters"]["delivered"] == 3
    finally:
        await clients.stop()
        await runtime.stop()
        echo_a.close()
        echo_b.close()
