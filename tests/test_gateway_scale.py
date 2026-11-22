"""Scaling guarantees: O(1)-ish hot paths, lazy expiry, bounded timer records.

These tests pin the behaviour required when the gateway runs with hundreds
of egress ports:

* send/receive cost must not grow linearly with sessions or ports (except
  processing items that actually expire and enumerating rejection reasons);
* liveness is judged by the last-forward clock even if the sweeper is late;
* quarantine windows start at the should-expire moment;
* timer records stay proportional to live sessions + quarantines under
  frequent renewals.
"""

from __future__ import annotations

import time

import pytest

from app.gateway import parse_ports
from app.gateway_core import GatewayCore

UPSTREAMS = {"echo-a": "上游 A", "echo-b": "上游 B"}


class Clock:
    def __init__(self) -> None:
        self.value = 1000.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> float:
        self.value += seconds
        return self.value


def make_core(ports: list[int] | int = 2, **kwargs) -> tuple[GatewayCore, Clock]:
    clock = Clock()
    port_list = (
        list(range(17000, 17000 + ports)) if isinstance(ports, int) else list(ports)
    )
    core = GatewayCore(
        egress_ports=port_list,
        upstreams=UPSTREAMS,
        monotonic=clock,
        wallclock=lambda: 1_700_000_000.0 + clock.value,
        **kwargs,
    )
    return core, clock


def live_count(core: GatewayCore) -> int:
    return len(core._sessions) + len(core._quarantines)


# ------------------------------------------------------------- parse_ports
def test_parse_ports_expands_ranges_in_config_order() -> None:
    assert parse_ports("17005,17000-17002") == [17005, 17000, 17001, 17002]
    assert parse_ports("17000-17003") == [17000, 17001, 17002, 17003]
    assert parse_ports("17000,17010-17011,17005") == [17000, 17010, 17011, 17005]
    assert len(parse_ports("17000-17599")) == 600


def test_parse_ports_rejects_bad_values() -> None:
    for bad in ("", "17000-16999", "0", "65536", "17000,17000", "17000-17001,17001"):
        with pytest.raises(ValueError):
            parse_ports(bad)


# ------------------------------------------------ config-order allocation
def test_allocation_follows_config_order_not_numeric_order() -> None:
    core, clock = make_core([17020, 17005, 17010])
    first = core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    second = core.on_forward("127.0.0.12", 40002, "echo-a", 10)
    third = core.on_forward("127.0.0.13", 40003, "echo-a", 10)
    assert (first.egress_port, second.egress_port, third.egress_port) == (
        17020,
        17005,
        17010,
    )
    # The other upstream shares the same first-in-config-order port.
    other = core.on_forward("127.0.0.14", 40004, "echo-b", 10)
    assert other.egress_port == 17020

    # Keep only the 17020 echo-a session alive; let the rest drain.  Once
    # 17005 is free again it is the first available port in config order
    # (17020 is quarantined for echo-a at that point).
    clock.advance(0.5)
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)  # renew the 17020 session
    clock.advance(2.6)  # t=+3.1: 17005 drained; 17020 quarantined until +3.5
    again = core.on_forward("127.0.0.15", 40005, "echo-a", 10)
    assert again.accepted and again.egress_port == 17005


def test_freed_port_is_preferred_in_config_order() -> None:
    core, clock = make_core(3)
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)  # 17000
    core.on_forward("127.0.0.11", 40002, "echo-a", 10)  # 17001
    core.on_forward("127.0.0.11", 40003, "echo-a", 10)  # 17002
    clock.advance(0.5)
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)  # renew only 17000
    clock.advance(2.6)  # t=+3.1: 17001/17002 drained, 17000 quarantined till +3.5
    first = core.on_forward("127.0.0.12", 40010, "echo-a", 10)
    second = core.on_forward("127.0.0.12", 40011, "echo-a", 10)
    assert (first.egress_port, second.egress_port) == (17001, 17002)
    third = core.on_forward("127.0.0.12", 40012, "echo-a", 10)
    assert not third.accepted  # 17000 still quarantined, the rest occupied
    assert {item["port"] for item in third.blocked} == {17000, 17001, 17002}


# ---------------------------------------------------------- lazy expiry
def test_forward_judges_expiry_by_last_forward_without_sweeper() -> None:
    core, clock = make_core(2)
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    clock.advance(1.5)  # expired by the clock; expire() never called

    # The other upstream can take the freed port immediately — the send path
    # retires the stale session on the spot instead of trusting the sweeper.
    result = core.on_forward("127.0.0.12", 40002, "echo-b", 10)
    assert result.accepted and result.egress_port == 17000

    # The old pair is quarantined, anchored at the should-expire moment.
    quarantine = core._quarantines[(17000, "echo-a")]
    assert quarantine.started_at == pytest.approx(1001.0)
    assert quarantine.expires_at == pytest.approx(1003.0)
    assert core.on_reply(17000, "echo-b", 10).delivered  # new session is live
    assert core.on_reply(17000, "echo-a", 10).outcome == "late"  # old pair drops


def test_reply_judges_expiry_by_last_forward_without_sweeper() -> None:
    core, clock = make_core(2)
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    clock.advance(1.5)  # expired by the clock; expire() never called

    late = core.on_reply(17000, "echo-a", 42)
    assert late.outcome == "late" and not late.delivered
    quarantine = core._quarantines[(17000, "echo-a")]
    assert quarantine.started_at == pytest.approx(1001.0)
    assert quarantine.expires_at == pytest.approx(1003.0)


def test_quarantine_fully_elapsed_before_anyone_notices() -> None:
    core, clock = make_core(2)
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    clock.advance(5.0)  # idle (1 s) + quarantine (2 s) fully elapsed

    # No quarantine is recorded; the port is simply free again.
    result = core.on_forward("127.0.0.12", 40002, "echo-a", 10)
    assert result.accepted and result.egress_port == 17000
    assert core._quarantines == {}
    assert core.on_reply(17001, "echo-a", 10).outcome == "no_session"


def test_same_client_after_expiry_starts_a_new_session() -> None:
    core, clock = make_core(2)
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    clock.advance(1.5)
    result = core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    assert result.outcome == "new_session"
    # The old pair is quarantined, so the new session takes the next port.
    assert result.egress_port == 17001


# ------------------------------------------------------- expiry boundaries
def test_session_alive_just_before_boundary() -> None:
    core, clock = make_core(2)
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    clock.advance(0.5)
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)  # expires at 1001.5
    clock.advance(0.999)  # 1 ms before the deadline
    assert core.on_forward("127.0.0.11", 40001, "echo-a", 10).outcome == "reused_session"


def test_session_expires_exactly_at_boundary() -> None:
    core, clock = make_core(2)
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    clock.advance(0.5)
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)  # expires at 1001.5 exactly
    clock.advance(1.0)  # t == last_forward + idle, binary-exact
    result = core.on_forward("127.0.0.12", 40002, "echo-b", 10)
    assert result.accepted and result.egress_port == 17000  # port already free
    quarantine = core._quarantines[(17000, "echo-a")]
    assert quarantine.started_at == 1001.5
    assert quarantine.expires_at == 1003.5


def test_quarantine_boundary_is_exact() -> None:
    core, clock = make_core(2)
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)  # t=1000.0
    clock.advance(1.0)  # session hits its deadline at 1001.0
    core.expire()  # quarantine 1001.0 .. 1003.0
    clock.advance(1.5)  # t=1002.5
    assert core.on_reply(17000, "echo-a", 10).outcome == "late"
    clock.advance(0.5)  # t=1003.0 == quarantine deadline exactly
    assert core.on_reply(17000, "echo-a", 10).outcome == "no_session"
    # ...and the port is allocatable by the same upstream again.
    assert core.on_forward("127.0.0.12", 40002, "echo-a", 10).egress_port == 17000


# ----------------------------------------------------- timer record bound
def test_timer_records_stay_proportional_under_frequent_renewals() -> None:
    core, clock = make_core(128)
    clients = [(f"10.1.{i // 256}.{i % 256}", 41000 + i) for i in range(100)]
    for ip, port in clients:
        core.on_forward(ip, port, "echo-a", 10)

    for _ in range(300):
        clock.advance(0.002)  # never idle long enough to expire
        for ip, port in clients:
            core.on_forward(ip, port, "echo-a", 10)
        assert len(core._timers) <= 2 * live_count(core) + 32

    assert len(core._sessions) == 100
    assert all(s.forward_count == 301 for s in core._sessions.values())
    # All 30 000 renewals left at most a bounded number of timer records.
    assert len(core._timers) <= 2 * live_count(core) + 32


def test_timer_heap_tracks_live_objects_after_churn() -> None:
    core, clock = make_core(64)
    for i in range(64):
        core.on_forward("127.0.0.11", 40000 + i, "echo-a", 10)
    clock.advance(1.0)
    core.expire()  # 64 quarantines
    assert len(core._timers) == live_count(core) == 64
    clock.advance(2.0)
    core.expire()  # quarantines drained
    assert len(core._timers) == live_count(core) == 0


# ------------------------------------------------------- scale behaviour
def test_rejection_enumerates_every_port_at_scale() -> None:
    core, _ = make_core(300)
    for i in range(300):
        assert core.on_forward("127.0.0.11", 40000 + i, "echo-a", 10).accepted
    rejected = core.on_forward("127.0.0.12", 39999, "echo-a", 10)
    assert not rejected.accepted and rejected.outcome == "no_port"
    assert len(rejected.blocked) == 300
    assert {item["cause"] for item in rejected.blocked} == {"active_session"}
    assert {item["port"] for item in rejected.blocked} == set(range(17000, 17300))
    assert "300 个端口均不可用" in rejected.message


def test_occupancy_and_quarantine_are_per_upstream_at_scale() -> None:
    core, clock = make_core(100)
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)  # port 17000
    clock.advance(1.05)
    core.expire()  # (17000, echo-a) quarantined

    # echo-b takes 17000 while echo-a is blocked from it.
    assert core.on_forward("127.0.0.12", 40002, "echo-b", 10).egress_port == 17000
    blocked = core.on_forward("127.0.0.13", 40003, "echo-a", 10)
    assert blocked.accepted and blocked.egress_port == 17001  # next free for echo-a
    assert core.on_reply(17000, "echo-a", 10).outcome == "late"
    assert core.on_reply(17000, "echo-b", 10).delivered


def test_hot_path_cost_does_not_blow_up_at_scale() -> None:
    """600 ports fully occupied: 40k hot-path ops must stay cheap.

    The old scan-per-packet core needed O(sessions) per packet; here the
    whole batch must finish in a time budget with an order-of-magnitude
    margin (typical: well under 0.5 s).
    """
    core, _ = make_core(600)
    for i in range(600):
        core.on_forward(f"10.2.{i // 256}.{i % 256}", 41000 + i, "echo-a", 64)
    for i in range(600):
        core.on_forward(f"10.3.{i // 256}.{i % 256}", 42000 + i, "echo-b", 64)

    start = time.perf_counter()
    for round_no in range(20):
        for i in range(600):
            core.on_forward(f"10.2.{i // 256}.{i % 256}", 41000 + i, "echo-a", 64)
        for i in range(600):
            assert core.on_reply(17000 + i, "echo-a", 32).delivered
    elapsed = time.perf_counter() - start
    assert elapsed < 3.0, f"24 000 hot-path ops took {elapsed:.2f}s"
    assert core._counters["delivered"] == 20 * 600
    assert len(core._timers) <= 2 * live_count(core) + 32
