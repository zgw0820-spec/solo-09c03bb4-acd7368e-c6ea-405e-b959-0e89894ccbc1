"""Timing and allocation rules for the dual-egress-port gateway (fake clock)."""

from __future__ import annotations

import pytest

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


def make_core(**kwargs) -> tuple[GatewayCore, Clock]:
    clock = Clock()
    core = GatewayCore(
        egress_ports=[17000, 17001],
        upstreams=UPSTREAMS,
        monotonic=clock,
        wallclock=lambda: 1_700_000_000.0 + clock.value,
        **kwargs,
    )
    return core, clock


def test_different_upstreams_share_both_ports() -> None:
    core, _ = make_core()
    first = core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    second = core.on_forward("127.0.0.12", 40002, "echo-b", 10)

    assert first.accepted and second.accepted
    assert first.egress_port == 17000
    assert second.egress_port == 17000  # shared port, different upstream
    ports = {s["port"]: s for s in core.snapshot()["ports"]}
    assert [s["upstream"] for s in ports[17000]["sessions"]] == ["echo-a", "echo-b"]


def test_same_upstream_sessions_need_separate_ports() -> None:
    core, _ = make_core()
    a = core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    b = core.on_forward("127.0.0.12", 40002, "echo-a", 10)
    c = core.on_forward("127.0.0.13", 40003, "echo-a", 10)

    assert a.egress_port == 17000
    assert b.egress_port == 17001
    assert c.accepted is False and c.outcome == "no_port" and c.egress_port is None
    assert {item["port"] for item in c.blocked} == {17000, 17001}
    assert {item["cause"] for item in c.blocked} == {"active_session"}
    assert "无可用出口端口" in c.message


def test_session_is_reused_for_same_client_and_upstream() -> None:
    core, clock = make_core()
    a = core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    clock.advance(0.5)
    b = core.on_forward("127.0.0.11", 40001, "echo-a", 12)

    assert b.outcome == "reused_session"
    assert b.egress_port == a.egress_port
    session = core.snapshot()["sessions"][0]
    assert session["forward_count"] == 2
    assert session["remaining_ms"] == pytest.approx(1000, abs=5)


def test_same_client_to_different_upstream_is_a_separate_session() -> None:
    core, _ = make_core()
    a = core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    b = core.on_forward("127.0.0.11", 40001, "echo-b", 10)
    assert a.egress_port == b.egress_port == 17000
    assert len(core.snapshot()["sessions"]) == 2


def test_session_expires_one_second_after_last_forward() -> None:
    core, clock = make_core()
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    clock.advance(0.6)
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)  # refresh
    clock.advance(0.9)
    assert len(core.snapshot()["sessions"]) == 1
    clock.advance(0.2)  # 1.1 s since last forward
    snapshot = core.snapshot()
    assert snapshot["sessions"] == []
    quarantine = snapshot["ports"][0]["quarantines"]
    assert quarantine == [
        {"upstream": "echo-a", "upstream_label": "上游 A", "remaining_ms": pytest.approx(1900, abs=20)}
    ]


def test_port_and_upstream_pair_kept_two_seconds_and_late_reply_dropped() -> None:
    core, clock = make_core()
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    clock.advance(1.05)
    core.expire()

    late = core.on_reply(17000, "echo-a", 42)
    assert late.outcome == "late"
    assert late.delivered is False and late.client_addr is None
    assert "迟到回包被丢弃" in late.message

    clock.advance(1.0)  # 2.05 s after expiry start? -> still inside 2 s pair window
    assert core.on_reply(17000, "echo-a", 42).outcome == "late"

    clock.advance(1.1)  # now beyond last_forward + 1 + 2
    assert core.on_reply(17000, "echo-a", 42).outcome == "no_session"


def test_port_can_be_reused_by_other_upstream_while_pair_quarantined() -> None:
    core, clock = make_core()
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    clock.advance(1.05)
    core.expire()

    reused = core.on_forward("127.0.0.12", 40002, "echo-b", 10)
    assert reused.accepted is True
    assert reused.egress_port == 17000

    # echo-b's live session on the same port still receives replies.
    delivered = core.on_reply(17000, "echo-b", 30)
    assert delivered.delivered is True
    assert delivered.client_addr == ("127.0.0.12", 40002)

    # echo-a's late reply for the quarantined pair is still discarded.
    assert core.on_reply(17000, "echo-a", 10).outcome == "late"


def test_quarantined_pair_blocks_same_upstream_but_not_other_upstream() -> None:
    core, clock = make_core()
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    core.on_forward("127.0.0.12", 40002, "echo-a", 10)  # ports 17000, 17001
    clock.advance(1.05)
    core.expire()  # both echo-a pairs quarantined, both ports free for echo-b

    echo_b = core.on_forward("127.0.0.13", 40003, "echo-b", 10)
    assert echo_b.accepted is True and echo_b.egress_port == 17000

    echo_a = core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    assert echo_a.accepted is False
    assert {item["port"] for item in echo_a.blocked} == {17000, 17001}
    assert {item["cause"] for item in echo_a.blocked} == {"quarantine"}
    assert "保留期" in echo_a.message

    clock.advance(2.0)  # quarantine over; echo-b's short session has idled out too
    again = core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    assert again.accepted is True
    assert again.egress_port == 17000


def test_reply_without_session_is_not_delivered() -> None:
    core, _ = make_core()
    result = core.on_reply(17001, "echo-b", 5)
    assert result.outcome == "no_session" and not result.delivered
    assert core.snapshot()["counters"]["dropped_unknown"] == 1


def test_reply_is_delivered_only_to_owning_client() -> None:
    core, _ = make_core()
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    core.on_forward("127.0.0.12", 40002, "echo-b", 10)
    reply_a = core.on_reply(17000, "echo-a", 20)
    reply_b = core.on_reply(17000, "echo-b", 20)
    assert reply_a.client_addr == ("127.0.0.11", 40001)
    assert reply_b.client_addr == ("127.0.0.12", 40002)


def test_no_port_message_mentions_both_blocking_reasons() -> None:
    core, clock = make_core()
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    core.on_forward("127.0.0.12", 40002, "echo-a", 10)
    clock.advance(0.5)
    result = core.on_forward("127.0.0.13", 40003, "echo-a", 10)
    assert "被同一上游的会话占用" in result.message
    assert "剩余" in result.message


def test_single_egress_port_configuration() -> None:
    clock = Clock()
    core = GatewayCore(
        egress_ports=[17000],
        upstreams=UPSTREAMS,
        monotonic=clock,
        wallclock=lambda: 0.0,
    )
    a = core.on_forward("127.0.0.11", 1, "echo-a", 1)
    b = core.on_forward("127.0.0.12", 2, "echo-b", 1)
    c = core.on_forward("127.0.0.13", 3, "echo-b", 1)
    assert a.egress_port == b.egress_port == 17000
    assert c.accepted is False


def test_reset_clears_sessions_and_counters() -> None:
    core, _ = make_core()
    core.on_forward("127.0.0.11", 40001, "echo-a", 10)
    snapshot = core.reset()
    assert snapshot["sessions"] == []
    assert snapshot["counters"]["forwarded"] == 0
    assert snapshot["ports"][0]["quarantines"] == []
    assert [e["kind"] for e in snapshot["events"]] == ["reset"]


def test_invalid_configuration_is_rejected() -> None:
    with pytest.raises(ValueError):
        GatewayCore(egress_ports=[], upstreams=UPSTREAMS)
    with pytest.raises(ValueError):
        GatewayCore(egress_ports=[17000, 17000], upstreams=UPSTREAMS)
