"""Measure GatewayCore per-packet hot-path cost at several port/session scales.

Run:  python3 -I scripts/bench_scale.py

The clock is fake, so nothing expires unless the script advances it.  Each
scale reports the average cost of the three hot operations:

* alloc  — on_forward for a brand-new (client, upstream) pair
* renew  — on_forward for an existing session (the steady-state hot path)
* reply  — on_reply delivery to the owning session

plus the same operations while half the ports sit in quarantine, the
rejection scan (intentionally O(ports): it enumerates per-port reasons) and
the observed timer-heap high-water mark vs. live objects.
"""

from __future__ import annotations

import random
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.gateway_core import GatewayCore

UPSTREAMS = {"up-a": "上游 A", "up-b": "上游 B"}


def make_core(ports: int, clock: list[float]) -> GatewayCore:
    return GatewayCore(
        egress_ports=list(range(20000, 20000 + ports)),
        upstreams=UPSTREAMS,
        monotonic=lambda: clock[0],
        wallclock=lambda: 1_700_000_000.0 + clock[0],
    )


def time_ops(count: int, fn) -> float:
    start = time.perf_counter_ns()
    for _ in range(count):
        fn()
    return (time.perf_counter_ns() - start) / count


def bench_scale(ports: int, ops: int = 20000) -> dict:
    clock = [1000.0]
    core = make_core(ports, clock)
    rng = random.Random(20261122)
    clients = [(f"10.{i // 65536}.{(i // 256) % 256}.{i % 256}", 40000 + i) for i in range(ports)]

    # Fill every port of up-a with one session (new-session allocation path).
    start = time.perf_counter_ns()
    for i, (ip, port) in enumerate(clients):
        result = core.on_forward(ip, port, "up-a", 64)
        assert result.accepted and result.egress_port == 20000 + i, result
    alloc_ns = (time.perf_counter_ns() - start) / ports

    renew_ns = time_ops(ops, lambda: core.on_forward(*rng.choice(clients), "up-a", 64))
    pairs = [(20000 + i, "up-a") for i in range(ports)]
    reply_ns = time_ops(ops, lambda: core.on_reply(*rng.choice(pairs), 32))

    # Idle out every up-a session: all ports go into quarantine for up-a.
    clock[0] += 1.0
    core.expire()
    assert len(core._quarantines) == ports

    # Half the ports additionally get a live up-b session (port sharing).
    mixed_clients = clients[: ports // 2]
    for ip, port in mixed_clients:
        assert core.on_forward(ip, port, "up-b", 64).accepted
    renew_mixed_ns = time_ops(ops, lambda: core.on_forward(*rng.choice(mixed_clients), "up-b", 64))
    mixed_pairs = [(20000 + i, "up-b") for i in range(ports // 2)]
    reply_mixed_ns = time_ops(ops, lambda: core.on_reply(*rng.choice(mixed_pairs), 32))

    # Rejection path: every port is quarantined for up-a right now.
    start = time.perf_counter_ns()
    rejected = core.on_forward("192.0.2.1", 5000, "up-a", 64)
    reject_ms = (time.perf_counter_ns() - start) / 1e6
    assert not rejected.accepted and len(rejected.blocked) == ports

    return {
        "ports": ports,
        "alloc_ns": alloc_ns,
        "renew_ns": renew_ns,
        "reply_ns": reply_ns,
        "renew_ns_quarantine": renew_mixed_ns,
        "reply_ns_quarantine": reply_mixed_ns,
        "reject_ms": reject_ms,
        "timers": len(core._timers),
        "live": len(core._sessions) + len(core._quarantines),
    }


def bench_renewal_churn() -> tuple[int, int]:
    """Frequent renewals must not inflate the timer heap beyond O(live)."""
    clock = [1000.0]
    core = make_core(128, clock)
    clients = [(f"10.1.{i // 256}.{i % 256}", 41000 + i) for i in range(100)]
    for ip, port in clients:
        core.on_forward(ip, port, "up-a", 32)
    high_water = 0
    for _ in range(500):
        clock[0] += 0.001  # 0.5 s of simulated time, no expiries
        for ip, port in clients:
            core.on_forward(ip, port, "up-a", 32)
        high_water = max(high_water, len(core._timers))
    live = len(core._sessions) + len(core._quarantines)
    return high_water, live


def main() -> int:
    print("GatewayCore 热路径基准（假时钟，单线程，ns/op 越低越好）")
    print(
        f"{'端口数':>6} {'新会话alloc':>12} {'续期renew':>12} {'回包reply':>12} "
        f"{'续期(有保留)':>14} {'回包(有保留)':>14} {'拒绝枚举':>10} {'计时堆/存活':>12}"
    )
    for ports in (64, 256, 1024):
        row = bench_scale(ports)
        print(
            f"{row['ports']:>6} "
            f"{row['alloc_ns']:>12.0f} "
            f"{row['renew_ns']:>12.0f} "
            f"{row['reply_ns']:>12.0f} "
            f"{row['renew_ns_quarantine']:>14.0f} "
            f"{row['reply_ns_quarantine']:>14.0f} "
            f"{row['reject_ms']:>8.2f}ms "
            f"{row['timers']:>6}/{row['live']:<5}"
        )
    high_water, live = bench_renewal_churn()
    print(
        f"\n频繁续期：100 个会话各续期 500 次（共 50 000 次），"
        f"计时堆峰值 {high_water} 条，存活对象 {live} 个"
        f"（上限 2×存活+32 = {2 * live + 32}）"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
