"""UDP session / egress-port bookkeeping for the drill gateway.

The core is deliberately free of sockets so that the timing rules can be
tested with an injected monotonic clock:

* A session is identified by (client address, upstream key).
* Every upstream may use each egress port at most once at a time, but two
  different upstreams may share the same egress port.
* A session expires 1 s after its last packet was forwarded upstream.
* The pair (egress port, upstream) is then kept in quarantine for another
  2 s.  Replies matching a quarantined pair are dropped.  The port itself
  may immediately be used for a *different* upstream.

Scaling notes (hundreds of egress ports, many sessions)
-------------------------------------------------------
The per-packet hot paths never scan the session table or the port list:

* ``_by_egress`` maps ``(egress_port, upstream)`` to the owning session, so
  reply delivery and "is this port busy for this upstream" are O(1) dict
  lookups.
* Each upstream owns a min-heap of *free port indices* (index order ==
  config order), so allocating the first available port in config order is
  one ``heappop`` — O(log P) — and returning a port is one ``heappush``.
* Expiry is driven by a min-heap of ``(expires_at, seq, kind, key)`` timer
  entries.  ``_expire_due`` pops only entries whose time has come, so the
  cost of a packet is proportional to the number of *actually expiring*
  items, not to the total number of sessions.  Renewals push a fresh entry
  and invalidate the old one lazily (the entry stores the expiry it was
  created for; a renewed session no longer matches).  A compaction rebuild
  keeps the heap within a constant factor of ``sessions + quarantines`` no
  matter how often sessions are renewed.
* Send/receive paths judge liveness directly from ``last_forward_at`` and
  run ``_expire_due`` themselves, so a late background sweeper cannot make
  an expired session look alive or a live one look expired.  Quarantine
  windows are anchored at the moment the session *should* have expired,
  never at the moment the sweeper happened to notice.

The only operations whose cost grows with the port count are the ones that
must touch every port by definition: enumerating per-port rejection reasons
when allocation fails, and rendering the status snapshot.
"""

from __future__ import annotations

import heapq
import time
from collections.abc import Callable
from dataclasses import dataclass, field

ClientAddr = tuple[str, int]

# Timer heap is rebuilt from live state once it exceeds
# 2 * (sessions + quarantines) + _TIMER_SLACK entries.
_TIMER_SLACK = 32


def format_addr(addr: ClientAddr) -> str:
    return f"{addr[0]}:{addr[1]}"


@dataclass
class Session:
    client_ip: str
    client_port: int
    upstream_key: str
    egress_port: int
    created_at: float
    last_forward_at: float
    forward_count: int = 1

    @property
    def client_addr(self) -> ClientAddr:
        return (self.client_ip, self.client_port)

    @property
    def key(self) -> tuple[str, int, str]:
        return (self.client_ip, self.client_port, self.upstream_key)


@dataclass
class Quarantine:
    egress_port: int
    upstream_key: str
    started_at: float
    expires_at: float


@dataclass
class ForwardResult:
    accepted: bool
    egress_port: int | None
    outcome: str  # new_session | reused_session | no_port
    message: str
    blocked: list[dict] = field(default_factory=list)


@dataclass
class ReplyResult:
    delivered: bool
    client_addr: ClientAddr | None
    outcome: str  # delivered | late | no_session
    message: str


class GatewayCore:
    def __init__(
        self,
        egress_ports: list[int],
        upstreams: dict[str, str],
        *,
        session_idle_seconds: float = 1.0,
        quarantine_seconds: float = 2.0,
        monotonic: Callable[[], float] = time.monotonic,
        wallclock: Callable[[], float] = time.time,
        event_limit: int = 400,
    ) -> None:
        if not egress_ports:
            raise ValueError("at least one egress port is required")
        if len(set(egress_ports)) != len(egress_ports):
            raise ValueError("egress ports must be unique")
        if session_idle_seconds <= 0 or quarantine_seconds < 0:
            raise ValueError("invalid session timing")

        self.egress_ports = list(egress_ports)
        self.upstreams = dict(upstreams)
        self.session_idle_seconds = float(session_idle_seconds)
        self.quarantine_seconds = float(quarantine_seconds)
        self._monotonic = monotonic
        self._wallclock = wallclock
        self._event_limit = event_limit

        self._sessions: dict[tuple[str, int, str], Session] = {}
        self._by_egress: dict[tuple[int, str], Session] = {}
        self._quarantines: dict[tuple[int, str], Quarantine] = {}
        self._port_index = {port: idx for idx, port in enumerate(self.egress_ports)}
        # Per-upstream min-heaps of free port *indices* (config order).  A
        # port index is in the heap iff the port is currently usable by that
        # upstream — no live session and no live quarantine on the pair.
        self._free: dict[str, list[int]] = {
            key: list(range(len(self.egress_ports))) for key in self.upstreams
        }
        # Min-heap of (expires_at, seq, kind, key); kind is "session" or
        # "quarantine".  Entries are validated against live state when popped,
        # so renewals can simply push a newer entry.
        self._timers: list[tuple[float, int, str, object]] = []
        self._timer_seq = 0
        self._events: list[dict] = []
        self._seq = 0
        self._counters: dict[str, int] = {
            "packets_in": 0,
            "forwarded": 0,
            "rejected": 0,
            "replies_in": 0,
            "delivered": 0,
            "dropped_late": 0,
            "dropped_unknown": 0,
            "sessions_created": 0,
            "sessions_expired": 0,
            "quarantines_started": 0,
        }

    # ------------------------------------------------------------------ time
    def now(self) -> float:
        return self._monotonic()

    def wallclock(self) -> float:
        return self._wallclock()

    def _upstream_label(self, key: str) -> str:
        return self.upstreams.get(key, key)

    @staticmethod
    def _secs(value: float) -> str:
        return f"{max(0.0, value):.1f} 秒"

    # --------------------------------------------------------------- events
    def log_event(
        self,
        kind: str,
        message: str,
        *,
        at: float,
        client: str | None = None,
        upstream: str | None = None,
        egress_port: int | None = None,
        byte_count: int | None = None,
    ) -> dict:
        self._seq += 1
        event = {
            "seq": self._seq,
            "at": self._wallclock(),
            "kind": kind,
            "client": client,
            "upstream": upstream,
            "upstream_label": self._upstream_label(upstream) if upstream else None,
            "egress_port": egress_port,
            "bytes": byte_count,
            "message": message,
        }
        self._events.append(event)
        if len(self._events) > self._event_limit:
            del self._events[: len(self._events) - self._event_limit]
        return event

    # --------------------------------------------------------------- timers
    def _push_timer(self, expires_at: float, kind: str, key: object) -> None:
        self._timer_seq += 1
        heapq.heappush(self._timers, (expires_at, self._timer_seq, kind, key))
        live = len(self._sessions) + len(self._quarantines)
        if len(self._timers) > 2 * live + _TIMER_SLACK:
            self._compact_timers()

    def _compact_timers(self) -> None:
        """Rebuild the timer heap from live state only.

        Stale entries (left behind by renewals) are dropped.  The rebuild is
        O(live) but runs at most once per O(live) pushes, so the amortized
        cost per push stays O(1) and the heap never holds more than a
        constant multiple of the live sessions plus quarantines.
        """
        timers: list[tuple[float, int, str, object]] = []
        idle = self.session_idle_seconds
        for session in self._sessions.values():
            self._timer_seq += 1
            timers.append(
                (session.last_forward_at + idle, self._timer_seq, "session", session.key)
            )
        for key, quarantine in self._quarantines.items():
            self._timer_seq += 1
            timers.append((quarantine.expires_at, self._timer_seq, "quarantine", key))
        heapq.heapify(timers)
        self._timers = timers

    def _free_heap_for(self, upstream_key: str) -> list[int]:
        heap = self._free.get(upstream_key)
        if heap is None:  # upstream not in config: behave like a fresh upstream
            heap = self._free[upstream_key] = list(range(len(self.egress_ports)))
        return heap

    def _release_port(self, egress_port: int, upstream_key: str) -> None:
        heapq.heappush(self._free_heap_for(upstream_key), self._port_index[egress_port])

    # ------------------------------------------------------------- expiry
    def _retire_session(
        self, key: tuple[str, int, str], session: Session, now: float
    ) -> None:
        """Move an expired session out of the live tables.

        The quarantine window is anchored at the moment the session *should*
        have expired (last forward + idle), however late the caller is.  If
        that window has already fully elapsed, the port goes straight back to
        the upstream's free heap.
        """
        del self._sessions[key]
        self._by_egress.pop((session.egress_port, session.upstream_key), None)
        self._counters["sessions_expired"] += 1
        expires_at = session.last_forward_at + self.session_idle_seconds
        q_until = expires_at + self.quarantine_seconds
        quarantine_note = ""
        if now < q_until:
            self._quarantines[(session.egress_port, session.upstream_key)] = Quarantine(
                egress_port=session.egress_port,
                upstream_key=session.upstream_key,
                started_at=expires_at,
                expires_at=q_until,
            )
            self._push_timer(q_until, "quarantine", (session.egress_port, session.upstream_key))
            self._counters["quarantines_started"] += 1
            quarantine_note = (
                f"；出口端口 {session.egress_port} 与"
                f" {self._upstream_label(session.upstream_key)} 的组合保留到"
                f" {self._secs(q_until - now)}后"
            )
        else:
            self._release_port(session.egress_port, session.upstream_key)
        self.log_event(
            "session_expired",
            f"会话失效：{format_addr(session.client_addr)} → "
            f"{self._upstream_label(session.upstream_key)}，出口端口 {session.egress_port}"
            f"{quarantine_note}",
            at=now,
            client=format_addr(session.client_addr),
            upstream=session.upstream_key,
            egress_port=session.egress_port,
        )

    def _retire_quarantine(
        self, key: tuple[int, str], quarantine: Quarantine, now: float
    ) -> None:
        del self._quarantines[key]
        self._release_port(quarantine.egress_port, quarantine.upstream_key)
        self.log_event(
            "quarantine_ended",
            f"迟到回包保留期结束：出口端口 {quarantine.egress_port} / "
            f"{self._upstream_label(quarantine.upstream_key)}",
            at=now,
            upstream=quarantine.upstream_key,
            egress_port=quarantine.egress_port,
        )

    def _expire_due(self, now: float) -> None:
        """Process timer entries whose deadline has passed — nothing else."""
        idle = self.session_idle_seconds
        timers = self._timers
        while timers and timers[0][0] <= now:
            expires_at, _, kind, key = heapq.heappop(timers)
            if kind == "session":
                session = self._sessions.get(key)  # type: ignore[arg-type]
                if session is None or session.last_forward_at + idle != expires_at:
                    continue  # stale entry from before a renewal
                self._retire_session(key, session, now)  # type: ignore[arg-type]
            else:
                quarantine = self._quarantines.get(key)  # type: ignore[arg-type]
                if quarantine is None or quarantine.expires_at != expires_at:
                    continue
                self._retire_quarantine(key, quarantine, now)  # type: ignore[arg-type]

    def expire(self, now: float | None = None) -> None:
        """Move expired sessions into quarantine and retire old quarantines.

        Only *due* timer entries are touched; the cost is proportional to the
        number of items that actually expired, independent of how many
        sessions or ports exist.
        """
        self._expire_due(self.now() if now is None else now)

    # ------------------------------------------------------------- forwarding
    def on_forward(
        self,
        client_ip: str,
        client_port: int,
        upstream_key: str,
        byte_count: int,
        now: float | None = None,
    ) -> ForwardResult:
        now = self.now() if now is None else now
        self._expire_due(now)
        self._counters["packets_in"] += 1
        client = format_addr((client_ip, client_port))
        session_key = (client_ip, client_port, upstream_key)

        session = self._sessions.get(session_key)
        if session is not None and now >= session.last_forward_at + self.session_idle_seconds:
            # The sweeper has not caught up yet: judge by last-forward time.
            self._retire_session(session_key, session, now)
            session = None

        if session is not None:
            session.last_forward_at = now
            session.forward_count += 1
            self._push_timer(now + self.session_idle_seconds, "session", session_key)
            self._counters["forwarded"] += 1
            message = (
                f"沿用会话，出口端口 {session.egress_port}，转发至 "
                f"{self._upstream_label(upstream_key)}"
            )
            self.log_event(
                "forward",
                message,
                at=now,
                client=client,
                upstream=upstream_key,
                egress_port=session.egress_port,
                byte_count=byte_count,
            )
            return ForwardResult(True, session.egress_port, "reused_session", message)

        free_heap = self._free_heap_for(upstream_key)
        if free_heap:
            chosen = self.egress_ports[heapq.heappop(free_heap)]
        else:
            chosen = None

        if chosen is None:
            # Allocation failed: enumerate every port's blocking reason.
            # This is the one place whose cost is intentionally O(ports).
            blocked: list[dict] = []
            for port in self.egress_ports:
                occupant = self._by_egress.get((port, upstream_key))
                if occupant is not None:
                    blocked.append(
                        {
                            "port": port,
                            "cause": "active_session",
                            "client": format_addr(occupant.client_addr),
                            "remaining_ms": round(
                                max(
                                    0.0,
                                    occupant.last_forward_at + self.session_idle_seconds - now,
                                )
                                * 1000
                            ),
                        }
                    )
                    continue
                quarantine = self._quarantines.get((port, upstream_key))
                if quarantine is not None and now < quarantine.expires_at:
                    blocked.append(
                        {
                            "port": port,
                            "cause": "quarantine",
                            "remaining_ms": round(
                                max(0.0, quarantine.expires_at - now) * 1000
                            ),
                        }
                    )
            self._counters["rejected"] += 1
            parts = []
            for item in blocked:
                if item["cause"] == "active_session":
                    parts.append(
                        f"{item['port']} 被同一上游的会话占用（会话剩余 "
                        f"{self._secs(item['remaining_ms'] / 1000)}）"
                    )
                else:
                    parts.append(
                        f"{item['port']} 处于迟到回包保留期（剩余 "
                        f"{self._secs(item['remaining_ms'] / 1000)}）"
                    )
            detail = "；".join(parts)
            message = (
                f"无可用出口端口：{self._upstream_label(upstream_key)} 的会话需要独占一个"
                f"出口端口，但 {len(self.egress_ports)} 个端口均不可用（{detail}）。"
                "其他上游与本上游共用端口的情况不影响本次分配。"
            )
            self.log_event(
                "reject",
                message,
                at=now,
                client=client,
                upstream=upstream_key,
                byte_count=byte_count,
            )
            return ForwardResult(False, None, "no_port", message, blocked)

        session = Session(
            client_ip=client_ip,
            client_port=client_port,
            upstream_key=upstream_key,
            egress_port=chosen,
            created_at=now,
            last_forward_at=now,
        )
        self._sessions[session_key] = session
        self._by_egress[(chosen, upstream_key)] = session
        self._push_timer(now + self.session_idle_seconds, "session", session_key)
        self._counters["forwarded"] += 1
        self._counters["sessions_created"] += 1
        message = (
            f"新建会话，出口端口 {chosen}，转发至 {self._upstream_label(upstream_key)}"
        )
        self.log_event(
            "forward",
            message,
            at=now,
            client=client,
            upstream=upstream_key,
            egress_port=chosen,
            byte_count=byte_count,
        )
        return ForwardResult(True, chosen, "new_session", message)

    # ---------------------------------------------------------------- replies
    def on_reply(
        self,
        egress_port: int,
        upstream_key: str,
        byte_count: int,
        now: float | None = None,
    ) -> ReplyResult:
        now = self.now() if now is None else now
        self._expire_due(now)
        self._counters["replies_in"] += 1
        label = self._upstream_label(upstream_key)
        pair = (egress_port, upstream_key)

        session = self._by_egress.get(pair)
        if session is not None and now >= session.last_forward_at + self.session_idle_seconds:
            # Sweeper lag: the last-forward clock says this session is gone.
            self._retire_session(session.key, session, now)
            session = None

        quarantine = self._quarantines.get(pair)
        if quarantine is not None and now < quarantine.expires_at:
            self._counters["dropped_late"] += 1
            remaining = quarantine.expires_at - now
            message = (
                f"迟到回包被丢弃：出口端口 {egress_port} 与 {label} 的组合仍在 "
                f"2 秒保留期内（剩余 {self._secs(remaining)}），不会转给当前使用该端口的"
                "其他客户端或上游。"
            )
            self.log_event(
                "drop_late",
                message,
                at=now,
                upstream=upstream_key,
                egress_port=egress_port,
                byte_count=byte_count,
            )
            return ReplyResult(False, None, "late", message)

        if session is None:
            self._counters["dropped_unknown"] += 1
            message = (
                f"回包被丢弃：出口端口 {egress_port} 上没有 {label} 的活动会话"
            )
            self.log_event(
                "drop_unknown",
                message,
                at=now,
                upstream=upstream_key,
                egress_port=egress_port,
                byte_count=byte_count,
            )
            return ReplyResult(False, None, "no_session", message)

        self._counters["delivered"] += 1
        message = (
            f"回包送达 {format_addr(session.client_addr)}（出口端口 {egress_port}，"
            f"{label}）"
        )
        self.log_event(
            "deliver",
            message,
            at=now,
            client=format_addr(session.client_addr),
            upstream=upstream_key,
            egress_port=egress_port,
            byte_count=byte_count,
        )
        return ReplyResult(True, session.client_addr, "delivered", message)

    def on_unknown_reply(
        self,
        egress_port: int,
        source: tuple[str, int] | str,
        byte_count: int,
        now: float | None = None,
    ) -> None:
        now = self.now() if now is None else now
        self._counters["replies_in"] += 1
        self._counters["dropped_unknown"] += 1
        source_text = format_addr(source) if isinstance(source, tuple) else str(source)
        self.log_event(
            "drop_unknown",
            f"回包被丢弃：出口端口 {egress_port} 收到未知上游 {source_text} 的数据",
            at=now,
            egress_port=egress_port,
            byte_count=byte_count,
        )

    # --------------------------------------------------------------- status
    def snapshot(self, now: float | None = None) -> dict:
        now = self.now() if now is None else now
        self._expire_due(now)
        sessions = sorted(
            self._sessions.values(),
            key=lambda s: (s.egress_port, s.upstream_key, s.client_ip, s.client_port),
        )
        session_rows = [
            {
                "client": format_addr(s.client_addr),
                "client_ip": s.client_ip,
                "client_port": s.client_port,
                "upstream": s.upstream_key,
                "upstream_label": self._upstream_label(s.upstream_key),
                "egress_port": s.egress_port,
                "forward_count": s.forward_count,
                "age_ms": round(max(0.0, now - s.created_at) * 1000),
                "idle_ms": round(max(0.0, now - s.last_forward_at) * 1000),
                "remaining_ms": round(
                    max(0.0, s.last_forward_at + self.session_idle_seconds - now) * 1000
                ),
            }
            for s in sessions
        ]

        sessions_by_port: dict[int, list[Session]] = {}
        for session in sessions:
            sessions_by_port.setdefault(session.egress_port, []).append(session)
        quarantines_by_port: dict[int, list[tuple[str, Quarantine]]] = {}
        for (port, key), quarantine in self._quarantines.items():
            if now < quarantine.expires_at:
                quarantines_by_port.setdefault(port, []).append((key, quarantine))

        ports = []
        for port in self.egress_ports:
            port_sessions = [
                {
                    "client": format_addr(s.client_addr),
                    "upstream": s.upstream_key,
                    "upstream_label": self._upstream_label(s.upstream_key),
                    "remaining_ms": round(
                        max(
                            0.0,
                            s.last_forward_at + self.session_idle_seconds - now,
                        )
                        * 1000
                    ),
                    "forward_count": s.forward_count,
                }
                for s in sessions_by_port.get(port, ())
            ]
            port_quarantines = [
                {
                    "upstream": key,
                    "upstream_label": self._upstream_label(key),
                    "remaining_ms": round(max(0.0, q.expires_at - now) * 1000),
                }
                for key, q in sorted(quarantines_by_port.get(port, ()))
            ]
            ports.append(
                {
                    "port": port,
                    "sessions": port_sessions,
                    "quarantines": port_quarantines,
                }
            )

        return {
            "config": {
                "egress_ports": self.egress_ports,
                "session_idle_seconds": self.session_idle_seconds,
                "quarantine_seconds": self.quarantine_seconds,
                "upstreams": [
                    {"key": key, "label": label}
                    for key, label in self.upstreams.items()
                ],
            },
            "ports": ports,
            "sessions": session_rows,
            "counters": dict(self._counters),
            "events": list(self._events[-120:]),
            "last_seq": self._seq,
            "server_time": self._wallclock(),
        }

    def recent_events(
        self,
        since: int = 0,
        *,
        client: str | None = None,
        upstream: str | None = None,
        limit: int = 100,
        now: float | None = None,
    ) -> list[dict]:
        self._expire_due(self.now() if now is None else now)
        result = []
        for event in self._events:
            if event["seq"] <= since:
                continue
            if client is not None and event["client"] != client:
                continue
            if upstream is not None and event["upstream"] != upstream:
                continue
            result.append(event)
        return result[-limit:]

    # ---------------------------------------------------------------- reset
    def reset(self, now: float | None = None) -> dict:
        now = self.now() if now is None else now
        self._sessions.clear()
        self._by_egress.clear()
        self._quarantines.clear()
        self._timers.clear()
        self._free = {key: list(range(len(self.egress_ports))) for key in self._free}
        for key in self._counters:
            self._counters[key] = 0
        self._events.clear()
        self._seq = 0
        self.log_event("reset", "演练状态已重置：会话、保留期与计数已清空", at=now)
        return self.snapshot(now)
