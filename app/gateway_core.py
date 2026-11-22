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
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field

ClientAddr = tuple[str, int]


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
        self._quarantines: dict[tuple[int, str], Quarantine] = {}
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

    # ------------------------------------------------------------- expiry
    def expire(self, now: float | None = None) -> None:
        """Move expired sessions into quarantine and retire old quarantines."""
        now = self.now() if now is None else now
        idle = self.session_idle_seconds

        for key, session in list(self._sessions.items()):
            expires_at = session.last_forward_at + idle
            if now < expires_at:
                continue
            del self._sessions[key]
            self._counters["sessions_expired"] += 1
            q_until = expires_at + self.quarantine_seconds
            quarantine_note = ""
            if now < q_until:
                self._quarantines[(session.egress_port, session.upstream_key)] = Quarantine(
                    egress_port=session.egress_port,
                    upstream_key=session.upstream_key,
                    started_at=expires_at,
                    expires_at=q_until,
                )
                self._counters["quarantines_started"] += 1
                quarantine_note = (
                    f"；出口端口 {session.egress_port} 与"
                    f" {self._upstream_label(session.upstream_key)} 的组合保留到"
                    f" {self._secs(q_until - now)}后"
                )
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

        for key, quarantine in list(self._quarantines.items()):
            if now < quarantine.expires_at:
                continue
            del self._quarantines[key]
            self.log_event(
                "quarantine_ended",
                f"迟到回包保留期结束：出口端口 {quarantine.egress_port} / "
                f"{self._upstream_label(quarantine.upstream_key)}",
                at=now,
                upstream=quarantine.upstream_key,
                egress_port=quarantine.egress_port,
            )

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
        self.expire(now)
        self._counters["packets_in"] += 1
        client = format_addr((client_ip, client_port))
        session_key = (client_ip, client_port, upstream_key)

        session = self._sessions.get(session_key)
        if session is not None:
            session.last_forward_at = now
            session.forward_count += 1
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

        used_by_upstream: set[int] = {
            s.egress_port for s in self._sessions.values() if s.upstream_key == upstream_key
        }
        blocked: list[dict] = []
        chosen: int | None = None
        for port in self.egress_ports:
            if port in used_by_upstream:
                occupant = next(
                    s
                    for s in self._sessions.values()
                    if s.egress_port == port and s.upstream_key == upstream_key
                )
                blocked.append(
                    {
                        "port": port,
                        "cause": "active_session",
                        "client": format_addr(occupant.client_addr),
                        "remaining_ms": round(
                            max(0.0, occupant.last_forward_at + self.session_idle_seconds - now)
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
                        "remaining_ms": round(max(0.0, quarantine.expires_at - now) * 1000),
                    }
                )
                continue
            chosen = port
            break

        if chosen is None:
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
        self.expire(now)
        self._counters["replies_in"] += 1
        label = self._upstream_label(upstream_key)

        quarantine = self._quarantines.get((egress_port, upstream_key))
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

        matches = [
            s
            for s in self._sessions.values()
            if s.egress_port == egress_port and s.upstream_key == upstream_key
        ]
        if not matches:
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

        session = matches[0]
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
        self.expire(now)
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
                for s in sessions
                if s.egress_port == port
            ]
            port_quarantines = [
                {
                    "upstream": key,
                    "upstream_label": self._upstream_label(key),
                    "remaining_ms": round(max(0.0, q.expires_at - now) * 1000),
                }
                for (p, key), q in sorted(self._quarantines.items())
                if p == port and now < q.expires_at
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
        self.expire(now)
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
        self._quarantines.clear()
        for key in self._counters:
            self._counters[key] = 0
        self._events.clear()
        self._seq = 0
        self.log_event("reset", "演练状态已重置：会话、保留期与计数已清空", at=now)
        return self.snapshot(now)
