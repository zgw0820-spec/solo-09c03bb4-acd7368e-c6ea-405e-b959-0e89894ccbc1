"""Drive the running drill page's HTTP API and print what actually happened.

Usage: python3 -I scripts/verify_api.py [base_url]

`-I` keeps the interpreter from importing anything out of the working
directory; the script only uses the standard library.
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"


def call(path: str, payload: dict | None = None) -> dict:
    data = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(
        BASE + path,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST" if payload is not None else "GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        return {"http_error": exc.code, "body": exc.read().decode()}


def send(client: str, upstream: str, delay_ms: int, body: str) -> dict:
    return call(
        "/api/send",
        {"client_id": client, "upstream": upstream, "delay_ms": delay_ms, "body": body},
    )["sent"]


def messages(client: str) -> list[dict]:
    return call(f"/api/client/{client}")["messages"]


def latest(client: str) -> dict:
    return messages(client)[0]


def wait_status(client: str, request_id: str, statuses: set[str], timeout: float = 4.0) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        for message in messages(client):
            if message["request_id"] == request_id:
                last = message
                if message["status"] in statuses:
                    return message
        time.sleep(0.05)
    return last


def state() -> dict:
    return call("/api/state")


def show(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def wait_until_free(timeout: float = 6.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        snap = state()
        busy = any(p["sessions"] or p["quarantines"] for p in snap["ports"])
        if not busy:
            return
        time.sleep(0.15)
    raise SystemExit("ports did not free up in time")


def main() -> int:
    call("/api/reset", {"confirm": True})
    failures: list[str] = []

    def check(condition: bool, label: str, detail: str = "") -> None:
        mark = "PASS" if condition else "FAIL"
        print(f"[{mark}] {label}" + (f" — {detail}" if detail else ""))
        if not condition:
            failures.append(label)

    show("场景 1：三个客户端 → 两个上游（0 ms 延迟）")
    wait_until_free()
    call("/api/send-batch", {"delay_ms": 0, "body": "场景1"})
    time.sleep(0.5)
    snap = state()
    for client in ("client-1", "client-2", "client-3"):
        message = latest(client)
        if message["status"] == "received":
            received = message["received"]
            print(
                f"  {client}: 已收包 回显={received['echo_service']} "
                f"出口端口={received['gateway_egress_port']} "
                f"上游看到源={received['gateway_egress']} "
                f"RTT={message['rtt_ms']} ms 内容={received['body']!r}"
            )
        else:
            print(f"  {client}: {message['status']} {message.get('error') or message.get('note')}")
    snap = state()
    local_addrs = {c["id"]: c["local_addr"] for c in snap["clients"]}
    sessions = {s["client"]: s for s in snap["sessions"]}
    c1 = sessions[local_addrs["client-1"]]
    c2 = sessions[local_addrs["client-2"]]
    c3 = sessions[local_addrs["client-3"]]
    check(
        latest("client-1")["status"] == "received"
        and latest("client-2")["status"] == "received"
        and latest("client-3")["status"] == "received",
        "三个客户端均收到真实回包",
    )
    check(
        c1["egress_port"] == c2["egress_port"] == 17000,
        "不同上游共用出口端口 17000",
        f"client-1={c1['egress_port']} client-2={c2['egress_port']}",
    )
    check(
        c3["egress_port"] == 17001,
        "同一上游（echo-a）的第二个会话占用另一端口 17001",
        f"client-3={c3['egress_port']}",
    )

    show("场景 2：两个客户端并发到 echo-a，第三个被拒绝（无可用端口）")
    call("/api/reset", {"confirm": True})
    wait_until_free()
    first = send("client-1", "echo-a", 0, "占用1")
    second = send("client-2", "echo-a", 0, "占用2")
    third = send("client-3", "echo-a", 0, "占用3")
    print(f"  client-1: {first['status']} {first.get('request_id')}")
    print(f"  client-2: {second['status']} {second.get('request_id')}")
    print(f"  client-3: {third['status']} → {third.get('error')}")
    wait_status("client-3", third["request_id"], {"received", "rejected", "timeout"})
    rejected = wait_status("client-3", third["request_id"], {"rejected"})
    check(rejected["status"] == "rejected", "第三个 echo-a 会话未转发并收到拒绝原因")
    check(
        "无可用出口端口" in (rejected.get("error") or ""),
        "网页可展示的拒绝原因包含无可用端口说明",
        (rejected.get("error") or "")[:120],
    )
    check(
        latest("client-3")["received"] is None,
        "被拒绝的请求没有伪造回包",
    )

    show("场景 3：800 ms 延迟仍在会话寿命内，回包送达")
    call("/api/reset", {"confirm": True})
    wait_until_free()
    msg = send("client-1", "echo-a", 800, "800ms")
    got = wait_status("client-1", msg["request_id"], {"received", "timeout"}, timeout=4)
    print(f"  status={got['status']} rtt={got.get('rtt_ms')} applied_delay={got.get('received', {}).get('applied_delay_ms') if got.get('received') else None}")
    check(got["status"] == "received", "800 ms 延迟回包被送达")
    check(got["rtt_ms"] is not None and got["rtt_ms"] >= 780, "RTT 反映真实延迟", f"{got['rtt_ms']} ms")

    show("场景 4：1500 ms 迟到回包被丢弃，端口可给另一上游复用")
    call("/api/reset", {"confirm": True})
    wait_until_free()
    late = send("client-1", "echo-a", 1500, "迟到包")
    print("  已发送 1500 ms 延迟请求，等会话失效后改用另一上游复用端口……")
    time.sleep(1.2)  # session (1 s) expired; echo-a quarantine runs until ~3.0 s
    snap = state()
    check(
        any(q["upstream"] == "echo-a" for p in snap["ports"] for q in p["quarantines"]),
        "echo-a 会话失效后原端口进入 2 秒保留期",
        json.dumps(snap["ports"], ensure_ascii=False),
    )
    fresh = send("client-2", "echo-b", 300, "复用端口")
    got_fresh = wait_status("client-2", fresh["request_id"], {"received", "timeout"})
    print(
        f"  client-2 → echo-b: status={got_fresh['status']} "
        f"出口端口={got_fresh.get('received', {}).get('gateway_egress_port')}"
    )
    check(got_fresh["status"] == "received", "保留期内端口仍可分配给另一上游")
    check(
        got_fresh.get("received", {}).get("gateway_egress_port") == 17000,
        "echo-b 复用了 17000",
    )
    check(
        got_fresh.get("received", {}).get("body") == "复用端口",
        "复用端口后收到的是新上游的回包，而非迟到旧包",
    )

    got_late = wait_status("client-1", late["request_id"], {"received", "timeout"}, timeout=4)
    print(f"  client-1: status={got_late['status']} note={got_late.get('note')}")
    check(got_late["status"] == "timeout", "1500 ms 迟到回包没有送达原客户端")
    check(
        not any(
            (m.get("received") or {}).get("body") == "迟到包"
            for m in messages("client-2")
        ),
        "迟到旧包没有被投递给复用端口的新客户端",
    )
    events = call("/api/events?limit=200")["events"]
    late_events = [e for e in events if e["kind"] == "drop_late"]
    check(bool(late_events), "网关记录 drop_late 事件", late_events[0]["message"] if late_events else "")
    snap = state()
    print(
        "  计数："
        + " ".join(f"{k}={v}" for k, v in snap["counters"].items())
    )

    show("场景 5：两个端口都在 echo-a 保留期时，echo-a 新会话被拒绝")
    call("/api/reset", {"confirm": True})
    wait_until_free()
    for client in ("client-1", "client-2"):
        sent = send(client, "echo-a", 0, "占位")
        wait_status(client, sent["request_id"], {"received", "timeout"})
    # Sessions expire 1 s after the forward; poll until the pairs are quarantined.
    deadline = time.time() + 3.0
    while time.time() < deadline:
        snap = state()
        q = [x for p in snap["ports"] for x in p["quarantines"] if x["upstream"] == "echo-a"]
        if not snap["sessions"] and len(q) == 2:
            break
        time.sleep(0.05)
    check(len(q) == 2, "两个端口都进入 echo-a 的保留期", json.dumps(q, ensure_ascii=False))
    blocked = send("client-3", "echo-a", 0, "保留期内")
    result = wait_status("client-3", blocked["request_id"], {"received", "rejected", "timeout"})
    print(f"  status={result['status']} error={result.get('error')}")
    check(result["status"] == "rejected", "保留期内同上游新会话被拒绝")
    check("保留期" in (result.get("error") or ""), "拒绝原因说明保留期")
    check(result.get("received") is None, "被拒绝时客户端没有收到伪造回包")

    show("最终状态")
    snap = state()
    print(json.dumps({"ports": snap["ports"], "counters": snap["counters"]}, ensure_ascii=False, indent=2))

    show("结果")
    if failures:
        print(f"{len(failures)} 项失败：")
        for item in failures:
            print("  -", item)
        return 1
    print("全部检查通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
