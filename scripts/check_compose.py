"""Validate compose.yaml structure without Docker, and check that the
network wiring the compose file describes is what the app expects."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def fail(message: str) -> None:
    print(f"FAIL {message}")
    sys.exit(1)


def main() -> None:
    try:
        import yaml  # type: ignore
    except ImportError:
        fail("PyYAML is required for this script: pip install pyyaml")

    compose = yaml.safe_load((ROOT / "compose.yaml").read_text())
    services = compose.get("services", {})
    for name in ("gateway", "echo-a", "echo-b"):
        if name not in services:
            fail(f"missing service {name}")

    gateway = services["gateway"]
    env = gateway["environment"]
    if env.get("ECHO_A_HOST") != "echo-a" or env.get("ECHO_B_HOST") != "echo-b":
        fail("gateway must reach the echo services by compose DNS name")
    if env.get("EGRESS_PORTS") != "17000,17001":
        fail("expected exactly two egress ports")

    ports = [str(p) for p in gateway.get("ports", [])]
    if not any(p.endswith(":8000") for p in ports):
        fail("web port 8000 must be published")
    if any("udp" in p for p in ports):
        fail("UDP ports must stay inside the compose network (only the web port is published)")

    for name, expected in (("echo-a", "16001"), ("echo-b", "16002")):
        svc = services[name]
        if svc["environment"].get("ECHO_PORT") != expected:
            fail(f"{name} must listen on {expected}")

    sources = "\n".join(
        p.read_text()
        for p in (ROOT / "app").glob("*.py")
    )
    for pattern in (
        r"ingress_port=ingress",
        r"16001, 15001",
        r"16002, 15002",
        r"17000,17001",
    ):
        if not re.search(pattern, sources):
            fail(f"app wiring does not match compose expectations: {pattern}")

    dockerfile = (ROOT / "Dockerfile").read_text()
    if "EXPOSE 8000 15001 15002 17000 17001" not in dockerfile:
        fail("Dockerfile must expose the web, ingress and egress ports")
    if "app.main:app" not in dockerfile:
        fail("Dockerfile must launch the FastAPI app")

    echo_dockerfile = (ROOT / "Dockerfile.echo").read_text()
    if "echo_service.py" not in echo_dockerfile:
        fail("echo image must ship the echo service")

    print("compose.yaml + Dockerfiles structurally OK")
    print(f"services: {', '.join(services)}")
    print(f"published: {ports}")
    print(f"egress: {env['EGRESS_PORTS']}  ingress: 15001 (echo-a), 15002 (echo-b)")


if __name__ == "__main__":
    main()
