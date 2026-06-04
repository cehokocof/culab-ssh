#!/usr/bin/env python3
"""Client for the culab JSON-RPC daemon running over a Jupyter terminal WS.

Bootstraps culab_rpc_server.py inline (base64-embedded) into a long-lived
JupyterHub terminal, then talks JSON to it. The terminal id is cached in
~/.cache/culab/rpc.json and reused across invocations.

CLI:
  culab_rpc.py ping
  culab_rpc.py exec  "<shell command>"  [--cwd PATH] [--timeout SEC]
  culab_rpc.py spawn "<shell command>"  [--cwd PATH]
  culab_rpc.py status JOB_ID
  culab_rpc.py log    JOB_ID  [--tail N]  [--grep PATTERN]
  culab_rpc.py kill   JOB_ID  [--signal SIGTERM]
  culab_rpc.py jobs
  culab_rpc.py reap   JOB_ID
  culab_rpc.py reset                    (force new terminal + bootstrap)

Stdout for the model is intentionally minimal:
  - exec: decoded stdout to stdout, decoded stderr to stderr, exit code = remote rc
  - spawn: prints job_id
  - log:   decoded log bytes to stdout
  - status/jobs/ping: compact JSON
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import socket
import sys
import time
import uuid
from pathlib import Path

from jupyter_terminal_exec import (
    create_terminal,
    discover_base_path,
    list_terminals,
    load_cookie_items,
    ws_connect,
    ws_recv_text,
    ws_send_text,
)

SERVER_SCRIPT = Path(__file__).resolve().parent / "culab_rpc_server.py"
STATE_DIR = Path.home() / ".cache" / "culab"
STATE_FILE = STATE_DIR / "rpc.json"

RESP_PREFIX = "__CULAB__"
RESP_SUFFIX = "__END__"


def _state_load() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


def _state_save(data: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(data))


def _send_stdin(sock, text: str) -> None:
    ws_send_text(sock, json.dumps(["stdin", text]))


def _recv_envelope(sock, timeout: float, want_id: str | None = None) -> dict:
    """Read PTY stream until an envelope arrives.

    If want_id is given, skip envelopes with a different id (lingering from
    previous calls) until the matching one is found.
    """
    deadline = time.time() + timeout
    buf = ""
    while time.time() < deadline:
        try:
            msg = ws_recv_text(sock, timeout=min(1.0, max(0.1, deadline - time.time())))
        except socket.timeout:
            continue
        if msg is None:
            raise RuntimeError("websocket closed mid-response")
        if not msg:
            continue
        try:
            _, data = json.loads(msg)
        except Exception:
            data = msg
        buf += str(data)
        while RESP_PREFIX in buf and RESP_SUFFIX in buf.split(RESP_PREFIX, 1)[1]:
            head, tail = buf.split(RESP_PREFIX, 1)
            payload, rest = tail.split(RESP_SUFFIX, 1)
            buf = rest
            try:
                parsed = json.loads(payload)
            except Exception:
                continue
            if want_id is not None and parsed.get("id") != want_id:
                continue
            return parsed
    raise TimeoutError(f"no daemon response in {timeout}s; tail={buf[-200:]!r}")


def _drain(sock, seconds: float) -> None:
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            ws_recv_text(sock, timeout=0.1)
        except socket.timeout:
            continue
        except Exception:
            return


def _bootstrap_command() -> str:
    script_b64 = base64.b64encode(SERVER_SCRIPT.read_bytes()).decode()
    return (
        f"exec python3 -c "
        f"\"import base64; exec(compile(base64.b64decode('{script_b64}'),"
        f"'culab_rpc_server','exec'))\""
    )


def _connect(hub_url: str, items: list[dict]):
    base_path = discover_base_path(items)
    state = _state_load()
    terminal_name = state.get("terminal")
    if terminal_name:
        try:
            sock = ws_connect(hub_url, base_path, terminal_name, items)
            return sock, base_path, terminal_name, True
        except Exception:
            terminal_name = None
    terminal_name = create_terminal(hub_url, base_path, items)
    sock = ws_connect(hub_url, base_path, terminal_name, items)
    return sock, base_path, terminal_name, False


def _ensure_daemon(sock, base_path: str, terminal_name: str) -> None:
    """Ping; on failure run inline-exec bootstrap and ping again."""
    ping_id = uuid.uuid4().hex
    _send_stdin(sock, json.dumps({"op": "ping", "id": ping_id}) + "\n")
    try:
        _recv_envelope(sock, timeout=2.5, want_id=ping_id)
        _state_save({"base_path": base_path, "terminal": terminal_name})
        return
    except TimeoutError:
        pass
    _send_stdin(sock, "\x03\rstty sane\rstty -echo\r")
    _drain(sock, 0.6)
    _send_stdin(sock, _bootstrap_command() + "\r")
    boot_id = uuid.uuid4().hex
    _send_stdin(sock, json.dumps({"op": "ping", "id": boot_id}) + "\n")
    _recv_envelope(sock, timeout=8.0, want_id=boot_id)
    _state_save({"base_path": base_path, "terminal": terminal_name})


def _call_once(req: dict, timeout: float) -> dict:
    hub_url = os.environ.get("HUB_URL", "https://jupyter.culab.ru")
    items = load_cookie_items(Path(os.environ.get("COOKIES_JSON", "cookies.json")))
    sock, base_path, terminal_name, _ = _connect(hub_url, items)
    try:
        _ensure_daemon(sock, base_path, terminal_name)
        req_id = uuid.uuid4().hex
        req_with_id = {**req, "id": req_id}
        _send_stdin(sock, json.dumps(req_with_id) + "\n")
        return _recv_envelope(sock, timeout=timeout, want_id=req_id)
    finally:
        try:
            sock.close()
        except Exception:
            pass


def _call(req: dict, *, timeout: float = 180.0) -> dict:
    try:
        return _call_once(req, timeout)
    except (RuntimeError, TimeoutError) as exc:
        msg = str(exc)
        recoverable = "websocket closed" in msg or "no daemon response" in msg
        if not recoverable:
            raise
        if STATE_FILE.exists():
            STATE_FILE.unlink()
        time.sleep(0.4)
        return _call_once(req, timeout)


def _without_id(d: dict) -> dict:
    return {k: v for k, v in d.items() if k != "id"}


def _reset() -> dict:
    """Drop cached terminal so the next call creates a fresh one."""
    if STATE_FILE.exists():
        STATE_FILE.unlink()
    return {"ok": True}


def cli() -> int:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="op", required=True)

    sub.add_parser("ping")
    sub.add_parser("reset")
    sub.add_parser("jobs")

    p_exec = sub.add_parser("exec")
    p_exec.add_argument("cmd")
    p_exec.add_argument("--cwd")
    p_exec.add_argument("--timeout", type=int, default=120)

    p_spawn = sub.add_parser("spawn")
    p_spawn.add_argument("cmd")
    p_spawn.add_argument("--cwd")

    for name in ("status", "reap"):
        sp = sub.add_parser(name)
        sp.add_argument("job_id")

    p_log = sub.add_parser("log")
    p_log.add_argument("job_id")
    p_log.add_argument("--tail", type=int)
    p_log.add_argument("--grep")

    p_kill = sub.add_parser("kill")
    p_kill.add_argument("job_id")
    p_kill.add_argument("--signal", default="SIGTERM")

    args = p.parse_args()
    op = args.op

    if op == "reset":
        print(json.dumps(_reset()))
        return 0

    if op == "ping":
        print(json.dumps(_without_id(_call({"op": "ping"}, timeout=10.0))))
        return 0

    if op == "exec":
        req = {"op": "exec", "cmd": args.cmd, "timeout": args.timeout}
        if args.cwd:
            req["cwd"] = args.cwd
        resp = _call(req, timeout=args.timeout + 30)
        if "error" in resp:
            sys.stderr.write(json.dumps(resp) + "\n")
            return 2
        sys.stdout.write(base64.b64decode(resp["stdout_b64"]).decode("utf-8", "replace"))
        sys.stderr.write(base64.b64decode(resp["stderr_b64"]).decode("utf-8", "replace"))
        return int(resp["rc"])

    if op == "spawn":
        req = {"op": "spawn", "cmd": args.cmd}
        if args.cwd:
            req["cwd"] = args.cwd
        resp = _call(req)
        if "error" in resp:
            sys.stderr.write(json.dumps(resp) + "\n")
            return 2
        print(resp["job_id"])
        return 0

    if op == "status":
        print(json.dumps(_without_id(_call({"op": "status", "job_id": args.job_id}))))
        return 0

    if op == "log":
        req = {"op": "log", "job_id": args.job_id}
        if args.tail is not None:
            req["tail"] = args.tail
        if args.grep:
            req["grep"] = args.grep
        resp = _call(req)
        if "error" in resp:
            sys.stderr.write(json.dumps(resp) + "\n")
            return 2
        sys.stdout.write(base64.b64decode(resp["data_b64"]).decode("utf-8", "replace"))
        return 0

    if op == "kill":
        print(json.dumps(_without_id(_call({"op": "kill", "job_id": args.job_id, "signal": args.signal}))))
        return 0

    if op == "jobs":
        print(json.dumps(_without_id(_call({"op": "list_jobs"}))))
        return 0

    if op == "reap":
        print(json.dumps(_without_id(_call({"op": "reap", "job_id": args.job_id}))))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(cli())
