#!/usr/bin/env python3
"""JSON-RPC over PTY stdin/stdout for culab remote control.

Run inside a Jupyter terminal WebSocket session. One JSON object per stdin
line; one wrapped response per stdout line: __CULAB__<json>__END__.

Operations:
  ping
  exec      {cmd, cwd?, timeout?, env?}                 -> {rc, stdout_b64, stderr_b64}
  spawn     {cmd, cwd?, env?}                           -> {job_id, pid, log}
  status    {job_id}                                    -> {running, rc?, pid}
  log       {job_id, tail?, grep?}                      -> {data_b64, bytes}
  kill      {job_id, signal?}                           -> {ok} | {error}
  list_jobs {}                                          -> {jobs:[...]}
  reap      {job_id}                                    -> {ok}
"""
from __future__ import annotations

import base64
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

JOBS_DIR = Path.home() / ".cache" / "culab-jobs"
JOBS_DIR.mkdir(parents=True, exist_ok=True)
JOBS: dict[str, subprocess.Popen] = {}

RESP_PREFIX = "__CULAB__"
RESP_SUFFIX = "__END__"


def _resp(payload: dict, req_id: str | None = None) -> None:
    if req_id is not None:
        payload = {"id": req_id, **payload}
    sys.stdout.write(RESP_PREFIX + json.dumps(payload, separators=(",", ":")) + RESP_SUFFIX + "\n")
    sys.stdout.flush()


def _env(req: dict) -> dict:
    extra = req.get("env") or {}
    if not isinstance(extra, dict):
        return os.environ.copy()
    merged = os.environ.copy()
    for key, value in extra.items():
        merged[str(key)] = str(value)
    return merged


def _exec(req: dict) -> dict:
    proc = subprocess.run(
        req["cmd"],
        shell=True,
        cwd=req.get("cwd") or None,
        env=_env(req),
        capture_output=True,
        timeout=req.get("timeout", 120),
    )
    return {
        "rc": proc.returncode,
        "stdout_b64": base64.b64encode(proc.stdout).decode(),
        "stderr_b64": base64.b64encode(proc.stderr).decode(),
    }


def _spawn(req: dict) -> dict:
    jid = uuid.uuid4().hex[:8]
    log_path = JOBS_DIR / f"{jid}.log"
    cmd_path = JOBS_DIR / f"{jid}.cmd"
    cmd_path.write_text(req["cmd"])
    log_f = open(log_path, "wb")
    proc = subprocess.Popen(
        req["cmd"],
        shell=True,
        cwd=req.get("cwd") or None,
        env=_env(req),
        stdout=log_f,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    JOBS[jid] = proc
    return {"job_id": jid, "pid": proc.pid, "log": str(log_path)}


def _status(req: dict) -> dict:
    proc = JOBS.get(req["job_id"])
    if proc is None:
        return {"error": "unknown_job"}
    rc = proc.poll()
    if rc is None:
        return {"running": True, "pid": proc.pid}
    return {"running": False, "rc": rc, "pid": proc.pid}


def _log(req: dict) -> dict:
    log_path = JOBS_DIR / f"{req['job_id']}.log"
    if not log_path.exists():
        return {"error": "no_log"}
    data = log_path.read_bytes()
    pat = req.get("grep")
    if pat:
        needle = pat.encode()
        data = b"\n".join(ln for ln in data.splitlines() if needle in ln)
    tail = req.get("tail")
    if tail is not None:
        data = b"\n".join(data.splitlines()[-int(tail):])
    return {"data_b64": base64.b64encode(data).decode(), "bytes": len(data)}


def _kill(req: dict) -> dict:
    proc = JOBS.get(req["job_id"])
    if proc is None:
        return {"error": "unknown_job"}
    sig_name = req.get("signal", "SIGTERM")
    sig = getattr(signal, sig_name, signal.SIGTERM)
    try:
        os.killpg(proc.pid, sig)
    except ProcessLookupError:
        return {"error": "no_process"}
    return {"ok": True, "signal": sig_name}


def _list_jobs(_req: dict) -> dict:
    rows = []
    for jid, proc in list(JOBS.items()):
        rc = proc.poll()
        rows.append({"job_id": jid, "pid": proc.pid, "running": rc is None, "rc": rc})
    return {"jobs": rows}


def _reap(req: dict) -> dict:
    jid = req["job_id"]
    JOBS.pop(jid, None)
    (JOBS_DIR / f"{jid}.log").unlink(missing_ok=True)
    (JOBS_DIR / f"{jid}.cmd").unlink(missing_ok=True)
    return {"ok": True}


HANDLERS = {
    "ping": lambda r: {"pong": True, "ts": time.time(), "pid": os.getpid()},
    "exec": _exec,
    "spawn": _spawn,
    "status": _status,
    "log": _log,
    "kill": _kill,
    "list_jobs": _list_jobs,
    "reap": _reap,
}


def main() -> int:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except Exception as exc:
            _resp({"error": "bad_json", "detail": str(exc)})
            continue
        req_id = req.get("id")
        op = req.get("op")
        handler = HANDLERS.get(op)
        if handler is None:
            _resp({"error": "unknown_op", "op": op}, req_id)
            continue
        try:
            _resp(handler(req), req_id)
        except subprocess.TimeoutExpired:
            _resp({"error": "timeout"}, req_id)
        except Exception as exc:
            _resp({"error": "exception", "detail": str(exc)}, req_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
