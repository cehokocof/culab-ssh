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
  culab_rpc.py push   LOCAL_PROJECT  REMOTE_PROJECT  [--exclude PATH]...
  culab_rpc.py reset                    (force new terminal + bootstrap)

Captcha guard: WS connections are throttled (default 0.6s between handshakes,
override via CULAB_MIN_INTERVAL). Multi-step ops like `push` reuse a single
WS for all RPC calls instead of one-WS-per-call.

Stdout for the model is intentionally minimal:
  - exec: decoded stdout to stdout, decoded stderr to stderr, exit code = remote rc
  - spawn: prints job_id
  - log:   decoded log bytes to stdout
  - status/jobs/ping: compact JSON
  - push:  one summary line "pushed N files (S bytes) -> REMOTE"
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import io as _io
import json
import os
import shlex
import socket
import sys
import tarfile
import tempfile
import time
import uuid
from pathlib import Path

from jupyter_terminal_exec import (
    create_terminal,
    delete_terminal,
    discover_base_path,
    list_terminals,
    list_terminals_detailed,
    load_cookie_items,
    ws_connect,
    ws_recv_text,
    ws_send_text,
)

SERVER_SCRIPT = Path(__file__).resolve().parent / "culab_rpc_server.py"
STATE_DIR = Path.home() / ".cache" / "culab"
STATE_FILE = STATE_DIR / "rpc.json"

# Filter for `push`: what goes into the code-only tarball.
_EXCLUDE_PARTS = {
    ".git", ".venv", ".venv-vllm", "venv", "__pycache__",
    ".pytest_cache", ".mypy_cache", ".ruff_cache", ".DS_Store",
    "node_modules", "outputs", "artifacts", "checkpoints", "models",
}
_EXCLUDE_SUFFIXES = {
    ".csv", ".parquet", ".pkl", ".pickle", ".joblib", ".db",
    ".zip", ".tgz", ".tar", ".gz",
    ".pt", ".pth", ".ckpt", ".safetensors", ".onnx",
}
_INCLUDE_SUFFIXES = {
    ".py", ".ipynb", ".toml", ".lock", ".md", ".txt",
    ".yaml", ".yml", ".json", ".ini", ".cfg", ".sh",
}
_INCLUDE_NAMES = {".gitignore", ".dockerignore", ".python-version", "Dockerfile", "Makefile"}
_INCLUDE_STEMS = {"Dockerfile", "Makefile"}


def _should_include(path: Path, root: Path, exclude_paths: set[str] | None) -> bool:
    rel = path.relative_to(root)
    if exclude_paths and rel.as_posix() in exclude_paths:
        return False
    if any(part in _EXCLUDE_PARTS for part in rel.parts):
        return False
    if rel.parts and rel.parts[0] == "data" and path.suffix in {".json", ".npz"}:
        return False
    if path.is_dir():
        return False
    if path.name in _INCLUDE_NAMES:
        return True
    if any(
        path.name.startswith(f"{stem}.") or path.name.endswith(f".{stem}")
        for stem in _INCLUDE_STEMS
    ):
        return True
    if path.suffix in _EXCLUDE_SUFFIXES:
        return False
    return path.suffix in _INCLUDE_SUFFIXES


def _make_tarball(root: Path, exclude_paths: set[str] | None = None) -> Path:
    fd, name = tempfile.mkstemp(prefix=f"{root.name}-code-", suffix=".tgz")
    os.close(fd)
    archive = Path(name)
    count = 0
    manifest: list[tuple[str, str]] = []
    with tarfile.open(archive, "w:gz") as tar:
        for path in sorted(root.rglob("*")):
            if _should_include(path, root, exclude_paths):
                rel = path.relative_to(root).as_posix()
                digest = hashlib.sha256(path.read_bytes()).hexdigest()
                manifest.append((digest, rel))
                tar.add(path, arcname=rel)
                count += 1
        manifest_text = "".join(f"{digest}  {rel}\n" for digest, rel in manifest)
        manifest_bytes = manifest_text.encode("utf-8")
        info = tarfile.TarInfo(".codex_push_manifest.sha256")
        info.size = len(manifest_bytes)
        info.mode = 0o644
        tar.addfile(info, _io.BytesIO(manifest_bytes))
    if count == 0:
        archive.unlink(missing_ok=True)
        raise SystemExit("no code files matched include rules")
    return archive

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


_PTY_PIECE = int(os.environ.get("CULAB_PTY_PIECE", "65536"))
_PTY_GAP = float(os.environ.get("CULAB_PTY_GAP", "0"))


def _send_stdin(sock, text: str) -> None:
    """Send stdin via WS in pieces small enough to fit terminado's PTY buffer.

    terminado on the server does os.write(master_fd, payload) which can short-
    write on a full PTY buffer; the dropped tail closes the WS. Splitting
    keeps every single write below the buffer high-watermark.
    """
    if len(text) <= _PTY_PIECE:
        ws_send_text(sock, json.dumps(["stdin", text]))
        return
    for i in range(0, len(text), _PTY_PIECE):
        ws_send_text(sock, json.dumps(["stdin", text[i : i + _PTY_PIECE]]))
        if _PTY_GAP > 0:
            time.sleep(_PTY_GAP)


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


def _safe_delete(hub_url: str, base_path: str, items: list[dict], name: str | None) -> None:
    if not name:
        return
    try:
        delete_terminal(hub_url, base_path, items, name)
    except Exception:
        pass


def _connect(hub_url: str, items: list[dict]):
    base_path = discover_base_path(items)
    state = _state_load()
    terminal_name = state.get("terminal")
    if terminal_name:
        try:
            sock = ws_connect(hub_url, base_path, terminal_name, items)
            return sock, base_path, terminal_name, True
        except Exception:
            # Saved terminal is dead. Best-effort delete it before creating
            # a replacement, so we do not leave a zombie in Jupyter UI.
            _safe_delete(hub_url, base_path, items, terminal_name)
            state.pop("terminal", None)
            _state_save(state)
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
    # -icanon is critical: canonical mode caps a single read at MAX_CANON
    # (4096 on Linux), so any RPC request larger than ~4 KB would have its
    # tail silently dropped and Python would wait forever for the missing
    # newline. -echo just keeps PTY from echoing our JSON back as noise.
    _send_stdin(sock, "\x03\rstty sane\rstty -echo -icanon\r")
    _drain(sock, 0.6)
    _send_stdin(sock, _bootstrap_command() + "\r")
    boot_id = uuid.uuid4().hex
    _send_stdin(sock, json.dumps({"op": "ping", "id": boot_id}) + "\n")
    _recv_envelope(sock, timeout=8.0, want_id=boot_id)
    _drain(sock, 0.2)  # eat any trailing \r\n from PTY echo
    _state_save({"base_path": base_path, "terminal": terminal_name})


_RECOVERABLE = (EOFError, TimeoutError, ConnectionError, socket.error)


def _is_recoverable(exc: BaseException) -> bool:
    if isinstance(exc, _RECOVERABLE):
        return True
    if isinstance(exc, RuntimeError):
        msg = str(exc).lower()
        return "websocket" in msg or "socket" in msg or "daemon" in msg
    return False


def _throttle() -> None:
    """Sleep so consecutive WS handshakes stay below captcha rate."""
    min_interval = float(os.environ.get("CULAB_MIN_INTERVAL", "0.6"))
    state = _state_load()
    last = float(state.get("last_connect", 0.0))
    delta = time.time() - last
    if delta < min_interval:
        time.sleep(min_interval - delta)


def _mark_connect() -> None:
    state = _state_load()
    state["last_connect"] = time.time()
    _state_save(state)


class RpcSession:
    """Holds one WS for many RPC calls. Use as a context manager.

    For one-shot calls just use `_call(req)`. For multi-step ops (upload many
    chunks, then exec extract) wrap them all in a single `with RpcSession()`
    block so they share one WS handshake.
    """

    def __init__(self) -> None:
        self.sock = None
        self.base_path: str | None = None
        self.terminal_name: str | None = None
        self._hub_url = os.environ.get("HUB_URL", "https://jupyter.culab.ru")
        self._items = load_cookie_items(
            Path(os.environ.get("COOKIES_JSON", "cookies.json"))
        )

    def __enter__(self) -> "RpcSession":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def _open(self) -> None:
        if self.sock is not None:
            return
        _throttle()
        self.sock, self.base_path, self.terminal_name, _reused = _connect(
            self._hub_url, self._items
        )
        _mark_connect()
        _ensure_daemon(self.sock, self.base_path, self.terminal_name)

    def call(self, req: dict, *, timeout: float = 180.0) -> dict:
        max_attempts = int(os.environ.get("CULAB_MAX_ATTEMPTS", "3"))
        attempts = 0
        while True:
            attempts += 1
            try:
                self._open()
                req_id = uuid.uuid4().hex
                _send_stdin(self.sock, json.dumps({**req, "id": req_id}) + "\n")
                return _recv_envelope(self.sock, timeout=timeout, want_id=req_id)
            except BaseException as exc:
                if attempts >= max_attempts or not _is_recoverable(exc):
                    raise
                # Recovery: close our WS, delete the (likely-broken) terminal
                # we created, and let _open() create a fresh one. Other
                # terminals (the user's own ones) are never touched.
                self.close()
                state = _state_load()
                stale = state.get("terminal")
                if stale is not None and self.base_path is not None:
                    _safe_delete(self._hub_url, self.base_path, self._items, stale)
                    state.pop("terminal", None)
                    _state_save(state)
                self.terminal_name = None
                self.base_path = None
                time.sleep(0.4)


def _call(req: dict, *, timeout: float = 180.0) -> dict:
    with RpcSession() as s:
        return s.call(req, timeout=timeout)


def _without_id(d: dict) -> dict:
    return {k: v for k, v in d.items() if k != "id"}


def _hub_and_items() -> tuple[str, list[dict], str]:
    hub_url = os.environ.get("HUB_URL", "https://jupyter.culab.ru")
    items = load_cookie_items(Path(os.environ.get("COOKIES_JSON", "cookies.json")))
    base_path = discover_base_path(items)
    return hub_url, items, base_path


def _reset() -> dict:
    """Drop cached terminal AND best-effort delete it on the server.

    Only deletes the terminal we created (the one stored in our state). The
    user's own Jupyter terminals are never touched.
    """
    state = _state_load()
    saved = state.get("terminal")
    if saved is not None:
        try:
            hub_url, items, base_path = _hub_and_items()
            _safe_delete(hub_url, base_path, items, saved)
        except Exception:
            pass
    if STATE_FILE.exists():
        STATE_FILE.unlink()
    return {"ok": True, "removed_terminal": saved}


def _list_terminals() -> dict:
    hub_url, items, base_path = _hub_and_items()
    rows = list_terminals_detailed(hub_url, base_path, items)
    ours = _state_load().get("terminal")
    return {
        "ours": ours,
        "terminals": [
            {"name": r["name"], "ours": r["name"] == ours, "last_activity": r["last_activity"]}
            for r in rows
        ],
    }


def _cleanup_terminals(scope: str, name: str | None = None) -> dict:
    """Safe deletion of terminals.

    scope='ours' deletes the saved-as-ours terminal only.
    scope='name' deletes a single named terminal (caller's choice).
    scope='all' is rejected — too risky, would kill the user's own sessions.
    """
    hub_url, items, base_path = _hub_and_items()
    state = _state_load()
    if scope == "all":
        return {"error": "refused", "reason": "all would kill user terminals too; use --name or --ours"}
    if scope == "ours":
        target = state.get("terminal")
        if not target:
            return {"ok": True, "removed": [], "note": "no ours saved"}
    elif scope == "name":
        if not name:
            return {"error": "name_required"}
        target = name
    else:
        return {"error": "bad_scope"}
    _safe_delete(hub_url, base_path, items, target)
    if state.get("terminal") == target:
        state.pop("terminal", None)
        _state_save(state)
    return {"ok": True, "removed": [target]}


def _push(local_project: Path, remote_project: str, excludes: list[str]) -> dict:
    """Build code tarball locally, upload via one RPC session, extract + verify."""
    root = local_project.expanduser().resolve()
    if not root.is_dir():
        raise SystemExit(f"not a directory: {root}")
    exclude_paths = {Path(item).as_posix().lstrip("/") for item in excludes}
    archive = _make_tarball(root, exclude_paths)
    try:
        data = archive.read_bytes()
        sha = hashlib.sha256(data).hexdigest()
        # After `stty -icanon` MAX_CANON limit is gone; chunks can be large.
        chunk_size = int(os.environ.get("CULAB_CHUNK", str(256 * 1024)))
        chunk_timeout = float(os.environ.get("CULAB_CHUNK_TIMEOUT", "10"))
        chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]
        remote_archive = f"/tmp/{root.name}-code.tgz"
        show_progress = sys.stderr.isatty() or os.environ.get("CULAB_PROGRESS")

        with RpcSession() as s:
            for idx, chunk in enumerate(chunks):
                req = {
                    "op": "upload_chunk",
                    "path": remote_archive,
                    "data_b64": base64.b64encode(chunk).decode(),
                    "offset": idx * chunk_size,
                    "first": idx == 0,
                    "last": idx == len(chunks) - 1,
                }
                if idx == len(chunks) - 1:
                    req["sha256"] = sha
                    req["expected_size"] = len(data)
                resp = s.call(req, timeout=chunk_timeout)
                if "error" in resp:
                    raise RuntimeError(f"upload chunk {idx + 1}/{len(chunks)}: {resp}")
                if show_progress:
                    sys.stderr.write(f"\rchunk {idx + 1}/{len(chunks)}")
                    sys.stderr.flush()
            if show_progress:
                sys.stderr.write("\n")

            extract = (
                f"mkdir -p {shlex.quote(remote_project)} && "
                f"tar --overwrite --touch -xzf {shlex.quote(remote_archive)} "
                f"-C {shlex.quote(remote_project)} && "
                f"rm -f {shlex.quote(remote_archive)} && "
                f"cd {shlex.quote(remote_project)} && "
                f"sha256sum -c .codex_push_manifest.sha256 > /dev/null && "
                f"wc -l < .codex_push_manifest.sha256"
            )
            resp = s.call({"op": "exec", "cmd": extract, "timeout": 180}, timeout=200)
            if resp.get("rc") != 0:
                err = base64.b64decode(resp.get("stderr_b64", "")).decode("utf-8", "replace")
                raise RuntimeError(f"extract failed rc={resp.get('rc')}: {err.strip()}")
            count = base64.b64decode(resp["stdout_b64"]).decode("utf-8", "replace").strip()
        return {
            "files": int(count),
            "bytes": len(data),
            "chunks": len(chunks),
            "sha256": sha,
            "remote": remote_project,
        }
    finally:
        archive.unlink(missing_ok=True)


def cli() -> int:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="op", required=True)

    sub.add_parser("ping")
    sub.add_parser("reset")
    sub.add_parser("jobs")
    sub.add_parser("terminals")

    p_cleanup = sub.add_parser("cleanup")
    g = p_cleanup.add_mutually_exclusive_group(required=True)
    g.add_argument("--ours", action="store_true", help="delete only the terminal we created")
    g.add_argument("--name", help="delete one specific terminal by name")

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

    p_push = sub.add_parser("push")
    p_push.add_argument("local_project", type=Path)
    p_push.add_argument("remote_project")
    p_push.add_argument("--exclude", action="append", default=[])

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

    if op == "terminals":
        print(json.dumps(_list_terminals()))
        return 0

    if op == "cleanup":
        if args.ours:
            print(json.dumps(_cleanup_terminals("ours")))
        else:
            print(json.dumps(_cleanup_terminals("name", args.name)))
        return 0

    if op == "push":
        info = _push(args.local_project, args.remote_project, args.exclude)
        print(
            f"pushed {info['files']} files ({info['bytes']} bytes, "
            f"{info['chunks']} chunks) -> {info['remote']}"
        )
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(cli())
