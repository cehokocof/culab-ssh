#!/usr/bin/env python3
"""Execute one shell command through a Jupyter Server terminal WebSocket.

This uses exported browser cookies and does not print cookie values.

Usage:
  HUB_URL=https://jupyter.culab.ru python3 jupyter_terminal_exec.py 'hostname; pwd; id'
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import socket
import ssl
import struct
import sys
import time
from http.cookiejar import Cookie, CookieJar
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote, urlparse
from urllib.request import HTTPCookieProcessor, Request, build_opener

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)


def load_cookie_items(path: Path) -> list[dict]:
    data = json.loads(path.read_text())
    if not isinstance(data, list):
        raise ValueError("Expected browser-exported cookie list")
    return [item for item in data if isinstance(item, dict)]


def cookie_header(items: list[dict], host: str, path: str) -> str:
    pairs = []
    for item in items:
        domain = str(item.get("domain", "")).lstrip(".")
        cpath = str(item.get("path", "/"))
        if host.endswith(domain) and path.startswith(cpath):
            pairs.append(f"{item['name']}={item['value']}")
    return "; ".join(pairs)


def xsrf_token(items: list[dict], host: str, path: str) -> str | None:
    for item in items:
        if item.get("name") == "_xsrf" and host.endswith(str(item.get("domain", "")).lstrip(".")):
            if path.startswith(str(item.get("path", "/"))):
                return str(item.get("value"))
    return None


def load_cookie_jar(items: list[dict], hub_url: str) -> CookieJar:
    jar = CookieJar()
    host = urlparse(hub_url).hostname or ""
    for item in items:
        domain = item.get("domain")
        if not domain or not host.endswith(str(domain).lstrip(".")):
            continue
        jar.set_cookie(
            Cookie(
                version=0,
                name=item["name"],
                value=item["value"],
                port=None,
                port_specified=False,
                domain=domain,
                domain_specified=True,
                domain_initial_dot=str(domain).startswith("."),
                path=item.get("path") or "/",
                path_specified=True,
                secure=bool(item.get("secure")),
                expires=int(item["expirationDate"]) if item.get("expirationDate") else None,
                discard=bool(item.get("session")),
                comment=None,
                comment_url=None,
                rest={"HttpOnly": item.get("httpOnly")},
                rfc2109=False,
            )
        )
    return jar


def discover_base_path(items: list[dict]) -> str:
    user_paths = sorted(
        {
            str(item.get("path", "")).rstrip("/")
            for item in items
            if str(item.get("path", "")).startswith("/user/")
        }
    )
    if not user_paths:
        raise RuntimeError("No /user/... cookie path found")
    return user_paths[0]


def create_terminal(hub_url: str, base_path: str, items: list[dict]) -> str:
    parsed = urlparse(hub_url)
    jar = load_cookie_jar(items, hub_url)
    opener = build_opener(HTTPCookieProcessor(jar))
    api_path = f"{base_path}/api/terminals"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "application/json",
        "Referer": f"{hub_url.rstrip('/')}{base_path}/tree",
    }
    token = xsrf_token(items, parsed.hostname or "", api_path)
    if token:
        headers["X-XSRFToken"] = token
    req = Request(
        f"{hub_url.rstrip('/')}{api_path}",
        data=b"{}",
        headers=headers,
        method="POST",
    )
    try:
        with opener.open(req, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        raise RuntimeError(f"create terminal failed: HTTP {exc.code}: {body[:500]}") from exc
    return str(payload["name"])


def list_terminals(hub_url: str, base_path: str, items: list[dict]) -> list[str]:
    jar = load_cookie_jar(items, hub_url)
    opener = build_opener(HTTPCookieProcessor(jar))
    req = Request(
        f"{hub_url.rstrip('/')}{base_path}/api/terminals",
        headers={"User-Agent": DEFAULT_USER_AGENT, "Accept": "application/json"},
        method="GET",
    )
    with opener.open(req, timeout=20) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return [str(item["name"]) for item in payload if "name" in item]


def delete_terminal(hub_url: str, base_path: str, items: list[dict], terminal_name: str) -> None:
    parsed = urlparse(hub_url)
    jar = load_cookie_jar(items, hub_url)
    opener = build_opener(HTTPCookieProcessor(jar))
    api_path = f"{base_path}/api/terminals/{quote(terminal_name)}"
    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "application/json",
        "Referer": f"{hub_url.rstrip('/')}{base_path}/tree",
    }
    token = xsrf_token(items, parsed.hostname or "", api_path)
    if token:
        headers["X-XSRFToken"] = token
    req = Request(
        f"{hub_url.rstrip('/')}{api_path}",
        headers=headers,
        method="DELETE",
    )
    with opener.open(req, timeout=20):
        pass


def reuse_terminal_enabled() -> bool:
    return os.environ.get("JUPYTER_REUSE_TERMINAL", "0") == "1"


def get_terminal(hub_url: str, base_path: str, items: list[dict]) -> str:
    if reuse_terminal_enabled():
        try:
            terminals = list_terminals(hub_url, base_path, items)
        except Exception:
            terminals = []
        if terminals:
            return terminals[-1]
    return create_terminal(hub_url, base_path, items)


def recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise EOFError("socket closed")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def ws_send_text(sock: socket.socket, text: str) -> None:
    previous_timeout = sock.gettimeout()
    sock.settimeout(None)
    payload = text.encode("utf-8")
    header = bytearray([0x81])
    length = len(payload)
    if length < 126:
        header.append(0x80 | length)
    elif length < 65536:
        header.append(0x80 | 126)
        header.extend(struct.pack("!H", length))
    else:
        header.append(0x80 | 127)
        header.extend(struct.pack("!Q", length))
    mask = secrets.token_bytes(4)
    header.extend(mask)
    masked = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))
    try:
        sock.sendall(bytes(header) + masked)
    finally:
        sock.settimeout(previous_timeout)


def ws_recv_text(sock: socket.socket, timeout: float) -> str | None:
    sock.settimeout(timeout)
    first = recv_exact(sock, 2)
    opcode = first[0] & 0x0F
    length = first[1] & 0x7F
    masked = bool(first[1] & 0x80)
    if length == 126:
        length = struct.unpack("!H", recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", recv_exact(sock, 8))[0]
    mask = recv_exact(sock, 4) if masked else b""
    payload = recv_exact(sock, length) if length else b""
    if masked:
        payload = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))
    if opcode == 0x8:
        return None
    if opcode == 0x9:
        return ""
    return payload.decode("utf-8", "replace")


def drain_ws(sock: socket.socket, seconds: float = 0.8) -> None:
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            ws_recv_text(sock, timeout=0.1)
        except socket.timeout:
            continue
        except Exception:
            return


def ws_connect(hub_url: str, base_path: str, terminal_name: str, items: list[dict]) -> socket.socket:
    parsed = urlparse(hub_url)
    host = parsed.hostname or ""
    port = parsed.port or 443
    ws_path = f"{base_path}/terminals/websocket/{quote(terminal_name)}"
    key = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
    headers = [
        f"GET {ws_path} HTTP/1.1",
        f"Host: {host}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {key}",
        "Sec-WebSocket-Version: 13",
        "Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits",
        f"Origin: https://{host}",
        f"User-Agent: {DEFAULT_USER_AGENT}",
        "Accept-Language: ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        f"Referer: https://{host}{base_path}/tree",
        f"Cookie: {cookie_header(items, host, ws_path)}",
        "",
        "",
    ]
    raw = socket.create_connection((host, port), timeout=20)
    sock = ssl.create_default_context().wrap_socket(raw, server_hostname=host)
    sock.sendall("\r\n".join(headers).encode("utf-8"))
    response = b""
    while b"\r\n\r\n" not in response:
        response += sock.recv(4096)
    head = response.decode("iso-8859-1", "replace")
    if " 101 " not in head.split("\r\n", 1)[0]:
        if "tmgrdfrend/showcaptcha" in head:
            raise RuntimeError(
                "Jupyter/Yandex anti-bot redirected this script to /tmgrdfrend/showcaptcha. "
                "The browser may look fine, but cookies.json is not accepted for WebSocket. "
                "Re-export cookies.json from the browser after opening the Jupyter page, then retry."
            )
        raise RuntimeError(f"websocket upgrade failed: {head[:500]}")
    accept = hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
    expected = base64.b64encode(accept).decode()
    accept_headers = [
        line.split(":", 1)[1].strip()
        for line in head.split("\r\n")
        if line.lower().startswith("sec-websocket-accept:")
    ]
    if expected not in accept_headers:
        raise RuntimeError("websocket accept header did not match")
    return sock


def main() -> int:
    hub_url = os.environ.get("HUB_URL", "https://jupyter.culab.ru")
    command = " ".join(sys.argv[1:]) or "hostname; pwd; id; uname -a"
    marker = f"__CODEX_DONE_{secrets.token_hex(6)}__"
    items = load_cookie_items(Path(os.environ.get("COOKIES_JSON", "cookies.json")))
    base_path = discover_base_path(items)
    terminal_name = get_terminal(hub_url, base_path, items)
    print(f"base_path={base_path}")
    print(f"terminal={terminal_name}")
    sock: socket.socket | None = None
    try:
        sock = ws_connect(hub_url, base_path, terminal_name, items)
        if reuse_terminal_enabled():
            ws_send_text(sock, json.dumps(["stdin", "\x03\rstty sane\r"]))
            drain_ws(sock, 1.0)
        ws_send_text(sock, json.dumps(["stdin", f"{command}; printf '\\n{marker}:$?\\n'\r"]))
        deadline = time.time() + int(os.environ.get("JUPYTER_EXEC_TIMEOUT", "25"))
        output = []
        while time.time() < deadline:
            try:
                message = ws_recv_text(sock, timeout=1.5)
            except socket.timeout:
                continue
            if not message:
                continue
            try:
                kind, data = json.loads(message)
            except Exception:
                data = message
            if data:
                output.append(str(data))
                if "".join(output).count(marker) >= 2:
                    break
        print("".join(output))
    finally:
        if sock is not None:
            sock.close()
        if not reuse_terminal_enabled():
            try:
                delete_terminal(hub_url, base_path, items, terminal_name)
            except Exception:
                pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
