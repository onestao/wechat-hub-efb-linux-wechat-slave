"""Isolated PID 1 probe for the final RC.14 functional image shutdown gate."""

from __future__ import annotations

import argparse
import json
import logging
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict
from urllib.parse import unquote, urlparse

import yaml

from efb_wechat_comwechat_slave.ComWechat import LinuxWeChatChannel


CHECKPOINT = 272548


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)


class CoreState:
    def __init__(self, evidence_path: Path) -> None:
        self.evidence_path = evidence_path
        self.lock = threading.Lock()
        self.checkpoints = []

    def record_checkpoint(self, payload: Dict[str, Any]) -> None:
        with self.lock:
            self.checkpoints.append(dict(payload))
            _write_json(
                self.evidence_path,
                {"checkpoint_requests": list(self.checkpoints), "count": len(self.checkpoints)},
            )


def _handler_type(state: CoreState):
    class Handler(BaseHTTPRequestHandler):
        server_version = "RC14CoreStub/1"

        def log_message(self, _format: str, *_args: Any) -> None:
            return

        def _send(self, status: int, payload: Dict[str, Any]) -> None:
            body = json.dumps(payload, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _json_body(self) -> Dict[str, Any]:
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length) if length else b"{}"
            payload = json.loads(raw.decode("utf-8"))
            return payload if isinstance(payload, dict) else {}

        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            path = urlparse(self.path).path
            if path == "/health":
                # The stub must satisfy the product contract exactly:
                # ``Core.py`` compares ``contract_version`` against the integer
                # ``CONTRACT_VERSION = 1``. This literal was previously the string
                # "v1", which made the stub fail the slave's own contract check and
                # aborted the exact-digest shutdown gate before it could measure
                # anything. Qualification tooling only -- no product code involved.
                self._send(200, {"status": "ok", "contract_version": 1})
                return
            if path.startswith("/v1/consumers/") and path.endswith("/bootstrap"):
                consumer = unquote(path[len("/v1/consumers/") : -len("/bootstrap")]).rstrip("/")
                self._send(
                    200,
                    {
                        "consumer_id": consumer,
                        "mode": "at_head",
                        "initial_cursor": CHECKPOINT,
                    },
                )
                return
            if path == "/v1/events/poll":
                self._send(
                    200,
                    {
                        "events": [],
                        "has_more": False,
                        "stream_head_cursor": CHECKPOINT,
                        "retention_floor_cursor": 0,
                    },
                )
                return
            self._send(404, {"error": {"code": "not_found", "message": path}})

        def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            path = urlparse(self.path).path
            payload = self._json_body()
            if path == "/v1/events/checkpoint":
                state.record_checkpoint(payload)
                self._send(200, {"ok": True, **payload})
                return
            if path == "/v1/consumers/bootstrap":
                self._send(200, {**payload, "initial_cursor": CHECKPOINT})
                return
            if path == "/v1/events/ack":
                self._send(200, {"ok": True, **payload})
                return
            self._send(404, {"error": {"code": "not_found", "message": path}})

    return Handler


class BlockingMaster:
    """Exercise the Retry3 bounded master-stop path without external traffic."""

    def stop_polling(self) -> None:
        threading.Event().wait(12.0)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--shutdown-evidence", type=Path, required=True)
    parser.add_argument("--core-evidence", type=Path, required=True)
    parser.add_argument("--ready", type=Path, required=True)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config = yaml.safe_load(args.profile.read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise RuntimeError("qualification profile must be a YAML mapping")

    state = CoreState(args.core_evidence)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _handler_type(state))
    thread = threading.Thread(target=server.serve_forever, name="rc14-core-stub", daemon=True)
    thread.start()

    config.setdefault("core", {})["base_url"] = f"http://127.0.0.1:{server.server_port}"
    channel = LinuxWeChatChannel(config=config, data_path=args.data_dir)
    channel.cursor_store.save(str(CHECKPOINT))
    coordinator = channel._shutdown_coordinator
    if coordinator is None:
        raise RuntimeError("shutdown coordinator was not installed")
    coordinator._evidence_path = str(args.shutdown_evidence)
    if not coordinator.install(master=BlockingMaster()):
        raise RuntimeError("blocking qualification master was not installed")

    _write_json(
        args.ready,
        {
            "ready": True,
            "pid": os.getpid(),
            "checkpoint": CHECKPOINT,
            "profile": str(args.profile),
            "core_stub": f"127.0.0.1:{server.server_port}",
        },
    )
    while True:
        time.sleep(60.0)


if __name__ == "__main__":
    raise SystemExit(main())
