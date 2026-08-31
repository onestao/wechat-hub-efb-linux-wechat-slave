"""HTTP adapter for WeChat Core Interface Contract V1.

This module intentionally has no EFB dependency. It is the only backend-facing
layer used by the Linux slave; Windows Hook/ComWechatRobot APIs must not leak
past this boundary.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional
from urllib.parse import quote

import requests


CONTRACT_VERSION = 1


class CoreError(RuntimeError):
    """Base class for Core adapter failures."""


class CoreUnavailableError(CoreError):
    """Core could not be reached or returned a non-protocol response."""


class CoreContractError(CoreError):
    """Core advertises an unsupported interface contract."""


class CoreAPIError(CoreError):
    """Structured non-2xx Core response."""

    def __init__(self, status_code: int, code: str, message: str, details: Optional[Mapping[str, Any]] = None):
        super().__init__(f"Core API {status_code} {code}: {message}")
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = dict(details or {})


@dataclass(frozen=True)
class CoreMedia:
    content: bytes
    mime_type: str
    filename: Optional[str]
    media_id: str


class CoreClient:
    """Small synchronous client for the frozen Core V1 contract."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 10.0,
        verify_tls: bool = True,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = float(timeout)
        self.verify_tls = bool(verify_tls)
        self.session = session or requests.Session()

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        kwargs.setdefault("timeout", self.timeout)
        kwargs.setdefault("verify", self.verify_tls)
        try:
            response = self.session.request(method, self._url(path), **kwargs)
        except requests.RequestException as exc:
            raise CoreUnavailableError(f"Core request failed: {method} {path}: {exc}") from exc

        if response.ok:
            return response

        try:
            payload = response.json()
        except ValueError as exc:
            raise CoreUnavailableError(
                f"Core returned non-JSON error response: {method} {path}: HTTP {response.status_code}"
            ) from exc
        error = payload.get("error") if isinstance(payload, dict) else None
        if not isinstance(error, dict):
            raise CoreUnavailableError(
                f"Core returned malformed error response: {method} {path}: HTTP {response.status_code}"
            )
        raise CoreAPIError(
            response.status_code,
            str(error.get("code") or "unknown_error"),
            str(error.get("message") or "Core request failed"),
            error.get("details") if isinstance(error.get("details"), dict) else {},
        )

    @staticmethod
    def _json(response: requests.Response) -> Dict[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise CoreUnavailableError("Core returned a non-JSON success response") from exc
        if not isinstance(payload, dict):
            raise CoreUnavailableError("Core returned a non-object JSON response")
        return payload

    def health(self) -> Dict[str, Any]:
        payload = self._json(self._request("GET", "/health"))
        version = payload.get("contract_version")
        if version != CONTRACT_VERSION:
            raise CoreContractError(
                f"Unsupported Core contract_version={version!r}; this slave requires {CONTRACT_VERSION}"
            )
        return payload

    def list_accounts(self) -> List[Dict[str, Any]]:
        payload = self._json(self._request("GET", "/v1/accounts"))
        accounts = payload.get("accounts")
        if not isinstance(accounts, list):
            raise CoreUnavailableError("Core /v1/accounts response has no accounts list")
        return [item for item in accounts if isinstance(item, dict)]

    def list_chats(self, account_id: str, *, query: str = "", limit: int = 200) -> List[Dict[str, Any]]:
        """Return all pages for one account without assuming cursor syntax."""
        rows: List[Dict[str, Any]] = []
        cursor = ""
        seen_cursors = set()
        while True:
            params: Dict[str, Any] = {"limit": max(1, min(int(limit), 200))}
            if cursor:
                params["cursor"] = cursor
            if query:
                params["query"] = query
            path = f"/v1/accounts/{quote(account_id, safe='')}/chats"
            payload = self._json(self._request("GET", path, params=params))
            chats = payload.get("chats")
            if not isinstance(chats, list):
                raise CoreUnavailableError("Core chat list response has no chats list")
            rows.extend(item for item in chats if isinstance(item, dict))
            next_cursor = payload.get("next_cursor")
            if not isinstance(next_cursor, str) or not next_cursor or next_cursor == cursor:
                break
            if next_cursor in seen_cursors:
                raise CoreUnavailableError("Core chat pagination returned a cursor loop")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        return rows

    def poll_events(
        self,
        *,
        after: str,
        consumer_id: str,
        timeout: int = 15,
        limit: int = 50,
        account_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "after": after or "0",
            "consumer_id": consumer_id,
            "timeout": max(0, min(int(timeout), 30)),
            "limit": max(1, min(int(limit), 200)),
        }
        if account_id:
            params["account_id"] = account_id
        return self._json(self._request("GET", "/v1/events/poll", params=params))

    def ack_events(self, consumer_id: str, event_ids: Iterable[str]) -> Dict[str, Any]:
        event_ids = [str(item) for item in event_ids if str(item)]
        if not event_ids:
            return {"consumer_id": consumer_id, "acked_event_ids": [], "acked_count": 0}
        return self._json(
            self._request(
                "POST",
                "/v1/events/ack",
                json={"consumer_id": consumer_id, "event_ids": event_ids},
            )
        )

    def get_media(self, account_id: str, media_id: str) -> CoreMedia:
        path = f"/v1/media/{quote(media_id, safe='')}"
        response = self._request("GET", path, params={"account_id": account_id})
        content_type = response.headers.get("Content-Type", "application/octet-stream").split(";", 1)[0].strip()
        disposition = response.headers.get("Content-Disposition", "")
        filename: Optional[str] = None
        if "filename=" in disposition:
            filename = disposition.split("filename=", 1)[1].strip().strip('"') or None
        return CoreMedia(
            content=response.content,
            mime_type=content_type or "application/octet-stream",
            filename=filename,
            media_id=response.headers.get("X-Media-Id", media_id),
        )

    def _send(self, kind: str, payload: Mapping[str, Any], idempotency_key: str) -> Dict[str, Any]:
        return self._json(
            self._request(
                "POST",
                f"/v1/send/{kind}",
                json=dict(payload),
                headers={"Idempotency-Key": idempotency_key} if idempotency_key else {},
            )
        )

    def send_text(self, payload: Mapping[str, Any], idempotency_key: str) -> Dict[str, Any]:
        return self._send("text", payload, idempotency_key)

    def send_image(self, payload: Mapping[str, Any], idempotency_key: str) -> Dict[str, Any]:
        return self._send("image", payload, idempotency_key)

    def send_file(self, payload: Mapping[str, Any], idempotency_key: str) -> Dict[str, Any]:
        return self._send("file", payload, idempotency_key)


class CursorStore:
    """Tiny durable cursor store using atomic replace, not a TTL delivery cache."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def load(self) -> str:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            cursor = payload.get("cursor") if isinstance(payload, dict) else None
            return str(cursor) if cursor not in (None, "") else "0"
        except FileNotFoundError:
            return "0"
        except (OSError, ValueError, TypeError):
            return "0"

    def save(self, cursor: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(prefix=self.path.name + ".", suffix=".tmp", dir=str(self.path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump({"cursor": str(cursor)}, handle, ensure_ascii=False, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, self.path)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)


class EchoStore:
    """Persist Core send-id to eventual message-id aliases across restarts.

    Kettly ETM stores the UID returned by ``send_message`` immediately.  Core
    may only learn the final WeChat echo message ID later via ``send.updated``.
    Persisting the alias lets the slave keep ETM's already-stored UID stable
    while translating reply targets and recalls back to the real Core ID.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._loaded = False
        self._send_to_echo: Dict[str, str] = {}
        self._pending: Dict[str, str] = {}

    def _load(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, ValueError, TypeError):
            return
        if not isinstance(payload, dict):
            return
        aliases = payload.get("send_to_echo")
        pending = payload.get("pending")
        if isinstance(aliases, dict):
            self._send_to_echo = {
                str(send_id): str(echo_id)
                for send_id, echo_id in aliases.items()
                if send_id and echo_id
            }
        if isinstance(pending, dict):
            self._pending = {
                str(send_id): str(request_id)
                for send_id, request_id in pending.items()
                if send_id and request_id
            }

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(prefix=self.path.name + ".", suffix=".tmp", dir=str(self.path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(
                    {"send_to_echo": self._send_to_echo, "pending": self._pending},
                    handle,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, self.path)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)

    def mark_pending(self, send_id: str, request_id: str) -> None:
        if not send_id:
            return
        self._load()
        self._pending[str(send_id)] = str(request_id or send_id)
        self._save()

    def link(self, send_id: str, echo_message_id: str) -> None:
        if not send_id or not echo_message_id:
            return
        self._load()
        self._send_to_echo[str(send_id)] = str(echo_message_id)
        self._pending.pop(str(send_id), None)
        self._save()

    def core_message_id(self, efb_message_id: str) -> Optional[str]:
        """Translate an ETM-stored send UID into Core's final message ID.

        ``None`` means the UID is a known outgoing send whose echo is not yet
        known, so it must not be passed to Core as ``target_message_id``.
        Unknown IDs are already Core message IDs and are returned unchanged.
        """
        value = str(efb_message_id)
        self._load()
        if value in self._send_to_echo:
            return self._send_to_echo[value]
        if value in self._pending:
            return None
        return value

    def efb_message_id(self, core_message_id: str) -> str:
        """Translate final Core echo/recall/target ID to ETM's stable send UID."""
        value = str(core_message_id)
        self._load()
        for send_id, echo_id in self._send_to_echo.items():
            if echo_id == value:
                return send_id
        return value

    def linked_echo(self, send_id: str) -> Optional[str]:
        self._load()
        return self._send_to_echo.get(str(send_id))
