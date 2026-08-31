"""Stable reversible EFB UID codec for opaque account-scoped Core IDs."""

from __future__ import annotations

import base64
import json
from typing import Tuple


class InvalidUID(ValueError):
    pass


def _encode(prefix: str, first: str, second: str) -> str:
    raw = json.dumps([str(first), str(second)], ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    token = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    return f"{prefix}.{token}"


def _decode(prefix: str, value: str) -> Tuple[str, str]:
    marker = prefix + "."
    if not isinstance(value, str) or not value.startswith(marker):
        raise InvalidUID(f"UID is not a {prefix} identifier")
    token = value[len(marker):]
    token += "=" * (-len(token) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8"))
    except (ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise InvalidUID(f"Malformed {prefix} UID") from exc
    if not isinstance(payload, list) or len(payload) != 2 or not all(isinstance(item, str) for item in payload):
        raise InvalidUID(f"Malformed {prefix} UID payload")
    return payload[0], payload[1]


def encode_chat_uid(account_id: str, chat_id: str) -> str:
    return _encode("c1", account_id, chat_id)


def decode_chat_uid(uid: str) -> Tuple[str, str]:
    return _decode("c1", uid)


def encode_member_uid(account_id: str, member_id: str) -> str:
    return _encode("m1", account_id, member_id)


def decode_member_uid(uid: str) -> Tuple[str, str]:
    return _decode("m1", uid)
