"""Durable account/chat-scoped Core, EFB, and Telegram message identity mapping."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, Optional


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class MessageMappingStore:
    """SQLite mapping used for reply resolution without cross-chat guesses."""

    def __init__(self, db_path: Path | str, synchronous: str = "FULL") -> None:
        self.db_path = Path(db_path) if str(db_path) != ":memory:" else db_path
        self._synchronous = synchronous
        if isinstance(self.db_path, Path):
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._uri = str(self.db_path)
        else:
            self._uri = ":memory:"
        self._init_db()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._uri, timeout=15.0)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute(f"PRAGMA synchronous = {self._synchronous};")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS message_mapping (
                    consumer_id TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    efb_uid TEXT NOT NULL,
                    core_message_id TEXT NOT NULL DEFAULT '',
                    telegram_chat_id TEXT NOT NULL DEFAULT '',
                    telegram_message_id TEXT NOT NULL DEFAULT '',
                    direction TEXT NOT NULL DEFAULT '',
                    sender_identity TEXT NOT NULL DEFAULT '',
                    core_cursor TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (consumer_id, account_id, chat_id, efb_uid)
                );
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_message_mapping_core
                ON message_mapping(consumer_id, account_id, chat_id, core_message_id);
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_message_mapping_telegram
                ON message_mapping(consumer_id, telegram_chat_id, telegram_message_id);
                """
            )

    def record(
        self,
        consumer_id: str,
        account_id: str,
        chat_id: str,
        efb_uid: str,
        *,
        core_message_id: str = "",
        telegram_chat_id: str = "",
        telegram_message_id: str = "",
        direction: str = "",
        sender_identity: str = "",
        core_cursor: str = "",
    ) -> bool:
        cid = str(consumer_id or "").strip()
        acc = str(account_id or "").strip()
        chat = str(chat_id or "").strip()
        uid = str(efb_uid or "").strip()
        if not cid or not acc or not chat or not uid:
            return False
        now = _utc_now_iso()
        with self._connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO message_mapping (
                    consumer_id, account_id, chat_id, efb_uid, core_message_id,
                    telegram_chat_id, telegram_message_id, direction,
                    sender_identity, core_cursor, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(consumer_id, account_id, chat_id, efb_uid) DO UPDATE SET
                    core_message_id = CASE
                        WHEN excluded.core_message_id != '' THEN excluded.core_message_id
                        ELSE message_mapping.core_message_id END,
                    telegram_chat_id = CASE
                        WHEN excluded.telegram_chat_id != '' THEN excluded.telegram_chat_id
                        ELSE message_mapping.telegram_chat_id END,
                    telegram_message_id = CASE
                        WHEN excluded.telegram_message_id != '' THEN excluded.telegram_message_id
                        ELSE message_mapping.telegram_message_id END,
                    direction = CASE
                        WHEN excluded.direction != '' THEN excluded.direction
                        ELSE message_mapping.direction END,
                    sender_identity = CASE
                        WHEN excluded.sender_identity != '' THEN excluded.sender_identity
                        ELSE message_mapping.sender_identity END,
                    core_cursor = CASE
                        WHEN excluded.core_cursor != '' THEN excluded.core_cursor
                        ELSE message_mapping.core_cursor END,
                    updated_at = excluded.updated_at
                """,
                (
                    cid,
                    acc,
                    chat,
                    uid,
                    str(core_message_id or ""),
                    str(telegram_chat_id or ""),
                    str(telegram_message_id or ""),
                    str(direction or ""),
                    str(sender_identity or ""),
                    str(core_cursor or ""),
                    now,
                    now,
                ),
            )
            return cursor.rowcount > 0

    def resolve_target(
        self,
        consumer_id: str,
        account_id: str,
        chat_id: str,
        efb_uid: str,
    ) -> Optional[Dict[str, Any]]:
        with self._connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM message_mapping
                WHERE consumer_id = ? AND account_id = ? AND chat_id = ? AND efb_uid = ?
                """,
                (
                    str(consumer_id or "").strip(),
                    str(account_id or "").strip(),
                    str(chat_id or "").strip(),
                    str(efb_uid or "").strip(),
                ),
            ).fetchone()
        return dict(row) if row is not None else None

    def link_core_message(
        self,
        consumer_id: str,
        account_id: str,
        chat_id: str,
        efb_uid: str,
        core_message_id: str,
    ) -> bool:
        now = _utc_now_iso()
        with self._connection() as conn:
            cursor = conn.execute(
                """
                UPDATE message_mapping
                SET core_message_id = ?, updated_at = ?
                WHERE consumer_id = ? AND account_id = ? AND chat_id = ? AND efb_uid = ?
                """,
                (
                    str(core_message_id or "").strip(),
                    now,
                    str(consumer_id or "").strip(),
                    str(account_id or "").strip(),
                    str(chat_id or "").strip(),
                    str(efb_uid or "").strip(),
                ),
            )
            return cursor.rowcount == 1

    def get_by_core(
        self,
        consumer_id: str,
        account_id: str,
        chat_id: str,
        core_message_id: str,
    ) -> Optional[Dict[str, Any]]:
        with self._connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM message_mapping
                WHERE consumer_id = ? AND account_id = ? AND chat_id = ? AND core_message_id = ?
                ORDER BY updated_at DESC LIMIT 1
                """,
                (
                    str(consumer_id or "").strip(),
                    str(account_id or "").strip(),
                    str(chat_id or "").strip(),
                    str(core_message_id or "").strip(),
                ),
            ).fetchone()
        return dict(row) if row is not None else None

    def close(self) -> None:
        pass
