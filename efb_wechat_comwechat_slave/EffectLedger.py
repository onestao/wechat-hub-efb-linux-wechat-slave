"""Durable External Effect Ledger for EFB Linux WeChat Slave.

Implements Normative Rule P3 from docs/RC14_NEW_CONSUMER_BOOTSTRAP_POLICY.md:
"Outbound-facing consumers require delivery idempotency, not just cursor durability.
 Any consumer that can drive an external side effect MUST hold a durable effect ledger
 keyed by (consumer_id, effect_id), where effect_id is a consumer-visible stable identity
 (for WeChat: account_id + message_id), checked before the external call."

Guarantees:
  - Zero duplicate deliveries across EFB restarts.
  - Core duplicate emissions for the same message_id are safely absorbed.
  - Atomic persistence to local SQLite in WAL mode.
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, Mapping, Optional


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class EffectLedger:
    """SQLite-backed durable ledger of externally visible effects dispatched to master."""

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path) if str(db_path) != ":memory:" else db_path
        if isinstance(self.db_path, Path):
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._uri = str(self.db_path)
        else:
            self._uri = ":memory:"
        self._init_db()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._uri, timeout=10.0)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
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
                CREATE TABLE IF NOT EXISTS effect_ledger (
                    consumer_id TEXT NOT NULL,
                    effect_id TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    efb_uid TEXT NOT NULL DEFAULT '',
                    event_type TEXT NOT NULL DEFAULT 'message.created',
                    status TEXT NOT NULL DEFAULT 'delivered',
                    created_at TEXT NOT NULL,
                    details_json TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (consumer_id, effect_id)
                );
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_effect_ledger_account_msg
                ON effect_ledger(account_id, message_id);
                """
            )

    @staticmethod
    def compute_effect_id(account_id: str, message_id: str, event_type: str = "message.created") -> str:
        """Compute the stable effect key from account_id and message_id."""
        acc = str(account_id or "").strip()
        msg = str(message_id or "").strip()
        if not acc or not msg:
            raise ValueError(f"account_id and message_id are required: account={acc!r}, message={msg!r}")
        return f"{acc}:{msg}"

    def is_effect_delivered(self, consumer_id: str, effect_id: str) -> bool:
        """Check if an effect has already been delivered to the master."""
        cid = str(consumer_id or "").strip()
        eid = str(effect_id or "").strip()
        if not cid or not eid:
            return False
        with self._connection() as conn:
            row = conn.execute(
                "SELECT status FROM effect_ledger WHERE consumer_id = ? AND effect_id = ?",
                (cid, eid),
            ).fetchone()
            if row is None:
                return False
            return str(row["status"]) == "delivered"

    def is_message_delivered(self, consumer_id: str, account_id: str, message_id: str) -> bool:
        """Helper to check if a specific message has already been delivered."""
        try:
            effect_id = self.compute_effect_id(account_id, message_id)
            return self.is_effect_delivered(consumer_id, effect_id)
        except ValueError:
            return False

    def record_delivered(
        self,
        consumer_id: str,
        effect_id: str,
        *,
        account_id: str,
        message_id: str,
        efb_uid: str = "",
        event_type: str = "message.created",
        details: Optional[Mapping[str, Any]] = None,
    ) -> bool:
        """Atomically record a delivered effect in the ledger.

        Returns True if newly inserted, False if it already existed.
        """
        cid = str(consumer_id or "").strip()
        eid = str(effect_id or "").strip()
        acc = str(account_id or "").strip()
        mid = str(message_id or "").strip()
        uid = str(efb_uid or "").strip()
        now = _utc_now_iso()
        details_str = json.dumps(dict(details or {}), ensure_ascii=False)

        with self._connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO effect_ledger (
                    consumer_id, effect_id, account_id, message_id,
                    efb_uid, event_type, status, created_at, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, 'delivered', ?, ?)
                ON CONFLICT(consumer_id, effect_id) DO UPDATE SET
                    efb_uid = CASE WHEN excluded.efb_uid != '' THEN excluded.efb_uid ELSE effect_ledger.efb_uid END,
                    status = 'delivered',
                    details_json = excluded.details_json
                """,
                (cid, eid, acc, mid, uid, event_type, now, details_str),
            )
            return cursor.rowcount > 0

    def get_effect(self, consumer_id: str, effect_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve details of a recorded effect."""
        cid = str(consumer_id or "").strip()
        eid = str(effect_id or "").strip()
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM effect_ledger WHERE consumer_id = ? AND effect_id = ?",
                (cid, eid),
            ).fetchone()
            if row is None:
                return None
            return {
                "consumer_id": row["consumer_id"],
                "effect_id": row["effect_id"],
                "account_id": row["account_id"],
                "message_id": row["message_id"],
                "efb_uid": row["efb_uid"],
                "event_type": row["event_type"],
                "status": row["status"],
                "created_at": row["created_at"],
                "details": json.loads(row["details_json"] or "{}"),
            }

    def count_effects(self, consumer_id: str = "") -> int:
        """Count total delivered effects in the ledger."""
        with self._connection() as conn:
            if consumer_id:
                row = conn.execute(
                    "SELECT COUNT(*) FROM effect_ledger WHERE consumer_id = ?",
                    (str(consumer_id).strip(),),
                ).fetchone()
            else:
                row = conn.execute("SELECT COUNT(*) FROM effect_ledger").fetchone()
            return int(row[0] if row else 0)

    def close(self) -> None:
        """Cleanly close ledger (no persistent connection held open)."""
        pass
