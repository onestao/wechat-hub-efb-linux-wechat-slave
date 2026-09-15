r"""Durable External Effect Ledger for EFB Linux WeChat Slave.

Implements Normative Rule P3 from docs/RC14_NEW_CONSUMER_BOOTSTRAP_POLICY.md and
governs outbound side effect deduplication:
"Outbound-facing consumers require delivery idempotency, not just cursor durability.
 Any consumer that can drive an external side effect MUST hold a durable effect ledger
 keyed by (consumer_id, effect_id), where effect_id is a consumer-visible stable identity
 (for WeChat: account_id + message_id), checked before the external call."

Delivery Semantics:
  AT_MOST_ONCE_WITH_FAIL_CLOSED_UNCERTAIN
  Because downstream master (e.g. Telegram) does not expose distributed 2PC or atomic
  reconciliation primitives across the network boundary, cross-system delivery CANNOT
  truthfully claim EXACTLY_ONCE. Instead, the durable state machine guarantees
  AT_MOST_ONCE external side effects by transitioning through:
    [UNSEEN] -> RESERVED -> [EXTERNAL CALL] -> DELIVERED
                            \-> UNCERTAIN (on crash / failure)
  If an unconfirmed RESERVED state is found after process crash or power-loss, the ledger
  fails closed with BLOCKED_UNCERTAIN_EFFECT and refuses to re-dispatch the external call.

Power-Loss Durability Semantics:
  The ledger is backed by SQLite in Write-Ahead Log (WAL) mode with PRAGMA synchronous = FULL.
  Under synchronous = FULL, every transaction commit issues an fsync on the WAL journal
  before returning control. Therefore, an effect reservation is durably persisted to non-volatile
  storage prior to invoking the external network call, preventing the crash gap where
  an external delivery occurs while the ledger record is lost.
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Tuple

STATE_RESERVED = "RESERVED"
STATE_DELIVERED = "DELIVERED"
STATE_UNCERTAIN = "UNCERTAIN"

BLOCKED_UNCERTAIN_EFFECT = "BLOCKED_UNCERTAIN_EFFECT"
DELIVERY_SEMANTICS = "AT_MOST_ONCE_WITH_FAIL_CLOSED_UNCERTAIN"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class EffectLedger:
    """SQLite WAL-backed durable state machine for external effect deduplication."""

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
                CREATE TABLE IF NOT EXISTS effect_ledger (
                    consumer_id TEXT NOT NULL,
                    effect_id TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    efb_uid TEXT NOT NULL DEFAULT '',
                    event_type TEXT NOT NULL DEFAULT 'message.created',
                    status TEXT NOT NULL DEFAULT 'RESERVED',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    details_json TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (consumer_id, effect_id)
                );
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_effect_ledger_status
                ON effect_ledger(consumer_id, status);
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

    def checkpoint_wal(self) -> None:
        """Consolidate the SQLite WAL into the main database file.

        Durable-state helper for graceful shutdown (Retry3, defect R14-EFB-D3).
        Every committed transaction is already fsync'd because the ledger runs
        with ``synchronous = FULL``; this call is a storage-consolidation step,
        not a durability requirement.  It never inserts, updates, deletes,
        truncates or recreates ledger rows, and it never removes the database
        file or its WAL/SHM sidecars.
        """
        if not isinstance(self.db_path, Path) or not self.db_path.exists():
            return
        conn = sqlite3.connect(self._uri, timeout=15.0)
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            conn.commit()
        finally:
            conn.close()

    def status_counts(self, consumer_id: Optional[str] = None) -> Dict[str, int]:
        """Read-only status histogram of the ledger.

        Used by forensic/qualification tooling (Retry3 cross-run sentinel and
        evidence generator). Never mutates the ledger.
        """
        counts: Dict[str, int] = {STATE_RESERVED: 0, STATE_DELIVERED: 0, STATE_UNCERTAIN: 0}
        if not isinstance(self.db_path, Path) or not self.db_path.exists():
            return counts
        conn = sqlite3.connect(self._uri, timeout=15.0)
        try:
            if consumer_id:
                rows = conn.execute(
                    "SELECT status, COUNT(*) FROM effect_ledger WHERE consumer_id = ? GROUP BY status",
                    (str(consumer_id).strip(),),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT status, COUNT(*) FROM effect_ledger GROUP BY status"
                ).fetchall()
        finally:
            conn.close()
        for status, count in rows:
            counts[str(status)] = int(count)
        return counts

    def get_effect_status(self, consumer_id: str, effect_id: str) -> Optional[str]:
        """Query current state machine status: RESERVED, DELIVERED, UNCERTAIN, or None."""
        cid = str(consumer_id or "").strip()
        eid = str(effect_id or "").strip()
        if not cid or not eid:
            return None
        with self._connection() as conn:
            row = conn.execute(
                "SELECT status FROM effect_ledger WHERE consumer_id = ? AND effect_id = ?",
                (cid, eid),
            ).fetchone()
            if row is None:
                return None
            return str(row["status"])

    def is_effect_delivered(self, consumer_id: str, effect_id: str) -> bool:
        """Check if an effect has reached DELIVERED state."""
        return self.get_effect_status(consumer_id, effect_id) == STATE_DELIVERED

    def is_message_delivered(self, consumer_id: str, account_id: str, message_id: str) -> bool:
        """Helper to check if a message has reached DELIVERED state."""
        try:
            effect_id = self.compute_effect_id(account_id, message_id)
            return self.is_effect_delivered(consumer_id, effect_id)
        except ValueError:
            return False

    def reserve_effect(
        self,
        consumer_id: str,
        effect_id: str,
        *,
        account_id: str,
        message_id: str,
        event_type: str = "message.created",
        details: Optional[Mapping[str, Any]] = None,
    ) -> Tuple[bool, str]:
        """Durably reserve an effect before attempting external dispatch.

        Returns:
            (True, STATE_RESERVED) if newly reserved.
            (False, STATE_DELIVERED) if already successfully delivered.
            (False, BLOCKED_UNCERTAIN_EFFECT) if prior attempt was RESERVED or UNCERTAIN.
        """
        cid = str(consumer_id or "").strip()
        eid = str(effect_id or "").strip()
        acc = str(account_id or "").strip()
        mid = str(message_id or "").strip()
        now = _utc_now_iso()
        details_map = dict(details or {})
        details_str = json.dumps(details_map, ensure_ascii=False)

        with self._connection() as conn:
            row = conn.execute(
                "SELECT status FROM effect_ledger WHERE consumer_id = ? AND effect_id = ?",
                (cid, eid),
            ).fetchone()

            if row is not None:
                existing_status = str(row["status"])
                if existing_status == STATE_DELIVERED:
                    return False, STATE_DELIVERED
                # In-flight reservation or previous uncertain crash: fail closed!
                return False, BLOCKED_UNCERTAIN_EFFECT

            conn.execute(
                """
                INSERT INTO effect_ledger (
                    consumer_id, effect_id, account_id, message_id,
                    efb_uid, event_type, status, created_at, updated_at, details_json
                ) VALUES (?, ?, ?, ?, '', ?, ?, ?, ?, ?)
                """,
                (cid, eid, acc, mid, event_type, STATE_RESERVED, now, now, details_str),
            )
            return True, STATE_RESERVED

    def mark_delivered(
        self,
        consumer_id: str,
        effect_id: str,
        *,
        efb_uid: str = "",
        event_type: str = "message.created",
        details: Optional[Mapping[str, Any]] = None,
    ) -> bool:
        """Atomically transition effect to DELIVERED status after successful external dispatch."""
        cid = str(consumer_id or "").strip()
        eid = str(effect_id or "").strip()
        uid = str(efb_uid or "").strip()
        now = _utc_now_iso()

        with self._connection() as conn:
            row = conn.execute(
                "SELECT details_json FROM effect_ledger WHERE consumer_id = ? AND effect_id = ?",
                (cid, eid),
            ).fetchone()

            merged_details: Dict[str, Any] = {}
            if row and row["details_json"]:
                try:
                    merged_details.update(json.loads(row["details_json"]))
                except Exception:
                    pass
            if details:
                merged_details.update(details)
            details_str = json.dumps(merged_details, ensure_ascii=False)

            cursor = conn.execute(
                """
                UPDATE effect_ledger
                SET status = ?, efb_uid = CASE WHEN ? != '' THEN ? ELSE efb_uid END,
                    updated_at = ?, details_json = ?
                WHERE consumer_id = ? AND effect_id = ?
                """,
                (STATE_DELIVERED, uid, uid, now, details_str, cid, eid),
            )
            return cursor.rowcount > 0

    def mark_uncertain(
        self,
        consumer_id: str,
        effect_id: str,
        *,
        reason: str = "",
        details: Optional[Mapping[str, Any]] = None,
    ) -> bool:
        """Transition effect to UNCERTAIN status when external delivery failed or was interrupted."""
        cid = str(consumer_id or "").strip()
        eid = str(effect_id or "").strip()
        now = _utc_now_iso()

        with self._connection() as conn:
            row = conn.execute(
                "SELECT details_json FROM effect_ledger WHERE consumer_id = ? AND effect_id = ?",
                (cid, eid),
            ).fetchone()

            merged_details: Dict[str, Any] = {}
            if row and row["details_json"]:
                try:
                    merged_details.update(json.loads(row["details_json"]))
                except Exception:
                    pass
            merged_details["blocked_reason"] = BLOCKED_UNCERTAIN_EFFECT
            if reason:
                merged_details["uncertain_reason"] = str(reason)
            if details:
                merged_details.update(details)
            details_str = json.dumps(merged_details, ensure_ascii=False)

            cursor = conn.execute(
                """
                UPDATE effect_ledger
                SET status = ?, updated_at = ?, details_json = ?
                WHERE consumer_id = ? AND effect_id = ?
                """,
                (STATE_UNCERTAIN, now, details_str, cid, eid),
            )
            return cursor.rowcount > 0

    def reconcile_on_startup(self, consumer_id: str) -> List[str]:
        """Startup reconciliation: identify unfinalized RESERVED effects and transition to UNCERTAIN.

        Any effect left in RESERVED indicates the process terminated or crashed while
        dispatching to the external network. Because the downstream master state cannot be
        guaranteed without 2PC, we fail closed and mark the effect UNCERTAIN
        (BLOCKED_UNCERTAIN_EFFECT) to prevent duplicate external re-deliveries.
        """
        cid = str(consumer_id or "").strip()
        if not cid:
            return []
        now = _utc_now_iso()
        reconciled_effects: List[str] = []

        with self._connection() as conn:
            rows = conn.execute(
                "SELECT effect_id, details_json FROM effect_ledger WHERE consumer_id = ? AND status = ?",
                (cid, STATE_RESERVED),
            ).fetchall()

            for row in rows:
                eid = row["effect_id"]
                reconciled_effects.append(eid)
                try:
                    cur_details = json.loads(row["details_json"] or "{}")
                except Exception:
                    cur_details = {}
                cur_details["reconciled_at"] = now
                cur_details["blocked_reason"] = BLOCKED_UNCERTAIN_EFFECT
                cur_details["uncertain_reason"] = "Process crash/restart while in RESERVED state"
                details_str = json.dumps(cur_details, ensure_ascii=False)

                conn.execute(
                    """
                    UPDATE effect_ledger
                    SET status = ?, updated_at = ?, details_json = ?
                    WHERE consumer_id = ? AND effect_id = ?
                    """,
                    (STATE_UNCERTAIN, now, details_str, cid, eid),
                )

        return reconciled_effects

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
        """Atomic reservation and delivery recording (legacy compatibility helper)."""
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
                    efb_uid, event_type, status, created_at, updated_at, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(consumer_id, effect_id) DO UPDATE SET
                    efb_uid = CASE WHEN excluded.efb_uid != '' THEN excluded.efb_uid ELSE effect_ledger.efb_uid END,
                    status = ?,
                    updated_at = excluded.updated_at,
                    details_json = excluded.details_json
                """,
                (cid, eid, acc, mid, uid, event_type, STATE_DELIVERED, now, now, details_str, STATE_DELIVERED),
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
                "updated_at": row["updated_at"],
                "details": json.loads(row["details_json"] or "{}"),
            }

    def count_effects(self, consumer_id: str = "", status: Optional[str] = None) -> int:
        """Count effects in the ledger, optionally filtered by status."""
        with self._connection() as conn:
            query = "SELECT COUNT(*) FROM effect_ledger WHERE 1=1"
            params: List[Any] = []
            if consumer_id:
                query += " AND consumer_id = ?"
                params.append(str(consumer_id).strip())
            if status:
                query += " AND status = ?"
                params.append(str(status).strip())
            row = conn.execute(query, params).fetchone()
            return int(row[0] if row else 0)

    def close(self) -> None:
        """Cleanly close ledger resources."""
        pass
