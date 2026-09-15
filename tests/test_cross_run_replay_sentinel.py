"""RC.14 Optional EFB — Retry3 cross-run replay safety (sentinel isolation tests).

Context
-------
Retry2's cross-run replay evidence is **INDETERMINATE** and stays that way: the
production ``core-effect-ledger.sqlite3`` was deleted and recreated empty during
that attempt, so no pre-existing DELIVERED baseline survived to prove that a
replayed ``message.created`` would be suppressed.

Retry3 does not rewrite that history.  It establishes a new forward-only
baseline from the ledger that exists today and pins one real DELIVERED effect as
a replay sentinel (see ``scripts/qualification/rc14_retry3_sentinel.py`` on the
ops side).  These tests prove, offline and without touching any production
state, that replaying such a sentinel produces:

    EXTERNAL_DELIVERY_COUNT  = 0
    DUPLICATE_SUPPRESSED     = YES
    LEDGER_ROW_NOT_RECREATED = YES

and that the ledger file is byte-identical before and after the replay.
"""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest import mock

try:
    import ehforwarderbot  # noqa: F401
except ImportError:  # pragma: no cover - offline fallback
    import tests.stub_ehforwarderbot as _stub

    _stub.install_stubs()

from ehforwarderbot import Message, coordinator  # noqa: E402

from efb_wechat_comwechat_slave.ComWechat import LinuxWeChatChannel  # noqa: E402
from efb_wechat_comwechat_slave.Core import CoreClient  # noqa: E402
from efb_wechat_comwechat_slave.EffectLedger import (  # noqa: E402
    STATE_DELIVERED,
    STATE_RESERVED,
    STATE_UNCERTAIN,
)

CONSUMER_BASE = "efb-linux-wechat"
SENTINEL_ACCOUNT = "acc-sentinel"
SENTINEL_MESSAGE = "msg-sentinel-0001"
SENTINEL_EFFECT = f"{SENTINEL_ACCOUNT}:{SENTINEL_MESSAGE}"


def _sha256(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


class OfflineCore(CoreClient):
    """Core double: no sockets, deterministic bootstrap provenance."""

    def __init__(self, base_url: str = "http://127.0.0.1:8080") -> None:
        super().__init__(base_url)
        self.stream_head = 0
        self.checkpoints: Dict[str, int] = {}
        self.events_queue: List[Dict[str, Any]] = []

    def health(self) -> Dict[str, Any]:
        return {"contract_version": 1, "status": "ok"}

    def list_accounts(self) -> List[Dict[str, Any]]:
        return [{"account_id": SENTINEL_ACCOUNT, "display_name": "Sentinel", "state": "online"}]

    def list_chats(self, account_id: str, *, query: str = "", limit: int = 200) -> List[Dict[str, Any]]:
        return [
            {
                "account_id": SENTINEL_ACCOUNT,
                "chat_id": "chat-sentinel",
                "type": "private",
                "display_name": "Sentinel Chat",
            }
        ]

    def get_bootstrap_provenance(self, consumer_id: str) -> Optional[Dict[str, Any]]:
        return {
            "consumer_id": consumer_id,
            "bootstrap_mode": "bounded_window",
            "initial_cursor": self.stream_head,
        }

    def bootstrap_consumer(self, consumer_id: str, **kwargs: Any) -> Dict[str, Any]:
        self.checkpoints[consumer_id] = self.stream_head
        return {
            "ok": True,
            "consumer_id": consumer_id,
            "initial_cursor": self.stream_head,
            "bootstrap_mode": "bounded_window",
            "stream_head_cursor": self.stream_head,
        }

    def poll_events(self, *, after: str, consumer_id: str, timeout: int = 15, limit: int = 50, account_id: Optional[str] = None) -> Dict[str, Any]:
        after_int = int(after or "0")
        available = [e for e in self.events_queue if int(e.get("cursor", 0)) > after_int]
        return {
            "events": available[:limit],
            "has_more": False,
            "stream_head_cursor": self.stream_head,
            "retention_floor_cursor": 0,
        }

    def checkpoint_events(self, consumer_id: str, processed_through_cursor: int, **kwargs: Any) -> Dict[str, Any]:
        self.checkpoints[consumer_id] = processed_through_cursor
        return {"ok": True, "consumer_id": consumer_id, "processed_through_cursor": processed_through_cursor}

    def ack_events(self, consumer_id: str, event_ids: Any) -> Dict[str, Any]:
        return {"ok": True, "acked_event_ids": list(event_ids)}


def sentinel_event(cursor: int = 500) -> Dict[str, Any]:
    """The replayed event for the pinned sentinel effect."""
    return {
        "event_id": "ev-sentinel-replay",
        "cursor": cursor,
        "event_type": "message.created",
        "account_id": SENTINEL_ACCOUNT,
        "payload": {
            "message": {
                "message_id": SENTINEL_MESSAGE,
                "chat_id": "chat-sentinel",
                "text": "sentinel body must never be delivered twice",
                "type": "text",
            }
        },
    }


class CrossRunReplaySentinelTest(unittest.TestCase):
    """Proves the three Retry3 cross-run replay guarantees."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.data_path = Path(self.temp_dir.name)
        self.core = OfflineCore()
        self.delivered: List[Message] = []

        self._orig_master = getattr(coordinator, "master", None)
        self._orig_send_message = getattr(coordinator, "send_message", None)
        coordinator.master = mock.MagicMock()
        coordinator.send_message = self.delivered.append

    def tearDown(self) -> None:
        coordinator.master = self._orig_master
        coordinator.send_message = self._orig_send_message
        self.temp_dir.cleanup()

    def make_channel(self) -> LinuxWeChatChannel:
        return LinuxWeChatChannel(
            core_client=self.core,
            config={
                "core": {"base_url": "http://mock-core", "timeout": 2, "poll_timeout": 0},
                "consumer_id": CONSUMER_BASE,
                "poll_interval": 0.01,
                "startup_healthcheck": True,
                "shutdown_hard_exit": False,
                "shutdown_install_deferred": False,
            },
            data_path=self.data_path,
        )

    def seed_sentinel(self, channel: LinuxWeChatChannel, status: str = STATE_DELIVERED) -> Dict[str, Any]:
        """Create the pinned sentinel exactly as the production ledger would hold it."""
        ledger = channel.effect_ledger
        cid = channel.consumer_id
        reserved_ok, _ = ledger.reserve_effect(
            cid, SENTINEL_EFFECT, account_id=SENTINEL_ACCOUNT, message_id=SENTINEL_MESSAGE
        )
        self.assertTrue(reserved_ok)
        if status == STATE_DELIVERED:
            self.assertTrue(
                ledger.mark_delivered(
                    cid,
                    SENTINEL_EFFECT,
                    efb_uid="efb-uid-sentinel",
                    details={"chat_id": "chat-sentinel"},
                )
            )
        row = ledger.get_effect(cid, SENTINEL_EFFECT)
        self.assertIsNotNone(row)
        return dict(row)

    # ------------------------------------------------------------------ proofs
    def test_replay_of_delivered_sentinel_is_suppressed(self) -> None:
        channel = self.make_channel()
        try:
            row_before = self.seed_sentinel(channel)
            ledger_path = channel.effect_ledger.db_path
            sha_before = _sha256(ledger_path)
            count_before = channel.effect_ledger.count_effects(channel.consumer_id)

            # --- the cross-run replay -------------------------------------
            channel._handle_event(sentinel_event())

            external_delivery_count = len(self.delivered)
            counts = channel.effect_ledger.status_counts(channel.consumer_id)
            row_after = channel.effect_ledger.get_effect(channel.consumer_id, SENTINEL_EFFECT)
            count_after = channel.effect_ledger.count_effects(channel.consumer_id)

            # 1. EXTERNAL_DELIVERY_COUNT = 0
            self.assertEqual(external_delivery_count, 0, "replayed sentinel must not be delivered")

            # 2. DUPLICATE_SUPPRESSED = YES  (no new reservation was created)
            self.assertEqual(counts.get(STATE_DELIVERED), 1)
            self.assertEqual(counts.get(STATE_RESERVED), 0)
            self.assertEqual(counts.get(STATE_UNCERTAIN), 0)
            self.assertEqual(count_after, count_before, "no ledger row may be added")

            # 3. LEDGER_ROW_NOT_RECREATED = YES
            self.assertEqual(row_after["effect_id"], row_before["effect_id"])
            self.assertEqual(row_after["created_at"], row_before["created_at"])
            self.assertEqual(row_after["efb_uid"], row_before["efb_uid"])
            self.assertEqual(row_after["status"], STATE_DELIVERED)
            self.assertEqual(
                row_after["details"],
                row_before["details"],
                "the replayed effect must not rewrite the DELIVERED row",
            )

            # the ledger file itself is untouched
            self.assertEqual(_sha256(ledger_path), sha_before)
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()

    def test_replay_suppression_survives_a_restart(self) -> None:
        """A new process over the same data dir must still suppress the sentinel."""
        channel1 = self.make_channel()
        try:
            self.seed_sentinel(channel1)
        finally:
            channel1.stop_polling()
            channel1.effect_ledger.close()

        channel2 = self.make_channel()
        try:
            sha_before = _sha256(channel2.effect_ledger.db_path)
            channel2._handle_event(sentinel_event(cursor=501))

            self.assertEqual(len(self.delivered), 0, "restart replay must deliver nothing")
            counts = channel2.effect_ledger.status_counts(channel2.consumer_id)
            self.assertEqual(counts.get(STATE_DELIVERED), 1)
            self.assertEqual(counts.get(STATE_RESERVED), 0)
            self.assertEqual(_sha256(channel2.effect_ledger.db_path), sha_before)
        finally:
            channel2.stop_polling()
            channel2.effect_ledger.close()

    def test_replay_of_uncertain_sentinel_is_fail_closed(self) -> None:
        channel = self.make_channel()
        try:
            ledger = channel.effect_ledger
            cid = channel.consumer_id
            ledger.reserve_effect(
                cid, SENTINEL_EFFECT, account_id=SENTINEL_ACCOUNT, message_id=SENTINEL_MESSAGE
            )
            ledger.mark_uncertain(cid, SENTINEL_EFFECT, reason="retry3 sentinel test")

            channel._handle_event(sentinel_event())

            self.assertEqual(len(self.delivered), 0, "UNCERTAIN must never be re-delivered")
            self.assertEqual(
                ledger.get_effect_status(cid, SENTINEL_EFFECT), STATE_UNCERTAIN,
                "an UNCERTAIN sentinel must not be promoted",
            )
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()

    def test_new_effect_still_delivers_normally(self) -> None:
        """Suppression must be targeted, not a blanket failure of the channel."""
        channel = self.make_channel()
        try:
            self.seed_sentinel(channel)
            channel._handle_event(
                {
                    "event_id": "ev-fresh",
                    "cursor": 600,
                    "event_type": "message.created",
                    "account_id": SENTINEL_ACCOUNT,
                    "payload": {
                        "message": {
                            "message_id": "msg-fresh-0001",
                            "chat_id": "chat-sentinel",
                            "text": "a genuinely new message",
                            "type": "text",
                        }
                    },
                }
            )
            self.assertEqual(len(self.delivered), 1, "a fresh effect must still be delivered")
            counts = channel.effect_ledger.status_counts(channel.consumer_id)
            self.assertEqual(counts.get(STATE_DELIVERED), 2)
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()

    def test_repeated_replays_never_accumulate_deliveries(self) -> None:
        channel = self.make_channel()
        try:
            self.seed_sentinel(channel)
            for index in range(10):
                channel._handle_event(sentinel_event(cursor=700 + index))

            self.assertEqual(len(self.delivered), 0)
            counts = channel.effect_ledger.status_counts(channel.consumer_id)
            self.assertEqual(counts.get(STATE_DELIVERED), 1)
            self.assertEqual(counts.get(STATE_RESERVED), 0)
            self.assertEqual(counts.get(STATE_UNCERTAIN), 0)
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()

    def test_ledger_is_never_replaced_by_an_empty_copy(self) -> None:
        """Guards the Retry2 procedural violation from ever recurring here."""
        channel = self.make_channel()
        try:
            self.seed_sentinel(channel)
            ledger_path = channel.effect_ledger.db_path
            before_bytes = ledger_path.read_bytes()

            channel._handle_event(sentinel_event())

            self.assertTrue(ledger_path.exists(), "the ledger file must still exist")
            self.assertEqual(ledger_path.read_bytes(), before_bytes)
            self.assertEqual(
                channel.effect_ledger.status_counts(channel.consumer_id).get(STATE_DELIVERED), 1
            )
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()

    def test_result_summary_fields(self) -> None:
        """Emit the three proof fields in the exact form the taskbook requires."""
        channel = self.make_channel()
        try:
            self.seed_sentinel(channel)
            channel._handle_event(sentinel_event())

            counts = channel.effect_ledger.status_counts(channel.consumer_id)
            summary = {
                "EXTERNAL_DELIVERY_COUNT": len(self.delivered),
                "DUPLICATE_SUPPRESSED": "YES" if counts.get(STATE_DELIVERED) == 1 else "NO",
                "LEDGER_ROW_NOT_RECREATED": "YES"
                if channel.effect_ledger.count_effects(channel.consumer_id) == 1
                else "NO",
            }
            self.assertEqual(
                summary,
                {
                    "EXTERNAL_DELIVERY_COUNT": 0,
                    "DUPLICATE_SUPPRESSED": "YES",
                    "LEDGER_ROW_NOT_RECREATED": "YES",
                },
            )
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
