"""EFB Replay Safety, Governed Bootstrap Alignment, and Lifecycle Shutdown tests.

Governing documents:
  - docs/RC14_OPTIONAL_EFB_PRODUCTION_LIVE_QUALIFICATION_RETRY1_RESULT.md
  - docs/RC14_NEW_CONSUMER_BOOTSTRAP_POLICY.md

Covers:
  1. Durable EffectLedger unit tests (WAL mode, persistence across reopen, account isolation)
  2. Duplicate Core message.created emissions absorbed with zero duplicate deliveries (D5)
  3. Restart zero duplicate deliveries across channel lifecycles (Rule P3)
  4. Crash after delivery before cursor save produces zero duplicate sends
  5. Cold start bootstrap alignment (sets stream head, never defaults to cursor 0) (D1, D4, Rule P0)
  6. Re-bootstrap remediation for failed qualification checkpoint 13181 (D2)
  7. Multi-account UID isolation (acc-1:msg-1 vs acc-2:msg-1)
  8. Graceful shutdown under 2.0 seconds with long-poll interruption (D3, no Exit 137)
  9. Zero real network calls or external Telegram calls (isolated test harnesses)
"""

from __future__ import annotations

import copy
import http.server
import json
import tempfile
import threading
import time
import unittest
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest import mock

try:
    import ehforwarderbot
except ImportError:
    import tests.stub_ehforwarderbot as _stub
    _stub.install_stubs()

from ehforwarderbot import Message, MsgType, coordinator
from ehforwarderbot.chat import Chat
from ehforwarderbot.types import ChatID, InstanceID, MessageID

from efb_wechat_comwechat_slave.ComWechat import LinuxWeChatChannel
from efb_wechat_comwechat_slave.Core import CoreClient, CursorStore
from efb_wechat_comwechat_slave.EffectLedger import (
    BLOCKED_UNCERTAIN_EFFECT,
    DELIVERY_SEMANTICS,
    STATE_DELIVERED,
    STATE_RESERVED,
    STATE_UNCERTAIN,
    EffectLedger,
)

# Subscription anchor / business origin for this suite's Core double. Real Core V1 (F3)
# reports both from its governed bootstrap provenance and embeds the projection
# timestamp in every message event. These fixtures were written before the
# unknown-identity hardening and carry neither, so the double supplies an anchor in the
# past and an origin after it: every synthetic message in this suite models genuinely
# new business, which is what each of these tests already assumed.
BOOTSTRAP_AT = "2026-01-01T00:00:00Z"
PROJECTION_CREATED_AT = "2026-06-01T00:00:00Z"


class TestEffectLedgerUnit(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "effect-ledger.sqlite3"
        self.ledger = EffectLedger(self.db_path)

    def tearDown(self) -> None:
        self.ledger.close()
        self.temp_dir.cleanup()

    def test_record_and_check_effect(self) -> None:
        consumer = "efb-linux-wechat:wechat.linux"
        effect_id = self.ledger.compute_effect_id("acc-alpha", "msg-001")
        self.assertEqual(effect_id, "acc-alpha:msg-001")

        # Initially not delivered
        self.assertFalse(self.ledger.is_effect_delivered(consumer, effect_id))
        self.assertFalse(self.ledger.is_message_delivered(consumer, "acc-alpha", "msg-001"))

        # Record delivered
        inserted = self.ledger.record_delivered(
            consumer,
            effect_id,
            account_id="acc-alpha",
            message_id="msg-001",
            efb_uid="efb-uid-001",
            details={"chat_id": "c1"},
        )
        self.assertTrue(inserted)

        # Now delivered
        self.assertTrue(self.ledger.is_effect_delivered(consumer, effect_id))
        self.assertTrue(self.ledger.is_message_delivered(consumer, "acc-alpha", "msg-001"))
        self.assertEqual(self.ledger.count_effects(consumer), 1)

        # Duplicate recording is idempotent
        dup_insert = self.ledger.record_delivered(
            consumer,
            effect_id,
            account_id="acc-alpha",
            message_id="msg-001",
            efb_uid="efb-uid-001",
        )
        self.assertEqual(self.ledger.count_effects(consumer), 1)

    def test_persistence_across_reopen(self) -> None:
        consumer = "efb-linux-wechat:wechat.linux"
        effect_id = self.ledger.compute_effect_id("acc-alpha", "msg-persistent")
        self.ledger.record_delivered(
            consumer,
            effect_id,
            account_id="acc-alpha",
            message_id="msg-persistent",
            efb_uid="uid-p",
        )
        self.ledger.close()

        # Reopen with a new instance pointing to the exact same file
        ledger2 = EffectLedger(self.db_path)
        try:
            self.assertTrue(ledger2.is_effect_delivered(consumer, effect_id))
            rec = ledger2.get_effect(consumer, effect_id)
            self.assertIsNotNone(rec)
            self.assertEqual(rec["message_id"], "msg-persistent")
            self.assertEqual(rec["efb_uid"], "uid-p")
        finally:
            ledger2.close()

    def test_multi_account_isolation(self) -> None:
        consumer = "efb-linux-wechat:wechat.linux"
        eid_alpha = self.ledger.compute_effect_id("acc-alpha", "same-msg-id")
        eid_beta = self.ledger.compute_effect_id("acc-beta", "same-msg-id")

        self.assertNotEqual(eid_alpha, eid_beta)

        self.ledger.record_delivered(
            consumer, eid_alpha, account_id="acc-alpha", message_id="same-msg-id"
        )
        self.assertTrue(self.ledger.is_message_delivered(consumer, "acc-alpha", "same-msg-id"))
        self.assertFalse(self.ledger.is_message_delivered(consumer, "acc-beta", "same-msg-id"))

    def test_state_machine_transitions(self) -> None:
        consumer = "efb-linux-wechat:wechat.linux"
        eid = self.ledger.compute_effect_id("acc-alpha", "msg-sm-1")

        # 1. Initially unseen
        self.assertIsNone(self.ledger.get_effect_status(consumer, eid))

        # 2. Reserve effect
        reserved, status = self.ledger.reserve_effect(
            consumer, eid, account_id="acc-alpha", message_id="msg-sm-1"
        )
        self.assertTrue(reserved)
        self.assertEqual(status, STATE_RESERVED)
        self.assertEqual(self.ledger.get_effect_status(consumer, eid), STATE_RESERVED)

        # 3. Duplicate reservation while RESERVED fails closed
        dup_reserved, dup_status = self.ledger.reserve_effect(
            consumer, eid, account_id="acc-alpha", message_id="msg-sm-1"
        )
        self.assertFalse(dup_reserved)
        self.assertEqual(dup_status, BLOCKED_UNCERTAIN_EFFECT)

        # 4. Transition to DELIVERED
        marked = self.ledger.mark_delivered(consumer, eid, efb_uid="uid-delivered")
        self.assertTrue(marked)
        self.assertEqual(self.ledger.get_effect_status(consumer, eid), STATE_DELIVERED)
        self.assertTrue(self.ledger.is_effect_delivered(consumer, eid))

        # 5. Subsequent reservation fails with DELIVERED
        after_reserved, after_status = self.ledger.reserve_effect(
            consumer, eid, account_id="acc-alpha", message_id="msg-sm-1"
        )
        self.assertFalse(after_reserved)
        self.assertEqual(after_status, STATE_DELIVERED)

    def test_reconcile_on_startup_transitions_reserved_to_uncertain(self) -> None:
        consumer = "efb-linux-wechat:wechat.linux"
        eid_1 = self.ledger.compute_effect_id("acc-alpha", "msg-crash-1")
        eid_2 = self.ledger.compute_effect_id("acc-alpha", "msg-delivered-2")

        # Effect 1 was left in RESERVED (simulating process crash during external call)
        self.ledger.reserve_effect(consumer, eid_1, account_id="acc-alpha", message_id="msg-crash-1")
        # Effect 2 was properly finalized to DELIVERED
        self.ledger.reserve_effect(consumer, eid_2, account_id="acc-alpha", message_id="msg-delivered-2")
        self.ledger.mark_delivered(consumer, eid_2, efb_uid="uid-2")

        # Simulate startup reconciliation
        reconciled = self.ledger.reconcile_on_startup(consumer)
        self.assertEqual(reconciled, [eid_1])

        # Effect 1 must now be UNCERTAIN and fail closed
        self.assertEqual(self.ledger.get_effect_status(consumer, eid_1), STATE_UNCERTAIN)
        rec1 = self.ledger.get_effect(consumer, eid_1)
        self.assertEqual(rec1["details"]["blocked_reason"], BLOCKED_UNCERTAIN_EFFECT)

        # Effect 2 remains DELIVERED
        self.assertEqual(self.ledger.get_effect_status(consumer, eid_2), STATE_DELIVERED)

    def test_wal_full_synchronous_durability(self) -> None:
        """Verify WAL mode and synchronous=FULL durability settings for power-loss safety."""
        with self.ledger._connection() as conn:
            journal_mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
            sync_mode = conn.execute("PRAGMA synchronous;").fetchone()[0]
            self.assertEqual(journal_mode.lower(), "wal")
            # In SQLite, PRAGMA synchronous = 2 corresponds to FULL
            self.assertEqual(int(sync_mode), 2)


class MockCoreClientForEFB(CoreClient):
    """Deterministic mock CoreClient avoiding external network sockets."""

    def __init__(self, base_url: str = "http://127.0.0.1:8080") -> None:
        super().__init__(base_url)
        self.accounts: List[Dict[str, Any]] = [
            {
                "account_id": "acc-1",
                "display_name": "Account One",
                "state": "online",
            }
        ]
        self.chats: List[Dict[str, Any]] = [
            {
                "account_id": "acc-1",
                "chat_id": "chat-1",
                "type": "private",
                "display_name": "User One",
            }
        ]
        self.events_queue: List[Dict[str, Any]] = []
        self.checkpoints: Dict[str, int] = {}
        self.bootstrap_records: Dict[str, Dict[str, Any]] = {}
        self.stream_head = 0

    def health(self) -> Dict[str, Any]:
        return {"contract_version": 1, "status": "ok"}

    def list_accounts(self) -> List[Dict[str, Any]]:
        return list(self.accounts)

    def list_chats(self, account_id: str, *, query: str = "", limit: int = 200) -> List[Dict[str, Any]]:
        return [c for c in self.chats if c["account_id"] == account_id]

    def get_bootstrap_provenance(self, consumer_id: str) -> Optional[Dict[str, Any]]:
        return self.bootstrap_records.get(consumer_id)

    # -- governed bootstrap provenance / authoritative projection (Core V1 F3) --
    # The unknown-identity hardening classifies an event whose effect identity is not
    # yet in the ledger against the durable subscription floor, using Core's
    # authoritative business origin. Real Core embeds `created_at` in every message
    # event; these fixtures predate that and omit it, so the double answers the
    # authoritative read instead of leaving the origin unresolvable.
    def get_message_projection(
        self,
        account_id: str,
        chat_id: str,
        message_id: str,
        **_kwargs: Any,
    ) -> Dict[str, Any]:
        return {
            "account_id": account_id,
            "chat_id": chat_id,
            "message_id": message_id,
            "created_at": PROJECTION_CREATED_AT,
        }

    def bootstrap_consumer(
        self,
        consumer_id: str,
        *,
        mode: str = "at_head",
        window: Optional[Dict[str, Any]] = None,
        operator_token: str = "",
    ) -> Dict[str, Any]:
        init_cursor = self.stream_head
        if mode == "bounded_window" and window and "events" in window:
            init_cursor = max(0, self.stream_head - int(window["events"]))
        record = {
            "ok": True,
            "consumer_id": consumer_id,
            "initial_cursor": init_cursor,
            "mode": mode,
            "bootstrap_mode": mode,
            "bootstrap_at": BOOTSTRAP_AT,
            "stream_head_cursor": self.stream_head,
            "audit_history": [{"action": "bootstrap", "initial_cursor": init_cursor, "mode": mode}],
        }
        self.bootstrap_records[consumer_id] = record
        self.checkpoints[consumer_id] = init_cursor
        return record

    def rebootstrap_consumer(
        self,
        consumer_id: str,
        *,
        mode: str = "bounded_window",
        window: Optional[Dict[str, Any]] = None,
        operator_token: str = "",
        quiescence_evidence: str = "",
        reason: str = "",
    ) -> Dict[str, Any]:
        init_cursor = self.stream_head
        if mode == "bounded_window" and window and "events" in window:
            init_cursor = max(0, self.stream_head - int(window["events"]))
        prev = self.checkpoints.get(consumer_id, 0)
        record = {
            "ok": True,
            "consumer_id": consumer_id,
            "initial_cursor": init_cursor,
            "previous_checkpoint": prev,
            "mode": mode,
            "bootstrap_mode": mode,
            "bootstrap_at": BOOTSTRAP_AT,
            "stream_head_cursor": self.stream_head,
        }
        self.bootstrap_records[consumer_id] = record
        self.checkpoints[consumer_id] = init_cursor
        return record

    def poll_events(
        self,
        *,
        after: str,
        consumer_id: str,
        timeout: int = 15,
        limit: int = 50,
        account_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        after_int = int(after or "0")
        available = [e for e in self.events_queue if int(e.get("cursor", 0)) > after_int]
        page_events = available[:limit]
        return {
            "events": page_events,
            "has_more": len(available) > limit,
            "stream_head_cursor": self.stream_head,
            "retention_floor_cursor": 0,
        }

    def checkpoint_events(
        self,
        consumer_id: str,
        processed_through_cursor: int,
        *,
        last_event_id: str = "",
        subscription_account_id: str = "",
    ) -> Dict[str, Any]:
        self.checkpoints[consumer_id] = processed_through_cursor
        return {"ok": True, "consumer_id": consumer_id, "processed_through_cursor": processed_through_cursor}

    def ack_events(self, consumer_id: str, event_ids: Any) -> Dict[str, Any]:
        return {"ok": True, "consumer_id": consumer_id, "acked_event_ids": list(event_ids)}


class TestEFBReplaySafetyAndLifecycle(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_path = Path(self.temp_dir.name)
        self.mock_core = MockCoreClientForEFB()
        self.delivered_messages: List[Message] = []

        # Mock coordinator to prevent real Telegram deliveries
        self.orig_master = getattr(coordinator, "master", None)
        self.orig_send_message = getattr(coordinator, "send_message", None)
        coordinator.master = mock.MagicMock()
        coordinator.send_message = self.delivered_messages.append

    def tearDown(self) -> None:
        coordinator.master = self.orig_master
        coordinator.send_message = self.orig_send_message
        self.temp_dir.cleanup()

    def _create_channel(self) -> LinuxWeChatChannel:
        return LinuxWeChatChannel(
            core_client=self.mock_core,
            config={
                "core": {"base_url": "http://mock-core", "timeout": 2, "poll_timeout": 0},
                "consumer_id": "efb-linux-wechat",
                "poll_interval": 0.01,
                "startup_healthcheck": True,
            },
            data_path=self.data_path,
        )

    def test_duplicate_message_created_events_absorbed(self) -> None:
        """Defect D5: absorbing Core duplicate message.created emissions."""
        channel = self._create_channel()
        try:
            # Seed duplicate message.created events with identical account_id and message_id
            ev1 = {
                "event_id": "ev-1",
                "cursor": 1,
                "event_type": "message.created",
                "account_id": "acc-1",
                "payload": {
                    "message": {
                        "message_id": "core-msg-100",
                        "chat_id": "chat-1",
                        "text": "Hello World",
                        "type": "text",
                        "direction": "incoming",
                        "author": {
                            "member_id": "peer-1",
                            "display_name": "Peer",
                            "is_self": False,
                        },
                    }
                },
            }
            ev2 = copy.deepcopy(ev1)
            ev2["event_id"] = "ev-2"
            ev2["cursor"] = 2

            ev3 = copy.deepcopy(ev1)
            ev3["event_id"] = "ev-3"
            ev3["cursor"] = 3

            self.mock_core.stream_head = 3
            self.mock_core.events_queue = [ev1, ev2, ev3]

            # Poll events
            processed = channel.poll_once()
            self.assertEqual(processed, 3)

            # Exactly 1 message must have been delivered to coordinator
            self.assertEqual(len(self.delivered_messages), 1)
            self.assertEqual(self.delivered_messages[0].text, "Hello World")

            # Effect ledger must reflect delivery
            effect_id = channel.effect_ledger.compute_effect_id("acc-1", "core-msg-100")
            self.assertTrue(channel.effect_ledger.is_effect_delivered(channel.consumer_id, effect_id))
            self.assertEqual(channel.effect_ledger.count_effects(channel.consumer_id), 1)
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()

    def test_restart_zero_duplicate_deliveries(self) -> None:
        """Rule P3: EFB restart must produce ZERO duplicate deliveries."""
        # --- Instance 1 ---
        channel1 = self._create_channel()
        ev1 = {
            "event_id": "ev-1",
            "cursor": 1,
            "event_type": "message.created",
            "account_id": "acc-1",
            "payload": {
                "message": {
                    "message_id": "msg-1", "chat_id": "chat-1", "text": "Msg 1", "type": "text",
                    "direction": "incoming",
                    "author": {"member_id": "peer-1", "display_name": "Peer", "is_self": False},
                }
            },
        }
        ev2 = {
            "event_id": "ev-2",
            "cursor": 2,
            "event_type": "message.created",
            "account_id": "acc-1",
            "payload": {
                "message": {
                    "message_id": "msg-2", "chat_id": "chat-1", "text": "Msg 2", "type": "text",
                    "direction": "incoming",
                    "author": {"member_id": "peer-1", "display_name": "Peer", "is_self": False},
                }
            },
        }
        self.mock_core.stream_head = 2
        self.mock_core.events_queue = [ev1, ev2]

        channel1.poll_once()
        self.assertEqual(len(self.delivered_messages), 2)
        channel1.stop_polling()
        channel1.effect_ledger.close()

        # --- Instance 2 (restart over exact same data directory) ---
        channel2 = self._create_channel()
        try:
            # Simulate Core replaying events from cursor 0 (e.g. checkpoint lag or re-read)
            # Channel 2 poll_once() processes them again
            channel2.cursor_store.save("0")  # force re-read
            channel2.poll_once()

            # Result: zero additional deliveries to coordinator! Still exactly 2!
            self.assertEqual(len(self.delivered_messages), 2)
        finally:
            channel2.stop_polling()
            channel2.effect_ledger.close()

    def test_crash_after_delivery_before_cursor_save(self) -> None:
        """Crash scenario: effect ledger recorded, but cursor store not updated."""
        channel = self._create_channel()
        try:
            ev = {
                "event_id": "ev-crash",
                "cursor": 10,
                "event_type": "message.created",
                "account_id": "acc-1",
                "payload": {
                    "message": {
                        "message_id": "msg-crash", "chat_id": "chat-1", "text": "Pre-crash", "type": "text",
                        "direction": "incoming",
                        "author": {"member_id": "peer-1", "display_name": "Peer", "is_self": False},
                    }
                },
            }
            # Deliver directly
            channel._handle_event(ev)
            self.assertEqual(len(self.delivered_messages), 1)

            # Local cursor was not advanced to 10 (simulating crash before cursor save)
            self.assertNotEqual(channel.cursor_store.load(), "10")

            # Simulate recovery / re-delivery of the same event
            channel._handle_event(ev)
            # Delivery was suppressed by effect ledger!
            self.assertEqual(len(self.delivered_messages), 1)
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()

    def test_cold_start_bootstrap_at_head_alignment(self) -> None:
        """Defect D1 & D4: cold start must align with Core stream head and NOT default to 0."""
        # Core is at stream head 272000
        self.mock_core.stream_head = 272000

        # Fresh channel without existing cursor file
        channel = self._create_channel()
        try:
            aligned = channel.cursor_store.load()
            # Local cursor must be aligned to stream head (272000), never "0"
            self.assertEqual(aligned, "272000")

            # Polling starts from 272000
            self.mock_core.events_queue = [
                {
                    "event_id": "ev-new",
                    "cursor": 272001,
                    "event_type": "message.created",
                    "account_id": "acc-1",
                    "payload": {
                        "message": {
                            "message_id": "msg-new", "chat_id": "chat-1", "text": "New", "type": "text",
                            "direction": "incoming",
                            "author": {"member_id": "peer-1", "display_name": "Peer", "is_self": False},
                        }
                    },
                }
            ]
            processed = channel.poll_once()
            self.assertEqual(processed, 1)
            self.assertEqual(channel.cursor_store.load(), "272001")
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()

    def test_rebootstrap_failed_checkpoint_13181_remediation(self) -> None:
        """Defect D2: re-bootstrap remediation for failed qualification checkpoint 13181."""
        # Preserve local cursor 13214 as found in production
        cursor_file = self.data_path / "core-event-cursor.json"
        cursor_file.write_text(json.dumps({"cursor": "13214"}), encoding="utf-8")

        # Mock Core has preserved checkpoint 13181 and current stream head 272000
        self.mock_core.stream_head = 272000
        self.mock_core.checkpoints["efb-linux-wechat:wechat.linux"] = 13181
        self.mock_core.bootstrap_records["efb-linux-wechat:wechat.linux"] = {
            "initial_cursor": 0,
            "mode": "legacy_unbounded",
            "bootstrap_mode": "legacy_unbounded",
            "bootstrap_at": BOOTSTRAP_AT,
        }

        # Operator triggers rebootstrap to bounded window (e.g. 500 events from head)
        re_res = self.mock_core.rebootstrap_consumer(
            "efb-linux-wechat:wechat.linux",
            mode="bounded_window",
            window={"events": 500},
            operator_token="QUAL-RETRY2-TOKEN",
            quiescence_evidence="EFB stopped, local cursor 13214 preserved",
        )
        self.assertEqual(re_res["initial_cursor"], 271500)

        channel = self._create_channel()
        try:
            # Channel must align its local cursor to the new initial cursor 271500
            cur = channel.cursor_store.align_with_core(self.mock_core, channel.consumer_id)
            self.assertEqual(cur, "271500")
            self.assertEqual(channel.cursor_store.load(), "271500")
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()


class HangingPollHandler(http.server.BaseHTTPRequestHandler):
    """HTTP handler that blocks on /v1/events/poll to simulate long-polling."""

    def do_GET(self) -> None:
        if self.path.startswith("/health"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"contract_version": 1, "status": "ok"}).encode("utf-8"))
            return
        if self.path.startswith("/v1/events/poll"):
            parsed = urllib.parse.urlparse(self.path)
            query = urllib.parse.parse_qs(parsed.query)
            timeout = min(float(query.get("timeout", [15])[0]), 15.0)
            time.sleep(timeout)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"events": [], "stream_head_cursor": 0}).encode("utf-8"))
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        pass


class TestEFBGracefulShutdown(unittest.TestCase):
    def setUp(self) -> None:
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), HangingPollHandler)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_address[1]}"

        self.temp_dir = tempfile.TemporaryDirectory()
        self.channel = LinuxWeChatChannel(
            core_client=CoreClient(self.base_url, timeout=10.0),
            config={
                "core": {"base_url": self.base_url, "timeout": 10.0, "poll_timeout": 15},
                "poll_interval": 0.05,
                "startup_healthcheck": True,
            },
            data_path=Path(self.temp_dir.name),
        )

    def tearDown(self) -> None:
        self.channel.stop_polling()
        self.channel.effect_ledger.close()
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=2)
        self.temp_dir.cleanup()

    def test_graceful_shutdown_under_two_seconds(self) -> None:
        """Defect D3: polling must abort immediately on stop_polling (< 2.0s), preventing Exit 137."""
        poll_thread = threading.Thread(target=self.channel.poll, daemon=True)
        poll_thread.start()

        # Allow poll thread to enter the blocking 15-second requests.get call
        time.sleep(0.3)
        self.assertTrue(poll_thread.is_alive())

        start_time = time.monotonic()
        # Trigger stop_polling (which closes requests.session)
        self.channel.stop_polling()
        poll_thread.join(timeout=2.0)
        elapsed = time.monotonic() - start_time

        # Poll thread MUST have terminated in under 2.0 seconds
        self.assertFalse(poll_thread.is_alive(), f"Poll thread did not terminate within 2s; took {elapsed:.2f}s")
        self.assertLess(elapsed, 2.0)


class TestDeterministicFailureInjection(unittest.TestCase):
    """Deterministic failure injection tests covering all 6 mandatory failure scenarios."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_path = Path(self.temp_dir.name)
        self.mock_core = MockCoreClientForEFB()
        self.delivered_messages: List[Message] = []

        self.orig_master = getattr(coordinator, "master", None)
        self.orig_send_message = getattr(coordinator, "send_message", None)
        coordinator.master = mock.MagicMock()
        coordinator.send_message = self.delivered_messages.append

    def tearDown(self) -> None:
        coordinator.master = self.orig_master
        coordinator.send_message = self.orig_send_message
        self.temp_dir.cleanup()

    def _create_channel(self) -> LinuxWeChatChannel:
        return LinuxWeChatChannel(
            core_client=self.mock_core,
            config={
                "core": {"base_url": "http://mock-core", "timeout": 5.0},
                "poll_interval": 0.05,
                "startup_healthcheck": False,
            },
            data_path=self.data_path,
        )

    def test_scenario_1_crash_before_reservation(self) -> None:
        """Scenario 1: Crash before reservation. No ledger row created; redelivery delivers normally."""
        channel = self._create_channel()
        ev = {
            "event_id": "ev-sc1",
            "cursor": 100,
            "event_type": "message.created",
            "account_id": "acc-1",
            "payload": {
                "message": {
                    "message_id": "msg-sc1", "chat_id": "chat-1", "text": "Sc1 test", "type": "text",
                    "direction": "incoming",
                    "author": {"member_id": "peer-1", "display_name": "Peer", "is_self": False},
                }
            },
        }

        class InjectedCrash(Exception):
            pass

        with mock.patch.object(channel.effect_ledger, "reserve_effect", side_effect=InjectedCrash("Simulated crash before reservation")):
            with self.assertRaises(InjectedCrash):
                channel._handle_event(ev)

        self.assertEqual(len(self.delivered_messages), 0)
        effect_id = channel.effect_ledger.compute_effect_id("acc-1", "msg-sc1")
        self.assertIsNone(channel.effect_ledger.get_effect_status(channel.consumer_id, effect_id))

        channel.stop_polling()
        channel.effect_ledger.close()

        channel_restarted = self._create_channel()
        try:
            channel_restarted._handle_event(ev)
            self.assertEqual(len(self.delivered_messages), 1)
            self.assertEqual(
                channel_restarted.effect_ledger.get_effect_status(channel_restarted.consumer_id, effect_id),
                STATE_DELIVERED,
            )
        finally:
            channel_restarted.stop_polling()
            channel_restarted.effect_ledger.close()

    def test_scenario_2_crash_after_reservation_before_external_call(self) -> None:
        """Scenario 2: Crash after reservation before external call.
        
        Reservation was persisted (RESERVED). Process crashed before _deliver_message.
        On restart, reconciliation marks RESERVED -> UNCERTAIN.
        When event is redelivered, send is suppressed (fail-closed, BLOCKED_UNCERTAIN_EFFECT).
        Zero external deliveries occur!
        """
        channel = self._create_channel()
        ev = {
            "event_id": "ev-sc2",
            "cursor": 200,
            "event_type": "message.created",
            "account_id": "acc-1",
            "payload": {
                "message": {
                    "message_id": "msg-sc2", "chat_id": "chat-1", "text": "Sc2 test", "type": "text",
                    "direction": "incoming",
                    "author": {"member_id": "peer-1", "display_name": "Peer", "is_self": False},
                }
            },
        }

        class InjectedCrash(Exception):
            pass

        with mock.patch.object(channel, "_deliver_message", side_effect=InjectedCrash("Crash before external delivery")):
            with self.assertRaises(InjectedCrash):
                channel._handle_event(ev)

        self.assertEqual(len(self.delivered_messages), 0)
        effect_id = channel.effect_ledger.compute_effect_id("acc-1", "msg-sc2")
        self.assertEqual(
            channel.effect_ledger.get_effect_status(channel.consumer_id, effect_id),
            STATE_UNCERTAIN,
        )

        channel.stop_polling()
        channel.effect_ledger.close()

        channel_restarted = self._create_channel()
        try:
            channel_restarted._handle_event(ev)
            self.assertEqual(len(self.delivered_messages), 0)
        finally:
            channel_restarted.stop_polling()
            channel_restarted.effect_ledger.close()

    def test_scenario_3_crash_immediately_after_external_delivery_before_ledger_finalize(self) -> None:
        """Scenario 3: CRUCIAL CRASH WINDOW.
        
        External delivery succeeded, but process crashed before mark_delivered executed!
        On disk, effect ledger is in RESERVED state.
        On restart, reconciliation transitions RESERVED to UNCERTAIN (BLOCKED_UNCERTAIN_EFFECT).
        When Core re-delivers the event (e.g. cursor lag), EFB suppresses re-sending.
        Guarantees ZERO DUPLICATE DELIVERIES to external master!
        """
        channel = self._create_channel()
        ev = {
            "event_id": "ev-sc3",
            "cursor": 300,
            "event_type": "message.created",
            "account_id": "acc-1",
            "payload": {
                "message": {
                    "message_id": "msg-sc3", "chat_id": "chat-1", "text": "Sc3 test", "type": "text",
                    "direction": "incoming",
                    "author": {"member_id": "peer-1", "display_name": "Peer", "is_self": False},
                }
            },
        }

        class ProcessCrashBeforeFinalize(Exception):
            pass

        with mock.patch.object(channel.effect_ledger, "mark_delivered", side_effect=ProcessCrashBeforeFinalize("Power loss before finalize")):
            with self.assertRaises(ProcessCrashBeforeFinalize):
                channel._handle_event(ev)

        self.assertEqual(len(self.delivered_messages), 1)

        effect_id = channel.effect_ledger.compute_effect_id("acc-1", "msg-sc3")
        self.assertEqual(
            channel.effect_ledger.get_effect_status(channel.consumer_id, effect_id),
            STATE_RESERVED,
        )

        channel.stop_polling()
        channel.effect_ledger.close()

        # Restart after crash
        channel_restarted = self._create_channel()
        try:
            self.assertEqual(
                channel_restarted.effect_ledger.get_effect_status(channel_restarted.consumer_id, effect_id),
                STATE_UNCERTAIN,
            )

            # Core redelivers the event
            channel_restarted._handle_event(ev)

            # CRITICAL: delivered_messages count MUST STILL BE 1!
            self.assertEqual(len(self.delivered_messages), 1)
        finally:
            channel_restarted.stop_polling()
            channel_restarted.effect_ledger.close()

    def test_scenario_4_crash_after_delivered_before_cursor_save(self) -> None:
        """Scenario 4: Crash after DELIVERED before cursor save.
        
        Ledger holds DELIVERED. On restart, Core redelivers event from old cursor.
        Ledger absorbs duplicate cleanly. Delivery count remains exactly 1.
        """
        channel = self._create_channel()
        ev = {
            "event_id": "ev-sc4",
            "cursor": 400,
            "event_type": "message.created",
            "account_id": "acc-1",
            "payload": {
                "message": {
                    "message_id": "msg-sc4", "chat_id": "chat-1", "text": "Sc4 test", "type": "text",
                    "direction": "incoming",
                    "author": {"member_id": "peer-1", "display_name": "Peer", "is_self": False},
                }
            },
        }

        channel._handle_event(ev)
        self.assertEqual(len(self.delivered_messages), 1)
        effect_id = channel.effect_ledger.compute_effect_id("acc-1", "msg-sc4")
        self.assertEqual(
            channel.effect_ledger.get_effect_status(channel.consumer_id, effect_id),
            STATE_DELIVERED,
        )

        channel.stop_polling()
        channel.effect_ledger.close()

        channel_restarted = self._create_channel()
        try:
            channel_restarted._handle_event(ev)
            self.assertEqual(len(self.delivered_messages), 1)
        finally:
            channel_restarted.stop_polling()
            channel_restarted.effect_ledger.close()

    def test_scenario_5_duplicate_core_message_created(self) -> None:
        """Scenario 5: Core emits duplicate message.created events.
        
        Ledger absorbs duplicate in real-time. Delivery count is 1.
        """
        channel = self._create_channel()
        try:
            ev1 = {
                "event_id": "ev-sc5-1",
                "cursor": 501,
                "event_type": "message.created",
                "account_id": "acc-1",
                "payload": {
                    "message": {
                        "message_id": "msg-sc5", "chat_id": "chat-1", "text": "Sc5 dup", "type": "text",
                        "direction": "incoming",
                        "author": {"member_id": "peer-1", "display_name": "Peer", "is_self": False},
                    }
                },
            }
            ev2 = copy.deepcopy(ev1)
            ev2["event_id"] = "ev-sc5-2"
            ev2["cursor"] = 502

            channel._handle_event(ev1)
            self.assertEqual(len(self.delivered_messages), 1)

            channel._handle_event(ev2)
            self.assertEqual(len(self.delivered_messages), 1)
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()

    def test_scenario_6_restart_with_reserved_and_uncertain_states(self) -> None:
        """Scenario 6: Restart with pre-existing RESERVED and UNCERTAIN states.
        
        Reconciliation handles RESERVED -> UNCERTAIN transition.
        Both RESERVED and UNCERTAIN fail closed on event arrival.
        """
        ledger = EffectLedger(self.data_path / "core-effect-ledger.sqlite3")
        consumer = "efb-linux-wechat:wechat.linux"
        eid_res = ledger.compute_effect_id("acc-1", "msg-pre-res")
        eid_unc = ledger.compute_effect_id("acc-1", "msg-pre-unc")

        ledger.reserve_effect(consumer, eid_res, account_id="acc-1", message_id="msg-pre-res")
        ledger.reserve_effect(consumer, eid_unc, account_id="acc-1", message_id="msg-pre-unc")
        ledger.mark_uncertain(consumer, eid_unc, reason="Prior crash")
        ledger.close()

        channel = self._create_channel()
        try:
            self.assertEqual(channel.effect_ledger.get_effect_status(consumer, eid_res), STATE_UNCERTAIN)
            self.assertEqual(channel.effect_ledger.get_effect_status(consumer, eid_unc), STATE_UNCERTAIN)

            ev_res = {
                "event_id": "ev-res",
                "cursor": 601,
                "event_type": "message.created",
                "account_id": "acc-1",
                "payload": {"message": {
                    "message_id": "msg-pre-res", "chat_id": "c1", "text": "T", "type": "text",
                    "direction": "incoming",
                    "author": {"member_id": "peer-1", "display_name": "Peer", "is_self": False},
                }},
            }
            ev_unc = {
                "event_id": "ev-unc",
                "cursor": 602,
                "event_type": "message.created",
                "account_id": "acc-1",
                "payload": {"message": {
                    "message_id": "msg-pre-unc", "chat_id": "c1", "text": "T", "type": "text",
                    "direction": "incoming",
                    "author": {"member_id": "peer-1", "display_name": "Peer", "is_self": False},
                }},
            }

            channel._handle_event(ev_res)
            channel._handle_event(ev_unc)

            self.assertEqual(len(self.delivered_messages), 0)
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()


if __name__ == "__main__":
    unittest.main()
