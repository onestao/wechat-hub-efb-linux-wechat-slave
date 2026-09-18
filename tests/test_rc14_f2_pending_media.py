from __future__ import annotations

import shutil
import sys
import time
import unittest
import uuid
from pathlib import Path

TESTS = Path(__file__).resolve().parent
if str(TESTS) not in sys.path:
    sys.path.insert(0, str(TESTS))

from stub_ehforwarderbot import install_stubs

install_stubs()

from efb_wechat_comwechat_slave.ComWechat import LinuxWeChatChannel
from efb_wechat_comwechat_slave.Core import CoreAPIError, CoreMedia
from efb_wechat_comwechat_slave.EffectLedger import (
    STATE_DELIVERED,
    STATE_MEDIA_FAILED,
    STATE_PENDING_MEDIA,
)


class DelayedMediaCore:
    def __init__(self) -> None:
        self.ready = False
        self.media_calls = 0

    def get_media(self, account_id: str, media_id: str) -> CoreMedia:
        self.media_calls += 1
        if not self.ready:
            raise CoreAPIError(404, "media_not_found", f"{media_id} is not ready")
        return CoreMedia(
            b"REAL-STICKER",
            "image/webp",
            "sticker.webp",
            media_id,
            "original",
            "ready",
        )

    def health(self):
        return {"contract_version": 1}

    # -- governed bootstrap provenance (Core V1, part of the F3 contract) -----
    # The unknown-identity hardening classifies an event whose effect identity is not
    # yet in the ledger against the durable subscription floor. Real Core always
    # answers both reads below; the double does too so that these synthetic messages
    # model new business rather than an unresolvable provenance read.
    def get_bootstrap_provenance(self, consumer_id: str):
        return {
            "consumer_id": consumer_id,
            "initial_cursor": 0,
            "bootstrap_mode": "at_head",
            "bootstrap_at": "2026-01-01T00:00:00Z",
        }

    def get_message_projection(self, account_id, chat_id, message_id, **_kwargs):
        return {
            "account_id": account_id,
            "chat_id": chat_id,
            "message_id": message_id,
            "created_at": "2026-06-01T00:00:00Z",
        }


class F2PendingMediaStateMachineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.data_path = (
            Path(__file__).resolve().parents[1]
            / ".tmp"
            / f"rc14-f2-{uuid.uuid4().hex}"
        )
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.core = DelayedMediaCore()
        self.deliveries = []
        self.channel = self._channel()

    def tearDown(self) -> None:
        self.channel.stop_polling()
        shutil.rmtree(self.data_path, ignore_errors=True)

    def _channel(self, *, max_attempts: int = 5) -> LinuxWeChatChannel:
        channel = LinuxWeChatChannel(
            core_client=self.core,
            config={
                "startup_healthcheck": False,
                "shutdown_install_deferred": False,
                "consumer_id": "rc14-f2",
                "account_ids": ["account-1"],
                "media_retry_max_attempts": max_attempts,
                "media_retry_deadline_sec": 60,
                "media_retry_base_sec": 0,
                "media_retry_max_sec": 0,
                "core": {"poll_timeout": 0},
            },
            data_path=self.data_path,
        )
        channel.chat_mgr.build_core_chat(
            {
                "account_id": "account-1",
                "chat_id": "chat-1",
                "type": "private",
                "display_name": "Peer",
            },
            "Self",
        )

        def capture(message):
            content = message.file.read() if message.file is not None else b""
            self.deliveries.append((str(message.uid), message.type, content))
            if message.file is not None:
                message.file.close()

        channel._deliver_message = capture
        return channel

    @staticmethod
    def _message(status: str = "original_pending"):
        return {
            "account_id": "account-1",
            "chat_id": "chat-1",
            "message_id": "sticker-1",
            "type": "sticker",
            "direction": "incoming",
            "author": {
                "member_id": "peer-1",
                "display_name": "Peer",
                "is_self": False,
            },
            "media_id": "sticker-media-1",
            "media_role": "original",
            "media_status": status,
            "filename": "sticker.webp",
            "mime_type": "image/webp",
        }

    @classmethod
    def _created_event(cls):
        return {
            "event_type": "message.created",
            "account_id": "account-1",
            "payload": {"message": cls._message()},
        }

    @staticmethod
    def _ready_event():
        return {
            "event_type": "media.ready",
            "account_id": "account-1",
            "payload": {
                "media": {
                    "media_id": "sticker-media-1",
                    "role": "original",
                    "status": "ready",
                    "filename": "sticker.webp",
                    "mime_type": "image/webp",
                }
            },
        }

    def _effect_status(self) -> str:
        return self.channel.effect_ledger.get_effect_status(
            self.channel.consumer_id,
            "account-1:sticker-1",
        )

    def test_f2_1_event_first_media_ready_later(self) -> None:
        self.channel._handle_event(self._created_event())
        self.assertEqual(STATE_PENDING_MEDIA, self._effect_status())
        self.assertEqual([], self.deliveries)

        self.core.ready = True
        self.channel._handle_event(self._ready_event())

        self.assertEqual(STATE_DELIVERED, self._effect_status())
        self.assertEqual([("sticker-1", self.deliveries[0][1], b"REAL-STICKER")], self.deliveries)

    def test_f2_2_restart_preserves_pending_media(self) -> None:
        self.channel._handle_event(self._created_event())
        self.assertEqual(STATE_PENDING_MEDIA, self._effect_status())

        restarted = self._channel()
        self.channel = restarted
        self.assertEqual(STATE_PENDING_MEDIA, self._effect_status())
        self.core.ready = True
        restarted._handle_event(self._ready_event())

        self.assertEqual(STATE_DELIVERED, self._effect_status())
        self.assertEqual(1, len(self.deliveries))

    def test_f2_3_media_ready_delivers_exactly_once(self) -> None:
        self.channel._handle_event(self._created_event())
        self.core.ready = True

        self.channel._handle_event(self._ready_event())
        self.channel._handle_event(self._ready_event())
        self.channel._handle_event(
            {
                "event_type": "message.updated",
                "account_id": "account-1",
                "payload": {"message": self._message("ready")},
            }
        )

        self.assertEqual(1, len(self.deliveries))
        self.assertEqual(STATE_DELIVERED, self._effect_status())

    def test_f2_4_media_never_ready_parks_as_recoverable(self) -> None:
        """DELIBERATE CONTRACT CHANGE -- defect R14-EFB-MISSING-MEDIA-TERMINAL.

        This test previously asserted that exhausting the retry budget produced a
        terminal ``MEDIA_FAILED``. That assertion *was* the defect: Core materialises
        media asynchronously, so "not ready yet" was being recorded as "never", which
        permanently closed the recovery path and made every later authoritative update
        for the object a ``SUPPRESSED_TERMINAL`` no-op. The expectation is inverted on
        purpose: the effect is parked as recoverable and stays non-terminal.
        """
        self.channel.stop_polling()
        self.channel = self._channel(max_attempts=2)
        self.channel._handle_event(self._created_event())

        self.channel._retry_pending_media()

        self.assertEqual(STATE_PENDING_MEDIA, self._effect_status())
        self.assertEqual([], self.deliveries)
        effect = self.channel.effect_ledger.get_effect(
            self.channel.consumer_id,
            "account-1:sticker-1",
        )
        self.assertEqual(2, effect["details"]["attempt_count"])
        self.assertIn("retry exhausted", effect["details"]["parked_reason"])
        self.assertIs(True, effect["details"]["retry_parked"])
        # Nothing was terminalised, so the historical terminal count is untouched.
        self.assertEqual(
            0,
            self.channel.effect_ledger.count_effects(
                self.channel.consumer_id, status=STATE_MEDIA_FAILED
            ),
        )

        # The parked effect is gone from the *scheduled* view even for a deadline far
        # beyond its (deliberately far-future) next_retry_at -- proving the durable
        # flag, not just the timestamp, is what stops the busy retry.
        self.assertEqual(
            [],
            self.channel.effect_ledger.pending_media(
                self.channel.consumer_id, due_before=time.time() + 10**9
            ),
        )

        # ...yet an authoritative media.ready still recovers it, exactly once.
        self.core.ready = True
        self.channel._handle_event(self._ready_event())
        self.assertEqual(STATE_DELIVERED, self._effect_status())
        self.assertEqual(1, len(self.deliveries))

    def test_f2_5_retries_do_not_duplicate_telegram_delivery(self) -> None:
        self.channel._handle_event(self._created_event())
        self.channel._retry_pending_media()
        self.channel._retry_pending_media()
        self.assertEqual([], self.deliveries)

        self.core.ready = True
        self.channel._handle_event(self._ready_event())
        self.channel._retry_pending_media()
        self.channel._handle_event(self._ready_event())

        self.assertEqual(1, len(self.deliveries))
        self.assertEqual(STATE_DELIVERED, self._effect_status())


if __name__ == "__main__":
    unittest.main()
