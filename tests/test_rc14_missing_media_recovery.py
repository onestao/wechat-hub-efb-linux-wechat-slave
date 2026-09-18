"""Permanent regression tests for defect R14-EFB-MISSING-MEDIA-TERMINAL.

The Retry1 Final Production Preflight failed its section 6 rule because restoring
from the production checkpoint turned 12 post-floor objects whose media Core had not
yet materialised into *new* terminal ``MEDIA_FAILED`` rows (42 -> 54); the terminal
rows then suppressed every later authoritative update for those objects as
``SUPPRESSED_TERMINAL``. The messages became permanently undeliverable even though
nothing was wrong with them -- Core simply had not published the media by stream head.

The fix parks such an effect instead of terminalising it. These tests pin exactly the
behaviour the preflight gate measures:

    MISSING_FILE_REMAINS_RECOVERABLE       = YES
    MISSING_FILE_BLOCKS_CURSOR             = NO
    MISSING_FILE_BUSY_RETRY_LOOP           = NO
    NEW_MEDIA_FAILED_FROM_MISSING_FILE     = 0
    DELIVERY_AFTER_MEDIA_READY             = EXACTLY_ONCE
    DUPLICATE_DELIVERY_AFTER_SECOND_REPLAY = 0
    TERMINAL_EFFECT_REOPENED               = 0

and that genuinely unrecoverable media still fails closed, and that a historical
terminal row is never resurrected.
"""

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

ACCOUNT_ID = "account-1"
CHAT_ID = "chat-1"

#: (message_type, media_id, filename, mime_type, payload_bytes)
#: ``file`` models a real ``appmsg/type == "6"`` file, not a quote reply or link.
MEDIA_TYPES = (
    ("image", "img-media-1", "photo.jpg", "image/jpeg", b"REAL-IMAGE"),
    ("sticker", "sticker-media-1", "sticker.webp", "image/webp", b"REAL-STICKER"),
    ("file", "file-media-1", "report.pdf", "application/pdf", b"REAL-FILE"),
)


class NeverReadyCore:
    """Core double whose media endpoint is unavailable until ``ready`` is set.

    Models the observed production behaviour: Core publishes media into its ``media``
    table only once ``status='ready'``, so a not-yet-materialised ``media_id`` answers
    ``404``. It deliberately exposes no write method, so any attempt to mutate Core
    raises ``AttributeError`` rather than silently succeeding.
    """

    def __init__(self) -> None:
        self.ready = False
        self.media_calls = 0
        self.blobs = {media_id: blob for _, media_id, _, _, blob in MEDIA_TYPES}
        self.filenames = {media_id: name for _, media_id, name, _, _ in MEDIA_TYPES}
        self.mimes = {media_id: mime for _, media_id, _, mime, _ in MEDIA_TYPES}

    def get_media(self, account_id: str, media_id: str) -> CoreMedia:
        self.media_calls += 1
        if not self.ready:
            raise CoreAPIError(404, "media_not_found", f"{media_id} is not ready")
        return CoreMedia(
            self.blobs.get(media_id, b"REAL"),
            self.mimes.get(media_id, "application/octet-stream"),
            self.filenames.get(media_id, "media.bin"),
            media_id,
            "original",
            "ready",
        )

    def health(self):
        return {"contract_version": 1}

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


class MissingMediaNonTerminalRecoveryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.data_path = (
            Path(__file__).resolve().parents[1]
            / ".tmp"
            / f"rc14-mmr-{uuid.uuid4().hex}"
        )
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.core = NeverReadyCore()
        self.deliveries: list = []
        self.channel = self._channel()

    def tearDown(self) -> None:
        self.channel.stop_polling()
        shutil.rmtree(self.data_path, ignore_errors=True)

    # -- helpers ------------------------------------------------------------

    def _channel(self, *, max_attempts: int = 3) -> LinuxWeChatChannel:
        channel = LinuxWeChatChannel(
            core_client=self.core,
            config={
                "startup_healthcheck": False,
                "shutdown_install_deferred": False,
                "consumer_id": "rc14-mmr",
                "account_ids": [ACCOUNT_ID],
                "media_retry_max_attempts": max_attempts,
                "media_retry_deadline_sec": 3600,
                "media_retry_base_sec": 0,
                "media_retry_max_sec": 0,
                "core": {"poll_timeout": 0},
            },
            data_path=self.data_path,
        )
        channel.chat_mgr.build_core_chat(
            {
                "account_id": ACCOUNT_ID,
                "chat_id": CHAT_ID,
                "type": "private",
                "display_name": "Peer",
            },
            "Self",
        )

        def capture(message):
            content = message.file.read() if message.file is not None else b""
            self.deliveries.append((str(message.uid), str(message.type), content))
            if message.file is not None:
                message.file.close()

        channel._deliver_message = capture
        return channel

    @staticmethod
    def _message(msg_type: str, media_id: str, filename: str, mime: str, status: str):
        return {
            "account_id": ACCOUNT_ID,
            "chat_id": CHAT_ID,
            "message_id": f"msg-{media_id}",
            "type": msg_type,
            "direction": "incoming",
            "author": {
                "member_id": "peer-1",
                "display_name": "Peer",
                "is_self": False,
            },
            "media_id": media_id,
            "media_role": "original",
            "media_status": status,
            "filename": filename,
            "mime_type": mime,
        }

    def _created_event(self, msg_type, media_id, filename, mime, status="missing_file"):
        return {
            "event_type": "message.created",
            "account_id": ACCOUNT_ID,
            "payload": {
                "message": self._message(msg_type, media_id, filename, mime, status)
            },
        }

    @staticmethod
    def _ready_event(media_id: str, filename: str, mime: str):
        return {
            "event_type": "media.ready",
            "account_id": ACCOUNT_ID,
            "payload": {
                "media": {
                    "media_id": media_id,
                    "role": "original",
                    "status": "ready",
                    "filename": filename,
                    "mime_type": mime,
                }
            },
        }

    def _effect_id(self, media_id: str) -> str:
        return f"{ACCOUNT_ID}:msg-{media_id}"

    def _status(self, media_id: str):
        return self.channel.effect_ledger.get_effect_status(
            self.channel.consumer_id, self._effect_id(media_id)
        )

    def _exhaust(self) -> int:
        """Drive the active retry budget to exhaustion; return attempts made."""
        attempts = 0
        for _ in range(10):
            attempts += self.channel._retry_pending_media()
        return attempts

    # -- 1. park is non-terminal -------------------------------------------

    def test_missing_file_parks_recoverably_for_every_media_type(self) -> None:
        for msg_type, media_id, filename, mime, _blob in MEDIA_TYPES:
            with self.subTest(msg_type=msg_type):
                self.deliveries.clear()
                self.channel._handle_event(
                    self._created_event(msg_type, media_id, filename, mime)
                )
                self._exhaust()

                self.assertEqual(
                    STATE_PENDING_MEDIA,
                    self._status(media_id),
                    f"{msg_type} must stay non-terminal after retry exhaustion",
                )
                self.assertEqual([], self.deliveries)
                self.assertEqual(
                    0,
                    self.channel.effect_ledger.count_effects(
                        self.channel.consumer_id, status=STATE_MEDIA_FAILED
                    ),
                    f"{msg_type} must not create a new MEDIA_FAILED row",
                )
                effect = self.channel.effect_ledger.get_effect(
                    self.channel.consumer_id, self._effect_id(media_id)
                )
                self.assertIs(True, effect["details"]["retry_parked"])
                self.assertIn("retry exhausted", effect["details"]["parked_reason"])

    def test_exhausted_budget_is_not_retried_on_a_schedule(self) -> None:
        """MISSING_FILE_BUSY_RETRY_LOOP = NO."""
        msg_type, media_id, filename, mime, _blob = MEDIA_TYPES[0]
        self.channel._handle_event(
            self._created_event(msg_type, media_id, filename, mime)
        )
        self._exhaust()
        calls_after_park = self.core.media_calls

        # Repeated scheduled polls must not touch Core again.
        for _ in range(5):
            self.assertEqual(0, self.channel._retry_pending_media())
        self.assertEqual(calls_after_park, self.core.media_calls)

        # The parked row is excluded from the scheduled view even for a deadline far
        # beyond its deliberately far-future next_retry_at, which proves the durable
        # flag (not merely the timestamp) is what stops the retry.
        self.assertEqual(
            [],
            self.channel.effect_ledger.pending_media(
                self.channel.consumer_id, due_before=time.time() + 10**9
            ),
        )

    # -- 2. recovery after an authoritative signal -------------------------

    def test_media_ready_after_park_delivers_exactly_once(self) -> None:
        for msg_type, media_id, filename, mime, blob in MEDIA_TYPES:
            with self.subTest(msg_type=msg_type):
                self.deliveries.clear()
                # Each type starts from "Core has not published this media yet".
                self.core.ready = False
                self.channel._handle_event(
                    self._created_event(msg_type, media_id, filename, mime)
                )
                self._exhaust()
                self.assertEqual(STATE_PENDING_MEDIA, self._status(media_id))
                self.assertEqual([], self.deliveries)

                # Authoritative media becomes available.
                self.core.ready = True
                self.channel._handle_event(self._ready_event(media_id, filename, mime))

                self.assertEqual(STATE_DELIVERED, self._status(media_id))
                self.assertEqual(1, len(self.deliveries))
                self.assertEqual(blob, self.deliveries[0][2])

                # A second identical authoritative signal, plus a scheduled retry,
                # must not produce a second external effect.
                self.channel._handle_event(self._ready_event(media_id, filename, mime))
                self.channel._retry_pending_media()
                self.assertEqual(1, len(self.deliveries))
                self.assertEqual(STATE_DELIVERED, self._status(media_id))

    def test_duplicate_replay_after_delivery_is_suppressed(self) -> None:
        """DUPLICATE_DELIVERY_AFTER_SECOND_REPLAY = 0."""
        msg_type, media_id, filename, mime, _blob = MEDIA_TYPES[2]
        self.channel._handle_event(
            self._created_event(msg_type, media_id, filename, mime)
        )
        self._exhaust()
        self.core.ready = True
        self.channel._handle_event(self._ready_event(media_id, filename, mime))
        self.assertEqual(1, len(self.deliveries))

        # Replay the original creation and the ready event several times over.
        for _ in range(3):
            self.channel._handle_event(
                self._created_event(msg_type, media_id, filename, mime, status="ready")
            )
            self.channel._handle_event(self._ready_event(media_id, filename, mime))
        self.assertEqual(1, len(self.deliveries))
        self.assertEqual(STATE_DELIVERED, self._status(media_id))

    # -- 3. restart safety --------------------------------------------------

    def test_restart_after_park_does_not_rearm_the_budget(self) -> None:
        """DUPLICATE_EXTERNAL_EFFECT_AFTER_RESTART = 0, budget stays exhausted."""
        msg_type, media_id, filename, mime, blob = MEDIA_TYPES[1]
        self.channel._handle_event(
            self._created_event(msg_type, media_id, filename, mime)
        )
        self._exhaust()
        parked = self.channel.effect_ledger.get_effect(
            self.channel.consumer_id, self._effect_id(media_id)
        )
        attempts_before = parked["details"]["attempt_count"]
        self.assertIs(True, parked["details"]["retry_parked"])

        # Restart on the same durable state.
        self.channel.stop_polling()
        restarted = self._channel()
        self.channel = restarted

        after = restarted.effect_ledger.get_effect(
            restarted.consumer_id, self._effect_id(media_id)
        )
        self.assertEqual(STATE_PENDING_MEDIA, after["status"])
        self.assertIs(True, after["details"]["retry_parked"])
        self.assertEqual(
            attempts_before,
            after["details"]["attempt_count"],
            "restart must not reset the retry budget to zero",
        )

        calls_after_restart = self.core.media_calls
        self.assertEqual(0, restarted._retry_pending_media())
        self.assertEqual(
            calls_after_restart,
            self.core.media_calls,
            "a restarted process must not re-hammer Core for a parked effect",
        )

        # The authoritative signal still recovers it, exactly once, across the restart.
        self.core.ready = True
        restarted._handle_event(self._ready_event(media_id, filename, mime))
        self.assertEqual(STATE_DELIVERED, self._status(media_id))
        self.assertEqual(1, len(self.deliveries))
        self.assertEqual(blob, self.deliveries[0][2])

    # -- 4. genuinely unrecoverable media still fails closed ---------------

    def test_permanent_media_status_still_fails_closed(self) -> None:
        """Requirement 8: do not turn every error into an endless pending."""
        for permanent_status in ("decode_failed", "encrypted_or_unknown", "unsupported_hevc"):
            with self.subTest(status=permanent_status):
                self.deliveries.clear()
                msg_type, media_id, filename, mime, _blob = MEDIA_TYPES[0]
                self.channel._handle_event(
                    self._created_event(
                        msg_type, media_id, filename, mime, status=permanent_status
                    )
                )
                self.assertEqual(
                    STATE_MEDIA_FAILED,
                    self._status(media_id),
                    f"{permanent_status} is a permanent Core verdict",
                )
                self.assertEqual([], self.deliveries)
                effect = self.channel.effect_ledger.get_effect(
                    self.channel.consumer_id, self._effect_id(media_id)
                )
                self.assertNotIn("retry_parked", effect["details"])

    def test_thumbnail_role_still_fails_closed(self) -> None:
        """A thumbnail is never an acceptable final original."""
        msg_type, media_id, filename, mime, _blob = MEDIA_TYPES[0]
        message = self._message(msg_type, media_id, filename, mime, "ready")
        message["media_role"] = "thumbnail"
        self.channel._handle_event(
            {
                "event_type": "message.created",
                "account_id": ACCOUNT_ID,
                "payload": {"message": message},
            }
        )
        self.assertEqual(STATE_MEDIA_FAILED, self._status(media_id))
        self.assertEqual([], self.deliveries)

    def test_historical_terminal_row_is_never_reopened(self) -> None:
        """TERMINAL_EFFECT_REOPENED = 0 -- the 42 historical rows stay history."""
        msg_type, media_id, filename, mime, _blob = MEDIA_TYPES[2]
        effect_id = self._effect_id(media_id)
        # Seed a pre-existing terminal row, as the production ledger already holds 42 of.
        self.channel.effect_ledger.mark_media_failed(
            self.channel.consumer_id,
            effect_id,
            account_id=ACCOUNT_ID,
            message_id=f"msg-{media_id}",
            event_type="message.created",
            reason="historical terminal row",
        )
        self.assertEqual(STATE_MEDIA_FAILED, self._status(media_id))

        self.core.ready = True
        self.channel._handle_event(self._ready_event(media_id, filename, mime))
        self.channel._handle_event(
            self._created_event(msg_type, media_id, filename, mime, status="ready")
        )
        self.channel._retry_pending_media()

        self.assertEqual(
            STATE_MEDIA_FAILED,
            self._status(media_id),
            "a historical terminal row must not be resurrected",
        )
        self.assertEqual([], self.deliveries)


if __name__ == "__main__":
    unittest.main()
