"""Permanent regression tests for defect R14-EFB-MEDIA-RETRY-HEAD-OF-LINE.

Production observation (2026-09-25): EFB's ``core-event-cursor.json`` had been
frozen at cursor 2719 for 30+ minutes while Core's stream head was 2732. The log
repeated, every ~30 seconds::

    Core polling iteration failed; cursor retained for retry:
      Core API 504 agent_wechat_error: CDN download failed: HTTP Error 400

The chain was:

1. ``CoreMessage._media_file`` converted only ``404 media_not_found`` into
   ``MediaPendingError`` and re-raised every other ``CoreAPIError`` verbatim.
2. ``ComWechat._handle_message_event`` catches only ``MediaPendingError`` /
   ``MediaPermanentError``, so the 504 escaped it.
3. ``_retry_pending_media`` adds no conversion, so the exception escaped
   ``poll_once``.
4. ``poll_once`` calls ``_retry_pending_media()`` *before* fetching new events,
   so one unservable old media object starved the entire stream -- pure text
   included.
5. The exception never reached ``_defer_media``, so ``attempt_count`` never grew,
   exponential backoff never engaged, and the recoverable-park branch was
   unreachable. The failure was permanent, not transient.

These tests pin the fix: a *transient* upstream media failure must be classified
as retryable media state and be handed to the existing deferral machinery, while
genuinely final verdicts and non-media protocol errors keep failing closed.

    MEDIA_5XX_ESCAPES_POLL_ONCE          = NO
    MEDIA_5XX_BLOCKS_NEW_TEXT            = NO
    MEDIA_5XX_INCREMENTS_ATTEMPT_COUNT   = YES
    MEDIA_5XX_PARKS_AFTER_BUDGET         = YES
    PARKED_MEDIA_RECOVERS_EXACTLY_ONCE   = YES
    MEDIA_UNSUPPORTED_TERMINALISES       = YES
    UNKNOWN_PROTOCOL_ERROR_STILL_RAISES  = YES
    TERMINAL_EFFECT_REOPENED             = 0
    DUPLICATE_TELEGRAM_DELIVERY          = 0
"""

from __future__ import annotations

import shutil
import sys
import unittest
import uuid
from pathlib import Path

TESTS = Path(__file__).resolve().parent
if str(TESTS) not in sys.path:
    sys.path.insert(0, str(TESTS))

from stub_ehforwarderbot import install_stubs

install_stubs()

from ehforwarderbot import MsgType

from efb_wechat_comwechat_slave.ComWechat import LinuxWeChatChannel
from efb_wechat_comwechat_slave.Core import (
    CoreAPIError,
    CoreClient,
    CoreMedia,
    CoreUnavailableError,
)
from efb_wechat_comwechat_slave.CoreMessage import (
    CoreMessageBuilder,
    MediaPendingError,
    MediaPermanentError,
)
from efb_wechat_comwechat_slave.EffectLedger import (
    STATE_DELIVERED,
    STATE_MEDIA_FAILED,
    STATE_PENDING_MEDIA,
)

ACCOUNT_ID = "account-1"
CHAT_ID = "chat-1"
#: The channel derives its ledger key as ``<consumer_id>:<channel_id>``; the tests
#: always read the ledger through ``self.channel.consumer_id`` so they follow it.
CONSUMER_BASE = "rc14-media-retry"

#: The exact production failure string, reproduced from the 2026-09-25 log.
PRODUCTION_504_MESSAGE = "CDN download failed: HTTP Error 400: Bad Request"


class ScriptedMediaCore:
    """Core double whose media endpoint is scripted per ``media_id``.

    A script entry is either a ``CoreMedia`` (served), an ``Exception``
    (raised), or a callable returning one of the two. A missing entry behaves
    like ``404 media_not_found``. Everything else the channel needs during
    ``poll_once`` is answered deterministically and the Core side effects are
    recorded so the tests can assert on cursor/checkpoint progression.
    """

    def __init__(self, script: dict | None = None) -> None:
        self.script = dict(script or {})
        self.media_calls: list[tuple[str, str]] = []
        self.media_calls_by_id: dict[str, int] = {}
        self.poll_calls: list[str] = []
        self.checkpoints: list[int] = []
        self.acks: list[str] = []
        self._events: list[dict] = []
        self.stream_head = "0"

    # -- scripting helpers --------------------------------------------------

    def queue_events(self, events: list[dict], *, stream_head: str | None = None) -> None:
        self._events = [dict(event) for event in events]
        if stream_head is not None:
            self.stream_head = stream_head
        elif events:
            self.stream_head = str(events[-1].get("cursor") or self.stream_head)

    # -- Core surface -------------------------------------------------------

    def health(self):
        return {"contract_version": 1, "ok": True}

    def list_accounts(self):
        return [{"account_id": ACCOUNT_ID, "state": "online"}]

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

    def get_media(self, account_id: str, media_id: str) -> CoreMedia:
        self.media_calls.append((account_id, media_id))
        self.media_calls_by_id[media_id] = self.media_calls_by_id.get(media_id, 0) + 1
        entry = self.script.get(media_id)
        if callable(entry):
            entry = entry(self.media_calls_by_id[media_id])
        if entry is None:
            raise CoreAPIError(404, "media_not_found", f"{media_id} is not ready")
        if isinstance(entry, Exception):
            raise entry
        return entry

    def poll_events(self, *, after, consumer_id, timeout=0, limit=50, account_id=None):
        self.poll_calls.append(str(after))
        events = [
            event
            for event in self._events
            if int(event.get("cursor") or 0) > int(after or 0)
        ]
        return {
            "events": events,
            "has_more": False,
            "stream_head_cursor": self.stream_head,
        }

    def ack_events(self, consumer_id: str, event_ids):
        self.acks.extend(str(item) for item in event_ids)
        return {"consumer_id": consumer_id, "acked_count": len(list(event_ids))}

    def checkpoint_events(self, consumer_id, processed_through_cursor, **_kwargs):
        self.checkpoints.append(int(processed_through_cursor))
        return {"consumer_id": consumer_id, "processed_through_cursor": processed_through_cursor}


def media_504() -> CoreAPIError:
    """The production failure: Core proxies the agent's 504 verbatim."""
    return CoreAPIError(504, "agent_wechat_error", PRODUCTION_504_MESSAGE)


def ready_media(media_id: str, blob: bytes = b"REAL-STICKER") -> CoreMedia:
    return CoreMedia(blob, "image/webp", "sticker.webp", media_id, "original", "ready")


class MediaRetryRecoveryTest(unittest.TestCase):
    # -- fixtures -----------------------------------------------------------

    def setUp(self) -> None:
        self.data_path = (
            Path(__file__).resolve().parents[1] / ".tmp" / f"rc14-mrr-{uuid.uuid4().hex}"
        )
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.core = ScriptedMediaCore()
        self.deliveries: list = []
        self.channel = self._channel()

    def tearDown(self) -> None:
        try:
            self.channel.stop_polling()
        except Exception:  # noqa: BLE001 - teardown must not mask the test result
            pass
        shutil.rmtree(self.data_path, ignore_errors=True)

    def _channel(self, *, max_attempts: int = 5) -> LinuxWeChatChannel:
        channel = LinuxWeChatChannel(
            core_client=self.core,
            config={
                "startup_healthcheck": False,
                "shutdown_install_deferred": False,
                "consumer_id": CONSUMER_BASE,
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
            self.deliveries.append((str(message.uid), message.type, content))
            if message.file is not None:
                message.file.close()

        channel._deliver_message = capture
        return channel

    @staticmethod
    def _sticker_message(message_id: str = "sticker-1", media_id: str = "sticker-media-1"):
        return {
            "account_id": ACCOUNT_ID,
            "chat_id": CHAT_ID,
            "message_id": message_id,
            "type": "sticker",
            "direction": "incoming",
            "author": {"member_id": "peer-1", "display_name": "Peer", "is_self": False},
            "media_id": media_id,
            "media_role": "original",
            "media_status": "ready",
            "filename": "sticker.webp",
            "mime_type": "image/webp",
        }

    @staticmethod
    def _text_message(message_id: str = "text-1"):
        return {
            "account_id": ACCOUNT_ID,
            "chat_id": CHAT_ID,
            "message_id": message_id,
            "type": "text",
            "direction": "incoming",
            "text": "hello after the sticker",
            "author": {"member_id": "peer-1", "display_name": "Peer", "is_self": False},
        }

    @staticmethod
    def _event(event_type: str, message: dict, cursor: int) -> dict:
        return {
            "event_type": event_type,
            "account_id": ACCOUNT_ID,
            "cursor": cursor,
            "event_id": f"event-{cursor}",
            "payload": {"message": message},
        }

    def _created_event(self, message: dict, cursor: int) -> dict:
        return self._event("message.created", message, cursor)

    def _ready_event(self, media_id: str = "sticker-media-1", cursor: int = 99) -> dict:
        return {
            "event_type": "media.ready",
            "account_id": ACCOUNT_ID,
            "cursor": cursor,
            "event_id": f"event-{cursor}",
            "payload": {
                "media": {
                    "media_id": media_id,
                    "role": "original",
                    "status": "ready",
                    "filename": "sticker.webp",
                    "mime_type": "image/webp",
                }
            },
        }

    def _effect_id(self, message_id: str) -> str:
        return f"{ACCOUNT_ID}:{message_id}"

    def _status(self, message_id: str):
        return self.channel.effect_ledger.get_effect_status(
            self.channel.consumer_id, self._effect_id(message_id)
        )

    def _effect(self, message_id: str):
        return self.channel.effect_ledger.get_effect(
            self.channel.consumer_id, self._effect_id(message_id)
        )

    def _media_failed_count(self) -> int:
        return self.channel.effect_ledger.count_effects(
            self.channel.consumer_id, status=STATE_MEDIA_FAILED
        )

    # -- 1. error classification at the _media_file boundary ----------------

    def test_1a_transient_core_errors_are_classified_as_retryable(self) -> None:
        """Requirement 4.1/4.2: 504, 503, 429, 202 and unreachable Core retry."""
        cases = {
            "504_agent_wechat_error": media_504(),
            "503_service_unavailable": CoreAPIError(503, "runtime_management_unavailable", "runtime down"),
            "500_internal": CoreAPIError(500, "registry_reload_failed", "boom"),
            "429_rate_limited": CoreAPIError(429, "rate_limited", "slow down"),
            "408_timeout": CoreAPIError(408, "request_timeout", "timeout"),
            "425_too_early": CoreAPIError(425, "too_early", "too early"),
            "202_media_pending": CoreAPIError(202, "media_pending", "still downloading"),
            "core_unreachable": CoreUnavailableError("Core request failed: Connection refused"),
        }
        for label, error in cases.items():
            with self.subTest(case=label):
                builder, core = self._builder({label: error})
                with self.assertRaises(MediaPendingError) as ctx:
                    builder._media_file(ACCOUNT_ID, label, "sticker.webp", "image/webp")
                self.assertNotIsInstance(ctx.exception, MediaPermanentError)
                self.assertEqual(1, len(core.media_calls))

    def test_1b_missing_media_stays_pending_and_unsupported_is_permanent(self) -> None:
        """Requirement 4.3/4.4: 404 media_not_found retries, media_unsupported does not."""
        builder, _core = self._builder(
            {
                "missing": CoreAPIError(404, "media_not_found", "Media bytes are unavailable"),
                "unsupported": CoreAPIError(404, "media_unsupported", "format unsupported"),
            }
        )
        with self.assertRaises(MediaPendingError):
            builder._media_file(ACCOUNT_ID, "missing", None, None)
        with self.assertRaises(MediaPermanentError):
            builder._media_file(ACCOUNT_ID, "unsupported", None, None)

    def test_1c_non_media_protocol_errors_still_fail_closed(self) -> None:
        """Requirement 4.5: unknown 4xx and auth failures must not be disguised."""
        cases = {
            "unknown_400": CoreAPIError(400, "invalid_request", "account_id is required"),
            "unknown_404": CoreAPIError(404, "account_not_found", "Unknown account"),
            "unauthorized_401": CoreAPIError(401, "unauthorized", "token rejected"),
            "forbidden_403": CoreAPIError(403, "forbidden", "not permitted"),
            "conflict_409": CoreAPIError(409, "consumer_foreign_conflict", "conflict"),
        }
        for label, error in cases.items():
            with self.subTest(case=label):
                builder, _core = self._builder({label: error})
                with self.assertRaises(CoreAPIError) as ctx:
                    builder._media_file(ACCOUNT_ID, label, None, None)
                self.assertIs(error, ctx.exception)
                self.assertNotIsInstance(ctx.exception, MediaPendingError)
                self.assertNotIsInstance(ctx.exception, MediaPermanentError)

    def _builder(self, script: dict):
        core = ScriptedMediaCore(script)
        from efb_wechat_comwechat_slave.ChatMgr import ChatMgr

        chats = ChatMgr(self.channel)
        chats.build_core_chat(
            {
                "account_id": ACCOUNT_ID,
                "chat_id": CHAT_ID,
                "type": "private",
                "display_name": "Peer",
            },
            "Self",
        )
        return CoreMessageBuilder(core, chats), core

    # -- 2. the production head-of-line block -------------------------------

    def test_2_504_media_does_not_block_poll_once_or_the_following_text(self) -> None:
        """Requirement 4.4: the cursor must pass the sticker and deliver the text."""
        self.core.script["sticker-media-1"] = media_504()
        self.core.queue_events(
            [
                self._created_event(self._sticker_message(), cursor=2724),
                self._created_event(self._text_message("text-2732"), cursor=2732),
            ]
        )

        processed = self.channel.poll_once()

        self.assertEqual(2, processed)
        # The text was delivered exactly once, despite the media failure in front.
        self.assertEqual([("text-2732", MsgType.Text, b"")], self.deliveries)
        # The media was deferred, not lost.
        self.assertEqual(STATE_PENDING_MEDIA, self._status("sticker-1"))
        self.assertEqual(1, self._effect("sticker-1")["details"]["attempt_count"])
        self.assertIn("504", self._effect("sticker-1")["details"]["last_error"])
        # Local cursor and Core checkpoint both advanced past the text event.
        self.assertEqual("2732", self.channel.cursor_store.load())
        self.assertEqual(2732, self.core.checkpoints[-1])
        self.assertEqual(0, self._media_failed_count())

    def test_2b_repeated_scheduled_retries_never_abort_polling(self) -> None:
        """The 30-second production loop: N failures must still poll every time."""
        self.core.script["sticker-media-1"] = media_504()
        self.channel._handle_event(self._created_event(self._sticker_message(), cursor=2724))

        for _ in range(5):
            # poll_once must return normally, not raise, on every iteration.
            self.assertEqual(0, self.channel.poll_once())

        self.assertEqual(5, len(self.core.poll_calls))
        # 1 initial deferral + 4 scheduled retries == the 5-attempt budget; the
        # fifth poll finds nothing due because the effect already parked.
        effect = self._effect("sticker-1")
        self.assertEqual(5, effect["details"]["attempt_count"])
        self.assertIs(True, effect["details"]["retry_parked"])
        self.assertEqual(STATE_PENDING_MEDIA, self._status("sticker-1"))
        self.assertEqual([], self.deliveries)
        self.assertEqual(0, self._media_failed_count())

    # -- 3. budget exhaustion parks, media.ready recovers -------------------

    def test_3_budget_exhaustion_parks_then_media_ready_delivers_once(self) -> None:
        """Requirement 4.5: park after the budget, recover exactly once."""
        self.channel.stop_polling()
        self.channel = self._channel(max_attempts=2)
        self.core.script["sticker-media-1"] = media_504()

        self.channel._handle_event(self._created_event(self._sticker_message(), cursor=2724))
        self.assertEqual(1, self.channel._retry_pending_media())

        # max_attempts=2 is reached by the first *retry* (the initial deferral is
        # attempt 1), so the effect parks and leaves the scheduled retry view.
        effect = self._effect("sticker-1")
        self.assertEqual(STATE_PENDING_MEDIA, effect["status"])
        self.assertEqual(2, effect["details"]["attempt_count"])
        self.assertIs(True, effect["details"]["retry_parked"])
        self.assertIn("retry exhausted", effect["details"]["parked_reason"])
        self.assertEqual(0, self._media_failed_count())

        # Parked rows leave the scheduled retry view: no more Core hammering.
        calls_after_park = len(self.core.media_calls)
        for _ in range(3):
            self.assertEqual(0, self.channel._retry_pending_media())
        self.assertEqual(calls_after_park, len(self.core.media_calls))

        # An authoritative media.ready still recovers it, exactly once.
        self.core.script["sticker-media-1"] = ready_media("sticker-media-1")
        self.channel._handle_event(self._ready_event())
        self.assertEqual(STATE_DELIVERED, self._status("sticker-1"))
        self.assertEqual(1, len(self.deliveries))
        self.assertEqual(b"REAL-STICKER", self.deliveries[0][2])

        # Replaying the authoritative signal must not produce a second effect.
        self.channel._handle_event(self._ready_event(cursor=100))
        self.channel._retry_pending_media()
        self.assertEqual(1, len(self.deliveries))

    # -- 4. isolation between pending objects ------------------------------

    def test_4_one_failing_pending_media_does_not_affect_the_others(self) -> None:
        """Requirement 4.6: independent pending objects keep their own fate."""
        self.core.script["sticker-media-1"] = media_504()
        self.core.script["sticker-media-2"] = ready_media("sticker-media-2", b"SECOND")
        self.core.queue_events(
            [
                self._created_event(
                    self._sticker_message("sticker-1", "sticker-media-1"), cursor=10
                ),
                self._created_event(
                    self._sticker_message("sticker-2", "sticker-media-2"), cursor=11
                ),
                self._created_event(self._text_message("text-12"), cursor=12),
            ]
        )

        self.assertEqual(3, self.channel.poll_once())

        # The healthy sticker and the text went out; the failing one is pending.
        self.assertEqual(
            [("sticker-2", MsgType.Sticker, b"SECOND"), ("text-12", MsgType.Text, b"")],
            self.deliveries,
        )
        self.assertEqual(STATE_DELIVERED, self._status("sticker-2"))
        self.assertEqual(STATE_PENDING_MEDIA, self._status("sticker-1"))
        self.assertEqual("12", self.channel.cursor_store.load())

        # A later poll keeps retrying only the failing object; the delivered one
        # is never re-fetched and never re-sent.
        media_calls_before = len(self.core.media_calls)
        self.channel.poll_once()
        self.assertEqual(2, len(self.deliveries))
        self.assertEqual(2, self._effect("sticker-1")["details"]["attempt_count"])
        self.assertEqual(
            [("account-1", "sticker-media-1")],
            self.core.media_calls[media_calls_before:],
        )
        self.assertEqual(STATE_DELIVERED, self._status("sticker-2"))

    def test_4b_terminal_effects_are_not_reopened_or_re_delivered(self) -> None:
        """Requirement 4.6: DELIVERED / MEDIA_FAILED stay closed under replay."""
        self.core.script["sticker-media-1"] = ready_media("sticker-media-1")
        self.core.script["sticker-media-2"] = CoreAPIError(
            404, "media_unsupported", "format unsupported"
        )
        self.channel._handle_event(
            self._created_event(self._sticker_message("sticker-1", "sticker-media-1"), cursor=20)
        )
        self.channel._handle_event(
            self._created_event(self._sticker_message("sticker-2", "sticker-media-2"), cursor=21)
        )

        self.assertEqual(STATE_DELIVERED, self._status("sticker-1"))
        self.assertEqual(STATE_MEDIA_FAILED, self._status("sticker-2"))
        self.assertEqual(1, len(self.deliveries))
        self.assertEqual(1, self._media_failed_count())

        # Replay both creation events and both authoritative signals.
        for cursor in (22, 23, 24):
            self.channel._handle_event(
                self._created_event(
                    self._sticker_message("sticker-1", "sticker-media-1"), cursor=cursor
                )
            )
            self.channel._handle_event(
                self._created_event(
                    self._sticker_message("sticker-2", "sticker-media-2"), cursor=cursor + 100
                )
            )
            self.channel._handle_event(self._ready_event("sticker-media-1", cursor=cursor + 200))
            self.channel._handle_event(self._ready_event("sticker-media-2", cursor=cursor + 300))
        self.channel._retry_pending_media()

        self.assertEqual(1, len(self.deliveries))
        self.assertEqual(STATE_DELIVERED, self._status("sticker-1"))
        self.assertEqual(STATE_MEDIA_FAILED, self._status("sticker-2"))
        self.assertEqual(1, self._media_failed_count())

    # -- 5. restart safety --------------------------------------------------

    def test_5_restart_preserves_attempts_park_cursor_and_exactly_once(self) -> None:
        """Requirement 4.7: a restarted process must not lose or redo state."""
        self.channel.stop_polling()
        self.channel = self._channel(max_attempts=2)
        self.core.script["sticker-media-1"] = media_504()
        self.core.queue_events(
            [
                self._created_event(self._sticker_message(), cursor=30),
                self._created_event(self._text_message("text-31"), cursor=31),
            ]
        )
        self.assertEqual(2, self.channel.poll_once())
        self.channel._retry_pending_media()  # second attempt -> park

        before = self._effect("sticker-1")
        self.assertIs(True, before["details"]["retry_parked"])
        self.assertEqual(2, before["details"]["attempt_count"])
        cursor_before = self.channel.cursor_store.load()
        self.assertEqual("31", cursor_before)
        self.assertEqual(1, len(self.deliveries))

        # Restart on the same durable state.
        self.channel.stop_polling()
        restarted = self._channel(max_attempts=2)
        self.channel = restarted

        after = self._effect("sticker-1")
        self.assertEqual(STATE_PENDING_MEDIA, after["status"])
        self.assertIs(True, after["details"]["retry_parked"])
        self.assertEqual(2, after["details"]["attempt_count"])
        self.assertEqual(cursor_before, self.channel.cursor_store.load())

        # A parked effect is not re-armed and Core is not re-hammered.
        calls_before = len(self.core.media_calls)
        self.assertEqual(0, self.channel._retry_pending_media())
        self.assertEqual(calls_before, len(self.core.media_calls))

        # The authoritative signal still recovers it exactly once across restart.
        self.core.script["sticker-media-1"] = ready_media("sticker-media-1")
        self.channel._handle_event(self._ready_event())
        self.assertEqual(STATE_DELIVERED, self._status("sticker-1"))
        self.assertEqual(2, len(self.deliveries))
        self.assertEqual(b"REAL-STICKER", self.deliveries[-1][2])

    def test_5b_restart_after_transient_failure_keeps_retrying(self) -> None:
        """A transient 504 must not be silently converted into a terminal state."""
        self.channel.stop_polling()
        self.channel = self._channel(max_attempts=5)
        self.core.script["sticker-media-1"] = media_504()
        self.channel._handle_event(self._created_event(self._sticker_message(), cursor=40))

        self.channel.stop_polling()
        restarted = self._channel(max_attempts=5)
        self.channel = restarted
        self.assertEqual(STATE_PENDING_MEDIA, self._status("sticker-1"))

        # Core recovers after the restart: the object must still be deliverable.
        self.core.script["sticker-media-1"] = ready_media("sticker-media-1")
        self.assertEqual(1, restarted._retry_pending_media())
        self.assertEqual(STATE_DELIVERED, self._status("sticker-1"))
        self.assertEqual(1, len(self.deliveries))
        self.assertEqual(0, self._media_failed_count())


class _FakeResponse:
    """Minimal ``requests.Response`` double for the media boundary."""

    def __init__(self, status_code: int, headers: dict, body: bytes) -> None:
        self.status_code = status_code
        self.headers = dict(headers)
        self.content = body

    @property
    def ok(self) -> bool:
        return self.status_code < 400

    def json(self):
        import json as _json

        return _json.loads(self.content.decode("utf-8"))


class _FakeSession:
    """Returns one scripted response for every request."""

    def __init__(self, response: _FakeResponse) -> None:
        self.response = response
        self.requests: list = []

    def request(self, method, url, **kwargs):
        self.requests.append((method, url, kwargs))
        return self.response


#: The exact body Core returns for ``202 media_pending`` (app.py:1041).
CORE_202_BODY = (
    b'{"error":{"code":"media_pending","message":"Media content is still downloading '
    b'in WeChat client","details":{"media_id":"m-1"}}}'
)


class CoreClientMediaBoundaryTest(unittest.TestCase):
    """Defect R14-EFB-MEDIA-PENDING-AS-TERMINAL.

    ``202`` is a 2xx status, so ``CoreClient._request`` returns it as a success and
    the JSON error body used to be handed to ``CoreMessage._media_file`` as if it
    were media bytes. With no ``X-Media-Role`` header the role check raised
    ``MediaPermanentError`` and ``ComWechat._fail_media`` wrote a terminal
    ``MEDIA_FAILED`` row for media Core explicitly says is still downloading.

    Production evidence (2026-09-25): two effects whose media answered
    ``HTTP/1.0 202 Accepted`` with ``Content-Type: application/json`` and no
    ``X-Media-Role`` were terminalised with reason
    ``Core returned media role '' ...; expected 'original'``. The same reason
    appears on historical rows from 2026-09-22.
    """

    def _client(self, response: _FakeResponse) -> tuple[CoreClient, _FakeSession]:
        session = _FakeSession(response)
        return CoreClient("http://core.test", session=session), session

    def test_202_media_pending_is_surfaced_as_a_structured_retryable_error(self) -> None:
        client, session = self._client(
            _FakeResponse(
                202,
                {"Content-Type": "application/json; charset=utf-8", "Content-Length": str(len(CORE_202_BODY))},
                CORE_202_BODY,
            )
        )
        with self.assertRaises(CoreAPIError) as ctx:
            client.get_media("account-1", "m-1")
        self.assertEqual(202, ctx.exception.status_code)
        self.assertEqual("media_pending", ctx.exception.code)
        self.assertEqual(1, len(session.requests))

    def test_202_classification_makes_the_media_file_path_pending_not_permanent(self) -> None:
        """The surfaced 202 must land in ``MediaPendingError``, never permanent."""
        client, _session = self._client(
            _FakeResponse(202, {"Content-Type": "application/json"}, CORE_202_BODY)
        )
        from efb_wechat_comwechat_slave.ChatMgr import ChatMgr
        from efb_wechat_comwechat_slave.CoreMessage import CoreMessageBuilder

        channel = LinuxWeChatChannel.__new__(LinuxWeChatChannel)
        builder = CoreMessageBuilder(client, ChatMgr(channel))
        with self.assertRaises(MediaPendingError) as ctx:
            builder._media_file("account-1", "m-1", "sticker.webp", "image/webp")
        self.assertNotIsInstance(ctx.exception, MediaPermanentError)

    def test_200_ready_media_is_still_served_normally(self) -> None:
        body = b"REAL-WEBP-BYTES"
        client, _session = self._client(
            _FakeResponse(
                200,
                {
                    "Content-Type": "image/webp",
                    "Content-Disposition": 'inline; filename="sticker.webp"',
                    "X-Media-Id": "m-1",
                    "X-Media-Role": "original",
                    "X-Media-Status": "ready",
                },
                body,
            )
        )
        media = client.get_media("account-1", "m-1")
        self.assertEqual(body, media.content)
        self.assertEqual("original", media.role)
        self.assertEqual("ready", media.status)
        self.assertEqual("sticker.webp", media.filename)
        self.assertEqual("image/webp", media.mime_type)

    def test_non_2xx_media_errors_still_raise_structured_errors(self) -> None:
        body = b'{"error":{"code":"agent_wechat_error","message":"CDN download failed"}}'
        client, _session = self._client(
            _FakeResponse(504, {"Content-Type": "application/json"}, body)
        )
        with self.assertRaises(CoreAPIError) as ctx:
            client.get_media("account-1", "m-1")
        self.assertEqual(504, ctx.exception.status_code)
        self.assertEqual("agent_wechat_error", ctx.exception.code)

    def test_404_media_unsupported_still_raises_the_unsupported_code(self) -> None:
        body = b'{"error":{"code":"media_unsupported","message":"Media format is unsupported"}}'
        client, _session = self._client(
            _FakeResponse(404, {"Content-Type": "application/json"}, body)
        )
        with self.assertRaises(CoreAPIError) as ctx:
            client.get_media("account-1", "m-1")
        self.assertEqual(404, ctx.exception.status_code)
        self.assertEqual("media_unsupported", ctx.exception.code)


if __name__ == "__main__":
    unittest.main()
