"""RC.14 EFB reprojection / replay idempotency regression suite (R1-R14).

Governing artefacts
-------------------
* ``docs/RC14_EFB_POST_F3_REPLAY_IDEMPOTENCY_AUDIT.md`` — the read-only audit that
  established Case C: the F3 re-projection windows are not replay-idempotent, and no
  single-cursor checkpoint fence can fix them because the real new business at
  ``274305..274307`` is adjacent to the re-projection at ``274308..274727``.
* ``tests/fixtures/rc14_post_f3_reprojection_stream.json`` — the real
  ``W1 / NEW_BUSINESS / W2`` event census, captured read-only from the production Core
  HTTP API. Message text and author identity are deliberately excluded; the fixture
  carries only identity and projection fields.

Defects closed
--------------
``R14-EFB-R1``  terminal-state suppression was gated on
    ``(event_type == "message.created" or is_media)``. A Core re-projection arrives as
    ``message.updated``, so a **non-media** replay fell through to the delivery path and
    re-opened an effect that was already ``DELIVERED`` (second external delivery) or
    already ``MEDIA_FAILED`` (second terminal row).

``R14-EFB-R2``  a ``message.updated`` for an identity the consumer had never
    established was dispatched exactly like a first delivery, so a re-projection of a
    message that predates the governed bootstrap window became a brand new delivery.

Isolation
---------
Every test runs against an offline Core double, a temporary ``EffectLedger`` and a
replaced ``_deliver_message`` boundary. No network egress, no production Telegram, no
production WeChat, no production ledger, no production Core.
"""

from __future__ import annotations

import json
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
from efb_wechat_comwechat_slave.Core import CoreAPIError, CoreMedia
from efb_wechat_comwechat_slave.EffectLedger import (
    STATE_DELIVERED,
    STATE_MEDIA_FAILED,
    STATE_PENDING_MEDIA,
    EffectLedger,
)

try:
    from efb_wechat_comwechat_slave.EffectLedger import EFFECT_KIND_DELIVERY
except ImportError:
    # Pre-fix tree (91a69cef): the explicit effect-kind column does not exist yet.
    # The behavioural tests still run there, which is what makes this suite a
    # discriminating regression check rather than a tautology.
    EFFECT_KIND_DELIVERY = "delivery"

FIXTURE = TESTS / "fixtures" / "rc14_post_f3_reprojection_stream.json"
PROVENANCE_FIXTURE = TESTS / "fixtures" / "rc14_unknown_identity_provenance.json"

# Sealed census boundaries (see the audit document section 2).
W1_LAST_CURSOR = 274304
NEW_BUSINESS_FIRST = 274305
NEW_BUSINESS_LAST = 274307
W2_FIRST_CURSOR = 274308

# The immutable production subscription anchor, read from Core's governed bootstrap
# provenance for consumer `efb-linux-wechat:wechat.linux`
# (GET /v1/consumers/efb-linux-wechat%3Awechat.linux/bootstrap):
#   initial_cursor 272037 / bootstrap_mode bounded_window / bootstrap_at 2026-09-15T11:18:09Z
PRODUCTION_FLOOR = {
    "consumer_id": "efb-linux-wechat:wechat.linux",
    "initial_cursor": 272037,
    "processed_through_cursor": 273844,
    "bootstrap_mode": "bounded_window",
    "bootstrap_source": "governed_rebootstrap",
    "bootstrap_at": "2026-09-15T11:18:09Z",
    "stream_head_cursor": 274765,
    "retention_floor_cursor": 1,
    "audit_history": [],
}

# A real origin from the W1/W2 re-projection window (cursor 273886) and a real origin
# from the genuinely new business at 274306.
PRE_FLOOR_CREATED_AT = "2026-09-01T06:22:51Z"
POST_FLOOR_CREATED_AT = "2026-09-17T00:30:06Z"

CONSUMER_ID = "rc14-reprojection"
FIXTURE_ACCOUNTS = ("f-live-a", "testB")

_UNSET = object()


class OfflineCore:
    """Offline Core double: no network, no production state."""

    def __init__(
        self,
        *,
        media_ready: bool = True,
        bootstrap=_UNSET,
        bootstrap_error: BaseException | None = None,
        projection: dict | None = None,
        projection_error: BaseException | None = None,
    ) -> None:
        self.media_ready = media_ready
        self.media_calls = 0
        if bootstrap is _UNSET:
            self.bootstrap = dict(PRODUCTION_FLOOR)
        elif bootstrap:
            self.bootstrap = dict(bootstrap)
        else:
            # An explicit None/empty bootstrap models a consumer with no governed
            # bootstrap provenance at all.
            self.bootstrap = None
        self.bootstrap_error = bootstrap_error
        self.projection = projection
        self.projection_error = projection_error
        self.projection_calls = 0

    def get_media(self, account_id: str, media_id: str) -> CoreMedia:
        self.media_calls += 1
        if not self.media_ready:
            raise CoreAPIError(404, "media_not_found", f"{media_id} is not ready")
        return CoreMedia(
            b"ORIGINAL-BYTES",
            "image/png",
            f"{media_id}.png",
            media_id,
            "original",
            "ready",
        )

    def health(self):
        return {"contract_version": 1}

    def list_chats(self, account_id: str):
        # Chats are pre-registered by the harness; the channel must not need to page
        # the Core chat index to resolve a fixture chat.
        return []

    def get_bootstrap_provenance(self, consumer_id: str):
        if self.bootstrap_error is not None:
            raise self.bootstrap_error
        return dict(self.bootstrap) if self.bootstrap else None

    def get_message_projection(self, account_id, chat_id, message_id, **_kwargs):
        self.projection_calls += 1
        if self.projection_error is not None:
            raise self.projection_error
        return dict(self.projection) if self.projection else None


def _load_fixture_rows():
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    columns = payload["columns"]
    return [dict(zip(columns, row)) for row in payload["rows"]]


def _load_provenance_fixture():
    """Return (floor, {cursor: (occurred_at, created_at)}, ledger_triples)."""
    payload = json.loads(PROVENANCE_FIXTURE.read_text(encoding="utf-8"))
    provenance = {str(row[0]): (str(row[1]), str(row[2])) for row in payload["provenance"]}
    ledger = [(str(row[0]), str(row[1]), str(row[2])) for row in payload["ledger"]]
    return payload["subscription_floor"], provenance, ledger


def _message_from_row(row, provenance=None):
    """Rebuild the Core message dict from a fixture row.

    The fixture stores only identity/projection fields, so ``text`` is empty and the
    author is synthesised. ``media_role`` / ``media_status`` are attached **only when
    non-empty**, which faithfully reproduces the two real payload shapes: the pre-F3
    W1 projection omits both keys entirely, and the F3 W2 projection carries both.

    ``created_at`` is attached when the provenance census has it, because it is Core's
    authoritative business origin and the classification decision depends on it.
    """
    message = {
        "account_id": row["account_id"],
        "chat_id": row["chat_id"],
        "message_id": row["message_id"],
        "type": row["type"],
        "direction": row["direction"],
        "text": "",
        "target_message_id": "",
        "substitutions": [],
        "author": {
            "member_id": "peer-1",
            "display_name": "Peer",
            "is_self": False,
        },
    }
    if provenance:
        created_at = provenance.get(str(row["cursor"]), ("", ""))[1]
        if created_at:
            message["created_at"] = created_at
    if row["media_id"]:
        message["media_id"] = row["media_id"]
    if row["media_role"]:
        message["media_role"] = row["media_role"]
    if row["media_status"]:
        message["media_status"] = row["media_status"]
    return message


def _event_from_row(row, provenance=None):
    event_type = row["event_type"]
    if event_type == "media.ready":
        return {
            "event_type": event_type,
            "account_id": row["account_id"],
            "payload": {
                "media": {
                    "account_id": row["account_id"],
                    "media_id": row["media_id"],
                    "role": row["media_role"],
                    "status": row["media_status"],
                    "filename": f"{row['media_id']}.png",
                    "mime_type": "image/png",
                }
            },
        }
    if event_type == "account.status":
        return {
            "event_type": event_type,
            "account_id": row["account_id"],
            "payload": {"status": "online"},
        }
    return {
        "event_type": event_type,
        "account_id": row["account_id"],
        "payload": {"message": _message_from_row(row, provenance)},
    }


class ReprojectionTestBase(unittest.TestCase):
    """Shared offline harness."""

    def setUp(self) -> None:
        self.data_path = (
            Path(__file__).resolve().parents[1] / ".tmp" / f"rc14-repro-{uuid.uuid4().hex}"
        )
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.core = OfflineCore()
        self.deliveries = []
        self.channel = self._new_channel()

    def tearDown(self) -> None:
        try:
            self.channel.stop_polling()
        except Exception:
            pass
        shutil.rmtree(self.data_path, ignore_errors=True)

    # ---------------------------------------------------------------- harness

    def _new_channel(
        self,
        *,
        consumer_id: str = CONSUMER_ID,
        max_attempts: int = 5,
        media_ready: bool = True,
        core: OfflineCore | None = None,
        data_path: Path | None = None,
    ):
        self.core = core if core is not None else OfflineCore(media_ready=media_ready)
        channel = LinuxWeChatChannel(
            core_client=self.core,
            config={
                "startup_healthcheck": False,
                "shutdown_install_deferred": False,
                "consumer_id": consumer_id,
                "account_ids": list(FIXTURE_ACCOUNTS) + ["account-1", "acc-1", "acc-2"],
                "media_retry_max_attempts": max_attempts,
                "media_retry_deadline_sec": 60,
                "media_retry_base_sec": 0,
                "media_retry_max_sec": 0,
                "core": {"poll_timeout": 0},
            },
            data_path=data_path if data_path is not None else self.data_path,
        )
        channel._deliver_message = self._capture
        return channel

    def _capture(self, message):
        content = message.file.read() if message.file is not None else b""
        self.deliveries.append(
            {
                "uid": str(message.uid),
                "type": message.type,
                "edit": bool(getattr(message, "edit", False)),
                "content": content,
            }
        )
        if message.file is not None:
            message.file.close()

    def _register_chat(self, account_id: str, chat_id: str) -> None:
        self.channel.chat_mgr.build_core_chat(
            {
                "account_id": account_id,
                "chat_id": chat_id,
                "type": "private",
                "display_name": chat_id,
            },
            account_id,
        )

    def _status(self, account_id: str, message_id: str):
        return self.channel.effect_ledger.get_effect_status(
            self.channel.consumer_id,
            self.channel.effect_ledger.compute_effect_id(account_id, message_id),
        )

    def _row_count(self) -> int:
        return self.channel.effect_ledger.count_effects(self.channel.consumer_id)

    @staticmethod
    def _event(event_type: str, message, account_id: str | None = None):
        return {
            "event_type": event_type,
            "account_id": account_id or message["account_id"],
            "payload": {"message": message},
        }

    @staticmethod
    def _msg(
        *,
        account_id: str = "f-live-a",
        chat_id: str = "38808757431@chatroom",
        message_id: str = "m-1",
        msg_type: str = "text",
        direction: str = "incoming",
        text: str = "",
        media_role: str = "",
        media_status: str = "",
        media_id: str = "",
        attributes=None,
        created_at: str = POST_FLOOR_CREATED_AT,
        origin_cursor=None,
    ):
        message = {
            "account_id": account_id,
            "chat_id": chat_id,
            "message_id": message_id,
            "type": msg_type,
            "direction": direction,
            "text": text,
            "target_message_id": "",
            "substitutions": [],
            "author": {"member_id": "peer-1", "display_name": "Peer", "is_self": False},
        }
        if created_at:
            message["created_at"] = created_at
        if origin_cursor is not None:
            message["origin_cursor"] = origin_cursor
        if media_id:
            message["media_id"] = media_id
        if media_role:
            message["media_role"] = media_role
        if media_status:
            message["media_status"] = media_status
        if attributes is not None:
            message["attributes"] = attributes
        return message

    def _media_msg(self, message_id: str, *, status: str = "ready", role: str = "original", **kwargs):
        return self._msg(
            message_id=message_id,
            msg_type="image",
            media_id=f"{message_id}-media",
            media_role=role,
            media_status=status,
            **kwargs,
        )

    def _floor(self, account_id: str = "f-live-a"):
        return self.channel.subscription_floor.get(account_id)

    def _provenance_counts(self):
        return dict(self.channel._provenance_counts)


class TestSingleMessageIdempotency(ReprojectionTestBase):
    """R1-R6, R9: per-message replay rules."""

    def setUp(self) -> None:
        super().setUp()
        self._register_chat("f-live-a", "38808757431@chatroom")

    def test_r1_delivered_then_replayed_created_and_update(self) -> None:
        message = self._media_msg("r1-msg")
        self.channel._handle_event(self._event("message.created", message))
        self.assertEqual(1, len(self.deliveries))
        self.assertEqual(STATE_DELIVERED, self._status("f-live-a", "r1-msg"))

        # A cursor rewind replays the very same creation event.
        self.channel._handle_event(self._event("message.created", message))
        # A re-projection arrives as an update for the same identity.
        self.channel._handle_event(self._event("message.updated", message))

        self.assertEqual(1, len(self.deliveries), "replay must not produce a second send")
        self.assertEqual(STATE_DELIVERED, self._status("f-live-a", "r1-msg"))
        self.assertEqual(1, self._row_count())

    def test_r2_delivered_non_media_message_update_is_suppressed(self) -> None:
        # The R14-EFB-R1 regression: `is_media` was part of the suppression gate, so a
        # non-media update re-opened a terminal effect.
        message = self._msg(message_id="r2-text", msg_type="text", text="hello")
        self.channel._handle_event(self._event("message.created", message))
        self.assertEqual(1, len(self.deliveries))
        self.assertEqual(MsgType.Text, self.deliveries[0]["type"])
        self.assertEqual(STATE_DELIVERED, self._status("f-live-a", "r2-text"))

        self.channel._handle_event(self._event("message.updated", message))

        self.assertEqual(1, len(self.deliveries), "non-media update must not re-deliver")
        self.assertEqual(1, self._row_count())

    def test_r3_delivered_media_message_f3_reprojection_is_suppressed(self) -> None:
        message = self._media_msg("r3-img", status="ready")
        self.channel._handle_event(self._event("message.created", message))
        self.assertEqual(1, len(self.deliveries))

        # The F3 re-projection carries a *different* projection for the same business
        # message — here the media has regressed to a permanent failure status. It must
        # not create a second delivery nor a new terminal row.
        reprojected = self._media_msg("r3-img", status="decode_failed")
        self.channel._handle_event(self._event("message.updated", reprojected))

        self.assertEqual(1, len(self.deliveries))
        self.assertEqual(STATE_DELIVERED, self._status("f-live-a", "r3-img"))
        self.assertEqual(1, self._row_count())

    def test_r4_historical_media_failed_is_not_reopened(self) -> None:
        self.channel.stop_polling()
        self.channel = self._new_channel(max_attempts=1, media_ready=False)
        self.channel._deliver_message = self._capture
        self._register_chat("f-live-a", "38808757431@chatroom")

        message = self._media_msg("r4-img", status="original_pending")
        self.channel._handle_event(self._event("message.created", message))
        self.assertEqual(STATE_MEDIA_FAILED, self._status("f-live-a", "r4-img"))
        self.assertEqual([], self.deliveries)
        self.assertEqual(1, self._row_count())

        # Replay the same identity as an update, twice.
        self.channel._handle_event(self._event("message.updated", message))
        self.channel._handle_event(self._event("message.updated", message))

        self.assertEqual(STATE_MEDIA_FAILED, self._status("f-live-a", "r4-img"))
        self.assertEqual(1, self._row_count(), "no duplicate terminal row")
        self.assertEqual([], self.deliveries)

    def test_r5_pending_media_progresses_to_exactly_one_delivery(self) -> None:
        self.channel.stop_polling()
        self.channel = self._new_channel(media_ready=False)
        self.channel._deliver_message = self._capture
        self._register_chat("f-live-a", "38808757431@chatroom")

        message = self._media_msg("r5-img", status="original_pending")
        self.channel._handle_event(self._event("message.created", message))
        self.assertEqual(STATE_PENDING_MEDIA, self._status("f-live-a", "r5-img"))
        self.assertEqual([], self.deliveries)

        self.core.media_ready = True
        ready = {
            "event_type": "media.ready",
            "account_id": "f-live-a",
            "payload": {
                "media": {
                    "media_id": "r5-img-media",
                    "role": "original",
                    "status": "ready",
                    "filename": "r5.png",
                    "mime_type": "image/png",
                }
            },
        }
        self.channel._handle_event(ready)
        self.channel._handle_event(ready)
        self.channel._handle_event(
            self._event("message.updated", self._media_msg("r5-img", status="ready"))
        )

        self.assertEqual(1, len(self.deliveries), "pending media must promote exactly once")
        self.assertEqual(STATE_DELIVERED, self._status("f-live-a", "r5-img"))
        self.assertEqual(1, self._row_count())

    def test_r6_delivered_media_ready_is_a_noop(self) -> None:
        message = self._media_msg("r6-img", status="ready")
        self.channel._handle_event(self._event("message.created", message))
        self.assertEqual(1, len(self.deliveries))
        rows_before = self._row_count()

        self.channel._handle_event(
            {
                "event_type": "media.ready",
                "account_id": "f-live-a",
                "payload": {
                    "media": {
                        "media_id": "r6-img-media",
                        "role": "original",
                        "status": "ready",
                        "filename": "r6.png",
                        "mime_type": "image/png",
                    }
                },
            }
        )

        self.assertEqual(1, len(self.deliveries))
        self.assertEqual(rows_before, self._row_count())
        self.assertEqual(STATE_DELIVERED, self._status("f-live-a", "r6-img"))

    def test_r9_first_seen_file_without_media_reference_is_fail_safe(self) -> None:
        # Mirrors the real cursor 274307: type=file with an empty media_id.
        message = self._msg(
            message_id="r9-file",
            msg_type="file",
            media_id="",
        )
        self.channel._handle_event(self._event("message.created", message))

        self.assertEqual([], self.deliveries)
        self.assertEqual(STATE_PENDING_MEDIA, self._status("f-live-a", "r9-file"))
        self.assertEqual(1, self._row_count())

        # It must not be misread as a duplicate: a second identical observation keeps a
        # single pending row rather than being swallowed as "already handled".
        self.channel._handle_event(self._event("message.created", message))
        self.assertEqual(1, self._row_count())
        self.assertEqual(STATE_PENDING_MEDIA, self._status("f-live-a", "r9-file"))


class TestFirstSeenDelivery(ReprojectionTestBase):
    """R7, R8: genuinely new business still flows."""

    def setUp(self) -> None:
        super().setUp()
        self._register_chat("f-live-a", "38808757431@chatroom")

    def test_r7_first_seen_link_message_delivers_once(self) -> None:
        message = self._msg(
            message_id="r7-link",
            msg_type="link",
            attributes={"url": "https://example.invalid/rc14-fixture", "title": "T"},
        )
        self.channel._handle_event(self._event("message.created", message))
        self.assertEqual(1, len(self.deliveries))
        self.assertEqual(MsgType.Link, self.deliveries[0]["type"])
        self.assertEqual(STATE_DELIVERED, self._status("f-live-a", "r7-link"))

    def test_r8_first_seen_text_message_delivers_once(self) -> None:
        message = self._msg(message_id="r8-text", msg_type="text", text="hello")
        self.channel._handle_event(self._event("message.created", message))
        self.assertEqual(1, len(self.deliveries))
        self.assertEqual(MsgType.Text, self.deliveries[0]["type"])
        self.assertEqual(STATE_DELIVERED, self._status("f-live-a", "r8-text"))


class TestScopeIsolation(ReprojectionTestBase):
    """R10-R13."""

    def setUp(self) -> None:
        super().setUp()
        self._register_chat("f-live-a", "38808757431@chatroom")
        self._register_chat("acc-1", "chat-1")
        self._register_chat("acc-2", "chat-2")

    def test_r10_restart_still_suppresses_the_same_reprojection(self) -> None:
        message = self._media_msg("r10-img", status="ready")
        self.channel._handle_event(self._event("message.created", message))
        self.assertEqual(1, len(self.deliveries))

        restarted = self._new_channel()
        self.channel = restarted
        self.channel._deliver_message = self._capture

        self.channel._handle_event(self._event("message.updated", message))
        self.channel._handle_event(self._event("message.updated", message))

        self.assertEqual(1, len(self.deliveries), "restart must not re-open the effect")
        self.assertEqual(STATE_DELIVERED, self._status("f-live-a", "r10-img"))
        self.assertEqual(1, self._row_count())

    def test_r11_account_scope_isolation(self) -> None:
        self.channel._handle_event(
            self._event("message.created", self._msg(account_id="acc-1", chat_id="chat-1", message_id="shared-id"))
        )
        self.assertEqual(1, len(self.deliveries))
        self.assertEqual(STATE_DELIVERED, self._status("acc-1", "shared-id"))

        # The same message id under a different account is a different effect and must
        # not be suppressed by acc-1's terminal state.
        self.channel._handle_event(
            self._event("message.created", self._msg(account_id="acc-2", chat_id="chat-2", message_id="shared-id"))
        )
        self.assertEqual(2, len(self.deliveries))
        self.assertEqual(STATE_DELIVERED, self._status("acc-2", "shared-id"))

    def test_r12_consumer_scope_isolation(self) -> None:
        ledger = self.channel.effect_ledger
        effect_id = ledger.compute_effect_id("acc-1", "consumer-scope")
        ledger.record_delivered(
            "consumer-A", effect_id, account_id="acc-1", message_id="consumer-scope"
        )
        self.assertEqual(STATE_DELIVERED, ledger.get_effect_status("consumer-A", effect_id))
        self.assertIsNone(
            ledger.get_effect_status("consumer-B", effect_id),
            "the consumer id is part of the primary key",
        )

    def test_r13_same_message_id_in_different_account_does_not_collide(self) -> None:
        ledger = self.channel.effect_ledger
        eid_1 = ledger.compute_effect_id("acc-1", "collide")
        eid_2 = ledger.compute_effect_id("acc-2", "collide")
        self.assertNotEqual(eid_1, eid_2)
        ledger.record_delivered("c", eid_1, account_id="acc-1", message_id="collide")
        self.assertIsNone(ledger.get_effect_status("c", eid_2))


class TestEffectIdentitySchema(ReprojectionTestBase):
    """Section 3 of the work package: the external-effect identity."""

    def test_identity_is_account_plus_message_only(self) -> None:
        ledger = self.channel.effect_ledger
        self.assertEqual(
            "acc-1:msg-1",
            ledger.compute_effect_id("acc-1", "msg-1"),
            "the delivery kind keeps the historical encoding",
        )

    def test_identity_is_independent_of_event_cursor_and_type(self) -> None:
        ledger = self.channel.effect_ledger
        # The signature has no cursor/event-type input at all: two projections of the
        # same business message necessarily collide on one key.
        self.assertEqual(
            ledger.compute_effect_id("acc-1", "msg-1"),
            ledger.compute_effect_id("acc-1", "msg-1"),
        )

    def test_second_effect_kind_is_addressable_explicitly(self) -> None:
        ledger = self.channel.effect_ledger
        delivery = ledger.compute_effect_id("acc-1", "msg-1", EFFECT_KIND_DELIVERY)
        other = ledger.compute_effect_id("acc-1", "msg-1", "some_future_kind")
        self.assertNotEqual(delivery, other)
        ledger.record_delivered("c", delivery, account_id="acc-1", message_id="msg-1")
        self.assertIsNone(
            ledger.get_effect_status("c", other),
            "a second effect kind must not be masked by the delivery effect",
        )

    def test_legacy_rows_gain_the_effect_kind_column_without_rewrite(self) -> None:
        # A ledger written by the previous candidate has no effect_kind column. Opening
        # it must add the column additively and leave every historical row untouched.
        import sqlite3

        legacy = self.data_path / "legacy-ledger.sqlite3"
        conn = sqlite3.connect(str(legacy))
        conn.execute(
            """
            CREATE TABLE effect_ledger (
                consumer_id TEXT NOT NULL, effect_id TEXT NOT NULL,
                account_id TEXT NOT NULL, message_id TEXT NOT NULL,
                efb_uid TEXT NOT NULL DEFAULT '', event_type TEXT NOT NULL DEFAULT 'message.created',
                status TEXT NOT NULL DEFAULT 'RESERVED',
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                details_json TEXT NOT NULL DEFAULT '{}',
                PRIMARY KEY (consumer_id, effect_id)
            );
            """
        )
        conn.execute(
            "INSERT INTO effect_ledger (consumer_id, effect_id, account_id, message_id, efb_uid,"
            " event_type, status, created_at, updated_at, details_json)"
            " VALUES ('c','acc-1:legacy','acc-1','legacy','','message.created','DELIVERED',"
            " '2026-01-01T00:00:00+00:00','2026-01-01T00:00:00+00:00','{}');"
        )
        conn.commit()
        conn.close()

        ledger = EffectLedger(legacy)
        try:
            record = ledger.get_effect("c", "acc-1:legacy")
            self.assertIsNotNone(record)
            self.assertEqual(STATE_DELIVERED, record["status"])
            self.assertEqual(EFFECT_KIND_DELIVERY, record["effect_kind"])
            self.assertEqual(1, ledger.count_effects("c"))
            self.assertEqual(1, ledger.status_counts("c")[STATE_DELIVERED])
        finally:
            ledger.close()


class TestRealW1W2Fixture(ReprojectionTestBase):
    """R14 plus the section 6 zero-effect gates, driven by the real census fixture."""

    def _seed_real_ledger(self, triples):
        """Seed the production EffectLedger identity/status snapshot verbatim.

        The snapshot is a read-only copy of the real ledger (417 rows: 375 DELIVERED +
        42 MEDIA_FAILED, single consumer, sha256 3e0853fb...0fcd). Seeding the real
        identities — rather than a deterministic stand-in — is what makes the offline
        replay discriminating: 69 of the window's 414 identities are known terminal
        state, 345 are not.
        """
        self.assertEqual(417, len(triples))
        ledger = self.channel.effect_ledger
        for account_id, message_id, status in triples:
            if status == STATE_DELIVERED:
                ledger.record_delivered(
                    self.channel.consumer_id,
                    ledger.compute_effect_id(account_id, message_id),
                    account_id=account_id,
                    message_id=message_id,
                    efb_uid=message_id,
                )
            else:
                self.assertEqual(STATE_MEDIA_FAILED, status)
                ledger.mark_media_failed(
                    self.channel.consumer_id,
                    ledger.compute_effect_id(account_id, message_id),
                    account_id=account_id,
                    message_id=message_id,
                    event_type="message.created",
                    reason="seeded historical terminal state",
                )
        return triples

    def test_fixture_matches_the_sealed_census(self) -> None:
        rows = _load_fixture_rows()
        self.assertEqual(883, len(rows))
        census = {}
        for row in rows:
            census[row["event_type"]] = census.get(row["event_type"], 0) + 1
        self.assertEqual(
            {
                "account.status": 39,
                "media.ready": 13,
                "message.created": 3,
                "message.updated": 828,
            },
            census,
        )
        w1 = [r for r in rows if r["event_type"] == "message.updated" and r["cursor"] <= W1_LAST_CURSOR]
        w2 = [r for r in rows if r["event_type"] == "message.updated" and r["cursor"] >= W2_FIRST_CURSOR]
        self.assertEqual(414, len(w1))
        self.assertEqual(414, len(w2))
        self.assertEqual(
            {r["message_id"] for r in w1},
            {r["message_id"] for r in w2},
            "W1 and W2 must be the same object set projected twice",
        )
        self.assertEqual(0, sum(1 for r in w1 if r["media_role"]), "W1 carries no F3 field")
        self.assertEqual(414, sum(1 for r in w2 if r["media_role"]), "W2 carries F3 fields")

    def test_provenance_census_matches_the_read_only_capture(self) -> None:
        """The provenance fixture must reproduce the read-only Core capture exactly."""
        floor, provenance, ledger = _load_provenance_fixture()
        self.assertEqual(883, len(provenance))
        self.assertEqual(417, len(ledger))
        self.assertEqual(272037, floor["subscription_floor_cursor"])
        self.assertEqual("2026-09-15T11:18:09Z", floor["subscription_floor_at"])
        self.assertEqual("bounded_window", floor["bootstrap_mode"])

        rows = _load_fixture_rows()
        message_rows = [r for r in rows if r["event_type"].startswith("message")]
        self.assertEqual(831, len(message_rows))
        self.assertEqual(
            0,
            sum(1 for r in message_rows if not provenance[str(r["cursor"])][1]),
            "every real message projection carries a business origin",
        )
        pre = [r for r in message_rows if provenance[str(r["cursor"])][1] < floor["subscription_floor_at"]]
        post = [r for r in message_rows if provenance[str(r["cursor"])][1] >= floor["subscription_floor_at"]]
        self.assertEqual(774, len(pre))
        self.assertEqual(57, len(post))
        post_by_type = {}
        for row in post:
            post_by_type[row["event_type"]] = post_by_type.get(row["event_type"], 0) + 1
        self.assertEqual(
            {"message.created": 3, "message.updated": 54},
            post_by_type,
            "post-floor business is overwhelmingly carried by message.updated",
        )
        self.assertEqual(
            27,
            len({r["message_id"] for r in post if r["event_type"] == "message.updated"}),
            "54 post-floor updates over 27 distinct objects: the event type cannot be "
            "the discriminator between new business and a re-projection",
        )
        # The event's own occurred_at is NOT a business origin: the 27 post-floor media
        # objects were created after the floor but re-projected long afterwards, so an
        # occurred_at comparison would have mislabelled them.
        self.assertEqual(
            27,
            len({r["message_id"] for r in post if r["event_type"] == "message.updated"}),
        )

    def test_real_ledger_snapshot_partitions_the_window_identities(self) -> None:
        """69 of the window's 414 identities are terminal; 345 are unknown."""
        floor, provenance, ledger = _load_provenance_fixture()
        rows = _load_fixture_rows()
        window = {
            (r["account_id"], r["message_id"])
            for r in rows
            if r["event_type"] == "message.updated"
        }
        self.assertEqual(414, len(window))
        known = {(a, m) for a, m, _ in ledger}
        known_window = window & known
        self.assertEqual(69, len(known_window))
        statuses = {s for a, m, s in ledger if (a, m) in window}
        self.assertEqual({STATE_DELIVERED, STATE_MEDIA_FAILED}, statuses)
        pre_floor = {
            (r["account_id"], r["message_id"])
            for r in rows
            if r["event_type"] == "message.updated"
            and provenance[str(r["cursor"])][1] < floor["subscription_floor_at"]
        }
        post_floor = {
            (r["account_id"], r["message_id"])
            for r in rows
            if r["event_type"] == "message.updated"
            and provenance[str(r["cursor"])][1] >= floor["subscription_floor_at"]
        }
        self.assertEqual(387, len(pre_floor))
        self.assertEqual(27, len(post_floor))
        # The 27 post-floor objects are all known MEDIA_FAILED, so they are decided by
        # the durable state machine; the 345 unknown pre-floor objects are exactly the
        # re-projections the provenance rule must suppress.
        self.assertEqual(
            27,
            len({(a, m) for a, m, s in ledger if (a, m) in post_floor}),
        )
        self.assertEqual(
            {STATE_MEDIA_FAILED},
            {s for a, m, s in ledger if (a, m) in post_floor},
        )
        self.assertEqual(42, len({(a, m) for a, m, s in ledger if (a, m) in pre_floor}))
        self.assertEqual(
            {STATE_DELIVERED},
            {s for a, m, s in ledger if (a, m) in pre_floor},
        )

    def test_r14_replay_ordering_w1_real_events_w2(self) -> None:
        rows = _load_fixture_rows()
        floor, provenance, ledger_triples = _load_provenance_fixture()
        for account_id, chat_id in sorted(
            {(r["account_id"], r["chat_id"]) for r in rows if r["chat_id"]}
        ):
            self._register_chat(account_id, chat_id)

        w1_rows = [
            r
            for r in rows
            if (r["event_type"] == "message.updated" and r["cursor"] <= W1_LAST_CURSOR)
            or r["event_type"] in {"media.ready", "account.status"}
            and r["cursor"] <= W1_LAST_CURSOR
        ]
        new_business_rows = [
            r for r in rows if NEW_BUSINESS_FIRST <= r["cursor"] <= NEW_BUSINESS_LAST
        ]
        w2_rows = [
            r
            for r in rows
            if (r["event_type"] == "message.updated" and r["cursor"] >= W2_FIRST_CURSOR)
            or (r["event_type"] == "account.status" and r["cursor"] >= W2_FIRST_CURSOR)
        ]
        self.assertEqual(460, len(w1_rows))
        self.assertEqual(3, len(new_business_rows))
        self.assertEqual(420, len(w2_rows))

        self._seed_real_ledger(ledger_triples)
        ledger_before = self._row_count()
        self.assertEqual(417, ledger_before)
        statuses_before = self.channel.effect_ledger.status_counts(self.channel.consumer_id)
        self.assertEqual(375, statuses_before[STATE_DELIVERED])
        self.assertEqual(42, statuses_before[STATE_MEDIA_FAILED])

        # ---- W1 (pre-F3 re-projection, no media_role / media_status at all) ----
        for row in w1_rows:
            self.channel._handle_event(_event_from_row(row, provenance))
        w1_deliveries = len(self.deliveries)
        w1_statuses = self.channel.effect_ledger.status_counts(self.channel.consumer_id)

        self.assertEqual(0, w1_deliveries)
        self.assertEqual(417, self._row_count())
        self.assertEqual(42, w1_statuses[STATE_MEDIA_FAILED])
        self.assertEqual(0, w1_statuses[STATE_PENDING_MEDIA])
        self.assertEqual(0, self.channel.provenance_deferrals.count())

        # ---- the three real new business events ----
        for row in new_business_rows:
            self.channel._handle_event(_event_from_row(row, provenance))
        new_business_deliveries = len(self.deliveries) - w1_deliveries

        self.assertEqual(2, new_business_deliveries)
        self.assertEqual(STATE_DELIVERED, self._status("f-live-a", new_business_rows[0]["message_id"]))
        self.assertEqual(STATE_DELIVERED, self._status("f-live-a", new_business_rows[1]["message_id"]))
        self.assertEqual(
            STATE_PENDING_MEDIA,
            self._status("f-live-a", new_business_rows[2]["message_id"]),
            "274307 is a file with an empty media_id: fail-safe pending, not a duplicate",
        )

        # ---- W2 (F3 re-projection, well-formed projection fields) ----
        deliveries_before_w2 = len(self.deliveries)
        rows_before_w2 = self._row_count()
        statuses_before_w2 = self.channel.effect_ledger.status_counts(self.channel.consumer_id)
        for row in w2_rows:
            self.channel._handle_event(_event_from_row(row, provenance))
        w2_deliveries = len(self.deliveries) - deliveries_before_w2
        statuses_after = self.channel.effect_ledger.status_counts(self.channel.consumer_id)

        self.assertEqual(0, w2_deliveries)
        self.assertEqual(rows_before_w2, self._row_count())
        self.assertEqual(42, statuses_after[STATE_MEDIA_FAILED])
        self.assertEqual(1, statuses_after[STATE_PENDING_MEDIA])
        self.assertEqual(0, self.channel.provenance_deferrals.count())

        # ---- section 6 gates ----
        # The gates are deltas across the phase they name, so a pre-existing terminal or
        # pending row carried in from the seeded ledger or from the genuinely-new 274307
        # object can never be mis-attributed to a re-projection.
        self.assertEqual(0, w1_deliveries, "W1_DUPLICATE_EXTERNAL_DELIVERY")
        self.assertEqual(
            0,
            w1_statuses[STATE_MEDIA_FAILED] - statuses_before[STATE_MEDIA_FAILED],
            "W1_NEW_MEDIA_FAILED_FROM_REPROJECTION",
        )
        self.assertEqual(
            0,
            w1_statuses[STATE_PENDING_MEDIA] - statuses_before[STATE_PENDING_MEDIA],
            "W1_UNWANTED_PENDING_MEDIA",
        )
        self.assertEqual(0, w2_deliveries, "W2_DUPLICATE_EXTERNAL_DELIVERY")
        self.assertEqual(
            0,
            statuses_after[STATE_MEDIA_FAILED] - statuses_before_w2[STATE_MEDIA_FAILED],
            "W2_NEW_MEDIA_FAILED_FROM_REPROJECTION",
        )
        self.assertEqual(
            0,
            statuses_after[STATE_PENDING_MEDIA] - statuses_before_w2[STATE_PENDING_MEDIA],
            "W2_UNWANTED_PENDING_MEDIA",
        )
        # Disclosed separately: the one pending row in the ledger is the 274307
        # file/empty-media_id gap, which the operator ruling keeps as an independent
        # Core follow-up (FILE_MEDIA_REFERENCE_GAP_CONFIRMED) rather than a
        # re-projection artefact.
        self.assertEqual(
            1,
            statuses_after[STATE_PENDING_MEDIA],
            "the single pending row is the known 274307 file gap, not a W2 artefact",
        )


if __name__ == "__main__":
    unittest.main()
