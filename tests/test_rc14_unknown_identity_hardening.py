"""RC.14 EFB reprojection idempotency — unknown identity hardening regression suite.

Governing ruling
----------------
The corrective engineering round that closed ``R14-EFB-R1``/``R14-EFB-R2`` still decided
"this is a re-projection" from ``event_type == "message.updated"``. The operator ruled
that this is not admissible: ``event_type`` describes Core's internal storage transition,
not whether this consumer owes an external effect, and it must not be the basis for
``duplicate`` / ``reprojection`` / ``historical object``.

The hardening replaces that conjunct with a decision over three durable facts:

1. the stable effect identity (``account_id`` + ``message_id``) — decides *known* vs
   *unknown*;
2. the durable subscription floor (Core ``initial_cursor`` + ``bootstrap_at``) — the
   immutable subscription anchor;
3. Core's authoritative business origin for the object (``created_at``, or a stronger
   immutable origin cursor if Core ever exposes one) — decides *pre-existing* vs *new*.

When (3) is unavailable nothing is guessed: the classification is ``INDETERMINATE``, no
external effect is produced, no terminal ledger row is written, and the event is parked
in a separate, additive store for a bounded retry.

Isolation
---------
Offline Core double, temporary data path, replaced ``_deliver_message`` boundary. No
network egress, no production Telegram, no production WeChat, no production ledger, no
production Core, no checkpoint mutation.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import sys
import unittest
import uuid
from pathlib import Path

TESTS = Path(__file__).resolve().parent
if str(TESTS) not in sys.path:
    sys.path.insert(0, str(TESTS))

from stub_ehforwarderbot import install_stubs

install_stubs()

from efb_wechat_comwechat_slave.Core import CoreAPIError, CoreUnavailableError
from efb_wechat_comwechat_slave.EffectLedger import (
    EFFECT_KIND_DELIVERY,
    STATE_DELIVERED,
    STATE_MEDIA_FAILED,
    STATE_PENDING_MEDIA,
)
from efb_wechat_comwechat_slave.Provenance import (
    CLASSIFICATION_FIRST_BUSINESS_EFFECT,
    CLASSIFICATION_INDETERMINATE,
    CLASSIFICATION_REPROJECTION_OF_PREEXISTING_OBJECT,
    PROVENANCE_INDETERMINATE_CORE_READ_ABSENT,
    PROVENANCE_INDETERMINATE_CORE_READ_FAILED,
    PROVENANCE_INDETERMINATE_MALFORMED_CREATED_AT,
    PROVENANCE_INDETERMINATE_MISSING_CREATED_AT,
    ProvenanceDeferralStore,
    SubscriptionFloorRecord,
    SubscriptionFloorStore,
    extract_business_origin,
    parse_utc,
)

from test_rc14_reprojection_idempotency import (  # noqa: E402  (path set above)
    NEW_BUSINESS_FIRST,
    NEW_BUSINESS_LAST,
    POST_FLOOR_CREATED_AT,
    PRE_FLOOR_CREATED_AT,
    PRODUCTION_FLOOR,
    W1_LAST_CURSOR,
    W2_FIRST_CURSOR,
    OfflineCore,
    ReprojectionTestBase,
    _event_from_row,
    _load_fixture_rows,
    _load_provenance_fixture,
)

CHAT = "38808757431@chatroom"
ACCOUNT = "f-live-a"
HARDENING_CONSUMER = "rc14-hardening"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


class HardeningTestBase(ReprojectionTestBase):
    """Shared harness: real production floor, deterministic chat registration."""

    def setUp(self) -> None:
        super().setUp()
        # Rebuild under this module's consumer id so every assertion and every durable
        # store is scoped consistently across restarts.
        self.channel.stop_polling()
        self.channel = self._new_channel(consumer_id=HARDENING_CONSUMER)
        self._register_chat(ACCOUNT, CHAT)

    def _fresh_channel(self):
        """Drop the current isolated state and start a brand new channel."""
        self.channel.stop_polling()
        shutil.rmtree(self.data_path, ignore_errors=True)
        self.data_path = (
            Path(__file__).resolve().parents[1] / ".tmp" / f"rc14-repro-{uuid.uuid4().hex}"
        )
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.deliveries = []
        self.channel = self._new_channel(consumer_id=HARDENING_CONSUMER)
        self._register_chat(ACCOUNT, CHAT)
        return self.channel

    def _restart(self, *, core: OfflineCore | None = None):
        """Reconstruct the channel on the same durable data path (a real restart)."""
        self.channel.stop_polling()
        self.channel = self._new_channel(core=core, consumer_id=HARDENING_CONSUMER)
        self._register_chat(ACCOUNT, CHAT)
        return self.channel

    def _classify(self, message, *, account_id: str = ACCOUNT):
        return self.channel._classify_unknown_effect(
            account_id, message, has_effect_identity=bool(message.get("message_id"))
        )

    def _floor_path(self) -> Path:
        return self.data_path / "core-subscription-floor.json"

    def _deferral_path(self) -> Path:
        return self.data_path / "core-provenance-deferrals.json"

    def _ledger_path(self) -> Path:
        return self.data_path / "core-effect-ledger.sqlite3"

    def _mapping_path(self) -> Path:
        return self.data_path / "core-message-mapping.sqlite3"

    def _cursor_path(self) -> Path:
        return self.data_path / "core-event-cursor.json"


# --------------------------------------------------------------------------- #
# Section 1 — durable subscription provenance
# --------------------------------------------------------------------------- #


class TestDurableSubscriptionFloor(HardeningTestBase):
    """§1: restart-safe, scoped, non-destructive subscription provenance."""

    def test_floor_is_persisted_with_core_cursor_and_timestamp(self) -> None:
        floor = self._floor(ACCOUNT)
        self.assertIsNotNone(floor)
        self.assertEqual(272037, floor.subscription_floor_cursor)
        self.assertEqual(parse_utc("2026-09-15T11:18:09Z"), floor.subscription_floor_at)
        self.assertEqual("bounded_window", floor.bootstrap_mode)
        self.assertEqual("governed_rebootstrap", floor.bootstrap_source)
        self.assertEqual("core_bootstrap", floor.provenance_source)
        self.assertTrue(floor.complete)
        self.assertFalse(floor.floor_at_is_derived)
        self.assertTrue(self._floor_path().exists(), "the floor must be durable, not memory-only")
        on_disk = json.loads(self._floor_path().read_text(encoding="utf-8"))
        self.assertEqual(1, on_disk["schema_version"])
        self.assertEqual(self.channel.consumer_id, on_disk["consumer_id"])

    def test_floor_scope_is_consumer_and_account(self) -> None:
        snapshot = self.channel.subscription_floor.snapshot()
        self.assertEqual(self.channel.consumer_id, snapshot["consumer_id"])
        self.assertTrue(snapshot["durable"])
        scoped = snapshot["accounts"]
        for account_id in ("f-live-a", "testB", "account-1", "acc-1", "acc-2"):
            self.assertIn(account_id, scoped)
            record = scoped[account_id]
            self.assertEqual(self.channel.consumer_id, record["consumer_id"])
            self.assertEqual(account_id, record["account_id"])
            self.assertEqual(272037, record["subscription_floor_cursor"])

    def test_floor_survives_core_being_unreachable(self) -> None:
        # The floor is written while Core answers...
        self.assertEqual(272037, self._floor(ACCOUNT).subscription_floor_cursor)
        # ...then Core goes away entirely.
        offline = OfflineCore(bootstrap_error=CoreUnavailableError("core is down"))
        self._restart(core=offline)
        floor = self._floor(ACCOUNT)
        self.assertIsNotNone(floor, "the durable floor must outlive Core availability")
        self.assertEqual(272037, floor.subscription_floor_cursor)
        self.assertEqual(parse_utc("2026-09-15T11:18:09Z"), floor.subscription_floor_at)
        # And the classification still works with no Core at all.
        classification, _ = self._classify(self._msg(message_id="offline-1"))
        self.assertEqual(CLASSIFICATION_FIRST_BUSINESS_EFFECT, classification)
        classification, _ = self._classify(
            self._msg(message_id="offline-2", created_at=PRE_FLOOR_CREATED_AT)
        )
        self.assertEqual(CLASSIFICATION_REPROJECTION_OF_PREEXISTING_OBJECT, classification)

    def test_floor_is_byte_stable_across_restarts(self) -> None:
        before = self._floor_path().read_bytes()
        self._restart()
        self._restart()
        after = self._floor_path().read_bytes()
        self.assertEqual(
            before,
            after,
            "an unchanged Core floor must not be rewritten by a restart",
        )

    def test_floor_reanchors_on_rebootstrap_and_keeps_history(self) -> None:
        original = self._floor(ACCOUNT)
        rebootstrap = dict(PRODUCTION_FLOOR)
        rebootstrap["initial_cursor"] = 274800
        rebootstrap["bootstrap_at"] = "2026-09-17T08:00:00Z"
        rebootstrap["bootstrap_source"] = "governed_rebootstrap"
        self._restart(core=OfflineCore(bootstrap=rebootstrap))
        reanchored = self._floor(ACCOUNT)
        self.assertEqual(274800, reanchored.subscription_floor_cursor)
        self.assertEqual(parse_utc("2026-09-17T08:00:00Z"), reanchored.subscription_floor_at)
        snapshot = self.channel.subscription_floor.snapshot()
        # The floor is scoped per account, so re-anchoring supersedes exactly one
        # record per scoped account: every superseded anchor is retained, none lost.
        self.assertEqual(
            len(snapshot["accounts"]),
            snapshot["history_entries"],
            "every superseded per-account anchor is retained",
        )
        on_disk = json.loads(self._floor_path().read_text(encoding="utf-8"))
        self.assertEqual(
            {original.subscription_floor_cursor},
            {entry["previous"]["subscription_floor_cursor"] for entry in on_disk["history"]},
        )

    def test_floor_initialisation_does_not_touch_ledger_mapping_or_checkpoint(self) -> None:
        ledger_before = _sha256(self._ledger_path())
        ledger_rows_before = self.channel.effect_ledger.count_effects()
        mapping_before = _sha256(self._mapping_path())
        cursor_before = self._cursor_path().read_bytes()

        self._restart()  # re-runs floor establishment from scratch
        self.channel._ensure_subscription_floor()

        self.assertEqual(ledger_before, _sha256(self._ledger_path()))
        self.assertEqual(ledger_rows_before, self.channel.effect_ledger.count_effects())
        self.assertEqual(mapping_before, _sha256(self._mapping_path()))
        self.assertEqual(cursor_before, self._cursor_path().read_bytes())
        self.assertTrue(self._floor_path().exists())

    def test_cursor_alignment_uses_the_same_immutable_anchor(self) -> None:
        cursor = json.loads(self._cursor_path().read_text(encoding="utf-8"))
        self.assertEqual("272037", str(cursor["cursor"]))
        self.assertEqual(
            self._floor(ACCOUNT).subscription_floor_cursor,
            int(cursor["cursor"]),
            "the resume cursor and the subscription floor share Core's initial_cursor",
        )

    def test_derived_floor_is_disclosed_when_bootstrap_at_is_absent(self) -> None:
        legacy = dict(PRODUCTION_FLOOR)
        legacy["bootstrap_at"] = ""
        legacy["bootstrap_mode"] = "legacy"
        core = OfflineCore(bootstrap=legacy)
        core.poll_events = lambda **_kwargs: {  # type: ignore[assignment]
            "events": [{"cursor": "272037", "occurred_at": "2026-09-15T11:20:00Z"}],
        }
        self._restart(core=core)
        floor = self._floor(ACCOUNT)
        self.assertTrue(floor.floor_at_is_derived)
        self.assertEqual("core_events_poll", floor.provenance_source)
        self.assertEqual(parse_utc("2026-09-15T11:20:00Z"), floor.subscription_floor_at)


# --------------------------------------------------------------------------- #
# Section 2 — unknown identity classification
# --------------------------------------------------------------------------- #


class TestUnknownIdentityClassification(HardeningTestBase):
    """§2/§5(a)/§5(b): the origin, not the event type, decides."""

    def test_unknown_update_after_floor_is_a_first_business_effect(self) -> None:
        """§5(a) — the most important new regression of this round."""
        message = self._msg(message_id="post-floor", created_at=POST_FLOOR_CREATED_AT)
        self.channel._handle_event(self._event("message.updated", message))

        self.assertEqual(1, len(self.deliveries), "a post-floor update is genuinely new")
        self.assertEqual(STATE_DELIVERED, self._status(ACCOUNT, "post-floor"))
        self.assertEqual(1, self._row_count())
        counts = self._provenance_counts()
        self.assertEqual(1, counts[CLASSIFICATION_FIRST_BUSINESS_EFFECT])
        self.assertEqual(0, counts[CLASSIFICATION_REPROJECTION_OF_PREEXISTING_OBJECT])
        self.assertEqual(0, counts[CLASSIFICATION_INDETERMINATE])

    def test_unknown_update_before_floor_is_a_reprojection(self) -> None:
        """§5(b) — no external effect and no ledger write."""
        message = self._msg(message_id="pre-floor", created_at=PRE_FLOOR_CREATED_AT)
        self.channel._handle_event(self._event("message.updated", message))

        self.assertEqual(0, len(self.deliveries))
        self.assertIsNone(self._status(ACCOUNT, "pre-floor"))
        self.assertEqual(0, self._row_count(), "a re-projection must not write a ledger row")
        self.assertEqual(0, self.channel.provenance_deferrals.count())
        counts = self._provenance_counts()
        self.assertEqual(1, counts[CLASSIFICATION_REPROJECTION_OF_PREEXISTING_OBJECT])

    def test_origin_exactly_at_the_floor_is_treated_as_new(self) -> None:
        message = self._msg(message_id="boundary", created_at="2026-09-15T11:18:09Z")
        self.channel._handle_event(self._event("message.updated", message))
        self.assertEqual(1, len(self.deliveries))
        self.assertEqual(STATE_DELIVERED, self._status(ACCOUNT, "boundary"))

    def test_known_effect_is_decided_by_the_state_machine_not_provenance(self) -> None:
        # A terminal identity is closed even when its origin is after the floor, so a
        # post-floor re-projection of an already-delivered object is still suppressed.
        message = self._msg(message_id="known", created_at=POST_FLOOR_CREATED_AT)
        self.channel._handle_event(self._event("message.created", message))
        self.assertEqual(1, len(self.deliveries))
        # The first event is genuinely a first business effect: the identity was not
        # yet established, so provenance decided it — exactly once.
        after_first = self._provenance_counts()
        self.assertEqual(1, after_first[CLASSIFICATION_FIRST_BUSINESS_EFFECT])

        self.channel._handle_event(self._event("message.updated", message))
        self.channel._handle_event(self._event("message.updated", message))

        self.assertEqual(1, len(self.deliveries), "known terminal effect is closed")
        self.assertEqual(1, self._row_count())
        counts = self._provenance_counts()
        self.assertEqual(
            after_first[CLASSIFICATION_FIRST_BUSINESS_EFFECT],
            counts[CLASSIFICATION_FIRST_BUSINESS_EFFECT],
            "provenance must not be consulted again for a known identity",
        )
        self.assertEqual(0, counts[CLASSIFICATION_REPROJECTION_OF_PREEXISTING_OBJECT])

    def test_cursor_provenance_is_preferred_over_created_at(self) -> None:
        """If Core ever exposes an immutable origin cursor it wins over the clock."""
        # A cursor origin below the floor suppresses even though created_at is post-floor.
        suppressed = self._msg(
            message_id="cursor-pre",
            created_at=POST_FLOOR_CREATED_AT,
            origin_cursor=272036,
        )
        self.channel._handle_event(self._event("message.updated", suppressed))
        self.assertEqual(0, len(self.deliveries))
        self.assertEqual(0, self._row_count())

        # A cursor origin at/above the floor is new even though created_at is pre-floor.
        delivered = self._msg(
            message_id="cursor-post",
            created_at=PRE_FLOOR_CREATED_AT,
            origin_cursor=272037,
        )
        self.channel._handle_event(self._event("message.updated", delivered))
        self.assertEqual(1, len(self.deliveries))
        self.assertEqual(STATE_DELIVERED, self._status(ACCOUNT, "cursor-post"))

    def test_media_progression_of_a_post_floor_object_happens_exactly_once(self) -> None:
        self._restart(core=OfflineCore(media_ready=False))
        message = self._media_msg("progress", status="original_pending")
        self.channel._handle_event(self._event("message.updated", message))
        self.assertEqual(STATE_PENDING_MEDIA, self._status(ACCOUNT, "progress"))
        self.assertEqual([], self.deliveries)

        self.core.media_ready = True
        for _ in range(3):
            self.channel._handle_message_event(
                "message.updated",
                ACCOUNT,
                self._media_msg("progress", status="ready"),
            )
        self.assertEqual(1, len(self.deliveries), "exactly one external delivery")
        self.assertEqual(STATE_DELIVERED, self._status(ACCOUNT, "progress"))
        self.assertEqual(1, self._row_count())


# --------------------------------------------------------------------------- #
# Section 3 — event type is a routing signal only
# --------------------------------------------------------------------------- #


class TestEventTypeIsRoutingOnly(HardeningTestBase):
    """§3: the event type selects a handler and nothing else."""

    def test_pre_floor_object_is_suppressed_under_both_event_types(self) -> None:
        for event_type in ("message.created", "message.updated"):
            with self.subTest(event_type=event_type):
                self._fresh_channel()
                message = self._msg(message_id=f"pre-{event_type}", created_at=PRE_FLOOR_CREATED_AT)
                self.channel._handle_event(self._event(event_type, message))
                self.assertEqual(
                    0,
                    len(self.deliveries),
                    f"{event_type} must not make a pre-existing object deliverable",
                )
                self.assertEqual(0, self._row_count())

    def test_post_floor_object_is_delivered_under_both_event_types(self) -> None:
        for event_type in ("message.created", "message.updated"):
            with self.subTest(event_type=event_type):
                self._fresh_channel()
                message = self._msg(message_id=f"post-{event_type}", created_at=POST_FLOOR_CREATED_AT)
                self.channel._handle_event(self._event(event_type, message))
                self.assertEqual(
                    1,
                    len(self.deliveries),
                    f"{event_type} must not make a new object undeliverable",
                )
                self.assertEqual(STATE_DELIVERED, self._status(ACCOUNT, f"post-{event_type}"))

    def test_real_new_business_is_recognised_under_a_flipped_event_type(self) -> None:
        """The three real new objects stay new even when relabelled message.updated."""
        rows = _load_fixture_rows()
        _floor, provenance, _ledger = _load_provenance_fixture()
        new_rows = [r for r in rows if NEW_BUSINESS_FIRST <= r["cursor"] <= NEW_BUSINESS_LAST]
        self.assertEqual(3, len(new_rows))
        for row in new_rows:
            event = _event_from_row(row, provenance)
            self.assertEqual("message.created", event["event_type"])
            # Flip the routing label; the provenance decision must not move.
            event["event_type"] = "message.updated"
            self.channel._handle_event(event)
        counts = self._provenance_counts()
        self.assertEqual(3, counts[CLASSIFICATION_FIRST_BUSINESS_EFFECT])
        self.assertEqual(2, len(self.deliveries))
        self.assertEqual(
            STATE_PENDING_MEDIA,
            self._status(ACCOUNT, new_rows[2]["message_id"]),
        )

    def test_real_reprojection_is_suppressed_under_a_flipped_event_type(self) -> None:
        rows = _load_fixture_rows()
        _floor, provenance, _ledger = _load_provenance_fixture()
        pre = [
            r
            for r in rows
            if r["event_type"] == "message.updated"
            and r["cursor"] <= W1_LAST_CURSOR
            and provenance[str(r["cursor"])][1] < PRODUCTION_FLOOR["bootstrap_at"]
        ]
        self.assertEqual(387, len(pre))
        for row in pre:
            event = _event_from_row(row, provenance)
            event["event_type"] = "message.created"  # the strongest possible relabel
            self.channel._handle_event(event)
        counts = self._provenance_counts()
        self.assertEqual(387, counts[CLASSIFICATION_REPROJECTION_OF_PREEXISTING_OBJECT])
        self.assertEqual(0, len(self.deliveries))
        self.assertEqual(0, self._row_count())


# --------------------------------------------------------------------------- #
# Section 4 — the historical ledger stays authoritative
# --------------------------------------------------------------------------- #


class TestHistoricalLedgerCompatibility(HardeningTestBase):
    """§4: the 417 production rows are additive-compatible and never rewritten."""

    def _build_legacy_ledger(self, path: Path, triples) -> None:
        """Recreate the production ledger with the pre-``effect_kind`` schema."""
        conn = sqlite3.connect(str(path))
        try:
            conn.execute(
                """
                CREATE TABLE effect_ledger (
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
            for index, (account_id, message_id, status) in enumerate(triples):
                conn.execute(
                    "INSERT INTO effect_ledger (consumer_id, effect_id, account_id, message_id,"
                    " efb_uid, event_type, status, created_at, updated_at, details_json)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        HARDENING_CONSUMER,
                        f"{account_id}:{message_id}",
                        account_id,
                        message_id,
                        message_id,
                        "message.created",
                        status,
                        f"2026-09-16T00:00:{index % 60:02d}Z",
                        f"2026-09-16T00:00:{index % 60:02d}Z",
                        "{}",
                    ),
                )
            conn.commit()
        finally:
            conn.close()

    def test_production_shaped_rows_survive_the_additive_migration(self) -> None:
        _floor, _provenance, triples = _load_provenance_fixture()
        self.assertEqual(417, len(triples))
        ledger_path = self._ledger_path()
        self.channel.effect_ledger.close()
        for suffix in ("", "-wal", "-shm"):
            sidecar = Path(str(ledger_path) + suffix)
            if sidecar.exists():
                sidecar.unlink()
        self._build_legacy_ledger(ledger_path, triples)
        before = _sha256(ledger_path)

        self._restart()  # reopens the ledger, which runs the additive migration

        after = _sha256(ledger_path)
        self.assertNotEqual(before, after, "the additive column migration must have run")
        conn = sqlite3.connect(str(ledger_path))
        try:
            rows = conn.execute(
                "SELECT account_id, message_id, status, effect_id, effect_kind FROM effect_ledger"
            ).fetchall()
        finally:
            conn.close()
        self.assertEqual(417, len(rows))
        self.assertEqual(
            {(a, m, s) for a, m, s in triples},
            {(a, m, s) for a, m, s, _e, _k in rows},
            "every historical identity and status is preserved verbatim",
        )
        self.assertEqual({EFFECT_KIND_DELIVERY}, {k for *_rest, k in rows})
        self.assertEqual(
            {f"{a}:{m}" for a, m, _s in triples},
            {r[3] for r in rows},
            "the historical effect_id encoding is unchanged",
        )

    def test_hardened_code_keeps_the_production_row_count(self) -> None:
        _floor, _provenance, triples = _load_provenance_fixture()
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
                ledger.mark_media_failed(
                    self.channel.consumer_id,
                    ledger.compute_effect_id(account_id, message_id),
                    account_id=account_id,
                    message_id=message_id,
                    event_type="message.created",
                    reason="seeded",
                )
        self.assertEqual(417, self._row_count())
        self._restart()
        self.assertEqual(417, self._row_count(), "a restart must not migrate or drop rows")
        counts = self.channel.effect_ledger.status_counts(self.channel.consumer_id)
        self.assertEqual(375, counts[STATE_DELIVERED])
        self.assertEqual(42, counts[STATE_MEDIA_FAILED])


# --------------------------------------------------------------------------- #
# Section 6 — indeterminate provenance fails closed
# --------------------------------------------------------------------------- #


class TestIndeterminateFailsClosed(HardeningTestBase):
    """§6: no guessing, no external effect, no terminal ledger write, retry allowed."""

    def _assert_indeterminate(self, message, *, expected_reason: str = "") -> None:
        before_rows = self._row_count()
        self.channel._handle_event(self._event("message.updated", message))
        self.assertEqual([], self.deliveries, "no external effect")
        self.assertEqual(before_rows, self._row_count(), "no terminal ledger write")
        self.assertEqual(
            0,
            self.channel.effect_ledger.status_counts(HARDENING_CONSUMER)[STATE_MEDIA_FAILED],
            "no fabricated terminal MEDIA_FAILED",
        )
        self.assertEqual(1, self.channel.provenance_deferrals.count(), "parked for retry")
        self.assertEqual(1, self._provenance_counts()[CLASSIFICATION_INDETERMINATE])
        if expected_reason:
            stored = self.channel.provenance_deferrals.due()[0]
            self.assertEqual(expected_reason, stored["last_reason"])

    def test_missing_created_at_and_no_core_projection_fails_closed(self) -> None:
        message = self._msg(message_id="no-origin")
        message.pop("created_at")
        self._assert_indeterminate(
            message, expected_reason=PROVENANCE_INDETERMINATE_CORE_READ_ABSENT
        )

    def test_missing_created_at_with_the_core_read_disabled_fails_closed(self) -> None:
        self.channel.provenance_core_read_enabled = False
        message = self._msg(message_id="no-origin-no-read")
        message.pop("created_at")
        self._assert_indeterminate(
            message, expected_reason=PROVENANCE_INDETERMINATE_MISSING_CREATED_AT
        )

    def test_core_projection_read_failure_fails_closed(self) -> None:
        self._restart(core=OfflineCore(projection_error=CoreUnavailableError("timeout")))
        message = self._msg(message_id="read-fail")
        message.pop("created_at")
        self._assert_indeterminate(message, expected_reason=PROVENANCE_INDETERMINATE_CORE_READ_FAILED)

    def test_core_projection_404_fails_closed(self) -> None:
        self._restart(core=OfflineCore(projection_error=CoreAPIError(404, "not_found", "no chat")))
        message = self._msg(message_id="read-404")
        message.pop("created_at")
        self._assert_indeterminate(message, expected_reason=PROVENANCE_INDETERMINATE_CORE_READ_FAILED)

    def test_core_projection_without_created_at_fails_closed(self) -> None:
        self._restart(core=OfflineCore(projection={"message_id": "projection-empty"}))
        message = self._msg(message_id="projection-empty")
        message.pop("created_at")
        self._assert_indeterminate(
            message, expected_reason=PROVENANCE_INDETERMINATE_MISSING_CREATED_AT
        )

    def test_malformed_created_at_fails_closed(self) -> None:
        message = self._msg(message_id="malformed", created_at="not-a-timestamp")
        self._assert_indeterminate(
            message, expected_reason=PROVENANCE_INDETERMINATE_MALFORMED_CREATED_AT
        )

    def test_indeterminate_media_does_not_become_media_failed(self) -> None:
        message = self._media_msg("indeterminate-media")
        message.pop("created_at")
        self._assert_indeterminate(message)
        self.assertIsNone(self._status(ACCOUNT, "indeterminate-media"))

    def test_indeterminate_is_retryable_and_resolves_once_core_recovers(self) -> None:
        message = self._msg(message_id="recover")
        message.pop("created_at")
        self.channel._handle_event(self._event("message.updated", message))
        self.assertEqual([], self.deliveries)
        self.assertEqual(1, self.channel.provenance_deferrals.count())

        # Core recovers and now answers the authoritative projection read.
        self.core.projection = {
            "message_id": "recover",
            "created_at": POST_FLOOR_CREATED_AT,
        }
        self.channel._retry_deferred_provenance()
        self.assertEqual(1, len(self.deliveries))
        self.assertEqual(STATE_DELIVERED, self._status(ACCOUNT, "recover"))
        self.assertEqual(0, self.channel.provenance_deferrals.count(), "the deferral resolved")

        # A second retry must not produce a second effect.
        self.channel._retry_deferred_provenance()
        self.assertEqual(1, len(self.deliveries))
        self.assertEqual(1, self._row_count())

    def test_exhausted_deferrals_stop_retrying_without_any_external_effect(self) -> None:
        self.channel.provenance_deferrals.max_attempts = 2
        message = self._msg(message_id="exhaust")
        message.pop("created_at")
        for _ in range(4):
            self.channel._handle_event(self._event("message.updated", message))
        self.assertEqual([], self.deliveries)
        self.assertEqual(0, self._row_count())
        self.assertEqual(0, len(self.channel.provenance_deferrals.due()))
        self.assertEqual(1, self.channel.provenance_deferrals.exhausted())
        self.assertEqual(0, self.channel._retry_deferred_provenance())


# --------------------------------------------------------------------------- #
# Section 7 — restart safety
# --------------------------------------------------------------------------- #


class TestRestartProvenanceSafety(HardeningTestBase):
    """§7: after a restart the floor and every classification are unchanged."""

    def test_restart_preserves_floor_and_decision(self) -> None:
        floor_before = self._floor(ACCOUNT)
        floor_file_before = json.loads(self._floor_path().read_text(encoding="utf-8"))
        message = self._msg(message_id="restart-pre", created_at=PRE_FLOOR_CREATED_AT)
        self.channel._handle_event(self._event("message.updated", message))
        counts_before = self._provenance_counts()

        self._restart()

        floor_after = self._floor(ACCOUNT)
        self.assertEqual(floor_before, floor_after, "the durable floor must be identical")
        self.assertEqual(
            floor_file_before,
            json.loads(self._floor_path().read_text(encoding="utf-8")),
            "an unchanged floor must not be rewritten by the restart",
        )
        # Replay the identical re-projection after the restart.
        self.channel._handle_event(self._event("message.updated", message))
        self.assertEqual(0, len(self.deliveries), "no duplicate external effect after restart")
        self.assertEqual(0, self._row_count())
        # The provenance counters are per-channel in-memory diagnostics, so a restart
        # restarts them at zero. The guarantee is that the *decision* is unchanged:
        # the replayed event is still classified as a re-projection.
        self.assertEqual(1, counts_before[CLASSIFICATION_REPROJECTION_OF_PREEXISTING_OBJECT])
        self.assertEqual(
            1,
            self._provenance_counts()[CLASSIFICATION_REPROJECTION_OF_PREEXISTING_OBJECT],
            "the post-restart replay is still classified as a re-projection",
        )

    def test_restart_replays_the_real_fixture_without_duplicate_effects(self) -> None:
        rows = _load_fixture_rows()
        _floor, provenance, ledger_triples = _load_provenance_fixture()
        for account_id, chat_id in sorted(
            {(r["account_id"], r["chat_id"]) for r in rows if r["chat_id"]}
        ):
            self._register_chat(account_id, chat_id)
        ledger = self.channel.effect_ledger
        for account_id, message_id, status in ledger_triples:
            if status == STATE_DELIVERED:
                ledger.record_delivered(
                    self.channel.consumer_id,
                    ledger.compute_effect_id(account_id, message_id),
                    account_id=account_id,
                    message_id=message_id,
                    efb_uid=message_id,
                )
            else:
                ledger.mark_media_failed(
                    self.channel.consumer_id,
                    ledger.compute_effect_id(account_id, message_id),
                    account_id=account_id,
                    message_id=message_id,
                    event_type="message.created",
                    reason="seeded",
                )
        window = [
            r
            for r in rows
            if (r["event_type"] == "message.updated" and r["cursor"] <= W1_LAST_CURSOR)
            or (r["event_type"] == "message.updated" and r["cursor"] >= W2_FIRST_CURSOR)
            or NEW_BUSINESS_FIRST <= r["cursor"] <= NEW_BUSINESS_LAST
        ]
        self.assertEqual(831, len(window))

        floor_before = self._floor(ACCOUNT)
        for row in window:
            self.channel._handle_event(_event_from_row(row, provenance))
        first_pass_deliveries = len(self.deliveries)
        first_pass_rows = self._row_count()
        self.assertEqual(2, first_pass_deliveries)
        # 417 seeded identities + 2 first deliveries + 1 fail-safe pending row for the
        # 274307 file/empty-media_id gap = 420. The status partition asserted below
        # (377 DELIVERED + 42 MEDIA_FAILED + 1 PENDING_MEDIA) must sum to the same.
        self.assertEqual(420, first_pass_rows)

        # Restart, then replay the entire window again.
        self._restart()
        self.assertEqual(floor_before, self._floor(ACCOUNT), "RESTART_PROVENANCE_SAFE")
        for row in window:
            self.channel._handle_event(_event_from_row(row, provenance))

        self.assertEqual(
            2,
            len(self.deliveries),
            "W1_DUPLICATE_EXTERNAL_DELIVERY / W2_DUPLICATE_EXTERNAL_DELIVERY must stay 0",
        )
        self.assertEqual(420, self._row_count())
        counts = self.channel.effect_ledger.status_counts(self.channel.consumer_id)
        self.assertEqual(377, counts[STATE_DELIVERED])
        self.assertEqual(42, counts[STATE_MEDIA_FAILED])
        self.assertEqual(1, counts[STATE_PENDING_MEDIA])
        self.assertEqual(0, self.channel.provenance_deferrals.count())


# --------------------------------------------------------------------------- #
# Unit level: the provenance primitives
# --------------------------------------------------------------------------- #


class TestProvenancePrimitives(unittest.TestCase):
    """The decision function itself, independent of the channel."""

    def test_origin_extraction_prefers_cursor_then_created_at(self) -> None:
        origin, reason = extract_business_origin({"origin_cursor": "12", "created_at": "2026-01-01T00:00:00Z"})
        self.assertEqual("origin_cursor", origin.kind)
        self.assertEqual(12, origin.cursor)
        self.assertEqual("", reason)

        origin, reason = extract_business_origin({"created_at": "2026-01-01T00:00:00Z"})
        self.assertEqual("message_created_at", origin.kind)
        self.assertEqual(parse_utc("2026-01-01T00:00:00Z"), origin.at)

        origin, reason = extract_business_origin({})
        self.assertIsNone(origin)
        self.assertEqual(PROVENANCE_INDETERMINATE_MISSING_CREATED_AT, reason)

        origin, reason = extract_business_origin({"created_at": "yesterday"})
        self.assertIsNone(origin)
        self.assertEqual(PROVENANCE_INDETERMINATE_MALFORMED_CREATED_AT, reason)

    def test_floor_classification_matrix(self) -> None:
        record = SubscriptionFloorRecord(
            consumer_id="c",
            account_id="a",
            subscription_floor_cursor=272037,
            subscription_floor_at=parse_utc("2026-09-15T11:18:09Z"),
        )
        pre = extract_business_origin({"created_at": "2026-09-01T00:00:00Z"})[0]
        post = extract_business_origin({"created_at": "2026-09-16T00:00:00Z"})[0]
        pre_cursor = extract_business_origin({"origin_cursor": "272036"})[0]
        post_cursor = extract_business_origin({"origin_cursor": "272037"})[0]
        self.assertEqual(CLASSIFICATION_REPROJECTION_OF_PREEXISTING_OBJECT, record.classify(pre))
        self.assertEqual(CLASSIFICATION_FIRST_BUSINESS_EFFECT, record.classify(post))
        self.assertEqual(
            CLASSIFICATION_REPROJECTION_OF_PREEXISTING_OBJECT, record.classify(pre_cursor)
        )
        self.assertEqual(CLASSIFICATION_FIRST_BUSINESS_EFFECT, record.classify(post_cursor))
        self.assertEqual(CLASSIFICATION_INDETERMINATE, record.classify(None))

        incomplete = SubscriptionFloorRecord(
            consumer_id="c", account_id="a", subscription_floor_cursor=None, subscription_floor_at=None
        )
        self.assertFalse(incomplete.complete)
        self.assertEqual(CLASSIFICATION_INDETERMINATE, incomplete.classify(post))

    def test_deferral_store_is_bounded_and_separate(self) -> None:
        scratch = Path(__file__).resolve().parents[1] / ".tmp" / f"prov-defer-{uuid.uuid4().hex}"
        path = scratch / "core-provenance-deferrals.json"
        scratch.mkdir(parents=True, exist_ok=True)
        try:
            store = ProvenanceDeferralStore(path, "c", max_attempts=2, max_entries=2)
            message = {"message_id": "m1", "chat_id": "c1"}
            self.assertEqual(1, store.defer("a", message, "message.updated", reason="r"))
            self.assertEqual(2, store.defer("a", message, "message.updated", reason="r"))
            self.assertEqual(0, len(store.due()), "the attempt budget is spent")
            self.assertEqual(1, store.exhausted())
            store.defer("a", {"message_id": "m2", "chat_id": "c1"}, "message.updated", reason="r")
            store.defer("a", {"message_id": "m3", "chat_id": "c1"}, "message.updated", reason="r")
            self.assertEqual(2, store.count(), "the store is bounded by max_entries")
            self.assertTrue(store.resolve("a", "m2"))
            self.assertFalse(store.resolve("a", "m2"))
            self.assertEqual(1, store.count())
            self.assertNotIn("effect_ledger", path.read_text(encoding="utf-8"))
        finally:
            shutil.rmtree(scratch, ignore_errors=True)

    def test_floor_store_is_a_noop_when_core_reports_nothing(self) -> None:
        scratch = Path(__file__).resolve().parents[1] / ".tmp" / f"prov-floor-{uuid.uuid4().hex}"
        path = scratch / "core-subscription-floor.json"
        scratch.mkdir(parents=True, exist_ok=True)
        try:
            store = SubscriptionFloorStore(path, "c")
            snapshot = store.ensure_from_core(OfflineCore(bootstrap=None), ["a"])
            self.assertEqual("unavailable", snapshot["source"])
            self.assertFalse(snapshot["durable"])
            self.assertIsNone(store.get("a"))
        finally:
            shutil.rmtree(scratch, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
