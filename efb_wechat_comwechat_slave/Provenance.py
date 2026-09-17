"""Durable subscription provenance and fail-closed unknown-identity classification.

Why this module exists
----------------------
RC.14 established (Case C) that the F3 Core re-projection windows are not
replay-idempotent, and that a single-cursor checkpoint fence cannot repair them
because genuinely new business sits adjacent to the re-projection.

The first corrective engineering round closed the two defects that made a replay
observable as a second external effect (``R14-EFB-R1`` terminal-state gating and
``R14-EFB-R2`` unestablished-update dispatch), but the R2 guard decided
"this is a re-projection" from ``event_type == "message.updated"``.  That is not a
sound basis: Core emits ``message.updated`` whenever a row it already holds changes
digest, which includes objects that were created *after* this consumer subscribed
and that this consumer has never delivered.  Classifying those as re-projections
would silently drop real business.

This module supplies the two durable facts the decision actually needs:

1. **Subscription provenance** (:class:`SubscriptionFloorStore`) — the immutable,
   Core-authoritative cursor (``initial_cursor``) and wall clock (``bootstrap_at``)
   at which this consumer's subscription began, persisted per ``consumer_id`` and
   ``account_id`` so the decision survives a restart without depending on memory.

2. **A fail-closed deferral ledger** (:class:`ProvenanceDeferralStore`) — when Core
   cannot supply a business origin, nothing is guessed and nothing is delivered;
   the event is parked in a *separate*, additive store so a later retry can resolve
   it.  It is deliberately not the :class:`EffectLedger`: the effect ledger holds
   the immutable 417-row delivery history and must never receive a synthetic row
   for a decision that was never made.

Neither store touches the ``EffectLedger``, the message mapping DB or the Core
checkpoint, and both are pure additions on disk.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

PROVENANCE_SCHEMA_VERSION = 1

# --------------------------------------------------------------------------- #
# Classification vocabulary
# --------------------------------------------------------------------------- #

#: The effect identity is already known to this consumer. The durable effect state
#: machine decides; provenance is not consulted at all.
CLASSIFICATION_KNOWN_EFFECT = "KNOWN_EFFECT"

#: No effect identity exists yet and the object's business origin is at or after the
#: subscription floor: this is the first time this consumer can observe the object,
#: so it must be delivered like any other first business effect.
CLASSIFICATION_FIRST_BUSINESS_EFFECT = "FIRST_BUSINESS_EFFECT"

#: No effect identity exists and the object's business origin predates the
#: subscription floor: the object existed before this consumer subscribed, so the
#: event is Core re-projecting something this consumer never owed a delivery for.
CLASSIFICATION_REPROJECTION_OF_PREEXISTING_OBJECT = "REPROJECTION_OF_PREEXISTING_OBJECT"

#: The business origin could not be established from Core's authoritative
#: projection. Nothing is guessed: no external effect and no terminal ledger write.
CLASSIFICATION_INDETERMINATE = "INDETERMINATE"

#: Reasons an origin lookup can end in :data:`CLASSIFICATION_INDETERMINATE`.
PROVENANCE_INDETERMINATE_MISSING_PROJECTION = "core_projection_absent"
PROVENANCE_INDETERMINATE_MISSING_CREATED_AT = "core_projection_has_no_created_at"
PROVENANCE_INDETERMINATE_MALFORMED_CREATED_AT = "core_projection_created_at_malformed"
PROVENANCE_INDETERMINATE_CORE_READ_FAILED = "core_provenance_read_failed"
PROVENANCE_INDETERMINATE_CORE_READ_ABSENT = "core_provenance_read_returned_no_object"
PROVENANCE_INDETERMINATE_FLOOR_UNAVAILABLE = "subscription_floor_unavailable"
PROVENANCE_INDETERMINATE_NO_EFFECT_IDENTITY = "no_durable_effect_identity"

# --------------------------------------------------------------------------- #
# Business origin
# --------------------------------------------------------------------------- #

#: Business-origin *cursor* fields, in priority order. Core V1 (F3 ``6538f79``) does
#: not emit any of them today: the ``events`` table has no message-origin cursor and
#: the ``messages`` table has no first-seen cursor, so a re-projection cannot be told
#: apart from a first projection by cursor alone. The read path exists so that the
#: stronger, clock-independent provenance is used automatically if Core ever exposes
#: it; until then the authoritative projection timestamp is the business origin.
BUSINESS_ORIGIN_CURSOR_FIELDS: Tuple[str, ...] = (
    "origin_cursor",
    "message_created_cursor",
    "first_seen_cursor",
    "created_cursor",
)

ORIGIN_KIND_CURSOR = "origin_cursor"
ORIGIN_KIND_CREATED_AT = "message_created_at"

#: Provenance sources for a subscription floor record.
FLOOR_SOURCE_CORE_BOOTSTRAP = "core_bootstrap"
FLOOR_SOURCE_CORE_EVENTS_POLL = "core_events_poll"
FLOOR_SOURCE_LOCAL_RECORD = "durable_local_record"
FLOOR_SOURCE_UNAVAILABLE = "unavailable"

#: Wildcard account scope, used when the consumer subscribes to every account Core
#: reports. A per-account record always wins over the wildcard.
WILDCARD_ACCOUNT = "*"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_utc(value: Any) -> Optional[datetime]:
    """Parse a Core ISO-8601 timestamp into an aware UTC datetime.

    Returns ``None`` for anything that is not a usable instant, so callers fail
    closed instead of comparing a malformed string.
    """
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class BusinessOrigin:
    """A stable business origin for a Core message object."""

    kind: str
    cursor: Optional[int] = None
    at: Optional[datetime] = None
    field: str = ""

    def describe(self) -> str:
        if self.kind == ORIGIN_KIND_CURSOR:
            return f"cursor:{self.field}={self.cursor}"
        if self.at is not None:
            return f"created_at:{self.field}={self.at.isoformat()}"
        return f"{self.kind}:{self.field}=<unparsed>"


def extract_business_origin(message: Mapping[str, Any]) -> Tuple[Optional[BusinessOrigin], str]:
    """Return ``(origin, reason)`` for a Core message projection.

    ``origin is None`` means the caller must fail closed; ``reason`` then names the
    specific gap so the deferral record and the logs stay diagnostic.
    """
    if not isinstance(message, Mapping):
        return None, PROVENANCE_INDETERMINATE_MISSING_PROJECTION
    for field in BUSINESS_ORIGIN_CURSOR_FIELDS:
        raw = message.get(field)
        if raw in (None, ""):
            continue
        try:
            return BusinessOrigin(ORIGIN_KIND_CURSOR, cursor=int(str(raw).strip()), field=field), ""
        except (TypeError, ValueError):
            # A present-but-unparsable cursor is a contract violation: fail closed
            # rather than falling through to a weaker signal.
            return None, PROVENANCE_INDETERMINATE_MALFORMED_CREATED_AT
    raw_created = message.get("created_at")
    if raw_created in (None, ""):
        return None, PROVENANCE_INDETERMINATE_MISSING_CREATED_AT
    parsed = parse_utc(raw_created)
    if parsed is None:
        return None, PROVENANCE_INDETERMINATE_MALFORMED_CREATED_AT
    return BusinessOrigin(ORIGIN_KIND_CREATED_AT, at=parsed, field="created_at"), ""


# --------------------------------------------------------------------------- #
# Subscription floor
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class SubscriptionFloorRecord:
    """Durable, restart-safe subscription provenance for one (consumer, account)."""

    consumer_id: str
    account_id: str
    subscription_floor_cursor: Optional[int]
    subscription_floor_at: Optional[datetime]
    bootstrap_mode: str = ""
    bootstrap_source: str = ""
    provenance_source: str = FLOOR_SOURCE_UNAVAILABLE
    recorded_at: str = ""
    floor_at_is_derived: bool = False

    @property
    def complete(self) -> bool:
        """True when at least one comparable floor exists."""
        return self.subscription_floor_cursor is not None or self.subscription_floor_at is not None

    def classify(self, origin: Optional[BusinessOrigin], reason: str = "") -> str:
        """Classify an unknown effect identity against this floor.

        A cursor origin is compared against the cursor floor, a timestamp origin
        against the timestamp floor. Anything else — including a floor that does not
        carry the comparable component — is :data:`CLASSIFICATION_INDETERMINATE`.
        """
        if origin is None:
            return CLASSIFICATION_INDETERMINATE
        if origin.kind == ORIGIN_KIND_CURSOR:
            if origin.cursor is None or self.subscription_floor_cursor is None:
                return CLASSIFICATION_INDETERMINATE
            return (
                CLASSIFICATION_REPROJECTION_OF_PREEXISTING_OBJECT
                if origin.cursor < self.subscription_floor_cursor
                else CLASSIFICATION_FIRST_BUSINESS_EFFECT
            )
        if origin.kind == ORIGIN_KIND_CREATED_AT:
            if origin.at is None or self.subscription_floor_at is None:
                return CLASSIFICATION_INDETERMINATE
            return (
                CLASSIFICATION_REPROJECTION_OF_PREEXISTING_OBJECT
                if origin.at < self.subscription_floor_at
                else CLASSIFICATION_FIRST_BUSINESS_EFFECT
            )
        return CLASSIFICATION_INDETERMINATE

    def to_json(self) -> Dict[str, Any]:
        return {
            "consumer_id": self.consumer_id,
            "account_id": self.account_id,
            "subscription_floor_cursor": self.subscription_floor_cursor,
            "subscription_floor_at": (
                self.subscription_floor_at.isoformat() if self.subscription_floor_at else ""
            ),
            "bootstrap_mode": self.bootstrap_mode,
            "bootstrap_source": self.bootstrap_source,
            "provenance_source": self.provenance_source,
            "recorded_at": self.recorded_at,
            "floor_at_is_derived": self.floor_at_is_derived,
        }

    @classmethod
    def from_json(cls, payload: Mapping[str, Any]) -> "SubscriptionFloorRecord":
        raw_cursor = payload.get("subscription_floor_cursor")
        cursor: Optional[int] = None
        if raw_cursor not in (None, ""):
            try:
                cursor = int(raw_cursor)
            except (TypeError, ValueError):
                cursor = None
        return cls(
            consumer_id=str(payload.get("consumer_id") or ""),
            account_id=str(payload.get("account_id") or ""),
            subscription_floor_cursor=cursor,
            subscription_floor_at=parse_utc(payload.get("subscription_floor_at")),
            bootstrap_mode=str(payload.get("bootstrap_mode") or ""),
            bootstrap_source=str(payload.get("bootstrap_source") or ""),
            provenance_source=str(payload.get("provenance_source") or FLOOR_SOURCE_UNAVAILABLE),
            recorded_at=str(payload.get("recorded_at") or ""),
            floor_at_is_derived=bool(payload.get("floor_at_is_derived")),
        )


class SubscriptionFloorStore:
    """Durable ``subscription_floor_cursor`` / ``subscription_floor_at`` per consumer.

    The floor is read from Core's governed bootstrap provenance, which is the only
    immutable, server-assigned subscription anchor in the V1 contract:

    * ``initial_cursor`` — the cursor Core assigned this consumer at bootstrap. It
      changes only through an audited rebootstrap.
    * ``bootstrap_at`` — the instant Core recorded that bootstrap.

    Both are persisted so the decision survives a restart, and both are re-read from
    Core on every start so a rebootstrap is detected rather than silently ignored.

    The store is additive: it owns one new JSON file and never reads or writes the
    ``EffectLedger``, the message mapping DB or the Core checkpoint.
    """

    def __init__(
        self,
        path: Path | str,
        consumer_id: str,
        *,
        logger: Optional[Any] = None,
    ) -> None:
        self.path = Path(path)
        self.consumer_id = str(consumer_id)
        self.logger = logger
        self._accounts: Dict[str, SubscriptionFloorRecord] = {}
        self._history: List[Dict[str, Any]] = []
        self._loaded = False
        self._last_source = FLOOR_SOURCE_UNAVAILABLE

    # ------------------------------------------------------------------ store

    def _load(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, ValueError, TypeError):
            return
        if not isinstance(payload, Mapping):
            return
        if str(payload.get("consumer_id") or "") != self.consumer_id:
            # A floor file belonging to another consumer is not authoritative here.
            return
        accounts = payload.get("accounts")
        if isinstance(accounts, Mapping):
            for account_id, record in accounts.items():
                if isinstance(record, Mapping):
                    parsed = SubscriptionFloorRecord.from_json(record)
                    self._accounts[str(account_id)] = parsed
        history = payload.get("history")
        if isinstance(history, list):
            self._history = [entry for entry in history if isinstance(entry, Mapping)]

    def _save(self) -> None:
        payload = {
            "schema_version": PROVENANCE_SCHEMA_VERSION,
            "consumer_id": self.consumer_id,
            "updated_at": _utc_now_iso(),
            "accounts": {
                account_id: record.to_json()
                for account_id, record in sorted(self._accounts.items())
            },
            "history": self._history,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(
            prefix=self.path.name + ".", suffix=".tmp", dir=str(self.path.parent)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, self.path)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)

    # ---------------------------------------------------------------- queries

    def get(self, account_id: str) -> Optional[SubscriptionFloorRecord]:
        """Return the floor for one account, falling back to the wildcard scope."""
        self._load()
        account = str(account_id or "")
        record = self._accounts.get(account)
        if record is not None:
            return record
        return self._accounts.get(WILDCARD_ACCOUNT)

    def scoped_accounts(self) -> Tuple[str, ...]:
        self._load()
        return tuple(sorted(self._accounts))

    def snapshot(self) -> Dict[str, Any]:
        self._load()
        return {
            "consumer_id": self.consumer_id,
            "path": str(self.path),
            "durable": self.path.exists(),
            "source": self._last_source,
            "accounts": {key: value.to_json() for key, value in sorted(self._accounts.items())},
            "history_entries": len(self._history),
        }

    # ------------------------------------------------------------------ write

    def ensure_from_core(
        self,
        core: Any,
        account_ids: Any = (),
        *,
        allow_core_read: bool = True,
    ) -> Dict[str, Any]:
        """Establish (or re-verify) the durable floor from Core's bootstrap provenance.

        Never raises: when Core cannot be reached the previously persisted floor is
        kept, which is exactly the restart-safety property the decision depends on.
        Returns a JSON-safe snapshot for evidence.
        """
        self._load()
        accounts = [str(item) for item in account_ids if str(item)] or [WILDCARD_ACCOUNT]

        provenance: Optional[Mapping[str, Any]] = None
        core_readable = False
        try:
            provenance = core.get_bootstrap_provenance(self.consumer_id)
            core_readable = provenance is not None
        except Exception as exc:  # noqa: BLE001 - provenance must never break startup
            if self.logger is not None:
                self.logger.warning(
                    "Subscription floor: Core bootstrap provenance unreadable (%s); "
                    "keeping the durable local record",
                    exc,
                )
            provenance = None

        if provenance is None:
            self._last_source = (
                FLOOR_SOURCE_LOCAL_RECORD if self._accounts else FLOOR_SOURCE_UNAVAILABLE
            )
            return self.snapshot()

        raw_cursor = provenance.get("initial_cursor")
        cursor: Optional[int] = None
        if raw_cursor not in (None, ""):
            try:
                cursor = int(raw_cursor)
            except (TypeError, ValueError):
                cursor = None
        floor_at = parse_utc(provenance.get("bootstrap_at"))
        floor_at_is_derived = False
        source = FLOOR_SOURCE_CORE_BOOTSTRAP
        if floor_at is None and cursor is not None and allow_core_read:
            derived = self._derive_floor_at_from_core(core, cursor)
            if derived is not None:
                floor_at = derived
                floor_at_is_derived = True
                source = FLOOR_SOURCE_CORE_EVENTS_POLL
        if floor_at is None and cursor is None:
            # Core answered but carried no usable anchor.
            self._last_source = (
                FLOOR_SOURCE_LOCAL_RECORD if self._accounts else FLOOR_SOURCE_UNAVAILABLE
            )
            return self.snapshot()

        bootstrap_mode = str(provenance.get("bootstrap_mode") or "")
        bootstrap_source = str(provenance.get("bootstrap_source") or "")
        changed = False
        for account_id in accounts:
            existing = self._accounts.get(account_id)
            if existing is not None and self._same_anchor(existing, cursor, floor_at):
                # Restart idempotence: the recorded_at of an unchanged floor is
                # preserved so the durable file is byte-stable across restarts.
                continue
            if existing is not None:
                self._history.append(
                    {
                        "superseded_at": _utc_now_iso(),
                        "reason": "core_bootstrap_provenance_changed",
                        "previous": existing.to_json(),
                    }
                )
            self._accounts[account_id] = SubscriptionFloorRecord(
                consumer_id=self.consumer_id,
                account_id=account_id,
                subscription_floor_cursor=cursor,
                subscription_floor_at=floor_at,
                bootstrap_mode=bootstrap_mode,
                bootstrap_source=bootstrap_source,
                provenance_source=source,
                recorded_at=_utc_now_iso(),
                floor_at_is_derived=floor_at_is_derived,
            )
            changed = True
        self._last_source = source
        if changed:
            self._save()
        return self.snapshot()

    @staticmethod
    def _same_anchor(
        record: SubscriptionFloorRecord,
        cursor: Optional[int],
        floor_at: Optional[datetime],
    ) -> bool:
        return (
            record.subscription_floor_cursor == cursor
            and record.subscription_floor_at == floor_at
        )

    @staticmethod
    def _derive_floor_at_from_core(core: Any, cursor: int) -> Optional[datetime]:
        """Read the instant Core recorded the event at the floor cursor.

        Only used for a legacy bootstrap that carries no ``bootstrap_at``. This is an
        *upper* bound on the bootstrap instant (the floor event was recorded at or
        after bootstrap), so it is biased toward suppression; it is therefore marked
        ``floor_at_is_derived`` and disclosed rather than presented as authoritative.
        """
        try:
            page = core.poll_events(
                after=str(max(0, int(cursor) - 1)),
                consumer_id="",
                timeout=0,
                limit=1,
            )
        except Exception:  # noqa: BLE001
            return None
        if not isinstance(page, Mapping):
            return None
        events = page.get("events")
        if not isinstance(events, list) or not events:
            return None
        first = events[0]
        if not isinstance(first, Mapping):
            return None
        return parse_utc(first.get("occurred_at"))


# --------------------------------------------------------------------------- #
# Fail-closed deferral ledger
# --------------------------------------------------------------------------- #


class ProvenanceDeferralStore:
    """Durable parking lot for events whose business origin could not be resolved.

    ``CLASSIFICATION_INDETERMINATE`` must produce no external effect and no terminal
    ledger write, but it must also be *retryable* rather than silently consumed. The
    event is therefore recorded here — in a file of its own, never in the
    :class:`~efb_wechat_comwechat_slave.EffectLedger.EffectLedger` — and re-offered to
    the classifier on later poll cycles and on ``media.ready``.

    The store is bounded by ``max_attempts`` per entry and ``max_entries`` overall, so
    a permanently unreadable Core cannot grow it without limit.
    """

    def __init__(
        self,
        path: Path | str,
        consumer_id: str,
        *,
        max_attempts: int = 5,
        max_entries: int = 512,
        logger: Optional[Any] = None,
    ) -> None:
        self.path = Path(path)
        self.consumer_id = str(consumer_id)
        self.max_attempts = max(1, int(max_attempts))
        self.max_entries = max(1, int(max_entries))
        self.logger = logger
        self._entries: Dict[str, Dict[str, Any]] = {}
        self._loaded = False

    @staticmethod
    def key(account_id: str, message_id: str) -> str:
        return f"{account_id}|{message_id}"

    def _load(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, ValueError, TypeError):
            return
        if not isinstance(payload, Mapping):
            return
        if str(payload.get("consumer_id") or "") != self.consumer_id:
            return
        entries = payload.get("entries")
        if isinstance(entries, Mapping):
            self._entries = {
                str(key): dict(value)
                for key, value in entries.items()
                if isinstance(value, Mapping)
            }

    def _save(self) -> None:
        payload = {
            "schema_version": PROVENANCE_SCHEMA_VERSION,
            "consumer_id": self.consumer_id,
            "updated_at": _utc_now_iso(),
            "entries": self._entries,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(
            prefix=self.path.name + ".", suffix=".tmp", dir=str(self.path.parent)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, self.path)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)

    def defer(
        self,
        account_id: str,
        message: Mapping[str, Any],
        event_type: str,
        *,
        reason: str,
        cursor: str = "",
    ) -> int:
        """Record (or re-record) one indeterminate event. Returns its attempt count."""
        self._load()
        message_id = str(message.get("message_id") or "")
        if not message_id:
            # Nothing to retry by: an identity-less event cannot be parked.
            return 0
        entry_key = self.key(account_id, message_id)
        existing = self._entries.get(entry_key)
        attempts = int(existing.get("attempts") or 0) + 1 if existing else 1
        self._entries[entry_key] = {
            "account_id": str(account_id),
            "chat_id": str(message.get("chat_id") or ""),
            "message_id": message_id,
            "event_type": str(event_type),
            "cursor": str(cursor or ""),
            "attempts": attempts,
            "first_deferred_at": (
                str(existing.get("first_deferred_at")) if existing else _utc_now_iso()
            ),
            "last_deferred_at": _utc_now_iso(),
            "last_reason": str(reason or ""),
            "message": dict(message),
        }
        if len(self._entries) > self.max_entries:
            overflow = sorted(
                self._entries.items(),
                key=lambda item: str(item[1].get("first_deferred_at") or ""),
            )[: len(self._entries) - self.max_entries]
            for key, _ in overflow:
                self._entries.pop(key, None)
            if self.logger is not None:
                self.logger.warning(
                    "Provenance deferral store at capacity (%d); dropped %d oldest entries",
                    self.max_entries,
                    len(overflow),
                )
        self._save()
        return attempts

    def due(self) -> List[Dict[str, Any]]:
        self._load()
        return [
            dict(entry)
            for entry in self._entries.values()
            if int(entry.get("attempts") or 0) < self.max_attempts
        ]

    def resolve(self, account_id: str, message_id: str) -> bool:
        self._load()
        entry_key = self.key(account_id, message_id)
        if entry_key not in self._entries:
            return False
        self._entries.pop(entry_key, None)
        self._save()
        return True

    def count(self) -> int:
        self._load()
        return len(self._entries)

    def exhausted(self) -> int:
        self._load()
        return sum(
            1 for entry in self._entries.values() if int(entry.get("attempts") or 0) >= self.max_attempts
        )

    def snapshot(self) -> Dict[str, Any]:
        self._load()
        return {
            "consumer_id": self.consumer_id,
            "path": str(self.path),
            "durable": self.path.exists(),
            "entries": len(self._entries),
            "exhausted": self.exhausted(),
            "max_attempts": self.max_attempts,
            "max_entries": self.max_entries,
        }
