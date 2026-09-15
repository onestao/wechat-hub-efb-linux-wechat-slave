"""RC.14 Optional EFB — Retry3 deterministic graceful-shutdown tests.

Defect ``R14-EFB-D3`` (Retry2 failure):
    ``docker stop -t 2 wechat-hub-f-live-efb`` took **5.732 s** and the container
    was SIGKILLed (``ExitCode 137``).  Root cause: ``ehforwarderbot`` 2.1.1 stops
    the **master first, synchronously**, and ``python-telegram-bot`` 13.15
    ``Updater.stop()`` joins its updater thread without a timeout while that
    thread is parked inside an in-flight ``getUpdates`` long poll whose socket
    read timeout is ``read_latency + timeout`` = 12 s.  Because the master is
    stopped before the slaves, the slave's durable flush never ran either.

These tests pin the Retry3 corrective behaviour:

    B1  Blocking poll cancellation   elapsed < 2.0 s, clean exit, poll thread terminated
    B2  Final checkpoint before exit durable flush completes before process exit
    B3  Ledger durability            DELIVERED kept, RESERVED kept fail-closed, ledger never deleted
    B4  Multiple shutdown signals    no double flush, no double delivery, no deadlock, no exception

Everything here is fully offline: no Telegram, no Core network call, no real
signal delivered to the test process, no production state touched.
"""

from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest import mock

try:
    import ehforwarderbot  # noqa: F401
except ImportError:  # pragma: no cover - offline fallback
    import tests.stub_ehforwarderbot as _stub

    _stub.install_stubs()

from ehforwarderbot import Message, MsgType, coordinator  # noqa: E402

from efb_wechat_comwechat_slave import ShutdownCoordinator as shutdown_mod  # noqa: E402
from efb_wechat_comwechat_slave.ComWechat import LinuxWeChatChannel  # noqa: E402
from efb_wechat_comwechat_slave.Core import CoreClient  # noqa: E402
from efb_wechat_comwechat_slave.EffectLedger import (  # noqa: E402
    STATE_DELIVERED,
    STATE_RESERVED,
    STATE_UNCERTAIN,
)
from efb_wechat_comwechat_slave.ShutdownCoordinator import (  # noqa: E402
    SHUTDOWN_EVIDENCE_MARKER,
    ShutdownCoordinator,
    _is_real_master,
)

try:  # real EFB master base class (present in the qualification venv)
    from ehforwarderbot.channel import MasterChannel as _RealMasterChannel
except Exception:  # pragma: no cover
    _RealMasterChannel = None


CONSUMER_SUFFIX = "wechat.linux"


# --------------------------------------------------------------------------- #
# Doubles
# --------------------------------------------------------------------------- #


class BlockingMaster:
    """Stand-in master reproducing PTB 13.15 ``Updater.stop()`` semantics.

    ``python-telegram-bot`` 13.15 ``Updater.stop()`` sets ``running = False`` and
    then calls ``_join_threads()`` with **no timeout**.  The updater thread only
    re-tests ``running`` after ``get_updates`` returns, and the socket read
    timeout there is ``read_latency (2.0) + timeout (10)`` = 12 s.  This double
    blocks for exactly that long unless the coordinator bounds it.
    """

    def __init__(self, block_sec: float = 12.0) -> None:
        self.block_sec = float(block_sec)
        self.stop_calls = 0
        self.stop_thread_names: List[str] = []
        self._release = threading.Event()

    def stop_polling(self) -> None:
        self.stop_calls += 1
        self.stop_thread_names.append(threading.current_thread().name)
        self._release.wait(timeout=self.block_sec)

    def release(self) -> None:
        self._release.set()


if _RealMasterChannel is not None:

    class RealishMaster(_RealMasterChannel):  # type: ignore[misc, valid-type]
        """Minimal concrete ``MasterChannel`` so ``_is_real_master`` is True."""

        channel_id = "stub.master"
        channel_name = "Stub Master"
        channel_emoji = "\U0001f9ea"

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self.stop_calls = 0

        def poll(self) -> None:  # pragma: no cover - never polled here
            pass

        def send_message(self, message: Message) -> Optional[Message]:  # type: ignore[override]
            return None

        def send_status(self, status: Any) -> None:  # type: ignore[override]
            pass

        def stop_polling(self) -> None:
            self.stop_calls += 1

else:  # pragma: no cover

    class RealishMaster:  # type: ignore[no-redef]
        pass


class RecordingCore(CoreClient):
    """Offline Core double that records every checkpoint transition."""

    def __init__(self, base_url: str = "http://127.0.0.1:8080") -> None:
        super().__init__(base_url)
        self.stream_head = 0
        self.checkpoints: Dict[str, int] = {}
        self.checkpoint_calls: List[Any] = []
        self.events_queue: List[Dict[str, Any]] = []
        self.accounts: List[Dict[str, Any]] = [
            {"account_id": "acc-1", "display_name": "Account One", "state": "online"}
        ]
        self.chats: List[Dict[str, Any]] = []
        self.bootstrap_records: Dict[str, Dict[str, Any]] = {}

    # -- health / discovery -------------------------------------------------
    def health(self) -> Dict[str, Any]:
        return {"contract_version": 1, "status": "ok"}

    def list_accounts(self) -> List[Dict[str, Any]]:
        return list(self.accounts)

    def list_chats(self, account_id: str, *, query: str = "", limit: int = 200) -> List[Dict[str, Any]]:
        return [c for c in self.chats if c["account_id"] == account_id]

    # -- bootstrap ----------------------------------------------------------
    def get_bootstrap_provenance(self, consumer_id: str) -> Optional[Dict[str, Any]]:
        return self.bootstrap_records.get(consumer_id)

    def bootstrap_consumer(
        self,
        consumer_id: str,
        *,
        mode: str = "at_head",
        window: Optional[Dict[str, Any]] = None,
        operator_token: str = "",
    ) -> Dict[str, Any]:
        record = {
            "ok": True,
            "consumer_id": consumer_id,
            "initial_cursor": self.stream_head,
            "bootstrap_mode": mode,
            "stream_head_cursor": self.stream_head,
        }
        self.bootstrap_records[consumer_id] = record
        self.checkpoints[consumer_id] = self.stream_head
        return record

    # -- polling ------------------------------------------------------------
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
        return {
            "events": available[:limit],
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
        self.checkpoint_calls.append((consumer_id, processed_through_cursor))
        return {"ok": True, "consumer_id": consumer_id, "processed_through_cursor": processed_through_cursor}

    def ack_events(self, consumer_id: str, event_ids: Any) -> Dict[str, Any]:
        return {"ok": True, "consumer_id": consumer_id, "acked_event_ids": list(event_ids)}


class CountingDrainable:
    """Drainable double that counts how often it was drained."""

    channel_id = "double.channel"

    def __init__(self, drain_delay: float = 0.0) -> None:
        self.drain_count = 0
        self.suppress_count = 0
        self.drain_delay = float(drain_delay)

    def drain_for_shutdown(self, *, budget_sec: float = 0.75) -> Dict[str, Any]:
        self.drain_count += 1
        if self.drain_delay:
            time.sleep(self.drain_delay)
        return {"drain_count": self.drain_count, "budget_sec": budget_sec}

    def suppress_external_dispatch(self) -> None:
        self.suppress_count += 1


# --------------------------------------------------------------------------- #
# Base
# --------------------------------------------------------------------------- #


class ShutdownTestBase(unittest.TestCase):
    """Isolates the process-wide coordinator registry between tests."""

    def setUp(self) -> None:
        self._registry_snapshot = dict(shutdown_mod._REGISTRY)
        shutdown_mod._REGISTRY.clear()
        self._orig_master = getattr(coordinator, "master", None)
        self._orig_send_message = getattr(coordinator, "send_message", None)
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_path = Path(self.temp_dir.name)

    def tearDown(self) -> None:
        coordinator.master = self._orig_master
        coordinator.send_message = self._orig_send_message
        shutdown_mod._REGISTRY.clear()
        shutdown_mod._REGISTRY.update(self._registry_snapshot)
        self.temp_dir.cleanup()

    def make_channel(self, core: Optional[CoreClient] = None, **overrides: Any) -> LinuxWeChatChannel:
        config: Dict[str, Any] = {
            "core": {"base_url": "http://mock-core", "timeout": 2, "poll_timeout": 0},
            "consumer_id": "efb-linux-wechat",
            "poll_interval": 0.01,
            "startup_healthcheck": True,
            # Never let a test accidentally terminate the test process.
            "shutdown_hard_exit": False,
            "shutdown_install_deferred": False,
        }
        config.update(overrides)
        return LinuxWeChatChannel(
            core_client=core if core is not None else RecordingCore(),
            config=config,
            data_path=self.data_path,
        )


# --------------------------------------------------------------------------- #
# B1 — Blocking poll cancellation
# --------------------------------------------------------------------------- #


class TestB1BlockingPollCancellation(ShutdownTestBase):
    """B1: a blocked external poll must not hold the container past 2.0 s."""

    def test_b1_blocking_master_stop_is_bounded_under_two_seconds(self) -> None:
        master = BlockingMaster(block_sec=12.0)
        exit_codes: List[int] = []

        coordinator_obj = ShutdownCoordinator(
            drainables=[CountingDrainable()],
            master_budget_sec=1.0,
            slave_drain_budget_sec=0.75,
            hard_exit=True,
            exit_code=0,
            exit_func=exit_codes.append,
        )
        self.assertTrue(coordinator_obj.install(master=master))

        started = time.monotonic()
        evidence = coordinator_obj.run(trigger="test:b1")
        elapsed = time.monotonic() - started

        self.assertLess(elapsed, 2.0, f"shutdown took {elapsed:.3f}s, must be < 2.0s")
        self.assertEqual(evidence["exit_code"], 0)
        self.assertTrue(evidence["master_stop_started"])
        self.assertTrue(
            evidence["master_stop_bounded_out"],
            "master stop must be reported as bounded-out, not completed",
        )
        self.assertFalse(evidence["master_stop_completed"])
        self.assertLess(evidence["master_stop_elapsed_sec"], 2.0)
        self.assertEqual(exit_codes, [0], "process must exit exactly once with code 0")

        master.release()

    def test_b1_original_master_stop_polling_is_still_invoked(self) -> None:
        """The wrapper must delegate to the real stop_polling, not replace it."""
        master = BlockingMaster(block_sec=5.0)
        coordinator_obj = ShutdownCoordinator(
            master_budget_sec=0.5,
            hard_exit=False,
        )
        self.assertTrue(coordinator_obj.install(master=master))
        coordinator_obj.run(trigger="test:b1-delegate")

        deadline = time.monotonic() + 2.0
        while master.stop_calls == 0 and time.monotonic() < deadline:
            time.sleep(0.01)

        self.assertEqual(master.stop_calls, 1, "master.stop_polling must be called exactly once")
        master.release()

    def test_b1_master_stop_runs_on_a_daemon_thread(self) -> None:
        """A daemon stop thread cannot keep the interpreter alive past the budget."""
        master = BlockingMaster(block_sec=8.0)
        coordinator_obj = ShutdownCoordinator(master_budget_sec=0.3, hard_exit=False)
        self.assertTrue(coordinator_obj.install(master=master))

        started = time.monotonic()
        coordinator_obj.run(trigger="test:b1-daemon")
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 1.0)

        stop_threads = [t for t in threading.enumerate() if t.name == "efb-master-stop"]
        self.assertTrue(stop_threads, "the master stop thread should be observable while blocked")
        for thread in stop_threads:
            self.assertTrue(thread.daemon, "master stop thread must be a daemon thread")
        master.release()

    def test_b1_install_is_idempotent_and_wraps_once(self) -> None:
        master = BlockingMaster(block_sec=0.0)
        coordinator_obj = ShutdownCoordinator(hard_exit=False)
        self.assertTrue(coordinator_obj.install(master=master))
        wrapped = master.stop_polling
        self.assertTrue(coordinator_obj.install(master=master))
        self.assertIs(master.stop_polling, wrapped, "second install must not double-wrap")
        master.release()

    def test_b1_slave_poll_loop_is_cancelled_under_two_seconds(self) -> None:
        """The slave Core poll loop must also abort promptly on shutdown drain."""
        core = RecordingCore()
        channel = self.make_channel(core=core)
        try:
            stop_event = threading.Event()

            def blocking_poll() -> None:
                # Emulates an in-flight long poll that only wakes on the timeout.
                stop_event.wait(timeout=12.0)

            poll_thread = threading.Thread(target=blocking_poll, name="blocking-core-poll", daemon=True)
            poll_thread.start()
            time.sleep(0.05)
            self.assertTrue(poll_thread.is_alive())

            started = time.monotonic()
            channel.drain_for_shutdown(budget_sec=0.5)
            elapsed = time.monotonic() - started
            self.assertLess(elapsed, 2.0)

            # The channel's own stop event must be set so the real loop exits.
            self.assertTrue(channel._stop_event.is_set())

            stop_event.set()
            poll_thread.join(timeout=2.0)
            self.assertFalse(poll_thread.is_alive(), "poll thread did not terminate")
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()


# --------------------------------------------------------------------------- #
# B2 — Final checkpoint before exit
# --------------------------------------------------------------------------- #


class TestB2FinalCheckpointBeforeExit(ShutdownTestBase):
    """B2: the durable cursor flush must complete *before* the process exits."""

    def test_b2_checkpoint_flushed_before_exit(self) -> None:
        core = RecordingCore()
        channel = self.make_channel(core=core)
        observed: Dict[str, Any] = {}

        def fake_exit(code: int) -> None:
            observed["exit_code"] = code
            observed["flush_completed"] = channel._checkpoint_flush_completed
            observed["flush_cursor"] = channel._checkpoint_flush_cursor
            observed["core_checkpoint"] = core.checkpoints.get(channel.consumer_id)
            observed["ledger_exists"] = channel.effect_ledger.db_path.exists()
            observed["drain"] = dict(channel._shutdown_evidence)

        coordinator_obj = ShutdownCoordinator(
            drainables=[channel],
            master_budget_sec=0.5,
            slave_drain_budget_sec=0.75,
            hard_exit=True,
            exit_code=0,
            exit_func=fake_exit,
        )

        try:
            channel.cursor_store.save("272548")
            evidence = coordinator_obj.run(trigger="test:b2")

            self.assertEqual(observed.get("exit_code"), 0)
            self.assertTrue(
                observed.get("flush_completed"),
                "final checkpoint flush must be complete at process-exit time",
            )
            self.assertEqual(observed.get("flush_cursor"), 272548)
            self.assertEqual(
                observed.get("core_checkpoint"),
                272548,
                "Core must have received the durable cursor before exit",
            )
            self.assertTrue(observed.get("ledger_exists"), "ledger must survive shutdown")
            self.assertTrue(observed["drain"]["checkpoint_flushed"])
            self.assertEqual(observed["drain"]["checkpoint_cursor"], 272548)
            self.assertTrue(observed["drain"]["ledger_wal_checkpointed"])
            self.assertEqual(evidence["exit_code"], 0)
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()

    def test_b2_flush_is_idempotent_no_double_checkpoint(self) -> None:
        """A second flush (e.g. poll()'s finally after the coordinator) is a no-op."""
        core = RecordingCore()
        channel = self.make_channel(core=core)
        try:
            channel.cursor_store.save("13181")
            baseline = len(core.checkpoint_calls)

            self.assertEqual(channel._flush_final_checkpoint(), 13181)
            after_first = len(core.checkpoint_calls)
            self.assertEqual(after_first - baseline, 1, "first flush must checkpoint once")

            self.assertEqual(channel._flush_final_checkpoint(), 13181)
            self.assertEqual(
                len(core.checkpoint_calls) - baseline,
                1,
                "repeated flush must not checkpoint again",
            )
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()

    def test_b2_flush_is_thread_safe_under_concurrent_signals(self) -> None:
        core = RecordingCore()
        channel = self.make_channel(core=core)
        try:
            channel.cursor_store.save("272548")
            baseline = len(core.checkpoint_calls)
            barrier = threading.Barrier(8)
            results: List[Any] = []
            errors: List[BaseException] = []

            def worker() -> None:
                try:
                    barrier.wait(timeout=5)
                    results.append(channel._flush_final_checkpoint())
                except BaseException as exc:  # noqa: BLE001 - recorded for assertion
                    errors.append(exc)

            threads = [threading.Thread(target=worker, daemon=True) for _ in range(8)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)

            self.assertFalse(errors, f"concurrent flush raised: {errors}")
            self.assertTrue(all(t for t in results if t is not None))
            self.assertEqual(
                len(core.checkpoint_calls) - baseline,
                1,
                "concurrent flushes must produce exactly one Core checkpoint",
            )
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()


# --------------------------------------------------------------------------- #
# B3 — Ledger durability
# --------------------------------------------------------------------------- #


class TestB3LedgerDurability(ShutdownTestBase):
    """B3: DELIVERED survives, RESERVED stays fail-closed, ledger is never deleted."""

    def _seed_ledger(self, channel: LinuxWeChatChannel) -> Dict[str, str]:
        ledger = channel.effect_ledger
        cid = channel.consumer_id
        delivered_effect = ledger.compute_effect_id("acc-1", "msg-delivered")
        reserved_effect = ledger.compute_effect_id("acc-1", "msg-reserved")

        reserved_ok, reserved_state = ledger.reserve_effect(
            cid, delivered_effect, account_id="acc-1", message_id="msg-delivered"
        )
        self.assertTrue(reserved_ok)
        self.assertEqual(reserved_state, STATE_RESERVED)
        self.assertTrue(
            ledger.mark_delivered(
                cid, delivered_effect, efb_uid="efb-uid-delivered", details={"chat_id": "chat-1"}
            )
        )

        reserved_ok2, reserved_state2 = ledger.reserve_effect(
            cid, reserved_effect, account_id="acc-1", message_id="msg-reserved"
        )
        self.assertTrue(reserved_ok2)
        self.assertEqual(reserved_state2, STATE_RESERVED)

        return {"delivered": delivered_effect, "reserved": reserved_effect}

    def test_b3_shutdown_preserves_ledger_rows_and_file(self) -> None:
        core = RecordingCore()
        channel = self.make_channel(core=core)
        try:
            effects = self._seed_ledger(channel)
            ledger = channel.effect_ledger
            db_path = ledger.db_path
            before = ledger.status_counts(channel.consumer_id)
            self.assertEqual(before[STATE_DELIVERED], 1)
            self.assertEqual(before[STATE_RESERVED], 1)

            detail = channel.drain_for_shutdown(budget_sec=0.5)

            self.assertTrue(detail["ledger_wal_checkpointed"])
            self.assertTrue(db_path.exists(), "shutdown must never delete the ledger file")

            after = ledger.status_counts(channel.consumer_id)
            self.assertEqual(after[STATE_DELIVERED], 1, "DELIVERED must be preserved")
            self.assertEqual(after[STATE_RESERVED], 1, "RESERVED must remain RESERVED (fail-closed)")
            self.assertEqual(after[STATE_UNCERTAIN], 0, "shutdown must not invent UNCERTAIN rows")

            delivered_row = ledger.get_effect(channel.consumer_id, effects["delivered"])
            self.assertIsNotNone(delivered_row)
            self.assertEqual(delivered_row["status"], STATE_DELIVERED)
            self.assertEqual(delivered_row["efb_uid"], "efb-uid-delivered")
            self.assertEqual(ledger.get_effect_status(channel.consumer_id, effects["reserved"]), STATE_RESERVED)
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()

    def test_b3_shutdown_preserves_rows_across_reopen_and_restart_reconcile(self) -> None:
        """Reopening after shutdown: DELIVERED stays, RESERVED becomes UNCERTAIN."""
        core = RecordingCore()
        channel = self.make_channel(core=core)
        effects = self._seed_ledger(channel)
        db_path = channel.effect_ledger.db_path
        try:
            channel.drain_for_shutdown(budget_sec=0.5)
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()

        from efb_wechat_comwechat_slave.EffectLedger import EffectLedger

        reopened = EffectLedger(db_path)
        try:
            counts = reopened.status_counts(channel.consumer_id)
            self.assertEqual(counts[STATE_DELIVERED], 1)
            self.assertEqual(counts[STATE_RESERVED], 1)

            reconciled = reopened.reconcile_on_startup(channel.consumer_id)
            self.assertEqual(reconciled, [effects["reserved"]])

            final = reopened.status_counts(channel.consumer_id)
            self.assertEqual(final[STATE_DELIVERED], 1, "DELIVERED must never be downgraded")
            self.assertEqual(final[STATE_RESERVED], 0)
            self.assertEqual(final[STATE_UNCERTAIN], 1)
            self.assertEqual(
                reopened.get_effect_status(channel.consumer_id, effects["delivered"]),
                STATE_DELIVERED,
            )
            self.assertEqual(
                reopened.get_effect_status(channel.consumer_id, effects["reserved"]),
                STATE_UNCERTAIN,
            )
        finally:
            reopened.close()

    def test_b3_checkpoint_wal_never_mutates_rows(self) -> None:
        core = RecordingCore()
        channel = self.make_channel(core=core)
        try:
            effects = self._seed_ledger(channel)
            ledger = channel.effect_ledger
            before = ledger.status_counts(channel.consumer_id)

            for _ in range(5):
                ledger.checkpoint_wal()

            after = ledger.status_counts(channel.consumer_id)
            self.assertEqual(before, after)
            self.assertEqual(ledger.get_effect_status(channel.consumer_id, effects["delivered"]), STATE_DELIVERED)
            self.assertEqual(ledger.get_effect_status(channel.consumer_id, effects["reserved"]), STATE_RESERVED)
            self.assertTrue(ledger.db_path.exists())
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()

    def test_b3_no_external_delivery_after_shutdown_begins(self) -> None:
        """Once shutdown starts, no outbound external delivery may be produced."""
        core = RecordingCore()
        channel = self.make_channel(core=core)
        delivered: List[Message] = []
        try:
            channel.suppress_external_dispatch()
            channel._handle_event(
                {
                    "event_id": "ev-late",
                    "cursor": 999999,
                    "event_type": "message.created",
                    "account_id": "acc-1",
                    "payload": {
                        "message": {
                            "message_id": "msg-late",
                            "chat_id": "chat-1",
                            "text": "must not be delivered",
                            "type": "text",
                        }
                    },
                }
            )
            self.assertEqual(delivered, [], "no delivery may occur after shutdown begins")
            self.assertIsNone(
                channel.effect_ledger.get_effect_status(
                    channel.consumer_id, channel.effect_ledger.compute_effect_id("acc-1", "msg-late")
                ),
                "a suppressed event must not even reserve an effect",
            )
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()


# --------------------------------------------------------------------------- #
# B4 — Multiple shutdown signals
# --------------------------------------------------------------------------- #


class TestB4MultipleShutdownSignals(ShutdownTestBase):
    """B4: repeated / concurrent shutdown must be inert, not destructive."""

    def test_b4_concurrent_run_calls_execute_exactly_once(self) -> None:
        drainable = CountingDrainable()
        master = BlockingMaster(block_sec=3.0)
        exit_codes: List[int] = []
        coordinator_obj = ShutdownCoordinator(
            drainables=[drainable],
            master_budget_sec=0.5,
            slave_drain_budget_sec=0.5,
            hard_exit=True,
            exit_code=0,
            exit_func=exit_codes.append,
        )
        self.assertTrue(coordinator_obj.install(master=master))

        errors: List[BaseException] = []
        barrier = threading.Barrier(8)

        def worker() -> None:
            try:
                barrier.wait(timeout=5)
                coordinator_obj.run(trigger="test:b4-concurrent")
            except BaseException as exc:  # noqa: BLE001 - recorded for assertion
                errors.append(exc)

        threads = [threading.Thread(target=worker, daemon=True) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertFalse(any(t.is_alive() for t in threads), "concurrent shutdown deadlocked")
        self.assertFalse(errors, f"concurrent shutdown raised: {errors}")
        self.assertEqual(drainable.drain_count, 1, "drain must happen exactly once")
        self.assertEqual(drainable.suppress_count, 1, "dispatch suppression must happen exactly once")
        self.assertEqual(exit_codes, [0], "process must exit exactly once")
        master.release()

    def test_b4_sequential_repeat_calls_are_inert(self) -> None:
        drainable = CountingDrainable()
        exit_codes: List[int] = []
        coordinator_obj = ShutdownCoordinator(
            drainables=[drainable],
            master_budget_sec=0.2,
            hard_exit=True,
            exit_code=0,
            exit_func=exit_codes.append,
        )
        master = BlockingMaster(block_sec=0.0)
        self.assertTrue(coordinator_obj.install(master=master))

        first = coordinator_obj.run(trigger="test:b4-first")
        for _ in range(4):
            repeated = coordinator_obj.run(trigger="test:b4-repeat")
            self.assertTrue(repeated.get("repeated"), "repeat call must be reported as repeated")
            self.assertEqual(repeated["exit_code"], 0)

        self.assertEqual(drainable.drain_count, 1)
        self.assertEqual(exit_codes, [0])
        self.assertEqual(master.stop_calls, 1)
        self.assertEqual(first["repeated"], False)
        master.release()

    def test_b4_repeated_signal_handler_does_not_double_flush(self) -> None:
        core = RecordingCore()
        channel = self.make_channel(core=core)
        try:
            channel.cursor_store.save("272548")
            baseline = len(core.checkpoint_calls)

            for _ in range(5):
                channel._sig_handler(15, None)

            self.assertEqual(
                len(core.checkpoint_calls) - baseline,
                1,
                "repeated shutdown signals must not double-flush the checkpoint",
            )
            self.assertTrue(channel._shutdown_in_progress.is_set())
        finally:
            channel.stop_polling()
            channel.effect_ledger.close()

    def test_b4_shutdown_evidence_is_emitted_once_with_marker(self) -> None:
        evidence_path = self.data_path / "shutdown-evidence.json"
        drainable = CountingDrainable()
        coordinator_obj = ShutdownCoordinator(
            drainables=[drainable],
            master_budget_sec=0.2,
            hard_exit=False,
            exit_code=0,
            evidence_path=str(evidence_path),
        )
        master = BlockingMaster(block_sec=0.0)
        self.assertTrue(coordinator_obj.install(master=master))

        evidence = coordinator_obj.run(trigger="test:b4-evidence")
        self.assertEqual(evidence["event"], "efb_shutdown_evidence")
        self.assertEqual(evidence["trigger"], "test:b4-evidence")
        self.assertTrue(evidence["delivery_suppressed"])
        self.assertEqual([p["phase"] for p in evidence["phases"]], [
            "slave_drain",
            "suppress_dispatch",
            "master_stop_bounded",
        ])
        self.assertTrue(evidence_path.exists())
        payload = json.loads(evidence_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["event"], "efb_shutdown_evidence")
        self.assertEqual(payload["exit_code"], 0)
        self.assertTrue(SHUTDOWN_EVIDENCE_MARKER.startswith("EFB_SHUTDOWN"))
        master.release()


# --------------------------------------------------------------------------- #
# Installer safety
# --------------------------------------------------------------------------- #


class TestShutdownInstallerSafety(ShutdownTestBase):
    """The deferred installer must never latch onto a non-master stand-in."""

    def test_installer_ignores_mock_master(self) -> None:
        coordinator.master = mock.MagicMock()
        coordinator_obj = ShutdownCoordinator(install_deferred=True, deferred_deadline_sec=0.4)
        self.assertFalse(coordinator_obj.install())
        self.assertFalse(coordinator_obj.installed)

    def test_is_real_master_rejects_stand_ins(self) -> None:
        self.assertFalse(_is_real_master(None))
        self.assertFalse(_is_real_master(mock.MagicMock()))
        self.assertFalse(_is_real_master(BlockingMaster()))

    def test_explicit_master_is_trusted_even_when_not_real(self) -> None:
        """Tests and embedders may inject their own master object."""
        master = BlockingMaster(block_sec=0.0)
        coordinator_obj = ShutdownCoordinator(hard_exit=False)
        self.assertTrue(coordinator_obj.install(master=master))
        self.assertTrue(coordinator_obj.installed)
        master.release()

    @unittest.skipIf(_RealMasterChannel is None, "real ehforwarderbot not installed")
    def test_deferred_installer_is_armed_when_master_is_absent(self) -> None:
        """Regression: the slave is built *before* the master.

        ``ehforwarderbot.__main__.init`` instantiates slaves first, so at
        construction time ``coordinator.master`` is ``None``.  ``install()`` must
        then arm the deferred installer and report "not yet installed" -- if it
        returned quietly, the whole Retry3 corrective would be dead code in
        production with no signal at all.
        """
        coordinator.master = None
        coordinator_obj = ShutdownCoordinator(install_deferred=True, deferred_deadline_sec=5.0)

        self.assertFalse(coordinator_obj.install(), "no master yet -> not installed")
        self.assertFalse(coordinator_obj.installed)
        self.assertIsNotNone(
            coordinator_obj._deferred_thread,
            "the deferred installer must be armed when the master does not exist yet",
        )
        self.assertTrue(coordinator_obj._deferred_thread.is_alive())
        self.assertTrue(coordinator_obj._deferred_thread.daemon)

    @unittest.skipIf(_RealMasterChannel is None, "real ehforwarderbot not installed")
    def test_deferred_installer_installs_when_real_master_appears(self) -> None:
        coordinator.master = None
        coordinator_obj = ShutdownCoordinator(install_deferred=True, deferred_deadline_sec=5.0)
        self.assertFalse(coordinator_obj.install())  # arms the deferred installer
        self.assertFalse(coordinator_obj.installed)

        real_master = RealishMaster()
        coordinator.master = real_master
        try:
            deadline = time.monotonic() + 5.0
            while getattr(real_master, shutdown_mod._WRAPPED_ATTR, None) is not coordinator_obj:
                if time.monotonic() > deadline:
                    break
                time.sleep(0.02)
            self.assertIs(
                getattr(real_master, shutdown_mod._WRAPPED_ATTR, None),
                coordinator_obj,
                "deferred installer must wrap a genuine master once it exists",
            )
            self.assertTrue(coordinator_obj.installed)
        finally:
            coordinator.master = None


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
