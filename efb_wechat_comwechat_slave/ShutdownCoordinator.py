r"""Deterministic bounded shutdown coordination for the Linux WeChat slave.

RC.14 Optional EFB — Retry3 narrow corrective engineering (defect `R14-EFB-D3`).

Why this module exists
----------------------
`ehforwarderbot` 2.1.1 registers ``stop_gracefully`` on ``SIGTERM`` and, when it
runs, stops the **master channel first, synchronously**:

    ehforwarderbot/__main__.py: stop_gracefully()
        coordinator.master.stop_polling()
            -> efb_telegram_master.TelegramChannel.stop_polling()
            -> efb_telegram_master.BotManager.graceful_stop()
            -> telegram.ext.Updater.stop()          # python-telegram-bot 13.15
                 self.running = False
                 self._join_threads()               # UNBOUNDED thr.join()

The updater thread is parked inside an in-flight long poll::

    telegram/ext/updater.py: updates = self.bot.get_updates(
                                 timeout=timeout, read_latency=read_latency, ...)
    telegram/bot.py:         self._post('getUpdates', data,
                                       timeout=float(read_latency) + float(timeout))

`BotManager` starts polling with ``timeout=10`` (``bot_manager.py``) and
``read_latency`` defaults to ``2.0`` (``telegram/bot.py``), so the socket read
timeout is **12 s**.  The polling loop only re-tests ``self.running`` *after*
``get_updates`` returns, and python-telegram-bot 13.15 exposes no cancellation
primitive -- ``Request.stop()`` is only ``self._con_pool.clear()``, which closes
*idle* pooled connections and cannot interrupt an in-flight request.

Measured consequence during Retry2:

* ``docker stop -t 2 wechat-hub-f-live-efb`` took **5.732 s**
* Docker's grace expired -> ``SIGKILL`` -> container ``ExitCode 137``
* because EFB stops the master *before* the slaves, the slave's
  ``_flush_final_checkpoint()`` never ran inside the grace window

What this module does
---------------------
A committed coordination wrapper -- not a runtime patch of a temporary file --
installed by the slave channel, which enforces the ordering EFB does not and
bounds the master stop:

1. ``slave_drain``          quiesce the Core poll loop, flush the durable cursor,
                            checkpoint and effect-ledger state
2. ``suppress_dispatch``    refuse any further outbound external delivery
3. ``master_stop_bounded``  run the master's own ``stop_polling`` inside a
                            **daemon** thread and join it with a hard budget
4. ``evidence``             emit a machine-readable shutdown evidence record
5. ``deterministic_exit``   once durable state is safe, terminate the process
                            with a controlled exit code so that a third-party
                            thread that cannot be cancelled can never hold the
                            container past the stop grace period

Step 5 is required because the updater thread created by
``Updater._init_thread`` is a **non-daemon** thread: even if we stop waiting for
it, CPython's interpreter shutdown would join it again and block for the
remainder of the long poll.  ``os._exit`` is therefore used *after* the durable
flush, never before.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

__all__ = [
    "SHUTDOWN_EVIDENCE_MARKER",
    "ShutdownCoordinator",
    "install_shutdown_coordinator",
    "current_master",
    "current_slave_channels",
]

#: Marker prefix used by the evidence generator to locate the shutdown record.
SHUTDOWN_EVIDENCE_MARKER = "EFB_SHUTDOWN_EVIDENCE"

#: Default budget for the (bounded) master ``stop_polling`` call.
DEFAULT_MASTER_STOP_BUDGET_SEC = 1.0

#: Default budget for draining the slave's durable state.
DEFAULT_SLAVE_DRAIN_BUDGET_SEC = 0.75

#: Attribute stamped on the master instance that has been wrapped.
_WRAPPED_ATTR = "_efb_retry3_shutdown_coordinator"

_LOG = logging.getLogger("efb_wechat_comwechat_slave.shutdown")


def current_master() -> Any:
    """Return ``ehforwarderbot.coordinator.master`` when available."""
    try:  # imported lazily so this module stays importable without EFB installed
        from ehforwarderbot import coordinator  # type: ignore

        return getattr(coordinator, "master", None)
    except Exception:
        return None


def current_slave_channels() -> List[Any]:
    """Return the list of registered slave channels (possibly empty)."""
    try:
        from ehforwarderbot import coordinator  # type: ignore

        slaves = getattr(coordinator, "slaves", None) or {}
        return list(slaves.values())
    except Exception:
        return []


def _is_real_master(obj: Any) -> bool:
    """True only for an actual EFB ``MasterChannel`` instance.

    The deferred installer must never latch onto a stand-in object (for example
    a mock used by unit tests), otherwise it could wrap the wrong target.
    """
    if obj is None:
        return False
    try:
        from ehforwarderbot.channel import MasterChannel  # type: ignore

        return isinstance(obj, MasterChannel)
    except Exception:
        return False


def _current_slave_threads() -> Mapping[str, threading.Thread]:
    try:
        from ehforwarderbot import coordinator  # type: ignore

        return dict(getattr(coordinator, "slave_threads", None) or {})
    except Exception:
        return {}


class ShutdownCoordinator:
    """Ordered, bounded and deterministic shutdown for the EFB slave process."""

    def __init__(
        self,
        *,
        drainables: Sequence[Any] = (),
        logger: Optional[logging.Logger] = None,
        master_budget_sec: float = DEFAULT_MASTER_STOP_BUDGET_SEC,
        slave_drain_budget_sec: float = DEFAULT_SLAVE_DRAIN_BUDGET_SEC,
        hard_exit: bool = True,
        exit_code: int = 0,
        clock: Callable[[], float] = time.monotonic,
        exit_func: Optional[Callable[[int], Any]] = None,
        evidence_path: Optional[str] = None,
        install_deferred: bool = False,
        deferred_deadline_sec: float = 60.0,
    ) -> None:
        self._drainables: List[Any] = [d for d in drainables]
        self.logger = logger or _LOG
        self.master_budget_sec = float(master_budget_sec)
        self.slave_drain_budget_sec = float(slave_drain_budget_sec)
        self.hard_exit = bool(hard_exit)
        self.exit_code = int(exit_code)
        self._clock = clock
        self._exit_func = exit_func or os._exit
        self._evidence_path = evidence_path

        self._lock = threading.RLock()
        self._ran = False
        self._repeat_count = 0
        self._evidence: Optional[Dict[str, Any]] = None
        self._master: Any = None
        self._master_stop: Optional[Callable[..., Any]] = None
        self._install_deferred = bool(install_deferred)
        self._deferred_deadline_sec = float(deferred_deadline_sec)
        self._deferred_thread: Optional[threading.Thread] = None
        self._installed = False

    # ------------------------------------------------------------------ setup

    def register_drainable(self, drainable: Any) -> None:
        """Register an object exposing ``drain_for_shutdown()``."""
        with self._lock:
            if drainable not in self._drainables:
                self._drainables.append(drainable)

    @property
    def installed(self) -> bool:
        return self._installed

    @property
    def ran(self) -> bool:
        return self._ran

    @property
    def evidence(self) -> Optional[Dict[str, Any]]:
        return self._evidence

    def install(self, master: Any = None) -> bool:
        """Wrap ``master.stop_polling`` with the coordinated shutdown.

        Idempotent: re-installing against the same master is a no-op, and
        installing against a *new* master (EFB channel reload) re-wraps.
        Returns ``True`` when the coordinator is active for that master.

        When the master is *resolved implicitly* (the deferred installer path),
        it must be a genuine EFB ``MasterChannel``.  That gate stops the
        background installer from latching onto a stand-in object while the
        real master has not been constructed yet.  An explicitly supplied
        master is trusted, so tests and embedders can pass their own object.
        """
        explicit = master is not None
        if not explicit:
            candidate = current_master()
            if not _is_real_master(candidate):
                # ``ehforwarderbot.__main__.init`` constructs slave channels
                # *before* the master, so at this point the master usually does
                # not exist yet.  Arm the deferred installer and report "not yet
                # installed" -- returning quietly here would disable the whole
                # corrective in production without any signal.
                if self._install_deferred:
                    self._start_deferred_installer()
                return False
            master = candidate
        if master is None:  # pragma: no cover - defensive
            return False
        with self._lock:
            if getattr(master, _WRAPPED_ATTR, None) is self:
                self._installed = True
                return True
            original = getattr(master, "stop_polling", None)
            if not callable(original):
                self.logger.debug("Master %r has no callable stop_polling", master)
                return False
            self._master = master
            self._master_stop = original

            coordinator_self = self

            def coordinated_stop(*args: Any, **kwargs: Any) -> Any:
                return coordinator_self.run(
                    trigger="coordinator.master.stop_polling",
                    master_args=args,
                    master_kwargs=kwargs,
                )

            try:
                setattr(master, "stop_polling", coordinated_stop)
                setattr(master, _WRAPPED_ATTR, self)
            except Exception as exc:  # pragma: no cover - exotic master objects
                self.logger.warning("Cannot install shutdown coordination on master: %s", exc)
                return False
            self._installed = True
            self.logger.debug("Shutdown coordination installed on master %r", master)
        if self._install_deferred:
            self._start_deferred_installer()
        return True

    def _start_deferred_installer(self) -> None:
        """Close the slave-before-master initialisation race.

        ``ehforwarderbot.__main__.init`` instantiates slave channels *before*
        the master channel, so a slave cannot wrap the master at construction
        time.  A short-lived daemon helper waits for the master to appear and
        installs the wrapper.  It always terminates.
        """
        with self._lock:
            if self._deferred_thread is not None and self._deferred_thread.is_alive():
                return
            if self._installed:
                return
            deadline = self._clock() + self._deferred_deadline_sec

            def _wait_and_install() -> None:
                while self._clock() < deadline:
                    if self.install():
                        return
                    time.sleep(0.02)
            thread = threading.Thread(
                target=_wait_and_install, name="efb-shutdown-installer", daemon=True
            )
            self._deferred_thread = thread
        thread.start()

    # -------------------------------------------------------------- execution

    def run(
        self,
        *,
        trigger: str = "manual",
        master_args: Sequence[Any] = (),
        master_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Execute the ordered, bounded shutdown exactly once."""
        master_kwargs = dict(master_kwargs or {})
        with self._lock:
            if self._ran:
                # Repeat signals / repeated stop calls must be inert (no double
                # flush, no double delivery, no deadlock, no exception) while
                # still being *observable*: the caller has to be able to tell
                # that this invocation performed no work.
                self._repeat_count += 1
                base = (
                    dict(self._evidence)
                    if self._evidence
                    else self._empty_evidence(trigger, repeated=True)
                )
                base["repeated"] = True
                base["repeat_count"] = self._repeat_count
                base["repeat_trigger"] = trigger
                return base
            self._ran = True

        started = self._clock()
        phases: List[Dict[str, Any]] = []
        evidence: Dict[str, Any] = {
            "event": "efb_shutdown_evidence",
            "version": 1,
            "trigger": trigger,
            "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "master_stop_budget_sec": self.master_budget_sec,
            "slave_drain_budget_sec": self.slave_drain_budget_sec,
            "hard_exit": self.hard_exit,
            "exit_code": self.exit_code,
            "slave_drain": [],
            "delivery_suppressed": False,
            "master_stop_started": False,
            "master_stop_completed": False,
            "master_stop_bounded_out": False,
            "master_stop_elapsed_sec": None,
            "repeated": False,
        }

        # ---- phase 1: drain slave durable state -----------------------------
        phase_started = self._clock()
        for drainable in list(self._drainables):
            name = str(
                getattr(drainable, "channel_id", None)
                or type(drainable).__name__
            )
            item: Dict[str, Any] = {"channel": name, "ok": False}
            item_started = self._clock()
            drain = getattr(drainable, "drain_for_shutdown", None)
            if callable(drain):
                try:
                    result = drain(budget_sec=self.slave_drain_budget_sec)
                    item["ok"] = True
                    if isinstance(result, Mapping):
                        item["detail"] = dict(result)
                except Exception as exc:
                    item["error"] = f"{type(exc).__name__}: {exc}"
                    self.logger.exception("Slave drain failed for %s", name)
            else:
                item["error"] = "drain_for_shutdown() not implemented"
            item["elapsed_sec"] = round(self._clock() - item_started, 6)
            evidence["slave_drain"].append(item)
        phases.append(
            {
                "phase": "slave_drain",
                "elapsed_sec": round(self._clock() - phase_started, 6),
                "ok": all(i["ok"] for i in evidence["slave_drain"])
                if evidence["slave_drain"]
                else None,
            }
        )

        # ---- phase 2: refuse further external dispatch ----------------------
        phase_started = self._clock()
        suppressed = True
        for drainable in list(self._drainables):
            suppress = getattr(drainable, "suppress_external_dispatch", None)
            if callable(suppress):
                try:
                    suppress()
                except Exception:
                    suppressed = False
                    self.logger.exception("Failed to suppress external dispatch")
        evidence["delivery_suppressed"] = suppressed
        phases.append(
            {"phase": "suppress_dispatch", "elapsed_sec": round(self._clock() - phase_started, 6)}
        )

        # ---- phase 3: bounded master stop -----------------------------------
        phase_started = self._clock()
        master_stop = self._master_stop
        if callable(master_stop):
            evidence["master_stop_started"] = True
            thread = threading.Thread(
                target=self._call_master_stop,
                args=(master_stop, tuple(master_args), master_kwargs),
                name="efb-master-stop",
                daemon=True,
            )
            thread.start()
            thread.join(timeout=max(0.0, self.master_budget_sec))
            evidence["master_stop_completed"] = not thread.is_alive()
            evidence["master_stop_bounded_out"] = thread.is_alive()
            if thread.is_alive():
                self.logger.warning(
                    "Master stop_polling exceeded %.3fs budget; detaching daemon "
                    "stop thread and continuing with durable-state-safe exit",
                    self.master_budget_sec,
                )
        else:
            self.logger.debug("No master stop_polling captured; skipping master stop")
        evidence["master_stop_elapsed_sec"] = round(self._clock() - phase_started, 6)
        phases.append(
            {"phase": "master_stop_bounded", "elapsed_sec": evidence["master_stop_elapsed_sec"]}
        )

        # ---- phase 4: evidence ---------------------------------------------
        evidence["phases"] = phases
        evidence["elapsed_sec"] = round(self._clock() - started, 6)
        with self._lock:
            self._evidence = evidence
        self._emit(evidence)

        # ---- phase 5: deterministic exit ------------------------------------
        if self.hard_exit:
            self._flush_streams()
            self._exit_func(self.exit_code)

        return evidence

    @staticmethod
    def _call_master_stop(
        master_stop: Callable[..., Any], args: Sequence[Any], kwargs: Mapping[str, Any]
    ) -> None:
        try:
            master_stop(*args, **dict(kwargs))
        except Exception:  # pragma: no cover - defensive; must not abort shutdown
            _LOG.exception("Master stop_polling raised during coordinated shutdown")

    def _emit(self, evidence: Mapping[str, Any]) -> None:
        payload = json.dumps(evidence, ensure_ascii=False, sort_keys=True)
        try:
            self.logger.info("%s %s", SHUTDOWN_EVIDENCE_MARKER, payload)
        except Exception:  # pragma: no cover
            pass
        if self._evidence_path:
            try:
                path = os.path.abspath(os.path.expanduser(str(self._evidence_path)))
                os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write(payload + "\n")
            except Exception:  # pragma: no cover - evidence must never break exit
                self.logger.warning("Could not persist shutdown evidence to %s", self._evidence_path)

    @staticmethod
    def _flush_streams() -> None:
        try:
            logging.shutdown()
        except Exception:  # pragma: no cover
            pass
        for stream in (sys.stdout, sys.stderr):
            try:
                stream.flush()
            except Exception:  # pragma: no cover
                pass

    def _empty_evidence(self, trigger: str, *, repeated: bool) -> Dict[str, Any]:
        return {
            "event": "efb_shutdown_evidence",
            "version": 1,
            "trigger": trigger,
            "repeated": repeated,
            "hard_exit": self.hard_exit,
            "exit_code": self.exit_code,
        }


# --------------------------------------------------------------------------- #
# Process-wide installation helper
# --------------------------------------------------------------------------- #

_REGISTRY_LOCK = threading.Lock()
_REGISTRY: Dict[str, ShutdownCoordinator] = {}


def install_shutdown_coordinator(
    channel: Any,
    *,
    logger: Optional[logging.Logger] = None,
    master_budget_sec: float = DEFAULT_MASTER_STOP_BUDGET_SEC,
    slave_drain_budget_sec: float = DEFAULT_SLAVE_DRAIN_BUDGET_SEC,
    hard_exit: bool = True,
    exit_code: int = 0,
    evidence_path: Optional[str] = None,
    install_deferred: bool = True,
    clock: Callable[[], float] = time.monotonic,
    exit_func: Optional[Callable[[int], Any]] = None,
) -> ShutdownCoordinator:
    """Install (once per process per channel id) the shutdown coordinator.

    Safe to call repeatedly; a new coordinator replaces the previous one only if
    the previously installed coordinator has not yet run.
    """
    key = str(getattr(channel, "channel_id", None) or "default")
    with _REGISTRY_LOCK:
        existing = _REGISTRY.get(key)
        if existing is not None and existing.installed and not existing.ran:
            existing.register_drainable(channel)
            return existing

        coordinator = ShutdownCoordinator(
            drainables=[channel],
            logger=logger or getattr(channel, "logger", None) or _LOG,
            master_budget_sec=master_budget_sec,
            slave_drain_budget_sec=slave_drain_budget_sec,
            hard_exit=hard_exit,
            exit_code=exit_code,
            evidence_path=evidence_path,
            install_deferred=install_deferred,
            clock=clock,
            exit_func=exit_func,
        )
        _REGISTRY[key] = coordinator

    # Drain any sibling slave channels registered before this one.
    for slave in current_slave_channels():
        coordinator.register_drainable(slave)

    coordinator.install()
    return coordinator
