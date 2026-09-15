"""EFB Linux WeChat Slave backed by WeChat Core HTTP API V1.

The class keeps the lifecycle/chat/message-dispatch architecture of the
ComWechat upstream channel while replacing its Windows Hook backend with the
frozen account-aware Core contract.
"""

from __future__ import annotations

import base64
import json
import logging
import mimetypes
import threading
import time
import uuid
from collections import OrderedDict
from pathlib import Path
from typing import Any, BinaryIO, Collection, Dict, Mapping, Optional, Set, Tuple

import yaml

from ehforwarderbot import Message, MsgType, Status, coordinator
from ehforwarderbot import utils as efb_utils
from ehforwarderbot.channel import SlaveChannel
from ehforwarderbot.chat import Chat, ChatMember
from ehforwarderbot.exceptions import (
    EFBChatNotFound,
    EFBException,
    EFBMessageError,
    EFBMessageTypeNotSupported,
    EFBOperationNotSupported,
)
from ehforwarderbot.status import ChatUpdates, MessageRemoval
from ehforwarderbot.types import ChatID, InstanceID, MessageID

from . import __version__ as version
from .ChatMgr import ChatMgr
from .Core import CoreAPIError, CoreClient, CoreContractError, CoreError, CoreUnavailableError, CursorStore, EchoStore
from .CoreMessage import CoreMessageBuilder, MediaPendingError, MediaPermanentError
from .EffectLedger import (
    BLOCKED_UNCERTAIN_EFFECT,
    DELIVERY_SEMANTICS,
    STATE_DELIVERED,
    STATE_MEDIA_FAILED,
    STATE_PENDING_MEDIA,
    STATE_RESERVED,
    STATE_UNCERTAIN,
    EffectLedger,
)
from .ShutdownCoordinator import (
    SHUTDOWN_EVIDENCE_MARKER,
    ShutdownCoordinator,
    install_shutdown_coordinator,
)
from .UID import InvalidUID, decode_chat_uid


DEFAULT_CONFIG: Dict[str, Any] = {
    "core": {
        "base_url": "http://127.0.0.1:8080",
        "timeout": 10,
        "poll_timeout": 15,
        "verify_tls": True,
    },
    "consumer_id": "efb-linux-wechat",
    "account_ids": [],
    "poll_interval": 1.0,
    "event_limit": 50,
    "startup_healthcheck": True,
    "bootstrap_mode": "at_head",
    "startup_history_projection": False,
    "media_retry_max_attempts": 20,
    "media_retry_deadline_sec": 300.0,
    "media_retry_base_sec": 1.0,
    "media_retry_max_sec": 30.0,
    # Retry3 graceful-shutdown coordination (defect R14-EFB-D3 / Exit 137).
    "shutdown_master_budget_sec": 1.0,
    "shutdown_slave_drain_budget_sec": 0.75,
    "shutdown_hard_exit": True,
    "shutdown_install_deferred": True,
}


class LinuxWeChatChannel(SlaveChannel):
    channel_name = "Linux WeChat"
    channel_emoji = "🐧"
    channel_id = "wechat.linux"
    __version__ = version.__version__

    supported_message_types = {
        MsgType.Text,
        MsgType.Link,
        MsgType.Image,
        MsgType.Sticker,
        MsgType.Animation,
        MsgType.File,
        MsgType.Video,
        MsgType.Voice,
    }

    logger = logging.getLogger("efb_wechat_linux_slave")

    def __init__(
        self,
        instance_id: InstanceID = None,
        *,
        core_client: Optional[CoreClient] = None,
        config: Optional[Mapping[str, Any]] = None,
        data_path: Optional[Path] = None,
    ) -> None:
        super().__init__(instance_id=instance_id)
        self.config = self._merge_config(config if config is not None else self._load_config())
        core_cfg = self.config["core"]
        self.core = core_client or CoreClient(
            str(core_cfg["base_url"]),
            timeout=float(core_cfg["timeout"]),
            verify_tls=bool(core_cfg["verify_tls"]),
        )
        self.poll_timeout = max(0, min(int(core_cfg.get("poll_timeout", 15)), 30))
        self.event_limit = max(1, min(int(self.config.get("event_limit", 50)), 200))
        self.poll_interval = max(0.05, float(self.config.get("poll_interval", 1.0)))
        self.account_filter: Set[str] = {
            str(item) for item in self.config.get("account_ids", []) if str(item)
        }
        self.sender_capabilities: Dict[str, Any] = {}
        self.account_sender_capabilities: Dict[str, Dict[str, Any]] = {}
        consumer_base = str(self.config.get("consumer_id") or "efb-linux-wechat")
        self.consumer_id = f"{consumer_base}:{self.channel_id}"

        self.chat_mgr = ChatMgr(self)
        self.message_builder = CoreMessageBuilder(self.core, self.chat_mgr)
        self._stop_event = threading.Event()
        # Retry3 shutdown coordination state.
        self._shutdown_in_progress = threading.Event()
        self._checkpoint_flush_lock = threading.Lock()
        self._checkpoint_flush_completed = False
        self._checkpoint_flush_cursor: Optional[int] = None
        self._stop_polling_lock = threading.Lock()
        self._shutdown_coordinator: Optional[ShutdownCoordinator] = None
        self._shutdown_evidence: Dict[str, Any] = {}
        self._message_cache: "OrderedDict[Tuple[str, str], Message]" = OrderedDict()
        self._message_cache_size = 2048
        self.media_retry_max_attempts = max(1, int(self.config.get("media_retry_max_attempts", 20)))
        self.media_retry_deadline_sec = max(0.0, float(self.config.get("media_retry_deadline_sec", 300.0)))
        self.media_retry_base_sec = max(0.0, float(self.config.get("media_retry_base_sec", 1.0)))
        self.media_retry_max_sec = max(
            self.media_retry_base_sec,
            float(self.config.get("media_retry_max_sec", 30.0)),
        )

        resolved_data_path = Path(data_path) if data_path is not None else efb_utils.get_data_path(self.channel_id)
        self.cursor_store = CursorStore(resolved_data_path / "core-event-cursor.json")
        self.echo_store = EchoStore(resolved_data_path / "core-send-echo.json")
        self.effect_ledger = EffectLedger(resolved_data_path / "core-effect-ledger.sqlite3")
        reconciled = self.effect_ledger.reconcile_on_startup(self.consumer_id)
        if reconciled:
            self.logger.warning(
                "Startup reconciliation marked %d lingering RESERVED effects as UNCERTAIN (%s): %s",
                len(reconciled),
                BLOCKED_UNCERTAIN_EFFECT,
                reconciled,
            )
        self.startup_history_projection = bool(self.config.get("startup_history_projection", False))

        # Register graceful shutdown handlers
        try:
            import signal
            if threading.current_thread() is threading.main_thread():
                signal.signal(signal.SIGINT, self._sig_handler)
                if hasattr(signal, "SIGTERM"):
                    signal.signal(signal.SIGTERM, self._sig_handler)
        except (ValueError, AttributeError):
            pass

        if bool(self.config.get("startup_healthcheck", True)):
            try:
                self._health()
            except CoreContractError:
                raise
            except CoreUnavailableError as exc:
                # Core may start after EFB. Polling retries without losing cursor.
                self.logger.warning("Core is not reachable during channel startup: %s", exc)

        try:
            aligned_cursor = self._ensure_bootstrap_aligned()
            self.logger.info("Cursor aligned with Core initial position: %s", aligned_cursor)
        except Exception as exc:
            self.logger.debug("Could not align cursor during init (will align before first poll): %s", exc)

        self.logger.info(
            "Linux WeChat Slave initialized: version=%s core=%s accounts=%s",
            self.__version__,
            getattr(self.core, "base_url", "injected"),
            sorted(self.account_filter) or "all",
        )

        self._install_shutdown_coordinator()

    # ------------------------------------------------------------------ #
    # Retry3 graceful shutdown coordination (defect R14-EFB-D3)
    # ------------------------------------------------------------------ #

    def _install_shutdown_coordinator(self) -> Optional[ShutdownCoordinator]:
        """Install the ordered/bounded shutdown coordinator (idempotent)."""
        if self._shutdown_coordinator is not None:
            return self._shutdown_coordinator
        try:
            coordinator_obj = install_shutdown_coordinator(
                self,
                logger=self.logger,
                master_budget_sec=float(self.config.get("shutdown_master_budget_sec", 1.0)),
                slave_drain_budget_sec=float(
                    self.config.get("shutdown_slave_drain_budget_sec", 0.75)
                ),
                hard_exit=bool(self.config.get("shutdown_hard_exit", True)),
                install_deferred=bool(self.config.get("shutdown_install_deferred", True)),
            )
        except Exception:
            self.logger.exception("Failed to install shutdown coordinator")
            return None
        self._shutdown_coordinator = coordinator_obj
        return coordinator_obj

    def suppress_external_dispatch(self) -> None:
        """Refuse any further outbound external delivery (idempotent)."""
        self._shutdown_in_progress.set()

    def drain_for_shutdown(self, *, budget_sec: float = 0.75) -> Dict[str, Any]:
        """Quiesce polling and durably flush local state before the master stops.

        Order (Retry3 corrective engineering, defect R14-EFB-D3):
          1. stop polling Core for new work;
          2. flush the durable cursor / checkpoint to Core;
          3. checkpoint the effect-ledger WAL so DELIVERED/RESERVED rows are
             durable on disk (the ledger is never deleted or truncated);
          4. forbid any further external dispatch.
        """
        started = time.monotonic()
        detail: Dict[str, Any] = {
            "poll_stopped": False,
            "core_session_closed": False,
            "checkpoint_flushed": False,
            "checkpoint_cursor": None,
            "ledger_wal_checkpointed": False,
            "poll_thread_joined": False,
            "delivery_suppressed": False,
        }

        # 4 first: no new outbound delivery may be attempted from here on.
        self.suppress_external_dispatch()
        detail["delivery_suppressed"] = True

        # 1: stop the Core poll loop.
        self.stop_polling()
        detail["poll_stopped"] = self._stop_event.is_set()

        # 1b: break any in-flight Core poll request so the poll thread can exit.
        session = getattr(self.core, "session", None)
        if session is not None:
            try:
                session.close()
                detail["core_session_closed"] = True
            except Exception:
                self.logger.debug("Core session close during shutdown failed", exc_info=True)

        # 1c: bounded join of the slave poll thread when it is reachable.
        deadline = started + max(0.0, float(budget_sec))
        for thread in self._slave_poll_threads():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if thread.is_alive():
                thread.join(timeout=remaining)
            detail["poll_thread_joined"] = not thread.is_alive()

        # 2: flush the final checkpoint synchronously (idempotent).
        cursor = self._flush_final_checkpoint()
        detail["checkpoint_flushed"] = self._checkpoint_flush_completed
        detail["checkpoint_cursor"] = cursor

        # 3: make the effect-ledger WAL durable without mutating any row.
        try:
            self.effect_ledger.checkpoint_wal()
            detail["ledger_wal_checkpointed"] = True
        except Exception:
            self.logger.warning("Effect-ledger WAL checkpoint on shutdown failed", exc_info=True)

        detail["elapsed_sec"] = round(time.monotonic() - started, 6)
        self._shutdown_evidence = detail
        self.logger.info(
            "%s drain complete: %s",
            SHUTDOWN_EVIDENCE_MARKER,
            json.dumps(detail, ensure_ascii=False, sort_keys=True),
        )
        return detail

    def _slave_poll_threads(self) -> Tuple[threading.Thread, ...]:
        """Best-effort lookup of this channel's EFB poll thread."""
        try:
            from ehforwarderbot import coordinator as efb_coordinator

            threads = getattr(efb_coordinator, "slave_threads", None) or {}
            found = [t for key, t in threads.items() if str(key).startswith(self.channel_id)]
            return tuple(found)
        except Exception:
            return ()

    def _sig_handler(self, signum: int, frame: Any) -> None:
        self.logger.info("Received termination signal %s, initiating graceful shutdown < 2s...", signum)
        coordinator_obj = self._shutdown_coordinator
        if coordinator_obj is not None and not coordinator_obj.ran:
            # Run the full ordered/bounded path even if EFB's own SIGTERM handler
            # is not the one that fired (e.g. the slave registered last).
            coordinator_obj.run(trigger=f"signal:{signum}")
            return
        self.stop_polling()

    def _ensure_bootstrap_aligned(self) -> str:
        """Align local cursor with Core initial_cursor before polling."""
        return self.cursor_store.align_with_core(
            self.core,
            self.consumer_id,
            default_mode=str(self.config.get("bootstrap_mode") or "at_head"),
        )

    def _health(self) -> Dict[str, Any]:
        payload = self.core.health()
        capabilities = payload.get("sender_capabilities")
        if isinstance(capabilities, Mapping):
            self.sender_capabilities = dict(capabilities)
        return payload

    @staticmethod
    def _merge_config(raw: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
        merged = {
            "core": dict(DEFAULT_CONFIG["core"]),
            **{key: value for key, value in DEFAULT_CONFIG.items() if key != "core"},
        }
        if raw:
            for key, value in raw.items():
                if key == "core" and isinstance(value, Mapping):
                    merged["core"].update(dict(value))
                else:
                    merged[key] = value
        account_ids = merged.get("account_ids")
        if account_ids in (None, ""):
            merged["account_ids"] = []
        elif isinstance(account_ids, str):
            merged["account_ids"] = [account_ids]
        elif not isinstance(account_ids, list):
            raise ValueError("account_ids must be a list of Core account IDs")
        return merged

    def _load_config(self) -> Dict[str, Any]:
        path = efb_utils.get_config_path(self.channel_id)
        if not path.exists():
            self.logger.warning("No config file at %s; using defaults", path)
            return {}
        try:
            with path.open("r", encoding="utf-8") as handle:
                loaded = yaml.safe_load(handle) or {}
        except (OSError, yaml.YAMLError) as exc:
            raise EFBException(f"Unable to read Linux WeChat Slave config: {exc}") from exc
        if not isinstance(loaded, dict):
            raise EFBException("Linux WeChat Slave config must contain a YAML mapping")
        return loaded

    def _selected_accounts(self) -> Collection[Dict[str, Any]]:
        accounts = self.core.list_accounts()
        selected = []
        for account in accounts:
            account_id = str(account.get("account_id") or "")
            if not account_id:
                continue
            if self.account_filter and account_id not in self.account_filter:
                continue
            display_name = str(account.get("display_name") or account_id)
            self.chat_mgr.set_account_name(account_id, display_name)
            runtime = account.get("runtime") if isinstance(account.get("runtime"), Mapping) else {}
            capabilities = runtime.get("sender_capabilities") if isinstance(runtime, Mapping) else None
            if isinstance(capabilities, Mapping):
                self.account_sender_capabilities[account_id] = dict(capabilities)
            selected.append(account)
        return selected

    def _sender_capabilities_for(self, account_id: str) -> Dict[str, Any]:
        capabilities = self.account_sender_capabilities.get(account_id)
        if capabilities is not None:
            return dict(capabilities)
        try:
            self._selected_accounts()
        except CoreError:
            pass
        capabilities = self.account_sender_capabilities.get(account_id)
        return dict(capabilities if capabilities is not None else self.sender_capabilities)

    def _resolve_core_chat(self, account_id: str, chat_id: str) -> Chat:
        cached = self.chat_mgr.get_by_core(account_id, chat_id)
        if cached is not None:
            return cached
        account_name = self.chat_mgr.account_name(account_id)
        if account_name == account_id:
            for account in self._selected_accounts():
                if str(account.get("account_id")) == account_id:
                    account_name = str(account.get("display_name") or account_id)
                    break
        for record in self.core.list_chats(account_id):
            chat = self.chat_mgr.build_core_chat(record, account_name)
            if str(record.get("chat_id")) == chat_id:
                return chat
        raise EFBChatNotFound(f"Core chat not found: account={account_id!r} chat={chat_id!r}")

    def get_chats(self) -> Collection[Chat]:
        try:
            self._health()
            result = []
            for account in self._selected_accounts():
                account_id = str(account["account_id"])
                account_name = str(account.get("display_name") or account_id)
                for record in self.core.list_chats(account_id):
                    result.append(self.chat_mgr.build_core_chat(record, account_name))
            return result
        except CoreError as exc:
            raise EFBException(str(exc)) from exc

    def get_chat(self, chat_uid: ChatID) -> Chat:
        uid = str(chat_uid)
        cached = self.chat_mgr.get_by_uid(uid)
        if cached is not None:
            return cached
        try:
            account_id, chat_id = decode_chat_uid(uid)
        except InvalidUID as exc:
            raise EFBChatNotFound(
                "Linux WeChat chat IDs are account-scoped encoded IDs; refresh ETM /link instead of using a raw wxid"
            ) from exc
        if self.account_filter and account_id not in self.account_filter:
            raise EFBChatNotFound(f"Account is not enabled for this slave: {account_id}")
        try:
            return self._resolve_core_chat(account_id, chat_id)
        except CoreError as exc:
            raise EFBChatNotFound(str(exc)) from exc

    def get_chat_picture(self, chat: Chat) -> BinaryIO:
        raise EFBOperationNotSupported("Core Interface Contract V1 does not expose chat avatars")

    def get_chat_member_picture(self, chat_member: ChatMember) -> BinaryIO:
        raise EFBOperationNotSupported("Core Interface Contract V1 does not expose member avatars")

    @staticmethod
    def _read_message_file(msg: Message) -> bytes:
        if msg.file is not None:
            handle = msg.file
            try:
                current = handle.tell()
            except (OSError, AttributeError):
                current = None
            try:
                handle.seek(0)
                content = handle.read()
            finally:
                if current is not None:
                    try:
                        handle.seek(current)
                    except OSError:
                        pass
            if isinstance(content, str):
                return content.encode("utf-8")
            return bytes(content)
        if msg.path:
            return Path(msg.path).read_bytes()
        raise EFBMessageError("Media message has neither file nor path")

    @staticmethod
    def _mention_member_ids(msg: Message) -> Collection[str]:
        if not msg.substitutions:
            return []
        result = []
        for target in msg.substitutions.values():
            vendor = getattr(target, "vendor_specific", {}) or {}
            core = vendor.get("core") if isinstance(vendor, dict) else None
            member_id = core.get("member_id") if isinstance(core, dict) else None
            if member_id and member_id not in result:
                result.append(str(member_id))
        return result

    @staticmethod
    def _link_text(msg: Message) -> str:
        text = msg.text or ""
        attrs = getattr(msg, "attributes", None)
        url = str(getattr(attrs, "url", "") or "")
        if url and url not in text:
            text = f"{text}\n{url}".strip()
        return text or url or "[Link]"

    @staticmethod
    def _quote_fallback(msg: Message, text: str) -> str:
        target = msg.target if isinstance(msg.target, Message) else None
        if target is None:
            return text
        target_text = target.text or target.type.name
        author = getattr(target, "author", None)
        author_name = getattr(author, "display_name", None) or getattr(author, "name", None) or ""
        prefix = f"@{author_name}：" if author_name else ""
        return f"「{prefix}{target_text}」\n---\n{text}"

    def _send_target_id(
        self,
        msg: Message,
        account_id: str,
        chat_id: str,
        capabilities: Mapping[str, Any],
    ) -> Tuple[Optional[str], bool]:
        if not isinstance(msg.target, Message) or not msg.target.uid or not msg.target.chat:
            return None, False
        try:
            target_account, target_chat = decode_chat_uid(str(msg.target.chat.uid))
        except InvalidUID:
            return None, True
        if target_account != account_id or target_chat != chat_id:
            return None, True
        translated = self.echo_store.core_message_id(str(msg.target.uid))
        if translated is None:
            # ETM has a Core send_id, but Core has not reported the final
            # echo_message_id yet. Sending send_id as target_message_id would
            # be invalid, so retain old-EWS visible quote behaviour for now.
            return None, True
        if capabilities.get("native_reply") is False:
            return None, True
        return translated, False

    def _base_send_payload(
        self,
        msg: Message,
        account_id: str,
        chat_id: str,
        capabilities: Mapping[str, Any],
    ) -> Tuple[Dict[str, Any], str, bool]:
        target_message_id, quote_fallback = self._send_target_id(
            msg, account_id, chat_id, capabilities
        )
        core_vendor = msg.vendor_specific.get("core", {}) if isinstance(msg.vendor_specific, dict) else {}
        request_id = str(core_vendor.get("client_request_id") or msg.uid or uuid.uuid4().hex)
        payload: Dict[str, Any] = {
            "account_id": account_id,
            "chat_id": chat_id,
            "client_request_id": request_id,
        }
        if target_message_id:
            payload["target_message_id"] = target_message_id
        return payload, request_id, quote_fallback

    def send_message(self, msg: Message) -> Message:
        if msg.edit:
            raise EFBOperationNotSupported("Core Interface Contract V1 has no message-edit operation")
        try:
            account_id, chat_id = decode_chat_uid(str(msg.chat.uid))
        except (AttributeError, InvalidUID) as exc:
            raise EFBChatNotFound("Destination is not a Linux WeChat account-scoped chat") from exc
        if self.account_filter and account_id not in self.account_filter:
            raise EFBChatNotFound(f"Account is not enabled for this slave: {account_id}")

        capabilities = self._sender_capabilities_for(account_id)
        payload, request_id, quote_fallback = self._base_send_payload(
            msg, account_id, chat_id, capabilities
        )
        if quote_fallback and msg.type not in {MsgType.Text, MsgType.Link}:
            raise EFBOperationNotSupported(
                "The connected Core sender cannot safely preserve reply semantics for this non-text message"
            )
        try:
            if msg.type in {MsgType.Text, MsgType.Link}:
                text = msg.text if msg.type == MsgType.Text else self._link_text(msg)
                if quote_fallback:
                    text = self._quote_fallback(msg, text)
                if not text:
                    raise EFBMessageError("Cannot send an empty text message")
                payload["text"] = text
                mentions = list(self._mention_member_ids(msg))
                if mentions:
                    raw_limit = capabilities.get("max_mentions")
                    try:
                        mention_limit = int(raw_limit)
                    except (TypeError, ValueError):
                        mention_limit = 0
                    if mention_limit > 0 and len(mentions) > mention_limit:
                        raise EFBOperationNotSupported(
                            f"The connected Core sender supports at most {mention_limit} verified mention(s) per send"
                        )
                    payload["mention_member_ids"] = mentions
                receipt = self.core.send_text(payload, request_id)
            elif msg.type in {MsgType.Image, MsgType.Sticker, MsgType.Animation}:
                if msg.text and capabilities.get("media_caption") is False:
                    raise EFBOperationNotSupported(
                        "The connected Core sender cannot currently preserve image captions"
                    )
                content = self._read_message_file(msg)
                payload["content_base64"] = base64.b64encode(content).decode("ascii")
                filename = msg.filename or (Path(msg.path).name if msg.path else "image")
                payload["filename"] = filename
                payload["mime_type"] = msg.mime or mimetypes.guess_type(filename)[0] or "application/octet-stream"
                if msg.text:
                    payload["caption"] = msg.text
                receipt = self.core.send_image(payload, request_id)
            elif msg.type in {MsgType.File, MsgType.Video, MsgType.Voice}:
                if capabilities.get("file") is False:
                    raise EFBOperationNotSupported(
                        "The connected Core sender does not currently provide a verified arbitrary-file send primitive"
                    )
                content = self._read_message_file(msg)
                payload["content_base64"] = base64.b64encode(content).decode("ascii")
                filename = msg.filename or (Path(msg.path).name if msg.path else "attachment")
                payload["filename"] = filename
                payload["mime_type"] = msg.mime or mimetypes.guess_type(filename)[0] or "application/octet-stream"
                if msg.text:
                    payload["caption"] = msg.text
                receipt = self.core.send_file(payload, request_id)
            else:
                raise EFBMessageTypeNotSupported(f"Unsupported outgoing EFB message type: {msg.type}")
        except CoreAPIError as exc:
            raise EFBMessageError(f"Core rejected message [{exc.code}]: {exc.message}") from exc
        except CoreError as exc:
            raise EFBMessageError(str(exc)) from exc

        send_id = str(receipt.get("send_id") or "")
        echo_message_id = str(receipt.get("echo_message_id") or "")
        msg.uid = MessageID(echo_message_id or send_id or request_id)
        vendor = dict(msg.vendor_specific or {})
        vendor["core"] = {
            **dict(vendor.get("core") or {}),
            "client_request_id": request_id,
            "send_receipt": dict(receipt),
            "uid_is_send_id": bool(send_id and not echo_message_id),
        }
        msg.vendor_specific = vendor
        if send_id:
            if echo_message_id:
                self.echo_store.link(send_id, echo_message_id)
            else:
                self.echo_store.mark_pending(send_id, request_id)
        self._remember_message(msg)
        return msg

    def send_status(self, status: Status) -> None:
        if isinstance(status, MessageRemoval):
            raise EFBOperationNotSupported("Core Interface Contract V1 does not expose outgoing recall")
        raise EFBOperationNotSupported(f"Unsupported outgoing EFB status: {type(status).__name__}")

    def _remember_message(self, msg: Message) -> None:
        if not msg.chat or not msg.uid:
            return
        key = (str(msg.chat.uid), str(msg.uid))
        self._message_cache[key] = msg
        self._message_cache.move_to_end(key)
        while len(self._message_cache) > self._message_cache_size:
            self._message_cache.popitem(last=False)

    def get_message_by_id(self, chat: Chat, msg_id: MessageID) -> Optional[Message]:
        return self._message_cache.get((str(chat.uid), str(msg_id)))

    def _deliver_message(self, msg: Message) -> None:
        if getattr(coordinator, "master", None) is None:
            raise CoreUnavailableError("EFB master is not ready; event cursor will not advance")
        msg.deliver_to = coordinator.master
        self._remember_message(msg)
        try:
            coordinator.send_message(msg)
        finally:
            if msg.file is not None and not getattr(msg.file, "closed", False):
                msg.file.close()

    def _emit_removal(self, account_id: str, payload: Mapping[str, Any]) -> None:
        nested = payload.get("message") if isinstance(payload.get("message"), dict) else {}
        chat_id = str(nested.get("chat_id") or payload.get("chat_id") or "")
        message_id = str(nested.get("message_id") or payload.get("message_id") or "")
        if not chat_id or not message_id:
            self.logger.warning("Ignoring message.removed without chat_id/message_id: %r", payload)
            return
        chat = self._resolve_core_chat(account_id, chat_id)
        efb_message_id = self.echo_store.efb_message_id(message_id)
        efb_msg = Message(chat=chat, uid=MessageID(efb_message_id), type=MsgType.Text)
        if getattr(coordinator, "master", None) is None:
            raise CoreUnavailableError("EFB master is not ready; recall cursor will not advance")
        coordinator.send_status(
            MessageRemoval(source_channel=self, destination_channel=coordinator.master, message=efb_msg)
        )

    def _emit_chat_update(self, account_id: str, payload: Mapping[str, Any]) -> None:
        chat_id = str(payload.get("chat_id") or "")
        if not chat_id:
            return
        old = self.chat_mgr.get_by_core(account_id, chat_id)
        try:
            records = self.core.list_chats(account_id)
        except CoreError:
            raise
        record = next((item for item in records if str(item.get("chat_id")) == chat_id), None)
        if record is None:
            removed_uid = self.chat_mgr.remove(account_id, chat_id)
            if removed_uid and getattr(coordinator, "master", None) is not None:
                coordinator.send_status(ChatUpdates(channel=self, removed_chats=(ChatID(removed_uid),)))
            return
        chat = self.chat_mgr.build_core_chat(record, self.chat_mgr.account_name(account_id))
        if getattr(coordinator, "master", None) is not None:
            if old is None:
                coordinator.send_status(ChatUpdates(channel=self, new_chats=(chat.uid,)))
            else:
                coordinator.send_status(ChatUpdates(channel=self, modified_chats=(chat.uid,)))

    def _handle_send_update(self, payload: Mapping[str, Any]) -> None:
        receipt = payload.get("send") if isinstance(payload.get("send"), dict) else payload
        send_id = str(receipt.get("send_id") or "")
        echo_message_id = str(receipt.get("echo_message_id") or "")
        if send_id and echo_message_id:
            self.echo_store.link(send_id, echo_message_id)
        status = str(receipt.get("status") or "")
        if status == "submitted":
            # Do not present upstream FSM success as confirmed delivery.  The
            # final WeChat/Core identity is only known after the DB echo is
            # uniquely reconciled.
            self.logger.info(
                "Core send %s 已提交、等待微信确认 (delivery_certainty=%s)",
                send_id or "<unknown>",
                str(receipt.get("delivery_certainty") or "pending_confirmation"),
            )
        elif status == "sent":
            self.logger.info(
                "Core send %s 已由微信确认 (echo_message_id=%s)",
                send_id or "<unknown>",
                echo_message_id or "<unknown>",
            )
        elif status == "uncertain":
            error = payload.get("error") if isinstance(payload.get("error"), Mapping) else {}
            details = payload.get("details") if isinstance(payload.get("details"), Mapping) else {}
            self.logger.warning(
                "Core delivery certainty is unknown for send %s [%s]: %s details=%r",
                send_id or "<unknown>",
                str(error.get("code") or "agent_wechat_delivery_unknown"),
                str(error.get("message") or "upstream response was not received"),
                dict(details),
            )

    @staticmethod
    def _is_media_message(message: Mapping[str, Any]) -> bool:
        return str(message.get("type") or "") in {"image", "sticker", "voice", "video", "file"}

    def _defer_media(
        self,
        account_id: str,
        message: Mapping[str, Any],
        event_type: str,
        effect_id: str,
        error: Exception,
    ) -> str:
        now = time.time()
        current = self.effect_ledger.get_effect(self.consumer_id, effect_id)
        details = current.get("details", {}) if current else {}
        try:
            previous_attempts = int(details.get("attempt_count") or 0)
        except (TypeError, ValueError):
            previous_attempts = 0
        attempt_count = previous_attempts + 1
        try:
            deadline_at = float(details.get("deadline_at"))
        except (TypeError, ValueError):
            deadline_at = now + self.media_retry_deadline_sec
        delay = min(
            self.media_retry_base_sec * (2 ** max(0, attempt_count - 1)),
            self.media_retry_max_sec,
        )
        next_retry_at = now + delay
        message_id = str(message.get("message_id") or "")
        media_id = str(message.get("media_id") or "")
        self.effect_ledger.mark_media_pending(
            self.consumer_id,
            effect_id,
            account_id=account_id,
            message_id=message_id,
            event_type=event_type,
            message=message,
            media_id=media_id,
            attempt_count=attempt_count,
            next_retry_at=next_retry_at,
            deadline_at=deadline_at,
            last_error=str(error),
        )
        if attempt_count >= self.media_retry_max_attempts or now >= deadline_at:
            reason = (
                f"media retry exhausted after {attempt_count} attempt(s)"
                if attempt_count >= self.media_retry_max_attempts
                else "media retry deadline exceeded"
            )
            self.effect_ledger.mark_media_failed(
                self.consumer_id,
                effect_id,
                account_id=account_id,
                message_id=message_id,
                event_type=event_type,
                reason=reason,
                details={
                    "message": dict(message),
                    "media_id": media_id,
                    "attempt_count": attempt_count,
                    "deadline_at": deadline_at,
                    "last_error": str(error),
                },
            )
            self.logger.error("Media effect %s failed permanently: %s", effect_id, reason)
            return STATE_MEDIA_FAILED
        self.logger.info(
            "Deferred media effect %s in %s (attempt=%d next_retry_at=%.3f): %s",
            effect_id,
            STATE_PENDING_MEDIA,
            attempt_count,
            next_retry_at,
            error,
        )
        return STATE_PENDING_MEDIA

    def _fail_media(
        self,
        account_id: str,
        message: Mapping[str, Any],
        event_type: str,
        effect_id: str,
        error: Exception,
    ) -> None:
        message_id = str(message.get("message_id") or "")
        self.effect_ledger.mark_media_failed(
            self.consumer_id,
            effect_id,
            account_id=account_id,
            message_id=message_id,
            event_type=event_type,
            reason=str(error),
            details={
                "message": dict(message),
                "media_id": str(message.get("media_id") or ""),
                "last_error": str(error),
                "permanent": True,
            },
        )
        self.logger.error("Media effect %s failed closed: %s", effect_id, error)

    def _handle_message_event(
        self,
        event_type: str,
        account_id: str,
        message: Mapping[str, Any],
    ) -> None:
        chat_id = str(message.get("chat_id") or "")
        if not chat_id:
            self.logger.warning("Ignoring %s without chat_id: %r", event_type, message)
            return
        core_message_id = str(message.get("message_id") or "")
        efb_message_id = self.echo_store.efb_message_id(core_message_id) if core_message_id else ""
        if (
            core_message_id
            and efb_message_id != core_message_id
            and str(message.get("direction") or "") == "outgoing"
        ):
            self.logger.debug(
                "Suppressing reconciled outgoing Core echo %s (EFB uid %s)",
                core_message_id,
                efb_message_id,
            )
            return

        is_media = self._is_media_message(message)
        if is_media and not core_message_id:
            self.logger.error("Ignoring media message without durable message_id: %r", message)
            return
        effect_id = (
            self.effect_ledger.compute_effect_id(account_id, core_message_id)
            if core_message_id
            else ""
        )
        current_status = (
            self.effect_ledger.get_effect_status(self.consumer_id, effect_id)
            if effect_id
            else None
        )
        if current_status == STATE_DELIVERED and (event_type == "message.created" or is_media):
            self.logger.info("Suppressing duplicate delivery for effect %s (DELIVERED)", effect_id)
            return
        if current_status in {STATE_RESERVED, STATE_UNCERTAIN}:
            self.logger.warning(
                "Suppressing delivery for effect %s: state is %s; fail-closed (%s)",
                effect_id,
                current_status,
                BLOCKED_UNCERTAIN_EFFECT,
            )
            return
        if current_status == STATE_MEDIA_FAILED:
            self.logger.warning("Suppressing permanently failed media effect %s", effect_id)
            return

        chat = self._resolve_core_chat(account_id, chat_id)
        try:
            efb_msg = self.message_builder.build(message, chat)
        except MediaPendingError as exc:
            if not is_media or not effect_id:
                raise
            self._defer_media(account_id, message, event_type, effect_id, exc)
            return
        except MediaPermanentError as exc:
            if not is_media or not effect_id:
                raise
            self._fail_media(account_id, message, event_type, effect_id, exc)
            return

        reserved = False
        if effect_id and current_status == STATE_PENDING_MEDIA:
            reserved = self.effect_ledger.reserve_pending_effect(
                self.consumer_id,
                effect_id,
                details={"chat_id": chat_id, "media_status": "ready"},
            )
        elif effect_id and (event_type == "message.created" or is_media):
            reserved, reserve_state = self.effect_ledger.reserve_effect(
                self.consumer_id,
                effect_id,
                account_id=account_id,
                message_id=core_message_id,
                event_type=event_type,
                details={"chat_id": chat_id},
            )
            if not reserved:
                self.logger.warning(
                    "Failed to reserve effect %s: state is %s; fail-closed (%s)",
                    effect_id,
                    reserve_state,
                    BLOCKED_UNCERTAIN_EFFECT,
                )
                if efb_msg.file is not None and not getattr(efb_msg.file, "closed", False):
                    efb_msg.file.close()
                return
        if current_status == STATE_PENDING_MEDIA and not reserved:
            if efb_msg.file is not None and not getattr(efb_msg.file, "closed", False):
                efb_msg.file.close()
            self.logger.warning("Pending media effect %s could not transition to RESERVED", effect_id)
            return

        if core_message_id:
            efb_msg.uid = MessageID(efb_message_id or core_message_id)
        if isinstance(efb_msg.target, Message) and efb_msg.target.uid:
            efb_msg.target.uid = MessageID(self.echo_store.efb_message_id(str(efb_msg.target.uid)))
        efb_msg.edit = event_type == "message.updated" and current_status != STATE_PENDING_MEDIA

        try:
            self._deliver_message(efb_msg)
        except Exception as exc:
            if reserved and effect_id:
                self.effect_ledger.mark_uncertain(
                    self.consumer_id,
                    effect_id,
                    reason=f"Exception during external delivery: {exc}",
                )
            raise

        if reserved and effect_id:
            self.effect_ledger.mark_delivered(
                self.consumer_id,
                effect_id,
                efb_uid=str(efb_msg.uid),
                event_type=event_type,
                details={"chat_id": chat_id, "type": str(efb_msg.type)},
            )

    def _retry_pending_media(
        self,
        *,
        account_id: str = "",
        ready_media: Optional[Mapping[str, Any]] = None,
    ) -> int:
        if self._shutdown_in_progress.is_set():
            return 0
        media_id = str(ready_media.get("media_id") or "") if ready_media else ""
        pending = self.effect_ledger.pending_media(
            self.consumer_id,
            due_before=None if ready_media else time.time(),
            media_id=media_id,
        )
        attempted = 0
        for row in pending:
            if account_id and str(row.get("account_id") or "") != account_id:
                continue
            raw_message = row.get("details", {}).get("message")
            if not isinstance(raw_message, dict):
                self.effect_ledger.mark_media_failed(
                    self.consumer_id,
                    str(row["effect_id"]),
                    account_id=str(row["account_id"]),
                    message_id=str(row["message_id"]),
                    event_type=str(row["event_type"]),
                    reason="pending media record has no recoverable message payload",
                )
                continue
            message = dict(raw_message)
            # A scheduled retry probes the authoritative media endpoint. A
            # media.ready event additionally supplies the latest role/status.
            message["media_status"] = str(
                ready_media.get("status") if ready_media else "ready"
            )
            if ready_media and ready_media.get("role"):
                message["media_role"] = str(ready_media["role"])
            if ready_media and ready_media.get("filename"):
                message["filename"] = str(ready_media["filename"])
            if ready_media and ready_media.get("mime_type"):
                message["mime_type"] = str(ready_media["mime_type"])
            self._handle_message_event(
                "message.updated",
                str(row["account_id"]),
                message,
            )
            attempted += 1
        return attempted

    def _handle_event(self, event: Mapping[str, Any]) -> None:
        if self._shutdown_in_progress.is_set():
            # Shutdown has begun: the durable state has already been flushed and
            # no further external side effect may be produced. Events are left
            # unprocessed on purpose so that the preserved cursor replays them
            # after the next start, where the effect ledger decides delivery.
            self.logger.info(
                "Suppressing event processing during shutdown (event_type=%s)",
                str(event.get("event_type") or ""),
            )
            return
        event_type = str(event.get("event_type") or "")
        account_id = str(event.get("account_id") or "")
        if self.account_filter and account_id not in self.account_filter:
            return
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}

        if event_type in {"message.created", "message.updated"}:
            message = payload.get("message") if isinstance(payload.get("message"), dict) else payload
            self._handle_message_event(event_type, account_id, message)
        elif event_type == "message.removed":
            self._emit_removal(account_id, payload)
        elif event_type == "chat.updated":
            self._emit_chat_update(account_id, payload)
        elif event_type == "send.updated":
            self._handle_send_update(payload)
        elif event_type == "media.ready":
            media = payload.get("media") if isinstance(payload.get("media"), dict) else payload
            self._retry_pending_media(account_id=account_id, ready_media=media)
        elif event_type == "account.status":
            self.logger.info("Core event %s for %s: %r", event_type, account_id, payload)
        else:
            # Contract explicitly requires future unknown event types to be tolerated.
            self.logger.warning("Ignoring unknown Core event type %r", event_type)

    def _flush_final_checkpoint(self) -> Optional[int]:
        """Flush the final checkpoint to Core before exiting (Defect D3).

        Idempotent and thread-safe: repeated shutdown signals, or a concurrent
        call from ``poll()``'s ``finally`` block and the shutdown coordinator,
        must not double-flush. Returns the flushed cursor (or ``None``).
        """
        with self._checkpoint_flush_lock:
            if self._checkpoint_flush_completed:
                return self._checkpoint_flush_cursor
            cursor: Optional[int] = None
            try:
                current_cursor = self.cursor_store.load(default=None)
                if current_cursor is not None:
                    cur_int = int(current_cursor)
                    self.core.checkpoint_events(
                        self.consumer_id,
                        cur_int,
                        subscription_account_id=next(iter(self.account_filter), "") if len(self.account_filter) == 1 else "",
                    )
                    cursor = cur_int
                    self.logger.info("Flushed final checkpoint %s on shutdown", cur_int)
                else:
                    self.logger.debug("No local cursor present; nothing to flush on shutdown")
            except Exception as exc:
                self.logger.debug("Final checkpoint flush on shutdown skipped/failed: %s", exc)
                return None
            self._checkpoint_flush_completed = True
            self._checkpoint_flush_cursor = cursor
            return cursor

    def poll_once(self, poll_timeout: Optional[int] = None) -> int:
        """Process one Core event page. Exposed for deterministic integration tests."""
        self._health()
        self._retry_pending_media()
        cursor = self.cursor_store.load(default=None)
        if cursor is None:
            cursor = self._ensure_bootstrap_aligned()
        single_account = next(iter(self.account_filter)) if len(self.account_filter) == 1 else None
        effective_timeout = self.poll_timeout if poll_timeout is None else poll_timeout
        page = self.core.poll_events(
            after=cursor,
            consumer_id=self.consumer_id,
            timeout=effective_timeout,
            limit=self.event_limit,
            account_id=single_account,
        )
        events = page.get("events")
        if not isinstance(events, list):
            raise CoreUnavailableError("Core poll response has no events list")
        processed = 0
        for event in events:
            if not isinstance(event, dict):
                continue
            self._handle_event(event)
            event_cursor = str(event.get("cursor") or "")
            if event_cursor:
                # Cursor is persisted only after local processing succeeds.
                self.cursor_store.save(event_cursor)
            event_id = str(event.get("event_id") or "")
            if event_id:
                try:
                    self.core.ack_events(self.consumer_id, [event_id])
                except CoreError as exc:
                    # Contract states ack does not replace cursor persistence.
                    self.logger.warning("Core event ack failed after cursor persistence: %s", exc)
            processed += 1
        has_more = bool(page.get("has_more"))
        stream_head = page.get("stream_head_cursor")
        current_cursor = self.cursor_store.load(default="0")
        try:
            cur_int = int(current_cursor or "0")
        except ValueError:
            cur_int = 0
        checkpoint_cursor = cur_int if has_more else (int(stream_head) if stream_head is not None else cur_int)
        try:
            self.core.checkpoint_events(
                self.consumer_id,
                checkpoint_cursor,
                subscription_account_id=single_account or "",
            )
        except Exception as exc:
            self.logger.warning("Core event checkpoint failed after local processing: %s", exc)
        return processed

    def poll(self) -> None:
        # The master channel only exists after all slaves are constructed, so the
        # shutdown coordinator is installed here as well (idempotent) to close the
        # slave-before-master initialisation race in ehforwarderbot.__main__.init.
        self._install_shutdown_coordinator()
        backoff = self.poll_interval
        try:
            while not self._stop_event.is_set():
                try:
                    # Bound long-poll duration during loop to <= 1s so stop_event is checked
                    # promptly, guaranteeing shutdown drain completes in < 2s (Defect D3).
                    loop_timeout = min(self.poll_timeout, 1)
                    processed = self.poll_once(poll_timeout=loop_timeout)
                    backoff = self.poll_interval
                    if processed == 0:
                        self._stop_event.wait(self.poll_interval)
                except CoreContractError:
                    self.logger.exception("Core contract is incompatible; polling stopped")
                    raise
                except Exception as exc:
                    if self._stop_event.is_set():
                        self.logger.info("Polling loop cleanly terminated on stop event")
                        break
                    self.logger.exception("Core polling iteration failed; cursor retained for retry: %s", exc)
                    self._stop_event.wait(backoff)
                    backoff = min(max(backoff * 2, self.poll_interval), 30.0)
        finally:
            self._flush_final_checkpoint()

    def stop_polling(self) -> None:
        """Idempotent, re-entrant stop of the Core poll loop (Retry3)."""
        with self._stop_polling_lock:
            already_stopped = self._stop_event.is_set()
            self._stop_event.set()
        if already_stopped:
            self.logger.debug("stop_polling() called again; stop event already set")
        try:
            if hasattr(self.core, "session") and self.core.session:
                self.core.session.close()
        except Exception:
            pass

    @efb_utils.extra(name="Core status", desc="Show WeChat Core V1 health and configured account states.")
    def core_status(self, _: str = "") -> str:
        try:
            payload = {
                "health": self.core.health(),
                "accounts": [
                    account
                    for account in self.core.list_accounts()
                    if not self.account_filter or str(account.get("account_id")) in self.account_filter
                ],
                "event_cursor": self.cursor_store.load(),
            }
            return json.dumps(payload, ensure_ascii=False, indent=2)
        except CoreError as exc:
            return f"Core unavailable: {exc}"


# Compatibility alias for code importing the upstream class name.  The class
# implementation itself is Linux/Core-backed; no ComWechatRobot backend remains.
ComWeChatChannel = LinuxWeChatChannel
