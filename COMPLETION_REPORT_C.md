# Session C Completion Report

Date: 2026-08-31  
Branch: `feat/linux-wechat-slave`  
Worktree: `work/efb-linux-wechat-slave`

## 1. Result

Session C has converted the locked ComWechat EFB slave source into a Linux/Core-backed EFB slave while retaining the EFB adapter architecture required by the taskbook. The active runtime path talks only to WeChat Core Interface Contract V1 over HTTP; it does not import the Windows ComWechatRobot backend and does not open Core SQLite.

Development integration verification passes against the workspace Mock Core and the **real local editable Kettly ETM source tree**. This is not claimed as a live WeChat + live Telegram end-to-end test.

## 2. Locked sources actually used

| Source | Commit | Use |
|---|---|---|
| `ehForwarderBot/efb-wechat-comwechat-slave` | `989db6947f565dbbb5588d04edfca3cf5ca49c24` | Primary implementation/source baseline |
| `ehForwarderBot/efb-wechat-slave` | `80dadf21558c1be28d7ec23f247383b5a229975b` | Legacy EWS behaviour reference |
| `kettly1260/efb-telegram-master` | `36b3382ed784efeba176dba269df47d4df0ef4e7` | Required Telegram Master compatibility target |

The detailed pre-implementation audit is in `SOURCE_AUDIT_C.md` and the behaviour mapping is in `docs/EFB_BEHAVIOR_COMPAT.md`.

## 3. Source reuse / migration map

| Old source | New location | Reuse/change |
|---|---|---|
| ComWechat `ComWechat.py::ComWeChatChannel` | `efb_wechat_comwechat_slave/ComWechat.py::LinuxWeChatChannel` | Retains `SlaveChannel` lifecycle, metadata role, polling/send/chat/status responsibilities; backend callbacks/Hook calls replaced by Core V1 HTTP. `ComWeChatChannel` remains as a compatibility alias. |
| ComWechat `ChatMgr.py` | `efb_wechat_comwechat_slave/ChatMgr.py::ChatMgr` | Retains EFB `GroupChat`/`PrivateChat`/member construction responsibility; global class state replaced by instance/account-scoped cache. |
| ComWechat `MsgProcess.py::MsgWrapper/MsgProcess` | `efb_wechat_comwechat_slave/CoreMessage.py::CoreMessageBuilder` | Retains normalized-backend-message -> EFB-message conversion stage; input is frozen Core normalized schema instead of Hook callback dictionaries. |
| ComWechat recall callback -> `MessageRemoval` | `ComWechat.py::_emit_removal` | Same EFB status contract; message IDs are reconciled through persistent Core send/echo mapping. |
| old EWS `ChatManager.get_chats/search_chat` behaviour | `LinuxWeChatChannel.get_chats/get_chat` + `UID.py` | Restores complete linkable chat enumeration and stable lookup, now account-aware and safe for opaque IDs. |
| old EWS visible quote fallback | `ComWechat.py::_quote_fallback` | Used only when a valid native Core target cannot be supplied, e.g. cross-chat or a still-pending outgoing echo. |
| Kettly `ChatObjectCacheManager` `/link` cache path | compatibility tests | Real Kettly class consumes this slave's `get_chats()` and enrols all account-scoped chats. |
| Kettly `MasterMessageProcessor.attach_target_message` | compatibility tests + `_send_target_id` | Real Kettly target reconstructed from its message-log semantics is translated to Core `target_message_id`. |

## 4. New code required by the Core contract

- `Core.py::CoreClient`: Core V1 health/accounts/chats/events/media/send client with structured errors and contract-version validation.
- `Core.py::CursorStore`: atomic durable event cursor; Core event ack is secondary and never substitutes cursor persistence.
- `Core.py::EchoStore`: durable `send_id` <-> final `echo_message_id` aliases for Kettly reply/recall/restart identity stability.
- `UID.py`: reversible account-aware EFB chat/member IDs without parsing opaque Core IDs.
- multi-account `get_chats/get_chat`, normalized media download, idempotent text/image/file sends, chat updates, incoming recall, unknown-event tolerance and Core status extra.

Known EFB-originated Core outgoing echoes are reconciled and suppressed so Kettly does not create a duplicate Telegram post. Native self-messages sent directly from WeChat have no such mapping and remain visible to ETM.

## 5. Removed runtime dependencies / state

The active package no longer uses `python-comwechatrobot-http`, `WeChatRobot`, Hook opcodes, Windows file paths, `wxpy`, `itchat`, `TTLCache` delivery reliability, global pending-file dictionaries, Peewee or SQLite. The old backend-only modules were removed after their relevant EFB structure/behaviour was migrated into the new modules.

The obsolete package-local Windows/Wine compose file was removed. The replacement `Dockerfile` installs the selected Kettly fork from its exact locked source commit and then installs this slave editable. `scripts/setup-host.sh` and `examples/efb-linux-wechat.service` provide the decoupled host-Python path.

## 6. Verification executed

| Check | Result |
|---|---|
| Branch | `feat/linux-wechat-slave` |
| Python compile | PASS |
| Unit + Mock Core + Kettly compatibility suite | **16 / 16 PASS** |
| Multi-account `/link` cache path | PASS using real Kettly `ChatObjectCacheManager` |
| Kettly reply target path | PASS using real `MasterMessageProcessor.attach_target_message()` |
| Text send/reply | PASS against Mock Core |
| Incoming text/image + file/media semantics | PASS against Mock Core |
| Image/file outgoing send | PASS against Mock Core |
| Durable event cursor + ack/replay boundary | PASS |
| `send.updated` + echo association + duplicate suppression + restart | PASS |
| Incoming recall -> `MessageRemoval` | PASS |
| Editable Kettly source path | PASS: `G:\LLM\WeChat_Hub\upstream\efb-telegram-master-kettly\...` |
| Editable slave package/version | PASS: `efb-linux-wechat-slave 2.0.0a1` |
| EFB entry point | PASS: `wechat.linux = efb_wechat_comwechat_slave:LinuxWeChatChannel` |
| Active runtime search for old Hook/SQLite dependencies | PASS, zero matches |
| `git diff --check` | PASS; only host line-ending conversion warnings were emitted by Git |

The Kettly fork needed `PYTHONUTF8=1` on this Windows development host because its `setup.py` otherwise reads its UTF-8 README using the platform GBK default. The test venv also uses a Windows-compatible `libmagic` package and a newer Tornado compatibility override because Kettly's legacy PTB 13.15 dependency pins Tornado 6.1, which is not a practical Python 3.12 Windows install target. These are development-host constraints; the checked-in Docker/host-Python path defaults to Linux Python 3.10 and the Docker image installs `libmagic1`.

## 7. Contract limitations intentionally not bypassed

1. Core V1 has no full group-member-list endpoint. C enrols normalized members as they are observed but cannot fabricate a complete cold-start roster.
2. Core V1 has no chat/member avatar endpoint, so EFB picture requests are explicitly unsupported.
3. Core V1 has no outgoing recall operation. Incoming recall is supported; Telegram-initiated recall reports `EFBOperationNotSupported` rather than bypassing Core.
4. A successful `/v1/send/*` response means accepted into the Core outbox, not confirmed WeChat delivery.
5. Core delivery is at-least-once. C persists cursors and stable identities but does not claim exactly-once semantics.

## 8. Acceptance boundary

Session C is ready for Session 0 integration at the EFB/Core contract boundary. The evidence here proves C against Mock Core plus real local Kettly source components. A real WeChat account + real Telegram bot/network end-to-end run remains an integration-stage test and has not been claimed as completed by Session C.
