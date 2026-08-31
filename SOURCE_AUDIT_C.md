# SOURCE_AUDIT_C

Audit date: 2026-08-31

Work package: C - EFB Linux WeChat Slave

Implementation checkout: `work/efb-linux-wechat-slave`

Implementation branch required by the taskbook: `feat/linux-wechat-slave`

## 1. Upstream repositories actually read

| Repository | Locked commit | Role |
|---|---|---|
| `ehForwarderBot/efb-wechat-comwechat-slave` | `989db6947f565dbbb5588d04edfca3cf5ca49c24` | Primary EFB adapter/source baseline |
| `ehForwarderBot/efb-wechat-slave` | `80dadf21558c1be28d7ec23f247383b5a229975b` | Legacy EWS behaviour and Telegram UX reference |
| `kettly1260/efb-telegram-master` | `36b3382ed784efeba176dba269df47d4df0ef4e7` | Required real Telegram Master compatibility target |

The Core boundary was also read from the frozen workspace contract:

- `docs/INTERFACE_CONTRACT_V1.md`
- `stack/contracts/openapi.yaml`
- `stack/mock-core/app.py`

No Core SQLite file is used by this work package.

## 2. Source files actually read

### ComWechat primary baseline

- `efb_wechat_comwechat_slave/ComWechat.py`
  - `ComWeChatChannel.__init__`
  - callback registration for self/friend/group/revoke events
  - `handle_msg`
  - `handle_file_msg`
  - `get_chats`
  - `get_chat`
  - `send_message`
  - `send_text`
  - `poll`
  - `stop_polling`
  - `GetContactListBySql`
- `efb_wechat_comwechat_slave/ChatMgr.py`
  - `build_efb_chat_as_group`
  - `build_efb_chat_as_private`
  - `build_efb_chat_as_member`
  - `build_efb_chat_as_system_user`
- `efb_wechat_comwechat_slave/MsgProcess.py`
  - `MsgWrapper`
  - `MsgProcess`
- `efb_wechat_comwechat_slave/CustomTypes.py`
- `setup.py`

### Legacy EWS behaviour reference

- `efb_wechat_slave/chats.py`
  - `ChatManager.wxpy_chat_to_efb_chat`
  - `ChatManager.get_chats`
  - `ChatManager.search_chat`
  - member caching and alias behaviour
- `efb_wechat_slave/__init__.py`
  - `WeChatChannel.poll`
  - `WeChatChannel.send_message`
  - `WeChatChannel.send_status`
  - `WeChatChannel.get_chats`
  - `WeChatChannel.get_chat`
- `efb_wechat_slave/slave_message.py`
  - `SlaveMessageManager.get_chat_and_author`
  - `Decorators.wechat_msg_meta`
  - text/media/location/link/system conversion
  - `ChatUpdates`
  - recall to `MessageRemoval`

### Kettly ETM required compatibility target

- `efb_telegram_master/chat_binding.py`
  - `link_chat_show_list`
  - `slave_chats_pagination`
  - `/link` depends on a complete slave chat cache and stable slave chat UIDs
- `efb_telegram_master/master_message.py`
  - Telegram -> slave `coordinator.send_message`
  - `attach_target_message`
  - reply targets are reconstructed from ETM DB message logs and sent back to the same slave channel
- `efb_telegram_master/slave_message.py`
  - `send_status`
  - `ChatUpdates`, `MemberUpdates`, `MessageRemoval`
  - recall lookup uses both stable slave message UID and slave chat origin UID
- `setup.py`
  - fork is installable editable and depends on `ehforwarderbot>=2.0.0`

## 3. Code/design to reuse

### Directly retained/adapted from ComWechat

1. EFB `SlaveChannel` lifecycle and metadata shape from `ComWeChatChannel`.
2. The separation of chat construction into `ChatMgr`.
3. `GroupChat`, `PrivateChat`, `ChatMember`, and system chat construction semantics.
4. Message-type dispatch in `send_message`.
5. The rule that incoming recalls become EFB `MessageRemoval` status objects.
6. Useful `vendor_specific` preservation from `MsgWrapper`/`MsgProcess`, but with Core-normalized names instead of Windows Hook payloads.
7. `ChatUpdates` as the mechanism to notify ETM of changed chat metadata.

### Behaviour retained from old EWS

1. `get_chats()` must return the actual linkable chat set; an empty implementation is not compatible with `/link`.
2. Private/group aliases and group-member enrolment should be reflected in EFB chat objects.
3. Text/media/file messages should carry stable message IDs so ETM can persist target/reply and recall mappings.
4. Reply targets should remain `Message.target` objects when received from ETM.
5. Unsupported message types should degrade into visible `MsgType.Unsupported`/text instead of crashing polling.

## 4. Code that must be replaced or materially modified

| Upstream symbol | Reason | C replacement |
|---|---|---|
| `ComWeChatChannel.bot = WeChatRobot()` | Windows/ComWechat backend | Core HTTP client |
| ComWechat callback decorators | Hook callback model | Durable `/v1/events/poll` loop |
| `ChatMgr.slave_channel` | Global mutable class state | Instance-bound `ChatMgr(channel)` |
| class-level `friends/groups/contacts/group_members` | Cross-instance/account leakage | Per-channel instance cache keyed by account-aware UID |
| `TTLCache` message dedupe | Not durable/reliable delivery | persisted Core event cursor and stable message/event IDs |
| `file_msg` / `delete_file` globals | File-system polling is backend-specific and non-durable | Core `media_id`, `/v1/media/{id}`, `media.ready` |
| local Hook file paths | Windows-only | HTTP media bytes / inline send base64 |
| `send_text` quote XML construction | Backend-specific XML | `target_message_id` in Core send contract |
| `get_chats() -> []` | Breaks ETM `/link` | account-aware Core chat enumeration |
| `get_chat` using `@chatroom` parsing | Core IDs are opaque | reversible channel-owned account/chat UID encoding |
| ComWechat login extras | Runtime/Core owns login lifecycle | Core account status surfaced read-only |

## 5. New functionality required because it does not exist upstream

1. `CoreClient` implementing Core Interface Contract V1, including structured errors and contract-version validation.
2. Reversible account-aware EFB chat/member UID codec. Core IDs are opaque, so the codec must not split on assumed delimiters.
3. Multi-account `get_chats` and `get_chat` over one slave instance.
4. Durable event cursor persistence for `/v1/events/poll` plus event acknowledgement.
5. Normalized Core message -> EFB message conversion.
6. Core media download -> seekable EFB file object.
7. EFB text/image/file dispatch -> `/v1/send/text|image|file` with idempotency keys.
8. Stable target translation: EFB target UID -> Core `target_message_id`.
9. Account-aware chat cache and dynamic group-member enrolment from normalized message authors.
10. Durable `send_id` <-> `echo_message_id` reconciliation so Kettly's already-persisted slave UID remains stable across `send.updated`, reply, recall and process restart; known EFB-originated outgoing echoes are absorbed instead of being posted to Telegram twice.

These are new because none of the three upstreams contains the frozen `wechat-core` HTTP contract.

## 6. Explicitly not reused

- `python-comwechatrobot-http`, `WeChatRobot`, Hook opcodes and Windows paths: wrong backend and operating-system boundary.
- old EWS `wxpy`, `itchat`, Web WeChat authentication and session handling: obsolete backend; behaviour reference only.
- global class caches in ComWechat: unsafe for multiple accounts/instances.
- ComWechat `TTLCache` as delivery reliability: the Core contract is durable and cursor-based.
- ComWechat global pending-file dictionary and file-system watcher: Core exposes media readiness and download by `media_id`.
- ComWechat quote XML (`QUOTE_MESSAGE`): Core V1 accepts `target_message_id` directly.
- PyPI-only Telegram Master as final compatibility proof: taskbook requires the local Kettly fork editable.

## 7. Test entry points and executed result

1. Unit tests for UID round-trip, chat construction, normalized message conversion and Core request payloads.
2. Mock Core integration test covering:
   - `/health` contract version
   - multi-account `get_chats`
   - incoming text/image conversion
   - text/image/file send payloads
   - reply `target_message_id`
   - event cursor/ack handling
   - recall -> `MessageRemoval`
   - `send_id` -> `echo_message_id` reconciliation
   - echo duplicate suppression while preserving native WeChat self-messages
   - persisted echo mapping after channel restart
3. Editable Kettly ETM proof:
   - create a local venv
   - `pip install -e ../../upstream/efb-telegram-master-kettly`
   - `pip install -e .`
   - verify `efb_telegram_master.__file__` resolves under `upstream/efb-telegram-master-kettly`
4. ETM compatibility smoke tests using the real editable Kettly source without live Telegram credentials:
   - real `ChatObjectCacheManager` consumes this slave's multi-account `get_chats()` in the same cache path used by `/link`
   - real `MasterMessageProcessor.attach_target_message()` reconstructs a Kettly reply target which this slave translates to the final Core echo message ID

Executed on 2026-08-31:

- `python -m unittest discover -s tests -v`: **16 tests passed**.
- `python -m compileall -q efb_wechat_comwechat_slave tests`: passed.
- editable Linux slave package reports version `2.0.0a1` and EFB entry point `wechat.linux = efb_wechat_comwechat_slave:LinuxWeChatChannel`.
- `efb_telegram_master.__file__` resolves to `G:\LLM\WeChat_Hub\upstream\efb-telegram-master-kettly\efb_telegram_master\__init__.py`.

Mock Core and mocked Telegram transport are development proofs only; they are not claimed as real WeChat/Telegram end-to-end tests.

## 8. Risks

1. Core V1 chat listing has `member_count` but no full member-list endpoint. Group members can be enrolled from observed message authors, but a complete cold-start member list cannot be reconstructed by C alone.
2. Core V1 has no chat-picture endpoint, so `get_chat_picture`/member picture cannot be implemented faithfully yet.
3. Core V1 has no outgoing recall endpoint. Incoming recall is supported; master-initiated recall must report `EFBOperationNotSupported` until the contract gains an operation.
4. `message.updated` behaviour depends on ETM edit support and should use the same stable message UID.
5. At-least-once delivery can replay after a crash boundary; stable message IDs and persisted cursors reduce duplicate effects but do not manufacture exactly-once semantics.

## 9. Real modification location

Only the C checkout is implementation-owned:

`work/efb-linux-wechat-slave`

Read-only compatibility sources remain under `upstream/`. Shared Core data stores are not accessed.

## 10. Source-utilization requirement

This work package is intentionally a derivative of ComWechat's EFB adapter structure rather than a blank slave. The implementation retains its Channel/Chat/message-dispatch architecture while replacing the backend-facing layer with the frozen Core HTTP contract and adopting old EWS/Kettly-compatible chat/message semantics.
