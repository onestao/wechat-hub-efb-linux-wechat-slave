# EFB behaviour compatibility: old EWS -> ComWechat -> Linux Slave

Reference commits are recorded in `../SOURCE_AUDIT_C.md`.

| Behaviour | old EWS | ComWechat | Linux Slave target |
|---|---|---|---|
| `get_chats` | Enumerates wxpy chats and cached system chats | Currently returns `[]` despite maintaining friend/group caches | Enumerate every Core account and its chats; return account-scoped EFB chats for ETM `/link` |
| `get_chat` | Resolves one stable PUID and refreshes cache | Looks through local friend/group arrays and infers group by `@chatroom` | Decode the slave-owned account/chat UID, then resolve via Core/cache without parsing opaque Core IDs |
| `GroupChat` | Includes aliases, members, notification/vendor flags | Uses `ChatMgr.build_efb_chat_as_group` and adds observed members | Keep ComWechat construction style; cache per instance, expose Core vendor metadata, enrol observed authors |
| `PrivateChat` | Friend/MP/self variants with alias | Uses `ChatMgr.build_efb_chat_as_private` | Preserve name/alias and account metadata; support same Core chat ID under different accounts without collisions |
| member | Cached from wxpy group list and added on observation | `build_efb_chat_as_member` plus alias DB | Use normalized author fields; encode account-aware member UID and add/update member on observation |
| `/link` | Works because `get_chats` returns real chats | Effectively broken by empty `get_chats()` | Real Kettly ETM must see all chats through its normal ChatObjectCache flow; stable account-aware UIDs prevent topic/link collisions |
| private text | Stable EFB message + author/chat | `MsgProcess` converts callbacks | `message.created` text -> `MsgType.Text`; stable Core message ID retained |
| group text | Includes member author/substitution semantics | Creates group member and handles `@me` hints | Author is a group member; normalized substitutions are retained where representable |
| image | Downloads image into EFB file | Decodes/waits for Hook file paths | Download `/v1/media/{id}` into seekable EFB file; preserve filename/MIME |
| file | Downloads and emits attachment | Global `file_msg` waits for local file | Download Core media directly; no global pending-file polling |
| reply | Old EWS formats a quote fallback on send; ETM records target | ComWechat builds native WeChat quote XML | Pass target's stable Core message ID as `target_message_id`; if target has no usable ID, degrade to normal send rather than inventing one |
| outgoing image/file | wxpy send primitives | `SendImage`/`SendFile` Hook API | `/v1/send/image` and `/v1/send/file`, inline base64 from EFB file, idempotency key |
| Core send echo | Web WeChat backend returns/observes platform IDs in one backend | Hook callback and local send state are backend-specific | Persist `send_id` -> `echo_message_id`; retain Kettly's original slave UID, suppress only known EFB-originated outgoing echo duplicates, but forward native WeChat self-messages |
| recall from WeChat | Converts recall to `MessageRemoval` | `on_revoked_msg` -> `MessageRemoval` | `message.removed` -> `MessageRemoval` using account-aware chat UID + stable Core message ID |
| recall from Telegram | Old EWS can call Web WeChat recall | Not meaningfully portable | Not supported by Core V1; raise `EFBOperationNotSupported` explicitly |
| chat updates | Sends `ChatUpdates` for observed changes | Sends `ChatUpdates` from contact refresh | `chat.updated` refreshes cache and emits `ChatUpdates` so Kettly ETM refreshes mappings |
| unsupported messages | Visible unsupported placeholder | Visible fallback string | Emit `MsgType.Unsupported` with useful text and normalized `vendor_specific` |
| `vendor_specific` | EWS contact/chat flags | `wx_xml`, `comwechat_info`, MP/share metadata | Preserve Core-provided `vendor_specific` under `core`, plus stable `account_id`, `chat_id`, `message_id`; do not expose Hook internals |

## Compatibility priorities

1. `/link` and Forum Topic identity must never collide across WeChat accounts.
2. ETM reply reconstruction must return a target whose UID is the original Core `message_id`.
3. Recall lookup must use the same stable chat/message pair that ETM stored when the message was first delivered.
4. Media objects must be normal EFB seekable files so Kettly ETM's existing upload handlers can consume them unchanged.
5. Unknown future Core event/message types must be logged and skipped/degraded, not terminate the polling loop.

## Known contract gaps

- Core V1 has no full group-member-list endpoint.
- Core V1 has no chat-avatar endpoint.
- Core V1 has no outgoing recall operation.

The Linux Slave must not bypass the HTTP contract to fill these gaps.
