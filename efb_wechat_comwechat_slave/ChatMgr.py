"""EFB chat construction adapted from ComWechat's ChatMgr.

The upstream static/global ``ChatMgr.slave_channel`` is deliberately replaced
with an instance-bound manager so multiple EFB instances and multiple WeChat
accounts cannot leak state into one another.
"""

from __future__ import annotations

import contextlib
from typing import Any, Dict, List, Mapping, Optional, Tuple

from ehforwarderbot.chat import Chat, ChatMember, GroupChat, PrivateChat, SystemChat, SystemChatMember
from ehforwarderbot.types import ChatID

from .UID import encode_chat_uid, encode_member_uid


class ChatMgr:
    def __init__(self, channel: Any) -> None:
        self.channel = channel
        self._by_uid: Dict[str, Chat] = {}
        self._by_core: Dict[Tuple[str, str], Chat] = {}
        self._account_names: Dict[str, str] = {}

    @staticmethod
    def _core_vendor(record: Mapping[str, Any], account_display_name: str) -> Dict[str, Any]:
        return {
            "core": {
                "account_id": str(record.get("account_id") or ""),
                "chat_id": str(record.get("chat_id") or ""),
                "account_display_name": account_display_name,
                "chat": dict(record.get("vendor_specific") or {}),
                "member_count": record.get("member_count"),
            }
        }

    def set_account_name(self, account_id: str, display_name: str) -> None:
        self._account_names[str(account_id)] = str(display_name)

    def account_name(self, account_id: str) -> str:
        return self._account_names.get(str(account_id), str(account_id))

    def build_core_chat(self, record: Mapping[str, Any], account_display_name: Optional[str] = None) -> Chat:
        account_id = str(record.get("account_id") or "")
        chat_id = str(record.get("chat_id") or "")
        if not account_id or not chat_id:
            raise ValueError("Core chat requires account_id and chat_id")
        account_display_name = account_display_name or self.account_name(account_id)
        self.set_account_name(account_id, account_display_name)
        uid = encode_chat_uid(account_id, chat_id)
        name = str(record.get("display_name") or chat_id)
        alias = record.get("alias")
        alias = str(alias) if alias not in (None, "") else None
        vendor = self._core_vendor(record, account_display_name)
        chat_type = str(record.get("type") or "private")

        cached = self._by_uid.get(uid)
        expected = {"group": GroupChat, "private": PrivateChat, "system": SystemChat}.get(chat_type, PrivateChat)
        if cached is not None and isinstance(cached, expected):
            cached.name = name
            cached.alias = alias
            cached.vendor_specific = vendor
            self._by_core[(account_id, chat_id)] = cached
            return cached

        if chat_type == "group":
            chat: Chat = GroupChat(channel=self.channel, uid=ChatID(uid), name=name, alias=alias, vendor_specific=vendor)
        elif chat_type == "system":
            chat = SystemChat(channel=self.channel, uid=ChatID(uid), name=name, alias=alias, vendor_specific=vendor)
        else:
            chat = PrivateChat(channel=self.channel, uid=ChatID(uid), name=name, alias=alias, vendor_specific=vendor)

        if chat.self:
            chat.self.name = account_display_name
            chat.self.vendor_specific = {"core": {"account_id": account_id, "is_self": True}}
        self._by_uid[uid] = chat
        self._by_core[(account_id, chat_id)] = chat
        return chat

    def get_by_uid(self, uid: str) -> Optional[Chat]:
        return self._by_uid.get(str(uid))

    def get_by_core(self, account_id: str, chat_id: str) -> Optional[Chat]:
        return self._by_core.get((str(account_id), str(chat_id)))

    def all_chats(self) -> List[Chat]:
        return list(self._by_uid.values())

    def upsert_author(self, chat: Chat, account_id: str, author: Mapping[str, Any]) -> ChatMember:
        member_id = str(author.get("member_id") or "")
        name = str(author.get("display_name") or member_id or "Unknown")
        alias = author.get("alias")
        alias = str(alias) if alias not in (None, "") else None
        is_self = bool(author.get("is_self"))
        vendor = {
            "core": {
                "account_id": str(account_id),
                "member_id": member_id,
                "is_self": is_self,
            }
        }

        if is_self and chat.self:
            chat.self.name = name
            chat.self.alias = alias
            chat.self.vendor_specific = vendor
            return chat.self

        encoded_uid = encode_member_uid(str(account_id), member_id or name)

        if isinstance(chat, PrivateChat):
            member = chat.other
            member.uid = ChatID(encoded_uid)
            member.name = name
            member.alias = alias
            member.vendor_specific = vendor
            return member

        if isinstance(chat, SystemChat):
            try:
                member = chat.get_member(SystemChatMember.SYSTEM_ID)
            except KeyError:
                member = chat.add_system_member()
            member.name = name
            member.alias = alias
            member.vendor_specific = vendor
            return member

        if isinstance(chat, GroupChat):
            with contextlib.suppress(KeyError):
                member = chat.get_member(encoded_uid)
                member.name = name
                member.alias = alias
                member.vendor_specific = vendor
                return member
            return chat.add_member(uid=ChatID(encoded_uid), name=name, alias=alias, vendor_specific=vendor)

        raise TypeError(f"Unsupported EFB chat type: {type(chat)!r}")

    def remove(self, account_id: str, chat_id: str) -> Optional[str]:
        chat = self._by_core.pop((str(account_id), str(chat_id)), None)
        if not chat:
            return None
        self._by_uid.pop(str(chat.uid), None)
        return str(chat.uid)
