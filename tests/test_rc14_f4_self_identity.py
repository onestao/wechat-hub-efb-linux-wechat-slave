from __future__ import annotations

import shutil
import sys
import unittest
import uuid
from pathlib import Path

TESTS = Path(__file__).resolve().parent
if str(TESTS) not in sys.path:
    sys.path.insert(0, str(TESTS))

from stub_ehforwarderbot import GroupChat, PrivateChat, install_stubs

install_stubs()

from efb_wechat_comwechat_slave.ChatMgr import ChatMgr
from efb_wechat_comwechat_slave.CoreMessage import CoreMessageBuilder
from efb_wechat_comwechat_slave.MessageMapping import MessageMappingStore


class UnusedCore:
    pass


class F4SelfIdentityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.data_path = (
            Path(__file__).resolve().parents[1]
            / ".tmp"
            / f"rc14-f4-{uuid.uuid4().hex}"
        )
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.channel = object()
        self.chats = ChatMgr(self.channel)
        self.builder = CoreMessageBuilder(UnusedCore(), self.chats)

    def tearDown(self) -> None:
        shutil.rmtree(self.data_path, ignore_errors=True)

    def _private(self) -> PrivateChat:
        return self.chats.build_core_chat(
            {
                "account_id": "account-1",
                "chat_id": "peer-1",
                "type": "private",
                "display_name": "Peer",
            },
            "Account Self",
        )

    def _group(self) -> GroupChat:
        return self.chats.build_core_chat(
            {
                "account_id": "account-1",
                "chat_id": "group-1@chatroom",
                "type": "group",
                "display_name": "Group",
            },
            "Account Self",
        )

    @staticmethod
    def _message(message_id: str, **overrides):
        message = {
            "account_id": "account-1",
            "chat_id": "peer-1",
            "message_id": message_id,
            "type": "text",
            "text": "hello",
            "direction": "incoming",
            "sender_id": "peer-1",
            "sender_name": "Peer",
            "is_self": False,
            # Deliberately misleading legacy author data: top-level actual
            # sender fields must win.
            "author": {
                "member_id": "wrong-id",
                "display_name": "Wrong Name",
                "is_self": True,
            },
        }
        message.update(overrides)
        return message

    def test_f4_1_private_self_message_uses_chat_self(self) -> None:
        chat = self._private()
        result = self.builder.build(
            self._message(
                "private-self",
                direction="outgoing",
                sender_id="wxid-self",
                sender_name="Me",
                is_self=True,
            ),
            chat,
        )

        self.assertIs(chat.self, result.author)
        self.assertEqual("Me", result.author.name)
        self.assertEqual("wxid-self", result.author.vendor_specific["core"]["member_id"])

    def test_f4_2_private_peer_message_uses_actual_peer(self) -> None:
        chat = self._private()
        result = self.builder.build(self._message("private-peer"), chat)

        self.assertIs(chat.other, result.author)
        self.assertIsNot(chat.self, result.author)
        self.assertEqual("peer-1", result.author.vendor_specific["core"]["member_id"])

    def test_f4_3_group_self_message_uses_chat_self(self) -> None:
        chat = self._group()
        result = self.builder.build(
            self._message(
                "group-self",
                chat_id="group-1@chatroom",
                direction="outgoing",
                sender_id="wxid-self",
                sender_name="Me",
                is_self=True,
            ),
            chat,
        )

        self.assertIs(chat.self, result.author)
        self.assertEqual("wxid-self", result.author.vendor_specific["core"]["member_id"])

    def test_f4_4_group_member_message_uses_actual_member(self) -> None:
        chat = self._group()
        result = self.builder.build(
            self._message(
                "group-peer",
                chat_id="group-1@chatroom",
                sender_id="member-a",
                sender_name="Member A",
            ),
            chat,
        )

        self.assertIsNot(chat.self, result.author)
        self.assertEqual("member-a", result.author.vendor_specific["core"]["member_id"])
        self.assertEqual("Member A", result.author.name)

    def test_f4_5_same_display_name_members_do_not_collide(self) -> None:
        chat = self._group()
        first = self.builder.build(
            self._message(
                "same-name-1",
                chat_id="group-1@chatroom",
                sender_id="member-a",
                sender_name="Same Name",
            ),
            chat,
        )
        second = self.builder.build(
            self._message(
                "same-name-2",
                chat_id="group-1@chatroom",
                sender_id="member-b",
                sender_name="Same Name",
            ),
            chat,
        )

        self.assertEqual(first.author.name, second.author.name)
        self.assertNotEqual(str(first.author.uid), str(second.author.uid))

    def test_f4_6_identity_and_mapping_are_stable_after_restart(self) -> None:
        first_chat = self._group()
        message = self._message(
            "restart-message",
            chat_id="group-1@chatroom",
            sender_id="member-stable",
            sender_name="Stable Name",
        )
        first = self.builder.build(message, first_chat)
        mapping = MessageMappingStore(self.data_path / "mapping.sqlite3")
        mapping.record(
            "consumer-1",
            "account-1",
            "group-1@chatroom",
            str(first.uid),
            core_message_id="restart-message",
            direction="incoming",
            sender_identity="member-stable",
            core_cursor="901",
        )

        restarted_chats = ChatMgr(object())
        restarted_builder = CoreMessageBuilder(UnusedCore(), restarted_chats)
        restarted_chat = restarted_chats.build_core_chat(
            {
                "account_id": "account-1",
                "chat_id": "group-1@chatroom",
                "type": "group",
                "display_name": "Group",
            },
            "Account Self",
        )
        second = restarted_builder.build(message, restarted_chat)
        reopened = MessageMappingStore(self.data_path / "mapping.sqlite3")
        row = reopened.resolve_target(
            "consumer-1",
            "account-1",
            "group-1@chatroom",
            "restart-message",
        )

        self.assertEqual(str(first.author.uid), str(second.author.uid))
        self.assertEqual("member-stable", row["sender_identity"])
        self.assertEqual("901", row["core_cursor"])


if __name__ == "__main__":
    unittest.main()
