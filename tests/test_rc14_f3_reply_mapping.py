from __future__ import annotations

import shutil
import sys
import unittest
import uuid
from pathlib import Path

TESTS = Path(__file__).resolve().parent
if str(TESTS) not in sys.path:
    sys.path.insert(0, str(TESTS))

from stub_ehforwarderbot import Message, MsgType, install_stubs

install_stubs()

from ehforwarderbot.exceptions import EFBMessageError

from efb_wechat_comwechat_slave.ComWechat import LinuxWeChatChannel


class ReplyCore:
    def __init__(self) -> None:
        self.sends = []

    def send_text(self, payload, idempotency_key):
        self.sends.append(dict(payload))
        return {
            "send_id": f"send-{len(self.sends)}",
            "status": "submitted",
            "account_id": payload["account_id"],
            "chat_id": payload["chat_id"],
        }


class F3ReplyMappingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.data_path = (
            Path(__file__).resolve().parents[1]
            / ".tmp"
            / f"rc14-f3-{uuid.uuid4().hex}"
        )
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.core = ReplyCore()
        self.channel = self._channel()

    def tearDown(self) -> None:
        self.channel.stop_polling()
        shutil.rmtree(self.data_path, ignore_errors=True)

    def _channel(self) -> LinuxWeChatChannel:
        channel = LinuxWeChatChannel(
            core_client=self.core,
            config={
                "startup_healthcheck": False,
                "shutdown_install_deferred": False,
                "consumer_id": "rc14-f3",
                "account_ids": [],
                "core": {"poll_timeout": 0},
            },
            data_path=self.data_path,
        )
        channel.account_sender_capabilities["account-1"] = {
            "native_reply": True,
            "max_mentions": 0,
        }
        channel.account_sender_capabilities["account-2"] = {
            "native_reply": True,
            "max_mentions": 0,
        }
        return channel

    def _chat(self, account_id: str, chat_id: str):
        return self.channel.chat_mgr.build_core_chat(
            {
                "account_id": account_id,
                "chat_id": chat_id,
                "type": "private",
                "display_name": chat_id,
            },
            f"Self {account_id}",
        )

    def _map(self, chat, efb_uid: str, core_message_id: str) -> None:
        account_id = chat.vendor_specific["core"]["account_id"]
        chat_id = chat.vendor_specific["core"]["chat_id"]
        self.channel.message_mapping.record(
            self.channel.consumer_id,
            account_id,
            chat_id,
            efb_uid,
            core_message_id=core_message_id,
            direction="incoming",
            sender_identity="peer-1",
            core_cursor="101",
        )

    def test_f3_1_reply_to_text_uses_scoped_native_target_when_available(self) -> None:
        chat = self._chat("account-1", "chat-1")
        self._map(chat, "efb-text-1", "core-text-1")
        target = Message(chat=chat, uid="efb-text-1", type=MsgType.Text, text="original text")

        self.channel.send_message(
            Message(chat=chat, type=MsgType.Text, text="reply body", target=target)
        )

        self.assertEqual("core-text-1", self.core.sends[-1]["target_message_id"])
        self.assertEqual("reply body", self.core.sends[-1]["text"])

    def test_f3_2_reply_to_image_keeps_visible_semantics_without_native_reply(self) -> None:
        chat = self._chat("account-1", "chat-1")
        self.channel.account_sender_capabilities["account-1"]["native_reply"] = False
        self._map(chat, "efb-image-1", "core-image-1")
        target = Message(chat=chat, uid="efb-image-1", type=MsgType.Image, text="")

        self.channel.send_message(
            Message(chat=chat, type=MsgType.Text, text="reply to image", target=target)
        )

        request = self.core.sends[-1]
        self.assertNotIn("target_message_id", request)
        self.assertIn("Image", request["text"])
        self.assertIn("reply to image", request["text"])

    def test_f3_3_mapping_survives_restart(self) -> None:
        chat = self._chat("account-1", "chat-1")
        self._map(chat, "efb-before-restart", "core-before-restart")

        restarted = self._channel()
        self.channel = restarted
        restarted_chat = self._chat("account-1", "chat-1")
        target = Message(
            chat=restarted_chat,
            uid="efb-before-restart",
            type=MsgType.Text,
            text="persisted",
        )
        restarted.send_message(
            Message(chat=restarted_chat, type=MsgType.Text, text="after restart", target=target)
        )

        self.assertEqual("core-before-restart", self.core.sends[-1]["target_message_id"])

    def test_f3_4_missing_mapping_uses_deterministic_visible_fallback(self) -> None:
        chat = self._chat("account-1", "chat-1")
        target = Message(chat=chat, uid="unknown-uid", type=MsgType.Text, text="lost target")

        self.channel.send_message(
            Message(chat=chat, type=MsgType.Text, text="fallback reply", target=target)
        )

        request = self.core.sends[-1]
        self.assertNotIn("target_message_id", request)
        self.assertEqual("「lost target」\n---\nfallback reply", request["text"])

    def test_f3_5_wrong_account_or_chat_target_fails_closed(self) -> None:
        destination = self._chat("account-1", "chat-1")
        wrong_chat = self._chat("account-1", "chat-2")
        target = Message(chat=wrong_chat, uid="efb-other-chat", type=MsgType.Text, text="secret")
        before = len(self.core.sends)

        with self.assertRaises(EFBMessageError):
            self.channel.send_message(
                Message(chat=destination, type=MsgType.Text, text="must not send", target=target)
            )

        self.assertEqual(before, len(self.core.sends))


if __name__ == "__main__":
    unittest.main()
