from __future__ import annotations

import importlib.util
import io
import copy
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from ehforwarderbot import Message, MsgType, coordinator
from ehforwarderbot.exceptions import EFBChatNotFound, EFBOperationNotSupported
from ehforwarderbot.status import MessageRemoval

from efb_wechat_comwechat_slave.ComWechat import LinuxWeChatChannel
from efb_wechat_comwechat_slave.Core import CoreClient
from efb_wechat_comwechat_slave.UID import decode_chat_uid


def load_mock_core_module():
    path = Path(__file__).resolve().parents[3] / "stack" / "mock-core" / "app.py"
    spec = importlib.util.spec_from_file_location("wechat_hub_mock_core_for_c", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load Mock Core: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


MOCK_CORE = load_mock_core_module()


class MockCoreChannelIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.state = MOCK_CORE.MockCoreState()
        cls.server = MOCK_CORE.create_server("127.0.0.1", 0, cls.state)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.channel = LinuxWeChatChannel(
            core_client=CoreClient(self.base_url, timeout=2),
            config={
                "core": {
                    "base_url": self.base_url,
                    "timeout": 2,
                    "poll_timeout": 0,
                    "verify_tls": True,
                },
                "poll_interval": 0.01,
                "event_limit": 50,
                "startup_healthcheck": True,
                "consumer_id": "test-c",
                "account_ids": [],
            },
            data_path=Path(self.tempdir.name),
        )

    def tearDown(self):
        self.channel.stop_polling()
        self.tempdir.cleanup()

    def _chat(self, account_id, chat_id):
        for chat in self.channel.get_chats():
            if decode_chat_uid(str(chat.uid)) == (account_id, chat_id):
                return chat
        self.fail(f"chat not found: {account_id}/{chat_id}")

    def test_get_chats_exposes_all_accounts_with_unique_uids(self):
        chats = list(self.channel.get_chats())
        self.assertEqual(3, len(chats))
        decoded = {decode_chat_uid(str(chat.uid)) for chat in chats}
        self.assertEqual(
            {
                ("account-alpha", "alpha-private-1"),
                ("account-alpha", "alpha-group-1@chatroom"),
                ("account-beta", "beta-private-1"),
            },
            decoded,
        )
        self.assertEqual(len(chats), len({str(chat.uid) for chat in chats}))

    def test_get_chat_rejects_raw_wechat_id(self):
        with self.assertRaises(EFBChatNotFound):
            self.channel.get_chat("alpha-group-1@chatroom")

    def test_text_send_preserves_reply_target_for_core(self):
        chat = self._chat("account-alpha", "alpha-private-1")
        target = Message(chat=chat, uid="alpha-msg-1", type=MsgType.Text, text="old")
        outgoing = Message(chat=chat, type=MsgType.Text, text="reply", target=target)
        sent = self.channel.send_message(outgoing)
        request = self.state.sends[-1]["request"]
        self.assertEqual("account-alpha", request["account_id"])
        self.assertEqual("alpha-private-1", request["chat_id"])
        self.assertEqual("alpha-msg-1", request["target_message_id"])
        self.assertEqual("reply", request["text"])
        self.assertTrue(sent.uid)

    def test_sender_capability_falls_back_from_native_reply(self):
        chat = self._chat("account-alpha", "alpha-private-1")
        self.channel.sender_capabilities["native_reply"] = False
        target = Message(chat=chat, uid="alpha-msg-1", type=MsgType.Text, text="old text")
        outgoing = Message(chat=chat, type=MsgType.Text, text="reply", target=target)
        self.channel.send_message(outgoing)
        request = self.state.sends[-1]["request"]
        self.assertNotIn("target_message_id", request)
        self.assertIn("old text", request["text"])
        self.assertIn("reply", request["text"])

    def test_sender_capability_rejects_unexecutable_file_before_core_queue(self):
        chat = self._chat("account-alpha", "alpha-private-1")
        self.channel.sender_capabilities["file"] = False
        before = len(self.state.sends)
        attachment = Message(
            chat=chat,
            type=MsgType.File,
            file=io.BytesIO(b"hello-file"),
            filename="hello.txt",
            mime="text/plain",
        )
        with self.assertRaises(EFBOperationNotSupported):
            self.channel.send_message(attachment)
        self.assertEqual(before, len(self.state.sends))

    def test_file_capability_is_account_scoped_for_mixed_runtime_providers(self):
        alpha = self.state.account("account-alpha")
        beta = self.state.account("account-beta")
        original_alpha = copy.deepcopy(alpha["runtime"])
        original_beta = copy.deepcopy(beta["runtime"])
        try:
            alpha["runtime"].update(
                {
                    "runtime_provider": "agent_wechat",
                    "sender_capabilities": {
                        "text": True,
                        "image": True,
                        "file": True,
                        "native_reply": False,
                        "media_caption": False,
                        "max_mentions": 0,
                        "echo_confirmation": False,
                        "verified_chat_target": True,
                        "driver": "agent_wechat",
                    },
                }
            )
            beta["runtime"].update(
                {
                    "runtime_provider": "legacy",
                    "sender_capabilities": {
                        "text": False,
                        "image": False,
                        "file": False,
                        "native_reply": False,
                        "media_caption": False,
                        "max_mentions": 0,
                        "echo_confirmation": False,
                        "verified_chat_target": False,
                        "driver": "legacy",
                    },
                }
            )
            self.channel.account_sender_capabilities.clear()
            alpha_chat = self._chat("account-alpha", "alpha-private-1")
            beta_chat = self._chat("account-beta", "beta-private-1")

            alpha_file = Message(
                chat=alpha_chat,
                type=MsgType.File,
                file=io.BytesIO(b"agent-file"),
                filename="agent.txt",
                mime="text/plain",
            )
            self.channel.send_message(alpha_file)
            self.assertEqual("account-alpha", self.state.sends[-1]["request"]["account_id"])

            before = len(self.state.sends)
            beta_file = Message(
                chat=beta_chat,
                type=MsgType.File,
                file=io.BytesIO(b"legacy-file"),
                filename="legacy.txt",
                mime="text/plain",
            )
            with self.assertRaises(EFBOperationNotSupported):
                self.channel.send_message(beta_file)
            self.assertEqual(before, len(self.state.sends))
        finally:
            alpha["runtime"] = original_alpha
            beta["runtime"] = original_beta
            self.channel.account_sender_capabilities.clear()

    def test_send_updated_reconciles_echo_reply_recall_and_restart(self):
        chat = self._chat("account-alpha", "alpha-private-1")
        outgoing = Message(chat=chat, type=MsgType.Text, text="outgoing")
        sent = self.channel.send_message(outgoing)
        send_id = str(sent.uid)
        self.assertTrue(send_id.startswith("send-"))

        with self.assertLogs(self.channel.logger, level="INFO") as submitted_logs:
            self.channel._handle_send_update(
                {
                    "send_id": send_id,
                    "status": "submitted",
                    "delivery_certainty": "pending_confirmation",
                }
            )
        self.assertTrue(any("已提交、等待微信确认" in line for line in submitted_logs.output))

        with self.assertLogs(self.channel.logger, level="INFO") as sent_logs:
            self.channel._handle_send_update(
                {"send_id": send_id, "echo_message_id": "alpha-outgoing-echo-1", "status": "sent"}
            )
        self.assertTrue(any("已由微信确认" in line for line in sent_logs.output))
        self.assertEqual("alpha-outgoing-echo-1", self.channel.echo_store.linked_echo(send_id))

        reply = Message(
            chat=chat,
            type=MsgType.Text,
            text="reply after echo",
            target=Message(chat=chat, uid=send_id, type=MsgType.Text, text="outgoing"),
        )
        self.channel.send_message(reply)
        self.assertEqual("alpha-outgoing-echo-1", self.state.sends[-1]["request"]["target_message_id"])

        delivered = []
        self.channel._deliver_message = lambda message: delivered.append(message)
        self.channel._handle_event(
            {
                "event_type": "message.created",
                "account_id": "account-alpha",
                "payload": {
                    "message": {
                        "message_id": "alpha-outgoing-echo-1",
                        "account_id": "account-alpha",
                        "chat_id": "alpha-private-1",
                        "direction": "outgoing",
                        "author": {
                            "member_id": "alpha-self",
                            "display_name": "Alpha User",
                            "is_self": True,
                        },
                        "type": "text",
                        "text": "outgoing",
                        "created_at": "2026-08-31T00:00:00Z",
                    }
                },
            }
        )
        # This is the Core echo of a Telegram-originated send. Kettly already
        # logged the Telegram message using send_id, so the echo is reconciled
        # but not delivered as a duplicate Telegram post.
        self.assertEqual([], delivered)

        # A self-message sent directly from native WeChat has no send mapping
        # and must still flow to Kettly as a normal slave-side self message.
        self.channel._handle_event(
            {
                "event_type": "message.created",
                "account_id": "account-alpha",
                "payload": {
                    "message": {
                        "message_id": "alpha-native-self-1",
                        "account_id": "account-alpha",
                        "chat_id": "alpha-private-1",
                        "direction": "outgoing",
                        "author": {
                            "member_id": "alpha-self",
                            "display_name": "Alpha User",
                            "is_self": True,
                        },
                        "type": "text",
                        "text": "native wechat send",
                        "created_at": "2026-08-31T00:00:01Z",
                    }
                },
            }
        )
        self.assertEqual("alpha-native-self-1", str(delivered[0].uid))

        seen = []
        with mock.patch.object(coordinator, "master", self.channel, create=True), mock.patch.object(
            coordinator, "send_status", side_effect=lambda status: seen.append(status)
        ):
            self.channel._emit_removal(
                "account-alpha",
                {"chat_id": "alpha-private-1", "message_id": "alpha-outgoing-echo-1"},
            )
        self.assertEqual(send_id, str(seen[0].message.uid))

        # The alias is adapter-owned durable state, so ETM's stored send UID is
        # still resolvable after the EFB process restarts.
        restarted = LinuxWeChatChannel(
            core_client=CoreClient(self.base_url, timeout=2),
            config=self.channel.config,
            data_path=Path(self.tempdir.name),
        )
        self.assertEqual("alpha-outgoing-echo-1", restarted.echo_store.core_message_id(send_id))
        restarted.stop_polling()

    def test_cross_chat_reply_uses_visible_quote_fallback(self):
        alpha = self._chat("account-alpha", "alpha-private-1")
        beta = self._chat("account-beta", "beta-private-1")
        target = Message(chat=beta, uid="beta-msg-1", type=MsgType.Text, text="from beta")
        outgoing = Message(chat=alpha, type=MsgType.Text, text="reply", target=target)
        self.channel.send_message(outgoing)
        request = self.state.sends[-1]["request"]
        self.assertNotIn("target_message_id", request)
        self.assertIn("from beta", request["text"])
        self.assertIn("reply", request["text"])

    def test_image_and_file_sends_use_core_media_operations(self):
        chat = self._chat("account-alpha", "alpha-private-1")
        image = Message(
            chat=chat,
            type=MsgType.Image,
            file=io.BytesIO(MOCK_CORE.SAMPLE_PNG),
            filename="test.png",
            mime="image/png",
        )
        self.channel.send_message(image)
        self.assertEqual("image", self.state.sends[-1]["receipt"]["kind"])
        self.assertTrue(self.state.sends[-1]["request"]["content_base64"])

        attachment = Message(
            chat=chat,
            type=MsgType.File,
            file=io.BytesIO(b"hello-file"),
            filename="hello.txt",
            mime="text/plain",
        )
        self.channel.send_message(attachment)
        self.assertEqual("file", self.state.sends[-1]["receipt"]["kind"])
        self.assertEqual("hello.txt", self.state.sends[-1]["request"]["filename"])

    def test_poll_once_converts_text_and_image_and_persists_cursor(self):
        delivered = []

        def capture(message):
            delivered.append(message)

        self.channel._deliver_message = capture  # deterministic local delivery boundary
        processed = self.channel.poll_once()
        self.assertEqual(3, processed)
        self.assertEqual("3", self.channel.cursor_store.load())
        self.assertEqual([MsgType.Text, MsgType.Image], [message.type for message in delivered])
        self.assertEqual("alpha-msg-1", str(delivered[0].uid))
        self.assertEqual("Hello from Alpha", delivered[0].text)
        self.assertEqual("beta-msg-1", str(delivered[1].uid))
        self.assertEqual("image/png", delivered[1].mime)
        self.assertEqual(MOCK_CORE.SAMPLE_PNG, delivered[1].file.read())
        delivered[1].file.close()

        acked = self.state.acked[self.channel.consumer_id]
        self.assertTrue({"event-0001", "event-0002", "event-0003"}.issubset(acked))

        # Durable cursor prevents replay on the next page request.
        self.assertEqual(0, self.channel.poll_once())

    def test_incoming_recall_maps_to_message_removal(self):
        seen = []
        fake_master = self.channel
        with mock.patch.object(coordinator, "master", fake_master, create=True), mock.patch.object(
            coordinator, "send_status", side_effect=lambda status: seen.append(status)
        ):
            self.channel._emit_removal(
                "account-alpha",
                {"chat_id": "alpha-private-1", "message_id": "alpha-msg-1"},
            )
        self.assertEqual(1, len(seen))
        self.assertIsInstance(seen[0], MessageRemoval)
        self.assertEqual("alpha-msg-1", str(seen[0].message.uid))

    def test_outgoing_recall_is_explicitly_unsupported_by_core_v1(self):
        chat = self._chat("account-alpha", "alpha-private-1")
        status = MessageRemoval(
            source_channel=self.channel,
            destination_channel=self.channel,
            message=Message(chat=chat, uid="alpha-msg-1"),
        )
        with self.assertRaises(EFBOperationNotSupported):
            self.channel.send_status(status)


if __name__ == "__main__":
    unittest.main()
