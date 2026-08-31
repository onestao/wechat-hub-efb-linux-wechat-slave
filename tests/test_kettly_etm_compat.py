from __future__ import annotations

import importlib.util
import logging
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import efb_telegram_master
from ehforwarderbot import Message, MsgType, coordinator

from efb_telegram_master.chat_object_cache import ChatObjectCacheManager
from efb_telegram_master.master_message import MasterMessageProcessor
from efb_telegram_master import utils as etm_utils
from efb_wechat_comwechat_slave.ComWechat import LinuxWeChatChannel
from efb_wechat_comwechat_slave.Core import CoreClient
from efb_wechat_comwechat_slave.UID import decode_chat_uid


def load_mock_core_module():
    path = Path(__file__).resolve().parents[3] / "stack" / "mock-core" / "app.py"
    spec = importlib.util.spec_from_file_location("wechat_hub_mock_core_for_kettly", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load Mock Core: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


MOCK_CORE = load_mock_core_module()


class KettlyETMCompatibilityTest(unittest.TestCase):
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
                "startup_healthcheck": True,
                "account_ids": [],
            },
            data_path=Path(self.tempdir.name),
        )

    def tearDown(self):
        self.channel.stop_polling()
        self.tempdir.cleanup()

    def test_etm_is_loaded_from_locked_editable_source(self):
        etm_path = Path(efb_telegram_master.__file__).resolve()
        expected_root = Path(__file__).resolve().parents[3] / "upstream" / "efb-telegram-master-kettly"
        self.assertTrue(etm_path.is_relative_to(expected_root.resolve()), (etm_path, expected_root))

    def test_real_kettly_chat_cache_consumes_get_chats_used_by_link(self):
        # ChatObjectCacheManager is the real Kettly component that /link reads.
        # Its constructor calls every slave's get_chats() and converts/enrols
        # those EFB chats into ETM chat objects.
        fake_telegram_channel = SimpleNamespace(db=object())
        with mock.patch.dict(coordinator.slaves, {self.channel.channel_id: self.channel}, clear=True):
            manager = ChatObjectCacheManager(fake_telegram_channel)

        chats = list(manager.all_chats)
        self.assertEqual(3, len(chats))
        self.assertEqual(3, len({str(chat.uid) for chat in chats}))
        self.assertEqual(
            {
                ("account-alpha", "alpha-private-1"),
                ("account-alpha", "alpha-group-1@chatroom"),
                ("account-beta", "beta-private-1"),
            },
            {decode_chat_uid(str(chat.uid)) for chat in chats},
        )
        self.assertTrue(all(str(chat.module_id) == str(self.channel.channel_id) for chat in chats))

    def test_real_kettly_attach_target_round_trips_through_core_echo_mapping(self):
        chat = next(
            chat
            for chat in self.channel.get_chats()
            if decode_chat_uid(str(chat.uid)) == ("account-alpha", "alpha-private-1")
        )

        original = self.channel.send_message(Message(chat=chat, type=MsgType.Text, text="original"))
        send_id = str(original.uid)
        self.channel._handle_send_update(
            {"send_id": send_id, "echo_message_id": "alpha-kettly-target-echo", "status": "sent"}
        )

        target_log = SimpleNamespace(
            slave_origin_uid=etm_utils.chat_id_to_str(chat=chat),
            build_etm_msg=lambda chat_manager, recur=False: Message(
                chat=chat,
                uid=send_id,
                type=MsgType.Text,
                text="original",
            ),
        )
        fake_db = SimpleNamespace(get_msg_log=lambda **kwargs: target_log)
        processor = MasterMessageProcessor.__new__(MasterMessageProcessor)
        processor.db = fake_db
        processor.chat_manager = None
        processor.logger = logging.getLogger("test.kettly.target")

        telegram_reply = SimpleNamespace(chat=SimpleNamespace(id=12345), message_id=67890)
        telegram_message = SimpleNamespace(reply_to_message=telegram_reply, message_id=67891)
        reply = Message(chat=chat, type=MsgType.Text, text="reply through real Kettly target")

        attached = processor.attach_target_message(telegram_message, reply, self.channel.channel_id)
        self.assertIsNotNone(attached.target)
        self.assertEqual(send_id, str(attached.target.uid))
        self.assertEqual(str(chat.uid), str(attached.target.chat.uid))

        self.channel.send_message(attached)
        self.assertEqual(
            "alpha-kettly-target-echo",
            self.state.sends[-1]["request"]["target_message_id"],
        )


if __name__ == "__main__":
    unittest.main()
