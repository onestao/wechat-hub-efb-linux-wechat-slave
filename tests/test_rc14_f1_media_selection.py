from __future__ import annotations

import sys
import unittest
from pathlib import Path

TESTS = Path(__file__).resolve().parent
if str(TESTS) not in sys.path:
    sys.path.insert(0, str(TESTS))

from stub_ehforwarderbot import install_stubs

install_stubs()

from ehforwarderbot.channel import SlaveChannel
from ehforwarderbot.chat import PrivateChat

from efb_wechat_comwechat_slave.ChatMgr import ChatMgr
from efb_wechat_comwechat_slave.Core import CoreMedia
from efb_wechat_comwechat_slave.CoreMessage import (
    CoreMessageBuilder,
    MediaPendingError,
    MediaPermanentError,
)


class DummySlave(SlaveChannel):
    channel_name = "RC14 Test"
    channel_emoji = "T"
    channel_id = "rc14.test"

    def get_chat(self, chat_uid):
        return None

    def get_chat_picture(self, chat):
        raise NotImplementedError

    def get_chats(self):
        return []

    def poll(self):
        return None

    def send_message(self, message):
        return message

    def send_status(self, status):
        return None


class FakeCore:
    def __init__(self, media: CoreMedia) -> None:
        self.media = media
        self.calls = []

    def get_media(self, account_id: str, media_id: str) -> CoreMedia:
        self.calls.append((account_id, media_id))
        return self.media


class F1OriginalMediaSelectionTest(unittest.TestCase):
    def _builder(self, media: CoreMedia) -> tuple[CoreMessageBuilder, FakeCore, PrivateChat]:
        core = FakeCore(media)
        channel = DummySlave()
        chats = ChatMgr(channel)
        chat = chats.build_core_chat(
            {
                "account_id": "account-1",
                "chat_id": "peer-1",
                "type": "private",
                "display_name": "Peer",
            },
            "Self",
        )
        return CoreMessageBuilder(core, chats), core, chat

    @staticmethod
    def _message(**overrides):
        message = {
            "account_id": "account-1",
            "chat_id": "peer-1",
            "message_id": "message-1",
            "type": "image",
            "author": {"member_id": "peer-1", "display_name": "Peer", "is_self": False},
            "media_id": "original-1",
            "media_role": "original",
            "media_status": "ready",
            "filename": "original.png",
            "mime_type": "image/png",
        }
        message.update(overrides)
        return message

    def test_f1_1_original_is_selected_when_thumbnail_also_exists(self) -> None:
        builder, core, chat = self._builder(
            CoreMedia(b"ORIGINAL", "image/png", "original.png", "original-1", "original", "ready")
        )
        message = self._message(
            vendor_specific={"media": {"thumbnail_media_id": "thumb-1", "original_media_id": "original-1"}}
        )

        result = builder.build(message, chat)

        self.assertEqual([("account-1", "original-1")], core.calls)
        self.assertEqual(b"ORIGINAL", result.file.read())
        result.file.close()

    def test_f1_2_thumbnail_only_pending_is_not_sent_as_final(self) -> None:
        builder, core, chat = self._builder(
            CoreMedia(b"THUMB", "image/jpeg", "thumb.jpg", "thumb-1", "thumbnail", "ready")
        )

        with self.assertRaises(MediaPendingError):
            builder.build(
                self._message(media_status="original_pending", vendor_specific={"media": {"thumbnail_media_id": "thumb-1"}}),
                chat,
            )
        self.assertEqual([], core.calls)

    def test_f1_3_original_ready_is_downloaded_and_sent(self) -> None:
        builder, core, chat = self._builder(
            CoreMedia(b"FULL-RESOLUTION", "image/png", "original.png", "original-1", "original", "ready")
        )

        result = builder.build(self._message(), chat)

        self.assertEqual(b"FULL-RESOLUTION", result.file.read())
        self.assertEqual("original.png", result.filename)
        self.assertEqual("image/png", result.mime)
        result.file.close()

    def test_f1_4_decode_failure_never_falls_back_to_thumbnail(self) -> None:
        builder, core, chat = self._builder(
            CoreMedia(b"THUMB", "image/jpeg", "thumb.jpg", "thumb-1", "thumbnail", "ready")
        )

        with self.assertRaises(MediaPermanentError):
            builder.build(self._message(media_status="decode_failed"), chat)
        self.assertEqual([], core.calls)


if __name__ == "__main__":
    unittest.main()
