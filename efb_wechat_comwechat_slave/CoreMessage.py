"""Normalized Core message -> EFB message conversion.

The shape follows ComWechat's MsgProcess/MsgWrapper split, but consumes the
frozen normalized Core schema instead of Hook callback dictionaries.
"""

from __future__ import annotations

import mimetypes
import tempfile
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple

from ehforwarderbot import Message, MsgType
from ehforwarderbot.chat import Chat, GroupChat
from ehforwarderbot.message import LinkAttribute, LocationAttribute, Substitutions
from ehforwarderbot.types import MessageID

from .ChatMgr import ChatMgr
from .Core import CoreClient


class CoreMessageBuilder:
    def __init__(self, core: CoreClient, chats: ChatMgr) -> None:
        self.core = core
        self.chats = chats

    def _media_file(
        self,
        account_id: str,
        media_id: str,
        filename: Optional[str],
        mime_type: Optional[str],
    ) -> Tuple[Any, str, str, Path]:
        media = self.core.get_media(account_id, media_id)
        final_name = filename or media.filename or media_id
        final_mime = mime_type or media.mime_type or mimetypes.guess_type(final_name)[0] or "application/octet-stream"
        suffix = Path(final_name).suffix
        handle = tempfile.NamedTemporaryFile(prefix="efb-core-", suffix=suffix)
        handle.write(media.content)
        handle.flush()
        handle.seek(0)
        return handle, final_name, final_mime, Path(handle.name)

    def _substitutions(self, message: Mapping[str, Any], chat: Chat, account_id: str) -> Optional[Substitutions]:
        raw = message.get("substitutions")
        if not isinstance(raw, list):
            return None
        result: Dict[Tuple[int, int], Any] = {}
        for item in raw:
            if not isinstance(item, dict):
                continue
            try:
                start = int(item.get("start"))
                end = int(item.get("end"))
            except (TypeError, ValueError):
                continue
            member_id = str(item.get("member_id") or "")
            if not member_id or end <= start:
                continue
            if isinstance(chat, GroupChat):
                member = self.chats.upsert_author(
                    chat,
                    account_id,
                    {
                        "member_id": member_id,
                        "display_name": item.get("display_name") or member_id,
                        "alias": item.get("alias"),
                        "is_self": bool(item.get("is_self")),
                    },
                )
                result[(start, end)] = member
            elif item.get("is_self") and chat.self:
                result[(start, end)] = chat.self
        return Substitutions(result) if result else None

    def build(self, message: Mapping[str, Any], chat: Chat) -> Message:
        account_id = str(message.get("account_id") or "")
        message_id = str(message.get("message_id") or "")
        author_data = message.get("author") if isinstance(message.get("author"), dict) else {}
        author = self.chats.upsert_author(chat, account_id, author_data)
        msg_type = str(message.get("type") or "unsupported")
        text = str(message.get("text") or "")
        vendor_specific = {
            "core": {
                "account_id": account_id,
                "chat_id": str(message.get("chat_id") or ""),
                "message_id": message_id,
                "direction": str(message.get("direction") or ""),
                "created_at": message.get("created_at"),
                "attributes": dict(message.get("attributes") or {}),
                "message": dict(message.get("vendor_specific") or {}),
            }
        }
        efb_msg = Message(
            chat=chat,
            author=author,
            uid=MessageID(message_id),
            text=text,
            vendor_specific=vendor_specific,
        )

        if msg_type == "text":
            efb_msg.type = MsgType.Text
        elif msg_type == "image":
            efb_msg.type = MsgType.Image
        elif msg_type == "sticker":
            efb_msg.type = MsgType.Sticker
        elif msg_type == "voice":
            efb_msg.type = MsgType.Voice
        elif msg_type == "video":
            efb_msg.type = MsgType.Video
        elif msg_type == "file":
            efb_msg.type = MsgType.File
        elif msg_type == "link":
            attrs = message.get("attributes") if isinstance(message.get("attributes"), dict) else {}
            url = str(attrs.get("url") or "")
            if url:
                efb_msg.type = MsgType.Link
                efb_msg.attributes = LinkAttribute(
                    title=str(attrs.get("title") or text or url),
                    description=str(attrs.get("description") or "") or None,
                    image=str(attrs.get("image") or "") or None,
                    url=url,
                )
            else:
                efb_msg.type = MsgType.Text
                if not efb_msg.text:
                    efb_msg.text = "[Link]"
        elif msg_type == "location":
            attrs = message.get("attributes") if isinstance(message.get("attributes"), dict) else {}
            try:
                latitude = float(attrs["latitude"])
                longitude = float(attrs["longitude"])
            except (KeyError, TypeError, ValueError):
                efb_msg.type = MsgType.Unsupported
                efb_msg.text = efb_msg.text or "[Location message without coordinates]"
            else:
                efb_msg.type = MsgType.Location
                efb_msg.attributes = LocationAttribute(latitude=latitude, longitude=longitude)
        elif msg_type in {"contact_card", "system"}:
            efb_msg.type = MsgType.Text
            efb_msg.text = efb_msg.text or f"[{msg_type.replace('_', ' ')}]"
        else:
            efb_msg.type = MsgType.Unsupported
            efb_msg.text = efb_msg.text or f"[Unsupported WeChat message: {msg_type}]"

        if efb_msg.type in {MsgType.Image, MsgType.Sticker, MsgType.Voice, MsgType.Video, MsgType.File}:
            media_id = str(message.get("media_id") or "")
            if not media_id:
                efb_msg.type = MsgType.Unsupported
                efb_msg.text = efb_msg.text or f"[{msg_type} media is not ready]"
            else:
                file_obj, filename, mime_type, path = self._media_file(
                    account_id,
                    media_id,
                    str(message.get("filename") or "") or None,
                    str(message.get("mime_type") or "") or None,
                )
                efb_msg.file = file_obj
                efb_msg.filename = filename
                efb_msg.mime = mime_type
                efb_msg.path = path

        substitutions = self._substitutions(message, chat, account_id)
        if substitutions:
            efb_msg.substitutions = substitutions

        target_message_id = str(message.get("target_message_id") or "")
        if target_message_id:
            # Kettly ETM resolves replies by target.uid + target.chat.  Full target
            # content/author is not required for its DB lookup.
            efb_msg.target = Message(chat=chat, uid=MessageID(target_message_id), type=MsgType.Text)

        return efb_msg
