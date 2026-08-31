import unittest

from efb_wechat_comwechat_slave.UID import (
    InvalidUID,
    decode_chat_uid,
    decode_member_uid,
    encode_chat_uid,
    encode_member_uid,
)


class UIDCodecTest(unittest.TestCase):
    def test_chat_uid_round_trip_for_opaque_ids(self):
        account_id = "acct/alpha:测试"
        chat_id = "room.with/slashes?and=delimiters@chatroom"
        uid = encode_chat_uid(account_id, chat_id)
        self.assertEqual((account_id, chat_id), decode_chat_uid(uid))

    def test_same_chat_id_different_accounts_do_not_collide(self):
        first = encode_chat_uid("account-a", "same-chat")
        second = encode_chat_uid("account-b", "same-chat")
        self.assertNotEqual(first, second)

    def test_member_uid_round_trip(self):
        uid = encode_member_uid("account-a", "member/1")
        self.assertEqual(("account-a", "member/1"), decode_member_uid(uid))

    def test_rejects_raw_wechat_id(self):
        with self.assertRaises(InvalidUID):
            decode_chat_uid("room@chatroom")


if __name__ == "__main__":
    unittest.main()
