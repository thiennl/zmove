"""Pure-function tests for paths.py: no I/O, no network, no servers."""
import unittest

from zmove.paths import (
    TYPE_INDEX,
    TYPE_MESSAGE,
    TYPE_MESSAGE_SECONDARY,
    Volume,
    group_db,
    locator_case_sql,
    mailbox_group_id,
    map_volumes,
    replace_host_in_transport,
)


def _vol(vid, vtype=TYPE_MESSAGE, fb=8, fgb=8, store=1):
    return Volume(id=vid, type=vtype, name=f"v{vid}", path=f"/store/{vid}",
                  file_bits=fb, file_group_bits=fgb, mailbox_bits=12,
                  mailbox_group_bits=8, store_type=store)


class GroupIdTest(unittest.TestCase):
    def test_boundaries(self):
        self.assertEqual(mailbox_group_id(1, 100), 1)
        self.assertEqual(mailbox_group_id(100, 100), 100)
        self.assertEqual(mailbox_group_id(101, 100), 1)
        self.assertEqual(mailbox_group_id(1, 1), 1)
        self.assertEqual(mailbox_group_id(10**9, 100), (10**9 - 1) % 100 + 1)

    def test_rejects_non_positive(self):
        with self.assertRaises(ValueError):
            mailbox_group_id(0, 100)
        with self.assertRaises(ValueError):
            mailbox_group_id(1, 0)

    def test_group_db(self):
        self.assertEqual(group_db(3), "mboxgroup3")


class MailboxDirTest(unittest.TestCase):
    def test_layout(self):
        v = _vol(1)
        # bucket = (5 >> 12) & 0xFF == 0
        self.assertEqual(v.mailbox_dir(5), "/store/1/0/5")
        self.assertEqual(v.mailbox_dir(5, "srv-id"), "/store/1/srv-id/0/5")

    def test_trailing_slash_root(self):
        v = Volume(id=1, type=TYPE_MESSAGE, name="m", path="/store/1/",
                   file_bits=8, file_group_bits=8, mailbox_bits=12,
                   mailbox_group_bits=8)
        self.assertEqual(v.mailbox_dir(5), "/store/1/0/5")

    def test_blob_relpath(self):
        v = _vol(1)
        # bucket = (12345 >> 8) & 0xFF == 48
        self.assertEqual(v.blob_relpath(12345, 2), "msg/48/12345-2.msg")
        self.assertEqual(v.blob_relpath(12345, -1), "msg/48/12345.msg")


class MapVolumesTest(unittest.TestCase):
    def _dst(self):
        return [_vol(10), _vol(20, TYPE_MESSAGE_SECONDARY),
                _vol(30, TYPE_INDEX)]

    def test_basic(self):
        m = map_volumes([_vol(1), _vol(2, TYPE_MESSAGE_SECONDARY),
                         _vol(3, TYPE_INDEX)], self._dst(), 10, 20, 30)
        self.assertEqual(m, {1: 10, 2: 20, 3: 30})

    def test_secondary_falls_back_to_primary(self):
        m = map_volumes([_vol(2, TYPE_MESSAGE_SECONDARY)], self._dst(),
                        10, None, 30)
        self.assertEqual(m, {2: 10})

    def test_explicit_override(self):
        dst = self._dst() + [_vol(99)]
        m = map_volumes([_vol(2, TYPE_MESSAGE_SECONDARY)], dst, 10, 20, 30,
                        explicit={2: 99})
        self.assertEqual(m, {2: 99})

    def test_unknown_type_skipped(self):
        m = map_volumes([_vol(7, 99)], self._dst(), 10, 20, 30)
        self.assertEqual(m, {})

    def test_external_src_needs_explicit(self):
        with self.assertRaises(ValueError):
            map_volumes([_vol(1, store=2)], self._dst(), 10, 20, 30)

    def test_external_dst_needs_explicit(self):
        dst = [_vol(10, store=2), _vol(20, TYPE_MESSAGE_SECONDARY),
               _vol(30, TYPE_INDEX)]
        with self.assertRaises(ValueError):
            map_volumes([_vol(1)], dst, 10, 20, 30)

    def test_dst_missing(self):
        with self.assertRaises(ValueError):
            map_volumes([_vol(1)], [_vol(30, TYPE_INDEX)], 10, 20, 30)

    def test_type_mismatch(self):
        # src msg mapped onto an index volume
        with self.assertRaises(ValueError):
            map_volumes([_vol(1)], [_vol(10, TYPE_INDEX)], 10, 20, 10)

    def test_layout_mismatch(self):
        dst = [_vol(10, fb=10), _vol(20, TYPE_MESSAGE_SECONDARY),
               _vol(30, TYPE_INDEX)]
        with self.assertRaises(ValueError):
            map_volumes([_vol(1)], dst, 10, 20, 30)

    def test_layout_mismatch_explicit_internal_still_rejected(self):
        dst = [_vol(99, fb=10)]
        with self.assertRaises(ValueError):
            map_volumes([_vol(1)], dst, 99, 20, 30, explicit={1: 99})


class LocatorSqlTest(unittest.TestCase):
    def test_identity(self):
        self.assertEqual(locator_case_sql("locator", {}), "locator")
        self.assertEqual(locator_case_sql("locator", {1: 1}), "locator")

    def test_mapping(self):
        self.assertEqual(
            locator_case_sql("loc", {1: 2}),
            "(CASE loc WHEN '1' THEN '2' ELSE loc END)")
        self.assertEqual(
            locator_case_sql("loc", {3: 4, 1: 2}),
            "(CASE loc WHEN '1' THEN '2' WHEN '3' THEN '4' ELSE loc END)")


class TransportTest(unittest.TestCase):
    def test_lmtp_rewritten(self):
        self.assertEqual(replace_host_in_transport("lmtp:a:7025", "b"),
                         "lmtp:b:7025")

    def test_non_lmtp_untouched(self):
        for t in ("smtp:relay:25", "hold:foo", "error:bar", "lmtp-vip:1:2"):
            self.assertEqual(replace_host_in_transport(t, "b"), t)

    def test_empty_gets_default(self):
        self.assertEqual(replace_host_in_transport("", "b"), "lmtp:b:7025")


if __name__ == "__main__":
    unittest.main()
