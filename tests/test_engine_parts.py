"""Tests for blob rsync parsing/command building and the NE server port
fault paths. Host objects are constructed only (no SSH traffic)."""
import unittest
from unittest.mock import MagicMock

from zmove.blobs import BlobSync, DirPlan, parse_stats
from zmove.ne_server import DEFAULT_MAX_SYNCS, DEFAULT_SYNC_FINISH_THRESHOLD_MS
from zmove.ne_server import NeMoveError, NeServerMove
from zmove.soap import SoapFault


class ParseStatsTest(unittest.TestCase):
    def test_parses_comma_numbers(self):
        out = ("Number of files: 1,300\n"
               "Number of regular files transferred: 1,234\n"
               "Total transferred file size: 56,789 bytes\n")
        self.assertEqual(parse_stats(out), (1234, 56789))

    def test_missing_is_zero(self):
        self.assertEqual(parse_stats(""), (0, 0))
        self.assertEqual(parse_stats("Number of regular files transferred: 7"),
                         (7, 0))


class SyncCmdTest(unittest.TestCase):
    def _sync(self, **kw):
        from zmove.config import Settings
        s = Settings(**kw)
        from zmove.remote import Host
        return BlobSync(s, Host("src", s), Host("dst", s))

    def _plan(self, shared=False, external=False):
        return DirPlan(kind="msg", dst_volume=10, dst_dir="/d/0/5",
                       src_dirs=["/s/0/5"], src_volumes=[1],
                       shared=shared, external=external)

    def test_final_adds_checksum_full_speed(self):
        bs = self._sync()
        host, cmd = bs._cmd(self._plan(), ["/s/0/5"], final=False)
        self.assertNotIn("--checksum", cmd)
        self.assertIn("ionice", cmd)
        host, cmd = bs._cmd(self._plan(), ["/s/0/5"], final=True)
        self.assertIn("--checksum", cmd)
        self.assertNotIn("ionice", cmd)

    def test_bwlimit_only_online(self):
        bs = self._sync(rsync_bwlimit_kbps=1000)
        _, cmd = bs._cmd(self._plan(), ["/s/0/5"], final=False)
        self.assertIn("--bwlimit=1000", cmd)
        _, cmd = bs._cmd(self._plan(), ["/s/0/5"], final=True)
        self.assertNotIn("--bwlimit", cmd)

    def test_shared_uses_link_dest_on_dst(self):
        bs = self._sync()
        host, cmd = bs._cmd(self._plan(shared=True), ["/s/0/5"], final=False)
        self.assertIn("--link-dest=", cmd)
        self.assertEqual(host.name, "dst")


class NeServerConstantsTest(unittest.TestCase):
    def test_match_ne_defaults(self):
        self.assertEqual(DEFAULT_MAX_SYNCS, 10)
        self.assertEqual(DEFAULT_SYNC_FINISH_THRESHOLD_MS, 180000)


class RegisterTest(unittest.TestCase):
    def _move(self):
        m = NeServerMove.__new__(NeServerMove)
        m._say = lambda *a, **k: None
        m.eng = MagicMock()
        return m

    def test_foss_unknown_document_tolerated(self):
        m = self._move()
        m.eng.soap.return_value.call.side_effect = SoapFault(
            "service.UNKNOWN_DOCUMENT", "unknown document")
        m._register("a@b", "src", "dst")  # must not raise

    def test_ne_conflict_still_aborts(self):
        m = self._move()
        m.eng.soap.return_value.call.side_effect = SoapFault(
            "mail.ALREADY_BEING_MOVED_OUT", "already moved")
        with self.assertRaises(NeMoveError):
            m._register("a@b", "src", "dst")

    def test_resolve_modes(self):
        m = self._move()
        self.assertTrue(m._resolve("include", True))
        self.assertFalse(m._resolve("exclude", False))
        self.assertFalse(m._resolve("config", True))
        self.assertTrue(m._resolve("config", False))
        self.assertTrue(m._resolve(None, False))


class SnapshotTest(unittest.TestCase):
    def _rep(self):
        return {
            "account": {"mail_host": "s1", "status": "active",
                        "mail_transport": "lmtp:s1:7025"},
            "src_mailbox": {"id": 7, "group_id": 2, "items": 10,
                            "size": 100},
            "volume_map": {1: 10},
            "copy": {"blobs": True},
        }

    def test_snapshot_keys(self):
        from zmove.engine import pre_move_snapshot
        snap = pre_move_snapshot(self._rep())
        self.assertEqual(snap["mail_host"], "s1")
        self.assertEqual(snap["mailbox"]["id"], 7)
        self.assertEqual(snap["volume_map"], {1: 10})
        self.assertIn("taken_at", snap)

    def test_in_window_still_works(self):
        import datetime as dt
        from zmove.engine import in_window
        noon = dt.datetime(2026, 1, 1, 12, 0)
        self.assertTrue(in_window("11:00-13:00", noon))
        self.assertFalse(in_window("13:00-14:00", noon))


class NeLockTest(unittest.TestCase):
    def _move(self):
        from zmove.ne_server import NeServerMove
        m = NeServerMove.__new__(NeServerMove)
        m._say = lambda *a, **k: None
        m.eng = MagicMock()
        return m

    def test_lock_delegates_to_benign_wrapper(self):
        from unittest.mock import patch
        m = self._move()
        with patch("zmove.caches.flush_account_cache"):
            m._lock("a@b", "src", "dst", skip_lockout=False)
        m.eng._lockout.assert_called_once_with("src", "a@b", "start")

    def test_skip_lockout(self):
        from unittest.mock import patch
        m = self._move()
        with patch("zmove.caches.flush_account_cache"):
            m._lock("a@b", "src", "dst", skip_lockout=True)
        m.eng._lockout.assert_not_called()


if __name__ == "__main__":
    unittest.main()
