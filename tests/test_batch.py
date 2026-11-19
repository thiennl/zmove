"""Batch canary tests with stubbed workers (no threads hit network)."""
import unittest
from unittest.mock import patch

from zmove.batch import Batch, Item
from zmove.config import Settings


def _ok(it, options):
    return it, "job-1", None


def _fail(it, options):
    return it, None, "MoveError: boom"


class CanaryTest(unittest.TestCase):
    def _batch(self):
        return Batch(Settings(), parallel=1, dry_run=True)

    def _items(self, n):
        return [Item(f"u{i}@x", "s", "d") for i in range(n)]

    def test_no_canary_runs_all(self):
        b = self._batch()
        with patch.object(Batch, "_one", autospec=True,
                          side_effect=lambda self, it, o: (it, "j", None)):
            res = b.run(self._items(3), {}, canary=0)
        self.assertEqual(len(res), 3)

    def test_canary_failure_stops_rest(self):
        b = self._batch()
        calls = []

        def one(self, it, options):
            calls.append(it.account)
            if it.account == "u0@x":
                return it, None, "MoveError: boom"
            return it, "j", None

        with patch.object(Batch, "_one", autospec=True, side_effect=one):
            res = b.run(self._items(3), {}, canary=1)
        self.assertEqual([it.account for it, _, _ in res], ["u0@x"])
        self.assertEqual(calls, ["u0@x"])

    def test_canary_pause_non_tty(self):
        b = self._batch()
        with patch.object(Batch, "_one", autospec=True,
                          side_effect=lambda self, it, o: (it, "j", None)):
            with patch("zmove.batch._confirm_continue", return_value=False):
                res = b.run(self._items(3), {}, canary=1)
        self.assertEqual(len(res), 1)

    def test_canary_approved_continues(self):
        b = self._batch()
        with patch.object(Batch, "_one", autospec=True,
                          side_effect=lambda self, it, o: (it, "j", None)):
            with patch("zmove.batch._confirm_continue", return_value=True):
                res = b.run(self._items(3), {}, canary=2)
        self.assertEqual(len(res), 3)

    def test_canary_ge_len_runs_all(self):
        b = self._batch()
        with patch.object(Batch, "_one", autospec=True,
                          side_effect=lambda self, it, o: (it, "j", None)):
            with patch("zmove.batch._confirm_continue",
                       side_effect=AssertionError("no prompt expected")):
                res = b.run(self._items(2), {}, canary=5)
        self.assertEqual(len(res), 2)


if __name__ == "__main__":
    unittest.main()
