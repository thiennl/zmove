"""Destructive-op gate tests: purge requires retyping the account."""
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from zmove.cli import _confirm_purge


def _job():
    return SimpleNamespace(account="u@x", data={"src_mailbox_id": 7},
                           src="s1")


class FakeStdin:
    def __init__(self, tty):
        self._tty = tty

    def isatty(self):
        return self._tty


class PurgeConfirmTest(unittest.TestCase):
    def test_non_tty_declines(self):
        with patch.object(sys, "stdin", FakeStdin(False)):
            self.assertFalse(_confirm_purge(_job()))

    def test_exact_email_approves(self):
        with patch.object(sys, "stdin", FakeStdin(True)), \
                patch("builtins.input", return_value="u@x"):
            self.assertTrue(_confirm_purge(_job()))

    def test_wrong_text_declines(self):
        with patch.object(sys, "stdin", FakeStdin(True)), \
                patch("builtins.input", return_value="y"):
            self.assertFalse(_confirm_purge(_job()))

    def test_eof_declines(self):
        with patch.object(sys, "stdin", FakeStdin(True)), \
                patch("builtins.input", side_effect=EOFError):
            self.assertFalse(_confirm_purge(_job()))


if __name__ == "__main__":
    unittest.main()
