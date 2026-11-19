"""CLI contract tests: flags exist, prog names and exit codes match NE."""
import contextlib
import io
import unittest

from zmove.compat_mboxmove import _read_authtoken, bind_exit1, build_parser


class ParserTest(unittest.TestCase):
    def test_prog_and_required_flags(self):
        p = build_parser()
        self.assertEqual(p.prog, "zmmboxmove")
        a = p.parse_args(["-a", "u@x", "-f", "s", "-t", "d"])
        self.assertEqual((a.account, a.src, a.dst), ("u@x", "s", "d"))
        self.assertFalse(a.sync)
        a = p.parse_args(["--account", "u@x", "--from", "s", "--to", "d",
                          "--sync", "--force", "--ng-migration"])
        self.assertTrue(a.sync and a.force and a.ng_migration)

    def test_missing_args_exit_1_not_2(self):
        p = build_parser()
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            with self.assertRaises(SystemExit) as cm:
                p.parse_args([])
        self.assertEqual(cm.exception.code, 1)
        self.assertIn("usage:", err.getvalue())

    def test_bind_exit1(self):
        import argparse
        p = argparse.ArgumentParser(prog="x")
        bind_exit1(p)
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as cm:
                p.error("boom")
        self.assertEqual(cm.exception.code, 1)


class AuthtokenTest(unittest.TestCase):
    def test_raw_passthrough(self):
        self.assertEqual(_read_authtoken("tok123", None), "tok123")
        self.assertIsNone(_read_authtoken(None, None))

    def test_json_peel(self):
        self.assertEqual(_read_authtoken('{"authtoken": "X"}', None), "X")
        self.assertEqual(_read_authtoken('{"id": "Y"}', None), "Y")

    def test_file(self):
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".json",
                                         delete=False) as f:
            f.write('{"token": "Z"}')
            path = f.name
        try:
            self.assertEqual(_read_authtoken(None, path), "Z")
        finally:
            import os
            os.unlink(path)


class RunFlowTest(unittest.TestCase):
    """run_mboxmove with faked transport: no SSH, no servers."""

    def _run(self, move_effect=None, gs_out=None):
        import io
        from contextlib import redirect_stderr
        from unittest.mock import MagicMock, patch
        from zmove import compat_mboxmove as cm
        from zmove.config import Settings
        from zmove.soap import SoapFault

        host = MagicMock()
        host.out.return_value = (
            gs_out if gs_out is not None
            else "# name dst\nzimbraId: id-1\nzimbraServiceEnabled: mailbox\n")
        soap = MagicMock()
        soap.token.return_value = "T"
        if isinstance(move_effect, Exception):
            soap.move_mailbox.side_effect = move_effect
        else:
            soap.move_mailbox.return_value = {}
        err = io.StringIO()
        with patch.object(cm, "Host", return_value=host), \
                patch.object(cm, "AdminSoap", return_value=soap), \
                redirect_stderr(err):
            rc = cm.run_mboxmove(Settings(), "a@b", "s", "d",
                                sync=False, force=False,
                                ng_migration=False)
        return rc, err.getvalue()

    def test_unknown_document_hints_foss(self):
        from zmove.soap import SoapFault
        rc, err = self._run(
            SoapFault("service.UNKNOWN_DOCUMENT", "unknown document"))
        self.assertEqual(rc, 1)
        self.assertIn("FOSS server?", err)

    def test_other_fault_has_no_hint(self):
        from zmove.soap import SoapFault
        rc, err = self._run(
            SoapFault("service.INVALID_REQUEST", "already on this server"))
        self.assertEqual(rc, 1)
        self.assertNotIn("FOSS server?", err)

    def test_success_async_message(self):
        import io
        from contextlib import redirect_stdout
        from unittest.mock import MagicMock, patch
        from zmove import compat_mboxmove as cm
        from zmove.config import Settings
        host = MagicMock()
        host.out.return_value = ("# name dst\nzimbraId: id-1\n"
                                 "zimbraServiceEnabled: mailbox\n")
        soap = MagicMock()
        soap.move_mailbox.return_value = {}
        out = io.StringIO()
        with patch.object(cm, "Host", return_value=host), \
                patch.object(cm, "AdminSoap", return_value=soap), \
                redirect_stdout(out):
            rc = cm.run_mboxmove(Settings(), "a@b", "s", "d", sync=True)
        self.assertEqual(rc, 0)
        self.assertIn("completed successfully", out.getvalue())


if __name__ == "__main__":
    unittest.main()
