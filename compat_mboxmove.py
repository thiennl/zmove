"""Faithful Python port of zmmboxmove (MboxMoveUtil + MoveMailboxRequest).

Sends the NE MoveMailbox request to the *destination* server; the server
performs the move. This is a thin client over admin SOAP, mirroring
com.zimbra.cs.mailbox.MboxMoveUtil argument handling, validation messages
and exit behavior.

Divergences from the Java original (all deliberate):
  * No license gate: this port targets FOSS servers, which have no license
    concept. Enforcement is left to the destination server; a client-side
    BACKUP_ENABLED check would wrongly fail closed, so it is omitted.
  * Auth accepts --authtoken/--authtokenfile (raw token string, or a JSON
    file carrying it under authtoken/authToken/token/id), else the configured
    admin credentials via localconfig, like Engine/AdminSoap. Java additionally
    supports interactive login, which this port does not implement.
  * -s/--server is accepted but unconditionally overridden by --to, exactly
    like MboxMoveUtil.main calling setServer(destHost).

NE message catalog (kept verbatim):
  * "specified destHost does not correspond to a valid server hostname: X"
  * "specified targetHost does not correspond to a valid mailclient server
     with service webapp enabled: X"
  * "WARNING: --force option specified. ..." (stdout)
  * "Mailbox move started" / "Mailbox move completed successfully" (stdout)
  * "Error occurred: ..." (stderr), exit 1; usage errors exit 1 (not 2).
"""
from __future__ import annotations

import argparse
import json
import sys

from .config import Settings
from .remote import Host
from .soap import AdminSoap, SoapFault
from .topology import first, parse_zmprov_attrs

COMMAND_USAGE = "zmmboxmove -a <email> --from <src> --to <dest> [--sync]"
TRAILER = "Move the account's mailbox from source server to destination server."
FORCE_WARNING = (
    "WARNING: --force option specified. Not all data may be copied from the "
    "source to destination mailbox. The source mailbox should be manually "
    "inspected and any remaining data must be migrated manually."
)
FORCE_HELP = (
    "Force mailbox move to work with servers predating 7.2.5/8.0.5. Use with "
    "caution. Not all data may be copied from the source to destination "
    "mailbox. The source mailbox should be manually inspected and any "
    "remaining data must be migrated manually."
)


class _Parser(argparse.ArgumentParser):
    """argparse exits 2 on error; MboxMoveUtil exits 1. Match Java."""

    def error(self, message: str) -> None:  # noqa: D102
        self.print_usage(sys.stderr)
        self.exit(1, f"{self.prog}: error: {message}\n")


def bind_exit1(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Make an existing parser exit 1 (not 2) on usage errors, like MboxMoveUtil."""
    parser.error = _Parser.error.__get__(parser, type(parser))  # type: ignore[method-assign]
    return parser



def add_mboxmove_args(p: argparse.ArgumentParser) -> None:
    """zmmboxmove-compatible flags. Shared by `zmove mboxmove` and the
    standalone `python -m zmove.compat_mboxmove` entry (prog=zmmboxmove)."""
    p.add_argument("-a", "--account", required=True,
                   help="Email address of account to move")
    p.add_argument("-f", "--from", dest="src", required=True,
                   help="Current home server of the account")
    p.add_argument("-t", "--to", dest="dst", required=True,
                   help="Destination server")
    p.add_argument("--sync", action="store_true",
                   help="Run synchronously")
    p.add_argument("--force", action="store_true", help=FORCE_HELP)
    p.add_argument("--ng-migration", action="store_true",
                   help="Run for ng migration, forces to migrate locators to Z-10 format")
    p.add_argument("-s", "--server", default=None,
                   help="Mail server hostname. Overridden by --to (NE parity).")
    p.add_argument("--authtoken", default=None,
                   help="use auth token string (has to be in JSON format) from command line")
    p.add_argument("--authtokenfile", default=None,
                   help="read auth token (has to be in JSON format) from a file")


def build_parser(prog: str = "zmmboxmove") -> argparse.ArgumentParser:
    p = _Parser(prog=prog, usage=COMMAND_USAGE, description=TRAILER,
                add_help=True)
    add_mboxmove_args(p)
    return p


def _read_authtoken(value: str | None, path: str | None) -> str | None:
    if value:
        raw = value
    elif path:
        with open(path, encoding="utf-8") as f:
            raw = f.read().strip()
    else:
        return None
    try:
        obj = json.loads(raw)
    except ValueError:
        return raw
    if isinstance(obj, dict):
        for key in ("authtoken", "authToken", "token", "id", "auth_token"):
            if obj.get(key):
                return str(obj[key])
    return raw if isinstance(obj, str) else raw


def _check_dest(host: Host, dst: str) -> None:
    """Mirror MboxMoveUtil dest validation: server must exist and offer the
    mailclient service. Raises RuntimeError with the NE message."""
    try:
        out = host.out(f"zmprov -l gs {dst} zimbraId zimbraServiceEnabled")
    except Exception as e:  # noqa: BLE001 transport/auth/ldap failure
        raise RuntimeError(
            f"specified destHost does not correspond to a valid server "
            f"hostname: {dst} ({e})") from e
    attrs = parse_zmprov_attrs(out)
    if not first(attrs, "zimbraId"):
        raise RuntimeError(
            f"specified destHost does not correspond to a valid server "
            f"hostname: {dst}")
    if "mailbox" not in (attrs.get("zimbraServiceEnabled") or []):
        raise RuntimeError(
            f"specified targetHost does not correspond to a valid mailclient "
            f"server with service webapp enabled: {dst}")


def run_mboxmove(s: Settings, account: str, src: str, dst: str, *,
                 sync: bool = False, force: bool = False,
                 ng_migration: bool = False,
                 authtoken: str | None = None,
                 authtokenfile: str | None = None) -> int:
    """Execute the zmmboxmove flow. Returns the process exit code."""
    host = Host(dst, s)
    try:
        _check_dest(host, dst)
        soap = AdminSoap(host, s.admin_port, s.admin_user, s.admin_password,
                         s.verify_tls, s.tls_ca)
        token = _read_authtoken(authtoken, authtokenfile)
        if token is not None:
            soap._token = token  # same slot token() caches into
        else:
            soap.token()  # fail fast on auth, like SoapCLI.auth()
        if force:
            print(FORCE_WARNING, flush=True)
        soap.move_mailbox(account, src, dst, sync=sync,
                          skip_remote_lockout=force,
                          ng_migration=ng_migration)
    except SoapFault as e:
        print(f"Error occurred: {e.code}: {e.reason}", file=sys.stderr)
        if str(e.code).endswith(("UNKNOWN_DOCUMENT", "UNKNOWN_SERVICE")):
            print("hint: the destination does not implement MoveMailbox "
                  "(FOSS server?). Use `zmove move` or `zmove ne-move` instead, "
                  "which perform the move without NE server code.",
                  file=sys.stderr)
        return 1
    except Exception as e:  # noqa: BLE001 parity with main()'s catch-all
        print(f"Error occurred: {e}", file=sys.stderr)
        return 1
    if sync:
        print("Mailbox move completed successfully", flush=True)
    else:
        print("Mailbox move started", flush=True)
    return 0


def main(argv: list[str] | None = None) -> int:
    from .config import load
    parser = build_parser()
    a = parser.parse_args(argv)
    try:
        s = load(None)
    except (ValueError, FileNotFoundError) as e:
        print(f"Error occurred: bad config: {e}", file=sys.stderr)
        return 1
    return run_mboxmove(s, a.account, a.src, a.dst, sync=a.sync,
                        force=a.force, ng_migration=a.ng_migration,
                        authtoken=a.authtoken, authtokenfile=a.authtokenfile)


if __name__ == "__main__":
    raise SystemExit(main())
