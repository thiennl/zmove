"""Minimal Zimbra admin SOAP (JSON) client.

Requests are POSTed with curl *on the target mailstore itself* to https://localhost:7071,
so the control node does not need firewall access to 7071 and secrets travel only on stdin.
"""
from __future__ import annotations

import json
import logging
import shlex
from typing import Any, Optional

from .remote import Host

log = logging.getLogger("zmove.soap")


class SoapFault(RuntimeError):
    def __init__(self, code: str, reason: str):
        self.code = code
        self.reason = reason


class AdminSoap:
    def __init__(self, host: Host, port: int = 7071, user: Optional[str] = None,
                 password: Optional[str] = None, verify_tls: bool = False,
                 tls_ca: Optional[str] = None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.verify_tls = verify_tls
        self.tls_ca = tls_ca
        self._token: Optional[str] = None

    def _curl_base(self) -> str:
        if self.verify_tls:
            verify = f" --cacert {shlex.quote(self.tls_ca)}" if self.tls_ca else ""
        else:
            verify = " -k"
        return (f"curl -sS --max-time 120{verify} -H 'Content-Type: application/json' "
                f"--data-binary @- https://localhost:{self.port}/service/admin/soap/")

    def _post(self, body: dict, token: Optional[str], mutating: bool) -> dict:
        env: dict[str, Any] = {"Header": {"context": {"_jsns": "urn:zimbra"}}, "Body": body}
        if token:
            env["Header"]["context"]["authToken"] = {"_content": token}
        cmd = self._curl_base()
        r = self.host.run(cmd, input=json.dumps(env), mutating=mutating)
        if self.host.dry_run and mutating:
            return {}
        try:
            data = json.loads(r.out)
        except json.JSONDecodeError as e:
            raise SoapFault("zmove.BAD_RESPONSE", f"{self.host.name}: {r.out[:300]}") from e
        b = data.get("Body", {})
        if "Fault" in b:
            f = b["Fault"]
            code = f.get("Detail", {}).get("Error", {}).get("Code", "?")
            reason = f.get("Reason", {}).get("Text", "?")
            raise SoapFault(code, reason)
        return b

    def token(self) -> str:
        if self._token:
            return self._token
        if self.host.dry_run:
            return "dry-run"  # never open a real admin session for a dry run
        user, pw = self.user, self.password
        if not user:
            user = self.host.out("zmlocalconfig -m nokey zimbra_ldap_user") or "zimbra"
            pw = self.host.out("zmlocalconfig -s -m nokey zimbra_ldap_password")
        b = self._post({"AuthRequest": {"_jsns": "urn:zimbraAdmin", "name": user, "password": pw}},
                       None, mutating=False)
        tok = b["AuthResponse"]["authToken"]
        self._token = tok[0]["_content"] if isinstance(tok, list) else tok["_content"]
        return self._token

    def call(self, name: str, payload: dict, mutating: bool = True) -> dict:
        req = {"_jsns": "urn:zimbraAdmin", **payload}
        b = self._post({name: req}, self.token(), mutating=mutating)
        return b.get(name.replace("Request", "Response"), {})

    # -- specific requests -------------------------------------------------
    def lockout(self, account: str, op: str) -> None:
        """LockoutMailboxRequest (OSE: service/admin/LockoutMailbox.java). op = start|end."""
        self.call("LockoutMailboxRequest",
                  {"op": op, "account": {"by": "name", "_content": account}})

    def query_moves(self, account: Optional[str] = None) -> Optional[list[dict]]:
        """Best-effort QueryMailboxMoveRequest (NE only).

        Returns the move list, or None when the server does not support the
        request (FOSS) or rejects the wire format. Request-level faults are
        swallowed on purpose; auth errors still propagate from token().
        """
        try:
            payload: dict[str, Any] = {}
            if account:
                payload["account"] = {"by": "name", "_content": account}
            b = self.call("QueryMailboxMoveRequest", payload, mutating=False)
        except SoapFault:
            log.debug("QueryMailboxMove unsupported on %s", self.host.name)
            return None
        moves = b.get("move", []) if isinstance(b, dict) else []
        if isinstance(moves, dict):
            moves = [moves]
        return moves if isinstance(moves, list) else None
