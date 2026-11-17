"""Cache coherence after cutover (NE: skipMemcachePurge controls the same step).

Every function here is scoped to ONE account: nothing flushes server-wide caches or the whole
MTA queue, and every remote call has a timeout so an unreachable cache/MTA cannot leave the
account stuck in maintenance.
"""
from __future__ import annotations

import json
import logging
import shlex
import subprocess
from typing import Iterable

from .remote import Host
from .topology import memcached_servers

log = logging.getLogger("zmove.caches")

# MemcachedKeyPrefix.java: keys keyed only by accountId (no checkpoint validation on load).
# FoldersTagsCache is read in Mailbox.loadFoldersAndTags() without comparing change ids.
ACCOUNT_KEYS = ("zmFldrsTags:{id}", "zmCalsList:{id}")

CMD_TIMEOUT = 120          # seconds for zmprov / zmproxypurge / postqueue calls
MEMCACHED_TIMEOUT = 5      # seconds per memcached delete (connect + reply)


def _run(host: Host, cmd: str, what: str, timeout: float = CMD_TIMEOUT):
    """Best-effort remote call: failures and timeouts are logged, never raised."""
    try:
        r = host.run(cmd, check=False, timeout=timeout)
    except subprocess.TimeoutExpired:
        log.warning("[%s] %s timed out after %ss", host.name, what, timeout)
        return None
    if r.rc != 0:
        log.warning("[%s] %s failed rc=%s: %s", host.name, what, r.rc, r.err.strip()[-500:])
    return r


def flush_account_cache(host: Host, account: str) -> None:
    """In-JVM cache entry of THIS account on all mailstores (zmprov fc -a account <name>)."""
    _run(host, f"zmprov flushCache -a account {shlex.quote(account)}", "flushCache")


def purge_proxy_routes(host: Host, account: str, aliases: Iterable[str] = ()) -> None:
    """nginx route cache in memcached (ProxyPurgeUtil: route:proto=...;user=...).

    Routes are keyed by the login name, and users may log in with an alias, so purge the
    canonical name and every alias -- all of them belong to the account being moved."""
    for name in dict.fromkeys([account, *aliases]):
        _run(host, f"zmproxypurge -a {shlex.quote(name)}", f"zmproxypurge {name}")


def delete_memcached_keys(host: Host, account_id: str) -> int:
    """Delete account-scoped mailbox caches on every memcached server (keys are distributed by
    consistent hashing, so we simply ask all of them). Executed from `host` via bash /dev/tcp,
    bounded by `timeout` so an unreachable memcached cannot hang the cutover."""
    deleted = 0
    try:
        servers = memcached_servers(host)
    except Exception as e:  # noqa: BLE001 discovery failure: caches expire on their own
        log.warning("[%s] memcached discovery failed: %s", host.name, e)
        return 0
    for hp in servers:
        h, p = hp.rsplit(":", 1)
        for tmpl in ACCOUNT_KEYS:
            key = tmpl.format(id=account_id)
            cmd = (f"exec 3<>/dev/tcp/{shlex.quote(h)}/{int(p)} && printf 'delete %s\\r\\n' "
                   f"{shlex.quote(key)} >&3 && head -1 <&3; exec 3>&-")
            r = _run(host, f"timeout {MEMCACHED_TIMEOUT} bash -c {shlex.quote(cmd)}",
                     f"memcached delete on {hp}", timeout=MEMCACHED_TIMEOUT + 10)
            if r is not None and "DELETED" in r.out:
                deleted += 1
    return deleted


def queue_ids_for(postqueue_json: str, recipients: Iterable[str]) -> list[str]:
    """Queue ids of DEFERRED messages with at least one recipient of this account
    (`postqueue -j` emits one JSON object per line)."""
    want = {r.lower() for r in recipients if r}
    ids = []
    for line in postqueue_json.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            m = json.loads(line)
        except ValueError:
            continue
        if m.get("queue_name") != "deferred":
            continue
        rcpts = {str(r.get("address", "")).lower() for r in m.get("recipients", [])}
        if rcpts & want and m.get("queue_id"):
            ids.append(str(m["queue_id"]))
    return ids


def flush_mta_queues(hosts: list[Host], account: str, zimbra_home: str = "/opt/zimbra",
                     aliases: Iterable[str] = ()) -> int:
    """Deliver mail deferred (450 4.2.1) during the lock for THIS account immediately.

    Never `postqueue -f`: that would re-try every deferred message of every user on the MTA
    at once. Only messages addressed to the account (canonical name or alias) are rescheduled
    with `postqueue -i`; the rest of the queue keeps its normal retry schedule."""
    pq = shlex.quote(zimbra_home.rstrip("/") + "/common/sbin/postqueue")
    rcpts = [account, *aliases]
    total = 0
    for h in hosts:
        r = _run(h, f"{pq} -j", "postqueue -j")
        if r is None or r.rc != 0:
            log.warning("[%s] cannot list the queue (postqueue -j needs Postfix >= 3.1); "
                        "mail for %s will be retried on the normal schedule", h.name, account)
            continue
        ids = queue_ids_for(r.out, rcpts)
        for i in range(0, len(ids), 200):
            chunk = ids[i:i + 200]
            _run(h, "; ".join(f"{pq} -i {shlex.quote(q)}" for q in chunk), "postqueue -i")
        total += len(ids)
        if ids:
            log.info("[%s] rescheduled %d deferred message(s) for %s", h.name, len(ids), account)
    return total
