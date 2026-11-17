"""Cache coherence after cutover (NE: skipMemcachePurge controls the same step)."""
from __future__ import annotations

import logging
import shlex

from .remote import Host
from .topology import memcached_servers

log = logging.getLogger("zmove.caches")

# MemcachedKeyPrefix.java: keys keyed only by accountId (no checkpoint validation on load).
# FoldersTagsCache is read in Mailbox.loadFoldersAndTags() without comparing change ids.
ACCOUNT_KEYS = ("zmFldrsTags:{id}", "zmCalsList:{id}")


def flush_account_cache(host: Host, account: str) -> None:
    """In-JVM account/LDAP caches on ALL mailstores (zmprov fc -a)."""
    r = host.run(f"zmprov flushCache -a account {shlex.quote(account)}", check=False)
    if r.rc != 0:
        log.warning("[%s] flushCache failed rc=%s: %s", host.name, r.rc, r.err.strip()[-500:])


def purge_proxy_routes(host: Host, account: str) -> None:
    """nginx route cache in memcached (ProxyPurgeUtil: route:proto=...;user=...)."""
    r = host.run(f"zmproxypurge -a {shlex.quote(account)}", check=False)
    if r.rc != 0:
        log.warning("[%s] zmproxypurge failed rc=%s: %s", host.name, r.rc, r.err.strip()[-500:])


def delete_memcached_keys(host: Host, account_id: str) -> int:
    """Delete account-scoped mailbox caches on every memcached server (keys are distributed by
    consistent hashing, so we simply ask all of them). Executed from `host` via bash /dev/tcp."""
    deleted = 0
    for hp in memcached_servers(host):
        h, p = hp.rsplit(":", 1)
        for tmpl in ACCOUNT_KEYS:
            key = tmpl.format(id=account_id)
            cmd = (f"exec 3<>/dev/tcp/{shlex.quote(h)}/{int(p)} && printf 'delete %s\\r\\n' "
                   f"{shlex.quote(key)} >&3 && timeout 3 head -1 <&3; exec 3>&-")
            r = host.run(f"bash -c {shlex.quote(cmd)}", check=False)
            if "DELETED" in r.out:
                deleted += 1
    return deleted


def flush_mta_queues(hosts: list[Host], account: str, zimbra_home: str = "/opt/zimbra") -> None:
    """Deliver mail deferred (450 4.2.1) during the lock immediately instead of waiting for retry."""
    postqueue = shlex.quote(zimbra_home.rstrip("/") + "/common/sbin/postqueue")
    for h in hosts:
        r = h.run(f"{postqueue} -f", check=False)
        if r.rc != 0:
            log.warning("[%s] postqueue -f failed rc=%s: %s", h.name, r.rc, r.err.strip()[-500:])
