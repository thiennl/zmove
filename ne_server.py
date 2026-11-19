"""NE-faithful server-side move coordinator (MoveMailbox.handleNetworkRequest/moveIt).

Runs the destination-side task the NE server performs: safety checks, version
check, RegisterMailboxMoveOut, unlocked rsync rounds, source lockout, final
sync, full DB copy while locked, cutover, cache purge, unregister.

Relationship to zmove:
  * `zmove move` (engine.py) is the improved reimplementation: delta prestage,
    staging schema + merge, journal with resume, batch mode.
  * This module mirrors NE order and semantics instead: no delta prestage, no
    journal/resume (in-memory run like MailboxMoveThread), always synchronous
    (async has no meaning for a control-node process that would exit).
  * Transport primitives (SSH/MySQL/rsync/SOAP/LDAP) are shared with zmove:
    Host, MySQL/DbMove, BlobSync, AdminSoap, caches, topology, paths.

Deliberate divergences from NE (all documented):
  * Blob bytes travel by rsync volume sync (proven zmove path) instead of the
    mboxexport zip + blobs-sync-token handshake; stop conditions
    (maxSyncs/threshold) and skip flags are identical, end state identical.
  * DB lands via stage + single-transaction merge (DbMove) instead of import
    straight into the live prepared mailbox; visible result identical, and a
    failed merge never leaves a half-written live mailbox.
  * Live orphan-blob sweep is omitted (destructive; wrong locator math would
    delete live data). Temp/stage dirs are still cleaned.
  * Post-cutover extras zmove added (postqueue -f, reindex-after) are NOT run;
    NE relies on postfix retry and keeps the existing index choice.
  * Failures best-effort unregister the move-out and unlock the source; NE
    leaves cleanup to FailedCleanupTaskInterval (default 20m).
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Optional

from . import caches
from .blobs import BlobSync
from .config import Settings
from .dbmove import DbMove, DbPlan, VerifyError, reserve_mailbox_id
from .engine import Engine
from .paths import TYPE_MESSAGE_SECONDARY, replace_host_in_transport
from .soap import SoapFault
from .state import State
from .topology import AccountInfo, ServerInfo, account_info

log = logging.getLogger("zmove.ne_server")

# Mirror MoveMailbox constants: DEFAULT_MAX_SYNCS = 10,
# DEFAULT_SYNC_FINISH_THRESHOLD = 180000ms.
DEFAULT_MAX_SYNCS = 10
DEFAULT_SYNC_FINISH_THRESHOLD_MS = 180000


class NeMoveError(RuntimeError):
    pass


class NeServerMove:
    """Destination-side move task. Owns no journal; a crash needs operator
    cleanup (drop stage schema + dst blob dirs, unlock source)."""

    def __init__(self, settings: Settings,
                 progress: Optional[Callable[[str], None]] = None):
        self.s = settings
        self.progress = progress or (lambda m: None)
        # Reuse Engine only for host/mysql/soap caching, version reads and
        # volume planning; orchestration below is NE order, not engine order.
        self.eng = Engine(settings, State(settings.state_db))

    # ------------------------------------------------------------------ setup
    def _say(self, msg: str) -> None:
        log.info("%s", msg)
        self.progress(msg)

    def _resolve(self, mode: str | None, cfg_skip: bool) -> bool:
        if mode == "include":
            return True
        if mode == "exclude":
            return False
        return not cfg_skip

    # ------------------------------------------------------------------ input
    def _checks(self, account: str, src: str, dst: str, opts: dict):
        """NE safety checks from handleNetworkRequest, pre-mutation."""
        # Auth both ends first (fail fast, like SoapCLI auth before the move).
        for peer in (src, dst):
            try:
                self.eng.soap(peer).token()
            except Exception as e:  # noqa: BLE001
                raise NeMoveError(f"admin SOAP auth failed on {peer}: {e}") from e
        acct: AccountInfo = account_info(self.eng.host(dst), account)
        if acct.mail_host != src:
            raise NeMoveError(
                f"Account {acct.name} is not on server {src}; "
                f"source server should be {acct.mail_host}")
        admin_user = (self.s.admin_user or "").strip().lower()
        if admin_user and admin_user == acct.name.lower():
            raise NeMoveError(
                "Account being moved is the same as the one used for "
                "authentication. Administrators may not move themselves "
                "directly.")
        srcI = self.eng.server(src, True)
        dstI = self.eng.server(dst, True)
        if not dstI.has_mail_client:
            raise NeMoveError(
                "specified targetHost does not correspond to a valid "
                f"mailclient server with service webapp enabled: {dst}")
        self._check_versions(acct, srcI, dstI)
        smb = self._mailbox_row(src, acct.id, with_stats=True)
        if not smb:
            raise NeMoveError(f"no mailbox for {acct.name} on {src}")
        from .topology import mailbox_row
        dmb = mailbox_row(self.eng.mysql(dst), acct.id, with_stats=False)
        if dmb:
            raise NeMoveError(
                f"A mailbox already exists (id={dmb.id}) on this server for "
                f"account {acct.name}; Purge it with zmpurgeoldmbox first.")
        return acct, srcI, dstI, smb

    def _check_versions(self, acct: AccountInfo, srcI: ServerInfo,
                        dstI: ServerInfo) -> None:
        """Mirror getRemoteVersionInfo: refuse when mailbox/db/index versions
        differ (prevents moving to an older server)."""
        for k in ("version", "db_version", "index_version"):
            if getattr(srcI, k) != getattr(dstI, k):
                raise NeMoveError(
                    f"{k} differs: {srcI.name}={getattr(srcI, k)} "
                    f"{dstI.name}={getattr(dstI, k)}")

    def _mailbox_row(self, server: str, account_id: str, with_stats: bool):
        from .topology import mailbox_row
        return mailbox_row(self.eng.mysql(server), account_id,
                           with_stats=with_stats)

    # ------------------------------------------------------------------ plan
    def _build(self, account: str, src: str, dst: str, opts: dict,
               acct: AccountInfo, srcI: ServerInfo, dstI: ServerInfo,
               smb) -> tuple:
        from .paths import map_volumes
        copy_blobs = self._resolve(opts.get("blobs"), dstI.move_skip_blobs)
        copy_sec = self._resolve(opts.get("secondary_blobs"),
                                 dstI.move_skip_hsm_blobs)
        copy_idx = self._resolve(opts.get("search_index"),
                                 dstI.move_skip_index)
        shared = dict(self.s.shared_volumes)
        for v in [x for x in srcI.volumes if x.is_message]:
            if not (copy_blobs and (copy_sec or v.type != TYPE_MESSAGE_SECONDARY)) \
                    and v.id not in shared:
                raise NeMoveError(
                    f"blobs on volume {v.id} excluded but no shared_volumes "
                    f"mapping for it")
        src_msg = [v for v in srcI.volumes if v.is_message]
        idx_vol = srcI.volume(smb.index_volume_id)
        try:
            vmap = map_volumes(src_msg + [idx_vol], dstI.volumes,
                               dstI.current_msg, dstI.current_secondary,
                               dstI.current_index, explicit=shared)
        except ValueError as e:
            raise NeMoveError(str(e)) from e
        dst_id = reserve_mailbox_id(self.eng.mysql(dst))
        plan = DbPlan(account=acct.name, account_id=acct.id, src_id=smb.id,
                      src_group=smb.group_id, dst_id=dst_id,
                      dst_group=self._group_id(dst_id, dstI.num_groups),
                      dst_index_volume=dstI.current_index,
                      stage=f"zmove_stage_ne_{dst_id}",
                      vol_map=dict(vmap),
                      exclude_tables=list(opts.get("exclude_tables") or []))
        dbm = DbMove(plan, self.eng.mysql(src), self.eng.mysql(dst),
                     self.eng.dry if hasattr(self.eng, "dry") else False)
        dbm.discover()
        dbm.ensure_dst_group_db()
        dbm.create_stage()
        plans = self.eng._dir_plans(srcI, dstI, smb.id, dst_id, vmap,
                                    copy_idx, idx_vol.id)
        return dbm, plans, BlobSync(self.s, self.eng.host(src),
                                    self.eng.host(dst)), vmap

    @staticmethod
    def _group_id(mailbox_id: int, num_groups: int) -> int:
        from .paths import mailbox_group_id
        return mailbox_group_id(mailbox_id, num_groups)

    # ------------------------------------------------------------------ run
    def run(self, account: str, src: str, dst: str, opts: dict,
            sync: bool = True) -> dict:
        """Execute the move. sync=False runs the task in a thread and returns
        immediately (NE async mode); the thread is non-daemon so the process
        stays alive until it finishes."""
        if not sync:
            th = threading.Thread(
                target=self._run_guarded,
                args=(account, src, dst, opts),
                name=f"ne-move-{account}", daemon=False)
            th.start()
            print("Mailbox move started", flush=True)
            return {"thread": th, "account": account, "src": src, "dst": dst}
        return self._run_guarded(account, src, dst, opts)

    def _run_guarded(self, account: str, src: str, dst: str,
                     opts: dict) -> dict:
        acct, srcI, dstI, smb = self._checks(account, src, dst, opts)
        dbm, plans, bs, vmap = self._build(account, src, dst, opts, acct,
                                           srcI, dstI, smb)
        self._register(account, src, dst)
        max_syncs = int(opts.get("max_syncs") or DEFAULT_MAX_SYNCS)
        threshold = int(opts.get("sync_finish_threshold_ms")
                        or DEFAULT_SYNC_FINISH_THRESHOLD_MS)
        skip_lockout = bool(opts.get("skip_lockout"))
        skip_memcache = bool(opts.get("skip_memcache_purge"))
        verify = opts.get("verify") or "full"
        started = time.time()
        self._say(f"[{account}] unlocked sync: up to {max_syncs} passes")
        for rnd in range(1, max(1, max_syncs) + 1):
            st = bs.sync_once(plans)
            self._say(f"[{account}] rsync round {rnd}: {st.files} files, "
                      f"{st.bytes}B in {st.seconds:.1f}s")
            if st.seconds * 1000 <= threshold or st.files == 0:
                break
        self._lock(account, src, dst, skip_lockout)
        try:
            st = bs.sync_once(plans, final=True)
            self._say(f"[{account}] final rsync round: {st.files} files, "
                      f"{st.bytes}B in {st.seconds:.1f}s")
            info = dbm.final("full", None)
            self._say(f"[{account}] database copied while locked: {info}")
            diff = dbm.compare("src", "stage", verify)
            if diff:
                raise VerifyError("stage != source: " + "; ".join(diff))
            dbm.merge()
            diff = dbm.compare("src_mapped", "live", verify)
            if diff:
                dbm.delete_live_dst()
                raise VerifyError(
                    "live != source after merge (rolled back): "
                    + "; ".join(diff))
            self._flip(account, src, dst)
            self._post_flip(account, src, dst, skip_memcache)
        finally:
            self._unlock_and_unregister(account, src, dst, skip_lockout)
        if not opts.get("keep_stage"):
            dbm.drop_stage()
        self._say(f"[{account}] Mailbox move completed successfully "
                  f"({time.time() - started:.0f}s)")
        return {"account": account, "src": src, "dst": dst,
                "dst_mailbox_id": dbm.p.dst_id,
                "seconds": round(time.time() - started, 1)}

    # ------------------------------------------------------------------ steps
    # Fault codes proving the server has no such handler (FOSS): the move
    # proceeds without server-side registration, like a standalone NE retry
    # after UnregisterMailboxMoveOut.
    _NO_HANDLER = ("UNKNOWN_DOCUMENT", "UNKNOWN_SERVICE")

    def _register(self, account: str, src: str, dst: str) -> None:
        """RegisterMailboxMoveOut on the source (ALREADY_BEING_MOVED_OUT aborts).

        FOSS servers do not implement the handler: degrade to a warning so the
        same flow runs FOSS-to-FOSS (the Python port targets FOSS)."""
        try:
            self.eng.soap(src).call(
                "RegisterMailboxMoveOutRequest",
                {"account": {"name": account, "target": dst}})
        except SoapFault as e:
            if str(e.code).endswith(self._NO_HANDLER):
                self._say(f"[{account}] source has no move-out registry "
                          f"(FOSS); continuing without it")
                return
            raise NeMoveError(f"register move-out on {src} failed: "
                              f"{e.code}: {e.reason}") from e

    def _unregister(self, account: str, src: str, dst: str) -> None:
        try:
            self.eng.soap(src).call(
                "UnregisterMailboxMoveOutRequest",
                {"account": {"name": account, "target": dst}})
        except Exception as e:  # noqa: BLE001 best effort, like NE cleanup
            log.warning("unregister move-out on %s failed: %s", src, e)

    def _lock(self, account: str, src: str, dst: str,
              skip_lockout: bool) -> None:
        h = self.eng.host(dst)
        h.run(f"zmprov -l ma {account} zimbraAccountStatus maintenance")
        from . import caches
        caches.flush_account_cache(h, account)
        if not skip_lockout:
            self.eng._lockout(src, account, "start")

    def _flip(self, account: str, src: str, dst: str) -> None:
        """Cutover: point zimbraMailHost (and LMTP transport) at dest."""
        from .topology import account_info
        h = self.eng.host(dst)
        acct = account_info(h, account)
        transport = replace_host_in_transport(acct.mail_transport or "", dst)
        if acct.mail_transport and transport == acct.mail_transport:
            self._say(f"[{account}] keeping non-LMTP transport "
                      f"{acct.mail_transport!r}")
        h.run(f"zmprov -l ma {account} zimbraMailHost {dst} "
              f"zimbraMailTransport {transport}")

    def _post_flip(self, account: str, src: str, dst: str,
                   skip_memcache: bool) -> None:
        from .topology import account_info
        h = self.eng.host(dst)
        acct = account_info(h, account)
        from . import caches
        caches.flush_account_cache(h, account)
        if not skip_memcache:
            caches.purge_proxy_routes(h, account)
            n = caches.delete_memcached_keys(h, acct.id)
            self._say(f"[{account}] memcached account keys deleted: {n}")
        h.run(f"zmprov -l ma {account} zimbraAccountStatus {acct.status}")
        caches.flush_account_cache(h, account)

    def _unlock_and_unregister(self, account: str, src: str, dst: str,
                               skip_lockout: bool) -> None:
        if not skip_lockout:
            try:
                self.eng.soap(src).lockout(account, "end")
            except Exception as e:  # noqa: BLE001
                log.warning("lockout(end) on %s failed: %s", src, e)
        self._unregister(account, src, dst)


def add_ne_move_args(p) -> None:
    """MoveMailboxRequest-shaped flags (no zmove extensions: full copy while
    locked, no delta prestage, no reindex, no MTA flush)."""
    p.add_argument("account")
    p.add_argument("-f", "--from", dest="src", required=True)
    p.add_argument("-t", "--to", dest="dst", required=True)
    p.add_argument("--max-syncs", type=int, default=None,
                   help="max unlocked sync passes (NE default 10)")
    p.add_argument("--sync-finish-threshold", dest="sync_finish_threshold_ms",
                   type=int, default=None, metavar="MS",
                   help="stop unlocked passes when one takes <= MS "
                   "(NE code default 180000)")
    for _name in ("blobs", "secondary-blobs", "search-index"):
        p.add_argument(f"--{_name}", choices=("include", "exclude", "config"),
                       default=None)
    p.add_argument("--skip-lockout", action="store_true",
                   help="NE skipRemoteLockout")
    p.add_argument("--skip-memcache-purge", action="store_true",
                   help="NE skipMemcachePurge")
    p.add_argument("--verify", choices=("full", "count", "none"), default=None)
    p.add_argument("--exclude-tables", default=None,
                   help="comma list of mboxgroup tables to skip")
    p.add_argument("--keep-stage", action="store_true",
                   help="keep staging schema for forensics")


def ne_opts(a) -> dict:
    """argparse namespace -> NeServerMove opts (None = engine default)."""
    return {
        "max_syncs": a.max_syncs,
        "sync_finish_threshold_ms": a.sync_finish_threshold_ms,
        "blobs": a.blobs,
        "secondary_blobs": a.secondary_blobs,
        "search_index": a.search_index,
        "skip_lockout": a.skip_lockout or None,
        "skip_memcache_purge": a.skip_memcache_purge or None,
        "verify": a.verify,
        "exclude_tables": a.exclude_tables.split(",")
        if a.exclude_tables else None,
        "keep_stage": a.keep_stage or None,
    }
