"""Move state machine (the part NE keeps closed-source in its MoveMailbox coordinator)."""
from __future__ import annotations

import datetime as dt
import logging
import os
import shlex
import sqlite3
import subprocess
import sys
import time
from dataclasses import asdict
from typing import Callable, Optional

from . import caches
from .blobs import BlobSync, DirPlan, SyncStats
from .config import Settings
from .db import DELTA_TABLES, ZIMBRA_MAILBOX_TABLES, MySQL
from .dbmove import DbMove, DbPlan, VerifyError, reserve_mailbox_id
from .paths import (STORE_INTERNAL, TYPE_MESSAGE_SECONDARY, map_volumes, mailbox_group_id,
                    replace_host_in_transport)
from .remote import Host
from .soap import AdminSoap, SoapFault
from .state import AlreadyMoving, Job, Phase, State
from .topology import (AccountInfo, ServerInfo, account_info, mailbox_id_in_use, mailbox_row,
                       mta_servers, server_info)
log = logging.getLogger("zmove.engine")

DEFAULT_OPTIONS = {
    "max_syncs": None,                 # None -> settings
    "sync_finish_threshold_ms": None,
    "blobs": "config",                 # include | exclude | config  (NE MoveMailboxInfo)
    "secondary_blobs": "config",
    "search_index": "config",
    "skip_lockout": False,             # NE skipRemoteLockout
    "skip_memcache_purge": False,      # NE skipMemcachePurge
    "overwrite": False,                # remove a stray destination mailbox first
    "db_strategy": None,               # delta | full
    "verify": None,                    # full | count | none
    "cutover_window": None,            # "HH:MM-HH:MM" local time of the controller
    "max_lock_seconds": None,
    "exclude_tables": [],
    "reindex_after": False,
    "flush_mta": True,
    "force": False,
    "keep_stage": False,
    "confirm_steps": False,          # ask before each mutating step (decline = clean pause)
}


class MoveError(RuntimeError):
    pass


class LockTimeout(MoveError):
    pass

class StepDeclined(MoveError):
    """Operator answered 'no' at a --confirm-steps gate (clean pause, resume continues)."""


CONFIRM_STEPS = ("reserve", "online-sync", "lock", "final-sync", "merge", "flip", "post-flip")


def _stdin_confirm(prompt: str) -> bool:
    """Default gate: ask on stdin. Non-TTY/EOF declines (never auto-proceed blind)."""
    if not sys.stdin.isatty():
        print("declined: stdin is not a TTY; run interactively or drop --confirm-steps",
              file=sys.stderr)
        return False
    try:
        return input(prompt).strip().lower() in ("y", "yes")
    except EOFError:
        return False


def parse_window(window: str) -> tuple[int, int]:
    """'HH:MM-HH:MM' -> (start, end) minutes. Raises MoveError on bad format."""
    try:
        a, b = window.split("-")
        ha, ma = map(int, a.split(":"))
        hb, mb = map(int, b.split(":"))
    except ValueError:
        raise MoveError(f"bad cutover window {window!r}, want HH:MM-HH:MM")
    for h, m in ((ha, ma), (hb, mb)):
        if not (0 <= h <= 23 and 0 <= m <= 59):
            raise MoveError(f"bad cutover window {window!r}, want HH:MM-HH:MM")
    return ha * 60 + ma, hb * 60 + mb


def in_window(window: str, now: Optional[dt.datetime] = None) -> bool:
    now = now or dt.datetime.now()
    start, end = parse_window(window)
    t = now.hour * 60 + now.minute
    return start <= t < end if start <= end else (t >= start or t < end)


class Engine:
    def __init__(self, settings: Settings, state: State, dry_run: bool = False,
                 progress: Optional[Callable[[str], None]] = None,
                 confirm: Optional[Callable[[str], bool]] = None):
        self.s = settings
        self.state = state
        self.dry = dry_run
        self.progress = progress or (lambda m: None)
        self.confirm = confirm or _stdin_confirm
        self._hosts: dict[str, Host] = {}
        self._servers: dict[str, ServerInfo] = {}
        self.lock_semaphore = None      # batch: cap simultaneous cutovers across the fleet

    # ------------------------------------------------------------------ helpers
    def host(self, name: str) -> Host:
        if name not in self._hosts:
            self._hosts[name] = Host(name, self.s, dry_run=self.dry)
        return self._hosts[name]

    def mysql(self, name: str) -> MySQL:
        return MySQL(self.host(name), self.s.mysql_bin, self.s.mysqldump_bin,
                     self.s.zimbra_home.rstrip("/") + "/conf/my.cnf")

    @staticmethod
    def _ma(host: Host, account: str, check: bool = True, **attrs: str) -> None:
        """zmprov -l ma with every argument shell-quoted (an empty value removes the attr)."""
        args = " ".join(f"{k} {shlex.quote(str(v))}" for k, v in attrs.items())
        host.run(f"zmprov -l ma {shlex.quote(account)} {args}", check=check)

    def soap(self, name: str) -> AdminSoap:
        return AdminSoap(self.host(name), self.s.admin_port, self.s.admin_user, self.s.admin_password,
                         self.s.verify_tls, self.s.tls_ca)

    def server(self, name: str, refresh: bool = False) -> ServerInfo:
        if refresh or name not in self._servers:
            self._servers[name] = server_info(self.host(name), self.mysql(name))
        return self._servers[name]

    def opt(self, job: Job, key: str):
        v = job.options.get(key, DEFAULT_OPTIONS.get(key))
        if v is None and key in self.s.__dataclass_fields__:
            return getattr(self.s, key)
        return DEFAULT_OPTIONS.get(key) if v is None else v

    def _say(self, job: Job, msg: str, level: str = "info", **data) -> None:
        getattr(log, level if level in ("info", "warning", "error") else "info")("[%s %s] %s",
                                                                                   job.id, job.account, msg)
        self.state.event(job, level, msg, **data)
        self.progress(f"[{job.account}] {msg}")

    def _phase(self, job: Job, ph: Phase, **data) -> None:
        job.phase = ph
        job.data.update(data)
        self.state.beat(job)
        self.state.save(job)
        self._say(job, f"phase -> {ph.name}", **data)

    @staticmethod
    def _deadline_passed(job: Job) -> bool:
        dl = job.data.get("lock_deadline")
        return bool(dl and time.time() > dl)

    def _deadline_check(self, job: Job) -> None:
        if self._deadline_passed(job):
            raise LockTimeout(f"lock held longer than max_lock_seconds={self.opt(job, 'max_lock_seconds')}")

    @staticmethod
    def _bounded(job: Job, base: Optional[float]) -> Optional[float]:
        """Cap a per-command timeout by the remaining lock budget, so a single long rsync or
        DB statement cannot hold the user lock past max_lock_seconds (the watchdog is only
        checked between commands)."""
        dl = job.data.get("lock_deadline")
        if not dl:
            return base
        left = max(1.0, dl - time.time())
        return min(base, left) if base else left

    def _dst_claimed_elsewhere(self, job: Job) -> Optional[str]:
        """Reason why the destination copy under job's dst_mailbox_id must NOT be deleted by
        this job's cleanup (another job owns that id now, or the account lives there)."""
        dst_id = job.data.get("dst_mailbox_id")
        others = [o for o in self.state.find(account=job.account_id, server=job.dst, limit=1000)
                  if o.id != job.id]
        for o in others:
            if o.dst != job.dst or o.data.get("dst_mailbox_id") != dst_id:
                continue
            if o.status not in ("running", "failed", "done"):
                continue
            # a finished move whose copy was purged later (account moved away again) no
            # longer owns the id
            purged = any(p.src == job.dst and p.data.get("src_mailbox_id") == dst_id
                         and p.phase >= Phase.PURGED and p.created > o.created for p in others)
            if not purged:
                return f"mailbox id {dst_id} on {job.dst} belongs to job {o.id} ({o.status})"
        if job.phase < Phase.FLIPPED:
            try:
                home = account_info(self.host(job.dst), job.account).mail_host
            except Exception as e:  # noqa: BLE001 unknown -> be conservative
                return f"cannot verify zimbraMailHost of {job.account} ({e})"
            if home == job.dst:
                return f"{job.account} is homed on {job.dst}"
        return None

    def _cleanup_dst(self, job: Job, dbm: DbMove, plans: list[DirPlan], bs: BlobSync) -> None:
        """Delete this job's destination copy (live rows + blob/index dirs) and its stage,
        unless the id has since been claimed by another job or the account is homed there."""
        claimed = self._dst_claimed_elsewhere(job)
        if claimed:
            self._say(job, f"keeping destination mailbox {dbm.p.dst_id}: {claimed}", "warning")
        else:
            if mailbox_id_in_use(dbm.dst, dbm.p.dst_id):
                self._delete_dst_live(dbm, job)
            if mailbox_id_in_use(dbm.dst, dbm.p.dst_id) and not self.dry:
                self._say(job, f"mailbox id {dbm.p.dst_id} still in use on {job.dst}; "
                               "not removing its blob dirs", "warning")
            else:
                bs.remove_dirs(self.host(job.dst), [p.dst_dir for p in plans if not p.external])
        dbm.drop_stage()

    def _delete_dst_live(self, dbm: DbMove, job: Job) -> None:
        """Delete the destination live copy, but refuse if the id is owned by another account."""
        row = dbm.dst.query(f"SELECT account_id FROM zimbra.mailbox WHERE id={int(dbm.p.dst_id)}")
        if row and row[0] and row[0][0] != job.account_id:
            raise MoveError(f"refusing to delete destination mailbox {dbm.p.dst_id}: owned by another account")
        dbm.delete_live_dst()

    def _confirm(self, job: Job, step: str, detail: str) -> None:
        """Prompt gate for --confirm-steps. Declining raises: the job is saved at the last
        committed phase, `resume` re-asks the same step, `abort` rolls back."""
        if not self.opt(job, "confirm_steps"):
            return
        n = CONFIRM_STEPS.index(step) + 1
        dl = job.data.get("lock_deadline")
        budget = f" (lock budget left: {max(0, int(dl - time.time()))}s)" if dl else ""
        prompt = (f"[{job.account} {job.src}->{job.dst}] step {n}/{len(CONFIRM_STEPS)}: {step}{budget}\n"
                  f"  {detail}\nproceed? [y/N] ")
        if not self.confirm(prompt):
            raise StepDeclined(f"operator declined step '{step}'; job saved at {job.phase.name}, "
                               "resume to continue or abort to roll back")

    # ------------------------------------------------------------------ planning
    def new_job(self, account: str, src: str, dst: str, options: dict) -> Job:
        if src == dst:
            raise MoveError("source and destination are the same server")
        acct = account_info(self.host(dst), account)
        opts = {**DEFAULT_OPTIONS, **{k: v for k, v in options.items() if v is not None}}
        return self.state.create(acct.name, acct.id, src, dst, opts)

    def _resolve(self, mode: str, cfg_skip: bool) -> bool:
        """include|exclude|config -> True if the component must be COPIED."""
        if mode == "include":
            return True
        if mode == "exclude":
            return False
        return not cfg_skip

    def plan(self, job: Job) -> dict:
        """Preflight: every check NE does (src/dest safety, versions, peer state) plus capacity,
        schema, volume-layout and stale-cache checks. Read-only."""
        srcH, dstH = self.host(job.src), self.host(job.dst)
        srcI, dstI = self.server(job.src, True), self.server(job.dst, True)
        acct: AccountInfo = account_info(dstH, job.account)
        rep: dict = {"errors": [], "warnings": []}
        err, warn = rep["errors"].append, rep["warnings"].append

        if acct.mail_host != job.src and job.phase < Phase.FLIPPED:
            err(f"zimbraMailHost is {acct.mail_host}, not {job.src} (NE 'src' safety check)")
        for k in ("version", "db_version", "index_version"):
            if getattr(srcI, k) != getattr(dstI, k):
                err(f"{k} differs: {job.src}={getattr(srcI, k)} {job.dst}={getattr(dstI, k)}")
        for h in (srcH, dstH):
            if h.run("zmmailboxdctl status", check=False, mutating=False).rc != 0:
                err(f"mailboxd not running on {h.name}")
        for peer, pname in (("src", job.src), ("dst", job.dst)):
            try:
                moves = self.soap(pname).query_moves(job.account)
            except Exception:  # noqa: BLE001 auth/network errors surface at lock time
                moves = None
            if moves:
                warn(f"NE reports an active move for {job.account} on {peer} {pname}: {moves} "
                     "(zmove does not coordinate with it; wait or abort one side)")
        for pname in (job.src, job.dst):
            try:
                self.soap(pname).token()
            except Exception as e:  # noqa: BLE001 fail fast here, not after hours of presync
                err(f"admin SOAP auth failed on {pname}: {e}")
        smb = mailbox_row(self.mysql(job.src), acct.id)
        if not smb:
            err(f"no mailbox for {acct.name} on {job.src}")
            return rep
        dmb = mailbox_row(self.mysql(job.dst), acct.id, with_stats=False)
        if dmb and job.data.get("dst_mailbox_id") != dmb.id:
            if job.options.get("overwrite"):
                warn(f"destination already has mailbox id {dmb.id} for this account; will be removed")
            else:
                err(f"destination already has a mailbox (id {dmb.id}) for this account; use --overwrite")

        # stale JVM cache on destination: account lived there since mailboxd last started?
        # (moved away from it, or rolled back away from it). The JVM still maps accountId -> old
        # mailbox id and may hold the old Mailbox object, so a restart is required first.
        for old in self.state.find(account=acct.id, server=job.dst):
            if old.id == job.id:
                continue
            left_at = None
            if old.src == job.dst and old.phase >= Phase.FLIPPED and old.data.get("flipped_at"):
                left_at = old.data["flipped_at"]
            if old.dst == job.dst and old.data.get("rolled_back_at"):
                left_at = old.data["rolled_back_at"]
            if not left_at or job.options.get("force"):
                continue
            if dstI.mailboxd_started is None:
                err(f"{acct.name} was homed on {job.dst} before (job {old.id}) and mailboxd start time "
                    "is unknown; restart mailboxd there or use --force")
            elif left_at > dstI.mailboxd_started:
                err(f"{acct.name} left {job.dst} (job {old.id}) after its mailboxd started; restart "
                    f"mailboxd on {job.dst} first (stale accountId->mailboxId cache) or use --force")

        if (self.opt(job, "db_strategy") == "delta" and self.opt(job, "verify") != "full"
                and not self.opt(job, "force")):
            err("db_strategy=delta needs verify=full: rows changed without a new change number "
                "are only caught by the full fingerprint (use --verify full, --db-strategy full "
                "or --force)")
        copy_blobs = self._resolve(self.opt(job, "blobs"), dstI.move_skip_blobs)
        copy_sec = self._resolve(self.opt(job, "secondary_blobs"), dstI.move_skip_hsm_blobs)
        copy_idx = self._resolve(self.opt(job, "search_index"), dstI.move_skip_index)
        shared = dict(self.s.shared_volumes)
        src_msg = [v for v in srcI.volumes if v.is_message]
        for v in src_msg:
            wants_copy = copy_blobs and (copy_sec or v.type != TYPE_MESSAGE_SECONDARY)
            if not wants_copy and v.id not in shared:
                err(f"blobs on volume {v.id} excluded but no shared_volumes mapping for it "
                    "(exclude is only valid when the destination sees the same storage)")
        idx_vol = srcI.volume(smb.index_volume_id)
        for v in src_msg + [idx_vol]:
            if v.id in shared and v.store_type == STORE_INTERNAL and not (
                    srcI.blob_dir_server_id and dstI.blob_dir_server_id):
                err(f"shared_volumes maps internal volume {v.id}, but "
                    "zimbraConfiguredServerIDForBlobDirEnabled is not TRUE on both servers: "
                    "mailbox dirs of the two servers would collide on the shared storage")
        try:
            vmap = map_volumes(src_msg + [idx_vol], dstI.volumes, dstI.current_msg,
                               dstI.current_secondary, dstI.current_index, explicit=shared)
        except ValueError as e:
            err(str(e))
            return rep

        dst_id = job.data.get("dst_mailbox_id") or 0
        try:
            plans = self._dir_plans(srcI, dstI, smb.id, dst_id or 999999999, vmap, copy_idx, idx_vol.id)
        except MoveError as e:
            err(str(e))
            return rep
        bs = BlobSync(self.s, srcH, dstH)
        need: dict[int, int] = {}
        external_vols = sorted({s for p in plans if p.external for s in p.src_volumes})
        for p in plans:
            if p.shared or p.external:
                continue
            srcs = bs.existing_src_dirs(p)
            need[p.dst_volume] = need.get(p.dst_volume, 0) + bs.du(srcH, srcs)
        for vid, n in need.items():
            free = bs.free_bytes(dstH, dstI.volume(vid).path)
            if free is not None and free < n * 1.1 + (1 << 30):
                err(f"not enough space on {job.dst} volume {vid}: need ~{n * 1.1:.0f}B, free {free}B")
        # node-to-node SSH: rsync runs on the source and dials the destination as node_ssh_user
        r = srcH.run(f"ssh -p {int(self.s.node_ssh_port)} {self.s.node_ssh_options} "
                     f"{shlex.quote(self.s.node_ssh_user + '@' + job.dst)} true",
                     check=False, mutating=False)
        if r.rc != 0:
            err(f"node SSH {job.src} -> {self.s.node_ssh_user}@{job.dst}:{self.s.node_ssh_port} failed "
                "(rsync needs it); fix keys/known_hosts or node_ssh_* settings")

        dbm = DbMove(self._dbplan(job, acct, smb, srcI, dstI, dst_id or 1, vmap),
                     self.mysql(job.src), self.mysql(job.dst), self.dry)
        try:
            dbm.discover()
        except Exception as e:  # noqa: BLE001
            err(str(e))

        rep.update({
            "account": asdict(acct), "src_mailbox": asdict(smb), "dst_mailbox": asdict(dmb) if dmb else None,
            "copy": {"blobs": copy_blobs, "secondary_blobs": copy_sec, "search_index": copy_idx},
            "volume_map": vmap, "bytes_to_copy": sum(need.values()),
            "external_volumes": external_vols,
            "group_tables": sorted(dbm.p.group_tables), "zimbra_tables": sorted(dbm.p.zimbra_tables),
            "dst_group_exists": self.mysql(job.dst).schema_exists(dbm.p.dst_gdb) if dst_id else None,
        })
        return rep

    def _dir_plans(self, srcI: ServerInfo, dstI: ServerInfo, src_id: int, dst_id: int,
                   vmap: dict[int, int], copy_idx: bool, src_idx_vol: int) -> list[DirPlan]:
        # NOTE: blobs/index land directly under the *reserved* dst mailbox id (no TempDir
        # staging). Safe: the destination zimbra.mailbox row is INSERTed only at merge time,
        # so mailboxd has no mailbox object for dst_id and never serves the directory.
        shared = self.s.shared_volumes
        by_dst: dict[int, DirPlan] = {}
        for sv in srcI.volumes:
            if not sv.is_message or sv.id not in vmap:
                continue
            dv = dstI.volume(vmap[sv.id])
            external = sv.store_type != STORE_INTERNAL or dv.store_type != STORE_INTERNAL
            p = by_dst.setdefault(dv.id, DirPlan("msg", dv.id, dv.mailbox_dir(dst_id, dstI.path_prefix),
                                                 shared=sv.id in shared, external=external))
            if p.shared != (sv.id in shared):
                raise MoveError(f"volumes mapped to dst {dv.id} mix shared and non-shared storage")
            p.external = p.external or external
            p.src_dirs.append(sv.mailbox_dir(src_id, srcI.path_prefix))
            p.src_volumes.append(sv.id)
        plans = list(by_dst.values())
        if copy_idx:
            sv = srcI.volume(src_idx_vol)
            dv = dstI.volume(vmap[sv.id])
            plans.append(DirPlan("index", dv.id, dv.mailbox_dir(dst_id, dstI.path_prefix),
                                 [sv.mailbox_dir(src_id, srcI.path_prefix)], [sv.id],
                                 shared=sv.id in shared, external=False))
        for p in plans:
            if p.shared and not p.external and p.dst_dir.rstrip("/") in {d.rstrip("/") for d in p.src_dirs}:
                # rsync --delete / abort / purge on one shared directory would destroy live data
                raise MoveError(f"source and destination mailbox dir are the same on shared storage "
                                f"({p.dst_dir}); enable zimbraConfiguredServerIDForBlobDirEnabled")
        return plans

    def _dbplan(self, job, acct, smb, srcI, dstI, dst_id, vmap) -> DbPlan:
        return DbPlan(account=acct.name, account_id=acct.id, src_id=smb.id, src_group=smb.group_id,
                      dst_id=dst_id, dst_group=mailbox_group_id(dst_id, dstI.num_groups),
                      dst_index_volume=dstI.current_index, stage=f"zmove_stage_{job.id}",
                      vol_map={k: v for k, v in vmap.items()},
                      exclude_tables=list(self.opt(job, "exclude_tables") or []))

    # ------------------------------------------------------------------ context rebuild
    def _ctx(self, job: Job, prepare: bool = True):
        """Rebuild DB/blob plans from the journal. prepare=True also makes sure the destination
        group schema and the stage exist (needed to copy); cleanup paths pass False."""
        srcI, dstI = self.server(job.src), self.server(job.dst)
        d = job.data
        vmap = {int(k): int(v) for k, v in d["volume_map"].items()}
        smb = type("MB", (), {"id": d["src_mailbox_id"], "group_id": d["src_group"],
                              "index_volume_id": d["src_index_volume"]})
        acct = type("A", (), {"name": job.account, "id": job.account_id})
        dbm = DbMove(self._dbplan(job, acct, smb, srcI, dstI, d["dst_mailbox_id"], vmap),
                     self.mysql(job.src), self.mysql(job.dst), self.dry)
        dbm.discover()
        if prepare:
            # resume after a crash between _reserve and _ctx (or before create_stage):
            # the destination group DB and stage may not exist yet
            dbm.ensure_dst_group_db()
            dbm.create_stage()
        plans = self._dir_plans(srcI, dstI, d["src_mailbox_id"], d["dst_mailbox_id"], vmap,
                                d["copy"]["search_index"], d["src_index_volume"])
        return dbm, plans, BlobSync(self.s, self.host(job.src), self.host(job.dst))

    def _rsync_timeout(self, job: Job) -> Optional[float]:
        t = int(self.opt(job, "rsync_timeout") or 0)
        return float(t) if t > 0 else None

    def _db_timeout(self, job: Job) -> Optional[float]:
        t = int(self.opt(job, "db_timeout") or 0)
        return float(t) if t > 0 else None

    # ------------------------------------------------------------------ main entry
    def _check_runnable(self, job: Job) -> None:
        """Refuse to (re)drive a job whose state makes another pass unsafe."""
        if job.data.get("rolled_back_at"):
            raise MoveError(f"job {job.id} was rolled back; start a new move instead")
        if job.phase >= Phase.PURGED:
            raise MoveError(f"job {job.id} is purged; nothing to resume")
        if job.status == "done":
            raise MoveError(f"job {job.id} is already done")
        if job.status == "aborted" and job.phase >= Phase.FLIPPED:
            raise MoveError(f"job {job.id} was aborted after cutover; start a new move instead")
        if self.state.owner_alive(job) and not self.opt(job, "force"):
            age = int(time.time() - (job.heartbeat or 0))
            raise MoveError(f"job {job.id} is still being run by {job.owner} (heartbeat {age}s ago); "
                            "wait for it, or use --force if that process is really gone")
        if job.status == "aborted":
            other = self.state.active_for(job.account_id)
            if other and other.id != job.id:
                raise AlreadyMoving(f"ALREADY_BEING_MOVED: {job.account} has active job {other.id}")

    def run(self, job: Job) -> Job:
        self._check_runnable(job)
        job.status, job.error, job.owner = "running", None, State.owner_tag()
        try:
            self.state.save(job)
        except sqlite3.IntegrityError as e:
            raise AlreadyMoving(f"ALREADY_BEING_MOVED: {job.account} has another active job") from e
        hb = self.state.start_heartbeat(job.id)
        try:
            return self._run(job)
        finally:
            hb.set()

    def _run(self, job: Job) -> Job:
        try:
            if job.phase < Phase.PREFLIGHT_OK:
                self._preflight(job)
            if job.phase < Phase.RESERVED:
                self._reserve(job)
            dbm, plans, bs = self._ctx(job, prepare=job.phase < Phase.FLIPPED)
            if job.phase < Phase.FLIPPED:
                fresh = job.phase < Phase.LOCKED
                if fresh:
                    self._online_phase(job, dbm, plans, bs)
                sem = self.lock_semaphore
                if sem:
                    sem.acquire()
                try:
                    if fresh:
                        self._lock(job)
                    else:
                        self._relock(job)
                    if job.phase < Phase.FLIPPED:
                        self._locked_phase(job, dbm, plans, bs)
                finally:
                    if sem:
                        sem.release()
            self._post_flip(job, dbm)
        except LockTimeout as e:
            self._on_lock_timeout(job, e)
            raise
        except subprocess.TimeoutExpired as e:
            if job.phase < Phase.FLIPPED and self._deadline_passed(job):
                lt = LockTimeout(f"lock budget exhausted during a long command ({e.timeout:.0f}s cap)")
                self._on_lock_timeout(job, lt)
                raise lt from e
            self._fail(job, e)
            raise
        except StepDeclined as e:
            job.error = str(e)
            job.status = "aborted" if job.phase < Phase.RESERVED else "failed"
            self.state.save(job)
            self._say(job, f"PAUSED in {job.phase.name}: {e}", "warning")
            if Phase.LOCKED <= job.phase < Phase.FLIPPED:
                self._say(job, "account is still LOCKED on source; run `zmove resume` or "
                               "`zmove abort` now", "warning")
            raise
        except Exception as e:  # noqa: BLE001
            self._fail(job, e)
            raise
        return job

    def _fail(self, job: Job, e: BaseException) -> None:
        job.error = f"{type(e).__name__}: {e}"
        # nothing was changed before RESERVED -> release the account immediately
        job.status = "aborted" if job.phase < Phase.RESERVED else "failed"
        self.state.save(job)
        self._say(job, f"FAILED in {job.phase.name}: {job.error}", "error")
        if Phase.LOCKED <= job.phase < Phase.FLIPPED or job.data.get("locking"):
            self._say(job, "account is still LOCKED on source; run `zmove resume` or "
                           "`zmove abort` now", "error")
        elif Phase.FLIPPED <= job.phase < Phase.UNLOCKED:
            self._say(job, f"account is in maintenance on {job.dst}; run `zmove resume` to finish "
                           "the cutover", "error")

    def _on_lock_timeout(self, job: Job, e: Exception) -> None:
        self._say(job, f"watchdog: {e}; aborting and unlocking", "error")
        job.error = str(e)
        try:
            self.abort(job, reason="lock watchdog")
        except Exception as ae:  # noqa: BLE001 keep the original timeout as the raised error
            self._say(job, f"watchdog abort incomplete: {ae}", "error")

    def _preflight(self, job: Job) -> None:
        for t in self.opt(job, "exclude_tables") or []:
            if t in DELTA_TABLES:
                raise MoveError(f"cannot exclude delta table {t!r}: rollback and verification depend on it")
        rep = self.plan(job)
        for w in rep["warnings"]:
            self._say(job, f"warning: {w}", "warning")
        if rep["errors"]:
            raise MoveError("preflight failed:\n  - " + "\n  - ".join(rep["errors"]))
        smb = rep["src_mailbox"]
        self._phase(job, Phase.PREFLIGHT_OK, volume_map=rep["volume_map"], copy=rep["copy"],
                    src_mailbox_id=smb["id"], src_group=smb["group_id"],
                    src_index_volume=smb["index_volume_id"], items=smb["items"], size=smb["size"],
                    orig_status=rep["account"]["status"], orig_transport=rep["account"]["mail_transport"],
                    stray_dst_mailbox=(rep["dst_mailbox"] or {}).get("id"))

    def _reserve(self, job: Job) -> None:
        stray = job.data.get("stray_dst_mailbox")
        self._confirm(job, "reserve",
                      f"allocate a mailbox id on {job.dst} + create stage schema zmove_stage_{job.id}" +
                      (f"; DELETE stray mailbox {stray} on {job.dst}"
                       if stray and job.options.get("overwrite") else ""))
        dst = self.mysql(job.dst)
        if stray and job.options.get("overwrite"):
            row = dst.query(f"SELECT account_id FROM zimbra.mailbox WHERE id={int(stray)}")
            if not row or not row[0] or row[0][0] != job.account_id:
                raise MoveError(f"refusing to remove stray destination mailbox {stray}: not owned by {job.account}")
            dstI = self.server(job.dst)
            gdb = f"mboxgroup{mailbox_group_id(stray, dstI.num_groups)}"
            gt = dst.tables_with_column(gdb, "mailbox_id")
            zt = [t for t in dst.tables_with_column("zimbra", "mailbox_id") if t in ZIMBRA_MAILBOX_TABLES]
            dst.execute(DbMove.delete_mailbox_sql(stray, gdb, gt, zt))
            self._say(job, f"removed stray destination mailbox {stray}", "warning")
        explicit = None
        rec = self.state.purge_record(job.account_id, job.dst)
        if rec and not mailbox_id_in_use(dst, rec[0]):
            explicit = rec[0]      # reuse: keeps any stale JVM accountId->id mapping consistent
        dst_id = reserve_mailbox_id(dst, explicit) if not self.dry else (explicit or 999999)
        dstI = self.server(job.dst)
        # group schema + stage are created by run() -> _ctx(prepare=True) right after this
        self._phase(job, Phase.RESERVED, dst_mailbox_id=dst_id,
                    dst_group=mailbox_group_id(dst_id, dstI.num_groups))

    def _online_phase(self, job: Job, dbm: DbMove, plans: list[DirPlan], bs: BlobSync) -> None:
        self._confirm(job, "online-sync",
                      f"rsync {len(plans)} blob/index dirs + DB prestage ({self.opt(job, 'db_strategy')}); "
                      "account stays online")
        maxs = int(self.opt(job, "max_syncs"))
        thr = int(self.opt(job, "sync_finish_threshold_ms"))

        def on_pass(i: int, st: SyncStats):
            self.state.beat(job)
            self._say(job, f"presync pass {i}: {st.files} files, {st.bytes} B, {st.seconds:.1f}s",
                      **asdict(st))

        passes = bs.converge(plans, maxs, thr, on_pass, timeout=self._rsync_timeout(job))
        self._phase(job, Phase.PRESYNC, presync_passes=len(passes),
                    last_pass_seconds=round(passes[-1].seconds, 2) if passes else 0)
        if self.opt(job, "db_strategy") == "delta":
            info = dbm.prestage(timeout=self._db_timeout(job))
            self._phase(job, Phase.PRESTAGED, watermark=info["watermark"], prestage=info)
        else:
            self._phase(job, Phase.PRESTAGED, watermark=None)

        window = self.opt(job, "cutover_window")
        if window and not in_window(window):
            self._say(job, f"waiting for cutover window {window} (keeping replicas warm)")
            last = time.time()
            while not in_window(window):
                time.sleep(30)
                self.state.beat(job)
                if time.time() - last > 1800:          # re-sync every 30 min while waiting
                    bs.sync_once(plans, timeout=self._rsync_timeout(job))
                    last = time.time()
            bs.converge(plans, maxs, thr, on_pass, timeout=self._rsync_timeout(job))

    def _check_home(self, job: Job) -> None:
        current_home = account_info(self.host(job.dst), job.account).mail_host
        if current_home != job.src:
            raise MoveError(f"zimbraMailHost is now {current_home}, not {job.src}; refusing to lock "
                            "(re-run preflight)")

    def _start_lock_budget(self, job: Job) -> Optional[float]:
        mx = int(self.opt(job, "max_lock_seconds") or 0)
        return (time.time() + mx) if mx else None

    def _lock(self, job: Job) -> None:
        self._confirm(job, "lock",
                      f"set {job.account} maintenance + LockoutMailbox(start) on {job.src}; "
                      "user-visible outage starts")
        self._check_home(job)
        # journal the intent first: a crash between the LDAP change and the LOCKED commit must
        # still make abort/unlock restore the source
        job.data["locking"] = True
        self.state.save(job)
        any_h = self.host(job.dst)
        self._ma(any_h, job.account, zimbraAccountStatus="maintenance")
        caches.flush_account_cache(any_h, job.account)
        if not self.opt(job, "skip_lockout"):
            self._lockout(job.src, job.account, "start")
        job.data.pop("locking", None)
        self._phase(job, Phase.LOCKED, locked_at=time.time(), lock_deadline=self._start_lock_budget(job))

    def _relock(self, job: Job) -> None:
        """Resume while locked: the lockout lives in source JVM memory and is lost on a mailboxd
        restart, and the account status may have been touched, so re-assert both and give the
        operator a fresh watchdog budget."""
        if job.phase == Phase.MERGED:
            home = account_info(self.host(job.dst), job.account).mail_host
            if home == job.dst:
                # crashed after the LDAP flip but before FLIPPED was journaled: the cutover
                # already happened, just record it and continue with post-flip
                self._say(job, f"zimbraMailHost already {job.dst}: committing FLIPPED", "warning")
                self._phase(job, Phase.FLIPPED, flipped_at=time.time(),
                            new_transport=replace_host_in_transport(job.data.get("orig_transport") or "",
                                                                    job.dst))
                return
        self._confirm(job, "lock", f"re-assert maintenance + LockoutMailbox(start) on {job.src} "
                                   "(resuming a locked job)")
        self._check_home(job)
        any_h = self.host(job.dst)
        self._ma(any_h, job.account, zimbraAccountStatus="maintenance")
        caches.flush_account_cache(any_h, job.account)
        if not self.opt(job, "skip_lockout"):
            self._lockout(job.src, job.account, "start")
        job.data["lock_deadline"] = self._start_lock_budget(job)
        job.data.setdefault("locked_at", time.time())
        self.state.save(job)
        # FINAL_SYNCED/DB_FINAL/VERIFIED/MERGED were computed against a source that may have
        # been unlocked in between: re-run everything from the lock point.
        if job.phase > Phase.LOCKED:
            self._phase(job, Phase.LOCKED)

    def _lockout(self, server: str, account: str, op: str) -> None:
        try:
            self.soap(server).lockout(account, op)
        except SoapFault as e:
            benign = (op == "start" and "MAINTENANCE" in e.code.upper()) or \
                     (op == "end" and e.code in ("service.NOT_FOUND", "account.NO_SUCH_ACCOUNT"))
            if not benign:
                raise

    def _locked_phase(self, job: Job, dbm: DbMove, plans: list[DirPlan], bs: BlobSync) -> None:
        verify = self.opt(job, "verify")
        dbt = lambda: self._bounded(job, self._db_timeout(job))  # noqa: E731
        if job.phase < Phase.FINAL_SYNCED:
            self._confirm(job, "final-sync", "final rsync --checksum while locked")
            st = bs.sync_once(plans, final=True, timeout=self._bounded(job, self._rsync_timeout(job)))
            self._deadline_check(job)
            self._phase(job, Phase.FINAL_SYNCED, final_sync=asdict(st))
        if job.phase < Phase.VERIFIED:
            info = dbm.final(self.opt(job, "db_strategy"), job.data.get("watermark"), timeout=dbt())
            self._phase(job, Phase.DB_FINAL, db_final=info)
            self._deadline_check(job)
            diff = dbm.compare("src", "stage", verify, timeout=dbt())
            if diff and info.get("mode") == "delta":
                self._say(job, "delta verification mismatch -> full re-copy of item tables",
                          "warning", diff=diff)
                dbm.refull_delta_tables(timeout=dbt())
                diff = dbm.compare("src", "stage", verify, timeout=dbt())
            if diff:
                raise VerifyError("stage != source: " + "; ".join(diff))
            # blob inventory (same file count & bytes on both sides; externals have no tree)
            for p in plans:
                if p.external:
                    continue
                srcs = bs.existing_src_dirs(p)
                a = bs.inventory(self.host(job.dst) if p.shared else self.host(job.src), srcs)
                b = bs.inventory(self.host(job.dst), [p.dst_dir])
                if a != b and not self.dry:
                    raise VerifyError(f"{p.kind} inventory differs for {p.dst_dir}: src={a} dst={b}")
            self._deadline_check(job)
            self._phase(job, Phase.VERIFIED)
        if job.phase < Phase.MERGED:
            self._confirm(job, "merge",
                          f"insert stage into live tables on {job.dst} as mailbox {dbm.p.dst_id}")
            if mailbox_id_in_use(dbm.dst, dbm.p.dst_id):     # resumed after a committed merge
                self._delete_dst_live(dbm, job)
            dbm.merge(timeout=dbt())
            diff = dbm.compare("src_mapped", "live", verify, timeout=dbt())
            if diff:
                dbm.delete_live_dst()
                raise VerifyError("live != source after merge (rolled back): " + "; ".join(diff))
            self._deadline_check(job)
            state = dbm.live_state() if not self.dry else [0, 0]
            self._phase(job, Phase.MERGED, merged_max_change=state[0], merged_state=state)
        self._flip(job)

    def _flip(self, job: Job) -> None:
        self._confirm(job, "flip", f"set zimbraMailHost={job.dst} -- POINT OF NO RETURN")
        self._deadline_check(job)
        h = self.host(job.dst)
        orig_transport = job.data.get("orig_transport") or ""
        transport = replace_host_in_transport(orig_transport, job.dst)
        if orig_transport and transport == orig_transport:
            self._say(job, f"keeping non-LMTP mailTransport {orig_transport!r}; verify it routes to {job.dst}", "warning")
        self._ma(h, job.account, zimbraMailHost=job.dst, zimbraMailTransport=transport)
        self._phase(job, Phase.FLIPPED, flipped_at=time.time(), new_transport=transport)

    def _post_flip(self, job: Job, dbm: DbMove) -> None:
        if job.phase >= Phase.DONE:
            return
        self._confirm(job, "post-flip", "purge caches, restore account status, drop stage")
        h = self.host(job.dst)
        if job.phase < Phase.CACHES_PURGED:
            caches.flush_account_cache(h, job.account)
            if not self.opt(job, "skip_memcache_purge"):
                caches.purge_proxy_routes(h, job.account)
                n = caches.delete_memcached_keys(h, job.account_id)
                self._say(job, f"memcached account keys deleted: {n}")
            self._phase(job, Phase.CACHES_PURGED)
        if job.phase < Phase.UNLOCKED:
            # Source mailbox intentionally STAYS in maintenance lockout until purge/rollback:
            # nothing on the source JVM may touch the old copy any more.
            self._ma(h, job.account, zimbraAccountStatus=job.data.get("orig_status") or "active")
            caches.flush_account_cache(h, job.account)
            locked = time.time() - job.data.get("locked_at", time.time())
            self._phase(job, Phase.UNLOCKED, lock_seconds=round(locked, 2))
        if job.phase < Phase.DONE:
            if not job.data["copy"]["search_index"] or self.opt(job, "reindex_after"):
                h.run(f"zmprov rim {shlex.quote(job.account)} start", check=False)
                self._say(job, "re-index started on destination")
            if self.opt(job, "flush_mta"):
                mta = list(self.s.mta_hosts) or mta_servers(h)
                if mta:
                    caches.flush_mta_queues([self.host(m) for m in mta], job.account, self.s.zimbra_home)
                else:
                    self._say(job, "no MTA hosts configured or discovered; deferred mail will "
                                   "retry on postfix schedule", "warning")
            if not self.opt(job, "keep_stage"):
                dbm.drop_stage()
            job.status = "done"
            if self.dry:
                job.data["dry_run"] = True
            self._phase(job, Phase.DONE)
            self._say(job, f"move complete; user-visible lock {job.data.get('lock_seconds')}s; "
                           f"run `zmove purge` later to reclaim {job.src}")
            self._metrics(job)

    # ------------------------------------------------------------------ abort / rollback
    def _restore_source(self, job: Job) -> list[str]:
        """End the source lockout and put the original account status back. Returns errors."""
        errors: list[str] = []
        if not self.opt(job, "skip_lockout"):
            try:
                self._lockout(job.src, job.account, "end")
            except Exception as e:  # noqa: BLE001
                errors.append(f"LockoutMailbox(end) on {job.src}: {e}")
        status = job.data.get("orig_status") or "active"
        for hn in dict.fromkeys((job.src, job.dst)):   # zmprov -l works from any mailstore
            try:
                h = self.host(hn)
                self._ma(h, job.account, zimbraAccountStatus=status)
                caches.flush_account_cache(h, job.account)
                break
            except Exception as e:  # noqa: BLE001
                errors.append(f"restore zimbraAccountStatus={status} via {hn}: {e}")
        else:
            return errors
        # status restored: earlier per-host failures for it no longer matter
        return [e for e in errors if not e.startswith("restore zimbraAccountStatus")]

    def abort(self, job: Job, reason: str = "manual") -> Job:
        if job.phase >= Phase.FLIPPED:
            raise MoveError("job is past the point of no return (FLIPPED); use `zmove rollback`")
        if job.status == "running" and self.state.owner_alive(job):
            raise MoveError(f"job {job.id} is still being run by {job.owner}; stop it first")
        if job.phase >= Phase.RESERVED:
            try:
                dbm, plans, bs = self._ctx(job, prepare=False)
            except Exception as e:  # noqa: BLE001
                self._say(job, f"abort: cannot rebuild context ({e}); dropping stage only, "
                               "run `zmove gc` later for destination leftovers", "warning")
                try:
                    self.mysql(job.dst).execute(f"DROP DATABASE IF EXISTS `zmove_stage_{job.id}`;")
                except Exception as e2:  # noqa: BLE001
                    self._say(job, f"abort: stage drop error {e2}", "warning")
            else:
                try:
                    self._cleanup_dst(job, dbm, plans, bs)
                except Exception as e:  # noqa: BLE001
                    self._say(job, f"abort: destination cleanup error {e}", "warning")
        # ALWAYS give the source back if this job ever (started to) lock it, even when the
        # destination is unreachable (the usual reason a watchdog fires).
        errors: list[str] = []
        if job.phase >= Phase.LOCKED or job.data.get("locking"):
            errors = self._restore_source(job)
        job.error = (job.error or "") + f" [aborted: {reason}]"
        # Destination copies and the stage are gone: a later resume must re-copy from the
        # online phase and must not inherit the old lock budget.
        if job.phase > Phase.RESERVED:
            job.phase = Phase.RESERVED
        for k in ("watermark", "prestage", "merged_max_change", "merged_state", "lock_deadline"):
            job.data.pop(k, None)
        if errors:
            job.status = "failed"       # keeps the account registered: retry abort or unlock
            job.data["locking"] = True
            job.error += " [source NOT restored: " + "; ".join(errors) + "]"
            self.state.save(job)
            self._say(job, f"abort incomplete, source still locked: {'; '.join(errors)}", "error")
            raise MoveError(f"abort could not restore {job.account} on {job.src}: "
                            + "; ".join(errors) + f"; fix and run `zmove abort {job.id}` or "
                            f"`zmove unlock {job.account} -s <server>`")
        job.data.pop("locking", None)
        job.data.pop("locked_at", None)
        job.status = "aborted"
        self.state.save(job)
        self._say(job, f"aborted ({reason}); source restored", "warning")
        return job

    def gc_job(self, job: Job) -> str:
        """Clean leftovers of one failed/aborted job. Returns what was done."""
        if job.phase >= Phase.FLIPPED:
            return "skipped (past cutover)"
        if job.status == "failed":
            self.abort(job, reason="gc")
            return "aborted"
        if job.data.get("gc_at"):
            return "skipped (already cleaned)"
        if job.phase >= Phase.RESERVED and job.data.get("dst_mailbox_id"):
            dbm, plans, bs = self._ctx(job, prepare=False)
            self._cleanup_dst(job, dbm, plans, bs)
        else:
            self.mysql(job.dst).execute(f"DROP DATABASE IF EXISTS `zmove_stage_{job.id}`;")
        if not self.dry:
            job.data["gc_at"] = time.time()
            self.state.save(job)
        return "cleaned"

    def rollback(self, job: Job, force: bool = False) -> Job:
        """Flip back to the untouched source copy. Only allowed if the destination copy has not
        changed since the merge (no deliveries, no user edits) unless --force (loses those)."""
        if job.data.get("dry_run"):
            raise MoveError(f"job {job.id} is a dry run; nothing was moved")
        if job.data.get("rolled_back_at"):
            raise MoveError(f"job {job.id} is already rolled back")
        if job.phase < Phase.FLIPPED:
            return self.abort(job, reason="rollback before flip")
        if job.phase >= Phase.PURGED:
            raise MoveError("source copy already purged; do a reverse move instead")
        if self.state.owner_alive(job):
            raise MoveError(f"job {job.id} is still being run by {job.owner}; stop it first")
        dbm, plans, bs = self._ctx(job, prepare=False)
        h = self.host(job.dst)
        orig_status = job.data.get("orig_status") or "active"
        self._ma(h, job.account, zimbraAccountStatus="maintenance")
        caches.flush_account_cache(h, job.account)
        cur = dbm.live_state()
        merged = job.data.get("merged_state")
        changed = (cur != merged) if merged else (cur[0] != job.data.get("merged_max_change"))
        if changed and not force:
            self._ma(h, job.account, zimbraAccountStatus=orig_status)
            caches.flush_account_cache(h, job.account)
            raise MoveError(f"destination mailbox changed since cutover ([max change, rows] {cur} vs "
                            f"{merged or job.data.get('merged_max_change')}); do a reverse move or --force")
        self._ma(h, job.account, zimbraMailHost=job.src,
                 zimbraMailTransport=job.data.get("orig_transport") or "")
        caches.purge_proxy_routes(h, job.account)
        caches.delete_memcached_keys(h, job.account_id)
        if not self.opt(job, "skip_lockout"):
            self._lockout(job.src, job.account, "end")
        self._ma(h, job.account, zimbraAccountStatus=orig_status)
        caches.flush_account_cache(h, job.account)
        job.data["rolled_back_at"] = time.time()
        job.status = "aborted"
        job.error = "rolled back after cutover"
        self.state.save(job)
        try:
            self._cleanup_dst(job, dbm, plans, bs)
        except Exception as e:  # noqa: BLE001 the user is already back on the source
            self._say(job, f"rollback: destination cleanup error {e}", "warning")
        self._say(job, "rolled back: account served from source again; restart mailboxd on "
                       f"{job.dst} before moving this account there again", "warning")
        return job

    # ------------------------------------------------------------------ purge (zmpurgeoldmbox)
    def purge(self, job: Job, keep_blobs: bool = False) -> Job:
        if job.phase < Phase.DONE or job.status != "done":
            raise MoveError(f"job {job.id} is not a finished move (phase={job.phase.name}, "
                            f"status={job.status})")
        if job.data.get("rolled_back_at"):
            raise MoveError(f"job {job.id} was rolled back; its source copy is live")
        if job.data.get("dry_run"):
            raise MoveError(f"job {job.id} is a dry run; refusing to purge a live mailbox")
        acct = account_info(self.host(job.dst), job.account)
        if acct.mail_host == job.src:
            raise MoveError(f"{job.account} is homed on {job.src} again; refusing to purge it")
        # a later move of the same account that has not finished may still need this copy
        for o in self.state.find(account=job.account_id, limit=1000):
            if o.id != job.id and o.created > job.created and o.status in ("running", "failed"):
                raise MoveError(f"{job.account} has an unfinished later move (job {o.id}); "
                                "finish or abort it first")
        srcI = self.server(job.src, True)
        mid, gid = job.data["src_mailbox_id"], job.data["src_group"]
        src = self.mysql(job.src)
        cur = mailbox_row(src, job.account_id, with_stats=False)
        if not cur or cur.id != mid:
            raise MoveError(f"mailbox {mid} for {job.account} not found on {job.src} (already purged?)")
        gdb = f"mboxgroup{gid}"
        gt = src.tables_with_column(gdb, "mailbox_id")
        zt = [t for t in src.tables_with_column("zimbra", "mailbox_id") if t in ZIMBRA_MAILBOX_TABLES]
        src.execute(DbMove.delete_mailbox_sql(mid, gdb, gt, zt))
        if not keep_blobs:
            # on shared storage the destination holds hard links in its own (server-id
            # prefixed) dir, so unlinking the old dir is safe
            dirs = [v.mailbox_dir(mid, srcI.path_prefix) for v in srcI.volumes
                    if (v.is_message or v.id == job.data["src_index_volume"])
                    and v.store_type == STORE_INTERNAL]
            BlobSync(self.s, self.host(job.src), self.host(job.src)).remove_dirs(self.host(job.src), dirs)
        self.state.record_purge(job.account_id, job.src, mid)
        job.phase = Phase.PURGED
        job.data["purged_at"] = time.time()
        self.state.save(job)
        self._say(job, f"purged old mailbox {mid} on {job.src}")
        return job

    # ------------------------------------------------------------------ unlock (zmprov ulm)
    def unlock(self, account: str, server: Optional[str] = None) -> None:
        if not server:
            raise MoveError("--server is required")
        any_h = self.host(server)
        acct = account_info(any_h, account)
        j = self.state.active_for(acct.id)
        if j and j.phase < Phase.FLIPPED:
            # full abort: also removes a merged-but-not-flipped destination copy
            try:
                self.abort(j, reason="unlocked manually")
            except Exception as e:  # noqa: BLE001 fall through to the manual restore below
                log.warning("unlock: abort of job %s failed: %s", j.id, e)
        last = j or next(iter(self.state.find(account=acct.id, limit=1)), None)
        status = (last.data.get("orig_status") if last else None) or "active"
        self._ma(any_h, account, zimbraAccountStatus=status)
        caches.flush_account_cache(any_h, account)
        # after cutover the old copy on the source keeps its lockout on purpose; only the
        # current home is unlocked here
        self._lockout(acct.mail_host, account, "end")

    # ------------------------------------------------------------------ metrics
    @staticmethod
    def _label(v: str) -> str:
        return str(v).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

    def _metrics(self, job: Job) -> None:
        self.state.prune_events(max_age_days=30)
        path = self.s.metrics_textfile
        if not path:
            return
        lines = []
        for j in self.state.find(status="done", limit=500):
            lab = ",".join(f'{k}="{self._label(v)}"' for k, v in
                           (("account", j.account), ("src", j.src), ("dst", j.dst), ("job", j.id)))
            d = j.data
            lines.append(f"zmove_lock_seconds{{{lab}}} {d.get('lock_seconds', 0)}")
            lines.append(f"zmove_items{{{lab}}} {d.get('items', 0)}")
            lines.append(f"zmove_bytes{{{lab}}} {d.get('size', 0)}")
            pre = d.get("prestage") or {}
            dbf = d.get("db_final") or {}
            fins = d.get("final_sync") or {}
            lines.append(f"zmove_db_prestage_seconds{{{lab}}} {pre.get('seconds', 0)}")
            lines.append(f"zmove_db_final_seconds{{{lab}}} {dbf.get('seconds', 0)}")
            lines.append(f"zmove_blob_final_bytes{{{lab}}} {fins.get('bytes', 0)}")
            lines.append(f"zmove_presync_passes{{{lab}}} {d.get('presync_passes', 0)}")
        try:
            header = ("\n".join(f"# TYPE {m} gauge" for m in (
                "zmove_lock_seconds", "zmove_items", "zmove_bytes",
                "zmove_db_prestage_seconds", "zmove_db_final_seconds", "zmove_blob_final_bytes",
                "zmove_presync_passes",)))
            with open(path + ".tmp", "w") as f:
                f.write(header + "\n" + "\n".join(lines) + "\n")
            os.replace(path + ".tmp", path)
        except OSError as e:
            log.warning("metrics write failed: %s", e)
