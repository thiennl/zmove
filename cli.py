"""Command line interface.

NE command        -> zmove equivalent
  zmmboxmove        zmove move ACCOUNT --from SRC --to DST [...]
  zmmboxmovequery   zmove query [ACCOUNT] [--server S] [--check-peer]
  zmpurgeoldmbox    zmove purge ACCOUNT [--server SRC]
  zmprov ulm        zmove unlock ACCOUNT --server S
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import subprocess
import sys

from . import __version__
from .batch import Batch, read_plan
from .config import load
from .engine import Engine, MoveError
from .state import Job, Phase, State
from .topology import account_info

log = logging.getLogger("zmove")


def _setup_logging(s, verbose: bool) -> None:
    os.makedirs(s.log_dir, exist_ok=True)
    fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    handlers = [logging.StreamHandler(sys.stderr),
                RotatingFileHandler(os.path.join(s.log_dir, "zmove.log"), maxBytes=10 << 20,
                                    backupCount=5)]
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, format=fmt, handlers=handlers)


def _move_opts(a) -> dict:
    if getattr(a, "ng_migration", False):
        raise MoveError("ngMigration is an NE-only contract flag with no documented semantics; "
                        "zmove refuses it rather than silently ignoring it")
    if getattr(a, "confirm_steps", False) and getattr(a, "yes", False):
        raise MoveError("--confirm-steps and --yes conflict: drop one")
    return {
        "max_syncs": a.max_syncs, "sync_finish_threshold_ms": a.sync_finish_threshold,
        "blobs": a.blobs, "secondary_blobs": a.secondary_blobs, "search_index": a.search_index,
        "skip_lockout": a.skip_lockout or None, "skip_memcache_purge": a.skip_memcache_purge or None,
        "overwrite": a.overwrite or None, "db_strategy": a.db_strategy, "verify": a.verify,
        "flush_mta": False if a.no_flush_mta else None,
        "cutover_window": a.cutover_window, "max_lock_seconds": a.max_lock_seconds,
        "rsync_timeout": a.rsync_timeout, "db_timeout": a.db_timeout,
        "exclude_tables": a.exclude_tables.split(",") if a.exclude_tables else None,
        "reindex_after": a.reindex_after or None, "force": a.force or None,
        "keep_stage": a.keep_stage or None,
        "confirm_steps": a.confirm_steps or None,
    }


def _add_move_args(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("zmmboxmove-compatible")
    g.add_argument("--max-syncs", type=int, help="max unlocked sync passes (NE default 10)")
    g.add_argument("--sync-finish-threshold", type=int, metavar="MS",
                   help="stop unlocked passes when one takes <= MS (NE default 30s)")
    g.add_argument("--sync", action="store_true",
                   help="NE compat no-op: zmove always runs synchronously and waits for completion")
    for name in ("blobs", "secondary-blobs", "search-index"):
        g.add_argument(f"--{name}", choices=("include", "exclude", "config"), default=None,
                       help=f"{name}: include|exclude|config (config = zimbraMailboxMoveSkip* attrs)")
    g.add_argument("--skip-lockout", action="store_true", help="NE skipRemoteLockout")
    g.add_argument("--skip-memcache-purge", action="store_true", help="NE skipMemcachePurge")
    g.add_argument("--overwrite", action="store_true", help="remove a stray mailbox on destination")
    g.add_argument("--ng-migration", action="store_true",
                   help="NE-only flag: rejected (no documented semantics, refusing beats ignoring)")
    x = p.add_argument_group("zmove extensions")
    x.add_argument("--db-strategy", choices=("delta", "full"), help="delta = pre-stage DB online")
    x.add_argument("--verify", choices=("full", "count", "none"))
    x.add_argument("--cutover-window", metavar="HH:MM-HH:MM", help="lock only inside this window")
    x.add_argument("--max-lock-seconds", type=int, help="watchdog: abort+unlock if lock exceeds")
    x.add_argument("--rsync-timeout", type=int, metavar="S", default=None,
                   help="timeout per rsync pass, 0 = no timeout")
    x.add_argument("--db-timeout", type=int, metavar="S", default=None,
                   help="timeout per DB copy/compare batch, 0 = no timeout")
    x.add_argument("--exclude-tables", help="comma list of mboxgroup tables to skip (e.g. event)")
    x.add_argument("--reindex-after", action="store_true", help="re-index on destination after move")
    x.add_argument("--no-flush-mta", action="store_true",
                   help="do not reschedule this account's deferred mail on the MTAs")
    x.add_argument("--background", action="store_true", help="detach and run asynchronously")
    x.add_argument("--force", action="store_true", help="override stale-cache safety check")
    x.add_argument("--keep-stage", action="store_true", help="keep staging schema for forensics")
    x.add_argument("--confirm-steps", action="store_true",
                   help="prompt before each mutating step (reserve, online sync, lock, final sync, "
                        "merge, flip, post-flip); answering no pauses the job, resume continues")
    x.add_argument("--yes", "-y", action="store_true", help="no interactive confirmation")


def _print_job(j, as_json: bool = False) -> None:
    if as_json:
        print(json.dumps({"id": j.id, "account": j.account, "src": j.src, "dst": j.dst,
                          "phase": j.phase.name, "status": j.status, "error": j.error,
                          "data": j.data, "created": j.created, "updated": j.updated}, default=str))
        return
    ts = dt.datetime.fromtimestamp(j.created).strftime("%Y-%m-%d %H:%M")
    hb = int(dt.datetime.now().timestamp() - j.heartbeat) if j.heartbeat else -1
    lock = j.data.get("lock_seconds")
    print(f"{j.id}  {ts}  {j.account:<32} {j.src} -> {j.dst}  {j.status:<8} {j.phase.name:<14}"
          f" lock={lock if lock is not None else '-'}s hb={hb}s" + (f"\n    error: {j.error}" if j.error else ""))


def _background(argv: list[str], log_dir: str) -> int:
    argv = [a for a in argv if a != "--background"]
    logf = os.path.join(log_dir, "background.log")
    os.makedirs(os.path.dirname(logf), exist_ok=True)
    with open(logf, "a") as f:
        p = subprocess.Popen([sys.executable, "-m", "zmove", *argv, "--yes"], stdout=f, stderr=f,
                             stdin=subprocess.DEVNULL, start_new_session=True)
    print(f"started in background, pid {p.pid}; follow with `zmove query` / {logf}")
    return 0


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    ap = argparse.ArgumentParser(prog="zmove", description="Online mailbox move for Zimbra FOSS "
                                 "(zmmboxmove-compatible, DB+blob level, item IDs preserved)")
    ap.add_argument("-c", "--config")
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("-n", "--dry-run", action="store_true", help="print mutating commands only")
    ap.add_argument("--version", action="version", version=__version__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("move", help="move one mailbox (zmmboxmove)")
    p.add_argument("account")
    p.add_argument("-f", "--from", dest="src", required=True)
    p.add_argument("-t", "--to", dest="dst", required=True)
    _add_move_args(p)

    p = sub.add_parser("preflight", help="all checks + estimates, changes nothing")
    p.add_argument("account")
    p.add_argument("-f", "--from", dest="src", required=True)
    p.add_argument("-t", "--to", dest="dst", required=True)
    _add_move_args(p)

    p = sub.add_parser("batch", help="move many mailboxes from a CSV plan (account,src,dst)")
    p.add_argument("plan")
    p.add_argument("--parallel", type=int, default=4)
    p.add_argument("--per-server", type=int, default=2, help="max concurrent jobs touching a server")
    p.add_argument("--max-cutovers", type=int, default=2, help="max mailboxes locked at the same time")
    _add_move_args(p)

    p = sub.add_parser("query", help="show moves (zmmboxmovequery)")
    p.add_argument("account", nargs="?")
    p.add_argument("-s", "--server")
    p.add_argument("--status")
    p.add_argument("--check-peer", action="store_true", help="also check LDAP/lock state live")
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("events", help="event log of a job")
    p.add_argument("job")

    for name, hlp in (("resume", "continue a failed/interrupted job"),
                      ("abort", "undo a job that has not reached FLIPPED"),
                      ("rollback", "flip back to the untouched source copy after cutover")):
        p = sub.add_parser(name, help=hlp)
        p.add_argument("job")
        p.add_argument("--force", action="store_true")
        if name == "resume":
            p.add_argument("--yes", "-y", action="store_true",
                           help="skip per-step confirmations for this run")

    p = sub.add_parser("purge", help="delete the old copy on the source (zmpurgeoldmbox)")
    p.add_argument("account", nargs="?")
    p.add_argument("-s", "--server", help="source server of the move")
    p.add_argument("--job")
    p.add_argument("--older-than", type=float, metavar="DAYS", default=0,
                   help="only purge moves finished more than DAYS ago")
    p.add_argument("--all", action="store_true", help="purge every eligible finished move")
    p.add_argument("--keep-blobs", action="store_true", help="delete DB rows only")
    p.add_argument("--yes", "-y", action="store_true")

    p = sub.add_parser("unlock", help="recover an account left locked (zmprov ulm)")
    p.add_argument("account")
    p.add_argument("-s", "--server", required=True, help="any mailstore to run zmprov on")

    p = sub.add_parser("gc", help="clean leftovers of failed/aborted jobs (stage schemas + blob dirs)")
    p.add_argument("--older-than", type=float, metavar="HOURS", default=24)
    p.add_argument("--prune-events", type=float, metavar="DAYS", default=0,
                   help="also drop event rows older than DAYS from finished jobs")

    a = ap.parse_args(argv)
    try:
        s = load(a.config)
    except (ValueError, FileNotFoundError) as e:
        print(f"error: bad config: {e}", file=sys.stderr)
        return 2
    if getattr(a, "background", False):
        if getattr(a, "confirm_steps", False):
            print("error: --confirm-steps needs an interactive terminal; drop --background",
                  file=sys.stderr)
            return 2
        return _background(argv, s.log_dir)
    _setup_logging(s, a.verbose)
    st = State(s.state_db)
    eng = Engine(s, st, dry_run=a.dry_run, progress=lambda m: print(m, flush=True))

    try:
        if a.cmd == "preflight":
            acct = account_info(eng.host(a.dst), a.account)
            tmp = Job(id="preflight", account=acct.name, account_id=acct.id, src=a.src, dst=a.dst,
                      options={k: v for k, v in _move_opts(a).items() if v is not None})
            rep = eng.plan(tmp)
            print(json.dumps(rep, indent=2, default=str))
            return 1 if rep["errors"] else 0

        if a.cmd == "move":
            job = eng.new_job(a.account, a.src, a.dst, _move_opts(a))
            print(f"job {job.id}")
            eng.run(job)
            _print_job(st.get(job.id))
            return 0

        if a.cmd == "batch":
            opts = _move_opts(a)
            if opts.get("confirm_steps"):
                print("error: --confirm-steps is interactive and cannot be used with batch",
                      file=sys.stderr)
                return 2
            items = read_plan(a.plan)
            b = Batch(s, a.parallel, a.per_server, a.max_cutovers, a.dry_run)
            res = b.run(items, opts)
            bad = [r for r in res if r[2]]
            print(f"{len(res) - len(bad)} ok, {len(bad)} failed")
            for it, _, err in bad:
                print(f"  FAIL {it.account}: {err}")
            return 1 if bad else 0

        if a.cmd == "query":
            jobs = st.find(account=a.account, server=a.server, status=a.status)
            if a.account and not jobs:
                try:
                    jobs = st.find(account=account_info(eng.host(a.server), a.account).id) \
                        if a.server else []
                except Exception:  # noqa: BLE001
                    pass
            for j in jobs:
                _print_job(j, a.json)
                if a.check_peer and j.status in ("running", "failed"):
                    acct = account_info(eng.host(j.dst), j.account)
                    print(f"    live: zimbraMailHost={acct.mail_host} status={acct.status}")
                    ne_hits = []
                    for peer in (j.src, j.dst):
                        try:
                            mv = eng.soap(peer).query_moves(j.account)
                        except Exception:  # noqa: BLE001 auth/network -> report LDAP only
                            mv = f"<lookup on {peer} failed>"
                        if mv:
                            ne_hits.append(f"{peer}: {mv}")
                    if ne_hits:
                        print("    NE QueryMailboxMove: " + "; ".join(ne_hits))
                    else:
                        print("    NE QueryMailboxMove: no move reported "
                              "(or unsupported on this server)")
            return 0

        if a.cmd == "events":
            for ts, ph, lvl, msg, _data in st.events(a.job):
                t = dt.datetime.fromtimestamp(ts).strftime("%H:%M:%S")
                print(f"{t} {Phase(ph).name:<14} {lvl:<7} {msg}")
            return 0

        if a.cmd == "resume":
            job = st.get(a.job)
            if job.status == "done":
                print("already done")
                return 0
            if job.status == "aborted" and not a.yes and sys.stdin.isatty():
                if input(f"job {job.id} was aborted; its copies were removed and it will restart "
                         f"from {Phase.RESERVED.name}. proceed? [y/N] ").strip().lower() != "y":
                    return 1
            if a.force:
                job.options["force"] = True
            if a.yes:
                job.options["confirm_steps"] = False
            eng.run(job)
            _print_job(st.get(job.id))
            return 0

        if a.cmd == "abort":
            eng.abort(st.get(a.job))
            return 0

        if a.cmd == "rollback":
            eng.rollback(st.get(a.job), force=a.force)
            return 0

        if a.cmd == "purge":
            if a.job:
                jobs = [st.get(a.job)]
            else:
                jobs = [j for j in st.find(account=a.account, server=a.server)
                        if j.phase == Phase.DONE and j.status == "done"
                        and not j.data.get("rolled_back_at")
                        and (not a.server or j.src == a.server)]
                if not a.all and not a.account:
                    print("give ACCOUNT, --job or --all")
                    return 2
            cutoff = dt.datetime.now().timestamp() - a.older_than * 86400
            jobs = [j for j in jobs if j.updated <= cutoff]
            if not jobs:
                print("nothing to purge")
                return 0
            for j in jobs:
                print(f"will purge mailbox {j.data.get('src_mailbox_id')} of {j.account} on {j.src}")
            if not a.yes and (not sys.stdin.isatty() or input("proceed? [y/N] ").lower() != "y"):
                return 1
            for j in jobs:
                eng.purge(j, keep_blobs=a.keep_blobs)
            return 0

        if a.cmd == "unlock":
            eng.unlock(a.account, a.server)
            print(f"{a.account} unlocked")
            return 0

        if a.cmd == "gc":
            cutoff = dt.datetime.now().timestamp() - a.older_than * 3600
            if a.prune_events:
                n = st.prune_events(max_age_days=a.prune_events)
                print(f"pruned {n} old event rows")
            cleaned = 0
            for j in st.find(status="aborted", limit=10000) + st.find(status="failed", limit=10000):
                if j.updated > cutoff or j.phase >= Phase.FLIPPED or j.data.get("gc_at"):
                    continue
                if j.status == "failed" and (Phase.LOCKED <= j.phase or j.data.get("locking")):
                    print(f"skip {j.id}: failed while locked, resolve with resume/abort")
                    continue
                try:
                    what = eng.gc_job(j)
                except Exception as e:  # noqa: BLE001 best-effort; journal keeps the job
                    print(f"  {j.id}: cleanup failed ({type(e).__name__}: {e})")
                    continue
                print(f"{what} {j.id}")
                cleaned += 1
            if not cleaned and not a.prune_events:
                print("nothing to clean")
            return 0
    except MoveError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    except Exception as e:  # noqa: BLE001
        log.debug("unhandled", exc_info=True)
        print(f"error: {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("interrupted; job state is saved, use `zmove resume <job>` or `zmove abort <job>`",
              file=sys.stderr)
        return 130
    return 0
