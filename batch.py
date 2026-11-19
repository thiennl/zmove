"""Fleet-scale moves: plan file, global parallelism, per-server limits, cutover throttling."""
from __future__ import annotations

import csv
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional

from .config import Settings
from .engine import Engine
from .state import State

log = logging.getLogger("zmove.batch")


@dataclass
class Item:
    account: str
    src: str
    dst: str


def read_plan(path: str) -> list[Item]:
    """CSV: account,src,dst  (header optional, '#' comments allowed)."""
    items = []
    with open(path, newline="") as f:
        for row in csv.reader(f):
            if not row or row[0].strip().startswith("#") or row[0].strip().lower() == "account":
                continue
            if len(row) < 3:
                raise ValueError(f"bad plan line: {row}")
            items.append(Item(row[0].strip(), row[1].strip(), row[2].strip()))
    return items


def _confirm_continue(msg: str) -> bool:
    """Ask to proceed; non-TTY/EOF always declines (never auto-proceed blind)."""
    import sys
    if not sys.stdin.isatty():
        return False
    try:
        return input(msg).strip().lower() in ("y", "yes")
    except EOFError:
        return False


class Batch:
    def __init__(self, settings: Settings, parallel: int = 4, per_server: int = 2,
                 max_cutovers: int = 2, dry_run: bool = False):
        self.s = settings
        self.parallel = parallel
        self.per_server = per_server
        self.dry = dry_run
        self.cutovers = threading.BoundedSemaphore(max(1, max_cutovers))
        self._srv: dict[str, threading.BoundedSemaphore] = {}
        self._srv_lock = threading.Lock()

    def _sem(self, server: str) -> threading.BoundedSemaphore:
        with self._srv_lock:
            if server not in self._srv:
                self._srv[server] = threading.BoundedSemaphore(max(1, self.per_server))
            return self._srv[server]

    def _one(self, it: Item, options: dict) -> tuple[Item, Optional[str], Optional[str]]:
        if it.src == it.dst:
            return it, None, "MoveError: source and destination are the same server"
        sa, sb = self._sem(it.src), self._sem(it.dst)
        if sa is sb:  # same host on both ends: one semaphore, never acquire twice
            with sa:
                return self._run_one(it, options)
        a, b = sorted((sa, sb), key=id)  # fixed order: no deadlock
        with a, b:
            return self._run_one(it, options)

    def _run_one(self, it: Item, options: dict) -> tuple[Item, Optional[str], Optional[str]]:
        st = State(self.s.state_db)          # one sqlite connection per thread
        eng = Engine(self.s, st, self.dry, progress=lambda m: log.info(m))
        eng.lock_semaphore = self.cutovers
        try:
            job = eng.new_job(it.account, it.src, it.dst, options)
            eng.run(job)
            return it, job.id, None
        except Exception as e:  # noqa: BLE001
            return it, None, f"{type(e).__name__}: {e}"

    def _run_all(self, items: list[Item], options: dict) -> list[tuple[Item, Optional[str], Optional[str]]]:
        results = []
        with ThreadPoolExecutor(max_workers=max(1, self.parallel)) as ex:
            futs = [ex.submit(self._one, it, options) for it in items]
            for f in as_completed(futs):
                it, jid, err = f.result()
                results.append((it, jid, err))
                if err:
                    log.error("FAIL %s %s->%s: %s", it.account, it.src, it.dst, err)
                else:
                    log.info("OK   %s %s->%s job=%s", it.account, it.src, it.dst, jid)
        return results

    def run(self, items: list[Item], options: dict, canary: int = 0) -> list[tuple[Item, Optional[str], Optional[str]]]:
        """Run a batch, optionally stopping after a canary prefix.

        With canary=N>0 the first N items run first: any failure stops the
        batch; otherwise the operator is asked before the rest (non-TTY
        always stops — re-run without --canary for the remainder).
        """
        if canary <= 0 or canary >= len(items):
            return self._run_all(items, options)
        first, rest = items[:canary], items[canary:]
        results = self._run_all(first, options)
        failed = [r for r in results if r[2]]
        if failed:
            log.error("canary: %d of first %d failed, stopping before remaining %d",
                      len(failed), len(first), len(rest))
            return results
        if not _confirm_continue(
                f"canary ok ({len(first)} moved); continue with remaining {len(rest)}? [y/N] "):
            log.warning("canary paused by operator/non-TTY; %d remaining skipped", len(rest))
            return results
        return results + self._run_all(rest, options)
