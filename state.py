"""Durable job journal (SQLite). Replaces NE's in-server move registry.

Guarantees:
  * at most one *active* job per account (partial UNIQUE index) == RegisterMailboxMoveOut
  * every phase transition is committed before the next side effect -> resumable
  * heartbeat + pid -> stale-job detection (crashed controller)
"""
from __future__ import annotations

import json
import os
import socket
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Optional


class Phase(IntEnum):
    PLANNED = 0
    PREFLIGHT_OK = 10
    RESERVED = 20
    PRESYNC = 30
    PRESTAGED = 40
    LOCKED = 50
    FINAL_SYNCED = 60
    DB_FINAL = 70
    VERIFIED = 80
    MERGED = 90
    FLIPPED = 100        # point of no return: zimbraMailHost now points at destination
    CACHES_PURGED = 110
    UNLOCKED = 120
    DONE = 130
    PURGED = 140


ACTIVE = ("running", "failed")   # failed jobs still hold the account until aborted/resumed


@dataclass
class Job:
    id: str
    account: str
    account_id: str
    src: str
    dst: str
    phase: Phase = Phase.PLANNED
    status: str = "running"        # running | failed | done | aborted
    options: dict = field(default_factory=dict)
    data: dict = field(default_factory=dict)
    error: Optional[str] = None
    created: float = 0.0
    updated: float = 0.0
    owner: Optional[str] = None
    heartbeat: float = 0.0


_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY, account TEXT NOT NULL, account_id TEXT NOT NULL,
  src TEXT NOT NULL, dst TEXT NOT NULL, phase INTEGER NOT NULL, status TEXT NOT NULL,
  options TEXT NOT NULL, data TEXT NOT NULL, error TEXT,
  created REAL NOT NULL, updated REAL NOT NULL, owner TEXT, heartbeat REAL NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS u_active_account ON jobs(account_id)
  WHERE status IN ('running','failed');
CREATE TABLE IF NOT EXISTS events (
  job_id TEXT NOT NULL, ts REAL NOT NULL, phase INTEGER, level TEXT, msg TEXT, data TEXT
);
CREATE INDEX IF NOT EXISTS i_events_job ON events(job_id, ts);
CREATE TABLE IF NOT EXISTS purges (
  account_id TEXT NOT NULL, server TEXT NOT NULL, mailbox_id INTEGER NOT NULL, ts REAL NOT NULL,
  PRIMARY KEY (account_id, server)
);
"""


class AlreadyMoving(RuntimeError):
    pass


class State:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        self.db = sqlite3.connect(path, timeout=30, isolation_level=None, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=30000")
        self.db.executescript(_SCHEMA)

    @staticmethod
    def owner_tag() -> str:
        return f"{socket.gethostname()}:{os.getpid()}"

    def _row(self, r) -> Job:
        return Job(id=r[0], account=r[1], account_id=r[2], src=r[3], dst=r[4], phase=Phase(r[5]),
                   status=r[6], options=json.loads(r[7]), data=json.loads(r[8]), error=r[9],
                   created=r[10], updated=r[11], owner=r[12], heartbeat=r[13])

    _COLS = ("id, account, account_id, src, dst, phase, status, options, data, error, created, "
             "updated, owner, heartbeat")

    def create(self, account: str, account_id: str, src: str, dst: str, options: dict) -> Job:
        now = time.time()
        j = Job(id=uuid.uuid4().hex[:12], account=account, account_id=account_id, src=src, dst=dst,
                options=options, created=now, updated=now, owner=self.owner_tag(), heartbeat=now)
        try:
            self.db.execute(f"INSERT INTO jobs ({self._COLS}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            (j.id, account, account_id, src, dst, int(j.phase), j.status,
                             json.dumps(options), "{}", None, now, now, j.owner, now))
        except sqlite3.IntegrityError as e:
            other = self.active_for(account_id)
            raise AlreadyMoving(f"ALREADY_BEING_MOVED: {account} has active job "
                                f"{other.id if other else '?'} ({other.status if other else ''})") from e
        return j

    def save(self, j: Job) -> None:
        j.updated = time.time()
        self.db.execute("UPDATE jobs SET phase=?, status=?, options=?, data=?, error=?, updated=?, "
                        "owner=?, heartbeat=? WHERE id=?",
                        (int(j.phase), j.status, json.dumps(j.options), json.dumps(j.data), j.error,
                         j.updated, j.owner, j.heartbeat, j.id))

    def beat(self, j: Job) -> None:
        j.heartbeat = time.time()
        self.db.execute("UPDATE jobs SET heartbeat=? WHERE id=?", (j.heartbeat, j.id))

    HEARTBEAT_INTERVAL = 30.0
    HEARTBEAT_STALE = 120.0

    def start_heartbeat(self, job_id: str, interval: Optional[float] = None) -> threading.Event:
        """Beat in the background (own SQLite connection) while a long rsync/DB step runs, so
        another controller can tell a live job from a crashed one. Set the event to stop."""
        stop = threading.Event()
        interval = interval or self.HEARTBEAT_INTERVAL

        def loop() -> None:
            con = sqlite3.connect(self.path, timeout=30, isolation_level=None)
            try:
                while not stop.wait(interval):
                    try:
                        con.execute("UPDATE jobs SET heartbeat=? WHERE id=?", (time.time(), job_id))
                    except sqlite3.Error:
                        pass
            finally:
                con.close()

        threading.Thread(target=loop, name=f"zmove-hb-{job_id}", daemon=True).start()
        return stop

    def owner_alive(self, j: Job) -> bool:
        """True if another process may still be driving this job."""
        if j.status != "running" or not j.owner or j.owner == self.owner_tag():
            return False
        host, _, pid = j.owner.rpartition(":")
        if host == socket.gethostname() and pid.isdigit():
            try:
                os.kill(int(pid), 0)
            except ProcessLookupError:
                return False
            except PermissionError:
                return True
            return True
        return (time.time() - (j.heartbeat or 0)) < self.HEARTBEAT_STALE

    def get(self, job_id: str) -> Job:
        r = self.db.execute(f"SELECT {self._COLS} FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not r:
            raise KeyError(job_id)
        return self._row(r)

    def active_for(self, account_id: str) -> Optional[Job]:
        r = self.db.execute(f"SELECT {self._COLS} FROM jobs WHERE account_id=? AND status IN "
                            "('running','failed')", (account_id,)).fetchone()
        return self._row(r) if r else None

    def find(self, account: Optional[str] = None, server: Optional[str] = None,
             status: Optional[str] = None, limit: int = 200) -> list[Job]:
        sql, args = f"SELECT {self._COLS} FROM jobs WHERE 1=1", []
        if account:
            sql += " AND (account=? OR account_id=?)"
            args += [account, account]
        if server:
            sql += " AND (src=? OR dst=?)"
            args += [server, server]
        if status:
            sql += " AND status=?"
            args.append(status)
        sql += " ORDER BY created DESC LIMIT ?"
        args.append(limit)
        return [self._row(r) for r in self.db.execute(sql, args).fetchall()]

    def event(self, j: Job, level: str, msg: str, **data: Any) -> None:
        self.db.execute("INSERT INTO events VALUES (?,?,?,?,?,?)",
                        (j.id, time.time(), int(j.phase), level, msg, json.dumps(data, default=str)))

    def events(self, job_id: str) -> list[tuple]:
        return self.db.execute("SELECT ts, phase, level, msg, data FROM events WHERE job_id=? "
                               "ORDER BY ts", (job_id,)).fetchall()

    def record_purge(self, account_id: str, server: str, mailbox_id: int) -> None:
        self.db.execute("INSERT OR REPLACE INTO purges VALUES (?,?,?,?)",
                        (account_id, server, mailbox_id, time.time()))

    def purge_record(self, account_id: str, server: str) -> Optional[tuple[int, float]]:
        r = self.db.execute("SELECT mailbox_id, ts FROM purges WHERE account_id=? AND server=?",
                            (account_id, server)).fetchone()
        return (int(r[0]), float(r[1])) if r else None

    def clear_purge(self, account_id: str, server: str) -> None:
        self.db.execute("DELETE FROM purges WHERE account_id=? AND server=?", (account_id, server))

    def prune_events(self, max_age_days: float = 30) -> int:
        """Drop old journal rows from finished jobs so events/ never grows unbounded."""
        cutoff = time.time() - max_age_days * 86400
        cur = self.db.execute(
            "DELETE FROM events WHERE ts < ? AND job_id NOT IN "
            "(SELECT id FROM jobs WHERE status IN ('running','failed'))", (cutoff,))
        return cur.rowcount if cur else 0
