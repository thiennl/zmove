"""Configuration (INI, stdlib only)."""
from __future__ import annotations

import configparser
import os
from dataclasses import dataclass, field
from typing import Optional

DEFAULT_PATHS = ("./zmove.ini", "/etc/zmove/zmove.ini", "~/.config/zmove/zmove.ini")


@dataclass
class Settings:
    # control node -> mailstore SSH
    ssh_user: str = "zimbra"
    ssh_port: int = 22
    ssh_key: Optional[str] = None
    ssh_options: list[str] = field(default_factory=list)
    run_as_zimbra: bool = False          # if ssh_user != zimbra_user: wrap with sudo -u zimbra -H
    zimbra_user: str = "zimbra"          # OS user that owns /opt/zimbra
    # mailstore -> mailstore SSH (used by rsync running on source)
    node_ssh_user: str = "zimbra"
    node_ssh_port: int = 22
    node_ssh_options: str = "-o BatchMode=yes -o StrictHostKeyChecking=accept-new"
    # zimbra install
    zimbra_home: str = "/opt/zimbra"
    mysql_bin: str = "/opt/zimbra/common/bin/mysql"
    mysqldump_bin: str = "/opt/zimbra/common/bin/mysqldump"
    rsync_bin: str = "rsync"
    # admin SOAP
    admin_port: int = 7071
    admin_user: Optional[str] = None      # None -> use localconfig zimbra_user / zimbra_ldap_password
    admin_password: Optional[str] = None
    verify_tls: bool = False
    # behaviour
    state_db: str = "~/.local/share/zmove/state.db"
    log_dir: str = "~/.local/share/zmove/logs"
    max_syncs: int = 10
    # NE code default is 180000 (MoveMailbox.DEFAULT_SYNC_FINISH_THRESHOLD);
    # NE soap doc says 30000. Follow the shipped code.
    sync_finish_threshold_ms: int = 180000
    rsync_bwlimit_kbps: int = 0
    ionice: bool = True
    max_lock_seconds: int = 900           # hard watchdog: abort+unlock if exceeded before flip
    cutover_window: Optional[str] = None      # "HH:MM-HH:MM" default lock window
    verify: str = "full"                  # full | count | none
    db_strategy: str = "delta"            # delta | full
    mta_hosts: list[str] = field(default_factory=list)
    shared_volumes: dict[int, int] = field(default_factory=dict)  # src vol id -> dst vol id
    metrics_textfile: Optional[str] = None
    rsync_timeout: int = 0                # seconds per rsync pass, 0 = no timeout
    db_timeout: int = 0                   # seconds per single DB copy/compare statement batch
    tls_ca: Optional[str] = None          # PEM bundle when verify_tls is on

    def expand(self) -> "Settings":
        self.state_db = os.path.expanduser(self.state_db)
        self.log_dir = os.path.expanduser(self.log_dir)
        if self.ssh_key:
            self.ssh_key = os.path.expanduser(self.ssh_key)
        return self


def _list(v: str) -> list[str]:
    return [x.strip() for x in v.replace("\n", ",").split(",") if x.strip()]


def load(path: Optional[str] = None) -> Settings:
    s = Settings()
    candidates = [path] if path else [os.path.expanduser(p) for p in DEFAULT_PATHS]
    cp = configparser.ConfigParser()
    found = None
    for c in candidates:
        if c and os.path.exists(c):
            cp.read(c)
            found = c
            break
    if path and not found:
        raise FileNotFoundError(path)
    if found:
        sec = cp["zmove"] if cp.has_section("zmove") else {}
        for f in s.__dataclass_fields__:
            if f not in sec:
                continue
            raw = sec[f]
            cur = getattr(s, f)
            if isinstance(cur, bool):
                setattr(s, f, cp.getboolean("zmove", f))
            elif isinstance(cur, int) and not isinstance(cur, bool):
                setattr(s, f, int(raw))
            elif isinstance(cur, list):
                setattr(s, f, _list(raw) if f != "ssh_options" else raw.split())
            elif isinstance(cur, dict):
                m = {}
                for pair in _list(raw):
                    a, b = pair.split(":")
                    m[int(a)] = int(b)
                setattr(s, f, m)
            else:
                setattr(s, f, raw if raw != "" else None)
    _validate(s, found)
    return s.expand()


def _validate(s: Settings, source: Optional[str]) -> None:
    where = f" ({source})" if source else ""
    if s.max_syncs < 1:
        raise ValueError(f"max_syncs must be >= 1{where}")
    if s.sync_finish_threshold_ms < 0:
        raise ValueError(f"sync_finish_threshold_ms must be >= 0{where}")
    if s.rsync_bwlimit_kbps < 0:
        raise ValueError(f"rsync_bwlimit_kbps must be >= 0{where}")
    if s.max_lock_seconds < 0:
        raise ValueError(f"max_lock_seconds must be >= 0{where}")
    if s.rsync_timeout < 0:
        raise ValueError(f"rsync_timeout must be >= 0{where}")
    if s.db_timeout < 0:
        raise ValueError(f"db_timeout must be >= 0{where}")
    if s.verify not in ("full", "count", "none"):
        raise ValueError(f"verify must be full|count|none{where}")
    if s.db_strategy not in ("delta", "full"):
        raise ValueError(f"db_strategy must be delta|full{where}")
    if getattr(s, "cutover_window", None):
        from .engine import parse_window  # deferred: config must not import engine at module load
        parse_window(s.cutover_window)
