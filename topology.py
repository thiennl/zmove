"""Discovery of servers, volumes, accounts and mailboxes."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from .db import MySQL, q
from .paths import Volume
from .remote import Host

log = logging.getLogger("zmove.topology")


def parse_zmprov_attrs(text: str) -> dict[str, list[str]]:
    """Parse `zmprov ga/gs` output ('# name x' header + 'attr: value' lines)."""
    out: dict[str, list[str]] = {}
    for line in text.splitlines():
        if not line or line.startswith("#") or ": " not in line:
            continue
        k, v = line.split(": ", 1)
        out.setdefault(k.strip(), []).append(v)
    return out


def first(attrs: dict[str, list[str]], key: str, default: Optional[str] = None) -> Optional[str]:
    v = attrs.get(key)
    return v[0] if v else default


def tf(v: Optional[str]) -> bool:
    return (v or "").upper() == "TRUE"


@dataclass
class ServerInfo:
    name: str
    zimbra_id: str
    version: str
    db_version: str
    index_version: str
    num_groups: int
    blob_dir_server_id: bool
    move_skip_blobs: bool
    move_skip_hsm_blobs: bool
    move_skip_index: bool
    volumes: list[Volume] = field(default_factory=list)
    current_msg: int = 0
    current_secondary: Optional[int] = None
    current_index: int = 0
    mailboxd_started: Optional[int] = None   # epoch seconds of mailboxd JVM start

    def volume(self, vid: int) -> Volume:
        for v in self.volumes:
            if v.id == vid:
                return v
        raise KeyError(f"volume {vid} not on {self.name}")

    @property
    def path_prefix(self) -> Optional[str]:
        """Volume.getConfiguredServerID() when zimbraConfiguredServerIDForBlobDirEnabled."""
        return self.zimbra_id if self.blob_dir_server_id else None


@dataclass
class AccountInfo:
    name: str
    id: str
    mail_host: str
    mail_transport: str
    status: str


@dataclass
class MailboxRow:
    id: int
    group_id: int
    index_volume_id: int
    items: int = 0
    size: int = 0


def server_info(host: Host, mysql: MySQL) -> ServerInfo:
    attrs = parse_zmprov_attrs(host.out(
        f"zmprov -l gs {host.name} zimbraId zimbraConfiguredServerIDForBlobDirEnabled "
        "zimbraMailboxMoveSkipBlobs zimbraMailboxMoveSkipHsmBlobs zimbraMailboxMoveSkipSearchIndex"))
    version = host.out("zmcontrol -v").splitlines()[0].strip()
    groups = int(host.out("zmlocalconfig -m nokey zimbra_mailbox_groups") or 100)
    started = host.out("pid=$(pgrep -u zimbra -f 'java.*mailboxd' | head -1); "
                       "[ -n \"$pid\" ] && stat -c %Y /proc/$pid || true")
    si = ServerInfo(
        name=host.name,
        zimbra_id=first(attrs, "zimbraId", ""),
        version=version,
        db_version=mysql.config_value("db.version") or "?",
        index_version=mysql.config_value("index.version") or "?",
        num_groups=groups,
        blob_dir_server_id=tf(first(attrs, "zimbraConfiguredServerIDForBlobDirEnabled")),
        move_skip_blobs=tf(first(attrs, "zimbraMailboxMoveSkipBlobs")),
        move_skip_hsm_blobs=tf(first(attrs, "zimbraMailboxMoveSkipHsmBlobs")),
        move_skip_index=tf(first(attrs, "zimbraMailboxMoveSkipSearchIndex")),
        mailboxd_started=int(started) if started.isdigit() else None,
    )
    has_store_type = mysql.scalar(
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='zimbra' "
        "AND table_name='volume' AND column_name='store_type'") == "1"
    st = "store_type" if has_store_type else "1"
    for r in mysql.query(f"SELECT id, type, name, path, file_bits, file_group_bits, mailbox_bits, "
                         f"mailbox_group_bits, compress_blobs, {st} FROM zimbra.volume"):
        si.volumes.append(Volume(int(r[0]), int(r[1]), r[2] or "", r[3] or "", int(r[4]), int(r[5]),
                                 int(r[6]), int(r[7]), r[8] == "1", int(r[9])))
    cv = mysql.query("SELECT message_volume_id, secondary_message_volume_id, index_volume_id "
                     "FROM zimbra.current_volumes")
    if cv:
        si.current_msg = int(cv[0][0])
        si.current_secondary = int(cv[0][1]) if cv[0][1] else None
        si.current_index = int(cv[0][2])
    return si


def account_info(host: Host, account: str) -> AccountInfo:
    attrs = parse_zmprov_attrs(host.out(
        f"zmprov -l ga {account} zimbraId zimbraMailHost zimbraMailTransport zimbraAccountStatus mail"))
    if not first(attrs, "zimbraId"):
        raise LookupError(f"account {account} not found")
    return AccountInfo(name=account, id=first(attrs, "zimbraId", ""),
                       mail_host=first(attrs, "zimbraMailHost", ""),
                       mail_transport=first(attrs, "zimbraMailTransport", ""),
                       status=first(attrs, "zimbraAccountStatus", "active"))


def mailbox_row(mysql: MySQL, account_id: str, with_stats: bool = True) -> Optional[MailboxRow]:
    r = mysql.query(f"SELECT id, group_id, index_volume_id FROM zimbra.mailbox "
                    f"WHERE account_id={q(account_id)}")
    if not r:
        return None
    mb = MailboxRow(int(r[0][0]), int(r[0][1]), int(r[0][2]))
    if with_stats:
        s = mysql.query(f"SELECT COUNT(*), COALESCE(SUM(size),0) FROM mboxgroup{mb.group_id}.mail_item "
                        f"WHERE mailbox_id={mb.id}")
        mb.items, mb.size = int(s[0][0]), int(s[0][1])
    return mb


def mailbox_id_in_use(mysql: MySQL, mailbox_id: int) -> bool:
    return mysql.scalar(f"SELECT COUNT(*) FROM zimbra.mailbox WHERE id={int(mailbox_id)}") != "0"


def memcached_servers(host: Host) -> list[str]:
    attrs = parse_zmprov_attrs(host.out("zmprov -l gcf zimbraMemcachedClientServerList"))
    servers = attrs.get("zimbraMemcachedClientServerList", [])
    if not servers:
        for s in host.out("zmprov -l gas memcached").split():
            servers.append(f"{s}:11211")
    out = []
    for s in servers:
        for part in s.replace(",", " ").split():
            out.append(part if ":" in part else f"{part}:11211")
    return out


def mta_servers(host: Host) -> list[str]:
    """Fallback when [zmove] mta_hosts is empty: discover MTAs from LDAP."""
    try:
        return [s for s in host.out("zmprov -l gas mta").split() if s]
    except Exception:  # noqa: BLE001 lookup failures just mean no auto-flush
        return []
