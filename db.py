"""MySQL/MariaDB access on a mailstore (through SSH, using Zimbra's own binaries/credentials)."""
from __future__ import annotations

import re
import shlex
from dataclasses import dataclass
from typing import Iterable, Optional

from .remote import Host

_IDENT = re.compile(r"^[A-Za-z0-9_]+$")

# Per-mailbox tables in the `zimbra` schema (db.sql). Discovered tables are intersected with this
# allowlist: deleted_account and volume_blobs (dedupe work queue) are intentionally NOT moved.
ZIMBRA_MAILBOX_TABLES = ("mailbox_metadata", "out_of_office", "scheduled_task",
                         "mobile_devices", "pending_acl_push", "zmg_devices")

# Tables whose rows carry change numbers -> eligible for delta sync (create_database.sql)
DELTA_TABLES = ("mail_item", "mail_item_dumpster")

# Tables carrying a FileBlobStore locator (= volume id)
LOCATOR_COLUMN = "locator"


def ident(name: str) -> str:
    if not _IDENT.match(name):
        raise ValueError(f"unsafe SQL identifier: {name!r}")
    return f"`{name}`"


def sh_ident(name: str) -> str:
    """Identifier for a *shell* argument (no backticks: they mean command substitution)."""
    if not _IDENT.match(name):
        raise ValueError(f"unsafe SQL identifier: {name!r}")
    return name


def q(value: str) -> str:
    """Quote a string literal for SQL (values come from LDAP/DB, still escape defensively)."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


@dataclass
class TableInfo:
    schema: str
    name: str
    columns: list[str]
    unique_indexes: list[str]

    @property
    def has_locator(self) -> bool:
        return LOCATOR_COLUMN in self.columns


class MySQL:
    def __init__(self, host: Host, mysql_bin: str, mysqldump_bin: str,
                 my_cnf: str = "/opt/zimbra/conf/my.cnf"):
        self.host = host
        self.mysql_bin = mysql_bin
        self.mysqldump_bin = mysqldump_bin
        self.my_cnf = my_cnf

    # Credentials: MYSQL_PWD env var (never on argv), socket from localconfig.
    _ENV = ('export MYSQL_PWD="$(zmlocalconfig -s -m nokey mysql_root_password)"; '
            'SOCK="$(zmlocalconfig -m nokey mysql_socket)"; ')

    def client_cmd(self, db: Optional[str] = None, batch: bool = True, env: bool = True) -> str:
        args = [self.mysql_bin, f"--defaults-file={shlex.quote(self.my_cnf)}", '-S "$SOCK"', "-u root",
                "--max_allowed_packet=1G"]
        if batch:
            args += ["--batch", "-N", "--raw"]
        if db:
            args.append(sh_ident(db))
        return (self._ENV if env else "") + " ".join(args)

    def dump_cmd(self, db: str, tables: Iterable[str], where: str, replace: bool = False) -> str:
        tbls = " ".join(sh_ident(t) for t in tables)
        opts = [f"--defaults-file={shlex.quote(self.my_cnf)}", '-S "$SOCK"', "-u root",
                "--no-create-info", "--skip-triggers", "--complete-insert", "--skip-add-locks",
                "--skip-disable-keys", "--single-transaction", "--quick", "--hex-blob",
                "--skip-tz-utc", "--skip-comments", "--extended-insert", "--max_allowed_packet=1G",
                f"--where={shlex.quote(where)}"]
        if replace:
            opts.append("--replace")
        return self._ENV + f"{self.mysqldump_bin} {' '.join(opts)} {sh_ident(db)} {tbls}"

    def query(self, sql: str, db: Optional[str] = None, timeout: Optional[float] = None) -> list[list[Optional[str]]]:
        out = self.host.run(self.client_cmd(db), input=sql, mutating=False, timeout=timeout).out
        rows = []
        for line in out.splitlines():
            if line == "":
                continue
            rows.append([None if c == "NULL" else c for c in line.split("\t")])
        return rows

    def scalar(self, sql: str, db: Optional[str] = None, timeout: Optional[float] = None) -> Optional[str]:
        rows = self.query(sql, db, timeout=timeout)
        return rows[0][0] if rows and rows[0] else None

    def execute(self, sql: str, db: Optional[str] = None, timeout: Optional[float] = None) -> str:
        return self.host.run(self.client_cmd(db), input=sql, mutating=True, timeout=timeout).out

    # -- schema helpers ---------------------------------------------------
    def schema_exists(self, schema: str) -> bool:
        return self.scalar(f"SELECT COUNT(*) FROM information_schema.schemata "
                           f"WHERE schema_name={q(schema)}") == "1"

    def tables_with_column(self, schema: str, column: str) -> list[str]:
        rows = self.query(f"SELECT table_name FROM information_schema.columns WHERE "
                          f"table_schema={q(schema)} AND column_name={q(column)} ORDER BY table_name")
        return [r[0] for r in rows]

    def all_tables(self, schema: str) -> list[str]:
        rows = self.query(f"SELECT table_name FROM information_schema.tables WHERE "
                          f"table_schema={q(schema)} AND table_type='BASE TABLE' ORDER BY table_name")
        return [r[0] for r in rows]

    def table_info(self, schema: str, tables: Iterable[str]) -> dict[str, TableInfo]:
        tl = list(tables)
        if not tl:
            return {}
        inlist = ",".join(q(t) for t in tl)
        cols = self.query(f"SELECT table_name, column_name FROM information_schema.columns "
                          f"WHERE table_schema={q(schema)} AND table_name IN ({inlist}) "
                          f"ORDER BY table_name, ordinal_position")
        idx = self.query(f"SELECT DISTINCT table_name, index_name FROM information_schema.statistics "
                         f"WHERE table_schema={q(schema)} AND table_name IN ({inlist}) "
                         f"AND non_unique=0 AND index_name<>'PRIMARY'")
        out = {t: TableInfo(schema, t, [], []) for t in tl}
        for t, c in cols:
            out[t].columns.append(c)
        for t, i in idx:
            out[t].unique_indexes.append(i)
        return {k: v for k, v in out.items() if v.columns}

    def config_value(self, name: str) -> Optional[str]:
        return self.scalar(f"SELECT value FROM zimbra.config WHERE name={q(name)}")


def checksum_sql(schema: str, table: TableInfo, key_col: str, key_val: int,
                 locator_expr: Optional[str] = None, exclude: Iterable[str] = ()) -> str:
    """Order-independent table fingerprint for one mailbox: COUNT + BIT_XOR(64-bit MD5 prefix).

    ``locator_expr`` lets the *source* side apply the volume remap so both sides compare equal.
    """
    skip = {key_col, *exclude}
    parts = []
    for c in table.columns:
        if c in skip:
            continue
        expr = ident(c)
        if c == LOCATOR_COLUMN and locator_expr:
            expr = locator_expr
        parts.append(f"IFNULL(CAST({expr} AS BINARY), 0x00)")
    row = f"CONCAT_WS(0x1f, {', '.join(parts)})" if parts else "''"
    return (f"SELECT COUNT(*), COALESCE(BIT_XOR(CAST(CONV(SUBSTRING(MD5({row}),1,16),16,10) "
            f"AS UNSIGNED)),0) FROM {ident(schema)}.{ident(table.name)} "
            f"WHERE {ident(key_col)}={int(key_val)}")


def insert_select_sql(dst_schema: str, stage_schema: str, table: TableInfo, key_col: str,
                      new_key: int, overrides: Optional[dict[str, str]] = None) -> str:
    """INSERT INTO live.t (cols) SELECT <cols with key/overrides> FROM stage.t."""
    overrides = dict(overrides or {})
    overrides[key_col] = str(int(new_key))
    cols = ", ".join(ident(c) for c in table.columns)
    sel = ", ".join(overrides.get(c, ident(c)) for c in table.columns)
    return (f"INSERT INTO {ident(dst_schema)}.{ident(table.name)} ({cols}) "
            f"SELECT {sel} FROM {ident(stage_schema)}.{ident(table.name)};")


def id_list_to_inserts_cmd(mysql: MySQL, schema: str, table: str, mailbox_id: int,
                           target_table: str, batch: int = 5000) -> str:
    """Shell pipeline (run on source) emitting batched INSERTs of the item-id set."""
    sel = (f"SELECT id FROM {ident(schema)}.{ident(table)} WHERE mailbox_id={int(mailbox_id)}")
    awk = ("awk -v T=" + shlex.quote(target_table) + f" -v B={int(batch)} "
           "'{ if (n % B == 0) { if (n) print \";\"; printf \"INSERT INTO %s (id) VALUES (%s)\", T, $1 } "
           "else printf \",(%s)\", $1; n++ } END { if (n) print \";\" }'")
    return f"{mysql.client_cmd()} -e {shlex.quote(sel)} | {awk}"
