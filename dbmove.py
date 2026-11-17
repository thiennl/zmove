"""Per-mailbox DB transfer: staging, delta sync, verification, merge, cleanup.

Strategy (improvement over NE, which exports/imports the whole mailbox DB *while locked*):
  1. PRESTAGE (account online): copy mail_item(+dumpster) rows into a staging schema on the
     destination and remember watermark W = max(mod_metadata, mod_content).
  2. FINAL (account locked): copy only rows with mod_metadata > W or mod_content > W, drop rows
     whose id disappeared at the source, fully re-copy the small tables.
  3. VERIFY: per-table COUNT + BIT_XOR(MD5) fingerprint, source vs stage. Mismatch -> automatic
     fallback to a full copy of the delta tables (still under lock), then re-verify.
  4. MERGE: one transaction remaps mailbox_id/group/locator and inserts into the live tables.
Change numbers are allocated under the per-mailbox lock (Mailbox.beginTransaction), so any
transaction not yet committed when W is read gets a number > W and is picked up by the delta.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from . import remote
from .db import (DELTA_TABLES, ZIMBRA_MAILBOX_TABLES, MySQL, TableInfo, checksum_sql,
                 id_list_to_inserts_cmd, ident, insert_select_sql, q, sh_ident)
from .paths import group_db, locator_case_sql

log = logging.getLogger("zmove.dbmove")

# mailbox row columns that legitimately differ between source and destination
MAILBOX_ROW_LOCAL_COLS = ("id", "group_id", "index_volume_id", "itemcache_checkpoint")


class SchemaMismatch(RuntimeError):
    pass


class VerifyError(RuntimeError):
    pass


@dataclass
class DbPlan:
    account: str
    account_id: str
    src_id: int
    src_group: int
    dst_id: int
    dst_group: int
    dst_index_volume: int
    stage: str
    vol_map: dict[int, int]
    exclude_tables: list[str] = field(default_factory=list)
    group_tables: dict[str, TableInfo] = field(default_factory=dict)
    zimbra_tables: dict[str, TableInfo] = field(default_factory=dict)
    mailbox_table: Optional[TableInfo] = None

    @property
    def src_gdb(self) -> str:
        return group_db(self.src_group)

    @property
    def dst_gdb(self) -> str:
        return group_db(self.dst_group)


class DbMove:
    def __init__(self, plan: DbPlan, src: MySQL, dst: MySQL, dry_run: bool = False):
        self.p, self.src, self.dst, self.dry = plan, src, dst, dry_run

    # ------------------------------------------------------------------ discovery
    def discover(self) -> None:
        p = self.p
        src_g = set(self.src.tables_with_column(p.src_gdb, "mailbox_id"))
        tmpl = p.dst_gdb if self.dst.schema_exists(p.dst_gdb) else self._dst_template_group()
        dst_g = set(self.dst.tables_with_column(tmpl, "mailbox_id"))
        missing = src_g - dst_g
        if missing:
            raise SchemaMismatch(f"tables missing on destination: {sorted(missing)}")
        names = sorted(t for t in src_g if t not in p.exclude_tables)
        s_info = self.src.table_info(p.src_gdb, names)
        d_info = self.dst.table_info(tmpl, names)
        self._compare(s_info, d_info, "group")
        p.group_tables = d_info

        src_z = set(self.src.tables_with_column("zimbra", "mailbox_id"))
        dst_z = set(self.dst.tables_with_column("zimbra", "mailbox_id"))
        excluded = set(p.exclude_tables)
        unknown = {t for t in (src_z | dst_z) - set(ZIMBRA_MAILBOX_TABLES) - excluded
                   if t != "acl"}  # Zimbra 10.1+: acl is a view, not a mailbox table
        if unknown:
            raise SchemaMismatch(f"unknown mailbox-scoped zimbra tables: {sorted(unknown)}; "
                                 "update ZIMBRA_MAILBOX_TABLES or pass --exclude-tables")
        migrate = sorted(t for t in ZIMBRA_MAILBOX_TABLES if t in src_z and t in dst_z
                         and t not in excluded)
        dst_only = sorted(t for t in ZIMBRA_MAILBOX_TABLES if t in dst_z and t not in src_z
                          and t not in excluded)
        if dst_only:
            raise SchemaMismatch("destination has mailbox tables the source lacks "
                                 f"({dst_only}); versions diverged, refusing silent drop")
        zn = sorted(t for t in migrate if t in src_z and t in dst_z)
        zs, zd = self.src.table_info("zimbra", zn), self.dst.table_info("zimbra", zn)
        self._compare(zs, zd, "zimbra")
        p.zimbra_tables = zd
        ms = self.src.table_info("zimbra", ["mailbox"])
        md = self.dst.table_info("zimbra", ["mailbox"])
        self._compare(ms, md, "zimbra")
        if "mailbox" not in md:
            raise SchemaMismatch("destination lacks zimbra.mailbox")
        p.mailbox_table = md["mailbox"]

    @staticmethod
    def _compare(s: dict[str, TableInfo], d: dict[str, TableInfo], what: str) -> None:
        for t, si in s.items():
            di = d.get(t)
            if di is None or si.columns != di.columns:
                raise SchemaMismatch(f"{what}.{t}: column layout differs "
                                     f"(src={si.columns} dst={di.columns if di else None}); "
                                     "source and destination must run the same Zimbra version")

    def _dst_template_group(self) -> str:
        r = self.dst.scalar("SELECT schema_name FROM information_schema.schemata "
                            "WHERE schema_name LIKE 'mboxgroup%' ORDER BY schema_name LIMIT 1")
        if not r:
            raise SchemaMismatch("destination has no mboxgroup schema to use as template")
        return r

    def ensure_dst_group_db(self) -> None:
        """DbMailbox.createMailboxDatabase equivalent: clone DDL from an existing group.

        Skip-check the row count so a crashed first run resumes instead of dying on CREATE.
        """
        if self.dst.schema_exists(self.p.dst_gdb):
            n = self.dst.tables_with_column(self.p.dst_gdb, "mailbox_id")
            if n:
                return
        else:
            tmpl = self._dst_template_group()
            log.info("creating %s on %s from template %s", self.p.dst_gdb, self.dst.host.name, tmpl)
            self.dst.execute(f"CREATE DATABASE {ident(self.p.dst_gdb)} DEFAULT CHARACTER SET utf8;")
            ddl = (f"{self.dst.mysqldump_bin} --defaults-file=/opt/zimbra/conf/my.cnf -S \"$SOCK\" -u root "
                   f"--no-data --skip-triggers --skip-comments {sh_ident(tmpl)}")
            self.dst.host.run(f"{MySQL._ENV}{ddl} | {self.dst.client_cmd(self.p.dst_gdb, env=False)}")

    # ------------------------------------------------------------------ staging
    def create_stage(self) -> None:
        """Idempotent: a crashed run resumes into the same stage instead of failing on CREATE."""
        p = self.p
        if self.dst.schema_exists(p.stage):
            self.discover()  # rebuild the table maps; content stays for resume
            have = set(self.dst.tables_with_column(p.stage, "mailbox_id"))
            want = (set(p.group_tables) | set(p.zimbra_tables) | {"mailbox"}) - {"acl"}
            missing = want - have
            for t in sorted(missing):
                self._create_stage_table(t)
            return
        sql = [f"DROP DATABASE IF EXISTS {ident(p.stage)};",
               f"CREATE DATABASE {ident(p.stage)} DEFAULT CHARACTER SET utf8;"]
        self.dst.execute("\n".join(sql))
        for t in list(p.group_tables) + list(p.zimbra_tables) + ["mailbox"]:
            self._create_stage_table(t)

    def _create_stage_table(self, table: str) -> None:
        p = self.p
        if table == "mailbox":
            ti = p.mailbox_table
            like = f"zimbra.{ident(table)}"
        elif table in p.group_tables:
            ti = p.group_tables[table]
            like = f"{ident(p.dst_gdb)}.{ident(table)}"
        else:
            ti = p.zimbra_tables[table]
            like = f"zimbra.{ident(table)}"
        self.dst.execute(f"CREATE TABLE IF NOT EXISTS {ident(p.stage)}.{ident(table)} LIKE {like};")
        for i in ti.unique_indexes:
            try:
                self.dst.execute(f"ALTER TABLE {ident(p.stage)}.{ident(table)} DROP INDEX {ident(i)};")
            except Exception:  # noqa: BLE001 index may already be gone on resume
                pass
    def drop_stage(self) -> None:
        self.dst.execute(f"DROP DATABASE IF EXISTS {ident(self.p.stage)};")

    def _copy(self, schema: str, tables: list[str], where: str, replace: bool = False,
              timeout: Optional[float] = None) -> float:
        if not tables:
            return 0.0
        return remote.pipe(self.src.host, self.src.dump_cmd(schema, tables, where, replace),
                           self.dst.host, self.dst.client_cmd(self.p.stage), dry_run=self.dry,
                           timeout=timeout)
    def _delta_tables(self) -> list[str]:
        return [t for t in DELTA_TABLES if t in self.p.group_tables]

    def _other_tables(self) -> list[str]:
        return [t for t in self.p.group_tables if t not in DELTA_TABLES]

    def watermark(self) -> int:
        parts = [f"SELECT GREATEST(COALESCE(MAX(mod_metadata),0), COALESCE(MAX(mod_content),0)) AS w "
                 f"FROM {ident(self.p.src_gdb)}.{ident(t)} WHERE mailbox_id={self.p.src_id}"
                 for t in self._delta_tables()]
        if not parts:
            return 0
        return int(self.src.scalar(f"SELECT MAX(w) FROM ({' UNION ALL '.join(parts)}) x") or 0)

    def prestage(self, timeout: Optional[float] = None) -> dict:
        """Online bulk copy. Watermark is read BEFORE the dump (see module docstring)."""
        self._truncate(self._delta_tables())  # resume-safe: a retry must not hit PK conflicts
        w = self.watermark()
        t0 = time.monotonic()
        self._copy(self.p.src_gdb, self._delta_tables(), f"mailbox_id={self.p.src_id}",
                   timeout=timeout)
        secs = time.monotonic() - t0
        rows = 0
        if self._delta_tables():
            rows = int(self.dst.scalar("SELECT " + " + ".join(
                f"(SELECT COUNT(*) FROM {ident(self.p.stage)}.{ident(t)})"
                for t in self._delta_tables())) or 0)
        return {"watermark": w, "seconds": round(secs, 2), "rows": rows}

    def _truncate(self, tables: list[str]) -> None:
        if tables:
            self.dst.execute("\n".join(f"TRUNCATE TABLE {ident(self.p.stage)}.{ident(t)};"
                                       for t in tables))

    def final(self, strategy: str, watermark: Optional[int], timeout: Optional[float] = None) -> dict:
        """Runs while the source mailbox is locked."""
        p, info = self.p, {}
        t0 = time.monotonic()
        dts = self._delta_tables()
        if strategy == "delta" and watermark is not None:
            w = int(watermark)
            self._copy(p.src_gdb, dts, f"mailbox_id={p.src_id} AND (mod_metadata>{w} OR mod_content>{w})",
                       replace=True, timeout=timeout)
            for t in dts:
                ids = f"_ids_{t}"
                self.dst.execute(f"DROP TABLE IF EXISTS {ident(p.stage)}.{ident(ids)}; "
                                 f"CREATE TABLE {ident(p.stage)}.{ident(ids)} (id INT UNSIGNED PRIMARY KEY);")
                remote.pipe(self.src.host,
                            id_list_to_inserts_cmd(self.src, p.src_gdb, t, p.src_id, ident(ids)),
                            self.dst.host, self.dst.client_cmd(p.stage), dry_run=self.dry,
                            timeout=timeout)
                self.dst.execute(
                    f"DELETE s FROM {ident(p.stage)}.{ident(t)} s LEFT JOIN {ident(p.stage)}.{ident(ids)} i "
                    f"ON s.id=i.id WHERE i.id IS NULL; DROP TABLE {ident(p.stage)}.{ident(ids)};",
                    db=p.stage)
            info["mode"] = "delta"
        else:
            self._truncate(dts)
            self._copy(p.src_gdb, dts, f"mailbox_id={p.src_id}", timeout=timeout)
            info["mode"] = "full"
        others = self._other_tables()
        self._truncate(others + list(p.zimbra_tables) + ["mailbox"])
        self._copy(p.src_gdb, others, f"mailbox_id={p.src_id}", timeout=timeout)
        self._copy("zimbra", list(p.zimbra_tables), f"mailbox_id={p.src_id}", timeout=timeout)
        self._copy("zimbra", ["mailbox"], f"id={p.src_id}", timeout=timeout)
        info["seconds"] = round(time.monotonic() - t0, 2)
        return info

    def refull_delta_tables(self, timeout: Optional[float] = None) -> None:
        dts = self._delta_tables()
        self._truncate(dts)
        self._copy(self.p.src_gdb, dts, f"mailbox_id={self.p.src_id}", timeout=timeout)

    # ------------------------------------------------------------------ verification
    def _fingerprints(self, side: str, mode: str,
                      timeout: Optional[float] = None) -> dict[str, tuple[str, str]]:
        """side: src | stage | live. live applies the remap on the *source* expression instead."""
        p, out = self.p, {}
        for t, ti in p.group_tables.items():
            if side == "src":
                schema, mid, loc = p.src_gdb, p.src_id, None
            elif side == "stage":
                schema, mid, loc = p.stage, p.src_id, None
            elif side == "src_mapped":
                schema, mid, loc = p.src_gdb, p.src_id, locator_case_sql("`locator`", p.vol_map)
            else:
                schema, mid, loc = p.dst_gdb, p.dst_id, None
            out[t] = self._fp(side, schema, ti, "mailbox_id", mid, loc, mode, timeout=timeout)
        for t, ti in p.zimbra_tables.items():
            schema = p.stage if side == "stage" else "zimbra"
            mid = p.dst_id if side == "live" else p.src_id
            out[f"zimbra.{t}"] = self._fp(side, schema, ti, "mailbox_id", mid, None, mode,
                                         timeout=timeout)
        schema = p.stage if side == "stage" else "zimbra"
        mid = p.dst_id if side == "live" else p.src_id
        out["zimbra.mailbox"] = self._fp(side, schema, p.mailbox_table, "id", mid, None, mode,
                                         exclude=MAILBOX_ROW_LOCAL_COLS, timeout=timeout)
        return out

    def _fp(self, side, schema, ti, key, val, loc, mode, exclude=(),
            timeout: Optional[float] = None) -> tuple[str, str]:
        db = self.src if side.startswith("src") else self.dst
        if mode == "count":
            return (db.scalar(f"SELECT COUNT(*) FROM {ident(schema)}.{ident(ti.name)} "
                              f"WHERE {ident(key)}={int(val)}", timeout=timeout) or "0", "-")
        r = db.query(checksum_sql(schema, ti, key, val, loc, exclude), timeout=timeout)
        return (r[0][0], r[0][1]) if r else ("0", "0")

    def compare(self, a: str, b: str, mode: str, timeout: Optional[float] = None) -> list[str]:
        if mode == "none" or self.dry:
            return []
        fa, fb = self._fingerprints(a, mode, timeout), self._fingerprints(b, mode, timeout)
        return [f"{t}: {a}={fa[t]} {b}={fb.get(t)}" for t in fa if fa[t] != fb.get(t)]

    # ------------------------------------------------------------------ merge / delete
    def merge_sql(self) -> str:
        p = self.p
        loc = locator_case_sql("`locator`", p.vol_map)
        stmts = ["SET FOREIGN_KEY_CHECKS=0;", "START TRANSACTION;"]
        mb_over = {"group_id": str(p.dst_group), "index_volume_id": str(p.dst_index_volume),
                   "itemcache_checkpoint": "`itemcache_checkpoint` + 1"}
        stmts.append(insert_select_sql("zimbra", p.stage, p.mailbox_table, "id", p.dst_id, mb_over))
        for ti in p.zimbra_tables.values():
            stmts.append(insert_select_sql("zimbra", p.stage, ti, "mailbox_id", p.dst_id))
        # parents before children is irrelevant with FK checks off; order kept for readability
        for ti in p.group_tables.values():
            over = {"locator": loc} if ti.has_locator else {}
            stmts.append(insert_select_sql(p.dst_gdb, p.stage, ti, "mailbox_id", p.dst_id, over))
        stmts.append(f"DELETE FROM zimbra.deleted_account WHERE email={q(p.account)};")
        stmts += ["COMMIT;", "SET FOREIGN_KEY_CHECKS=1;"]
        return "\n".join(stmts)

    def merge(self, timeout: Optional[float] = None) -> None:
        self.dst.execute(self.merge_sql(), timeout=timeout)

    @staticmethod
    def delete_mailbox_sql(mailbox_id: int, gdb: str, group_tables: list[str],
                           zimbra_tables: list[str]) -> str:
        mid = int(mailbox_id)
        stmts = ["SET FOREIGN_KEY_CHECKS=0;", "START TRANSACTION;"]
        stmts += [f"DELETE FROM {ident(gdb)}.{ident(t)} WHERE mailbox_id={mid};" for t in group_tables]
        stmts += [f"DELETE FROM zimbra.{ident(t)} WHERE mailbox_id={mid};" for t in zimbra_tables]
        stmts.append(f"DELETE FROM zimbra.mailbox WHERE id={mid};")
        stmts += ["COMMIT;", "SET FOREIGN_KEY_CHECKS=1;"]
        return "\n".join(stmts)

    def delete_live_dst(self) -> None:
        self.dst.execute(self.delete_mailbox_sql(self.p.dst_id, self.p.dst_gdb,
                                                 list(self.p.group_tables), list(self.p.zimbra_tables)))

    def live_max_change(self) -> int:
        """Used by rollback to prove the destination copy is still untouched."""
        parts = [f"SELECT GREATEST(COALESCE(MAX(mod_metadata),0), COALESCE(MAX(mod_content),0)) AS w "
                 f"FROM {ident(self.p.dst_gdb)}.{ident(t)} WHERE mailbox_id={self.p.dst_id}"
                 for t in self._delta_tables()]
        return int(self.dst.scalar(f"SELECT MAX(w) FROM ({' UNION ALL '.join(parts)}) x") or 0)


def reserve_mailbox_id(dst: MySQL, explicit: Optional[int] = None) -> int:
    """DbMailbox.getNextMailboxId equivalent, atomic in one transaction (row lock on
    current_volumes serialises with mailboxd's own allocations)."""
    if explicit:
        sql = (f"START TRANSACTION; UPDATE zimbra.current_volumes SET next_mailbox_id="
               f"GREATEST(next_mailbox_id, {int(explicit) + 1}); "
               f"SELECT COUNT(*) FROM zimbra.mailbox WHERE id={int(explicit)}; COMMIT;")
        rows = dst.query(sql)
        if rows and rows[-1][0] != "0":
            raise RuntimeError(f"mailbox id {explicit} already in use on {dst.host.name}")
        return int(explicit)
    rows = dst.query("START TRANSACTION; UPDATE zimbra.current_volumes SET next_mailbox_id="
                     "next_mailbox_id + 1; SELECT next_mailbox_id - 1 FROM zimbra.current_volumes; COMMIT;")
    if not rows:
        raise RuntimeError("could not allocate mailbox id")
    return int(rows[-1][0])
