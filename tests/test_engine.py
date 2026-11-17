import time

import pytest

from zmove import engine as E
from zmove.config import Settings
from zmove.dbmove import DbMove, DbPlan
from zmove.engine import Engine, MoveError
from zmove.state import Phase, State
from zmove.topology import AccountInfo

from fakes import FakeDbm, FakeHost


@pytest.fixture
def env(tmp_path, monkeypatch):
    st = State(str(tmp_path / "s.db"))
    eng = Engine(Settings(state_db=str(tmp_path / "s.db")), st)
    hosts = {n: FakeHost(n) for n in ("src", "dst")}
    eng._hosts.update(hosts)
    lockouts = []
    monkeypatch.setattr(eng, "_lockout", lambda server, acct, op: lockouts.append((server, op)))
    home = {"h": "src"}
    monkeypatch.setattr(E, "account_info",
                        lambda h, a: AccountInfo(a, "id1", home["h"], "lmtp:src:7025", "active"))
    job = st.create("o'brien@x", "id1", "src", "dst", dict(E.DEFAULT_OPTIONS))
    job.data.update(orig_status="closed", orig_transport="lmtp:src:7025",
                    copy={"search_index": True}, dst_mailbox_id=42)
    return eng, st, hosts, lockouts, home, job


def test_flip_and_post_flip_complete(env):
    eng, st, hosts, _, _, job = env
    job.phase = Phase.MERGED
    eng._flip(job)
    assert job.phase == Phase.FLIPPED
    assert any("zimbraMailHost dst zimbraMailTransport lmtp:dst:7025" in c for c in hosts["dst"].cmds)
    assert any("'o'\"'\"'brien@x'" in c for c in hosts["dst"].cmds)       # quoted account
    dbm = FakeDbm()
    eng._post_flip(job, dbm)
    assert job.phase == Phase.DONE and job.status == "done" and dbm.dropped == 1
    assert any("zimbraAccountStatus closed" in c for c in hosts["dst"].cmds)  # orig status kept


def test_abort_restores_source_even_without_context(env, monkeypatch):
    eng, st, hosts, lockouts, _, job = env
    job.phase = Phase.VERIFIED
    job.data.update(lock_deadline=time.time() - 1, locked_at=1.0, watermark=5)
    monkeypatch.setattr(eng, "_ctx", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("dst down")))
    eng.abort(job, reason="test")
    assert ("src", "end") in lockouts
    assert any("zimbraAccountStatus closed" in c for c in hosts["src"].cmds)
    j = st.get(job.id)
    assert j.status == "aborted" and j.phase == Phase.RESERVED
    assert "lock_deadline" not in j.data and "watermark" not in j.data


def test_abort_reports_unrestored_source(env, monkeypatch):
    eng, st, hosts, _, _, job = env
    job.phase = Phase.LOCKED
    monkeypatch.setattr(eng, "_ctx", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("dst down")))
    for h in hosts.values():
        h.fail = ("zimbraAccountStatus",)
    with pytest.raises(MoveError):
        eng.abort(job)
    j = st.get(job.id)
    assert j.status == "failed" and j.data.get("locking")


def test_abort_before_lock_does_not_touch_status(env, monkeypatch):
    eng, st, hosts, lockouts, _, job = env
    job.phase = Phase.PRESTAGED
    monkeypatch.setattr(eng, "_ctx", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")))
    eng.abort(job)
    assert not lockouts and not any("zimbraAccountStatus" in c for c in hosts["src"].cmds)


def test_cleanup_keeps_copy_claimed_by_later_job(env, monkeypatch):
    eng, st, hosts, _, _, job = env
    job.status, job.phase = "aborted", Phase.RESERVED
    st.save(job)
    later = st.create("o'brien@x", "id1", "src", "dst", {})
    later.status, later.phase, later.data = "done", Phase.DONE, {"dst_mailbox_id": 42}
    st.save(later)
    assert "belongs to job" in eng._dst_claimed_elsewhere(job)
    dbm = FakeDbm(in_use=True)
    removed = []
    bs = type("BS", (), {"remove_dirs": lambda self, h, d: removed.append(d)})()
    monkeypatch.setattr(E, "mailbox_id_in_use", lambda db, i: dbm.in_use)
    eng._cleanup_dst(job, dbm, [], bs)
    assert dbm.deleted == 0 and not removed and dbm.dropped == 1


def test_cleanup_refuses_when_account_homed_on_dst(env):
    eng, _, _, _, home, job = env
    home["h"] = "dst"
    assert "homed on dst" in eng._dst_claimed_elsewhere(job)


def test_check_runnable(env):
    eng, st, _, _, _, job = env
    job.data["rolled_back_at"] = 1.0
    with pytest.raises(MoveError):
        eng._check_runnable(job)
    job.data.pop("rolled_back_at")
    job.owner, job.heartbeat = "elsewhere:1", time.time()
    with pytest.raises(MoveError, match="still being run"):
        eng._check_runnable(job)
    job.options["force"] = True
    eng._check_runnable(job)


def test_bounded_timeout(env):
    _, _, _, _, _, job = env
    assert Engine._bounded(job, 50.0) == 50.0
    job.data["lock_deadline"] = time.time() + 10
    assert Engine._bounded(job, None) <= 10
    assert Engine._bounded(job, 3.0) == 3.0


class FakeMySQL:
    def __init__(self, schemas):
        self.schemas = schemas          # name -> set(tables)
        self.host = FakeHost("dst")
        self.mysqldump_bin, self.my_cnf = "mysqldump", "/opt/zimbra/conf/my.cnf"
        self.executed = []

    def schema_exists(self, s):
        return s in self.schemas

    def tables_with_column(self, s, c):
        return sorted(self.schemas.get(s, ()))

    def scalar(self, sql, **kw):
        cands = sorted(k for k, v in self.schemas.items()
                       if k.startswith("mboxgroup") and "mail_item" in v and f"'{k}'" not in sql)
        return cands[0] if cands else None

    def execute(self, sql, **kw):
        self.executed.append(sql)

    def client_cmd(self, db=None, batch=True, env=True):
        return f"mysql {db}"


def _dbm(dst):
    plan = DbPlan("a", "id", 1, 1, 7, 7, 3, "zmove_stage_x", {})
    return DbMove(plan, None, dst)


def test_ensure_group_fills_empty_schema():
    dst = FakeMySQL({"mboxgroup1": {"mail_item", "tombstone"}, "mboxgroup7": set()})
    _dbm(dst).ensure_dst_group_db()
    assert any("mysqldump" in c and "mboxgroup1" in c for c in dst.host.cmds)


def test_ensure_group_fills_half_imported_schema():
    dst = FakeMySQL({"mboxgroup1": {"mail_item", "tombstone"}, "mboxgroup7": {"mail_item"}})
    _dbm(dst).ensure_dst_group_db()
    assert dst.host.cmds


def test_ensure_group_ready_is_noop():
    dst = FakeMySQL({"mboxgroup1": {"mail_item", "tombstone"}, "mboxgroup7": {"mail_item", "tombstone"}})
    _dbm(dst).ensure_dst_group_db()
    assert not dst.host.cmds and not dst.executed


def test_dir_plans_reject_same_shared_dir(env):
    from zmove.paths import Volume
    from zmove.topology import ServerInfo
    eng, *_ = env
    eng.s.shared_volumes = {1: 1}
    v = Volume(1, 1, "v", "/nfs", 12, 8, 12, 8)
    mk = lambda n: ServerInfo(n, "", "", "", "", 100, False, False, False, False, volumes=[v])  # noqa: E731
    with pytest.raises(MoveError, match="same on shared storage"):
        eng._dir_plans(mk("s"), mk("d"), 5, 5, {1: 1}, False, 1)


def test_cli_imports_account_info():
    from zmove import cli, topology
    assert cli.account_info is topology.account_info


def test_resume_after_unjournaled_flip_commits_flipped(env):
    eng, st, hosts, lockouts, home, job = env
    job.phase = Phase.MERGED
    home["h"] = "dst"
    eng._relock(job)
    assert job.phase == Phase.FLIPPED and not lockouts
    assert not any("maintenance" in c for c in hosts["dst"].cmds)


def test_relock_resets_to_locked(env):
    eng, st, hosts, lockouts, home, job = env
    job.phase = Phase.VERIFIED
    eng._relock(job)
    assert job.phase == Phase.LOCKED and ("src", "start") in lockouts
    assert job.data["lock_deadline"] > time.time()
    assert any("zimbraAccountStatus maintenance" in c for c in hosts["dst"].cmds)
