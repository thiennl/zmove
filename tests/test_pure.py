import pytest

from zmove import config, db, paths
from zmove.paths import Volume, map_volumes


def vol(id, type=1, path="/opt/zimbra/store", fb=12, fgb=8, st=1):
    return Volume(id, type, f"v{id}", path, fb, fgb, 12, 8, store_type=st)


def test_group_and_dirs():
    assert paths.mailbox_group_id(1, 100) == 1
    assert paths.mailbox_group_id(101, 100) == 1
    assert paths.mailbox_group_id(100, 100) == 100
    assert vol(1).mailbox_dir(5) == "/opt/zimbra/store/0/5"
    assert vol(1).mailbox_dir(5, "sid") == "/opt/zimbra/store/sid/0/5"
    assert vol(1).blob_relpath(4097, 3) == "msg/1/4097-3.msg"


def test_map_volumes_layout_mismatch():
    with pytest.raises(ValueError):
        map_volumes([vol(1)], [vol(2, fb=10)], 2, None, 3)


def test_map_volumes_secondary_falls_back_to_primary():
    m = map_volumes([vol(1), vol(2, type=2), vol(3, type=10)], [vol(4), vol(5, type=10)], 4, None, 5)
    assert m == {1: 4, 2: 4, 3: 5}


def test_locator_case_and_transport():
    assert paths.locator_case_sql("`locator`", {1: 1}) == "`locator`"
    assert "WHEN '1' THEN '4'" in paths.locator_case_sql("`locator`", {1: 4})
    assert paths.replace_host_in_transport("lmtp:a:7025", "b") == "lmtp:b:7025"
    assert paths.replace_host_in_transport("smtp:[relay]:25", "b") == "smtp:[relay]:25"
    assert paths.replace_host_in_transport("", "b") == "lmtp:b:7025"
    assert paths.replace_host_in_transport("lmtp:a:7025", "b", 7026) == "lmtp:b:7026"


def test_ident_and_quote():
    assert db.ident("mail_item") == "`mail_item`"
    with pytest.raises(ValueError):
        db.ident("x`; drop")
    assert db.q("o'b\\") == "'o\\'b\\\\'"


def test_checksum_and_insert_sql():
    ti = db.TableInfo("g", "mail_item", ["mailbox_id", "id", "locator"], [])
    sql = db.checksum_sql("g", ti, "mailbox_id", 7, "(CASE x END)")
    assert "WHERE `mailbox_id`=7" in sql and "(CASE x END)" in sql and "`mailbox_id`," not in sql
    ins = db.insert_select_sql("dst", "stage", ti, "mailbox_id", 9, {"locator": "L"})
    assert ins.startswith("INSERT INTO `dst`.`mail_item` (`mailbox_id`, `id`, `locator`) SELECT 9, `id`, L")


def test_mysql_my_cnf_is_configurable():
    m = db.MySQL(None, "mysql", "mysqldump", "/srv/zimbra/conf/my.cnf")
    assert "--defaults-file=/srv/zimbra/conf/my.cnf" in m.client_cmd()
    assert "--defaults-file=/srv/zimbra/conf/my.cnf" in m.dump_cmd("g", ["t"], "1=1")


def test_config_load_and_validate(tmp_path):
    p = tmp_path / "z.ini"
    p.write_text("[zmove]\nmax_syncs = 3\nshared_volumes = 1:2, 3:3\nverify_tls = yes\n"
                 "cutover_window = 22:00-02:00\n")
    s = config.load(str(p))
    assert s.max_syncs == 3 and s.shared_volumes == {1: 2, 3: 3} and s.verify_tls is True
    p.write_text("[zmove]\nverify = maybe\n")
    with pytest.raises(ValueError):
        config.load(str(p))
