import json

from zmove import caches, topology

from fakes import FakeHost

Q = "\n".join(json.dumps(m) for m in [
    {"queue_name": "deferred", "queue_id": "A1", "recipients": [{"address": "User@X"}]},
    {"queue_name": "deferred", "queue_id": "B2", "recipients": [{"address": "other@x"}]},
    {"queue_name": "active", "queue_id": "C3", "recipients": [{"address": "user@x"}]},
    {"queue_name": "deferred", "queue_id": "D4", "recipients": [{"address": "alias@x"},
                                                                 {"address": "other@x"}]},
])


def test_queue_ids_only_for_this_account():
    assert caches.queue_ids_for(Q, ["user@x", "alias@x"]) == ["A1", "D4"]
    assert caches.queue_ids_for("garbage\n", ["user@x"]) == []


def test_flush_never_flushes_whole_queue():
    h = FakeHost("mta", answers={"postqueue -j": Q})
    n = caches.flush_mta_queues([h], "user@x", aliases=["alias@x"])
    assert n == 2
    assert not any(c.endswith(" -f") or " -f;" in c for c in h.cmds)
    assert any("-i A1" in c and "-i D4" in c and "B2" not in c for c in h.cmds)


def test_flush_skips_when_queue_listing_unsupported():
    h = FakeHost("mta", fail=("postqueue -j",))
    assert caches.flush_mta_queues([h], "user@x") == 0
    assert len(h.cmds) == 1


def test_proxy_purge_covers_aliases_and_memcached_has_timeout():
    h = FakeHost("d", answers={"gcf": "zimbraMemcachedClientServerList: m1:11211"})
    caches.purge_proxy_routes(h, "user@x", ["alias@x", "user@x"])
    assert sum("zmproxypurge" in c for c in h.cmds) == 2
    caches.delete_memcached_keys(h, "id1")
    assert all(c.startswith("timeout ") for c in h.cmds if "/dev/tcp" in c)


def test_account_info_canonical_name_and_aliases():
    out = ("# name user@x\nzimbraId: id1\nzimbraMailHost: s\nzimbraAccountStatus: active\n"
           "mail: user@x\nmail: alias@x\nzimbraMailAlias: alias@x\nzimbraMailDeliveryAddress: user@x\n")
    h = FakeHost("d", answers={"zmprov -l ga": out})
    a = topology.account_info(h, "alias@x")
    assert a.name == "user@x" and a.aliases == ["alias@x"]
