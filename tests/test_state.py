import socket
import time

import pytest

from zmove.state import AlreadyMoving, State


def test_one_active_job_per_account(tmp_path):
    st = State(str(tmp_path / "s.db"))
    j = st.create("a@x", "id1", "s", "d", {})
    with pytest.raises(AlreadyMoving):
        st.create("a@x", "id1", "s", "d2", {})
    j.status = "aborted"
    st.save(j)
    st.create("a@x", "id1", "s", "d2", {})


def test_owner_alive(tmp_path):
    st = State(str(tmp_path / "s.db"))
    j = st.create("a@x", "id1", "s", "d", {})
    assert not st.owner_alive(j)                       # it is us
    j.owner = f"{socket.gethostname()}:{2 ** 22 + 12345}"   # same host, dead pid
    assert not st.owner_alive(j)
    j.owner = f"{socket.gethostname()}:1"               # pid 1 always exists
    assert st.owner_alive(j)
    j.owner, j.heartbeat = "otherhost:1", time.time()
    assert st.owner_alive(j)
    j.heartbeat = time.time() - 3600
    assert not st.owner_alive(j)


def test_heartbeat_thread(tmp_path):
    st = State(str(tmp_path / "s.db"))
    j = st.create("a@x", "id1", "s", "d", {})
    st.db.execute("UPDATE jobs SET heartbeat=0 WHERE id=?", (j.id,))
    stop = st.start_heartbeat(j.id, interval=0.05)
    time.sleep(0.3)
    stop.set()
    assert st.get(j.id).heartbeat > time.time() - 5
