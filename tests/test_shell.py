"""Exercise the real shell snippets locally (no SSH)."""
import subprocess

import pytest

from zmove import remote
from zmove.blobs import BlobSync
from zmove.config import Settings
from zmove.remote import Host, RemoteError


class LocalHost(Host):
    def ssh_argv(self):
        return []

    def wrap(self, cmd):
        return cmd

    def run(self, cmd, **kw):
        kw.pop("mutating", None)
        p = subprocess.run(["bash", "-c", "set -o pipefail; " + cmd], capture_output=True, text=True,
                           input=kw.get("input"), timeout=kw.get("timeout"))
        if kw.get("check", True) and p.returncode:
            raise RemoteError(self.name, cmd, p.returncode, p.stderr)
        return remote.Result(p.returncode, p.stdout, p.stderr, 0.0)


class PipeHost(Host):
    def ssh_argv(self):
        return ["bash", "-c"]

    def wrap(self, cmd):
        return "set -o pipefail; " + cmd


def test_inventory_dedupes_merged_sources(tmp_path):
    a, b = tmp_path / "a" / "msg" / "0", tmp_path / "b" / "msg" / "0"
    a.mkdir(parents=True)
    b.mkdir(parents=True)
    (a / "1-1.msg").write_text("abc")
    (b / "1-1.msg").write_text("abc")
    (b / "2-1.msg").write_text("hello")
    h = LocalHost("local", Settings())
    bs = BlobSync(Settings(), h, h)
    assert bs.inventory(h, [str(tmp_path / "a"), str(tmp_path / "b")]) == (2, 8)
    assert bs.inventory(h, []) == (0, 0)


def test_remove_dirs_guard():
    h = LocalHost("local", Settings())
    with pytest.raises(ValueError):
        BlobSync(Settings(), h, h).remove_dirs(h, ["/opt/zimbra/store"])


def test_pipe_ok_and_failing_producer(tmp_path):
    s = Settings()
    src, dst = PipeHost("s", s), PipeHost("d", s)
    out = tmp_path / "o"
    remote.pipe(src, "echo hi", dst, f"cat > {out}")
    assert out.read_text() == "hi\n"
    with pytest.raises(RemoteError):
        remote.pipe(src, "echo x; exit 3", dst, "cat >/dev/null")


def test_pipe_chatty_stderr_does_not_deadlock():
    s = Settings()
    src, dst = PipeHost("s", s), PipeHost("d", s)
    # 1 MB on stderr would block the producer without the drain thread
    remote.pipe(src, "head -c 1000000 /dev/zero | tr '\\0' x >&2; echo ok", dst, "cat >/dev/null",
                timeout=30)


def test_pipe_timeout_reaps():
    s = Settings()
    src, dst = PipeHost("s", s), PipeHost("d", s)
    with pytest.raises(subprocess.TimeoutExpired):
        remote.pipe(src, "sleep 30", dst, "cat >/dev/null", timeout=0.5)


def _sudo_ok(user):
    import shutil
    return shutil.which("sudo") and subprocess.run(
        ["sudo", "-n", "-u", user, "true"], capture_output=True).returncode == 0


def test_sudo_wrap_keeps_dollar_quotes_stdin_and_errors():
    """ssh_user=root + run_as_zimbra: the wrapped command must reach the target shell intact."""
    import getpass
    user = getpass.getuser()          # sudo to ourselves: same code path, no extra account
    if not _sudo_ok(user):
        pytest.skip("passwordless sudo not available")
    s = Settings(ssh_user="someone-else", run_as_zimbra=True, zimbra_user=user)
    h = Host("x", s)
    cmd = ("export MYSQL_PWD=\"$(printf %s \"s3cr'et\")\"; SOCK=\"$(echo /tmp/my.sock)\"; "
           "echo \"sock=$SOCK pwd=$MYSQL_PWD\"; tr a-z A-Z | awk '{print \"in:\" $1}'; "
           "printf '%s\\n' 'o'\"'\"'brien@x'")
    w = h.wrap(cmd)
    assert w.startswith(f"sudo -n -u {user} -H bash -lc ") and " -i" not in w.split("bash")[0]
    p = subprocess.run(["bash", "-c", w], input="hello stdin\n", capture_output=True, text=True)
    assert p.returncode == 0, p.stderr
    assert p.stdout.splitlines() == ["sock=/tmp/my.sock pwd=s3cr'et", "in:HELLO", "o'brien@x"]
    p = subprocess.run(["bash", "-c", h.wrap("false | tr a b")], capture_output=True, text=True)
    assert p.returncode != 0          # pipefail survives the sudo hop


def test_wrap_direct_zimbra_user_has_no_sudo():
    h = Host("x", Settings(ssh_user="zimbra", run_as_zimbra=True))
    assert h.wrap("true").startswith("bash -lc ")
