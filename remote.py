"""SSH execution layer (OpenSSH client, multiplexed via ControlMaster)."""
from __future__ import annotations

import logging
import os
import shlex
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass
from typing import Optional

from .config import Settings

log = logging.getLogger("zmove.remote")


class RemoteError(RuntimeError):
    def __init__(self, host: str, cmd: str, rc: int, stderr: str):
        super().__init__(f"[{host}] rc={rc}: {stderr.strip()[-2000:]}\n  cmd: {cmd[:500]}")
        self.host, self.cmd, self.rc, self.stderr = host, cmd, rc, stderr


@dataclass
class Result:
    rc: int
    out: str
    err: str
    seconds: float


_CTRL_DIR = os.path.join(tempfile.gettempdir(), f"zmove-ssh-{os.getuid()}")


class Host:
    def __init__(self, name: str, settings: Settings, dry_run: bool = False):
        self.name = name
        self.s = settings
        self.dry_run = dry_run
        os.makedirs(_CTRL_DIR, mode=0o700, exist_ok=True)

    def __repr__(self) -> str:
        return f"Host({self.name})"

    def ssh_argv(self) -> list[str]:
        argv = ["ssh", "-p", str(self.s.ssh_port),
                "-o", "BatchMode=yes",
                "-o", "ServerAliveInterval=30",
                "-o", "ControlMaster=auto",
                "-o", f"ControlPath={_CTRL_DIR}/%r@%h:%p",
                "-o", "ControlPersist=600"]
        if self.s.ssh_key:
            argv += ["-i", self.s.ssh_key]
        argv += list(self.s.ssh_options)
        argv.append(f"{self.s.ssh_user}@{self.name}")
        return argv

    def wrap(self, cmd: str) -> str:
        """Run as zimbra with a login shell (PATH includes /opt/zimbra/bin)."""
        inner = f"set -o pipefail; {cmd}"
        if self.s.run_as_zimbra and self.s.ssh_user != "zimbra":
            return f"sudo -n -iu zimbra bash -lc {shlex.quote(inner)}"
        return f"bash -lc {shlex.quote(inner)}"

    def run(self, cmd: str, *, input: Optional[str] = None, check: bool = True,
            timeout: Optional[float] = None, mutating: bool = True) -> Result:
        full = self.wrap(cmd)
        log.debug("[%s] $ %s", self.name, cmd)
        if self.dry_run and mutating:
            log.info("[dry-run][%s] %s", self.name, cmd[:300])
            return Result(0, "", "", 0.0)
        t0 = time.monotonic()
        p = subprocess.run(self.ssh_argv() + [full], input=input, capture_output=True,
                           text=True, timeout=timeout)
        r = Result(p.returncode, p.stdout, p.stderr, time.monotonic() - t0)
        if check and p.returncode != 0:
            raise RemoteError(self.name, cmd, p.returncode, p.stderr)
        return r

    def out(self, cmd: str, **kw) -> str:
        kw.setdefault("mutating", False)
        return self.run(cmd, **kw).out.strip()


def pipe(src: Host, src_cmd: str, dst: Host, dst_cmd: str, *,
         timeout: Optional[float] = None, dry_run: bool = False, compress: bool = True) -> float:
    """Stream stdout of src_cmd (on src) into stdin of dst_cmd (on dst) through the control node.

    Both exit codes are checked (a failing producer must never yield a half import).
    With compress (default) the source gzips the stream and the destination gunzips it,
    so multi-GB dumps do not saturate the control node's links uncompressed.
    Returns elapsed seconds.
    """
    if dry_run:
        log.info("[dry-run] %s:%s | %s:%s", src.name, src_cmd[:200], dst.name, dst_cmd[:200])
        return 0.0
    if compress:
        src_cmd = f"{src_cmd} | gzip -1"
        dst_cmd = "gzip -dc | " + dst_cmd
    t0 = time.monotonic()
    p1 = subprocess.Popen(src.ssh_argv() + [src.wrap(src_cmd)], stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)
    p2 = subprocess.Popen(dst.ssh_argv() + [dst.wrap(dst_cmd)], stdin=p1.stdout,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert p1.stdout is not None and p1.stderr is not None
    p1.stdout.close()  # let p1 get SIGPIPE if p2 dies
    # drain the producer's stderr concurrently: a chatty mysqldump must not fill the pipe
    # buffer and block while we wait on the consumer
    e1_buf: list[bytes] = []
    drain = threading.Thread(target=lambda: e1_buf.append(p1.stderr.read()), daemon=True)
    drain.start()
    try:
        _, e2 = p2.communicate(timeout=timeout)
        p1.wait(timeout=60)
    except subprocess.TimeoutExpired:
        for p in (p1, p2):
            p.kill()
        for p in (p1, p2):
            p.wait()            # reap: no zombie ssh processes
        raise
    finally:
        drain.join(timeout=5)
    e1 = b"".join(e1_buf)
    if p1.returncode != 0:
        raise RemoteError(src.name, src_cmd, p1.returncode, e1.decode(errors="replace"))
    if p2.returncode != 0:
        raise RemoteError(dst.name, dst_cmd, p2.returncode, e2.decode(errors="replace"))
    return time.monotonic() - t0
