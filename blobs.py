"""Blob / Lucene index transfer (rsync) with NE-compatible convergence loop."""
from __future__ import annotations

import logging
import re
import shlex
from dataclasses import dataclass, field
from typing import Optional

from .config import Settings
from .remote import Host

log = logging.getLogger("zmove.blobs")


@dataclass
class DirPlan:
    """One destination mailbox dir fed by one or more source mailbox dirs."""
    kind: str                       # "msg" | "index"
    dst_volume: int
    dst_dir: str
    src_dirs: list[str] = field(default_factory=list)
    src_volumes: list[int] = field(default_factory=list)
    shared: bool = False            # src paths are visible on dst host (shared storage) -> local link copy
    external: bool = False          # object/S3-style volume: no filesystem tree, DB locator remap only

@dataclass
class SyncStats:
    seconds: float = 0.0
    files: int = 0
    bytes: int = 0

    def add(self, o: "SyncStats") -> None:
        self.seconds += o.seconds
        self.files += o.files
        self.bytes += o.bytes


_RE_FILES = re.compile(r"Number of regular files transferred:\s*([\d,]+)")
_RE_BYTES = re.compile(r"Total transferred file size:\s*([\d,]+)")


def parse_stats(out: str) -> tuple[int, int]:
    f = _RE_FILES.search(out)
    b = _RE_BYTES.search(out)
    n = lambda m: int(m.group(1).replace(",", "")) if m else 0  # noqa: E731
    return n(f), n(b)


class BlobSync:
    def __init__(self, s: Settings, src: Host, dst: Host):
        self.s, self.src, self.dst = s, src, dst

    def existing_src_dirs(self, plan: DirPlan) -> list[str]:
        """Filter source dirs that actually exist (a mailbox may have nothing on old volumes)."""
        if plan.shared:
            host = self.dst
        else:
            host = self.src
        keep = []
        for d in plan.src_dirs:
            if host.run(f"test -d {shlex.quote(d)}", check=False, mutating=False).rc == 0:
                keep.append(d)
        return keep

    def _cmd(self, plan: DirPlan, srcs: list[str], final: bool) -> tuple[Host, str]:
        # online passes yield to user I/O; the final pass runs while users are locked -> full speed
        nice = "ionice -c2 -n7 nice -n 10 " if (self.s.ionice and not final) else ""
        opts = ["-a", "--numeric-ids", "--stats", "--partial", "--delete"]
        # Multiple source dirs (several old primary volumes -> one dest volume) are merged by rsync;
        # --delete then removes only files absent from *all* sources. Hard links preserved per run.
        opts.append("-H")
        if final:
            # bit-rot safety: the last pass compares content, not just size/mtime
            opts.append("--checksum")
        if self.s.rsync_bwlimit_kbps and not final:
            opts.append(f"--bwlimit={self.s.rsync_bwlimit_kbps}")
        srcs_q = " ".join(shlex.quote(d.rstrip("/") + "/") for d in srcs)
        dq = shlex.quote(plan.dst_dir.rstrip("/") + "/")
        if plan.shared:
            # shared storage: hard-link unchanged files from the old dir, copy nothing over the wire
            link = " ".join(f"--link-dest={shlex.quote(d.rstrip('/'))}" for d in srcs)
            cmd = (f"mkdir -p {shlex.quote(plan.dst_dir)} && {nice}{self.s.rsync_bin} {' '.join(opts)} "
                   f"{link} {srcs_q} {dq}")
            return self.dst, cmd
        e = f"ssh -p {self.s.node_ssh_port} {self.s.node_ssh_options}"
        rpath = f"mkdir -p {shlex.quote(plan.dst_dir)} && {self.s.rsync_bin}"
        cmd = (f"{nice}{self.s.rsync_bin} {' '.join(opts)} -e {shlex.quote(e)} "
               f"--rsync-path={shlex.quote(rpath)} {srcs_q} "
               f"{shlex.quote(self.s.node_ssh_user + '@' + self.dst.name)}:{dq}")
        return self.src, cmd

    def sync_once(self, plans: list[DirPlan], final: bool = False,
                  timeout: Optional[float] = None) -> SyncStats:
        total = SyncStats()
        for p in plans:
            if p.external:
                # external/object storage: no filesystem tree to copy, the DB
                # locator remap keeps pointing at the shared store
                log.info("skipping external volume plan %s -> %s", p.kind, p.dst_dir)
                continue
            srcs = self.existing_src_dirs(p)
            if not srcs:
                if final:
                    # stray content from an older attempt must not survive under the new id
                    self.remove_dirs(self.dst, [p.dst_dir])
                continue
            host, cmd = self._cmd(p, srcs, final)
            r = host.run(cmd, timeout=timeout)
            f, b = parse_stats(r.out)
            total.add(SyncStats(r.seconds, f, b))
            log.info("rsync %s -> %s:%s files=%d bytes=%d %.1fs", p.kind, self.dst.name, p.dst_dir,
                     f, b, r.seconds)
        return total

    def converge(self, plans: list[DirPlan], max_syncs: int, threshold_ms: int,
                 on_pass=None, timeout: Optional[float] = None) -> list[SyncStats]:
        """NE semantics (MoveMailboxRequest doc): repeat until a pass takes <= syncFinishThreshold
        or maxSyncs passes ran. Improvement: also stop early when a pass transferred nothing."""
        passes: list[SyncStats] = []
        for i in range(max(1, max_syncs)):
            st = self.sync_once(plans, timeout=timeout)
            passes.append(st)
            if on_pass:
                on_pass(i + 1, st)
            if st.seconds * 1000 <= threshold_ms or st.files == 0:
                break
        return passes

    def inventory(self, host: Host, dirs: list[str]) -> tuple[int, int]:
        """(file count, apparent bytes) under the given dirs, as rsync would merge them.

        Several source dirs are merged into one destination dir, so a relative path present in
        more than one source (blob names are <item>-<rev>.msg, i.e. same name == same content)
        counts once, exactly as it ends up on the destination."""
        if not dirs:
            return 0, 0
        dq = " ".join(shlex.quote(d) for d in dirs)
        out = host.out(f"find {dq} -type f -printf '%P\\t%s\\n' 2>/dev/null | "
                       "awk -F'\\t' '!seen[$1]++ {n++; s+=$2} END {printf \"%d %d\", n, s}'")
        parts = out.split()
        return (int(parts[0]), int(parts[1])) if len(parts) == 2 else (0, 0)

    def du(self, host: Host, dirs: list[str]) -> int:
        return self.inventory(host, dirs)[1]

    def free_bytes(self, host: Host, path: str) -> Optional[int]:
        out = host.out(f"df -PB1 {shlex.quote(path)} | awk 'NR==2 {{print $4}}'")
        return int(out) if out.isdigit() else None

    def remove_dirs(self, host: Host, dirs: list[str]) -> None:
        for d in dirs:
            # defensive: only remove paths that look like <root>/<bucket>/<mboxid>
            if not re.search(r"/\d+/\d+/?$", d):
                raise ValueError(f"refusing to remove unexpected path {d}")
            host.run(f"rm -rf --one-file-system {shlex.quote(d)}")
