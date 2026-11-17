"""Pure functions mirroring Zimbra's on-disk / DB layout.

References (Zimbra/zm-mailbox):
  - DbMailbox.calculateMailboxGroupId()
  - Volume.getMailboxDir() / getBlobDir()
  - FileBlobStore.getBlobPath()
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

TYPE_MESSAGE = 1
TYPE_MESSAGE_SECONDARY = 2
TYPE_INDEX = 10

STORE_INTERNAL = 1


def mailbox_group_id(mailbox_id: int, num_groups: int) -> int:
    """DbMailbox.calculateMailboxGroupId: (id - 1) % groups + 1 (never 0)."""
    if mailbox_id < 1:
        raise ValueError("mailbox_id must be >= 1")
    if num_groups < 1:
        raise ValueError("num_groups must be >= 1")
    return (mailbox_id - 1) % num_groups + 1


def group_db(group_id: int) -> str:
    return f"mboxgroup{group_id}"


@dataclass(frozen=True)
class Volume:
    id: int
    type: int
    name: str
    path: str
    file_bits: int
    file_group_bits: int
    mailbox_bits: int
    mailbox_group_bits: int
    compress_blobs: bool = False
    store_type: int = STORE_INTERNAL

    @property
    def is_message(self) -> bool:
        return self.type in (TYPE_MESSAGE, TYPE_MESSAGE_SECONDARY)

    @property
    def is_index(self) -> bool:
        return self.type == TYPE_INDEX

    def layout_key(self) -> tuple:
        """Two volumes with the same layout_key name files identically inside a mailbox dir."""
        return (self.file_bits, self.file_group_bits)

    def mailbox_dir(self, mailbox_id: int, server_id_prefix: Optional[str] = None) -> str:
        """Volume.getMailboxDir(mboxId, null): <root>/[serverId/]<hash>/<mboxId>"""
        mask = (1 << self.mailbox_group_bits) - 1
        bucket = (mailbox_id >> self.mailbox_bits) & mask
        root = self.path.rstrip("/")
        if server_id_prefix:
            root = f"{root}/{server_id_prefix}"
        return f"{root}/{bucket}/{mailbox_id}"

    def blob_relpath(self, item_id: int, revision: int) -> str:
        """Path of a blob relative to mailbox_dir: msg/<bucket>/<item>-<rev>.msg"""
        mask = (1 << self.file_group_bits) - 1
        bucket = (item_id >> self.file_bits) & mask
        name = f"{item_id}-{revision}.msg" if revision >= 0 else f"{item_id}.msg"
        return f"msg/{bucket}/{name}"


def map_volumes(src_vols: list[Volume], dst_vols: list[Volume],
                dst_current_msg: int, dst_current_secondary: Optional[int],
                dst_current_index: int, explicit: Optional[dict[int, int]] = None) -> dict[int, int]:
    """Map every source volume id to a destination volume id.

    Primary message volumes -> dest current primary, secondary (HSM) -> dest current secondary
    (or primary if dest has none), index -> dest current index. ``explicit`` overrides
    (e.g. shared storage where src vol 3 == dst vol 3).
    Raises ValueError on layout mismatch, since blob file names inside the mailbox dir
    depend on file_bits/file_group_bits.
    """
    by_id = {v.id: v for v in dst_vols}
    out: dict[int, int] = {}
    for sv in src_vols:
        is_explicit = bool(explicit and sv.id in explicit)
        if sv.store_type != STORE_INTERNAL and not is_explicit:
            raise ValueError(f"source volume {sv.id} ({sv.name}) is external storage; not supported")
        if explicit and sv.id in explicit:
            target = explicit[sv.id]
        elif sv.type == TYPE_MESSAGE:
            target = dst_current_msg
        elif sv.type == TYPE_MESSAGE_SECONDARY:
            target = dst_current_secondary if dst_current_secondary else dst_current_msg
        elif sv.type == TYPE_INDEX:
            target = dst_current_index
        else:
            continue
        dv = by_id.get(target)
        if dv is None:
            raise ValueError(f"destination volume {target} not found")
        if dv.store_type != STORE_INTERNAL and not is_explicit:
            raise ValueError(f"destination volume {dv.id} ({dv.name}) is external storage; not supported")
        if sv.is_index != dv.is_index:
            raise ValueError(f"volume type mismatch: src {sv.id} -> dst {dv.id}")
        if sv.is_message and sv.layout_key() != dv.layout_key():
            if not (is_explicit and (sv.store_type != STORE_INTERNAL
                                    or dv.store_type != STORE_INTERNAL)):
                raise ValueError(
                    f"blob layout mismatch src vol {sv.id} bits={sv.layout_key()} "
                    f"vs dst vol {dv.id} bits={dv.layout_key()}; set explicit mapping to a compatible volume")
        out[sv.id] = dv.id
    return out


def locator_case_sql(column: str, mapping: dict[int, int]) -> str:
    """SQL expression remapping a FileBlobStore locator (volume id as string)."""
    pairs = [(s, d) for s, d in sorted(mapping.items()) if s != d]
    if not pairs:
        return column
    whens = " ".join(f"WHEN '{int(s)}' THEN '{int(d)}'" for s, d in pairs)
    return f"(CASE {column} {whens} ELSE {column} END)"


def replace_host_in_transport(transport: str, new_host: str, port: int = 7025) -> str:
    """'lmtp:mbx01.dom:7025' -> 'lmtp:mbx02.dom:<port>'.

    ``port`` is the destination's zimbraLmtpBindPort: the source's LMTP port is not
    necessarily the destination's. Only LMTP transports point at a mailbox server. Anything
    else (smtp relay, hold, error, VIP) is returned unchanged so a move never rewrites
    custom routing.
    """
    if not transport:
        return f"lmtp:{new_host}:{int(port)}"
    parts = transport.split(":")
    if parts[0].lower() != "lmtp":
        return transport
    return f"{parts[0]}:{new_host}:{int(port)}"
