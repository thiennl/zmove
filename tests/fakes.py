from zmove.remote import Result


class FakeHost:
    """Records commands; answers from a {substring: output} table."""

    def __init__(self, name, dry_run=False, answers=None, fail=()):
        self.name, self.dry_run = name, dry_run
        self.cmds: list[str] = []
        self.answers = dict(answers or {})
        self.fail = tuple(fail)

    def run(self, cmd, *, input=None, check=True, timeout=None, mutating=True):
        self.cmds.append(cmd)
        if any(f in cmd for f in self.fail):
            if check:
                from zmove.remote import RemoteError
                raise RemoteError(self.name, cmd, 1, "boom")
            return Result(1, "", "boom", 0.0)
        for k, v in self.answers.items():
            if k in cmd:
                return Result(0, v, "", 0.0)
        return Result(0, "", "", 0.0)

    def out(self, cmd, **kw):
        return self.run(cmd, **kw).out.strip()


class FakeDbm:
    def __init__(self, dst_id=42, in_use=False):
        self.dropped = 0
        self.deleted = 0
        self.in_use = in_use
        self.dst = object()
        self.p = type("P", (), {"dst_id": dst_id})()

    def drop_stage(self):
        self.dropped += 1

    def delete_live_dst(self):
        self.deleted += 1
        self.in_use = False
