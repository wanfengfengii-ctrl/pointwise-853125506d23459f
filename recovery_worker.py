"""Subprocess worker for crash-boundary recovery checks.

Runs the real upgrade + poll loop against a file database and SIGKILLs itself
at the first hook phase matching the stop argument.  Every resume/fetch call
is appended to a sidecar file, so the parent can prove -- across the killed
process and the resumed process -- that an accepted page is never refetched.

Modes:
  run <db> <stop_phase>   seed a legacy database, upgrade, then poll; kill at
                          the first hook whose phase equals stop_phase
  resume <db> <stop_phase>  reopen an existing file database and continue;
                          upgrade() is called (idempotent no-op if already
                          upgraded) before the poll loop
"""
import os
import signal
import sqlite3
import sys

sys.path.insert(0, "/workspace")

from collector import Collector
from migration import upgrade
from samples import seed
from vendor.sdk2 import Client

MODE = sys.argv[1]
DB_PATH = sys.argv[2]
STOP_PHASE = sys.argv[3]
CALLS_PATH = DB_PATH + ".calls"


class RecordingClient(Client):
    def resume(self, legacy_checkpoint):
        with open(CALLS_PATH, "a") as fh:
            fh.write("resume:%d\n" % legacy_checkpoint)
        return super().resume(legacy_checkpoint)

    def fetch(self, cursor):
        with open(CALLS_PATH, "a") as fh:
            fh.write("fetch:%d\n" % cursor)
        return super().fetch(cursor)


def hook(phase):
    if phase == STOP_PHASE:
        # Hard kill: no finally blocks, no Python-level cleanup.  A pre-commit
        # kill leaves the hot SQLite journal to roll back on reopen; a
        # post-commit kill leaves the durable new state in place.
        os.kill(os.getpid(), signal.SIGKILL)


# Composite stop points for gaps that sit between the six commit hooks:
# the page is durably accepted but its publication sweep has not committed.
def make_hook(stop_phase):
    if stop_phase == "page_accepted_before_publish":
        # Kill right after the first page (cursor=12) is durably accepted:
        # A and B are staged, no publication has committed on that page.
        def composite(phase):
            if phase == "page.after_commit":
                os.kill(os.getpid(), signal.SIGKILL)
        return composite
    if stop_phase == "between_publishes":
        # Kill right after transaction B's publish commit, while A stays open.
        seen = {"n": 0}

        def composite(phase):
            if phase == "publish.after_commit":
                seen["n"] += 1
                if seen["n"] == 1:
                    os.kill(os.getpid(), signal.SIGKILL)
        return composite
    return hook


def main():
    db = sqlite3.connect(DB_PATH)
    try:
        client = RecordingClient()
        phase_hook = make_hook(STOP_PHASE)
        if MODE == "run":
            seed(db, 3)
        upgrade(db, client, phase_hook)
        collector = Collector(db, client, phase_hook)
        while True:
            result = collector.poll()
            if result["done"]:
                break
    finally:
        db.close()


if __name__ == "__main__":
    main()
