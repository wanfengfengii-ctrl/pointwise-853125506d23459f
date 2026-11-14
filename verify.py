"""Full delivery verification.

Keeps the frozen baseline and adds:
  * upgrade at every mapped legacy checkpoint, incl. exact body bytes,
    tombstones, receipts and audit-table immutability;
  * idempotent re-upgrade (no hooks, no resume, no data drift) and
    pre-commit failure rollback to a fully readable legacy database;
  * page-by-page SDK2 publication against the CONTRACT.md expectations,
    interleaved/cross-page transactions, aborts, zero-change commits and
    the unfinished-at-eof ValueError;
  * real file-database subprocess SIGKILL / reopen / resume checks at every
    commit boundary, proving no missed, duplicated or refetched work;
  * SDK1 is never imported on the upgraded path.
"""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile

from collector import Collector
from migration import upgrade
from samples import seed, ROWS
from store import Store
from vendor.sdk2 import Client as EventClient
import checks as baseline

WORKER = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      "recovery_worker.py")

# CONTRACT.md expected state after each accepted page of the default sample.
EXPECTED_PAGES = [
    (12, "旧公告", False, []),
    (15, "B先提交", False, ["b"]),
    (18, "A定稿", False, ["b", "a"]),
    (21, "A定稿", False, ["b", "a"]),
    (24, None, True, ["b", "a", "d"]),
    (26, None, True, ["b", "a", "d"]),
]
EXPECTED_NEW_RECEIPTS = [
    {"seq": 4, "token": "b", "changes": 1},
    {"seq": 5, "token": "a", "changes": 2},
    {"seq": 6, "token": "d", "changes": 2},
]


def snapshot(store):
    out = {}
    for key in ("x", "y", "z", "gone"):
        out[key] = store.read(key)
    out["receipts"] = store.receipts()
    return out


def upgraded_db(checkpoint=3):
    db = sqlite3.connect(":memory:")
    seed(db, checkpoint)
    client = EventClient()
    upgrade(db, client)
    return db, client


def check_mapping_all_checkpoints():
    for checkpoint, expected_cursor in ((0, 0), (1, 3), (2, 6), (3, 9)):
        db = sqlite3.connect(":memory:")
        try:
            seed(db, checkpoint)
            client = EventClient()
            upgrade(db, client)
            assert client.resume_calls == [checkpoint]
            assert db.execute("PRAGMA user_version").fetchone()[0] == 2
            row = db.execute(
                "SELECT read_cursor, eof FROM ingest_state").fetchone()
            assert row == (expected_cursor, 0)
            assert db.execute(
                "SELECT COUNT(*) FROM ingest_pending").fetchone()[0] == 0

            store = Store(db)
            # Legacy material remains queryable through the old API.
            if checkpoint == 0:
                assert store.read("x") is None
            else:
                assert store.read("x") == {
                    "id": "x", "deleted": False,
                    "data": {"title": "旧公告", "value": 1}}
            if checkpoint >= 3:
                assert store.read("gone") == {
                    "id": "gone", "deleted": True, "data": None}
                # Exact original bytes, not re-serialised text.
                body = db.execute(
                    "SELECT body FROM documents WHERE id='x'").fetchone()[0]
                assert body == ROWS[0]["payload"].encode("utf-8")
                tomb = db.execute(
                    "SELECT body, deleted FROM documents WHERE id='gone'"
                ).fetchone()
                assert tomb == (None, 1)
            assert store.receipts() == [
                {"seq": i + 1, "token": r["receipt"], "changes": 1}
                for i, r in enumerate(ROWS[:checkpoint])]

            # Legacy audit tables survive unchanged and still carry v1 shape.
            legacy_checkpoint = db.execute(
                "SELECT checkpoint FROM legacy_state").fetchone()[0]
            assert legacy_checkpoint == checkpoint
            legacy_count = db.execute(
                "SELECT COUNT(*) FROM legacy_items").fetchone()[0]
            assert legacy_count == len(
                {(r["id"]) for r in ROWS[:checkpoint]})

            # Resuming to the end still works from the mapped position.
            collector = Collector(db, client)
            for _ in range(20):
                result = collector.poll()
                if result["done"]:
                    break
            assert result["done"] is True
            assert result["cursor"] == 26
            # Every checkpoint converges on the same publication log: migrated
            # receipts plus the events published from the mapped position,
            # each token produced exactly once overall.
            assert [r["token"] for r in store.receipts()] == [
                "h1", "h2", "h3", "b", "a", "d"]
            assert len(store.receipts()) == 6
        finally:
            db.close()
    print("OK: upgrade at checkpoints 0-3, exact bytes, tombstones, receipts")


def check_idempotent_and_rollback():
    # Idempotent re-upgrade: no hooks, no resume call, no data drift.
    db, client = upgraded_db(3)
    try:
        store = Store(db)
        before = snapshot(store)
        phases = []
        assert upgrade(db, client, phases.append) is None
        assert phases == []
        assert client.resume_calls == [3]
        assert snapshot(store) == before
        assert db.execute(
            "SELECT read_cursor, eof FROM ingest_state").fetchone() == (9, 0)
    finally:
        db.close()

    # Failing before the upgrade commit leaves a complete legacy database.
    db = sqlite3.connect(":memory:")
    try:
        seed(db, 3)

        def fail_hook(phase):
            if phase == "upgrade.before_commit":
                raise RuntimeError("maintenance window abort")

        try:
            upgrade(db, EventClient(), fail_hook)
        except RuntimeError:
            pass
        else:
            raise AssertionError("hook failure should propagate")
        assert db.execute("PRAGMA user_version").fetchone()[0] == 1
        assert db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='documents'"
        ).fetchone() is None
        store = Store(db)
        assert store.read("x")["data"] == {"title": "旧公告", "value": 1}
        assert store.read("gone")["deleted"] is True
        assert len(store.receipts()) == 3
        # The preserved legacy library still ingests through SDK1.
        result = Collector(db).poll()
        assert result == {"cursor": 3, "published": [], "done": True}
        # A later retry in a new window completes the upgrade.
        upgrade(db)
        assert db.execute("PRAGMA user_version").fetchone()[0] == 2
        assert Store(db).read("gone")["deleted"] is True
    finally:
        db.close()

    # resume() failure happens before BEGIN: nothing is touched at all.
    db = sqlite3.connect(":memory:")
    try:
        seed(db, 3)
        class BrokenClient(EventClient):
            def resume(self, legacy_checkpoint):
                raise RuntimeError("network down")
        try:
            upgrade(db, BrokenClient())
        except RuntimeError:
            pass
        else:
            raise AssertionError("resume failure should propagate")
        assert db.execute("PRAGMA user_version").fetchone()[0] == 1
        assert db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='documents'"
        ).fetchone() is None
    finally:
        db.close()
    print("OK: idempotent upgrade, pre-commit rollback, resume failure safe")


def check_page_by_page():
    db, client = upgraded_db(3)
    try:
        store = Store(db)
        collector = Collector(db, client)
        all_new_tokens = []
        for index, (cursor, x_title, x_deleted, tokens) in enumerate(
                EXPECTED_PAGES, 1):
            result = collector.poll()
            assert result["cursor"] == cursor, (index, result)
            all_new_tokens.extend(result["published"])
            assert all_new_tokens == tokens
            assert result["done"] is (cursor == 26)
            x_row = store.read("x")
            if x_title is None:
                assert x_row["deleted"] is x_deleted and x_row["data"] is None
            else:
                value = {"旧公告": 1, "B先提交": 3, "A定稿": 4}[x_title]
                assert x_row == {
                    "id": "x", "deleted": False,
                    "data": {"title": x_title, "value": value}}
            if cursor >= 24:
                assert store.read("y") == {
                    "id": "y", "deleted": False,
                    "data": {"title": "新资料", "value": None}}
                # A real null value inside the body differs from a tombstone.
                assert store.read("y")["data"]["value"] is None
            else:
                assert store.read("y") is None
            assert store.read("z") is None
            assert store.read("gone") == {
                "id": "gone", "deleted": True, "data": None}
            # Accepted pages are never requested again.
            assert client.calls == [9, 12, 15, 18, 21, 24][:index]

        assert store.receipts()[3:] == EXPECTED_NEW_RECEIPTS
        receipts = store.receipts()
        assert len(receipts) == len({r["token"] for r in receipts})
        # Further polls publish nothing and do not refetch.
        again = collector.poll()
        assert again == {"cursor": 26, "published": [], "done": True}
        assert client.calls == [9, 12, 15, 18, 21, 24]
        assert db.execute(
            "SELECT COUNT(*) FROM ingest_pending").fetchone()[0] == 0
    finally:
        db.close()
    print("OK: page-by-page publication matches CONTRACT expectations")


def check_custom_semantics():
    # Hand-built source: zero-change commit, repeated same-id writes, abort,
    # interleaved commit ordering and an abort never producing a receipt.
    events = [
        {"kind": "begin", "tx": "T0"},
        {"kind": "commit", "tx": "T0", "token": "zero"},
        {"kind": "begin", "tx": "A"},
        {"kind": "upsert", "tx": "A", "id": "k",
         "body": {"v": "a1"}},
        {"kind": "begin", "tx": "B"},
        {"kind": "upsert", "tx": "B", "id": "k",
         "body": {"v": "b-wins"}},
        {"kind": "upsert", "tx": "A", "id": "k",
         "body": {"v": "a2"}},
        {"kind": "upsert", "tx": "A", "id": "m",
         "body": {"v": "m1"}},
        {"kind": "delete", "tx": "A", "id": "m"},
        {"kind": "commit", "tx": "B", "token": "tb"},
        {"kind": "begin", "tx": "C"},
        {"kind": "upsert", "tx": "C", "id": "k", "body": {"v": "dropped"}},
        {"kind": "abort", "tx": "C"},
        {"kind": "commit", "tx": "A", "token": "ta"},
    ]
    db = sqlite3.connect(":memory:")
    try:
        seed(db, 0)
        client = EventClient(events=events, page_size=2)
        upgrade(db, client)
        collector = Collector(db, client)
        store = Store(db)
        published = []
        for _ in range(20):
            r = collector.poll()
            published.extend(r["published"])
            if r["done"]:
                break
        assert published == ["zero", "tb", "ta"]
        # B commits first even though A began first; A's later commit then
        # overwrites in commit-position order.
        assert store.read("k")["data"] == {"v": "a2"}
        assert store.read("m") == {"id": "m", "deleted": True, "data": None}
        assert store.receipts() == [
            {"seq": 1, "token": "zero", "changes": 0},
            {"seq": 2, "token": "tb", "changes": 1},
            {"seq": 3, "token": "ta", "changes": 4}]
        assert db.execute(
            "SELECT COUNT(*) FROM ingest_pending").fetchone()[0] == 0
    finally:
        db.close()

    # Cross-page open transaction stays staged while later commits publish.
    events2 = [
        {"kind": "begin", "tx": "LONG"},
        {"kind": "upsert", "tx": "LONG", "id": "p", "body": {"v": 1}},
        {"kind": "begin", "tx": "FAST"},
        {"kind": "upsert", "tx": "FAST", "id": "q", "body": {"v": 2}},
        {"kind": "commit", "tx": "FAST", "token": "fast"},
        {"kind": "upsert", "tx": "LONG", "id": "p", "body": {"v": 3}},
        {"kind": "commit", "tx": "LONG", "token": "long"},
    ]
    db = sqlite3.connect(":memory:")
    try:
        seed(db, 0)
        client = EventClient(events=events2, page_size=2)
        upgrade(db, client)
        collector = Collector(db, client)
        store = Store(db)
        first = collector.poll()  # cursor 2: LONG open, nothing published
        assert first == {"cursor": 2, "published": [], "done": False}
        staged = db.execute(
            "SELECT tx, kind FROM ingest_pending ORDER BY position").fetchall()
        assert staged == [("LONG", "begin"), ("LONG", "upsert")]
        second = collector.poll()  # cursor 4: page holds FAST begin/upsert
        assert second["published"] == []
        assert store.read("q") is None
        third = collector.poll()  # cursor 6: FAST commits, LONG still open
        assert third["published"] == ["fast"]
        assert store.read("q")["data"] == {"v": 2}
        assert store.read("p") is None
        fourth = collector.poll()  # cursor 8: LONG commits across pages
        assert fourth["published"] == ["long"]
        assert store.read("p")["data"] == {"v": 3}
        final = collector.poll()
        assert final["done"] is True and final["published"] == []
        assert final["cursor"] == len(events2)
    finally:
        db.close()

    # Source ends with an unfinished transaction: error, progress retained,
    # previously published work is not rolled back.
    events3 = [
        {"kind": "begin", "tx": "OK"},
        {"kind": "upsert", "tx": "OK", "id": "k", "body": {"v": 1}},
        {"kind": "commit", "tx": "OK", "token": "ok"},
        {"kind": "begin", "tx": "DANGLING"},
        {"kind": "upsert", "tx": "DANGLING", "id": "z", "body": {"v": 2}},
    ]
    db = sqlite3.connect(":memory:")
    try:
        seed(db, 0)
        client = EventClient(events=events3, page_size=2)
        upgrade(db, client)
        collector = Collector(db, client)
        store = Store(db)
        raised = False
        for _ in range(10):
            try:
                collector.poll()
            except ValueError as exc:
                assert "unfinished" in str(exc)
                raised = True
                break
        assert raised
        assert db.execute("SELECT eof FROM ingest_state").fetchone()[0] == 1
        assert db.execute(
            "SELECT read_cursor FROM ingest_state").fetchone()[0] == len(events3)
        assert store.read("k")["data"] == {"v": 1}
        assert store.read("z") is None  # uncommitted material never visible
        assert [r["token"] for r in store.receipts()] == ["ok"]
        staged = db.execute(
            "SELECT tx, kind FROM ingest_pending ORDER BY position").fetchall()
        assert staged == [("DANGLING", "begin"), ("DANGLING", "upsert")]
        # Re-polling keeps raising without duplicating the receipt.
        try:
            collector.poll()
        except ValueError:
            pass
        else:
            raise AssertionError("dangling state must keep raising")
        assert [r["token"] for r in store.receipts()] == ["ok"]
    finally:
        db.close()
    print("OK: interleaving, abort, zero-change, cross-page, unfinished eof")


def check_transport_error():
    db, client = upgraded_db(3)
    try:
        store = Store(db)

        class FlakyClient(EventClient):
            def __init__(self):
                super().__init__()
                self.attempts = 0

            def fetch(self, cursor):
                self.attempts += 1
                if self.attempts == 1:
                    raise ConnectionError("transient transport failure")
                return super().fetch(cursor)

        flaky = FlakyClient()
        collector = Collector(db, flaky)
        try:
            collector.poll()
        except ConnectionError:
            pass
        else:
            raise AssertionError("transport error must propagate")
        # Nothing was accepted: cursor unchanged, no staging, no receipts.
        assert db.execute(
            "SELECT read_cursor FROM ingest_state").fetchone()[0] == 9
        assert db.execute(
            "SELECT COUNT(*) FROM ingest_pending").fetchone()[0] == 0
        assert len(store.receipts()) == 3
        # Retry refetches the same cursor and proceeds normally.
        result = collector.poll()
        assert result["cursor"] == 12 and result["published"] == []
        assert flaky.calls == [9]
    finally:
        db.close()
    print("OK: transport error propagates without damaging persisted state")


def _run_worker(db_path, stop_phase, mode="run"):
    if mode == "run" and os.path.exists(db_path + ".calls"):
        os.remove(db_path + ".calls")
    proc = subprocess.run(
        [sys.executable, WORKER, mode, db_path, stop_phase],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return proc


def _finish(db_path, stop_phase):
    """Resume a killed worker in a fresh process until done.

    The resumed process records into the same sidecar, so fetch evidence
    spans both processes.  No stop phase matches anymore, which means the
    call only returns normally once the source is drained.
    """
    proc = _run_worker(db_path, "no-such-phase", mode="resume")
    assert proc.returncode == 0, proc.stderr.decode()[:600]


def _golden_final_state():
    db, _ = upgraded_db(3)
    try:
        collector = Collector(db)
        while not collector.poll()["done"]:
            pass
        return snapshot(Store(db)), db.execute(
            "SELECT read_cursor, eof FROM ingest_state").fetchone()
    finally:
        db.close()


def check_crash_recovery():
    golden_state, golden_cursor = _golden_final_state()
    tmpdir = tempfile.mkdtemp(prefix="portal-recovery-")
    phases = [
        "upgrade.before_commit",
        "upgrade.after_commit",
        "page.before_commit",
        "page.after_commit",
        "publish.before_commit",
        "publish.after_commit",
    ]
    for phase in phases:
        db_path = os.path.join(tmpdir, phase.replace(".", "_") + ".db")
        proc = _run_worker(db_path, phase)
        assert proc.returncode and proc.returncode < 0, (
            phase, proc.returncode, proc.stderr.decode()[:400])

        # Reopen: SQLite's own rollback/journal recovery must succeed.
        db = sqlite3.connect(db_path)
        try:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if phase == "upgrade.before_commit":
                assert version == 1
                assert db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                    " AND name='documents'").fetchone() is None
                # Legacy library still fully readable.
                store = Store(db)
                assert store.read("x")["data"] == {"title": "旧公告", "value": 1}
                assert store.read("gone")["deleted"] is True
                assert len(store.receipts()) == 3
                legacy_cursor = db.execute(
                    "SELECT checkpoint FROM legacy_state").fetchone()[0]
                assert legacy_cursor == 3
                calls = open(db_path + ".calls").read().split()
                assert calls == ["resume:3"]
            else:
                assert version == 2
        finally:
            db.close()

        # Resume in a brand-new process and run to the end.
        _finish(db_path, phase)
        db = sqlite3.connect(db_path)
        try:
            assert db.execute("PRAGMA user_version").fetchone()[0] == 2
            assert db.execute(
                "SELECT read_cursor, eof FROM ingest_state").fetchone() == (26, 1)
            assert snapshot(Store(db)) == golden_state
            assert db.execute(
                "SELECT COUNT(*) FROM ingest_pending").fetchone()[0] == 0
            # Audit-only legacy tables were never modified post-upgrade.
            assert db.execute(
                "SELECT checkpoint FROM legacy_state").fetchone()[0] == 3
        finally:
            db.close()

        # Fetch/resume evidence spanning the killed and resumed processes:
        # no accepted page is ever requested twice.
        calls = [line for line in open(db_path + ".calls").read().splitlines()
                 if line]
        fetches = [int(c.split(":")[1]) for c in calls if c.startswith("fetch:")]
        resumes = [int(c.split(":")[1]) for c in calls if c.startswith("resume:")]
        counts = {cursor: fetches.count(cursor)
                  for cursor in set(fetches)}
        if phase == "upgrade.before_commit":
            # Rolled-back upgrade is retried in the resumed process; once it
            # commits, the resumed process drains every page, each once.
            assert resumes == [3, 3]
            assert sorted(fetches) == [9, 12, 15, 18, 21, 24]
            assert len(fetches) == len(set(fetches))
            continue
        assert resumes == [3]
        if phase == "page.before_commit":
            # First page acceptance had not committed: its cursor is the one
            # and only cursor allowed to be refetched after reopen.
            assert counts.pop(9) == 2
        assert all(count == 1 for count in counts.values()), (phase, counts)
        # Every remaining cursor of the six-page recording appears once.
        assert sorted(counts) == [c for c in (9, 12, 15, 18, 21, 24)
                                  if c in counts]

    # Composite boundaries sitting between commit hooks: the page is durable
    # while publication has not (fully) committed.
    extra_cases = [
        # Kill right after the first page commit: cursor 12 is durable with
        # A and B staged, and no publication has committed on that page.
        ("page_accepted_before_publish", 12, [], ["A", "B"]),
        # Kill immediately after B's publish commit; A remains open.
        ("between_publishes", 15, ["b"], ["A"]),
    ]
    for stop_phase, cursor, receipts_tokens, open_tx in extra_cases:
        db_path = os.path.join(tmpdir, stop_phase + ".db")
        proc = _run_worker(db_path, stop_phase)
        assert proc.returncode and proc.returncode < 0, (
            stop_phase, proc.returncode, proc.stderr.decode()[:400])
        db = sqlite3.connect(db_path)
        try:
            assert db.execute("PRAGMA user_version").fetchone()[0] == 2
            assert db.execute(
                "SELECT read_cursor, eof FROM ingest_state").fetchone() == (
                cursor, 0)
            assert [r[0] for r in db.execute(
                "SELECT token FROM receipts ORDER BY seq")] == [
                "h1", "h2", "h3"] + receipts_tokens
            assert [r[0] for r in db.execute(
                "SELECT DISTINCT tx FROM ingest_pending ORDER BY tx")] == open_tx
        finally:
            db.close()
        _finish(db_path, stop_phase)
        db = sqlite3.connect(db_path)
        try:
            assert snapshot(Store(db)) == golden_state
            assert db.execute(
                "SELECT read_cursor, eof FROM ingest_state").fetchone() == (26, 1)
            assert db.execute(
                "SELECT COUNT(*) FROM ingest_pending").fetchone()[0] == 0
        finally:
            db.close()
        calls = [line for line in open(db_path + ".calls").read().splitlines()]
        fetches = [int(c.split(":")[1]) for c in calls if c.startswith("fetch:")]
        counts = {c: fetches.count(c) for c in set(fetches)}
        # Both kills land after an accepted page commit: no cursor repeats.
        assert all(v == 1 for v in counts.values()), (stop_phase, counts)
    print("OK: SIGKILL at all six commit boundaries, reopen, resume, no refetch")


def check_no_sdk1_on_upgraded_path():
    # A clean interpreter importing collector then only touching an upgraded
    # database must never load vendor.sdk1.
    tmpdir = tempfile.mkdtemp(prefix="portal-isolation-")
    db_path = os.path.join(tmpdir, "isolated.db")
    db = sqlite3.connect(db_path)
    try:
        seed(db, 3)
        upgrade(db)
    finally:
        db.close()
    workdir = os.path.dirname(os.path.abspath(__file__))
    code = (
        "import sys; sys.path.insert(0, %r);"
        "import sqlite3;"
        "import collector, store, migration;"
        "db=sqlite3.connect(%r);"
        "c=collector.Collector(db);"
        "r=c.poll();"
        "assert r['cursor']==12;"
        "assert store.Store(db).read('gone')['deleted'] is True;"
        "db.close();"
        "assert 'vendor.sdk1' not in sys.modules, sorted(sys.modules);"
        "assert 'vendor.sdk2' in sys.modules"
    ) % (workdir, db_path)
    proc = subprocess.run([sys.executable, "-I", "-c", code],
                          cwd=workdir,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert proc.returncode == 0, proc.stderr.decode()
    print("OK: upgraded path imports SDK2 only; legacy tables are audit-only")


def main():
    baseline.run(demo=False)
    check_mapping_all_checkpoints()
    check_idempotent_and_rollback()
    check_page_by_page()
    check_custom_semantics()
    check_transport_error()
    check_crash_recovery()
    check_no_sdk1_on_upgraded_path()
    print("ALL_VERIFY_OK")


if __name__ == "__main__":
    main()
