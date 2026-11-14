"""Maintenance-window upgrade from the SDK1 row callback store to SDK2 events.

The whole upgrade happens inside one SQLite commit.  If anything fails before
that commit (including an upgrade.before_commit hook raising or the process
being killed), reopening the file yields the untouched, fully readable legacy
database.  A second successful call on an already upgraded database is a
no-op: no resume call, no business-data change and no upgrade commit hooks.
"""
from vendor.sdk2 import Client

# Persistent staging for events whose transaction is not closed yet, plus
# events of freshly accepted pages whose terminal has not been swept.  Rows
# are deleted in the same commit that publishes or aborts the transaction.
PENDING_SCHEMA = """
CREATE TABLE ingest_pending(
 position INTEGER PRIMARY KEY,
 tx TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('begin','upsert','delete','commit','abort')),
 ref_id TEXT,
 body BLOB,
 token TEXT)
"""


def _schema_version(db):
    return db.execute("PRAGMA user_version").fetchone()[0]


def upgrade(db, client=None, hook=None):
    """Migrate one legacy database to the SDK2 schema inside a single commit."""
    hook = hook or (lambda phase: None)
    if _schema_version(db) >= 2:
        # Idempotent re-entry: already upgraded, nothing to do, no hooks.
        return None
    client = Client() if client is None else client

    checkpoint = db.execute(
        "SELECT checkpoint FROM legacy_state WHERE singleton=1").fetchone()[0]
    # Real SDK2 resume call: the mapped cursor is the next unread event.
    read_cursor = client.resume(checkpoint)

    db.execute("BEGIN IMMEDIATE")
    try:
        db.execute(
            "CREATE TABLE documents("
            " id TEXT PRIMARY KEY, body BLOB,"
            " deleted INTEGER NOT NULL CHECK(deleted IN(0,1)),"
            " CHECK((deleted=1 AND body IS NULL)"
            " OR (deleted=0 AND body IS NOT NULL)))")
        db.execute(
            "CREATE TABLE ingest_state("
            " singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
            " read_cursor INTEGER NOT NULL,"
            " eof INTEGER NOT NULL CHECK(eof IN(0,1)))")
        db.execute(PENDING_SCHEMA)

        # True copy of every live record and tombstone.  Bodies are the exact
        # UTF-8 bytes of the original payload text; tombstones keep NULL.
        for item_id, payload, deleted in db.execute(
                "SELECT id, payload, deleted FROM legacy_items"):
            body = None if deleted else payload.encode("utf-8")
            db.execute(
                "INSERT INTO documents(id, body, deleted) VALUES(?,?,?)",
                (item_id, body, int(deleted)))

        # receipts is the shared, append-only publication log and is copied
        # implicitly by being left in place; its seq order keeps running.
        db.execute(
            "INSERT INTO ingest_state(singleton, read_cursor, eof) VALUES(1,?,0)",
            (read_cursor,))

        # PRAGMA user_version is transactional: rollback restores version 1.
        db.execute("PRAGMA user_version=2")
        hook("upgrade.before_commit")
        db.commit()
    except BaseException:
        db.rollback()
        raise
    hook("upgrade.after_commit")
    return None
