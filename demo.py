"""Delivery demo: frozen legacy store upgraded, then drained page by page.

Prints the migrated bytes/tombstones, every SDK2 page acceptance with the
visible business state after each poll, the receipt log and the final
staging/state rows -- real upgraded data, not a success flag.
"""
import json
import sqlite3

from collector import Collector
from migration import upgrade
from samples import seed
from store import Store
from vendor.sdk2 import Client


def main():
    db = sqlite3.connect(":memory:")
    seed(db, 3)
    store = Store(db)
    client = Client()

    frozen = {
        "version_before": db.execute("PRAGMA user_version").fetchone()[0],
        "x": store.read("x"),
        "gone": store.read("gone"),
        "receipts": store.receipts(),
    }
    phases = []
    upgrade(db, client, phases.append)
    migrated = {
        "version_after": db.execute("PRAGMA user_version").fetchone()[0],
        "resume_calls": list(client.resume_calls),
        "read_cursor": db.execute(
            "SELECT read_cursor FROM ingest_state").fetchone()[0],
        "document_bodies_utf8": {
            row[0]: (None if row[1] is None else row[1].decode("utf-8"))
            for row in db.execute(
                "SELECT id, body FROM documents ORDER BY id")},
        "tombstones": [
            row[0] for row in db.execute(
                "SELECT id FROM documents WHERE deleted=1 ORDER BY id")],
        "audit_legacy_tables": [
            row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
                " AND name LIKE 'legacy_%' ORDER BY name")],
        "upgrade_hooks": phases,
        "old_queries_unchanged": {
            "x": store.read("x"),
            "gone": store.read("gone"),
            "receipts": store.receipts(),
        },
    }

    collector = Collector(db, client)
    pages = []
    while True:
        result = collector.poll()
        pages.append({
            "poll_result": result,
            "fetch_cursor_next": client.calls[len(pages)]
            if len(pages) < len(client.calls) else None,
            "x": store.read("x"),
            "y": store.read("y"),
            "z": store.read("z"),
            "staged_transactions": [
                row[0] for row in db.execute(
                    "SELECT DISTINCT tx FROM ingest_pending ORDER BY tx")],
        })
        if result["done"]:
            break

    summary = {
        "final_cursor_eof": db.execute(
            "SELECT read_cursor, eof FROM ingest_state").fetchone(),
        "receipts": store.receipts(),
        "pending_rows_left": db.execute(
            "SELECT COUNT(*) FROM ingest_pending").fetchone()[0],
        "all_fetch_cursors": list(client.calls),
        "further_poll": collector.poll(),
    }
    print(json.dumps(
        {"frozen_legacy_store": frozen,
         "after_upgrade": migrated,
         "page_by_page": pages,
         "summary": summary},
        ensure_ascii=False, indent=2))
    db.close()


if __name__ == "__main__":
    main()
