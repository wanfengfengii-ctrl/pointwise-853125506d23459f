"""Portal ingestion collector.

Two engines share one public signature:

* legacy engine (PRAGMA user_version < 2): the frozen SDK1 row callback flow;
* event engine (user_version == 2): SDK2 transaction events with persisted
  page acceptance and transactional publication.

The engine is chosen per ``poll`` from the live database version, so the same
Collector object immediately follows an in-process upgrade.  After an upgrade
the SDK1 client is never imported or instantiated on the new path.
"""
import json


class Collector:
    def __init__(self, db, client=None, hook=None):
        self.db = db
        # client=None resolves to the default client of the active engine.
        self._client = client
        self.hook = hook or (lambda phase: None)

    def _schema_version(self):
        return self.db.execute("PRAGMA user_version").fetchone()[0]

    def poll(self):
        if self._schema_version() >= 2:
            return self._poll_events()
        return self._poll_legacy()

    # ------------------------------------------------------------------ SDK1
    def _poll_legacy(self):
        from vendor.sdk1 import Client
        client = self._client if self._client is not None else Client()
        cursor = self.db.execute(
            "SELECT checkpoint FROM legacy_state WHERE singleton=1").fetchone()[0]
        published = []
        with self.db:
            def accept(row):
                self.db.execute("INSERT OR REPLACE INTO legacy_items VALUES(?,?,?)",
                    (row["id"], row["payload"], int(row["deleted"])))
                seq = self.db.execute("SELECT COALESCE(MAX(seq),0)+1 FROM receipts").fetchone()[0]
                self.db.execute("INSERT INTO receipts VALUES(?,?,1)", (seq,row["receipt"]))
                published.append(row["receipt"])
            next_cursor = client.fetch(cursor, accept)
            if next_cursor is not None:
                self.db.execute("UPDATE legacy_state SET checkpoint=?", (next_cursor,))
            self.hook("row.before_commit")
        self.hook("row.after_commit")
        return {"cursor":cursor if next_cursor is None else next_cursor,
                "published":published,"done":next_cursor is None}

    # ------------------------------------------------------------------ SDK2
    def _read_state(self):
        return self.db.execute(
            "SELECT read_cursor, eof FROM ingest_state WHERE singleton=1"
        ).fetchone()

    def _accept_page(self, page):
        """Persist one whole fetched page and advance the cursor atomically."""
        next_cursor, eof = page.next_cursor, 1 if page.eof else 0
        self.db.execute("BEGIN IMMEDIATE")
        try:
            for event in page.events:
                kind = event["kind"]
                ref_id = event.get("id")
                body = None
                if kind == "upsert":
                    body = json.dumps(event["body"], ensure_ascii=False).encode("utf-8")
                token = event.get("token")
                self.db.execute(
                    "INSERT INTO ingest_pending"
                    "(position, tx, kind, ref_id, body, token) VALUES(?,?,?,?,?,?)",
                    (event["position"], event["tx"], kind, ref_id, body, token))
            self.db.execute(
                "UPDATE ingest_state SET read_cursor=?, eof=? WHERE singleton=1",
                (next_cursor, eof))
            self.hook("page.before_commit")
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        self.hook("page.after_commit")

    def _sweep(self):
        """Close every transaction whose terminal event has been accepted.

        Terminals are handled in commit/abort position order, each in its own
        SQLite commit, so an open transaction never blocks other publications.
        Returns the receipt tokens newly published during this call.
        """
        published = []
        terminals = self.db.execute(
            "SELECT position, tx, kind, token FROM ingest_pending"
            " WHERE kind IN ('commit','abort') ORDER BY position").fetchall()
        for _position, tx, kind, token in terminals:
            if kind == "commit":
                published.append(self._publish(tx, token))
            else:
                self._abort(tx)
        return published

    def _publish(self, tx, token):
        """Apply one whole transaction and clear its staging in one commit."""
        self.db.execute("BEGIN IMMEDIATE")
        try:
            changes = 0
            for kind, ref_id, body in self.db.execute(
                    "SELECT kind, ref_id, body FROM ingest_pending"
                    " WHERE tx=? AND kind IN ('upsert','delete') ORDER BY position",
                    (tx,)):
                changes += 1
                if kind == "delete":
                    self.db.execute(
                        "INSERT INTO documents(id, body, deleted) VALUES(?,NULL,1)"
                        " ON CONFLICT(id) DO UPDATE SET body=excluded.body,"
                        " deleted=excluded.deleted", (ref_id,))
                else:
                    # Last write to an id within this transaction wins, because
                    # rows are applied in event order.
                    self.db.execute(
                        "INSERT INTO documents(id, body, deleted) VALUES(?,?,0)"
                        " ON CONFLICT(id) DO UPDATE SET body=excluded.body,"
                        " deleted=excluded.deleted", (ref_id, body))
            seq = self.db.execute(
                "SELECT COALESCE(MAX(seq),0)+1 FROM receipts").fetchone()[0]
            self.db.execute(
                "INSERT INTO receipts(seq, token, changes) VALUES(?,?,?)",
                (seq, token, changes))
            self.db.execute("DELETE FROM ingest_pending WHERE tx=?", (tx,))
            self.hook("publish.before_commit")
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        self.hook("publish.after_commit")
        return token

    def _abort(self, tx):
        """Discard an aborted transaction's staging without touching docs."""
        with self.db:
            self.db.execute("DELETE FROM ingest_pending WHERE tx=?", (tx,))

    def _poll_events(self):
        from vendor.sdk2 import Client
        client = self._client if self._client is not None else Client()

        # 1. Anything left over from an earlier interrupted poll becomes
        #    visible first, before any new page is fetched.
        published = self._sweep()

        read_cursor, eof = self._read_state()
        if not eof:
            # 2. At most one new page per poll.  A transport error propagates
            #    untouched: nothing local is written, the same cursor refetches
            #    the same page because fetch does not consume remote data.
            page = client.fetch(read_cursor)
            self._accept_page(page)
            # 3. Publish whatever the accepted page made committable.
            published.extend(self._sweep())
            read_cursor, eof = self._read_state()

        # done requires the accepted end marker and no unfinished material.
        pending_open = self.db.execute(
            "SELECT 1 FROM ingest_pending LIMIT 1").fetchone()
        if eof and pending_open is not None:
            # Accepted progress and already published transactions stay put.
            raise ValueError(
                "unfinished transactions remain when source ended")
        return {"cursor": read_cursor, "published": published,
                "done": bool(eof) and pending_open is None}
