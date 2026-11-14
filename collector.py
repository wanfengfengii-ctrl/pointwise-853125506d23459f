"""SDK1 collector: callback, business write and checkpoint share a commit."""
from vendor.sdk1 import Client

class Collector:
    def __init__(self, db, client=None, hook=None):
        self.db = db
        self.client = Client() if client is None else client
        self.hook = hook or (lambda phase: None)

    def poll(self):
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
            next_cursor = self.client.fetch(cursor, accept)
            if next_cursor is not None:
                self.db.execute("UPDATE legacy_state SET checkpoint=?", (next_cursor,))
            self.hook("row.before_commit")
        self.hook("row.after_commit")
        return {"cursor":cursor if next_cursor is None else next_cursor,
                "published":published,"done":next_cursor is None}
