"""Public query API; SQLite connections belong to callers.

The same read/receipts calls work on both the frozen SDK1 store and the
upgraded SDK2 store.  The engine is selected per call from PRAGMA
user_version, so an in-process upgrade is visible immediately.  After the
upgrade the legacy tables are audit-only and never queried here.
"""
import json


class Store:
    def __init__(self, db):
        self.db = db

    def _upgraded(self):
        return self.db.execute("PRAGMA user_version").fetchone()[0] >= 2

    def read(self, id):
        if self._upgraded():
            row = self.db.execute(
                "SELECT body, deleted FROM documents WHERE id=?", (id,)
            ).fetchone()
            if row is None:
                return None
            body, deleted = row
            # Tombstone (deleted=1, NULL body) differs from a real record whose
            # own JSON content carries a null somewhere inside it.
            return {"id": id, "deleted": bool(deleted),
                    "data": None if deleted else json.loads(body.decode("utf-8"))}
        row = self.db.execute(
            "SELECT payload,deleted FROM legacy_items WHERE id=?", (id,)).fetchone()
        if row is None:
            return None
        payload, deleted = row
        return {"id":id, "deleted":bool(deleted),
                "data":None if deleted else json.loads(payload)}

    def receipts(self):
        return [{"seq":seq,"token":token,"changes":changes}
                for seq,token,changes in self.db.execute(
                    "SELECT seq,token,changes FROM receipts ORDER BY seq")]
