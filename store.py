"""Existing public query API; SQLite connections belong to callers."""
import json

class Store:
    def __init__(self, db):
        self.db = db

    def read(self, id):
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
