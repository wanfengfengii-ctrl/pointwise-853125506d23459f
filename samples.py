"""Frozen old database and immutable portal recordings."""
import json

OLD_SCHEMA = """
PRAGMA user_version=1;
CREATE TABLE legacy_state(
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 checkpoint INTEGER NOT NULL);
CREATE TABLE legacy_items(
 id TEXT PRIMARY KEY, payload TEXT, deleted INTEGER NOT NULL CHECK(deleted IN(0,1)),
 CHECK((deleted=1 AND payload IS NULL) OR (deleted=0 AND payload IS NOT NULL)));
CREATE TABLE receipts(
 seq INTEGER PRIMARY KEY, token TEXT NOT NULL UNIQUE, changes INTEGER NOT NULL);
INSERT INTO legacy_state VALUES(1,0);
"""
ROWS = [
 {"id":"x","payload":'{"title":"旧公告","value":1}',"deleted":False,"receipt":"h1"},
 {"id":"gone","payload":'{"title":"撤回件"}',"deleted":False,"receipt":"h2"},
 {"id":"gone","payload":None,"deleted":True,"receipt":"h3"},
]

def seed(db, checkpoint=3):
    """Complete old DB at any of the four committed boundaries."""
    if checkpoint not in range(4):
        raise ValueError("checkpoint")
    db.executescript(OLD_SCHEMA)
    with db:
        for seq, row in enumerate(ROWS[:checkpoint], 1):
            db.execute("INSERT OR REPLACE INTO legacy_items VALUES(?,?,?)",
                       (row["id"], row["payload"], int(row["deleted"])))
            db.execute("INSERT INTO receipts VALUES(?,?,1)", (seq, row["receipt"]))
        db.execute("UPDATE legacy_state SET checkpoint=?", (checkpoint,))

def _history():
    events = []
    for i, row in enumerate(ROWS, 1):
        tx = "history-" + str(i)
        events.append({"kind":"begin","tx":tx})
        if row["deleted"]:
            events.append({"kind":"delete","tx":tx,"id":row["id"]})
        else:
            events.append({"kind":"upsert","tx":tx,"id":row["id"],
                           "body":json.loads(row["payload"])})
        events.append({"kind":"commit","tx":tx,"token":row["receipt"]})
    return events

EVENTS = _history() + [
 {"kind":"begin","tx":"A"},
 {"kind":"upsert","tx":"A","id":"x","body":{"title":"A初稿","value":2}},
 {"kind":"begin","tx":"B"},
 {"kind":"upsert","tx":"B","id":"x","body":{"title":"B先提交","value":3}},
 {"kind":"commit","tx":"B","token":"b"},
 {"kind":"upsert","tx":"A","id":"x","body":{"title":"A定稿","value":4}},
 {"kind":"begin","tx":"C"},
 {"kind":"delete","tx":"C","id":"x"},
 {"kind":"commit","tx":"A","token":"a"},
 {"kind":"abort","tx":"C"},
 {"kind":"begin","tx":"D"},
 {"kind":"delete","tx":"D","id":"x"},
 {"kind":"upsert","tx":"D","id":"y","body":{"title":"新资料","value":None}},
 {"kind":"commit","tx":"D","token":"d"},
 {"kind":"begin","tx":"E"},
 {"kind":"upsert","tx":"E","id":"z","body":{"title":"将撤销"}},
 {"kind":"abort","tx":"E"},
]
