"""Frozen normal regressions; extend verification through another entry if needed."""
import json
import sqlite3
import sys
from collector import Collector
from store import Store
from samples import seed
from vendor.sdk2 import Client as EventClient

def run(demo=False):
    db = sqlite3.connect(":memory:")
    try:
        seed(db, 0)
        collector = Collector(db)
        store = Store(db)
        assert store.read("x") is None
        first = collector.poll()
        assert first == {"cursor":1,"published":["h1"],"done":False}
        assert store.read("x") == {"id":"x","deleted":False,
                                   "data":{"title":"旧公告","value":1}}
        collector = Collector(db)
        assert collector.poll()["published"] == ["h2"]
        assert store.read("gone")["deleted"] is False
        assert collector.poll()["published"] == ["h3"]
        assert store.read("gone") == {"id":"gone","deleted":True,"data":None}
        assert collector.poll() == {"cursor":3,"published":[],"done":True}
        assert store.receipts() == [
            {"seq":1,"token":"h1","changes":1},
            {"seq":2,"token":"h2","changes":1},
            {"seq":3,"token":"h3","changes":1}]
        frozen = sqlite3.connect(":memory:")
        try:
            seed(frozen)
            assert Store(frozen).read("x") == store.read("x")
            assert Store(frozen).read("gone") == store.read("gone")
            assert Store(frozen).receipts() == store.receipts()
        finally:
            frozen.close()
        client = EventClient()
        cursor = client.resume(3)
        pages = []
        while True:
            page = client.fetch(cursor)
            pages.append(page)
            cursor = page.next_cursor
            if page.eof:
                break
        assert [p.next_cursor for p in pages] == [12,15,18,21,24,26]
        assert [e["token"] for p in pages for e in p.events if e["kind"] == "commit"] == ["b","a","d"]
        if demo:
            print(json.dumps({"old_cursor":3,"x":store.read("x"),
                "gone":store.read("gone"),"receipts":store.receipts(),
                "sdk2_pages":[{"next_cursor":p.next_cursor,"eof":p.eof,"events":p.events}
                              for p in pages]}, ensure_ascii=False, indent=2))
        else:
            print("BASELINE_OK: old callback ingestion, seed equivalence, SDK2 recording")
    finally:
        db.close()

if __name__ == "__main__":
    run("demo" in sys.argv[1:])
