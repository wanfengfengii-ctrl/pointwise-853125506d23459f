"""SDK2 only returns events: no database or transaction assembly."""
from copy import deepcopy
from dataclasses import dataclass
from samples import EVENTS

@dataclass(frozen=True)
class Page:
    events: list
    next_cursor: int
    eof: bool

class Client:
    def __init__(self, events=None, page_size=3):
        if type(page_size) is not int or page_size < 1:
            raise ValueError("page_size")
        self.events = deepcopy(EVENTS if events is None else events)
        self.page_size = page_size
        self.calls = []
        self.resume_calls = []

    def resume(self, legacy_checkpoint):
        mapping = {0:0, 1:3, 2:6, 3:9}
        if type(legacy_checkpoint) is not int or legacy_checkpoint not in mapping:
            raise ValueError("unmapped checkpoint")
        self.resume_calls.append(legacy_checkpoint)
        return mapping[legacy_checkpoint]

    def fetch(self, cursor):
        if type(cursor) is not int or not 0 <= cursor <= len(self.events):
            raise ValueError("cursor")
        self.calls.append(cursor)
        end = min(len(self.events), cursor + self.page_size)
        rows = []
        for position in range(cursor, end):
            event = deepcopy(self.events[position])
            event["position"] = position
            rows.append(event)
        return Page(rows, end, end == len(self.events))
