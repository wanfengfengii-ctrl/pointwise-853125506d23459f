"""SDK1: one already committed mutation per callback."""
from copy import deepcopy
from samples import ROWS

class Client:
    def __init__(self):
        self.calls = []

    def fetch(self, checkpoint, callback):
        if type(checkpoint) is not int or not 0 <= checkpoint <= len(ROWS):
            raise ValueError("checkpoint")
        self.calls.append(checkpoint)
        if checkpoint == len(ROWS):
            return None
        callback(deepcopy(ROWS[checkpoint]))
        return checkpoint + 1
