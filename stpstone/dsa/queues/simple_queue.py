from collections import deque
from typing import Any


class Queue:

    def __init__(self):
        self._items = deque()

    @property
    def is_empty(self) -> bool:
        return not self._items

    def enqueue(self, item: Any) -> None:
        self._items.appendleft(item)

    @property
    def dequeue(self):
        if self.is_empty:
            raise IndexError("Dequeue from an empty queue")
        return self._items.pop()

    @property
    def size(self) -> int:
        return len(self._items)
