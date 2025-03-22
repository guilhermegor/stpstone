from collections import deque
from typing import Any


class Deque:

    def __init__(self) -> None:
        self._items: deque[Any] = deque()

    @property
    def is_empty(self) -> bool:
        return not self._items

    def add_front(self, item: Any) -> None:
        self._items.append(item)

    def add_rear(self, item: Any) -> None:
        self._items.appendleft(item)

    @property
    def remove_front(self) -> Any:
        return self._items.pop()

    @property
    def remove_rear(self) -> Any:
        return self._items.popleft()

    @property
    def size(self) -> int:
        return len(self._items)
