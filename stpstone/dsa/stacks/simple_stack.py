import numpy as np
from typing import Any


class Stack:

    def __init__(self, initial_capacity: int = 10) -> None:
        self._items = np.empty(initial_capacity, dtype=object)
        self._size = 0

    @property
    def is_empty(self) -> bool:
        return self._size == 0

    def push(self, item: Any) -> None:
        if self._size == len(self._items):
            self._resize(2 * len(self._items))
        self._items[self._size] = item
        self._size += 1

    def pop(self) -> Any:
        if self.is_empty:
            raise IndexError("Pop from an empty stack")
        self._size -= 1
        item = self._items[self._size]
        self._items[self._size] = None
        return item

    @property
    def peek(self) -> Any:
        if self.is_empty:
            raise IndexError("Peek from an empty stack")
        return self._items[self._size - 1]

    @property
    def size(self) -> int:
        return self._size

    def _resize(self, new_capacity: int) -> None:
        """
        Private method to resize the underlying array of the stack to the new capacity.

        Args:
            new_capacity (int): New capacity of the stack

        Returns:
            None
        """
        new_items = np.empty(new_capacity, dtype=object)
        new_items[:self._size] = self._items[:self._size]
        self._items = new_items
