"""Queue implementation using collections.deque."""

from collections import deque
from typing import Any


class Queue:
    """
    A queue implementation using collections.deque.

    Attributes
    ----------
    _items : deque
        Internal storage for queue items using deque for efficient operations.
    """

    def __init__(self) -> None:
        """Initialize an empty queue."""
        self._items = deque()

    def is_empty(self) -> bool:
        """
        Check if the queue is empty.

        Returns
        -------
        bool
            True if queue is empty, False otherwise.
        """
        return not self._items

    def enqueue(self, item: type[Any]) -> None:
        """
        Add an item to the end of the queue.

        Parameters
        ----------
        item : type[Any]
            The item to be added to the queue.
        """
        self._items.appendleft(item)

    def dequeue(self) -> type[Any]:
        """
        Remove and return the first item from the queue.

        Returns
        -------
        type[Any]
            The first item in the queue.

        Raises
        ------
        IndexError
            If attempting to dequeue from an empty queue.
        """
        if self.is_empty():
            raise IndexError("Dequeue from an empty queue")
        return self._items.pop()

    def size(self) -> int:
        """
        Get the current number of items in the queue.

        Returns
        -------
        int
            The number of items in the queue.
        """
        return len(self._items)