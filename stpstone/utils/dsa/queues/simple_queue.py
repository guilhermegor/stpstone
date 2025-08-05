"""Queue implementation using collections.deque."""

from collections import deque
from typing import Any

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class Queue(metaclass=TypeChecker):
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

    @property
    def is_empty(self) -> bool:
        """
        Check if the queue is empty.

        Returns
        -------
        bool
            True if queue is empty, False otherwise.
        """
        return len(self._items) == 0

    def enqueue(self, item: Any) -> None: # noqa: ANN401 - typing.Any disallowed
        """
        Add an item to the end of the queue.

        Parameters
        ----------
        item : Any
            The item to be added to the queue.
        """
        self._items.append(item)

    def dequeue(self) -> Any: # noqa: ANN401 - typing.Any disallowed
        """
        Remove and return the first item from the queue.

        Returns
        -------
        Any
            The first item in the queue.

        Raises
        ------
        IndexError
            If attempting to dequeue from an empty queue.
        """
        if self.is_empty:
            raise IndexError("Dequeue from an empty queue")
        return self._items.popleft()

    @property
    def size(self) -> int:
        """
        Get the current number of items in the queue.

        Returns
        -------
        int
            The number of items in the queue.
        """
        return len(self._items)