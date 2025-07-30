"""Double-ended queue (deque) implementation.

This module provides a Deque class that supports adding and removing items from both ends
with O(1) time complexity for all operations. The implementation uses collections.deque
as the underlying data structure.
"""

from collections import deque
from typing import Any

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class Deque(metaclass=TypeChecker):
    """Double-ended queue implementation.

    Attributes
    ----------
    _items : deque[Any]
        Internal storage for deque items using collections.deque
    """

    def __init__(self) -> None:
        """Initialize an empty deque."""
        self._items: deque[Any] = deque()

    @property
    def is_empty(self) -> bool:
        """Check if the deque is empty.

        Returns
        -------
        bool
            True if deque is empty, False otherwise
        """
        return not self._items

    def add_front(self, item: Any) -> None: # noqa: ANN401 - typing.Any disallowed
        """Add an item to the front of the deque.

        Parameters
        ----------
        item : Any
            Item to add to the front of the deque
        """
        self._items.append(item)

    def add_rear(self, item: Any) -> None: # noqa: ANN401 - typing.Any disallowed
        """Add an item to the rear of the deque.

        Parameters
        ----------
        item : Any
            Item to add to the rear of the deque
        """
        self._items.appendleft(item)

    def remove_front(self) -> Any: # noqa: ANN401 - typing.Any disallowed
        """Remove and return the front item from the deque.

        Returns
        -------
        Any
            Item removed from the front of the deque
        """
        return self._items.pop()

    def remove_rear(self) -> Any: # noqa: ANN401 - typing.Any disallowed
        """Remove and return the rear item from the deque.

        Returns
        -------
        Any
            Item removed from the rear of the deque
        """
        return self._items.popleft()

    @property
    def size(self) -> int:
        """Get the current number of items in the deque.

        Returns
        -------
        int
            Number of items in the deque
        """
        return len(self._items)