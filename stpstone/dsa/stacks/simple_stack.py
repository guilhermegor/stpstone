"""Stack implementation using NumPy arrays for efficient memory management.

This module provides a Stack class with standard push/pop operations implemented
using NumPy arrays for underlying storage with automatic resizing capabilities.
"""

from typing import Any

import numpy as np

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class Stack(metaclass=TypeChecker):
    """A stack data structure implementation using NumPy arrays.

    Parameters
    ----------
    initial_capacity : int, optional
        Initial capacity of the stack (default: 10)

    Attributes
    ----------
    is_empty : bool
        True if stack contains no elements
    size : int
        Number of elements in the stack
    peek : Any
        Top element of the stack without removing it

    Methods
    -------
    push(item: Any) -> None
        Add an item to the top of the stack
    pop() -> Any
        Remove and return the top item from the stack
    """

    def __init__(self, initial_capacity: int = 10) -> None:
        """Initialize stack with specified initial capacity.

        Parameters
        ----------
        initial_capacity : int, optional
            Initial capacity of the stack (default: 10)
        """
        self._validate_capacity(initial_capacity)
        self._items = np.empty(initial_capacity, dtype=object)
        self._size = 0

    def _validate_capacity(self, capacity: int) -> None:
        """Validate that capacity is a positive integer.

        Parameters
        ----------
        capacity : int
            Capacity value to validate

        Raises
        ------
        ValueError
            If capacity is not positive
        """
        if not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("Capacity must be a positive integer")

    @property
    def is_empty(self) -> bool:
        """Check if stack contains no elements.

        Returns
        -------
        bool
            True if stack is empty, False otherwise
        """
        return self._size == 0

    def push(self, item: Any) -> None: # noqa: ANN401 - typing.Any disallowed
        """Add an item to the top of the stack.

        Parameters
        ----------
        item : Any
            Item to be added to the stack

        Notes
        -----
        Automatically resizes underlying array if capacity is reached
        """
        if self._size == len(self._items):
            self._resize(2 * len(self._items))
        self._items[self._size] = item
        self._size += 1

    def pop(self) -> Any: # noqa: ANN401 - typing.Any disallowed
        """Remove and return the top item from the stack.

        Returns
        -------
        Any
            The top item from the stack
        """
        self._validate_not_empty()
        self._size -= 1
        item = self._items[self._size]
        self._items[self._size] = None
        return item

    def _validate_not_empty(self) -> None:
        """Validate that stack is not empty.

        Raises
        ------
        IndexError
            If stack is empty
        """
        if self.is_empty:
            raise IndexError("Operation on an empty stack")

    @property
    def peek(self) -> Any: # noqa: ANN401 - typing.Any disallowed
        """Return the top item without removing it.

        Returns
        -------
        Any
            The top item from the stack
        """
        self._validate_not_empty()
        return self._items[self._size - 1]

    @property
    def size(self) -> int:
        """Return the number of items in the stack.

        Returns
        -------
        int
            Current size of the stack
        """
        return self._size

    def _resize(self, new_capacity: int) -> None:
        """Resize the underlying array to specified capacity.

        Parameters
        ----------
        new_capacity : int
            New capacity for the underlying array

        Notes
        -----
        Private method - should not be called directly
        """
        self._validate_capacity(new_capacity)
        new_items = np.empty(new_capacity, dtype=object)
        new_items[:self._size] = self._items[:self._size]
        self._items = new_items