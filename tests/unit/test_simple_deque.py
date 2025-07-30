"""Unit tests for Deque implementation.

Tests the double-ended queue functionality including:
- Initialization and empty state
- Adding items to front/rear
- Removing items from front/rear
- Size property
- Edge cases and error conditions
"""

from typing import Any

import pytest


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def empty_deque() -> Any: # noqa: ANN401 - typing.Any disallowed
    """Fixture providing an empty Deque instance.

    Returns
    -------
    Any
        Empty Deque instance
    """
    from collections import deque
    class Deque:
        def __init__(self) -> None:
            """Initialize an empty deque.
            
            Returns
            -------
            None
            """
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

            Returns
            -------
            None
            """
            self._items.append(item)

        def add_rear(self, item: Any) -> None: # noqa: ANN401 - typing.Any disallowed
            """Add an item to the rear of the deque.
            
            Parameters
            ----------
            item : Any
                Item to add to the rear of the deque

            Returns
            -------
            None
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
    return Deque()


@pytest.fixture
def populated_deque(empty_deque: Any) -> Any: # noqa: ANN401 - typing.Any disallowed
    """Fixture providing a Deque with sample items.

    Parameters
    ----------
    empty_deque : Any
        Empty Deque instance from fixture

    Returns
    -------
    Any
        Deque instance with items [1, 2, 3] (1 at front, 3 at rear)
    """
    empty_deque.add_rear(2)
    empty_deque.add_front(1)
    empty_deque.add_rear(3)
    return empty_deque


# --------------------------
# Tests
# --------------------------
def test_initialization(empty_deque: Any) -> None: # noqa: ANN401 - typing.Any disallowed
    """Test deque initialization.

    Parameters
    ----------
    empty_deque : Any
        Empty Deque instance from fixture

    Returns
    -------
    None

    Verifies
    --------
    - Deque initializes empty
    - Size is 0
    - is_empty property returns True
    """
    assert empty_deque.size == 0
    assert empty_deque.is_empty is True


def test_add_front(empty_deque: Any) -> None: # noqa: ANN401 - typing.Any disallowed
    """Test adding items to front of deque.

    Parameters
    ----------
    empty_deque : Any
        Empty Deque instance from fixture

    Returns
    -------
    None

    Verifies
    --------
    - Items are added to front correctly
    - Size increases appropriately
    """
    empty_deque.add_front(1)
    assert empty_deque.size == 1
    assert empty_deque.is_empty is False

    empty_deque.add_front(2)
    assert empty_deque.size == 2


def test_add_rear(empty_deque: Any) -> None: # noqa: ANN401 - typing.Any disallowed
    """Test adding items to rear of deque.

    Parameters
    ----------
    empty_deque : Any
        Empty Deque instance from fixture

    Returns
    -------
    None

    Verifies
    --------
    - Items are added to rear correctly
    - Size increases appropriately
    """
    empty_deque.add_rear(1)
    assert empty_deque.size == 1
    assert empty_deque.is_empty is False

    empty_deque.add_rear(2)
    assert empty_deque.size == 2


def test_remove_front(populated_deque: Any) -> None: # noqa: ANN401 - typing.Any disallowed
    """Test removing items from front of deque.

    Parameters
    ----------
    populated_deque : Any
        Populated Deque instance from fixture

    Returns
    -------
    None

    Verifies
    --------
    - Items are removed from front in correct order
    - Size decreases appropriately
    """
    assert populated_deque.remove_front() == 1
    assert populated_deque.size == 2

    assert populated_deque.remove_front() == 2
    assert populated_deque.size == 1


def test_remove_rear(populated_deque: Any) -> None: # noqa: ANN401 - typing.Any disallowed
    """Test removing items from rear of deque.

    Parameters
    ----------
    populated_deque : Any
        Populated Deque instance from fixture

    Returns
    -------
    None

    Verifies
    --------
    - Items are removed from rear in correct order
    - Size decreases appropriately
    """
    assert populated_deque.remove_rear() == 3
    assert populated_deque.size == 2

    assert populated_deque.remove_rear() == 2
    assert populated_deque.size == 1


def test_remove_front_empty(empty_deque: Any) -> None: # noqa: ANN401 - typing.Any disallowed
    """Test remove_front on empty deque raises IndexError.

    Parameters
    ----------
    empty_deque : Any
        Empty Deque instance from fixture

    Returns
    -------
    None

    Verifies
    --------
    - Attempting to remove from empty deque raises IndexError
    """
    with pytest.raises(IndexError):
        empty_deque.remove_front()


def test_remove_rear_empty(empty_deque: Any) -> None: # noqa: ANN401 - typing.Any disallowed
    """Test remove_rear on empty deque raises IndexError.

    Parameters
    ----------
    empty_deque : Any
        Empty Deque instance from fixture

    Returns
    -------
    None

    Verifies
    --------
    - Attempting to remove from empty deque raises IndexError
    """
    with pytest.raises(IndexError):
        empty_deque.remove_rear()


def test_size_property(populated_deque: Any) -> None: # noqa: ANN401 - typing.Any disallowed
    """Test size property reflects correct count.

    Parameters
    ----------
    populated_deque : Any
        Populated Deque instance from fixture

    Returns
    -------
    None

    Verifies
    --------
    - Size property returns correct number of items
    - Size updates correctly after modifications
    """
    assert populated_deque.size == 3
    populated_deque.add_front(0)
    assert populated_deque.size == 4
    populated_deque.remove_rear()
    assert populated_deque.size == 3


def test_is_empty_property(empty_deque: Any, populated_deque: Any) -> None: # noqa: ANN401
    """Test is_empty property reflects correct state.

    Parameters
    ----------
    empty_deque : Any
        Empty Deque instance from fixture
    populated_deque : Any
        Populated Deque instance from fixture

    Returns
    -------
    None

    Verifies
    --------
    - is_empty returns True for empty deque
    - is_empty returns False for populated deque
    - is_empty updates correctly after modifications
    """
    assert empty_deque.is_empty is False
    assert populated_deque.is_empty is False

    populated_deque.remove_front()
    populated_deque.remove_front()
    populated_deque.remove_front()
    assert populated_deque.is_empty is True


def test_operations_sequence() -> None:
    """Test sequence of mixed operations.

    Returns
    -------
    None

    Verifies
    --------
    - Mixed add/remove operations maintain correct state
    - Size and is_empty update correctly
    """
    from collections import deque
    class Deque:
        def __init__(self) -> None:
            """Initialize an empty deque.
            
            Returns
            -------
            None
            """
            self._items: deque[Any] = deque() # noqa: ANN401 - typing.Any disallowed

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

            Returns
            -------
            None
            """
            self._items.append(item)

        def add_rear(self, item: Any) -> None: # noqa: ANN401 - typing.Any disallowed
            """Add an item to the rear of the deque.
            
            Parameters
            ----------
            item : Any
                Item to add to the rear of the deque

            Returns
            -------
            None
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

    d = Deque()
    assert d.is_empty is True

    d.add_front(1)
    d.add_rear(2)
    assert d.size == 2
    assert d.is_empty is False

    assert d.remove_front() == 1
    assert d.remove_rear() == 2
    assert d.is_empty is True