"""Unit tests for PriorityQueue class.

Tests the priority queue functionality including:
- Initialization and empty queue behavior
- Push operations with various priorities
- Pop operations and ordering
- Edge cases and error conditions
"""

from typing import TypeVar

import pytest

from stpstone.utils.dsa.queues.priority_queues import PriorityQueue


T = TypeVar('T')

# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def empty_queue() -> PriorityQueue[str]:
    """Provide empty PriorityQueue instance.

    Returns
    -------
    PriorityQueue[str]
        Empty priority queue instance
    """
    return PriorityQueue()


@pytest.fixture
def populated_queue() -> PriorityQueue[str]:
    """Provide PriorityQueue with test items.

    Returns
    -------
    PriorityQueue[str]
        Priority queue with test items:
        - "item1" with priority 3
        - "item2" with priority 1
        - "item3" with priority 2
    """
    queue = PriorityQueue()
    queue.push("item1", 3.0)
    queue.push("item2", 1.0)
    queue.push("item3", 2.0)
    return queue


# --------------------------
# Tests
# --------------------------
def test_init_empty_queue(empty_queue: PriorityQueue[str]) -> None:
    """Test initialization of empty priority queue.

    Parameters
    ----------
    empty_queue : PriorityQueue[str]
        Empty queue fixture

    Verifies
    --------
    - Queue is initialized with empty internal storage
    - Index counter starts at 0
    """
    assert len(empty_queue._queue) == 0
    assert empty_queue._index == 0


def test_push_single_item(empty_queue: PriorityQueue[str]) -> None:
    """Test pushing single item into queue.

    Parameters
    ----------
    empty_queue : PriorityQueue[str]
        Empty queue fixture

    Verifies
    --------
    - Item is added to queue with correct priority
    - Index is incremented
    - Queue length increases by 1
    """
    empty_queue.push("test_item", 5.0)
    assert len(empty_queue._queue) == 1
    assert empty_queue._queue[0] == (-5.0, 1, "test_item")
    assert empty_queue._index == 1


def test_push_multiple_items(empty_queue: PriorityQueue[str]) -> None:
    """Test pushing multiple items with different priorities.

    Parameters
    ----------
    empty_queue : PriorityQueue[str]
        Empty queue fixture

    Verifies
    --------
    - Items are stored with correct priorities
    - Indices are properly assigned
    - Queue length reflects number of pushes
    """
    empty_queue.push("item1", 3.0)
    empty_queue.push("item2", 1.0)
    empty_queue.push("item3", 2.0)
    
    assert len(empty_queue._queue) == 3
    assert empty_queue._index == 3
    
    # verify heap structure (priority, index, item)
    assert (-3.0, 1, "item1") in empty_queue._queue
    assert (-1.0, 2, "item2") in empty_queue._queue
    assert (-2.0, 3, "item3") in empty_queue._queue


def test_pop_from_empty_queue(empty_queue: PriorityQueue[str]) -> None:
    """Test popping from empty queue raises IndexError.

    Parameters
    ----------
    empty_queue : PriorityQueue[str]
        Empty queue fixture

    Verifies
    --------
    - IndexError is raised when popping from empty queue
    """
    with pytest.raises(IndexError):
        empty_queue.pop()


def test_pop_ordering(populated_queue: PriorityQueue[str]) -> None:
    """Test items are popped in correct priority order.

    Parameters
    ----------
    populated_queue : PriorityQueue[str]
        Populated queue fixture

    Verifies
    --------
    - Items are popped in descending priority order
    - Ties are broken by insertion order (FIFO)
    """
    assert populated_queue.pop() == "item1"  # Highest priority (3.0)
    assert populated_queue.pop() == "item3"  # Next priority (2.0)
    assert populated_queue.pop() == "item2"  # Lowest priority (1.0)


def test_push_after_pop(populated_queue: PriorityQueue[str]) -> None:
    """Test pushing new items after popping.

    Parameters
    ----------
    populated_queue : PriorityQueue[str]
        Populated queue fixture

    Verifies
    --------
    - New items can be added after popping
    - Correct ordering is maintained
    """
    populated_queue.pop()  # Remove first item
    populated_queue.push("item4", 4.0)  # New highest priority
    
    assert populated_queue.pop() == "item4"
    assert populated_queue.pop() == "item3"
    assert populated_queue.pop() == "item2"


def test_equal_priority_ordering(empty_queue: PriorityQueue[str]) -> None:
    """Test FIFO ordering for items with equal priority.

    Parameters
    ----------
    empty_queue : PriorityQueue[str]
        Empty queue fixture

    Verifies
    --------
    - Items with equal priority are popped in insertion order
    """
    empty_queue.push("item1", 1.0)
    empty_queue.push("item2", 1.0)
    empty_queue.push("item3", 1.0)
    
    assert empty_queue.pop() == "item1"
    assert empty_queue.pop() == "item2"
    assert empty_queue.pop() == "item3"


def test_negative_priority(empty_queue: PriorityQueue[str]) -> None:
    """Test handling of negative priorities.

    Parameters
    ----------
    empty_queue : PriorityQueue[str]
        Empty queue fixture

    Verifies
    --------
    - Negative priorities are handled correctly
    - Correct ordering is maintained
    """
    empty_queue.push("item1", -1.0)
    empty_queue.push("item2", -2.0)
    empty_queue.push("item3", -3.0)
    
    # Higher negative numbers should come out first (treated as higher priority)
    assert empty_queue.pop() == "item1"  # -1.0 is highest
    assert empty_queue.pop() == "item2"  # -2.0 next
    assert empty_queue.pop() == "item3"  # -3.0 lowest


def test_queue_length_property(populated_queue: PriorityQueue[str]) -> None:
    """Test queue length property.

    Parameters
    ----------
    populated_queue : PriorityQueue[str]
        Populated queue fixture

    Verifies
    --------
    - Queue length matches number of items
    - Length decreases after pops
    """
    assert len(populated_queue._queue) == 3
    populated_queue.pop()
    assert len(populated_queue._queue) == 2
    populated_queue.pop()
    assert len(populated_queue._queue) == 1
    populated_queue.pop()
    assert len(populated_queue._queue) == 0


def test_generic_type_support() -> None:
    """Test support for generic type parameters.

    Verifies
    --------
    - Queue works with different types (int, str, custom objects)
    - Type hints are preserved
    """
    class CustomObject:
        """Create custom object for testing.
        
        Parameters
        ----------
        value : str
            Value to be stored in the object
        """

        def __init__(self, value: str) -> None:
            """Initialize constructor method.
            
            Parameters
            ----------
            value : str
                Value to be stored in the object
            
            Returns
            -------
            None
            """
            self.value = value
    
    int_queue = PriorityQueue[int]()
    int_queue.push(42, 1.0)
    assert int_queue.pop() == 42
    
    str_queue = PriorityQueue[str]()
    str_queue.push("test", 1.0)
    assert str_queue.pop() == "test"
    
    obj_queue = PriorityQueue[CustomObject]()
    obj = CustomObject("test")
    obj_queue.push(obj, 1.0)
    assert obj_queue.pop() is obj


def test_large_number_of_items(empty_queue: PriorityQueue[str]) -> None:
    """Test with large number of items.

    Parameters
    ----------
    empty_queue : PriorityQueue[str]
        Empty queue fixture

    Verifies
    --------
    - Queue handles large numbers of items correctly
    - Items are popped in correct order
    """
    for i in range(1000):
        empty_queue.push(f"item{i}", float(i))
    
    for i in range(999, -1, -1):
        assert empty_queue.pop() == f"item{i}"


def test_duplicate_items(empty_queue: PriorityQueue[str]) -> None:
    """Test handling of duplicate items.

    Parameters
    ----------
    empty_queue : PriorityQueue[str]
        Empty queue fixture

    Verifies
    --------
    - Duplicate items are handled correctly
    - Correct ordering is maintained
    """
    empty_queue.push("duplicate", 1.0)
    empty_queue.push("duplicate", 2.0)
    empty_queue.push("duplicate", 3.0)
    
    assert empty_queue.pop() == "duplicate"  # priority 3.0
    assert empty_queue.pop() == "duplicate"  # priority 2.0
    assert empty_queue.pop() == "duplicate"  # priority 1.0