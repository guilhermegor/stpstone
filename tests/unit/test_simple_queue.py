"""Unit tests for Queue implementation using collections.deque."""

from collections import deque

import pytest

from stpstone.dsa.queues.simple_queue import Queue


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def empty_queue() -> Queue:
    """Fixture providing an empty Queue instance.
    
    Returns
    -------
    Queue
        Empty Queue instance
    """
    return Queue()


@pytest.fixture
def filled_queue() -> Queue:
    """Fixture providing a Queue instance with 3 items.
    
    Returns
    -------
    Queue
        Filled Queue instance
    """
    q = Queue()
    for i in range(3):
        q.enqueue(i)
    return q


# --------------------------
# Tests
# --------------------------
class TestQueueInitialization:
    """Tests for Queue initialization and basic properties."""

    def test_init_creates_empty_queue(self, empty_queue: Queue) -> None:
        """Test that initialization creates an empty queue.
        
        Parameters
        ----------
        empty_queue : Queue
            Queue fixture

        Returns
        -------
        None
        """
        assert empty_queue.size() == 0
        assert empty_queue.is_empty() is True

    def test_internal_storage_is_deque(self, empty_queue: Queue) -> None:
        """Test that internal storage uses collections.deque.
        
        Parameters
        ----------
        empty_queue : Queue
            Queue fixture

        Returns
        -------
        None
        """
        assert isinstance(empty_queue._items, deque)


class TestQueueOperations:
    """Tests for standard queue operations."""

    def test_enqueue_adds_items(self, empty_queue: Queue) -> None:
        """Test that enqueue adds items to the queue.
        
        Parameters
        ----------
        empty_queue : Queue
            Queue fixture

        Returns
        -------
        None
        """
        empty_queue.enqueue(1)
        assert empty_queue.size() == 1
        assert empty_queue.is_empty() is False

    def test_dequeue_removes_items(self, filled_queue: Queue) -> None:
        """Test that dequeue removes items in FIFO order.
        
        Parameters
        ----------
        filled_queue : Queue
            Queue fixture

        Returns
        -------
        None
        """
        assert filled_queue.dequeue() == 0
        assert filled_queue.size() == 2
        assert filled_queue.dequeue() == 1
        assert filled_queue.size() == 1

    def test_size_returns_correct_value(self, filled_queue: Queue) -> None:
        """Test that size returns the correct number of items.
        
        Parameters
        ----------
        filled_queue : Queue
            Queue fixture

        Returns
        -------
        None
        """
        assert filled_queue.size() == 3
        filled_queue.dequeue()
        assert filled_queue.size() == 2

    def test_is_empty_returns_correct_state(self, empty_queue: Queue) -> None:
        """Test that is_empty returns correct queue state.
        
        Parameters
        ----------
        empty_queue : Queue
            Queue fixture

        Returns
        -------
        None
        """
        assert empty_queue.is_empty() is True
        empty_queue.enqueue(1)
        assert empty_queue.is_empty() is False


class TestQueueEdgeCases:
    """Tests for edge cases and special scenarios."""

    def test_dequeue_from_empty_raises_error(self, empty_queue: Queue) -> None:
        """Test that dequeue from empty queue raises IndexError.
        
        Parameters
        ----------
        empty_queue : Queue
            Queue fixture

        Returns
        -------
        None
        """
        with pytest.raises(IndexError) as excinfo:
            empty_queue.dequeue()
        assert "Dequeue from an empty queue" in str(excinfo.value)

    def test_queue_maintains_fifo_order(self, empty_queue: Queue) -> None:
        """Test that queue maintains first-in-first-out order.
        
        Parameters
        ----------
        empty_queue : Queue
            Queue fixture

        Returns
        -------
        None
        """
        items = ["first", "second", "third"]
        for item in items:
            empty_queue.enqueue(item)

        for expected in items:
            assert empty_queue.dequeue() == expected

    def test_multiple_enqueue_dequeue_operations(self, empty_queue: Queue) -> None:
        """Test alternating enqueue and dequeue operations.
        
        Parameters
        ----------
        empty_queue : Queue
            Queue fixture

        Returns
        -------
        None
        """
        for i in range(5):
            empty_queue.enqueue(i)
            assert empty_queue.dequeue() == i
            assert empty_queue.is_empty() is True


class TestQueueTypeHandling:
    """Tests for handling different data types."""

    @pytest.mark.parametrize("item", [
        None,
        42,
        3.14,
        "string",
        [1, 2, 3],
        {"key": "value"},
        (1, 2, 3),
        True,
        deque()
    ])
    def test_enqueue_dequeue_various_types(self, empty_queue: Queue, item: object) -> None:
        """Test that queue can handle various data types.
        
        Parameters
        ----------
        empty_queue : Queue
            Queue fixture
        item : object
            Item to be added to the queue

        Returns
        -------
        None
        """
        empty_queue.enqueue(item)
        assert empty_queue.dequeue() == item

    def test_mixed_type_operations(self, empty_queue: Queue) -> None:
        """Test queue with mixed type operations.
        
        Parameters
        ----------
        empty_queue : Queue
            Queue fixture

        Returns
        -------
        None
        """
        items = [1, "two", 3.0, None, [4]]
        for item in items:
            empty_queue.enqueue(item)

        for expected in items:
            assert empty_queue.dequeue() == expected


class TestQueuePerformance:
    """Tests for queue performance characteristics."""

    def test_large_number_of_operations(self, empty_queue: Queue) -> None:
        """Test queue with large number of operations.
        
        Parameters
        ----------
        empty_queue : Queue
            Queue fixture

        Returns
        -------
        None
        """
        test_size = 10000
        for i in range(test_size):
            empty_queue.enqueue(i)
            assert empty_queue.size() == i + 1

        for i in range(test_size):
            assert empty_queue.dequeue() == i
            assert empty_queue.size() == test_size - i - 1

        assert empty_queue.is_empty() is True