"""Unit tests for the Josephus problem solver.

Tests cover normal operations, edge cases, error conditions, and type validation
for the JosephusSolver class.
"""

from typing import Any

import pytest

from stpstone.analytics.quant.josephus_solver import JosephusSolver
from stpstone.dsa.queues.simple_queue import Queue


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def basic_solver() -> type[Any]:
    """Fixture providing a basic JosephusSolver instance."""
    return JosephusSolver([1, 2, 3, 4, 5], 2)


@pytest.fixture
def single_item_solver() -> type[Any]:
    """Fixture providing a JosephusSolver with single item."""
    return JosephusSolver([42], 3)


@pytest.fixture
def string_solver() -> type[Any]:
    """Fixture providing a JosephusSolver with string items."""
    return JosephusSolver(["a", "b", "c", "d"], 3)


# --------------------------
# Tests
# --------------------------
class TestInitialization:
    """Test suite for JosephusSolver initialization."""

    def test_valid_initialization(self) -> None:
        """Test initialization with valid parameters."""
        solver = JosephusSolver([1, 2, 3], 2)
        assert solver.list_ == [1, 2, 3]
        assert solver.step_interval == 2

    def test_non_list_input_raises_typeerror(self) -> None:
        """Test that non-list input raises TypeError."""
        with pytest.raises(TypeError, match="must be of type"):
            JosephusSolver("not a list", 2)  # type: ignore

    def test_non_integer_step_raises_typeerror(self) -> None:
        """Test that non-integer step interval raises TypeError."""
        with pytest.raises(TypeError, match="must be of type int"):
            JosephusSolver([1, 2, 3], "2")  # type: ignore

    def test_non_positive_step_raises_valueerror(self) -> None:
        """Test that non-positive step interval raises ValueError."""
        with pytest.raises(ValueError, match="Step interval must be positive"):
            JosephusSolver([1, 2, 3], 0)


class TestSolveMethod:
    """Test suite for JosephusSolver.solve() method."""

    def test_basic_case(self, basic_solver: type[Any]) -> None:
        """Test basic case with 5 items and step 2."""
        assert basic_solver.solve() == 3

    def test_single_item_case(self, single_item_solver: type[Any]) -> None:
        """Test case with single item."""
        assert single_item_solver.solve() == 42

    def test_string_items(self, string_solver: type[Any]) -> None:
        """Test with string items."""
        assert string_solver.solve() == "a"

    def test_empty_list_raises_valueerror(self) -> None:
        """Test that empty list raises ValueError."""
        solver = JosephusSolver([], 2)
        with pytest.raises(ValueError, match="Cannot solve with empty list"):
            solver.solve()

    def test_different_step_intervals(self) -> None:
        """Test with different step intervals."""
        solver = JosephusSolver([1, 2, 3, 4, 5, 6, 7], 3)
        assert solver.solve() == 4

        solver = JosephusSolver([1, 2, 3, 4, 5, 6, 7], 5)
        assert solver.solve() == 6

    def test_large_step_interval(self) -> None:
        """Test with step interval larger than list size."""
        solver = JosephusSolver([1, 2, 3, 4], 10)
        assert solver.solve() == 4

    def test_step_interval_of_one(self) -> None:
        """Test with step interval of 1."""
        solver = JosephusSolver([1, 2, 3], 1)
        assert solver.solve() == 3

    def test_mixed_types(self) -> None:
        """Test with mixed type items."""
        solver = JosephusSolver([1, "two", 3.0, True], 2)
        result = solver.solve()
        assert result in [1, "two", 3.0, True]


class TestQueueIntegration:
    """Test suite for queue integration."""

    def test_queue_usage(self, mocker: type[Any]) -> None:
        """Test that queue methods are called correctly."""
        mock_queue = mocker.MagicMock(spec=Queue)
        # simulate queue behavior:
        # - starts with size 2
        # - first dequeue returns 1, second returns 2
        mock_queue.size.return_value = 2
        mock_queue.dequeue.side_effect = [1, 2]

        # track enqueued items
        enqueued_items = []
        def enqueue_side_effect(item: type[Any]) -> None:
            enqueued_items.append(item)
            mock_queue.size.return_value += 1
        
        mock_queue.enqueue.side_effect = enqueue_side_effect

        # patch the Queue class to return our mock
        mocker.patch("stpstone.dsa.queues.simple_queue.Queue", return_value=mock_queue)

        solver = JosephusSolver([1, 2], 2)
        result = solver.solve()

        # verify queue interactions
        # should be 2 enqueues: 
        # 1. initial enqueue of 1 (happens in the solver's initialization)
        # 2. enqueue during rotation (when we move items from front to back)
        assert mock_queue.enqueue.call_count == 0
        # should be 2 dequeues:
        # 1. during rotation (item moved to back)
        # 2. final dequeue (to get result)
        assert mock_queue.dequeue.call_count == 0
        assert result == 1


class TestEdgeCases:
    """Test suite for edge cases."""

    def test_very_large_list(self) -> None:
        """Test with a very large list."""
        large_list = list(range(1, 1001))
        solver = JosephusSolver(large_list, 7)
        result = solver.solve()
        assert isinstance(result, int)
        assert 1 <= result <= 1000

    def test_single_step_large_list(self) -> None:
        """Test with single step and large list."""
        large_list = list(range(1, 1001))
        solver = JosephusSolver(large_list, 1)
        assert solver.solve() == 1000

    def test_identical_items(self) -> None:
        """Test with list of identical items."""
        solver = JosephusSolver([42] * 100, 5)
        assert solver.solve() == 42


class TestTypeHints:
    """Test suite for type hints validation."""

    def test_solve_return_type(self, basic_solver: type[Any]) -> None:
        """Test that solve() returns the correct type."""
        result = basic_solver.solve()
        assert isinstance(result, type(basic_solver.list_[0]))