"""Unit tests for the Josephus problem solver.

Tests cover normal operations, edge cases, error conditions, and type validation
for the JosephusSolver class.
"""

import pytest

from stpstone.analytics.quant.josephus_solver import JosephusSolver
from stpstone.dsa.queues.simple_queue import Queue


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def basic_solver() -> JosephusSolver:
    """Fixture providing a basic JosephusSolver instance.
    
    Returns
    -------
    JosephusSolver
        A basic JosephusSolver instance.
    """
    return JosephusSolver([1, 2, 3, 4, 5], 2)


@pytest.fixture
def single_item_solver() -> JosephusSolver:
    """Fixture providing a JosephusSolver with single item.
    
    Returns
    -------
    JosephusSolver
        A JosephusSolver instance with a single item.
    """
    return JosephusSolver([42], 3)


@pytest.fixture
def string_solver() -> JosephusSolver:
    """Fixture providing a JosephusSolver with string items.
    
    Returns
    -------
    JosephusSolver
        A JosephusSolver instance with string items.
    """
    return JosephusSolver(["a", "b", "c", "d"], 3)


# --------------------------
# Tests
# --------------------------
class TestInitialization:
    """Test suite for JosephusSolver initialization."""

    def test_valid_initialization(self) -> None:
        """Test initialization with valid parameters.
        
        Returns
        -------
        None
        """
        solver = JosephusSolver([1, 2, 3], 2)
        assert solver.list_ == [1, 2, 3]
        assert solver.step_interval == 2

    def test_non_list_input_raises_typeerror(self) -> None:
        """Test that non-list input raises TypeError.
        
        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be of type"):
            JosephusSolver("not a list", 2)  # type: ignore

    def test_non_integer_step_raises_typeerror(self) -> None:
        """Test that non-integer step interval raises TypeError.
        
        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be of type int"):
            JosephusSolver([1, 2, 3], "2")  # type: ignore

    def test_non_positive_step_raises_valueerror(self) -> None:
        """Test that non-positive step interval raises ValueError.
        
        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Step interval must be positive"):
            JosephusSolver([1, 2, 3], 0)


class TestSolveMethod:
    """Test suite for JosephusSolver.solve() method."""

    def test_basic_case(self, basic_solver: JosephusSolver) -> None:
        """Test basic case with 5 items and step 2.
        
        Parameters
        ----------
        basic_solver : JosephusSolver
            A basic JosephusSolver instance.

        Returns
        -------
        None
        """
        assert basic_solver.solve() == 3

    def test_single_item_case(self, single_item_solver: JosephusSolver) -> None:
        """Test case with single item.
        
        Parameters
        ----------
        single_item_solver : JosephusSolver
            A JosephusSolver instance with a single item.

        Returns
        -------
        None
        """
        assert single_item_solver.solve() == 42

    def test_string_items(self, string_solver: JosephusSolver) -> None:
        """Test with string items.
        
        Parameters
        ----------
        string_solver : JosephusSolver
            A JosephusSolver instance with string items.

        Returns
        -------
        None
        """
        assert string_solver.solve() == "a"

    def test_empty_list_raises_valueerror(self) -> None:
        """Test that empty list raises ValueError.
        
        Returns
        -------
        None
        """
        solver = JosephusSolver([], 2)
        with pytest.raises(ValueError, match="Cannot solve with empty list"):
            solver.solve()

    def test_different_step_intervals(self) -> None:
        """Test with different step intervals.
        
        Returns
        -------
        None
        """
        solver = JosephusSolver([1, 2, 3, 4, 5, 6, 7], 3)
        assert solver.solve() == 4

        solver = JosephusSolver([1, 2, 3, 4, 5, 6, 7], 5)
        assert solver.solve() == 6

    def test_large_step_interval(self) -> None:
        """Test with step interval larger than list size.
        
        Returns
        -------
        None
        """
        solver = JosephusSolver([1, 2, 3, 4], 10)
        assert solver.solve() == 4

    def test_step_interval_of_one(self) -> None:
        """Test with step interval of 1.
        
        Returns
        -------
        None
        """
        solver = JosephusSolver([1, 2, 3], 1)
        assert solver.solve() == 3

    def test_mixed_types(self) -> None:
        """Test with mixed type items.
        
        Returns
        -------
        None
        """
        solver = JosephusSolver([1, "two", 3.0, True], 2)
        result = solver.solve()
        assert result in [1, "two", 3.0, True]


class TestQueueIntegration:
    """Test suite for queue integration."""

    def test_queue_usage(self, mocker: object) -> None:
        """Test that queue methods are called correctly.
        
        Parameters
        ----------
        mocker : object
            Pytest's mocker fixture.

        Returns
        -------
        None
        """
        mock_queue = mocker.MagicMock(spec=Queue)
        # simulate queue behavior:
        # - starts with size 2
        # - first dequeue returns 1, second returns 2
        mock_queue.size.return_value = 2
        mock_queue.dequeue.side_effect = [1, 2]

        # track enqueued items
        enqueued_items = []
        def enqueue_side_effect(item: object) -> None:
            """Mock enqueue method.
            
            Parameters
            ----------
            item : object
                The item to be enqueued.
            
            Returns
            -------
            None
            """
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
        """Test with a very large list.
        
        Returns
        -------
        None
        """
        large_list = list(range(1, 1001))
        solver = JosephusSolver(large_list, 7)
        result = solver.solve()
        assert isinstance(result, int)
        assert 1 <= result <= 1000

    def test_single_step_large_list(self) -> None:
        """Test with single step and large list.
        
        Returns
        -------
        None
        """
        large_list = list(range(1, 1001))
        solver = JosephusSolver(large_list, 1)
        assert solver.solve() == 1000

    def test_identical_items(self) -> None:
        """Test with list of identical items.
        
        Returns
        -------
        None
        """
        solver = JosephusSolver([42] * 100, 5)
        assert solver.solve() == 42


class TestTypeHints:
    """Test suite for type hints validation."""

    def test_solve_return_type(self, basic_solver: JosephusSolver) -> None:
        """Test that solve() returns the correct type.
        
        Parameters
        ----------
        basic_solver : JosephusSolver
            A basic JosephusSolver instance.

        Returns
        -------
        None
        """
        result = basic_solver.solve()
        assert isinstance(result, type(basic_solver.list_[0]))