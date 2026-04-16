"""Unit tests for Stack class implementation.

Tests the stack data structure functionality including:
- Initialization with various capacities
- Push/pop operations
- Edge cases and error conditions
- Type validation and capacity management
"""

from typing import Any

import pytest

from stpstone.utils.dsa.stacks.simple_stack import Stack


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def empty_stack() -> Stack:
	"""Fixture providing empty stack with default capacity.

	Returns
	-------
	Stack
		Stack instance with no elements
	"""
	return Stack()


@pytest.fixture
def filled_stack() -> Stack:
	"""Fixture providing stack with sample data.

	Returns
	-------
	Stack
		Stack instance with 3 elements
	"""
	stack = Stack()
	for item in ["a", "b", "c"]:
		stack.push(item)
	return stack


# --------------------------
# Tests
# --------------------------
class TestInitialization:
	"""Test cases for Stack initialization."""

	def test_default_initialization(self) -> None:
		"""Test initialization with default capacity.

		Verifies that:
		- Stack initializes with default capacity
		- Size is 0
		- Underlying array has correct length
		"""
		stack = Stack()
		assert stack.size == 0
		assert stack.is_empty
		assert len(stack._items) == 10

	def test_custom_initialization(self) -> None:
		"""Test initialization with custom capacity.

		Verifies that:
		- Stack initializes with specified capacity
		- Size is 0
		- Underlying array has correct length
		"""
		stack = Stack(20)
		assert stack.size == 0
		assert stack.is_empty
		assert len(stack._items) == 20

	@pytest.mark.parametrize("invalid_capacity", [0, -1, -10])
	def test_invalid_initial_capacity(self, invalid_capacity: int) -> None:
		"""Test initialization with invalid capacities.

		Parameters
		----------
		invalid_capacity : int
			Invalid capacity values to test

		Verifies that:
		- ValueError is raised for non-positive capacities
		"""
		with pytest.raises(ValueError, match="Capacity must be a positive integer"):
			Stack(invalid_capacity)

	@pytest.mark.parametrize("invalid_type", [1.5, "10", None, []])
	def test_non_integer_initial_capacity(self, invalid_type: Any) -> None:  # noqa: ANN401
		"""Test initialization with non-integer capacities.

		Parameters
		----------
		invalid_type : Any
			Invalid type values to test

		Verifies that:
		- ValueError is raised for non-integer capacities
		"""
		with pytest.raises(TypeError, match="must be of type"):
			Stack(invalid_type)


class TestPushOperations:
	"""Test cases for stack push operations."""

	def test_push_single_item(self, empty_stack: Stack) -> None:
		"""Test pushing single item to stack.

		Parameters
		----------
		empty_stack : Stack
			Empty stack fixture

		Verifies that:
		- Item is added to stack
		- Size increases by 1
		- Item is at top of stack
		"""
		empty_stack.push("test")
		assert empty_stack.size == 1
		assert not empty_stack.is_empty
		assert empty_stack.peek == "test"

	def test_push_multiple_items(self, empty_stack: Stack) -> None:
		"""Test pushing multiple items to stack.

		Parameters
		----------
		empty_stack : Stack
			Empty stack fixture

		Verifies that:
		- Items are added in correct order
		- Size increases appropriately
		- Last item is at top of stack
		"""
		items = ["a", "b", "c"]
		for i, item in enumerate(items, 1):
			empty_stack.push(item)
			assert empty_stack.size == i
			assert empty_stack.peek == item

	def test_push_resize(self) -> None:
		"""Test automatic resizing when pushing beyond capacity.

		Verifies that:
		- Stack resizes when capacity is reached
		- All items are preserved after resize
		- Size remains correct
		"""
		stack = Stack(2)  # Small capacity for testing
		stack.push(1)
		stack.push(2)
		assert len(stack._items) == 2
		stack.push(3)  # Should trigger resize
		assert len(stack._items) == 4
		assert stack.size == 3
		assert stack.pop() == 3
		assert stack.pop() == 2
		assert stack.pop() == 1


class TestPopOperations:
	"""Test cases for stack pop operations."""

	def test_pop_single_item(self, filled_stack: Stack) -> None:
		"""Test popping from non-empty stack.

		Parameters
		----------
		filled_stack : Stack
			Stack with items fixture

		Verifies that:
		- Correct item is returned
		- Size decreases by 1
		- Item is removed from stack
		"""
		initial_size = filled_stack.size
		item = filled_stack.pop()
		assert item == "c"
		assert filled_stack.size == initial_size - 1
		assert filled_stack.peek == "b"

	def test_pop_all_items(self, filled_stack: Stack) -> None:
		"""Test popping all items from stack.

		Parameters
		----------
		filled_stack : Stack
			Stack with items fixture

		Verifies that:
		- Items are popped in correct order
		- Size decreases appropriately
		- Stack becomes empty
		"""
		assert filled_stack.pop() == "c"
		assert filled_stack.pop() == "b"
		assert filled_stack.pop() == "a"
		assert filled_stack.is_empty
		assert filled_stack.size == 0

	def test_pop_empty_stack(self, empty_stack: Stack) -> None:
		"""Test popping from empty stack.

		Parameters
		----------
		empty_stack : Stack
			Empty stack fixture

		Verifies that:
		- IndexError is raised
		- Size remains 0
		"""
		with pytest.raises(IndexError, match="Operation on an empty stack"):
			empty_stack.pop()
		assert empty_stack.size == 0


class TestPeekOperations:
	"""Test cases for stack peek operations."""

	def test_peek_filled_stack(self, filled_stack: Stack) -> None:
		"""Test peeking at non-empty stack.

		Parameters
		----------
		filled_stack : Stack
			Stack with items fixture

		Verifies that:
		- Correct item is returned
		- Size remains unchanged
		"""
		assert filled_stack.peek == "c"
		assert filled_stack.size == 3

	def test_peek_empty_stack(self, empty_stack: Stack) -> None:
		"""Test peeking at empty stack.

		Parameters
		----------
		empty_stack : Stack
			Empty stack fixture

		Verifies that:
		- IndexError is raised
		- Size remains 0
		"""
		with pytest.raises(IndexError, match="Operation on an empty stack"):
			_ = empty_stack.peek
		assert empty_stack.size == 0


class TestSizeOperations:
	"""Test cases for stack size operations."""

	def test_size_empty_stack(self, empty_stack: Stack) -> None:
		"""Test size of empty stack.

		Parameters
		----------
		empty_stack : Stack
			Empty stack fixture

		Verifies that:
		- Size is 0
		"""
		assert empty_stack.size == 0

	def test_size_filled_stack(self, filled_stack: Stack) -> None:
		"""Test size of filled stack.

		Parameters
		----------
		filled_stack : Stack
			Stack with items fixture

		Verifies that:
		- Size matches number of pushed items
		"""
		assert filled_stack.size == 3


class TestResizeOperations:
	"""Test cases for stack resize operations."""

	def test_resize_larger(self) -> None:
		"""Test resizing to larger capacity.

		Verifies that:
		- Underlying array grows to new capacity
		- All items are preserved
		- Size remains unchanged
		"""
		stack = Stack(2)
		stack.push(1)
		stack.push(2)
		stack._resize(4)
		assert len(stack._items) == 4
		assert stack.size == 2
		assert stack.pop() == 2
		assert stack.pop() == 1

	def test_resize_smaller(self) -> None:
		"""Test resizing to smaller capacity.

		Verifies that:
		- Underlying array shrinks to new capacity
		- Items beyond new capacity are discarded
		- Size is adjusted if necessary
		"""
		stack = Stack(4)
		stack.push(1)
		stack.push(2)
		stack._resize(2)
		assert len(stack._items) == 2
		assert stack.size == 2
		assert stack.pop() == 2
		assert stack.pop() == 1

	@pytest.mark.parametrize("invalid_capacity", [0, -1, -10])
	def test_invalid_resize_capacity(self, invalid_capacity: int) -> None:
		"""Test resizing with invalid capacities.

		Parameters
		----------
		invalid_capacity : int
			Invalid capacity values to test

		Verifies that:
		- ValueError is raised for non-positive capacities
		"""
		stack = Stack()
		with pytest.raises(ValueError, match="Capacity must be a positive integer"):
			stack._resize(invalid_capacity)

	def test_resize_empty_stack(self, empty_stack: Stack) -> None:
		"""Test resizing empty stack.

		Parameters
		----------
		empty_stack : Stack
			Empty stack fixture

		Verifies that:
		- Underlying array is resized
		- Size remains 0
		"""
		empty_stack._resize(20)
		assert len(empty_stack._items) == 20
		assert empty_stack.size == 0


class TestTypeHandling:
	"""Test cases for stack type handling."""

	@pytest.mark.parametrize("item", [1, "str", 3.14, None, [1, 2], {"key": "value"}])
	def test_push_different_types(self, item: Any, empty_stack: Stack) -> None:  # noqa: ANN401
		"""Test pushing various types to stack.

		Parameters
		----------
		item : Any
			Item of various types to push
		empty_stack : Stack
			Empty stack fixture

		Verifies that:
		- Stack can handle different types
		- Items are stored and retrieved correctly
		"""
		empty_stack.push(item)
		assert empty_stack.pop() == item
