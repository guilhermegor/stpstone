"""Unit tests for BinaryComparator class.

Tests the binary comparison functionality with various input scenarios.
"""

import pytest

from stpstone.analytics.arithmetic.binary_comparator import BinaryComparator


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def comparator_a_less_than_b() -> BinaryComparator:
    """Fixture for case where a < b."""
    return BinaryComparator(a=5, b=10)


@pytest.fixture
def comparator_a_greater_than_b() -> BinaryComparator:
    """Fixture for case where a > b."""
    return BinaryComparator(a=15, b=10)


@pytest.fixture
def comparator_a_equal_to_b() -> BinaryComparator:
    """Fixture for case where a == b."""
    return BinaryComparator(a=10, b=10)


@pytest.fixture
def comparator_zero_values() -> BinaryComparator:
    """Fixture for case with zero values."""
    return BinaryComparator(a=0, b=0)


@pytest.fixture
def comparator_negative_values() -> BinaryComparator:
    """Fixture for case with negative values."""
    return BinaryComparator(a=-5, b=5)


@pytest.fixture
def comparator_large_numbers() -> BinaryComparator:
    """Fixture for case with very large numbers."""
    large_num = 2**64
    return BinaryComparator(a=large_num, b=large_num + 1)


@pytest.fixture
def comparator_small_numbers() -> BinaryComparator:
    """Fixture for case with very small numbers."""
    small_num = -2**64
    return BinaryComparator(a=small_num, b=small_num - 1)


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs() -> None:
    """Test initialization with valid integer inputs."""
    comparator = BinaryComparator(a=5, b=10)
    assert comparator.a == 5
    assert comparator.b == 10


def test_compare_a_less_than_b(comparator_a_less_than_b: BinaryComparator) -> None:
    """Test comparison when a is less than b."""
    result = comparator_a_less_than_b.compare()
    assert result == "A is less than B"


def test_compare_a_greater_than_b(comparator_a_greater_than_b: BinaryComparator) -> None:
    """Test comparison when a is greater than b."""
    result = comparator_a_greater_than_b.compare()
    assert result == "A is greater than B"


def test_compare_a_equal_to_b(comparator_a_equal_to_b: BinaryComparator) -> None:
    """Test comparison when a is equal to b."""
    result = comparator_a_equal_to_b.compare()
    assert result == "A is equal to B"


def test_compare_with_zero_values(comparator_zero_values: BinaryComparator) -> None:
    """Test comparison with zero values."""
    result = comparator_zero_values.compare()
    assert result == "A is equal to B"


def test_compare_with_negative_values(comparator_negative_values: BinaryComparator) -> None:
    """Test comparison with negative values."""
    result = comparator_negative_values.compare()
    assert result == "A is less than B"


def test_edge_case_large_numbers(comparator_large_numbers: BinaryComparator) -> None:
    """Test comparison with very large numbers."""
    result = comparator_large_numbers.compare()
    assert result == "A is less than B"


def test_edge_case_small_numbers(comparator_small_numbers: BinaryComparator) -> None:
    """Test comparison with very small numbers."""
    result = comparator_small_numbers.compare()
    assert result == "A is greater than B"