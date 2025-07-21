"""Unit tests for stpstone arithmetic subtractors.

Tests the HalfSubtractor and FullSubtractor classes functionality.
"""

from typing import Any

import pytest

from stpstone.analytics.arithmetic.bit_subtractor import FullSubtractor, HalfSubtractor


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture(params=[(0, 0, 0, 0), (0, 1, 1, 1), (1, 0, 1, 0), (1, 1, 0, 0)])
def half_subtractor_cases(request: pytest.FixtureRequest) -> tuple[int, int, int, int]:
    """Fixture providing all possible input combinations for HalfSubtractor.
    
    Parameters
    ----------
    request : pytest.FixtureRequest
        The fixture request object

    Returns
    -------
    tuple[int, int, int, int]
        (a, b, expected_diff, expected_borrow)
    """
    return request.param

@pytest.fixture(params=[
    (0, 0, 0, 0, 0),
    (0, 1, 0, 1, 1),
    (1, 0, 1, 0, 0),
    (1, 1, 1, 1, 1),
    (0, 0, 1, 1, 1),
    (0, 1, 1, 0, 1),
    (1, 0, 0, 1, 0),
    (1, 1, 0, 0, 0)
])
def full_subtractor_cases(request: pytest.FixtureRequest) -> tuple[int, int, int, int, int]:
    """Fixture providing various input combinations for FullSubtractor.
    
    Parameters
    ----------
    request : pytest.FixtureRequest
        The fixture request object

    Returns
    -------
    tuple[int, int, int, int, int]
        (a, b, borrow_in, expected_diff, expected_borrow)
    """
    return request.param


# --------------------------
# Tests for HalfSubtractor
# --------------------------
def test_half_subtractor_normal_operations(half_subtractor_cases: tuple[int, int, int, int]) \
    -> None:
    """Test normal operations of HalfSubtractor.
    
    Parameters
    ----------
    half_subtractor_cases : tuple[int, int, int, int]
        (a, b, expected_diff, expected_borrow)
    
    Returns
    -------
    None
    """
    a, b, expected_diff, expected_borrow = half_subtractor_cases
    subtractor = HalfSubtractor(a, b)
    assert subtractor.get_difference() == expected_diff
    assert subtractor.get_borrow() == expected_borrow

@pytest.mark.parametrize("a,b", [
    ("0", 1),
    (0, "1"),
    (0.5, 1),
    (0, 1.0)
])
def test_half_subtractor_type_validation(a: type[Any], b: type[Any]) -> None:
    """Test type validation in HalfSubtractor.
    
    Parameters
    ----------
    a : type[Any]
        First bit input type
    b : type[Any]
        Second bit input type
    """
    with pytest.raises(TypeError):
        HalfSubtractor(a, b)

@pytest.mark.parametrize("a,b", [
    (2, 1),
    (0, -1),
    (3, 4)
])
def test_half_subtractor_value_validation(a: int, b: int) -> None:
    """Test value validation in HalfSubtractor.
    
    Parameters
    ----------
    a : int
        First bit input (0 or 1)
    b : int
        Second bit input (0 or 1)
    """
    with pytest.raises(ValueError):
        HalfSubtractor(a, b)

def test_half_subtractor_docstring_examples() -> None:
    """Test the examples provided in docstrings."""
    subtractor = HalfSubtractor(1, 0)
    assert subtractor.get_difference() == 1
    assert subtractor.get_borrow() == 0

    assert HalfSubtractor(0, 1).get_borrow() == 1
    assert HalfSubtractor(1, 0).get_borrow() == 0


# --------------------------
# Tests for FullSubtractor
# --------------------------
def test_full_subtractor_normal_operations(
    full_subtractor_cases: tuple[int, int, int, int, int]
) -> None:
    """Test normal operations of FullSubtractor.
    
    Parameters
    ----------
    full_subtractor_cases : tuple[int, int, int, int, int]
        (a, b, borrow_in, expected_diff, expected_borrow)
    
    Returns
    -------
    None
    """
    a, b, borrow_in, expected_diff, expected_borrow = full_subtractor_cases
    subtractor = FullSubtractor(a, b, borrow_in)
    assert subtractor.get_difference() == expected_diff
    assert subtractor.get_borrow_out() == expected_borrow

@pytest.mark.parametrize("a,b,borrow_in", [
    (0, 1, 0),
    (0, 0, 1),
    (1, 1, 1)
])
def test_full_subtractor_borrow_propagation(a: int, b: int, borrow_in: int) -> None:
    """Test borrow propagation in FullSubtractor.
    
    Parameters
    ----------
    a : int
        First bit input (0 or 1)
    b : int
        Second bit input (0 or 1)
    borrow_in : int
        Borrow input from previous stage (0 or 1)
    """
    assert FullSubtractor(a, b, borrow_in).get_borrow_out() == 1

@pytest.mark.parametrize("a,b,borrow_in", [
    ("0", 1, 0),
    (0, [1], 0),
    (0, 1, 0.5)
])
def test_full_subtractor_type_validation(a: type[Any], b: type[Any], borrow_in: type[Any]) -> None:
    """Test type validation in FullSubtractor.
    
    Parameters
    ----------
    a : type[Any]
        First bit input type
    b : type[Any]
        Second bit input type
    borrow_in : type[Any]
        Borrow input from previous stage type
    """
    with pytest.raises(TypeError):
        FullSubtractor(a, b, borrow_in)

@pytest.mark.parametrize("a,b,borrow_in", [
    (2, 1, 0),
    (0, -1, 0),
    (0, 1, 2)
])
def test_full_subtractor_value_validation(a: int, b: int, borrow_in: int) -> None:
    """Test value validation in FullSubtractor.
    
    Parameters
    ----------
    a : int
        First bit input (0 or 1)
    b : int
        Second bit input (0 or 1)
    borrow_in : int
        Borrow input from previous stage (0 or 1)
    """
    with pytest.raises(ValueError):
        FullSubtractor(a, b, borrow_in)

def test_full_subtractor_docstring_examples() -> None:
    """Test the examples provided in docstrings."""
    subtractor = FullSubtractor(1, 0, 1)
    assert subtractor.get_difference() == 0
    assert subtractor.get_borrow_out() == 0

    assert FullSubtractor(1, 0, 1).get_borrow_out() == 0
    assert FullSubtractor(0, 0, 1).get_borrow_out() == 1