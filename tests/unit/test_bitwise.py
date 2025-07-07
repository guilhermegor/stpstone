"""Unit tests for bitwise operation utilities.

This module provides comprehensive tests for the Bitwise class that implements
basic bitwise operations including AND, OR, XOR, and NOT.
"""

from typing import Any

import pytest

from stpstone.analytics.arithmetic.bitwise import Bitwise


@pytest.fixture
def bitwise_utils() -> Bitwise:
    """Fixture providing a Bitwise instance for testing."""
    return Bitwise()


# --------------------------
# Bitwise AND Tests
# --------------------------
def test_bitwise_and_basic_operations(bitwise_utils: Bitwise) -> None:
    """Test basic bitwise AND operations."""
    assert bitwise_utils.bitwise_and(3, 5) == 1
    assert bitwise_utils.bitwise_and(0b1100, 0b1010) == 0b1000
    assert bitwise_utils.bitwise_and(7, 3) == 3  # 0b111 & 0b011 = 0b011
    assert bitwise_utils.bitwise_and(15, 7) == 7  # 0b1111 & 0b0111 = 0b0111
    # 0b11111111 & 0b01010101 = 0b01010101
    assert bitwise_utils.bitwise_and(255, 85) == 85


def test_bitwise_and_edge_cases(bitwise_utils: Bitwise) -> None:
    """Test edge cases for bitwise AND."""
    assert bitwise_utils.bitwise_and(0, 0) == 0
    assert bitwise_utils.bitwise_and(0, 5) == 0
    assert bitwise_utils.bitwise_and(5, 0) == 0
    
    assert bitwise_utils.bitwise_and(5, 5) == 5
    assert bitwise_utils.bitwise_and(255, 255) == 255
    
    assert bitwise_utils.bitwise_and(1024, 512) == 0
    assert bitwise_utils.bitwise_and(2**31 - 1, 2**30 - 1) == 2**30 - 1


def test_bitwise_and_negative_numbers(bitwise_utils: Bitwise) -> None:
    """Test bitwise AND with negative numbers."""
    assert bitwise_utils.bitwise_and(-1, 5) == 5  # All 1s & 5 = 5
    assert bitwise_utils.bitwise_and(-2, 7) == 6  # ...11111110 & 00000111 = 00000110
    assert bitwise_utils.bitwise_and(-5, -3) == -7  # Two's complement arithmetic


# --------------------------
# Bitwise OR Tests
# --------------------------
def test_bitwise_or_basic_operations(bitwise_utils: Bitwise) -> None:
    """Test basic bitwise OR operations."""
    assert bitwise_utils.bitwise_or(3, 5) == 7
    assert bitwise_utils.bitwise_or(0b1100, 0b1010) == 0b1110
    
    assert bitwise_utils.bitwise_or(1, 2) == 3  # 0b001 | 0b010 = 0b011
    assert bitwise_utils.bitwise_or(8, 4) == 12  # 0b1000 | 0b0100 = 0b1100
    # 0b00001111 | 0b11110000 = 0b11111111
    assert bitwise_utils.bitwise_or(15, 240) == 255


def test_bitwise_or_edge_cases(bitwise_utils: Bitwise) -> None:
    """Test edge cases for bitwise OR."""
    assert bitwise_utils.bitwise_or(0, 0) == 0
    assert bitwise_utils.bitwise_or(0, 5) == 5
    assert bitwise_utils.bitwise_or(5, 0) == 5
    
    assert bitwise_utils.bitwise_or(5, 5) == 5
    assert bitwise_utils.bitwise_or(255, 255) == 255
    
    assert bitwise_utils.bitwise_or(1024, 512) == 1536
    assert bitwise_utils.bitwise_or(2**31 - 1, 2**30) == 2**31 - 1


def test_bitwise_or_negative_numbers(bitwise_utils: Bitwise) -> None:
    """Test bitwise OR with negative numbers."""
    assert bitwise_utils.bitwise_or(-1, 5) == -1  # all 1s | anything = all 1s
    assert bitwise_utils.bitwise_or(-8, 3) == -5  # two's complement arithmetic
    assert bitwise_utils.bitwise_or(-5, -3) == -1


# --------------------------
# Bitwise XOR Tests
# --------------------------
def test_bitwise_xor_basic_operations(bitwise_utils: Bitwise) -> None:
    """Test basic bitwise XOR operations."""
    assert bitwise_utils.bitwise_xor(3, 5) == 6
    assert bitwise_utils.bitwise_xor(0b1100, 0b1010) == 0b0110
    
    assert bitwise_utils.bitwise_xor(1, 3) == 2  # 0b001 ^ 0b011 = 0b010
    assert bitwise_utils.bitwise_xor(15, 7) == 8  # 0b1111 ^ 0b0111 = 0b1000
    # 0b11111111 ^ 0b10101010 = 0b01010101
    assert bitwise_utils.bitwise_xor(255, 170) == 85


def test_bitwise_xor_edge_cases(bitwise_utils: Bitwise) -> None:
    """Test edge cases for bitwise XOR."""
    assert bitwise_utils.bitwise_xor(0, 0) == 0
    assert bitwise_utils.bitwise_xor(0, 5) == 5
    assert bitwise_utils.bitwise_xor(5, 0) == 5
    
    assert bitwise_utils.bitwise_xor(5, 5) == 0
    assert bitwise_utils.bitwise_xor(255, 255) == 0
    assert bitwise_utils.bitwise_xor(1024, 1024) == 0
    
    # xor properties
    # a ^ b ^ b = a
    a, b = 42, 73
    result = bitwise_utils.bitwise_xor(a, b)
    assert bitwise_utils.bitwise_xor(result, b) == a
    
    # large numbers
    assert bitwise_utils.bitwise_xor(2**31 - 1, 2**30) == 2**31 - 1 - 2**30


def test_bitwise_xor_negative_numbers(bitwise_utils: Bitwise) -> None:
    """Test bitwise XOR with negative numbers."""
    assert bitwise_utils.bitwise_xor(-1, 5) == -6  # All 1s ^ 5
    assert bitwise_utils.bitwise_xor(-8, 3) == -5  # two's complement arithmetic
    assert bitwise_utils.bitwise_xor(-5, -3) == 6


# --------------------------
# Bitwise NOT Tests
# --------------------------
def test_bitwise_not_basic_operations(bitwise_utils: Bitwise) -> None:
    """Test basic bitwise NOT operations."""
    assert bitwise_utils.bitwise_not(3) == -4
    assert bitwise_utils.bitwise_not(0b1010) == -11
    
    assert bitwise_utils.bitwise_not(0) == -1  # ~0 = -1
    assert bitwise_utils.bitwise_not(1) == -2  # ~1 = -2
    assert bitwise_utils.bitwise_not(255) == -256  # ~255 = -256


def test_bitwise_not_edge_cases(bitwise_utils: Bitwise) -> None:
    """Test edge cases for bitwise NOT."""
    assert bitwise_utils.bitwise_not(0) == -1
    
    assert bitwise_utils.bitwise_not(1) == -2
    assert bitwise_utils.bitwise_not(2) == -3
    assert bitwise_utils.bitwise_not(4) == -5
    assert bitwise_utils.bitwise_not(8) == -9
    
    assert bitwise_utils.bitwise_not(1024) == -1025
    assert bitwise_utils.bitwise_not(2**31 - 1) == -2**31
    
    # double negation should return original - 1
    # ~(~a) = a for unsigned, but in Python with two's complement: ~(~a) = a
    for num in [0, 1, 5, 42, 255, 1024]:
        assert bitwise_utils.bitwise_not(bitwise_utils.bitwise_not(num)) == num


def test_bitwise_not_negative_numbers(bitwise_utils: Bitwise) -> None:
    """Test bitwise NOT with negative numbers."""
    assert bitwise_utils.bitwise_not(-1) == 0  # ~(-1) = 0
    assert bitwise_utils.bitwise_not(-2) == 1  # ~(-2) = 1
    assert bitwise_utils.bitwise_not(-5) == 4  # ~(-5) = 4
    assert bitwise_utils.bitwise_not(-256) == 255  # ~(-256) = 255


# --------------------------
# Type Validation Tests
# --------------------------
@pytest.mark.parametrize("a,b", [
    ("5", 3),
    (5, "3"),
    (5.5, 3),
    (5, 3.5),
    (None, 3),
    (5, None),
])
def test_type_validation_bitwise_and(bitwise_utils: Bitwise, a: type[Any], b: type[Any]) -> None:
    """Test type validation for bitwise_and method."""
    with pytest.raises(TypeError):
        bitwise_utils.bitwise_and(a, b)


@pytest.mark.parametrize("a,b", [
    ("5", 3),
    (5, "3"),
    (5.5, 3),
    ([5], 3),
])
def test_type_validation_bitwise_or(bitwise_utils: Bitwise, a: type[Any], b: type[Any]) -> None:
    """Test type validation for bitwise_or method."""
    with pytest.raises(TypeError):
        bitwise_utils.bitwise_or(a, b)


@pytest.mark.parametrize("a,b", [
    ("5", 3),
    (5, "3"),
    (5.5, 3),
    ({'a': 5}, 3),
])
def test_type_validation_bitwise_xor(bitwise_utils: Bitwise, a: type[Any], b: type[Any]) -> None:
    """Test type validation for bitwise_xor method."""
    with pytest.raises(TypeError):
        bitwise_utils.bitwise_xor(a, b)


@pytest.mark.parametrize("a", [
    "5",
    5.5,
    None,
    [5],
    {'a': 5},
])
def test_type_validation_bitwise_not(bitwise_utils: Bitwise, a: type[Any]) -> None:
    """Test type validation for bitwise_not method."""
    with pytest.raises(TypeError):
        bitwise_utils.bitwise_not(a)


# --------------------------
# Bitwise Properties Tests
# --------------------------
def test_bitwise_properties(bitwise_utils: Bitwise) -> None:
    """Test mathematical properties of bitwise operations."""
    a, b, c = 42, 73, 156
    
    # and properties
    assert bitwise_utils.bitwise_and(a, b) == bitwise_utils.bitwise_and(b, a)
    
    # associative: (a & b) & c = a & (b & c) # noqa: ERA001 - found commented-out code
    left = bitwise_utils.bitwise_and(bitwise_utils.bitwise_and(a, b), c)
    right = bitwise_utils.bitwise_and(a, bitwise_utils.bitwise_and(b, c))
    assert left == right
    
    # identity: a & 0 = 0, a & -1 = a
    assert bitwise_utils.bitwise_and(a, 0) == 0
    assert bitwise_utils.bitwise_and(a, -1) == a
    
    # or properties
    assert bitwise_utils.bitwise_or(a, b) == bitwise_utils.bitwise_or(b, a)
    
    # associative: (a | b) | c = a | (b | c) # noqa: ERA001 - found commented-out code
    left = bitwise_utils.bitwise_or(bitwise_utils.bitwise_or(a, b), c)
    right = bitwise_utils.bitwise_or(a, bitwise_utils.bitwise_or(b, c))
    assert left == right
    
    # identity: a | 0 = a, a | -1 = -1
    assert bitwise_utils.bitwise_or(a, 0) == a
    assert bitwise_utils.bitwise_or(a, -1) == -1
    
    # xor properties
    assert bitwise_utils.bitwise_xor(a, b) == bitwise_utils.bitwise_xor(b, a)
    
    # associative: (a ^ b) ^ c = a ^ (b ^ c) # noqa: ERA001 - found commented-out code
    left = bitwise_utils.bitwise_xor(bitwise_utils.bitwise_xor(a, b), c)
    right = bitwise_utils.bitwise_xor(a, bitwise_utils.bitwise_xor(b, c))
    assert left == right
    
    # identity: a ^ 0 = a, a ^ a = 0 # noqa: ERA001 - found commented-out code
    assert bitwise_utils.bitwise_xor(a, 0) == a
    assert bitwise_utils.bitwise_xor(a, a) == 0


def test_de_morgan_laws(bitwise_utils: Bitwise) -> None:
    """Test De Morgan's laws for bitwise operations."""
    a, b = 42, 73
    
    # ~(a & b) = ~a | ~b
    left = bitwise_utils.bitwise_not(bitwise_utils.bitwise_and(a, b))
    right = bitwise_utils.bitwise_or(bitwise_utils.bitwise_not(a), bitwise_utils.bitwise_not(b))
    assert left == right
    
    # ~(a | b) = ~a & ~b
    left = bitwise_utils.bitwise_not(bitwise_utils.bitwise_or(a, b))
    right = bitwise_utils.bitwise_and(bitwise_utils.bitwise_not(a), bitwise_utils.bitwise_not(b))
    assert left == right


# --------------------------
# Other Tests
# --------------------------
def test_docstring_examples() -> None:
    """Test all examples provided in the docstrings."""
    utils = Bitwise()
    assert utils.bitwise_and(3, 5) == 1
    assert utils.bitwise_or(3, 5) == 7
    
    assert Bitwise().bitwise_and(0b1100, 0b1010) == 0b1000
    assert Bitwise().bitwise_and(3, 5) == 1
    
    assert Bitwise().bitwise_or(0b1100, 0b1010) == 0b1110
    assert Bitwise().bitwise_or(3, 5) == 7
    
    assert Bitwise().bitwise_xor(0b1100, 0b1010) == 0b0110
    assert Bitwise().bitwise_xor(3, 5) == 6
    
    assert Bitwise().bitwise_not(0b1010) == -11
    assert Bitwise().bitwise_not(3) == -4


def test_large_numbers(bitwise_utils: Bitwise) -> None:
    """Test operations with very large numbers."""
    large_a = 2**63 - 1  # maximum positive value for 64-bit signed integer
    large_b = 2**62
    
    result_and = bitwise_utils.bitwise_and(large_a, large_b)
    result_or = bitwise_utils.bitwise_or(large_a, large_b)
    result_xor = bitwise_utils.bitwise_xor(large_a, large_b)
    result_not = bitwise_utils.bitwise_not(large_a)
    
    assert result_and == large_b  # large_a has all bits set including large_b
    assert result_or == large_a   # large_a already has all bits that large_b has
    assert result_xor == large_a - large_b  # XOR of overlapping bits
    assert result_not == -large_a - 1  # Two's complement inversion


def test_multiple_operations_chain(bitwise_utils: Bitwise) -> None:
    """Test chaining multiple operations together."""
    a, b, c = 85, 170, 255  # 0b01010101, 0b10101010, 0b11111111
    
    # complex operation: ((a & b) | c) ^ (~a)
    step1 = bitwise_utils.bitwise_and(a, b)       # 0
    step2 = bitwise_utils.bitwise_or(step1, c)    # 255
    step3 = bitwise_utils.bitwise_not(a)          # -86
    result = bitwise_utils.bitwise_xor(step2, step3)
    
    # verify it's computed correctly
    expected = ((a & b) | c) ^ (~a)
    assert result == expected