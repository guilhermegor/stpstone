import unittest
from unittest.mock import Mock, patch

from stpstone.analytics.arithmetic.binary_comparator import BinaryComparator


class TestBinaryComparator(unittest.TestCase):
    """Test cases for the BinaryComparator class."""

    def test_init_with_valid_inputs(self):
        """Test initialization with valid integer inputs."""
        comparator = BinaryComparator(a=5, b=10)
        self.assertEqual(comparator.a, 5)
        self.assertEqual(comparator.b, 10)

    def test_compare_a_less_than_b(self):
        """Test comparison when a is less than b."""
        comparator = BinaryComparator(a=5, b=10)
        result = comparator.compare()
        self.assertEqual(result, "A is less than B")

    def test_compare_a_greater_than_b(self):
        """Test comparison when a is greater than b."""
        comparator = BinaryComparator(a=15, b=10)
        result = comparator.compare()
        self.assertEqual(result, "A is greater than B")

    def test_compare_a_equal_to_b(self):
        """Test comparison when a is equal to b."""
        comparator = BinaryComparator(a=10, b=10)
        result = comparator.compare()
        self.assertEqual(result, "A is equal to B")

    def test_compare_with_zero_values(self):
        """Test comparison with zero values."""
        comparator = BinaryComparator(a=0, b=0)
        result = comparator.compare()
        self.assertEqual(result, "A is equal to B")

    def test_compare_with_negative_values(self):
        """Test comparison with negative values."""
        comparator = BinaryComparator(a=-5, b=5)
        result = comparator.compare()
        self.assertEqual(result, "A is less than B")

    def test_edge_case_large_numbers(self):
        """Test comparison with very large numbers."""
        large_num = 2**64
        comparator = BinaryComparator(a=large_num, b=large_num + 1)
        result = comparator.compare()
        self.assertEqual(result, "A is less than B")

    def test_edge_case_small_numbers(self):
        """Test comparison with very small numbers."""
        small_num = -2**64
        comparator = BinaryComparator(a=small_num, b=small_num - 1)
        result = comparator.compare()
        self.assertEqual(result, "A is greater than B")


if __name__ == '__main__':
    unittest.main()