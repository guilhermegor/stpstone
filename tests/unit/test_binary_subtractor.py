import unittest
from unittest import TestCase

from stpstone.analytics.arithmetic.binary_subtractor import BinarySubtractor


class TestBinarySubtractor(TestCase):
    """Test cases for BinarySubtractor class."""

    # Normal Operations
    def test_subtraction_normal_cases(self):
        """Test binary subtraction with normal input cases."""
        # Test case 1: Simple subtraction (no borrow)
        sub = BinarySubtractor("101", "010")  # 5 - 2
        self.assertEqual(sub.subtract(), "011")  # 3

        # Test case 2: Subtraction with single borrow
        sub = BinarySubtractor("1101", "0110")  # 13 - 6
        self.assertEqual(sub.subtract(), "0111")  # 7

        # Test case 3: Subtraction with multiple borrows
        sub = BinarySubtractor("1000", "0001")  # 8 - 1
        self.assertEqual(sub.subtract(), "0111")  # 7

        # Test case 4: Equal numbers
        sub = BinarySubtractor("1111", "1111")  # 15 - 15
        self.assertEqual(sub.subtract(), "0000")  # 0

    def test_different_length_inputs(self):
        """Test subtraction with inputs of different lengths."""
        # Minuend longer than subtrahend
        sub = BinarySubtractor("10101", "110")  # 21 - 6
        self.assertEqual(sub.subtract(), "01111")  # 15

        # Subtrahend longer than minuend
        sub = BinarySubtractor("110", "10101")  # 6 - 21
        self.assertEqual(sub.subtract(), "10001")  # -15 (two's complement)

    # Edge Cases
    def test_edge_cases(self):
        """Test binary subtraction with edge cases."""
        # Subtraction from zero
        sub = BinarySubtractor("0000", "0101")  # 0 - 5
        self.assertEqual(sub.subtract(), "1011")  # -5 (two's complement)

        # Subtraction of zero
        sub = BinarySubtractor("1010", "0000")  # 10 - 0
        self.assertEqual(sub.subtract(), "1010")  # 10

        # Single bit subtraction
        sub = BinarySubtractor("1", "0")  # 1 - 0
        self.assertEqual(sub.subtract(), "1")  # 1

        sub = BinarySubtractor("1", "1")  # 1 - 1
        self.assertEqual(sub.subtract(), "0")  # 0

    # Error Conditions
    def test_empty_strings(self):
        """Test with empty input strings."""
        with self.assertRaises(ValueError):
            BinarySubtractor("", "101")  # Empty minuend

        with self.assertRaises(ValueError):
            BinarySubtractor("101", "")  # Empty subtrahend

        with self.assertRaises(ValueError):
            BinarySubtractor("", "")  # Both empty

    def test_invalid_binary_strings(self):
        """Test with invalid binary strings."""
        with self.assertRaises(ValueError):
            BinarySubtractor("1021", "0101")  # Invalid digit in minuend

        with self.assertRaises(ValueError):
            BinarySubtractor("1010", "01x1")  # Invalid digit in subtrahend

        with self.assertRaises(ValueError):
            BinarySubtractor("abc", "101")  # Non-binary characters

    # Type Validation
    def test_type_validation(self):
        """Test type validation of inputs."""
        with self.assertRaises(TypeError):
            BinarySubtractor(101, "0101")  # Minuend not string

        with self.assertRaises(TypeError):
            BinarySubtractor("1010", 101)  # Subtrahend not string

        with self.assertRaises(TypeError):
            BinarySubtractor(True, "1010")  # Boolean minuend

        with self.assertRaises(TypeError):
            BinarySubtractor("1010", None)  # None subtrahend

    # Borrow Propagation Tests
    def test_borrow_propagation(self):
        """Test proper borrow propagation in subtraction."""
        # Case requiring borrow across multiple bits
        sub = BinarySubtractor("10000", "00001")  # 16 - 1
        self.assertEqual(sub.subtract(), "01111")  # 15

        # Case with borrow from higher bits
        sub = BinarySubtractor("1010101", "0101010")  # 85 - 42
        self.assertEqual(sub.subtract(), "0101011")  # 43

    # Result Storage Test
    def test_result_storage(self):
        """Test that result is properly stored in the instance."""
        sub = BinarySubtractor("1101", "0110")  # 13 - 6
        result = sub.subtract()
        self.assertEqual(sub.result, "0111")  # Should match the returned result
        self.assertEqual(result, sub.result)  # Return value should match stored result

    # Negative Results (Two's Complement)
    def test_negative_results(self):
        """Test cases that produce negative results."""
        # Small number minus large number
        sub = BinarySubtractor("0101", "1010")  # 5 - 10
        self.assertEqual(sub.subtract(), "1011")  # -5 (two's complement)

        # Single bit negative result
        sub = BinarySubtractor("0", "1")  # 0 - 1
        self.assertEqual(sub.subtract(), "1")  # -1 (two's complement)

if __name__ == '__main__':
    unittest.main()