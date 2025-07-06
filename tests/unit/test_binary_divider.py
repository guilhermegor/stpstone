import unittest

from stpstone.analytics.arithmetic.binary_divider import BinaryDivider


class TestBinaryDivider(unittest.TestCase):
    """Test cases for BinaryDivider class."""

    def test_divide_positive_numbers(self):
        """Test division of positive numbers."""
        # test exact division
        divider = BinaryDivider(12, 2)
        quotient, remainder = divider.divide()
        self.assertEqual(quotient, 6)
        self.assertEqual(remainder, 0)

        # test division with remainder
        divider = BinaryDivider(15, 4)
        quotient, remainder = divider.divide()
        self.assertEqual(quotient, 3)
        self.assertEqual(remainder, 3)

    def test_divide_large_numbers(self):
        """Test division with larger numbers."""
        # test with 8-bit boundary
        divider = BinaryDivider(255, 16)
        quotient, remainder = divider.divide()
        self.assertEqual(quotient, 15)
        self.assertEqual(remainder, 15)

        # test with numbers beyond 8-bit
        divider = BinaryDivider(1024, 256)
        quotient, remainder = divider.divide()
        self.assertEqual(quotient, 4)
        self.assertEqual(remainder, 0)

    def test_divide_by_one(self):
        """Test division by one."""
        divider = BinaryDivider(42, 1)
        quotient, remainder = divider.divide()
        self.assertEqual(quotient, 42)
        self.assertEqual(remainder, 0)

    def test_divide_zero_dividend(self):
        """Test division with zero dividend."""
        divider = BinaryDivider(0, 5)
        quotient, remainder = divider.divide()
        self.assertEqual(quotient, 0)
        self.assertEqual(remainder, 0)

    def test_divide_same_numbers(self):
        """Test division when dividend equals divisor."""
        divider = BinaryDivider(7, 7)
        quotient, remainder = divider.divide()
        self.assertEqual(quotient, 1)
        self.assertEqual(remainder, 0)

    def test_divide_dividend_smaller_than_divisor(self):
        """Test when dividend is smaller than divisor."""
        divider = BinaryDivider(3, 5)
        quotient, remainder = divider.divide()
        self.assertEqual(quotient, 0)
        self.assertEqual(remainder, 3)

    def test_divide_negative_dividend(self):
        """Test initialization with negative dividend."""
        with self.assertRaises(ValueError):
            BinaryDivider(-10, 5)

    def test_divide_negative_divisor(self):
        """Test initialization with negative divisor."""
        with self.assertRaises(ValueError):
            BinaryDivider(10, -5)

    def test_divide_by_zero(self):
        """Test division by zero."""
        with self.assertRaises(ValueError):
            BinaryDivider(10, 0)

    def test_type_checking(self):
        """Test type checking of inputs."""
        with self.assertRaises(TypeError):
            BinaryDivider("10", 5)
        with self.assertRaises(TypeError):
            BinaryDivider(10, "5")

    def test_property_access(self):
        """Test property access after division."""
        divider = BinaryDivider(10, 3)
        self.assertEqual(divider.dividend, 10)
        self.assertEqual(divider.divisor, 3)


if __name__ == '__main__':
    unittest.main()