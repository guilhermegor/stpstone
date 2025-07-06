import unittest

from stpstone.analytics.arithmetic.binary_multiplier import BinaryMultiplier


class TestBinaryMultiplier(unittest.TestCase):
    """Test cases for BinaryMultiplier class."""

    def test_multiply_positive_numbers(self):
        """Test multiplication of positive numbers."""
        # Test basic multiplication
        multiplier = BinaryMultiplier(3, 5)
        self.assertEqual(multiplier.multiply(), 15)

        # Test with binary literals
        multiplier = BinaryMultiplier(0b1100, 0b1010)  # 12 × 10
        self.assertEqual(multiplier.multiply(), 120)

        # Test with larger numbers
        multiplier = BinaryMultiplier(15, 16)
        self.assertEqual(multiplier.multiply(), 240)

    def test_multiply_with_zero(self):
        """Test multiplication involving zero."""
        # Zero as multiplicand
        multiplier = BinaryMultiplier(0, 5)
        self.assertEqual(multiplier.multiply(), 0)

        # Zero as multiplier
        multiplier = BinaryMultiplier(5, 0)
        self.assertEqual(multiplier.multiply(), 0)

        # Both numbers zero
        multiplier = BinaryMultiplier(0, 0)
        self.assertEqual(multiplier.multiply(), 0)

    def test_multiply_with_one(self):
        """Test multiplication with one."""
        # One as multiplicand
        multiplier = BinaryMultiplier(1, 8)
        self.assertEqual(multiplier.multiply(), 8)

        # One as multiplier
        multiplier = BinaryMultiplier(8, 1)
        self.assertEqual(multiplier.multiply(), 8)

    def test_multiply_negative_numbers(self):
        """Test initialization with negative numbers."""
        with self.assertRaises(ValueError):
            BinaryMultiplier(-3, 5)

        with self.assertRaises(ValueError):
            BinaryMultiplier(3, -5)

        with self.assertRaises(ValueError):
            BinaryMultiplier(-3, -5)

    def test_multiply_max_values(self):
        """Test with maximum 8-bit values."""
        # Test with maximum allowed values
        multiplier = BinaryMultiplier(255, 255)
        self.assertEqual(multiplier.multiply(), 65025)

        # Test that values beyond 8-bit are rejected
        with self.assertRaises(ValueError):
            BinaryMultiplier(256, 1)
        with self.assertRaises(ValueError):
            BinaryMultiplier(1, 256)

    def test_type_validation(self):
        """Test type validation of inputs."""
        with self.assertRaises(TypeError):
            BinaryMultiplier("3", 5)

        with self.assertRaises(TypeError):
            BinaryMultiplier(3, "5")

        with self.assertRaises(TypeError):
            BinaryMultiplier(3.5, 5)

    def test_property_access(self):
        """Test property access after initialization."""
        multiplier = BinaryMultiplier(4, 7)
        self.assertEqual(multiplier.a, 4)
        self.assertEqual(multiplier.b, 7)


if __name__ == '__main__':
    unittest.main()