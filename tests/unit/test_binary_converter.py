import unittest
from unittest import TestCase

from stpstone.analytics.arithmetic.binary_converter import BinaryConverter


class TestBinaryConverter(TestCase):
    """Test cases for BinaryConverter class."""

    def setUp(self):
        """Set up test fixture."""
        self.converter = BinaryConverter()

    def test_binary_to_decimal_normal(self):
        """Test binary to decimal conversion with normal inputs."""
        self.assertEqual(self.converter.binary_to_decimal('0'), 0)
        self.assertEqual(self.converter.binary_to_decimal('1'), 1)
        self.assertEqual(self.converter.binary_to_decimal('1010'), 10)
        self.assertEqual(self.converter.binary_to_decimal('11111111'), 255)

    def test_binary_to_decimal_edge_cases(self):
        """Test binary to decimal with edge cases."""
        # large binary number
        self.assertEqual(self.converter.binary_to_decimal('1111111111111111'), 65535)
        # binary with leading zeros
        self.assertEqual(self.converter.binary_to_decimal('0001010'), 10)

    def test_binary_to_decimal_type_validation(self):
        """Test binary to decimal type validation."""
        with self.assertRaises(TypeError):
            self.converter.binary_to_decimal(1010)  # not a string
        with self.assertRaises(TypeError):
            self.converter.binary_to_decimal(None)

    def test_binary_to_decimal_error_conditions(self):
        """Test binary to decimal with invalid binary strings."""
        with self.assertRaises(ValueError):
            self.converter.binary_to_decimal('')  # empty string
        with self.assertRaises(ValueError):
            self.converter.binary_to_decimal('10102')  # invalid binary digit
        with self.assertRaises(ValueError):
            self.converter.binary_to_decimal('ABC')  # non-binary characters

    def test_decimal_to_binary_normal(self):
        """Test decimal to binary conversion with normal inputs."""
        self.assertEqual(self.converter.decimal_to_binary(0), '0')
        self.assertEqual(self.converter.decimal_to_binary(1), '1')
        self.assertEqual(self.converter.decimal_to_binary(10), '1010')
        self.assertEqual(self.converter.decimal_to_binary(255), '11111111')

    def test_decimal_to_binary_edge_cases(self):
        """Test decimal to binary with edge cases."""
        # large decimal number
        self.assertEqual(self.converter.decimal_to_binary(65535), '1111111111111111')
        # minimum value
        self.assertEqual(self.converter.decimal_to_binary(0), '0')

    def test_decimal_to_binary_type_validation(self):
        """Test decimal to binary type validation."""
        with self.assertRaises(TypeError):
            self.converter.decimal_to_binary('10')  # not an integer
        with self.assertRaises(TypeError):
            self.converter.decimal_to_binary(10.0)  # float instead of int

    # Decimal to Hexadecimal Tests
    def test_decimal_to_hexadecimal_normal(self):
        """Test decimal to hexadecimal conversion with normal inputs."""
        self.assertEqual(self.converter.decimal_to_hexadecimal(0), '0')
        self.assertEqual(self.converter.decimal_to_hexadecimal(10), 'A')
        self.assertEqual(self.converter.decimal_to_hexadecimal(255), 'FF')
        self.assertEqual(self.converter.decimal_to_hexadecimal(4095), 'FFF')

    def test_decimal_to_hexadecimal_edge_cases(self):
        """Test decimal to hexadecimal with edge cases."""
        # Large decimal number
        self.assertEqual(self.converter.decimal_to_hexadecimal(65535), 'FFFF')
        # Minimum value
        self.assertEqual(self.converter.decimal_to_hexadecimal(0), '0')

    def test_decimal_to_hexadecimal_type_validation(self):
        """Test decimal to hexadecimal type validation."""
        with self.assertRaises(TypeError):
            self.converter.decimal_to_hexadecimal('FF')  # not an integer
        with self.assertRaises(TypeError):
            self.converter.decimal_to_hexadecimal([255])  # List instead of int

    def test_hexadecimal_to_decimal_normal(self):
        """Test hexadecimal to decimal conversion with normal inputs."""
        self.assertEqual(self.converter.hexadecimal_to_decimal('0'), 0)
        self.assertEqual(self.converter.hexadecimal_to_decimal('A'), 10)
        self.assertEqual(self.converter.hexadecimal_to_decimal('FF'), 255)
        self.assertEqual(self.converter.hexadecimal_to_decimal('FFF'), 4095)
        # test case insensitivity
        self.assertEqual(self.converter.hexadecimal_to_decimal('ff'), 255)
        self.assertEqual(self.converter.hexadecimal_to_decimal('aBc'), 2748)

    def test_hexadecimal_to_decimal_edge_cases(self):
        """Test hexadecimal to decimal with edge cases."""
        # large hexadecimal number
        self.assertEqual(self.converter.hexadecimal_to_decimal('FFFF'), 65535)
        # with leading zeros
        self.assertEqual(self.converter.hexadecimal_to_decimal('000A'), 10)

    def test_hexadecimal_to_decimal_type_validation(self):
        """Test hexadecimal to decimal type validation."""
        with self.assertRaises(TypeError):
            self.converter.hexadecimal_to_decimal(0xFF)  # not a string
        with self.assertRaises(TypeError):
            self.converter.hexadecimal_to_decimal(255)  # integer instead of string

    def test_hexadecimal_to_decimal_error_conditions(self):
        """Test hexadecimal to decimal with invalid hex strings."""
        with self.assertRaises(ValueError):
            self.converter.hexadecimal_to_decimal('')  # empty string
        with self.assertRaises(ValueError):
            self.converter.hexadecimal_to_decimal('G')  # invalid hex character
        with self.assertRaises(ValueError):
            self.converter.hexadecimal_to_decimal('FFG')  # invalid character in string

    # Integration Tests
    def test_conversion_chain(self):
        """Test chaining conversions together."""
        # Binary -> Decimal -> Hexadecimal
        decimal = self.converter.binary_to_decimal('11111111')
        hex_val = self.converter.decimal_to_hexadecimal(decimal)
        self.assertEqual(hex_val, 'FF')

        # Hexadecimal -> Decimal -> Binary
        decimal = self.converter.hexadecimal_to_decimal('FF')
        binary = self.converter.decimal_to_binary(decimal)
        self.assertEqual(binary, '11111111')

if __name__ == '__main__':
    unittest.main()