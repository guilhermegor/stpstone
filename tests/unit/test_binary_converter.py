"""Unit tests for BinaryConverter class.

Tests the binary, decimal, and hexadecimal conversion functionality.
"""

import pytest

from stpstone.analytics.arithmetic.binary_converter import BinaryConverter


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def converter() -> BinaryConverter:
    """Fixture providing a BinaryConverter instance."""
    return BinaryConverter()


# --------------------------
# Tests
# --------------------------
class TestBinaryToDecimal:
    """Tests for binary_to_decimal method."""

    def test_normal_inputs(self, converter: BinaryConverter) -> None:
        """Test binary to decimal conversion with normal inputs."""
        assert converter.binary_to_decimal('0') == 0
        assert converter.binary_to_decimal('1') == 1
        assert converter.binary_to_decimal('1010') == 10
        assert converter.binary_to_decimal('11111111') == 255

    def test_edge_cases(self, converter: BinaryConverter) -> None:
        """Test binary to decimal with edge cases."""
        # large binary number
        assert converter.binary_to_decimal('1111111111111111') == 65535
        # binary with leading zeros
        assert converter.binary_to_decimal('0001010') == 10

    def test_type_validation(self, converter: BinaryConverter) -> None:
        """Test binary to decimal type validation."""
        with pytest.raises(TypeError):
            converter.binary_to_decimal(1010)  # not a string
        with pytest.raises(TypeError):
            converter.binary_to_decimal(None)

    def test_error_conditions(self, converter: BinaryConverter) -> None:
        """Test binary to decimal with invalid binary strings."""
        with pytest.raises(ValueError):
            converter.binary_to_decimal('')  # empty string
        with pytest.raises(ValueError):
            converter.binary_to_decimal('10102')  # invalid binary digit
        with pytest.raises(ValueError):
            converter.binary_to_decimal('ABC')  # non-binary characters


class TestDecimalToBinary:
    """Tests for decimal_to_binary method."""

    def test_normal_inputs(self, converter: BinaryConverter) -> None:
        """Test decimal to binary conversion with normal inputs."""
        assert converter.decimal_to_binary(0) == '0'
        assert converter.decimal_to_binary(1) == '1'
        assert converter.decimal_to_binary(10) == '1010'
        assert converter.decimal_to_binary(255) == '11111111'

    def test_edge_cases(self, converter: BinaryConverter) -> None:
        """Test decimal to binary with edge cases."""
        # large decimal number
        assert converter.decimal_to_binary(65535) == '1111111111111111'
        # minimum value
        assert converter.decimal_to_binary(0) == '0'

    def test_type_validation(self, converter: BinaryConverter) -> None:
        """Test decimal to binary type validation."""
        with pytest.raises(TypeError):
            converter.decimal_to_binary('10')  # not an integer
        with pytest.raises(TypeError):
            converter.decimal_to_binary(10.0)  # float instead of int


class TestDecimalToHexadecimal:
    """Tests for decimal_to_hexadecimal method."""

    def test_normal_inputs(self, converter: BinaryConverter) -> None:
        """Test decimal to hexadecimal conversion with normal inputs."""
        assert converter.decimal_to_hexadecimal(0) == '0'
        assert converter.decimal_to_hexadecimal(10) == 'A'
        assert converter.decimal_to_hexadecimal(255) == 'FF'
        assert converter.decimal_to_hexadecimal(4095) == 'FFF'

    def test_edge_cases(self, converter: BinaryConverter) -> None:
        """Test decimal to hexadecimal with edge cases."""
        # Large decimal number
        assert converter.decimal_to_hexadecimal(65535) == 'FFFF'
        # Minimum value
        assert converter.decimal_to_hexadecimal(0) == '0'

    def test_type_validation(self, converter: BinaryConverter) -> None:
        """Test decimal to hexadecimal type validation."""
        with pytest.raises(TypeError):
            converter.decimal_to_hexadecimal('FF')  # not an integer
        with pytest.raises(TypeError):
            converter.decimal_to_hexadecimal([255])  # List instead of int


class TestHexadecimalToDecimal:
    """Tests for hexadecimal_to_decimal method."""

    def test_normal_inputs(self, converter: BinaryConverter) -> None:
        """Test hexadecimal to decimal conversion with normal inputs."""
        assert converter.hexadecimal_to_decimal('0') == 0
        assert converter.hexadecimal_to_decimal('A') == 10
        assert converter.hexadecimal_to_decimal('FF') == 255
        assert converter.hexadecimal_to_decimal('FFF') == 4095
        # test case insensitivity
        assert converter.hexadecimal_to_decimal('ff') == 255
        assert converter.hexadecimal_to_decimal('aBc') == 2748

    def test_edge_cases(self, converter: BinaryConverter) -> None:
        """Test hexadecimal to decimal with edge cases."""
        # large hexadecimal number
        assert converter.hexadecimal_to_decimal('FFFF') == 65535
        # with leading zeros
        assert converter.hexadecimal_to_decimal('000A') == 10

    def test_type_validation(self, converter: BinaryConverter) -> None:
        """Test hexadecimal to decimal type validation."""
        with pytest.raises(TypeError):
            converter.hexadecimal_to_decimal(0xFF)  # not a string
        with pytest.raises(TypeError):
            converter.hexadecimal_to_decimal(255)  # integer instead of string

    def test_error_conditions(self, converter: BinaryConverter) -> None:
        """Test hexadecimal to decimal with invalid hex strings."""
        with pytest.raises(ValueError):
            converter.hexadecimal_to_decimal('')  # empty string
        with pytest.raises(ValueError):
            converter.hexadecimal_to_decimal('G')  # invalid hex character
        with pytest.raises(ValueError):
            converter.hexadecimal_to_decimal('FFG')  # invalid character in string


class TestIntegration:
    """Integration tests for conversion methods."""

    def test_conversion_chain(self, converter: BinaryConverter) -> None:
        """Test chaining conversions together."""
        # Binary -> Decimal -> Hexadecimal
        decimal = converter.binary_to_decimal('11111111')
        hex_val = converter.decimal_to_hexadecimal(decimal)
        assert hex_val == 'FF'

        # Hexadecimal -> Decimal -> Binary
        decimal = converter.hexadecimal_to_decimal('FF')
        binary = converter.decimal_to_binary(decimal)
        assert binary == '11111111'