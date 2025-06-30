"""Example usage of BinaryConverter class.

This demonstrates various number system conversions:
- Binary to Decimal
- Decimal to Binary
- Decimal to Hexadecimal
- Hexadecimal to Decimal
"""

from stpstone.analytics.arithmetics.binary_converter import BinaryConverter


converter = BinaryConverter()
print(f"Binary to Decimal (1010) => {converter.binary_to_decimal('1010')}")  # output: 10
print(f"Decimal to Binary (10) => {converter.decimal_to_binary(10)}")  # output: 1010
print(f"Decimal to Hexadecimal (255) => {converter.decimal_to_hexadecimal(255)}")  # output: FF
print(f"Hexadecimal to Decimal (FF) => {converter.hexadecimal_to_decimal('FF')}")  # output: 255
