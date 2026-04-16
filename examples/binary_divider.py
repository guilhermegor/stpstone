"""Example usage of BinaryDivider class.

Demonstrates binary division operation showing quotient and remainder.
"""

from stpstone.analytics.arithmetic.binary_divider import BinaryDivider


divider = BinaryDivider(0b1100, 0b0010)
quotient, remainder = divider.divide()
print(f"Binary Divider (1100 / 10) => Quotient: {bin(quotient)}, Remainder: {bin(remainder)}")
