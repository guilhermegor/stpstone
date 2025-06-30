"""Example usage of BinaryMultiplier class.

Demonstrates binary multiplication operation between two binary numbers.
"""

from stpstone.analytics.arithmetics.binary_multiplier import BinaryMultiplier


multiplier = BinaryMultiplier(0b1100, 0b1010)
product = multiplier.multiply()
print(f"Binary Multiplier (1100 × 1010) => Product: {bin(product)}")
