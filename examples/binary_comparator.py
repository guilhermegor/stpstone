"""Example usage of BinaryComparator class.

This demonstrates how to compare two binary numbers using the BinaryComparator class.
"""

from stpstone.analytics.arithmetic.binary_comparator import BinaryComparator


comparator = BinaryComparator(0b1100, 0b1010)
result = comparator.compare()
print(f"Binary Comparator (1100, 1010) => {result}")  # output: A is greater than B
