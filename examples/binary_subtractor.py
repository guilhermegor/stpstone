"""Example usage of BinarySubtractor class.

Demonstrates binary subtraction operation between two binary numbers represented as strings.
Shows the result of subtracting a subtrahend from a minuend in binary format.
"""

from stpstone.analytics.arithmetic.binary_subtractor import BinarySubtractor


minuend = "1011"  # 11 in decimal
subtrahend = "0101"  # 5 in decimal
subtractor = BinarySubtractor(minuend, subtrahend)
result = subtractor.subtract()
print(f"Binary Subtraction: {minuend} - {subtrahend} = {result}")
