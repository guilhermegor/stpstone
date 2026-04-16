"""Example usage of bitwise operations from Utilities class.

Demonstrates fundamental bitwise operations:
- AND: Bitwise AND operation
- OR: Bitwise OR operation
- XOR: Bitwise exclusive OR operation
- NOT: Bitwise complement/inversion

Shows example outputs for each operation on sample inputs.
"""

from stpstone.analytics.arithmetic.bitwise import Utilities


utils = Utilities()
print(f"Bitwise AND (3, 5) => {utils.bitwise_and(3, 5)}")  # output: 1
print(f"Bitwise OR (3, 5) => {utils.bitwise_or(3, 5)}")  # output: 7
print(f"Bitwise XOR (3, 5) => {utils.bitwise_xor(3, 5)}")  # output: 6
print(f"Bitwise NOT (3) => {utils.bitwise_not(3)}")  # output: -4
