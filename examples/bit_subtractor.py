"""Example usage of bit subtractor classes.

Demonstrates digital subtractor circuits including:
- Half Subtractor: Subtracts two single bits
- Full Subtractor: Subtracts two bits with borrow-in

Shows the difference and borrow outputs for each operation.
"""

from stpstone.analytics.arithmetic.bit_subtractor import FullSubtractor, HalfSubtractor


hs = HalfSubtractor(1, 1)
print(f"Half Subtractor (1, 1) => Difference: {hs.get_difference()}, Borrow: {hs.get_borrow()}")

fs = FullSubtractor(1, 0, 1)
print(f"Full Subtractor (1, 0, 1) => Difference: {fs.get_difference()}, "
      + f"Borrow Out: {fs.get_borrow_out()}")
