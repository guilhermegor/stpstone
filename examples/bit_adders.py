"""Example usage of bit adder classes.

Demonstrates various digital adder circuits:
- Half Adder: Adds two single bits
- Full Adder: Adds two bits with carry-in
- 8-bit Full Adder: Adds two 8-bit numbers

Shows the sum and carry outputs for each adder type.
"""

from stpstone.analytics.arithmetic.bit_adders import EightBitFullAdder, FullAdder, HalfAdder


ha1 = HalfAdder(0, 0)
print(f"Half Adder (0, 0) => Sum: {ha1.get_sum()}, Carry: {ha1.get_carry()}")

ha2 = HalfAdder(0, 1)
print(f"Half Adder (0, 1) => Sum: {ha2.get_sum()}, Carry: {ha2.get_carry()}")

fa1 = FullAdder(1, 1, 0)
print(f"Full Adder (1, 1, 0) => Sum: {fa1.get_sum()}, Carry Out: {fa1.get_carry_out()}")

eight_bit_adder = EightBitFullAdder(0b11001100, 0b10101010)
result, carry = eight_bit_adder.add()
print(f"8-bit Full Adder Result: {bin(result)}, Carry Out: {carry}")
