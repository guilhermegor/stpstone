"""Example usage of logic gate implementations.

Demonstrates basic logic gates and their truth table outputs:
- NAND Gate: Outputs False only when all inputs are True
- NOR Gate: Outputs True only when all inputs are False
- XOR Gate: Outputs True when inputs are different

Shows both boolean output and string representation for each gate.
"""

from stpstone.analytics.arithmetic.logic_gate import NANDGate, NORGate, XORGate


# NAND Gate examples
print("NAND Gate Examples:")
nand_gate1 = NANDGate(False, False)
print(f"  (False, False): {bool(nand_gate1)}")  # True
print(f"  Representation: {repr(nand_gate1)}")  # Shows full state

nand_gate2 = NANDGate(True, True)
print(f"  (True, True): {bool(nand_gate2)}")  # False

# NOR Gate examples
print("\nNOR Gate Examples:")
nor_gate = NORGate(True, False)
print(f"  (True, False): {bool(nor_gate)}")  # False
print(f"  Representation: {repr(nor_gate)}")

# XOR Gate examples
print("\nXOR Gate Examples:")
xor_gate = XORGate(True, False)
print(f"  (True, False): {bool(xor_gate)}")  # True
print(f"  Representation: {repr(xor_gate)}")
