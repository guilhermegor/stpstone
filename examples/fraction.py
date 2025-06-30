"""Example usage of Fraction class for exact arithmetic operations.

Demonstrates fraction creation and various arithmetic operations including:
- Basic arithmetic: addition, subtraction, multiplication, division
- In-place operations
- Mixed operations with integers
- Relational comparisons

Shows proper fraction handling with automatic simplification.
"""

from stpstone.analytics.arithmetics.fraction import Fraction


fraction1 = Fraction(1, 2)  # represents 1/2
fraction2 = Fraction(3, 4)  # represents 3/4

print(f"Fraction 1: {fraction1}")  # output: 1/2
print(f"Fraction 2: {fraction2}")  # output: 3/4

# arithmetic operations
print(f"Addition: {fraction1 + fraction2}")  # output: 5/4
print(f"Subtraction: {fraction1 - fraction2}")  # output: -1/4
print(f"Multiplication: {fraction1 * fraction2}")  # output: 3/8
print(f"Division: {fraction1 / fraction2}")  # output: 2/3

# in-place addition
fraction1 += fraction2
print(f"In-place Addition: {fraction1}")  # output: 5/4

# right Addition
print(f"Right Addition with int: {5 + fraction1}")  # output: 25/4

# relational operations
print(f"Is Fraction 1 > Fraction 2? {fraction1 > fraction2}")  # output: True
print(f"Is Fraction 1 < Fraction 2? {fraction1 < fraction2}")  # output: False
