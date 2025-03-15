# pypi.org libs
import math

# local libs
from stpstone.transformations.validation.metaclass_type_checker import \
    TypeChecker


class Fraction(metaclass=TypeChecker):
    def __init__(self, numerator: int, denominator: int) -> None:
        if denominator == 0:
            raise ValueError("Denominator cannot be zero.")
        gcd = math.gcd(numerator, denominator)
        self.numerator: int = numerator // gcd
        self.denominator: int = denominator // gcd
        if self.denominator < 0:
            self.numerator = -self.numerator
            self.denominator = -self.denominator

    def get_num(self) -> int:
        return self.numerator

    def get_den(self) -> int:
        return self.denominator

    def __add__(self, fraction_new_instance: "Fraction") -> "Fraction":
        new_numerator = (
            self.numerator * fraction_new_instance.denominator
            + fraction_new_instance.numerator * self.denominator
        )
        new_denominator = self.denominator * fraction_new_instance.denominator
        return Fraction(new_numerator, new_denominator)

    def __radd__(self, fraction_new_instance: int) -> "Fraction":
        """
        Implements addition of a Fraction with an int.
        Args:
            fraction_new_instance (int): The int to add to the Fraction.
        Returns:
            Fraction: The sum of the two fractions.
        """
        if isinstance(fraction_new_instance, int):
            return Fraction(
                fraction_new_instance * self.denominator + self.numerator,
                self.denominator,
            )
        return NotImplemented

    def __iadd__(self, fraction_new_instance: "Fraction") -> "Fraction":
        """
        Implements in-place addition of another Fraction to this Fraction.
        Args:
            fraction_new_instance (Fraction): The Fraction instance to add.
        Returns:
            Fraction: The result of the addition, reduced to its simplest form.
        """
        if isinstance(fraction_new_instance, Fraction):
            self.numerator = (
                self.numerator * fraction_new_instance.denominator
                + fraction_new_instance.numerator * self.denominator
            )
            self.denominator *= fraction_new_instance.denominator
            gcd = math.gcd(self.numerator, self.denominator)
            self.numerator //= gcd
            self.denominator //= gcd
            return self
        return NotImplemented

    def __sub__(self, fraction_new_instance: "Fraction") -> "Fraction":
        new_numerator = (
            self.numerator * fraction_new_instance.denominator
            - fraction_new_instance.numerator * self.denominator
        )
        new_denominator = self.denominator * fraction_new_instance.denominator
        return Fraction(new_numerator, new_denominator)

    def __mul__(self, fraction_new_instance: "Fraction") -> "Fraction":
        new_numerator = self.numerator * fraction_new_instance.numerator
        new_denominator = self.denominator * fraction_new_instance.denominator
        return Fraction(new_numerator, new_denominator)

    def __truediv__(self, fraction_new_instance: "Fraction") -> "Fraction":
        """
        Implements true division of two Fraction instances.
        Args:
            fraction_new_instance (Fraction): The Fraction to divide by.
        Returns:
            Fraction: The result of the division as a new Fraction instance.
        Raises:
            ValueError: If the numerator of the fraction_new_instance is zero,
                        indicating division by zero is not allowed.
        """
        if fraction_new_instance.numerator == 0:
            raise ValueError("Cannot divide by zero.")
        new_numerator = self.numerator * fraction_new_instance.denominator
        new_denominator = self.denominator * fraction_new_instance.numerator
        return Fraction(new_numerator, new_denominator)

    def __gt__(self, fraction_new_instance: "Fraction") -> bool:
        return (
            self.numerator * fraction_new_instance.denominator
            > fraction_new_instance.numerator * self.denominator
        )

    def __ge__(self, fraction_new_instance: "Fraction") -> bool:
        return (
            self.numerator * fraction_new_instance.denominator
            >= fraction_new_instance.numerator * self.denominator
        )

    def __lt__(self, fraction_new_instance: "Fraction") -> bool:
        return (
            self.numerator * fraction_new_instance.denominator
            < fraction_new_instance.numerator * self.denominator
        )

    def __le__(self, fraction_new_instance: "Fraction") -> bool:
        return (
            self.numerator * fraction_new_instance.denominator
            <= fraction_new_instance.numerator * self.denominator
        )

    def __ne__(self, fraction_new_instance: "Fraction") -> bool:
        """
        Returns True if the two fractions are not equal, False otherwise.
        Args:
            fraction_new_instance (Fraction): The Fraction instance to compare with.
        Returns:
            bool: True if the two fractions are not equal, False otherwise.
        """
        return not (self == fraction_new_instance)

    def __eq__(self, fraction_new_instance: "Fraction") -> bool:
        return (
            self.numerator * fraction_new_instance.denominator
            == fraction_new_instance.numerator * self.denominator
        )

    def __repr__(self) -> str:
        """
        Returns an unambiguous string representation of the Fraction instance that is
        useful for debugging.
        Args:
            None
        Returns:
            str: A string representation of the Fraction instance.
        """
        return f"Fraction({self.numerator}, {self.denominator})"

    def __str__(self) -> str:
        """
        Returns a user-friendly string representation of the fraction.
        Args:
            None
        Returns:
            str: A string representation of the fraction.
        """
        return f"{self.numerator}/{self.denominator}"


# example usage of the Fraction class
if __name__ == "__main__":
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
