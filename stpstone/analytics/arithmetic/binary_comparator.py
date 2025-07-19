"""Module containing the BinaryComparator class for comparing two binary inputs.

This module provides functionality to compare two binary numbers and determine
their relationship (less than, greater than, or equal to).
"""
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class BinaryComparator(metaclass=TypeChecker):
    """A class for comparing two binary inputs and returning a comparison result."""

    def __init__(self, a: int, b: int) -> None:
        """Initialize the Binary Comparator with two binary inputs.

        Parameters
        ----------
        a : int
            Binary input 1
        b : int
            Binary input 2
        """
        self.a = a
        self.b = b

    def compare(self) -> str:
        """Compare two binary inputs and return a comparison result as a string.

        This method compares the binary inputs `a` and `b` and returns a string
        indicating their relationship. The possible return values are:
        - "A is less than B" if `a` is less than `b`
        - "A is greater than B" if `a` is greater than `b`
        - "A is equal to B" if `a` is equal to `b`

        Returns
        -------
        str
            A string indicating the comparison result
        """
        if self.a < self.b:
            return "A is less than B"
        elif self.a > self.b:
            return "A is greater than B"
        else:
            return "A is equal to B"