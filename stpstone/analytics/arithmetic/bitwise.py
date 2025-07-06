"""Bitwise operation utilities.

This module provides a collection of utility methods for performing basic bitwise operations
on integers. The operations include AND, OR, XOR, and NOT.

Classes
-------
Bitwise
    A utility class providing bitwise operation methods.
"""

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class Bitwise(metaclass=TypeChecker):
    """A utility class providing basic bitwise operations.

    This class implements fundamental bitwise operations that are commonly used in
    digital logic and low-level programming. All methods operate on integer values
    and return integer results.

    Examples
    --------
    >>> utils = Bitwise()
    >>> utils.bitwise_and(3, 5)
    1
    >>> utils.bitwise_or(3, 5)
    7
    """

    def bitwise_and(self, a: int, b: int) -> int:
        """Perform a bitwise AND operation between two integers.

        The bitwise AND operation compares each bit of two numbers and returns 1
        if both bits are 1, otherwise 0.

        Parameters
        ----------
        a : int
            First integer operand
        b : int
            Second integer operand

        Returns
        -------
        int
            Result of the bitwise AND operation

        Examples
        --------
        >>> Bitwise().bitwise_and(0b1100, 0b1010)
        0b1000  # 8 in decimal
        >>> Bitwise().bitwise_and(3, 5)
        1
        """
        return a & b

    def bitwise_or(self, a: int, b: int) -> int:
        """Perform a bitwise OR operation between two integers.

        The bitwise OR operation compares each bit of two numbers and returns 1
        if at least one of the bits is 1, otherwise 0.

        Parameters
        ----------
        a : int
            First integer operand
        b : int
            Second integer operand

        Returns
        -------
        int
            Result of the bitwise OR operation

        Examples
        --------
        >>> Bitwise().bitwise_or(0b1100, 0b1010)
        0b1110  # 14 in decimal
        >>> Bitwise().bitwise_or(3, 5)
        7
        """
        return a | b

    def bitwise_xor(self, a: int, b: int) -> int:
        """Perform a bitwise XOR (exclusive OR) operation between two integers.

        The bitwise XOR operation compares each bit of two numbers and returns 1
        if the bits are different, and 0 if they are the same.

        Parameters
        ----------
        a : int
            First integer operand
        b : int
            Second integer operand

        Returns
        -------
        int
            Result of the bitwise XOR operation

        Examples
        --------
        >>> Bitwise().bitwise_xor(0b1100, 0b1010)
        0b0110  # 6 in decimal
        >>> Bitwise().bitwise_xor(3, 5)
        6
        """
        return a ^ b

    def bitwise_not(self, a: int) -> int:
        """Perform a bitwise NOT (inversion) operation on an integer.

        The bitwise NOT operation inverts all the bits of the number. Note that
        this is affected by Python's representation of negative numbers using
        two's complement.

        Parameters
        ----------
        a : int
            Integer operand to invert

        Returns
        -------
        int
            Result of the bitwise NOT operation

        Examples
        --------
        >>> Bitwise().bitwise_not(0b1010)
        -0b1011  # -11 in decimal (depends on bit length)
        >>> Bitwise().bitwise_not(3)
        -4
        """
        return ~a
