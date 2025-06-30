"""Binary multiplication implementation.

This module provides a class for performing binary multiplication operations on integers
using bitwise operations. The implementation is optimized for efficiency by leveraging
bit shifting properties.

Classes
-------
BinaryMultiplier
    A class that performs binary multiplication on two numbers using bit shifting.
"""

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class BinaryMultiplier(metaclass=TypeChecker):
    """A class for performing binary multiplication between two numbers.

    This class implements binary multiplication using bit shifting and addition,
    which is more efficient than standard multiplication for some applications.
    The algorithm works by examining each bit of the multiplier and adding shifted
    versions of the multiplicand accordingly.

    Parameters
    ----------
    a : int
        The multiplicand (first binary number to multiply).
    b : int
        The multiplier (second binary number to multiply).

    Examples
    --------
    >>> multiplier = BinaryMultiplier(0b1100, 0b1010)  # 12 × 10
    >>> product = multiplier.multiply()
    >>> bin(product)
    '0b1111000'  # 120 in binary
    """

    def __init__(self, a: int, b: int) -> None:
        """Initialize the Binary Multiplier with two binary numbers.

        Parameters
        ----------
        a : int
            The multiplicand (first binary number to multiply).
        b : int
            The multiplier (second binary number to multiply).

        Examples
        --------
        >>> BinaryMultiplier(0b1100, 0b1010)  # 12 × 10
        """
        self.a = a
        self.b = b

    def multiply(self) -> int:
        """Multiply two binary numbers using bit shifting.

        This method implements binary multiplication by:
        1. Iterating through each bit of the multiplier (b)
        2. When a set bit is found, shifting the multiplicand (a) left by the bit position
        3. Accumulating the shifted values to form the final product

        The current implementation supports 8-bit numbers but can be extended.

        Returns
        -------
        int
            The product of the two binary numbers.

        Examples
        --------
        >>> multiplier = BinaryMultiplier(0b1100, 0b1010)  # 12 × 10
        >>> product = multiplier.multiply()
        >>> product
        120
        >>> bin(product)
        '0b1111000'

        >>> multiplier = BinaryMultiplier(0b1111, 0b0001)  # 15 × 1
        >>> multiplier.multiply()
        15
        """
        result = 0
        for i in range(8):
            if (self.b >> i) & 1:
                result += (self.a << i)
        return result
