"""Binary division implementation.

This module provides a class for performing binary division operations on integers,
returning both quotient and remainder. The division is implemented using bitwise
operations for efficiency.

Classes
-------
BinaryDivider
    A class that performs binary division on two numbers, returning quotient and remainder.
"""

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class BinaryDivider(metaclass=TypeChecker):
    """A class for performing binary division between two numbers.

    This class implements binary division using bit shifting operations, which is
    more efficient than standard division for some applications. It returns both
    the quotient and remainder of the division operation.

    Parameters
    ----------
    dividend : int
        The number to be divided (must be non-negative).
    divisor : int
        The number to divide by (must be positive).

    Raises
    ------
    ValueError
        If divisor is zero.

    Examples
    --------
    >>> divider = BinaryDivider(12, 2)  # 12 in binary is 1100, 2 is 10
    >>> quotient, remainder = divider.divide
    >>> print(f"Quotient: {bin(quotient)}, Remainder: {bin(remainder)}")
    Quotient: 0b110, Remainder: 0b0  # 6 with remainder 0
    """

    def __init__(self, dividend: int, divisor: int) -> None:
        """Initialize the Binary Divider with dividend and divisor.

        Parameters
        ----------
        dividend : int
            The number to be divided (must be non-negative).
        divisor : int
            The number to divide by (must be positive).

        Raises
        ------
        ValueError
            If divisor is zero.

        Examples
        --------
        >>> BinaryDivider(12, 2)  # Valid initialization
        >>> BinaryDivider(10, 0)   # Raises ValueError
        """
        if divisor == 0:
            raise ValueError("Divisor cannot be zero.")
        self.dividend = dividend
        self.divisor = divisor

    def divide(self) -> tuple[int, int]:
        """Perform binary division and return quotient and remainder.

        This method implements binary division using bit shifting operations.
        It works by iteratively shifting the divisor and comparing it with
        the remaining dividend. The algorithm is optimized for 8-bit numbers
        but can be extended for larger numbers by adjusting the range.

        Returns
        -------
        tuple[int, int]
            A tuple containing (quotient, remainder).

        Examples
        --------
        >>> divider = BinaryDivider(12, 2)
        >>> quotient, remainder = divider.divide
        >>> quotient
        6
        >>> remainder
        0

        >>> divider = BinaryDivider(15, 4)
        >>> quotient, remainder = divider.divide
        >>> quotient
        3
        >>> remainder
        3
        """
        quotient = 0
        remainder = self.dividend
        for i in range(7, -1, -1):
            if remainder >= (self.divisor << i):
                remainder -= (self.divisor << i)
                quotient |= (1 << i)
        return quotient, remainder
