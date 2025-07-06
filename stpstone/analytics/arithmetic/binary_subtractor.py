"""Binary subtraction implementation.

This module provides a class for performing binary subtraction operations on binary strings
using full subtractor logic. The implementation handles borrow propagation between bits.

Classes
-------
BinarySubtractor
    A class that performs binary subtraction on two binary strings.
"""

from stpstone.analytics.arithmetics.bit_subtractor import FullSubtractor
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class BinarySubtractor(metaclass=TypeChecker):
    """A class for performing binary subtraction between two binary numbers.

    This class implements binary subtraction using full subtractor components to handle
    borrow propagation between bits. The numbers are processed as strings of binary digits
    (0s and 1s) of equal length, with the shorter number being zero-padded.

    Parameters
    ----------
    minuend : str
        The binary number being subtracted from (must contain only 0s and 1s).
    subtrahend : str
        The binary number to subtract (must contain only 0s and 1s).

    Attributes
    ----------
    minuend : str
        Zero-padded minuend string.
    subtrahend : str
        Zero-padded subtrahend string.
    result : str
        Stores the result of the subtraction operation.

    Examples
    --------
    >>> subtractor = BinarySubtractor("1011", "0101")  # 11 - 5
    >>> result = subtractor.subtract()
    >>> result
    '0110'  # 6 in decimal
    """

    def __init__(self, minuend: str, subtrahend: str) -> None:
        """Initialize the BinarySubtractor with two binary numbers.

        Parameters
        ----------
        minuend : str
            The binary number being subtracted from (must contain only 0s and 1s).
        subtrahend : str
            The binary number to subtract (must contain only 0s and 1s).

        Notes
        -----
        The input strings are automatically zero-padded to equal length.
        """
        self.minuend = minuend.zfill(max(len(minuend), len(subtrahend)))
        self.subtrahend = subtrahend.zfill(max(len(minuend), len(subtrahend)))
        self.result = ""

    def subtract(self) -> str:
        """Perform binary subtraction using full subtractor logic.

        This method implements binary subtraction by:
        1. Processing bits from least significant to most significant
        2. Using FullSubtractor components for each bit position
        3. Propagating borrow between bit positions
        4. Returning the result as a binary string

        Returns
        -------
        str
            The binary subtraction result as a string (may contain leading zeros).

        Examples
        --------
        >>> subtractor = BinarySubtractor("1101", "0110")  # 13 - 6
        >>> subtractor.subtract()
        '0111'  # 7 in decimal

        >>> subtractor = BinarySubtractor("1000", "0001")  # 8 - 1
        >>> subtractor.subtract()
        '0111'  # 7 in decimal
        """
        result = []
        borrow = 0
        for i in range(len(self.minuend) - 1, -1, -1):
            a = int(self.minuend[i])
            b = int(self.subtrahend[i])
            fs = FullSubtractor(a, b, borrow)
            result.append(str(fs.difference))
            borrow = fs.borrow_out

        self.result = "".join(result[::-1])
        return self.result
