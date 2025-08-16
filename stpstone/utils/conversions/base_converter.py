"""Base conversion utilities for numeric strings.

This module provides a class for converting numbers between different bases (2-16)
using stack-based operations and proper input validation.
"""

from typing import Optional

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.dsa.stacks.simple_stack import Stack


class BaseConverter(metaclass=TypeChecker):
    """Class for converting numbers between different bases (2-16).

    Attributes
    ----------
    str_num : str
        The number to be converted, represented as a string
    int_from_base : int
        The base of the input number (2-16)
    int_to_base : int
        The base to convert to (2-16)
    digits : str
        Valid digits for bases up to 16
    """

    def __init__(
        self,
        str_num: str,
        int_from_base: Optional[int] = 10,
        int_to_base: Optional[int] = 2
    ) -> None:
        """Initialize the BaseConverter with a number and its conversion bases.

        Parameters
        ----------
        str_num : str
            The number to be converted, represented as a string
        int_from_base : Optional[int]
            The base of the input number (default: 10)
        int_to_base : Optional[int]
            The base to convert to (default: 2)
        """
        self.str_num = str_num
        self.int_from_base = int_from_base
        self.int_to_base = int_to_base
        self.digits = "0123456789ABCDEF"

        self._validate_base(int_from_base, "int_from_base")
        self._validate_base(int_to_base, "int_to_base")
        self._validate_str_num(str_num)

    def _validate_base(self, base: Optional[int], name: str) -> None:
        """Validate that a base is between 2 and 16.

        Parameters
        ----------
        base : Optional[int]
            Base value to validate
        name : str
            Variable name for error messages

        Raises
        ------
        ValueError
            If base is not between 2 and 16
        """
        if base is None or not 2 <= base <= 16:
            raise ValueError(f"{name} must be between 2 and 16, got {base}")

    def _validate_str_num(self, str_num: str) -> None:
        """Validate that all characters in str_num are valid for the given base.

        Parameters
        ----------
        str_num : str
            Number string to validate

        Raises
        ------
        ValueError
            If str_num contains invalid characters for the given base
        """
        if not str_num:
            raise ValueError("Input string cannot be empty")
        
        max_digit = self.digits[:self.int_from_base]
        for char in str_num.upper():
            if char not in max_digit:
                raise ValueError(
                    f"Invalid character '{char}' in input for base {self.int_from_base}. "
                    f"Valid digits are: {max_digit}"
                )

    def _to_decimal(self) -> int:
        """Convert the number from its original base to decimal.

        Returns
        -------
        int
            The decimal equivalent of the input number

        Notes
        -----
        Uses positional notation for base conversion:
        decimal_value = sum(digit_value * (base^position))
        """
        decimal_value = 0
        for i, digit in enumerate(reversed(self.str_num)):
            digit_value = self.digits.index(digit.upper())
            decimal_value += digit_value * (self.int_from_base ** i)
        return decimal_value

    def convert(self) -> str:
        """Convert the number to the target base.

        Returns
        -------
        str
            The number represented in the target base

        Notes
        -----
        Conversion process:
        1. Convert to decimal first (if not already in decimal)
        2. Use division-remainder method for target base conversion
        3. Uses a stack to reverse the remainders
        """
        decimal_value = self._to_decimal()
        
        if self.int_to_base == 10:
            return str(decimal_value)
        if decimal_value == 0:
            return "0"

        rem_stack = Stack()
        num = decimal_value
        while num > 0:
            rem_stack.push(num % self.int_to_base)
            num //= self.int_to_base

        new_string = ""
        while not rem_stack.is_empty:
            new_string += self.digits[rem_stack.pop()]
        return new_string