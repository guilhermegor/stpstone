from stpstone.dsa.stacks.simple_stack import Stack
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from typing import Optional


class BaseConverter(metaclass=TypeChecker):

    def __init__(
        self,
        str_num: str,
        int_from_base: Optional[int] = 10,
        int_to_base: Optional[int] = 2
    ) -> None:
        """
        Initialize the BaseConverter with a str_num and its conversion bases.

        Args:
            str_num (str): The str_num to be converted, represented as a string.
            int_from_base (int): The base of the input str_num, must be between 2 and 16.
            int_to_base (int): The base to convert the input str_num to, must be between 2 and 16.

        Raises:
            ValueError: If either `int_from_base` or `int_to_base` is not between 2 and 16.
        """
        if not (2 <= int_from_base <= 16) or not (2 <= int_to_base <= 16):
            raise ValueError("Base must be between 2 and 16")
        self.str_num = str_num
        self.int_from_base = int_from_base
        self.int_to_base = int_to_base
        self.digits = "0123456789ABCDEF"
        for char in self.str_num:
            if char.upper() not in self.digits:
                raise ValueError(f"Invalid str_num: {str_num}, please enter a valid "
                                 + f"str_num with characters between {self.digits}")

    @property
    def _to_decimal(self) -> int:
        decimal_value = 0
        for i, digit in enumerate(reversed(self.str_num)):
            decimal_value += self.digits.index(digit.upper()) * (self.int_from_base ** i)
        return decimal_value

    @property
    def convert(self) -> str:
        decimal_value = self._to_decimal
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
