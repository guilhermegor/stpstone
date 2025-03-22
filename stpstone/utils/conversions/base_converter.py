from stpstone.dsa.stacks.simple_stack import Stack
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class BaseConverter(metaclass=TypeChecker):

    def __init__(self, int_decimal: int, int_base: int) -> None:
        self.int_decimal = int_decimal
        if int_base < 2 or int_base > 16:
            raise ValueError("Base must be between 2 and 16")
        self.int_base = int_base
        self.digits = "0123456789ABCDEF"

    @property
    def convert(self) -> str:
        if self.int_decimal == 0:
            return "0"
        rem_stack = Stack()
        num = self.int_decimal
        while num > 0:
            rem_stack.push(num % self.int_base)
            num //= self.int_base
        new_string = ""
        while not rem_stack.is_empty():
            new_string += self.digits[rem_stack.pop()]
        return new_string
