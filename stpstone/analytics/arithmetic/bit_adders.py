"""Bit adder implementations with enhanced type checking.

This module provides implementations of various bit adders with automatic
runtime type checking using the enhanced TypeChecker metaclass.
"""

from stpstone.transformations.validation.metaclass_type_checker import (
    AdvancedTypeChecker,
    ConfigurableTypeChecker,
    TypeChecker,
)


class HalfAdder(metaclass=TypeChecker):
    """A half adder circuit that adds two single-bit numbers.

    The half adder performs bit addition of two bits and produces
    a sum bit and a carry bit as outputs.

    Parameters
    ----------
    a : int
        First bit input (0 or 1)
    b : int
        Second bit input (0 or 1)

    Examples
    --------
    >>> adder = HalfAdder(0, 1)
    >>> adder.get_sum()
    1
    >>> adder.get_carry()
    0
    """

    def __init__(self, a: int, b: int) -> None:
        """Initialize the half adder with two bit inputs."""
        if a not in (0, 1) or b not in (0, 1):
            raise ValueError("Inputs must be 0 or 1")
        
        self.a = a
        self.b = b

    def get_sum(self) -> int:
        """Calculate the sum output of the half adder."""
        return self.a ^ self.b

    def get_carry(self) -> int:
        """Calculate the carry output of the half adder."""
        return self.a & self.b


class FullAdder(metaclass=TypeChecker):
    """A full adder circuit that adds two bits with carry-in."""

    def __init__(self, a: int, b: int, carry_in: int) -> None:
        """Initialize the full adder with two bits and carry-in."""
        if a not in (0, 1) or b not in (0, 1) or carry_in not in (0, 1):
            raise ValueError("All inputs must be 0 or 1")
        
        self.a = a
        self.b = b
        self.carry_in = carry_in

    def get_sum(self) -> int:
        """Calculate the sum output of the full adder."""
        return (self.a ^ self.b) ^ self.carry_in

    def get_carry_out(self) -> int:
        """Calculate the carry-out of the full adder."""
        return (self.a & self.b) | (self.carry_in & (self.a ^ self.b))


class EightBitFullAdder(metaclass=TypeChecker):
    """An 8-bit adder implemented using full adders."""

    def __init__(self, a: int, b: int) -> None:
        """Initialize the 8-bit adder with two 8-bit numbers."""
        if a < 0 or a > 255 or b < 0 or b > 255:
            raise ValueError("Inputs must be 8-bit numbers (0-255)")
        
        self.a = a
        self.b = b

    def add(self) -> tuple[int, int]:
        """Add two 8-bit numbers and return the sum and final carry-out."""
        carry = 0
        sum_result = 0
        
        for i in range(8):
            bit_a = (self.a >> i) & 1
            bit_b = (self.b >> i) & 1
            full_adder = FullAdder(bit_a, bit_b, carry)
            sum_result |= (full_adder.get_sum() << i)
            carry = full_adder.get_carry_out()
        
        return sum_result, carry

class ConfigurableHalfAdder(metaclass=AdvancedTypeChecker):
    """HalfAdder with advanced type checking configuration."""
    
    _type_check_config = {
        'strict': True,
        'check_returns': True,
        'exclude': {'_private_method'}
    }

    def __init__(self, a: int, b: int) -> None:
        """Initialize with automatic type checking."""
        if a not in (0, 1) or b not in (0, 1):
            raise ValueError("Inputs must be 0 or 1")
        self.a = a
        self.b = b

    def get_sum(self) -> int:
        """Get sum with return type checking."""
        return self.a ^ self.b

    def get_carry(self) -> int:
        """Get carry with return type checking."""
        return self.a & self.b
    
    def _private_method(self, x) -> int: # noqa: ANN001 - missing type annotation
        """Excluded from type checking."""
        return x


class FlexibleAdder(metaclass=ConfigurableTypeChecker):
    """Adder with flexible type checking configuration."""
    
    __type_check_config__ = {
        'enabled': True,
        'strict': False,           # only warn, don't raise exceptions
        'exclude_methods': {'debug_method', '_private_helper'},  # exclude private helper
        'include_private': True    # check private methods including __init__
    }

    def __init__(self, a: int, b: int) -> None:
        """Initialize with configurable type checking."""
        # store raw values first for testing purposes
        self.a = a
        self.b = b
        
        # only validate if both inputs are actually integers
        # this allows type checking to work with non-strict mode
        if isinstance(a, int) and isinstance(b, int) and (a not in (0, 1) or b not in (0, 1)):
                raise ValueError("Inputs must be 0 or 1")

    def add_numbers(self, x: int, y: int) -> int:
        """Add two numbers with type checking."""
        # simple addition - let the type checker handle type validation
        # don't do internal type conversion to allow type checker to work
        return x + y

    def debug_method(self, data: str) -> None:
        """Debug method excluded from type checking."""
        print(f"Debug: {data}")

    def _private_helper(self, value: str) -> str:
        """Private method - explicitly excluded from type checking."""
        return value.upper()