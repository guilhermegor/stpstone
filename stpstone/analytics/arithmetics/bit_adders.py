"""Bit adder implementations.

This module provides implementations of various bit adders:
- HalfAdder: Adds two single bits
- FullAdder: Adds two bits with carry-in
- EightBitFullAdder: Adds two 8-bit numbers using full adders

Classes
-------
HalfAdder
    Adds two single bits producing sum and carry
FullAdder
    Adds two bits with carry-in producing sum and carry-out
EightBitFullAdder
    Adds two 8-bit numbers using cascaded full adders
"""

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class HalfAdder(metaclass=TypeChecker):
    """A half adder circuit that adds two single-bit bit numbers.

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
        if not isinstance(a, int) or not isinstance(b, int):
            raise TypeError("Inputs must be integers")
        if a not in (0, 1) or b not in (0, 1):
            raise ValueError("Inputs must be 0 or 1")
        self.a = a
        self.b = b

    def get_sum(self) -> int:
        """Calculate the sum output of the half adder.

        The sum is computed using XOR operation:
        Sum = A XOR B

        Returns
        -------
        int
            The sum output (0 or 1)

        Examples
        --------
        >>> HalfAdder(0, 0).get_sum()
        0
        >>> HalfAdder(1, 1).get_sum()
        0
        """
        return self.a ^ self.b

    def get_carry(self) -> int:
        """Calculate the carry output of the half adder.

        The carry is computed using AND operation:
        Carry = A AND B

        Returns
        -------
        int
            The carry output (0 or 1)

        Examples
        --------
        >>> HalfAdder(0, 1).get_carry()
        0
        >>> HalfAdder(1, 1).get_carry()
        1
        """
        return self.a & self.b


class FullAdder(metaclass=TypeChecker):
    """A full adder circuit that adds two bits with carry-in.

    The full adder performs bit addition of two bits with a carry-in
    and produces a sum bit and a carry-out bit.

    Parameters
    ----------
    a : int
        First bit input (0 or 1)
    b : int
        Second bit input (0 or 1)
    carry_in : int
        Carry input from previous stage (0 or 1)

    Examples
    --------
    >>> adder = FullAdder(1, 1, 0)
    >>> adder.get_sum()
    0
    >>> adder.get_carry_out()
    1
    """

    def __init__(self, a: int, b: int, carry_in: int) -> None:
        """Initialize the full adder with two bits and carry-in."""
        if not isinstance(a, int) or not isinstance(b, int) or not isinstance(carry_in, int):
            raise TypeError("All inputs must be integers")
        if a not in (0, 1) or b not in (0, 1) or carry_in not in (0, 1):
            raise ValueError("All inputs must be 0 or 1")
        self.a = a
        self.b = b
        self.carry_in = carry_in

    def get_sum(self) -> int:
        """Calculate the sum output of the full adder.

        The sum is computed using:
        Sum = A XOR B XOR Carry-In

        Returns
        -------
        int
            The sum output (0 or 1)

        Examples
        --------
        >>> FullAdder(1, 0, 1).get_sum()
        0
        >>> FullAdder(1, 1, 1).get_sum()
        1
        """
        return (self.a ^ self.b) ^ self.carry_in

    def get_carry_out(self) -> int:
        """Calculate the carry-out of the full adder.

        The carry-out is computed using:
        Carry-Out = (A AND B) OR (Carry-In AND (A XOR B))

        Returns
        -------
        int
            The carry-out bit (0 or 1)

        Examples
        --------
        >>> FullAdder(1, 0, 1).get_carry_out()
        1
        >>> FullAdder(0, 0, 1).get_carry_out()
        0
        """
        return (self.a & self.b) | (self.carry_in & (self.a ^ self.b))


class EightBitFullAdder(metaclass=TypeChecker):
    """An 8-bit adder implemented using full adders.

    This class adds two 8-bit numbers by cascading eight full adders,
    with the carry-out of each adder feeding into the next.

    Parameters
    ----------
    a : int
        First 8-bit number (0-255)
    b : int
        Second 8-bit number (0-255)

    Examples
    --------
    >>> adder = EightBitFullAdder(0b11001100, 0b00110011)
    >>> sum_result, carry = adder.add()
    >>> bin(sum_result)
    '0b11111111'
    """

    def __init__(self, a: int, b: int) -> None:
        """Initialize the 8-bit adder with two 8-bit numbers."""
        if not isinstance(a, int) or not isinstance(b, int):
            raise TypeError("Inputs must be integers")
        if a < 0 or a > 255 or b < 0 or b > 255:
            raise ValueError("Inputs must be 8-bit numbers (0-255)")
        self.a = a
        self.b = b

    def add(self) -> tuple[int, int]:
        """Add two 8-bit numbers and return the sum and final carry-out.

        The addition is performed by:
        1. Processing each bit from LSB to MSB
        2. Using a full adder for each bit position
        3. Propagating the carry between stages

        Returns
        -------
        tuple[int, int]
            A tuple containing:
            - sum_result: The 8-bit sum (0-255)
            - carry: The final carry-out bit (0 or 1)

        Examples
        --------
        >>> EightBitFullAdder(10, 20).add()
        (30, 0)
        >>> EightBitFullAdder(255, 1).add()
        (0, 1)
        """
        carry = 0
        sum_result = 0
        for i in range(8):
            bit_a = (self.a >> i) & 1
            bit_b = (self.b >> i) & 1
            full_adder = FullAdder(bit_a, bit_b, carry)
            sum_result |= (full_adder.get_sum() << i)
            carry = full_adder.get_carry_out()
        return sum_result, carry
