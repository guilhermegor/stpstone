from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class HalfAdder(metaclass=TypeChecker):

    def __init__(self, a: int, b: int) -> None:
        """
        Initialize the Half Adder with two binary inputs.
        Args:
            a (int): First binary input (0 or 1)
            b (int): Second binary input (0 or 1)
        Returns:
            None
        """
        self.a = a
        self.b = b

    @property
    def sum(self) -> int:
        """
        Returns the sum output of the half adder.

        The half adder computes the sum of two binary digits (bits) and produces
        two outputs: the sum and the carry. The sum is calculated using the XOR
        operation:
            - Sum (S) = A XOR B
        Args:
            None
        Returns:
            int: The sum of the two binary inputs.
        """
        return self.a ^ self.b

    @property
    def carry(self) -> int:
        """
        Returns the carry output of the half adder.

        The carry output indicates whether there is an overflow from the addition
        of the two bits. The carry is calculated using the AND operation:
            - Carry (C) = A AND B
        Args:
            None
        Returns:
            int: The carry output of the half adder.
        """
        return self.a & self.b


class FullAdder(metaclass=TypeChecker):

    def __init__(self, a: int, b: int, carry_in: int) -> None:
        """
        Initialize the Full Adder with two binary inputs and a carry-in.
        Args:
            a (int): First binary input (0 or 1)
            b (int): Second binary input (0 or 1)
            carry_in (int): Carry input from the previous stage (0 or 1).
        Returns:
            None
        """
        self.a = a
        self.b = b
        self.carry_in = carry_in

    @property
    def sum(self) -> int:
        """
        Returns the sum output of the full adder.

        The full adder computes the sum of two binary digits (bits) and a carry-in
        bit, producing a sum and a carry-out. The sum is calculated using the XOR
        operation:
            - Sum (S) = A XOR B XOR Carry-In
        Args:
            None
        Returns:
            int: The sum of the two binary inputs and the carry-in
        """
        return (self.a ^ self.b) ^ self.carry_in

    @property
    def carry_out(self) -> int:
        """
        Returns the carry output of the full adder.

        The carry output indicates whether there is an overflow from the addition
        of the two bits and the carry-in. The carry-out is calculated using the
        OR operation on the AND results:
            - Carry-Out (Cout) = (A AND B) OR (Carry-In AND (A XOR B))
        Args:
            None
        Returns:
            int: The carry output of the full adder
        """
        return (self.a & self.b) | (self.carry_in & (self.a ^ self.b))


class EightBitFullAdder(metaclass=TypeChecker):

    def __init__(self, a: int, b: int) -> None:
        """
        Initialize the Eight-Bit Full Adder with two 8-bit binary numbers.
        Args:
            a (int): First 8-bit binary number
            b (int): Second 8-bit binary number
        Returns:
            None
        """
        self.a = a
        self.b = b

    @property
    def add(self) -> tuple:
        """
        Adds two 8-bit numbers and returns the sum and carry out.

        This method iterates through each bit of the two 8-bit numbers, using
        a full adder to compute the sum and carry for each bit position. The
        final result is an 8-bit sum and a carry-out bit.
        Args:
            None
        Returns:
            tuple: A tuple containing the sum as an integer and the carry-out bit.
        """
        carry = 0
        sum_result = 0
        for i in range(8):
            # extract the i-th bit from a and b
            bit_a = (self.a >> i) & 1
            bit_b = (self.b >> i) & 1
            # create a full adder for the current bit
            full_adder = FullAdder(bit_a, bit_b, carry)
            sum_result |= (full_adder.sum << i)  # set the i-th bit of the sum
            carry = full_adder.carry_out  # update carry for the next bit
        return sum_result, carry


# example usage of the Half Adder, Full Adder, and Eight-Bit Full Adder
if __name__ == "__main__":
    # test half adder
    ha1 = HalfAdder(0, 0)
    print(f"Half Adder (0, 0) => Sum: {ha1.sum}, Carry: {ha1.carry}")  # output: Sum: 0, Carry: 0

    ha2 = HalfAdder(0, 1)
    print(f"Half Adder (0, 1) => Sum: {ha2.sum}, Carry: {ha2.carry}")  # output: Sum: 1, Carry: 0

    ha3 = HalfAdder(1, 0)
    print(f"Half Adder (1, 0) => Sum: {ha3.sum}, Carry: {ha3.carry}")  # output: Sum: 1, Carry: 0

    ha4 = HalfAdder(1, 1)
    print(f"Half Adder (1, 1) => Sum: {ha4.sum}, Carry: {ha4.carry}")  # output: Sum: 0, Carry: 1

    # test full adder
    fa1 = FullAdder(1, 1, 0)
    print(f"Full Adder (1, 1, 0) => Sum: {fa1.sum}, Carry Out: {fa1.carry_out}")  # output: Sum: 0, Carry Out: 1

    fa2 = FullAdder(1, 0, 1)
    print(f"Full Adder (1, 0, 1) => Sum: {fa2.sum}, Carry Out: {fa2.carry_out}")  # output: Sum: 0, Carry Out: 1

    # test eight bit full adder
    eight_bit_adder = EightBitFullAdder(0b11001100, 0b10101010)
    result, carry = eight_bit_adder.add()
    print(f"8-bit Full Adder Result: {bin(result)}, Carry Out: {carry}")  # output: Result in binary, Carry Out: 0 or 1
