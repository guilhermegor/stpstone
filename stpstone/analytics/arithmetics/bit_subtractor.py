from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class HalfSubtractor(metaclass=TypeChecker):

    def __init__(self, a: int, b: int) -> None:
        """
        Initialize the Half Subtractor with two binary inputs.
        Args:
            a (int): First binary input (0 or 1)
            b (int): Second binary input (0 or 1)
        Returns:
            None
        """
        self.a = a
        self.b = b

    @property
    def difference(self) -> int:
        """
        Returns the difference output of the half subtractor.
        The difference output indicates the difference of the two binary inputs.
        The difference is calculated using the XOR operation:
            - Difference (D) = A XOR B
        Args:
            None
        Returns:
            int: The difference output of the half subtractor.
        """
        return self.a ^ self.b

    @property
    def borrow(self) -> int:
        """
        Returns the borrow output of the half subtractor.
        The borrow output indicates whether there is a borrow from the subtraction
        of the two binary inputs.
        The borrow is calculated using the following logic:
            - Borrow occurs when A is less than B
        Args:
            None
        Returns:
            int: The borrow output of the half subtractor.
        """
        return not self.a and self.b


class FullSubtractor(metaclass=TypeChecker):

    def __init__(self, a: int, b: int, borrow_in: int) -> None:
        """
        Initialize the Full Subtractor with two binary inputs and a borrow-in.
        Args:
            a (int): First binary input (0 or 1)
            b (int): Second binary input (0 or 1)
            borrow_in (int): Borrow input from the previous stage (0 or 1).
        Returns:
            None
        """
        self.a = a
        self.b = b
        self.borrow_in = borrow_in

    @property
    def difference(self) -> int:
        """
        Returns the difference output of the full subtractor.
        The difference output indicates the difference of the two binary inputs and
        the borrow-in. The difference is calculated using the XOR operation:
            - Difference (D) = A XOR B XOR Borrow-In
        Args:
            None
        Returns:
            int: The difference output of the full subtractor.
        """
        return (self.a ^ self.b) ^ self.borrow_in

    @property
    def borrow_out(self) -> int:
        """
        Returns the borrow output of the full subtractor.
        The borrow output indicates whether there is a borrow from the subtraction
        of the two binary inputs and the borrow-in.
        The borrow is calculated using the following logic:
            - Borrow occurs when A is less than B, or
            - Borrow occurs when Borrow-In is 1 and A XOR B is 0
        Args:
            None
        Returns:
            int: The borrow output of the full subtractor.
        """
        return (not self.a and self.b) or (self.borrow_in and not (self.a ^ self.b))


if __name__ == "__main__":
    hs = HalfSubtractor(1, 1)
    # output: Difference: 0, Borrow: 1
    print(f"Half Subtractor (1, 1) => Difference: {hs.difference()}, Borrow: {hs.borrow()}")

    fs = FullSubtractor(1, 0, 1)
    # output: Difference: 0, Borrow Out: 1
    print(f"Full Subtractor (1, 0, 1) => Difference: {fs.difference()}, Borrow Out: {fs.borrow_out()}")
