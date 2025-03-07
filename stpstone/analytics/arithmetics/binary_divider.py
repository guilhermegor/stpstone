from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class BinaryDivider(metaclass=TypeChecker):

    def __init__(self, dividend: int, divisor: int) -> None:
        """
        Initialize the Binary Divider with two binary inputs.

        Args:
            dividend (int): The dividend, the number being divided.
            divisor (int): The divisor, the number by which we are dividing.

        Returns:
            None

        Raises:
            ValueError: If the divisor is zero.
        """
        if divisor == 0:
            raise ValueError("Divisor cannot be zero.")
        self.dividend = dividend
        self.divisor = divisor

    @property
    def divide(self) -> tuple:
        """
        Divide the dividend by the divisor using binary division.

        The algorithm works by shifting the divisor left and subtracting it
        from the dividend if the dividend is greater than or equal to the
        divisor. The result is then shifted back to the right, and the
        remainder is returned.

        Args:
            None

        Returns:
            tuple: (quotient, remainder)

        Raises:
            ValueError: If the divisor is zero.
        """
        quotient = 0
        remainder = self.dividend
        for i in range(7, -1, -1):
            # if remainder is greater than or equal to divisor, subtract divisor
            if remainder >= (self.divisor << i):
                remainder -= (self.divisor << i)
                # set the i-th bit of the quotient
                quotient |= (1 << i)
        return quotient, remainder


if __name__ == "__main__":
    divider = BinaryDivider(0b1100, 0b0010)
    quotient, remainder = divider.divide()
    # output: Quotient and Remainder in binary
    print(f"Binary Divider (1100 / 10) => Quotient: {bin(quotient)}, Remainder: {bin(remainder)}")
