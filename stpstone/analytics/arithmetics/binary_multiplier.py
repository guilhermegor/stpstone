from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class BinaryMultiplier(metaclass=TypeChecker):

    def __init__(self, a: int, b: int) -> None:
        """
        Initialize the Binary Multiplier with two binary numbers.
        Args:
            a (int): First binary number
            b (int): Second binary number
        Returns:
            None
        """
        self.a = a
        self.b = b

    def multiply(self) -> int:
        """
        Multiply two binary numbers and return the result.

        The multiplication is performed by shifting the bits of the first number
        (self.a) to the left by the bit position of the second number (self.b) and
        adding the results.

        Args:
            None

        Returns:
            int: The result of multiplying the two binary numbers.
        """
        result = 0
        for i in range(8):
            # check if the i-th bit of b is set
            if (self.b >> i) & 1:
                # shift a left by i and add to result
                result += (self.a << i)
        return result


# example usage of the binary multiplier
if __name__ == "__main__":
    multiplier = BinaryMultiplier(0b1100, 0b1010)
    product = multiplier.multiply()
    print(f"Binary Multiplier (1100, 1010) => Product: {bin(product)}")  # output: Product in binary
