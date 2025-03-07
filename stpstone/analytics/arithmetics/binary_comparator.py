from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class BinaryComparator(metaclass=TypeChecker):
    def __init__(self, a: int, b: int) -> None:
        """Initialize the Binary Comparator with two binary inputs."""
        self.a = a
        self.b = b

    @property
    def compare(self) -> str:
        """
        Compare two binary inputs and return a comparison result as a string.

        This property compares the binary inputs `a` and `b` and returns a string
        indicating their relationship. The possible return values are:
            - "A is less than B" if `a` is less than `b`
            - "A is greater than B" if `a` is greater than `b`
            - "A is equal to B" if `a` is equal to `b`

        Returns:
            str: A string describing the comparison result between `a` and `b`.
        """
        if self.a < self.b:
            return "A is less than B"
        elif self.a > self.b:
            return "A is greater than B"
        else:
            return "A is equal to B"


if __name__ == "__main__":
    comparator = BinaryComparator(0b1100, 0b1010)
    result = comparator.compare()
    # output: A is greater than B
    print(f"Binary Comparator (1100, 1010) => {result}")
