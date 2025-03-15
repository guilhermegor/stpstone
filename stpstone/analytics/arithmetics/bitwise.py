from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class Utilities(metaclass=TypeChecker):

    def bitwise_and(self, a: int, b: int) -> int:
        return a & b

    def bitwise_or(self, a: int, b: int) -> int:
        return a | b

    def bitwise_xor(self, a: int, b: int) -> int:
        return a ^ b

    def bitwise_not(self, a: int) -> int:
        return ~a


if __name__ == "__main__":
    utils = Utilities()
    print(f"Bitwise AND (3, 5) => {utils.bitwise_and(3, 5)}")  # output: 1
    print(f"Bitwise OR (3, 5) => {utils.bitwise_or(3, 5)}")    # output: 7
    print(f"Bitwise XOR (3, 5) => {utils.bitwise_xor(3, 5)}")  # output: 6
    print(f"Bitwise NOT (3) => {utils.bitwise_not(3)}")        # output: -4
