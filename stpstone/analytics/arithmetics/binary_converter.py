from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class Converter(metaclass=TypeChecker):

    def binary_to_decimal(self, binary: str) -> int:
        """
        Converts a binary string to a decimal integer.

        Args:
            binary: A string of binary digits (0s and 1s)

        Returns:
            int: The decimal integer equivalent of the binary string
        """
        return int(binary, 2)

    def decimal_to_binary(self, decimal: int) -> str:
        """
        Converts a decimal integer to a binary string. Remove the '0b' prefix

        Args:
            decimal: An integer

        Returns:
            str: The binary string equivalent of the decimal integer
        """
        return bin(decimal)[2:]

    def decimal_to_hexadecimal(self, decimal: int) -> str:
        """
        Converts a decimal integer to a hexadecimal string. Remove the '0x' prefix and convert
        to uppercase

        Args:
            decimal: An integer

        Returns:
            str: The hexadecimal string equivalent of the decimal integer, in uppercase
        """
        return hex(decimal)[2:].upper()

    def hexadecimal_to_decimal(self, hexadecimal: str) -> int:
        """
        Converts a hexadecimal string to a decimal integer.

        Args:
            hexadecimal: A string of hexadecimal digits (0-9, A-F)

        Returns:
            int: The decimal integer equivalent of the hexadecimal string
        """
        return int(hexadecimal, 16)


if __name__ == "__main__":
    converter = Converter()
    print(f"Binary to Decimal (1010) => {converter.binary_to_decimal('1010')}")  # output: 10
    print(f"Decimal to Binary (10) => {converter.decimal_to_binary(10)}")  # output: 1010
    print(f"Decimal to Hexadecimal (255) => {converter.decimal_to_hexadecimal(255)}")  # output: FF
    print(f"Hexadecimal to Decimal (FF) => {converter.hexadecimal_to_decimal('FF')}")  # output: 255
