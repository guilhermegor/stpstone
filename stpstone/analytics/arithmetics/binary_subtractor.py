from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.analytics.arithmetics.bit_subtractor import FullSubtractor


class BinarySubtractor(metaclass=TypeChecker):
    def __init__(self, minuend: str, subtrahend: str) -> None:
        """
        Initialize the BinarySubtractor with two binary numbers as strings.
        Args:
            minuend (str): The first binary number (the number being subtracted from).
            subtrahend (str): The second binary number (the number to subtract).
        """
        self.minuend = minuend.zfill(max(len(minuend), len(subtrahend)))
        self.subtrahend = subtrahend.zfill(max(len(minuend), len(subtrahend)))
        self.result = ""

    @property
    def subtract(self) -> str:
        """
        Perform binary subtraction using the Full Subtractor logic.
        Returns:
            str: The binary subtraction result as a string.
        """
        result = []
        borrow = 0
        for i in range(len(self.minuend) - 1, -1, -1):
            a = int(self.minuend[i])
            b = int(self.subtrahend[i])
            fs = FullSubtractor(a, b, borrow)
            result.append(str(fs.difference))
            borrow = fs.borrow_out

        self.result = "".join(result[::-1])
        return self.result

if __name__ == "__main__":
    minuend = "1011"  # 11 in decimal
    subtrahend = "0101"  # 5 in decimal
    subtractor = BinarySubtractor(minuend, subtrahend)
    result = subtractor.subtract()
    print(f"Binary Subtraction: {minuend} - {subtrahend} = {result}")  # output: "0110" (6 in decimal)
