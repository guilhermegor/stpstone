### LOGIC GATE - BASIC LOGICAL OPERATIONS ON TWO BINARY INPUTS AND PRODUCES A SINGLE BINARY OUTPUT ###

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class NANDGate(metaclass=TypeChecker):
    def __init__(self, a: bool, b: bool) -> None:
        self.a = a
        self.b = b

    def __bool__(self) -> bool:
        """
        Returns the output of the NAND gate as a boolean.

        The NAND gate outputs False only when both inputs are True.
        In all other cases (when at least one input is False), it outputs True.
        This means:
            - NAND(False, False) => True
            - NAND(False, True) => True
            - NAND(True, False) => True
            - NAND(True, True) => False
        """
        return not (self.a and self.b)

    def __repr__(self) -> str:
        return f"NANDGate(a={self.a}, b={self.b}, output={bool(self)})"


class NORGate(metaclass=TypeChecker):
    def __init__(self, a: bool, b: bool) -> None:
        self.a = a
        self.b = b

    def __bool__(self) -> bool:
        """
        Returns the output of the NOR gate as a boolean.

        The NOR gate outputs True only when both inputs are False.
        In all other cases (when at least one input is True), it outputs False.
        This means:
            - NOR(False, False) => True
            - NOR(False, True) => False
            - NOR(True, False) => False
            - NOR(True, True) => False
        """
        return not (self.a or self.b)

    def __repr__(self) -> str:
        return f"NORGate(a={self.a}, b={self.b}, output={bool(self)})"


class XORGate(metaclass=TypeChecker):
    def __init__(self, a: bool, b: bool) -> None:
        self.a = a
        self.b = b

    def __bool__(self) -> bool:
        """
        Returns the output of the XOR gate as a boolean.

        The XOR (Exclusive OR) gate outputs True if exactly one of the inputs is True.
        It outputs False if both inputs are the same (both True or both False).
        This means:
            - XOR(False, False) => False
            - XOR(False, True) => True
            - XOR(True, False) => True
            - XOR(True, True) => False
        """
        return (self.a and not self.b) or (not self.a and self.b)

    def __repr__(self) -> str:
        return f"XORGate(a={self.a}, b={self.b}, output={bool(self)})"


# example usage of the logic gates
if __name__ == "__main__":
    nand_gate1 = NANDGate(False, False)
    print(f"NAND Gate Output (False, False): {bool(nand_gate1)}")  # output: True

    nand_gate2 = NANDGate(False, True)
    print(f"NAND Gate Output (False, True): {bool(nand_gate2)}")  # output: True

    nand_gate3 = NANDGate(True, False)
    print(f"NAND Gate Output (True, False): {bool(nand_gate3)}")  # output: True

    nand_gate4 = NANDGate(True, True)
    print(f"NAND Gate Output (True, True): {bool(nand_gate4)}")  # output: False

    nor_gate = NORGate(True, False)
    print(f"NOR Gate Output (True, False): {bool(nor_gate)}")  # output: False

    xor_gate = XORGate(True, False)
    print(f"XOR Gate Output (True, False): {bool(xor_gate)}")  # output: True

    nand_gate1 = NANDGate(False, False)
    print(repr(nand_gate1))  # output: NANDGate(a=False, b=False, output=True)

    nor_gate = NORGate(True, False)
    print(repr(nor_gate))  # output: NORGate(a=True, b=False, output=False)

    xor_gate = XORGate(True, False)
    print(repr(xor_gate))  # output: XORGate(a=True, b=False, output=True)
