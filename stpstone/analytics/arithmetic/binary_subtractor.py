"""Binary subtractor implementation."""

from stpstone.analytics.arithmetic.bit_subtractor import FullSubtractor
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class BinarySubtractor(metaclass=TypeChecker):
    """A class for performing binary subtraction between two binary numbers."""

    def __init__(self, minuend: str, subtrahend: str) -> None:
        """Initialize the BinarySubtractor with two binary numbers.
        
        Parameters
        ----------
        minuend : str
            The minuend (the number to be subtracted from)
        subtrahend : str
            The subtrahend (the number to be subtracted)
        """
        self._validate_inputs(minuend, subtrahend)
        max_length = max(len(minuend), len(subtrahend))
        self.minuend = minuend.zfill(max_length)
        self.subtrahend = subtrahend.zfill(max_length)
        self.result = ""

    def _validate_inputs(self, minuend: str, subtrahend: str) -> None:
        """Validate that inputs are non-empty binary strings.
        
        Parameters
        ----------
        minuend : str
            The minuend (the number to be subtracted from)
        subtrahend : str
            The subtrahend (the number to be subtracted)

        Raises
        ------
        TypeError
            If minuend or subtrahend is not a string
        ValueError
            If minuend or subtrahend is empty
        ValueError
            If minuend or subtrahend is not a binary string
        """
        if not isinstance(minuend, str) or not isinstance(subtrahend, str):
            raise TypeError("Inputs must be strings")
        if not minuend or not subtrahend:
            raise ValueError("Input strings cannot be empty")
        if not all(c in '01' for c in minuend):
            raise ValueError("Minuend must be a binary string")
        if not all(c in '01' for c in subtrahend):
            raise ValueError("Subtrahend must be a binary string")

    def subtract(self) -> str:
        """Perform binary subtraction using full subtractor logic.
        
        Returns
        -------
        str
            The binary result of the subtraction.
        """
        result = []
        borrow = 0
        
        for i in range(len(self.minuend) - 1, -1, -1):
            a = int(self.minuend[i])
            b = int(self.subtrahend[i])
            fs = FullSubtractor(a, b, borrow)
            result.append(str(fs.get_difference()))
            borrow = fs.get_borrow_out()

        self.result = "".join(reversed(result))
        return self.result