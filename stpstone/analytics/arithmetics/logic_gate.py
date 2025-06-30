"""Logic gate implementations.

This module provides implementations of basic logic gates (NAND, NOR, XOR) that
operate on boolean inputs. Each gate follows standard digital logic behavior
and can be used in boolean expressions.

Classes
-------
NANDGate
    Implements the NAND (NOT AND) logic operation
NORGate
    Implements the NOR (NOT OR) logic operation
XORGate
    Implements the XOR (Exclusive OR) logic operation
"""

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class NANDGate(metaclass=TypeChecker):
    """A NAND (NOT AND) logic gate implementation.

    The NAND gate outputs False only when all inputs are True, and outputs
    True otherwise. It is a universal gate that can be used to construct
    all other logic gates.

    Parameters
    ----------
    a : bool
        First input to the gate
    b : bool
        Second input to the gate

    Examples
    --------
    >>> gate = NANDGate(True, True)
    >>> bool(gate)
    False
    >>> NANDGate(True, False)
    NANDGate(a=True, b=False, output=True)
    """

    def __init__(self, a: bool, b: bool) -> None:
        """Initialize the NAND gate with two boolean inputs."""
        self.a = a
        self.b = b

    def __bool__(self) -> bool:
        """Compute and return the NAND gate output.

        The output follows these rules:
        - Returns False only when both inputs are True
        - Returns True in all other cases

        Returns
        -------
        bool
            The output of the NAND operation

        Examples
        --------
        >>> bool(NANDGate(False, False))
        True
        >>> bool(NANDGate(True, True))
        False
        """
        return not (self.a and self.b)

    def __repr__(self) -> str:
        """Return an unambiguous string representation of the gate.

        Returns
        -------
        str
            String showing inputs and current output state

        Examples
        --------
        >>> repr(NANDGate(False, True))
        'NANDGate(a=False, b=True, output=True)'
        """
        return f"NANDGate(a={self.a}, b={self.b}, output={bool(self)})"


class NORGate(metaclass=TypeChecker):
    """A NOR (NOT OR) logic gate implementation.

    The NOR gate outputs True only when all inputs are False, and outputs
    False otherwise. Like NAND, it is a universal gate.

    Parameters
    ----------
    a : bool
        First input to the gate
    b : bool
        Second input to the gate

    Examples
    --------
    >>> gate = NORGate(False, False)
    >>> bool(gate)
    True
    >>> NORGate(True, False)
    NORGate(a=True, b=False, output=False)
    """

    def __init__(self, a: bool, b: bool) -> None:
        """Initialize the NOR gate with two boolean inputs."""
        self.a = a
        self.b = b

    def __bool__(self) -> bool:
        """Compute and return the NOR gate output.

        The output follows these rules:
        - Returns True only when both inputs are False
        - Returns False in all other cases

        Returns
        -------
        bool
            The output of the NOR operation

        Examples
        --------
        >>> bool(NORGate(False, False))
        True
        >>> bool(NORGate(True, False))
        False
        """
        return not (self.a or self.b)

    def __repr__(self) -> str:
        """Return an unambiguous string representation of the gate.

        Returns
        -------
        str
            String showing inputs and current output state

        Examples
        --------
        >>> repr(NORGate(True, True))
        'NORGate(a=True, b=True, output=False)'
        """
        return f"NORGate(a={self.a}, b={self.b}, output={bool(self)})"


class XORGate(metaclass=TypeChecker):
    """An XOR (Exclusive OR) logic gate implementation.

    The XOR gate outputs True when the inputs are different, and False
    when they are the same. It is commonly used in arithmetic circuits.

    Parameters
    ----------
    a : bool
        First input to the gate
    b : bool
        Second input to the gate

    Examples
    --------
    >>> gate = XORGate(True, False)
    >>> bool(gate)
    True
    >>> XORGate(False, False)
    XORGate(a=False, b=False, output=False)
    """

    def __init__(self, a: bool, b: bool) -> None:
        """Initialize the XOR gate with two boolean inputs."""
        self.a = a
        self.b = b

    def __bool__(self) -> bool:
        """Compute and return the XOR gate output.

        The output follows these rules:
        - Returns True when inputs are different
        - Returns False when inputs are the same

        Returns
        -------
        bool
            The output of the XOR operation

        Examples
        --------
        >>> bool(XORGate(False, True))
        True
        >>> bool(XORGate(True, True))
        False
        """
        return (self.a and not self.b) or (not self.a and self.b)

    def __repr__(self) -> str:
        """Return an unambiguous string representation of the gate.

        Returns
        -------
        str
            String showing inputs and current output state

        Examples
        --------
        >>> repr(XORGate(True, False))
        'XORGate(a=True, b=False, output=True)'
        """
        return f"XORGate(a={self.a}, b={self.b}, output={bool(self)})"
