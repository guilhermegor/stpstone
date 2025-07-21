"""Unit tests for logic gate implementations.

Tests the NAND, NOR, and XOR gate implementations with various input combinations.
"""

import pytest

from stpstone.analytics.arithmetic.logic_gate import NANDGate, NORGate, XORGate


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def all_false_inputs() -> tuple[bool, bool]:
    """Fixture providing (False, False) inputs."""
    return (False, False)

@pytest.fixture
def all_true_inputs() -> tuple[bool, bool]:
    """Fixture providing (True, True) inputs."""
    return (True, True)

@pytest.fixture
def mixed_inputs() -> list[tuple[bool, bool]]:
    """Fixture providing mixed (True, False) and (False, True) inputs."""
    return [(False, True), (True, False)]


# --------------------------
# NAND Gate Tests
# --------------------------
def test_nand_normal_operations(
    all_false_inputs: tuple[bool, bool],
    all_true_inputs: tuple[bool, bool],
    mixed_inputs: list[tuple[bool, bool]]
) -> None:
    """Test NAND gate with normal input combinations.
    
    Parameters
    ----------
    all_false_inputs : tuple[bool, bool]
        (False, False) inputs
    all_true_inputs : tuple[bool, bool]
        (True, True) inputs
    mixed_inputs : list[tuple[bool, bool]]
        Mixed (True, False) and (False, True) inputs
    """
    # test all False case
    assert bool(NANDGate(*all_false_inputs))
    
    # test mixed cases
    for a, b in mixed_inputs:
        assert bool(NANDGate(a, b))
    
    # test all True case
    assert not bool(NANDGate(*all_true_inputs))

def test_nand_repr() -> None:
    """Test NAND gate string representation."""
    assert repr(NANDGate(False, False)) == "NANDGate(a=False, b=False, output=True)"
    assert repr(NANDGate(True, False)) == "NANDGate(a=True, b=False, output=True)"


# --------------------------
# NOR Gate Tests
# --------------------------
def test_nor_normal_operations(
    all_false_inputs: tuple[bool, bool],
    all_true_inputs: tuple[bool, bool],
    mixed_inputs: list[tuple[bool, bool]]
) -> None:
    """Test NOR gate with normal input combinations.
    
    Parameters
    ----------
    all_false_inputs : tuple[bool, bool]
        (False, False) inputs
    all_true_inputs : tuple[bool, bool]
        (True, True) inputs
    mixed_inputs : list[tuple[bool, bool]]
        Mixed (True, False) and (False, True) inputs
    """
    # test all False case
    assert bool(NORGate(*all_false_inputs))
    
    # test mixed cases
    for a, b in mixed_inputs:
        assert not bool(NORGate(a, b))
    
    # test all True case
    assert not bool(NORGate(*all_true_inputs))

def test_nor_repr() -> None:
    """Test NOR gate string representation."""
    assert repr(NORGate(False, False)) == "NORGate(a=False, b=False, output=True)"
    assert repr(NORGate(True, True)) == "NORGate(a=True, b=True, output=False)"


# --------------------------
# XOR Gate Tests
# --------------------------
def test_xor_normal_operations(
    all_false_inputs: tuple[bool, bool],
    all_true_inputs: tuple[bool, bool],
    mixed_inputs: list[tuple[bool, bool]]
) -> None:
    """Test XOR gate with normal input combinations.
    
    Parameters
    ----------
    all_false_inputs : tuple[bool, bool]
        (False, False) inputs
    all_true_inputs : tuple[bool, bool]
        (True, True) inputs
    mixed_inputs : list[tuple[bool, bool]]
        Mixed (True, False) and (False, True) inputs
    """
    # test all False case
    assert not bool(XORGate(*all_false_inputs))
    
    # test mixed cases
    for a, b in mixed_inputs:
        assert bool(XORGate(a, b))
    
    # test all True case
    assert not bool(XORGate(*all_true_inputs))

def test_xor_repr() -> None:
    """Test XOR gate string representation."""
    assert repr(XORGate(False, True)) == "XORGate(a=False, b=True, output=True)"
    assert repr(XORGate(True, True)) == "XORGate(a=True, b=True, output=False)"


# --------------------------
# Edge Case Tests
# --------------------------
def test_edge_case_all_false(all_false_inputs: tuple[bool, bool]) -> None:
    """Test all gates with all False inputs.
    
    Parameters
    ----------
    all_false_inputs : tuple[bool, bool]
        (False, False) inputs
    """
    assert bool(NANDGate(*all_false_inputs))
    assert bool(NORGate(*all_false_inputs))
    assert not bool(XORGate(*all_false_inputs))

def test_edge_case_all_true(all_true_inputs: tuple[bool, bool]) -> None:
    """Test all gates with all True inputs.
    
    Parameters
    ----------
    all_true_inputs : tuple[bool, bool]
        (True, True) inputs
    """
    assert not bool(NANDGate(*all_true_inputs))
    assert not bool(NORGate(*all_true_inputs))
    assert not bool(XORGate(*all_true_inputs))


# --------------------------
# Type Validation Tests
# --------------------------
@pytest.mark.parametrize("a,b", [
    (1, True),       # first input not bool
    (False, "True"), # second input not bool
    (1, "True")      # both inputs wrong type
])
def test_type_validation_nand(a: object, b: object) -> None:
    """Test NAND gate type validation.
    
    Parameters
    ----------
    a : object
        First input to the gate
    b : object
        Second input to the gate
    """
    with pytest.raises(TypeError):
        NANDGate(a, b)

@pytest.mark.parametrize("a,b", [
    (None, False),  # first input not bool
    (True, 0)       # second input not bool
])
def test_type_validation_nor(a: object, b: object) -> None:
    """Test NOR gate type validation.
    
    Parameters
    ----------
    a : object
        First input to the gate
    b : object
        Second input to the gate
    """
    with pytest.raises(TypeError):
        NORGate(a, b)

@pytest.mark.parametrize("a,b", [
    (True, 1.0),   # second input not bool
    ([], False)    # first input not bool
])
def test_type_validation_xor(a: object, b: object) -> None:
    """Test XOR gate type validation.
    
    Parameters
    ----------
    a : object
        First input to the gate
    b : object
        Second input to the gate
    """
    with pytest.raises(TypeError):
        XORGate(a, b)


# --------------------------
# Boolean Conversion Tests
# --------------------------
def test_boolean_conversion(
    all_false_inputs: tuple[bool, bool],
    all_true_inputs: tuple[bool, bool],
    mixed_inputs: list[tuple[bool, bool]]
) -> None:
    """Test that gates work properly in boolean contexts.
    
    Parameters
    ----------
    all_false_inputs : tuple[bool, bool]
        (False, False) inputs
    all_true_inputs : tuple[bool, bool]
        (True, True) inputs
    mixed_inputs : list[tuple[bool, bool]]
        Mixed (True, False) and (False, True) inputs
    """
    # NAND Gate tests
    assert NANDGate(*all_false_inputs)
    assert not NANDGate(*all_true_inputs)
    
    # NOR Gate tests
    assert NORGate(*all_false_inputs)
    assert not NORGate(mixed_inputs[0][0], mixed_inputs[0][1])
    
    # XOR Gate tests
    assert XORGate(*mixed_inputs[0])
    assert not XORGate(*all_false_inputs)


# --------------------------
# Input Preservation Tests
# --------------------------
def test_input_preservation() -> None:
    """Test that inputs are stored correctly and not modified."""
    # NAND Gate
    nand = NANDGate(True, False)
    assert nand.a is True
    assert nand.b is False
    
    # NOR Gate
    nor = NORGate(False, True)
    assert nor.a is False
    assert nor.b is True
    
    # XOR Gate
    xor = XORGate(True, True)
    assert xor.a is True
    assert xor.b is True