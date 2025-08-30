"""Unit tests for BalanceBrackets class.

Tests the bracket balancing functionality with various input scenarios including:
- Valid bracket expressions
- Invalid bracket expressions
- Edge cases (empty strings, non-string inputs)
- Type validation
- Stack operations
"""

import importlib
import sys
from unittest.mock import PropertyMock, call

import pytest
from pytest_mock import MockerFixture

from stpstone.transformations.validation.balance_brackets import BalanceBrackets
from stpstone.utils.dsa.stacks.simple_stack import Stack


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def balance_brackets() -> BalanceBrackets:
    """Provide a BalanceBrackets instance for testing.

    Returns
    -------
    BalanceBrackets
        Initialized BalanceBrackets instance
    """
    return BalanceBrackets()


@pytest.fixture
def mock_stack(mocker: MockerFixture) -> Stack:
    """Provide a mocked Stack instance for testing.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    Stack
        Mocked Stack instance
    """
    return mocker.Mock(spec=Stack)


# --------------------------
# Tests for _validate_expression
# --------------------------
def test_validate_expression_empty_string(balance_brackets: BalanceBrackets) -> None:
    """Test that empty string raises ValueError.

    Verifies
    --------
    - Empty string input raises ValueError with correct message

    Parameters
    ----------
    balance_brackets : BalanceBrackets
        BalanceBrackets instance from fixture

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Expression cannot be empty"):
        balance_brackets._validate_expression("")


def test_validate_expression_non_string(balance_brackets: BalanceBrackets) -> None:
    """Test that non-string input raises ValueError.

    Verifies
    --------
    - Non-string input (e.g., int) raises ValueError with correct message

    Parameters
    ----------
    balance_brackets : BalanceBrackets
        BalanceBrackets instance from fixture

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        balance_brackets._validate_expression(123)


def test_validate_expression_valid_string(balance_brackets: BalanceBrackets) -> None:
    """Test that valid string input does not raise an error.

    Verifies
    --------
    - Valid string input passes without raising an exception

    Parameters
    ----------
    balance_brackets : BalanceBrackets
        BalanceBrackets instance from fixture

    Returns
    -------
    None
    """
    try:
        balance_brackets._validate_expression("({[]})")
    except ValueError:
        pytest.fail("Valid string raised unexpected ValueError")


# --------------------------
# Tests for is_balanced
# --------------------------
def test_is_balanced_valid_expression(balance_brackets: BalanceBrackets) -> None:
    """Test balanced bracket expressions return True.

    Verifies
    --------
    - Correctly balanced expressions like "({[]})" return True
    - Multiple valid cases are handled correctly

    Parameters
    ----------
    balance_brackets : BalanceBrackets
        BalanceBrackets instance from fixture

    Returns
    -------
    None

    Examples
    --------
    >>> balancer = BalanceBrackets()
    >>> balancer.is_balanced("({[]})")
    True
    """
    assert balance_brackets.is_balanced("({[]})") is True
    assert balance_brackets.is_balanced("()") is True
    assert balance_brackets.is_balanced("[]") is True
    assert balance_brackets.is_balanced("{}") is True
    assert balance_brackets.is_balanced("({[]})") is True


def test_is_balanced_unmatched_brackets(balance_brackets: BalanceBrackets) -> None:
    """Test unmatched bracket expressions return False.

    Verifies
    --------
    - Unmatched bracket expressions like "({[})" return False
    - Incorrect closing brackets are detected

    Parameters
    ----------
    balance_brackets : BalanceBrackets
        BalanceBrackets instance from fixture

    Returns
    -------
    None

    Examples
    --------
    >>> balancer = BalanceBrackets()
    >>> balancer.is_balanced("({[})")
    False
    """
    assert balance_brackets.is_balanced("({[})") is False
    assert balance_brackets.is_balanced("(]") is False
    assert balance_brackets.is_balanced("{)") is False


def test_is_balanced_extra_closing_brackets(balance_brackets: BalanceBrackets) -> None:
    """Test expressions with extra closing brackets return False.

    Verifies
    --------
    - Expressions with unmatched closing brackets like "())" return False

    Parameters
    ----------
    balance_brackets : BalanceBrackets
        BalanceBrackets instance from fixture

    Returns
    -------
    None
    """
    assert balance_brackets.is_balanced("())") is False
    assert balance_brackets.is_balanced("]]") is False
    assert balance_brackets.is_balanced("}}") is False


def test_is_balanced_extra_opening_brackets(balance_brackets: BalanceBrackets) -> None:
    """Test expressions with extra opening brackets return False.

    Verifies
    --------
    - Expressions with unmatched opening brackets like "(()" return False

    Parameters
    ----------
    balance_brackets : BalanceBrackets
        BalanceBrackets instance from fixture

    Returns
    -------
    None
    """
    assert balance_brackets.is_balanced("(()") is False
    assert balance_brackets.is_balanced("[[") is False
    assert balance_brackets.is_balanced("{{") is False


def test_is_balanced_empty_string_raises_error(balance_brackets: BalanceBrackets) -> None:
    """Test that empty string input raises ValueError.

    Verifies
    --------
    - Empty string input to is_balanced raises ValueError via _validate_expression

    Parameters
    ----------
    balance_brackets : BalanceBrackets
        BalanceBrackets instance from fixture

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Expression cannot be empty"):
        balance_brackets.is_balanced("")


def test_is_balanced_non_string_raises_error(balance_brackets: BalanceBrackets) -> None:
    """Test that non-string input raises ValueError.

    Verifies
    --------
    - Non-string input (e.g., int) to is_balanced raises ValueError via _validate_expression

    Parameters
    ----------
    balance_brackets : BalanceBrackets
        BalanceBrackets instance from fixture

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        balance_brackets.is_balanced(123)


def test_is_balanced_non_bracket_characters(balance_brackets: BalanceBrackets) -> None:
    """Test expressions with non-bracket characters.

    Verifies
    --------
    - Non-bracket characters are ignored and balanced brackets return True
    - Unbalanced brackets with non-bracket characters return False

    Parameters
    ----------
    balance_brackets : BalanceBrackets
        BalanceBrackets instance from fixture

    Returns
    -------
    None
    """
    assert balance_brackets.is_balanced("abc()def") is True
    assert balance_brackets.is_balanced("abc(def") is False
    assert balance_brackets.is_balanced("abc)def") is False


def test_is_balanced_unicode_characters(balance_brackets: BalanceBrackets) -> None:
    """Test expressions with Unicode characters.

    Verifies
    --------
    - Unicode characters are ignored and balanced brackets return True
    - Unbalanced brackets with Unicode characters return False

    Parameters
    ----------
    balance_brackets : BalanceBrackets
        BalanceBrackets instance from fixture

    Returns
    -------
    None
    """
    assert balance_brackets.is_balanced("😊()😊") is True
    assert balance_brackets.is_balanced("😊(😊") is False
    assert balance_brackets.is_balanced("😊)😊") is False


def test_is_balanced_nested_brackets(balance_brackets: BalanceBrackets) -> None:
    """Test deeply nested bracket expressions.

    Verifies
    --------
    - Deeply nested valid bracket expressions return True
    - Incorrectly nested brackets return False

    Parameters
    ----------
    balance_brackets : BalanceBrackets
        BalanceBrackets instance from fixture

    Returns
    -------
    None
    """
    assert balance_brackets.is_balanced("((({{{[[[]]]}}})))") is True
    assert balance_brackets.is_balanced("((({{{[[[}]]]}}})))") is False


# --------------------------
# Tests with Mocked Stack
# --------------------------
def test_is_balanced_stack_operations(
    balance_brackets: BalanceBrackets, mock_stack: Stack, mocker: MockerFixture
) -> None:
    """Test stack operations during bracket balancing.

    Verifies
    --------
    - Correct push and pop operations on the stack for a valid expression
    - Stack is empty at the end for balanced expression

    Parameters
    ----------
    balance_brackets : BalanceBrackets
        BalanceBrackets instance from fixture
    mock_stack : Stack
        Mocked Stack instance from fixture
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_stack.push.return_value = None
    mock_stack.pop.return_value = "("
    
    # Criar o PropertyMock explicitamente
    is_empty_mock = PropertyMock(side_effect=[False, True])
    type(mock_stack).is_empty = is_empty_mock
    
    mocker.patch(
        "stpstone.transformations.validation.balance_brackets.Stack",
        return_value=mock_stack,
    )

    balance_brackets = BalanceBrackets()
    result = balance_brackets.is_balanced("()")

    assert result is True
    mock_stack.push.assert_called_once_with("(")
    mock_stack.pop.assert_called_once()
    assert is_empty_mock.call_count == 2

def test_is_balanced_stack_empty_on_closing(
    balance_brackets: BalanceBrackets, mock_stack: Stack, mocker: MockerFixture
) -> None:
    """Test stack empty condition on closing bracket.

    Verifies
    --------
    - Returns False when stack is empty on encountering a closing bracket

    Parameters
    ----------
    balance_brackets : BalanceBrackets
        BalanceBrackets instance from fixture
    mock_stack : Stack
        Mocked Stack instance from fixture
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_stack.push.return_value = None
    mock_stack.pop.return_value = None

    is_empty_mock = PropertyMock(return_value=True)
    type(mock_stack).is_empty = is_empty_mock

    mocker.patch(
        "stpstone.transformations.validation.balance_brackets.Stack",
        return_value=mock_stack,
    )

    balance_brackets = BalanceBrackets()
    result = balance_brackets.is_balanced(")")

    assert result is False
    mock_stack.push.assert_not_called()
    mock_stack.pop.assert_not_called()
    is_empty_mock.assert_called_once()


def test_is_balanced_stack_mismatch(
    balance_brackets: BalanceBrackets, mock_stack: Stack, mocker: MockerFixture
) -> None:
    """Test stack pop mismatch with closing bracket.

    Verifies
    --------
    - Returns False when popped bracket does not match closing bracket

    Parameters
    ----------
    balance_brackets : BalanceBrackets
        BalanceBrackets instance from fixture
    mock_stack : Stack
        Mocked Stack instance from fixture
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_stack.push.return_value = None
    mock_stack.pop.return_value = "["

    is_empty_mock = PropertyMock(return_value=False)
    type(mock_stack).is_empty = is_empty_mock

    mocker.patch(
        "stpstone.transformations.validation.balance_brackets.Stack",
        return_value=mock_stack,
    )

    balance_brackets = BalanceBrackets()
    result = balance_brackets.is_balanced("([)")

    assert result is False
    assert mock_stack.push.call_args_list == [call("("), call("[")]
    mock_stack.pop.assert_called_once()
    is_empty_mock.assert_called_once()


# --------------------------
# Reload Logic Tests
# --------------------------
def test_module_reload(mocker: MockerFixture) -> None:
    """Test module reload behavior.

    Verifies
    --------
    - Module can be reloaded without errors
    - BalanceBrackets class retains functionality after reload

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_stack = mocker.Mock(spec=Stack)
    mock_stack.is_empty = mocker.PropertyMock(side_effect=[True, False, True])
    mock_stack.push.side_effect = lambda x: None
    mock_stack.pop.side_effect = ["("]
    mocker.patch(
        "stpstone.transformations.validation.balance_brackets.Stack",
        return_value=mock_stack
    )

    _ = BalanceBrackets()
    importlib.reload(sys.modules["stpstone.transformations.validation.balance_brackets"])
    reloaded_instance = BalanceBrackets()
    assert reloaded_instance.is_balanced("()") is True
    assert reloaded_instance.is_balanced("(]") is False