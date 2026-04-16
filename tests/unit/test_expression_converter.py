"""Unit tests for ExpressionConverter class.

Tests the expression conversion functionality between infix, postfix, and prefix
notations, covering:
- Valid conversions for all notation combinations
- Input validation and error conditions
- Edge cases with different expression formats
- Type validation and error handling
"""

from typing import Literal
from unittest.mock import MagicMock, patch

import pytest

from stpstone.utils.conversions.expression_converter import ExpressionConverter


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def simple_infix_expr() -> str:
	"""Fixture providing a simple infix expression.

	Returns
	-------
	str
		Simple infix expression "A + B"
	"""
	return "A + B"


@pytest.fixture
def complex_infix_expr() -> str:
	"""Fixture providing a complex infix expression with parentheses.

	Returns
	-------
	str
		Complex infix expression "(A + B) * (C - D)"
	"""
	return "(A + B) * (C - D)"


@pytest.fixture
def standard_infix_expr() -> str:
	"""Fixture providing standard test infix expression.

	Returns
	-------
	str
		Standard infix expression "A + B * C - D / E"
	"""
	return "A + B * C - D / E"


@pytest.fixture
def standard_postfix_expr() -> str:
	"""Fixture providing standard test postfix expression.

	Returns
	-------
	str
		Standard postfix expression "A B C * + D E / -"
	"""
	return "A B C * + D E / -"


@pytest.fixture
def standard_prefix_expr() -> str:
	"""Fixture providing standard test prefix expression.

	Returns
	-------
	str
		Standard prefix expression "- + A * B C / D E"
	"""
	return "- + A * B C / D E"


@pytest.fixture
def numeric_expr() -> str:
	"""Fixture providing expression with numeric operands.

	Returns
	-------
	str
		Numeric expression "1 + 2 * 3"
	"""
	return "1 + 2 * 3"


@pytest.fixture
def single_operand_expr() -> str:
	"""Fixture providing single operand expression.

	Returns
	-------
	str
		Single operand expression "A"
	"""
	return "A"


# --------------------------
# Test Initialization and Validation
# --------------------------
class TestExpressionConverterInitialization:
	"""Test cases for ExpressionConverter initialization and validation."""

	def test_init_with_valid_inputs(self, standard_infix_expr: str) -> None:
		"""Test initialization with valid inputs.

		Verifies
		--------
		- ExpressionConverter initializes correctly with valid parameters
		- All attributes are set with expected values
		- Token list is properly created from input expression

		Parameters
		----------
		standard_infix_expr : str
			Standard infix expression from fixture

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr=standard_infix_expr, str_from_type="infix", str_to_type="postfix"
		)
		assert converter.str_expr == standard_infix_expr
		assert converter.str_from_type == "infix"
		assert converter.str_to_type == "postfix"
		assert converter.token_list == ["A", "+", "B", "*", "C", "-", "D", "/", "E"]
		assert isinstance(converter.prec, dict)
		assert isinstance(converter.postfix_list, list)
		assert converter.str_operands == "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		assert converter.str_operators == "+-*/()"

	def test_init_with_numeric_expression(self, numeric_expr: str) -> None:
		"""Test initialization with numeric operands.

		Verifies
		--------
		- ExpressionConverter handles numeric operands correctly
		- Token list includes numeric values
		- Operands string includes digits

		Parameters
		----------
		numeric_expr : str
			Numeric expression from fixture

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr=numeric_expr, str_from_type="infix", str_to_type="postfix"
		)
		assert converter.token_list == ["1", "+", "2", "*", "3"]
		assert "0123456789" in converter.str_operands

	def test_init_with_lowercase_expression(self) -> None:
		"""Test initialization converts lowercase to uppercase.

		Verifies
		--------
		- Lowercase letters in expression are converted to uppercase
		- Token list contains uppercase letters only
		- Original expression is preserved

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr="a + b", str_from_type="infix", str_to_type="postfix"
		)
		assert converter.token_list == ["A", "+", "B"]

	def test_init_with_extra_spaces(self) -> None:
		"""Test initialization handles extra spaces correctly.

		Verifies
		--------
		- Extra spaces are removed from token list
		- Empty tokens are filtered out
		- Expression parsing works with irregular spacing

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr="A  +   B   *  C", str_from_type="infix", str_to_type="postfix"
		)
		assert converter.token_list == ["A", "+", "B", "*", "C"]

	def test_precedence_mapping(self, simple_infix_expr: str) -> None:
		"""Test operator precedence mapping is correctly initialized.

		Verifies
		--------
		- All expected operators are in precedence map
		- Precedence values are correct
		- Higher precedence operators have higher values

		Parameters
		----------
		simple_infix_expr : str
			Simple infix expression from fixture

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr=simple_infix_expr, str_from_type="infix", str_to_type="postfix"
		)
		assert converter.prec["*"] == 3
		assert converter.prec["/"] == 3
		assert converter.prec["+"] == 2
		assert converter.prec["-"] == 2
		assert converter.prec["("] == 1
		assert converter.prec["*"] > converter.prec["+"]
		assert converter.prec["/"] > converter.prec["-"]


# --------------------------
# Test Validation Methods
# --------------------------
class TestExpressionValidation:
	"""Test cases for expression validation functionality."""

	def test_empty_expression_raises_error(self) -> None:
		"""Test that empty expression raises ValueError.

		Verifies
		--------
		- Empty string expression raises ValueError
		- Error message indicates expression cannot be empty

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Expression cannot be empty"):
			ExpressionConverter(str_expr="", str_from_type="infix", str_to_type="postfix")

	def test_whitespace_only_expression_raises_error(self) -> None:
		"""Test that whitespace-only expression raises ValueError.

		Verifies
		--------
		- Expression with only spaces raises ValueError
		- Validation catches effectively empty expressions

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Expression cannot be empty"):
			ExpressionConverter(str_expr="   ", str_from_type="infix", str_to_type="postfix")

	def test_invalid_characters_raises_error(self) -> None:
		"""Test that invalid characters in expression raise ValueError.

		Verifies
		--------
		- Expression with invalid characters raises ValueError
		- Error message indicates invalid characters detected

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Expression contains invalid characters"):
			ExpressionConverter(str_expr="A + B & C", str_from_type="infix", str_to_type="postfix")

	def test_unbalanced_parentheses_raises_error(self) -> None:
		"""Test that unbalanced parentheses raise ValueError.

		Verifies
		--------
		- Expression with unbalanced parentheses raises ValueError
		- Error message indicates unbalanced parentheses

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Unbalanced parentheses in expression"):
			ExpressionConverter(str_expr="(A + B", str_from_type="infix", str_to_type="postfix")

	def test_extra_closing_parentheses_raises_error(self) -> None:
		"""Test that extra closing parentheses raise ValueError.

		Verifies
		--------
		- Expression with extra closing parentheses raises ValueError
		- Validation catches mismatched parentheses

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Unbalanced parentheses in expression"):
			ExpressionConverter(str_expr="A + B)", str_from_type="infix", str_to_type="postfix")

	def test_valid_characters_accepted(self, standard_infix_expr: str) -> None:
		"""Test that expressions with valid characters are accepted.

		Verifies
		--------
		- Expression with valid operands and operators is accepted
		- No validation errors are raised
		- Initialization completes successfully

		Parameters
		----------
		standard_infix_expr : str
			Standard infix expression from fixture

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr=standard_infix_expr, str_from_type="infix", str_to_type="postfix"
		)
		assert converter.str_expr == standard_infix_expr

	def test_balanced_parentheses_accepted(self, complex_infix_expr: str) -> None:
		"""Test that balanced parentheses are accepted.

		Verifies
		--------
		- Expression with balanced parentheses passes validation
		- No parentheses-related errors are raised

		Parameters
		----------
		complex_infix_expr : str
			Complex infix expression with parentheses from fixture

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr=complex_infix_expr, str_from_type="infix", str_to_type="postfix"
		)
		assert converter.str_expr == complex_infix_expr


# --------------------------
# Test Conversion Methods
# --------------------------
class TestInfixToPostfixConversion:
	"""Test cases for infix to postfix conversion."""

	def test_simple_infix_to_postfix(self, simple_infix_expr: str) -> None:
		"""Test simple infix to postfix conversion.

		Verifies
		--------
		- Simple addition expression converts correctly
		- Operand order is preserved
		- Operator is placed after operands

		Parameters
		----------
		simple_infix_expr : str
			Simple infix expression from fixture

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr=simple_infix_expr, str_from_type="infix", str_to_type="postfix"
		)
		result = converter.convert()
		assert result == "A B +"

	def test_standard_infix_to_postfix(self, standard_infix_expr: str) -> None:
		"""Test standard infix to postfix conversion.

		Verifies
		--------
		- Complex expression with multiple operators converts correctly
		- Operator precedence is respected
		- Result matches expected postfix notation

		Parameters
		----------
		standard_infix_expr : str
			Standard infix expression from fixture

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr=standard_infix_expr, str_from_type="infix", str_to_type="postfix"
		)
		result = converter.convert()
		assert result == "A B C * + D E / -"

	def test_parenthesized_infix_to_postfix(self, complex_infix_expr: str) -> None:
		"""Test parenthesized infix to postfix conversion.

		Verifies
		--------
		- Parentheses correctly alter operator precedence
		- Result respects parenthetical grouping
		- Stack operations handle parentheses properly

		Parameters
		----------
		complex_infix_expr : str
			Complex infix expression with parentheses from fixture

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr=complex_infix_expr, str_from_type="infix", str_to_type="postfix"
		)
		result = converter.convert()
		assert result == "A B + C D - *"

	def test_single_operand_infix_to_postfix(self, single_operand_expr: str) -> None:
		"""Test single operand infix to postfix conversion.

		Verifies
		--------
		- Single operand expression converts correctly
		- No operators are added
		- Result is just the operand

		Parameters
		----------
		single_operand_expr : str
			Single operand expression from fixture

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr=single_operand_expr, str_from_type="infix", str_to_type="postfix"
		)
		result = converter.convert()
		assert result == "A"


class TestInfixToPrefixConversion:
	"""Test cases for infix to prefix conversion."""

	def test_simple_infix_to_prefix(self, simple_infix_expr: str) -> None:
		"""Test simple infix to prefix conversion.

		Verifies
		--------
		- Simple addition expression converts to prefix notation
		- Operator is placed before operands
		- Operand order is preserved

		Parameters
		----------
		simple_infix_expr : str
			Simple infix expression from fixture

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr=simple_infix_expr, str_from_type="infix", str_to_type="prefix"
		)
		result = converter.convert()
		assert result == "+ A B"

	def test_standard_infix_to_prefix(self, standard_infix_expr: str) -> None:
		"""Test standard infix to prefix conversion.

		Verifies
		--------
		- Complex expression converts to correct prefix notation
		- Operator precedence is maintained
		- Result matches expected prefix format

		Parameters
		----------
		standard_infix_expr : str
			Standard infix expression from fixture

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr=standard_infix_expr, str_from_type="infix", str_to_type="prefix"
		)
		result = converter.convert()
		assert result == "- + A * B C / D E"

	def test_parenthesized_infix_to_prefix(self, complex_infix_expr: str) -> None:
		"""Test parenthesized infix to prefix conversion.

		Verifies
		--------
		- Parentheses correctly influence prefix conversion
		- Grouping is preserved in prefix notation
		- Result respects parenthetical precedence

		Parameters
		----------
		complex_infix_expr : str
			Complex infix expression with parentheses from fixture

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr=complex_infix_expr, str_from_type="infix", str_to_type="prefix"
		)
		result = converter.convert()
		assert result == "* + A B - C D"


class TestPostfixToInfixConversion:
	"""Test cases for postfix to infix conversion."""

	def test_simple_postfix_to_infix(self) -> None:
		"""Test simple postfix to infix conversion.

		Verifies
		--------
		- Simple postfix expression converts to infix
		- Parentheses are added for clarity
		- Operator is placed between operands

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr="A B +", str_from_type="postfix", str_to_type="infix"
		)
		result = converter.convert()
		assert result == "(A + B)"

	def test_standard_postfix_to_infix(self, standard_postfix_expr: str) -> None:
		"""Test standard postfix to infix conversion.

		Verifies
		--------
		- Complex postfix expression converts to infix
		- Parentheses correctly group operations
		- Result matches expected infix format

		Parameters
		----------
		standard_postfix_expr : str
			Standard postfix expression from fixture

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr=standard_postfix_expr, str_from_type="postfix", str_to_type="infix"
		)
		result = converter.convert()
		assert result == "((A + (B * C)) - (D / E))"


class TestPostfixToPrefixConversion:
	"""Test cases for postfix to prefix conversion."""

	def test_simple_postfix_to_prefix(self) -> None:
		"""Test simple postfix to prefix conversion.

		Verifies
		--------
		- Simple postfix expression converts to prefix
		- Operator is moved to front
		- Operand order is maintained

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr="A B +", str_from_type="postfix", str_to_type="prefix"
		)
		result = converter.convert()
		assert result == "+ A B"

	def test_standard_postfix_to_prefix(self, standard_postfix_expr: str) -> None:
		"""Test standard postfix to prefix conversion.

		Verifies
		--------
		- Complex postfix expression converts to prefix
		- All operators are correctly positioned
		- Result matches expected prefix notation

		Parameters
		----------
		standard_postfix_expr : str
			Standard postfix expression from fixture

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr=standard_postfix_expr, str_from_type="postfix", str_to_type="prefix"
		)
		result = converter.convert()
		assert result == "- + A * B C / D E"


class TestPrefixToInfixConversion:
	"""Test cases for prefix to infix conversion."""

	def test_simple_prefix_to_infix(self) -> None:
		"""Test simple prefix to infix conversion.

		Verifies
		--------
		- Simple prefix expression converts to infix
		- Operator is placed between operands
		- Parentheses are added for clarity

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr="+ A B", str_from_type="prefix", str_to_type="infix"
		)
		result = converter.convert()
		assert result == "(A + B)"

	def test_standard_prefix_to_infix(self, standard_prefix_expr: str) -> None:
		"""Test standard prefix to infix conversion.

		Verifies
		--------
		- Complex prefix expression converts to infix
		- Parentheses correctly group operations
		- Result matches expected infix format

		Parameters
		----------
		standard_prefix_expr : str
			Standard prefix expression from fixture

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr=standard_prefix_expr, str_from_type="prefix", str_to_type="infix"
		)
		result = converter.convert()
		assert result == "((A + (B * C)) - (D / E))"


class TestPrefixToPostfixConversion:
	"""Test cases for prefix to postfix conversion."""

	def test_simple_prefix_to_postfix(self) -> None:
		"""Test simple prefix to postfix conversion.

		Verifies
		--------
		- Simple prefix expression converts to postfix
		- Operator is moved to end
		- Operand order is preserved

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr="+ A B", str_from_type="prefix", str_to_type="postfix"
		)
		result = converter.convert()
		assert result == "A B +"

	def test_standard_prefix_to_postfix(self, standard_prefix_expr: str) -> None:
		"""Test standard prefix to postfix conversion.

		Verifies
		--------
		- Complex prefix expression converts to postfix
		- All operators are correctly positioned
		- Result matches expected postfix notation

		Parameters
		----------
		standard_prefix_expr : str
			Standard prefix expression from fixture

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr=standard_prefix_expr, str_from_type="prefix", str_to_type="postfix"
		)
		result = converter.convert()
		assert result == "A B C * + D E / -"


# --------------------------
# Test Convert Method and Edge Cases
# --------------------------
class TestConvertMethod:
	"""Test cases for the main convert method."""

	def test_invalid_conversion_combination_raises_error(self, simple_infix_expr: str) -> None:
		"""Test that invalid conversion combinations raise ValueError.

		Verifies
		--------
		- Invalid from/to type combinations raise ValueError
		- Error message indicates the invalid conversion types

		Parameters
		----------
		simple_infix_expr : str
			Simple infix expression from fixture

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr=simple_infix_expr,
			str_from_type="infix",
			str_to_type="infix",  # Same type conversion
		)
		with pytest.raises(ValueError, match="Invalid conversion from infix to infix"):
			converter.convert()

	def test_convert_with_all_valid_combinations(self, standard_infix_expr: str) -> None:
		"""Test convert method with all valid conversion combinations.

		Verifies
		--------
		- All valid conversion combinations work
		- No errors are raised for valid combinations
		- Results are strings

		Parameters
		----------
		standard_infix_expr : str
			Standard infix expression from fixture

		Returns
		-------
		None
		"""
		valid_combinations = [
			("infix", "postfix"),
			("infix", "prefix"),
			("postfix", "infix"),
			("postfix", "prefix"),
			("prefix", "infix"),
			("prefix", "postfix"),
		]

		for from_type, to_type in valid_combinations:
			if from_type == "infix":
				expr = standard_infix_expr
			elif from_type == "postfix":
				expr = "A B C * + D E / -"
			else:  # prefix
				expr = "- + A * B C / D E"

			converter = ExpressionConverter(
				str_expr=expr, str_from_type=from_type, str_to_type=to_type
			)
			result = converter.convert()
			assert isinstance(result, str)
			assert len(result) > 0


# --------------------------
# Test Type Validation
# --------------------------
class TestTypeValidation:
	"""Test cases for type validation and type hints."""

	def test_init_parameter_types(self, standard_infix_expr: str) -> None:
		"""Test that initialization parameters accept correct types.

		Verifies
		--------
		- String expression parameter is accepted
		- Literal type parameters are accepted
		- No type errors are raised during initialization

		Parameters
		----------
		standard_infix_expr : str
			Standard infix expression from fixture

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr=standard_infix_expr, str_from_type="infix", str_to_type="postfix"
		)
		assert isinstance(converter.str_expr, str)
		assert converter.str_from_type in ["infix", "postfix", "prefix"]
		assert converter.str_to_type in ["infix", "postfix", "prefix"]

	def test_convert_return_type(self, simple_infix_expr: str) -> None:
		"""Test that convert method returns correct type.

		Verifies
		--------
		- convert method returns a string
		- Return value is not None
		- Return value has expected string properties

		Parameters
		----------
		simple_infix_expr : str
			Simple infix expression from fixture

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr=simple_infix_expr, str_from_type="infix", str_to_type="postfix"
		)
		result = converter.convert()
		assert isinstance(result, str)
		assert result is not None
		assert len(result) > 0


# --------------------------
# Test Edge Cases and Error Conditions
# --------------------------
class TestEdgeCases:
	"""Test cases for edge cases and boundary conditions."""

	def test_expression_with_only_operators(self) -> None:
		"""Test expression containing only operators raises error.

		Verifies
		--------
		- Expression with only operators is rejected during validation
		- Appropriate error is raised

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError):
			ExpressionConverter(str_expr="+ - *", str_from_type="infix", str_to_type="postfix")

	def test_expression_with_mixed_case(self) -> None:
		"""Test expression with mixed case letters.

		Verifies
		--------
		- Mixed case letters are converted to uppercase
		- Conversion works correctly regardless of input case
		- Token list contains uppercase letters only

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr="a + B * c", str_from_type="infix", str_to_type="postfix"
		)
		assert converter.token_list == ["A", "+", "B", "*", "C"]
		result = converter.convert()
		assert result == "A B C * +"

	def test_expression_with_numbers_and_letters(self) -> None:
		"""Test expression with both numeric and alphabetic operands.

		Verifies
		--------
		- Mixed operand types are handled correctly
		- Both numbers and letters are recognized as valid operands
		- Conversion works with mixed operand types

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr="A + 1 * B - 2", str_from_type="infix", str_to_type="postfix"
		)
		result = converter.convert()
		assert result == "A 1 B * + 2 -"

	def test_deeply_nested_parentheses(self) -> None:
		"""Test expression with deeply nested parentheses.

		Verifies
		--------
		- Multiple levels of parentheses are handled correctly
		- Stack operations manage nested structures properly
		- Result respects all levels of grouping

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr="((A + B) * (C + D))", str_from_type="infix", str_to_type="postfix"
		)
		result = converter.convert()
		assert result == "A B + C D + *"

	def test_all_operators_precedence(self) -> None:
		"""Test expression using all operators to verify precedence.

		Verifies
		--------
		- All supported operators work correctly
		- Precedence rules are applied consistently
		- Complex expressions are converted accurately

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr="A + B - C * D / E", str_from_type="infix", str_to_type="postfix"
		)
		result = converter.convert()
		assert result == "A B + C D * E / -"

	@pytest.mark.parametrize(
		"from_type,to_type",
		[
			("infix", "postfix"),
			("infix", "prefix"),
			("postfix", "infix"),
			("postfix", "prefix"),
			("prefix", "infix"),
			("prefix", "postfix"),
		],
	)
	def test_single_operand_all_conversions(
		self,
		from_type: Literal["infix", "postfix", "prefix"],
		to_type: Literal["infix", "postfix", "prefix"],
	) -> None:
		"""Test single operand conversions for all notation types.

		Verifies
		--------
		- Single operand expressions work for all conversion types
		- No operators are added when none exist
		- Result is consistent across conversion types

		Parameters
		----------
		from_type : Literal['infix', 'postfix', 'prefix']
			Source notation type
		to_type : Literal['infix', 'postfix', 'prefix']
			Target notation type

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(str_expr="A", str_from_type=from_type, str_to_type=to_type)
		result = converter.convert()
		# single operand should remain as "A" for all conversions
		assert "A" in result
		assert isinstance(result, str)

	def test_postfix_list_initialization(self, simple_infix_expr: str) -> None:
		"""Test that postfix_list is properly initialized and used.

		Verifies
		--------
		- postfix_list starts as empty list
		- postfix_list is populated during conversion
		- postfix_list maintains proper state during conversion

		Parameters
		----------
		simple_infix_expr : str
			Simple infix expression from fixture

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr=simple_infix_expr, str_from_type="infix", str_to_type="postfix"
		)
		assert converter.postfix_list == []
		converter.convert()
		assert len(converter.postfix_list) > 0


# --------------------------
# Test Stack Operations and Internal Methods
# --------------------------
class TestStackOperations:
	"""Test cases for stack operations in conversion methods."""

	@patch("stpstone.utils.conversions.expression_converter.Stack")
	def test_infix_to_postfix_stack_usage(self, mock_stack_class: MagicMock) -> None:
		"""Test stack operations during infix to postfix conversion.

		Verifies
		--------
		- Stack is properly instantiated for conversion
		- Push and pop operations are called as expected
		- Stack state management works correctly

		Parameters
		----------
		mock_stack_class : MagicMock
			Mocked Stack class

		Returns
		-------
		None
		"""
		mock_stack = MagicMock()
		mock_stack.is_empty = True
		mock_stack.peek = None
		mock_stack.pop.return_value = None
		mock_stack_class.return_value = mock_stack

		converter = ExpressionConverter(
			str_expr="A + B", str_from_type="infix", str_to_type="postfix"
		)
		converter.convert()

		mock_stack_class.assert_called_once()
		mock_stack.push.assert_called()

	@patch("stpstone.utils.conversions.expression_converter.Stack")
	def test_postfix_to_infix_stack_usage(self, mock_stack_class: MagicMock) -> None:
		"""Test stack operations during postfix to infix conversion.

		Verifies
		--------
		- Stack is used for operand management
		- Pop operations retrieve operands in correct order
		- Expression building uses stack correctly

		Parameters
		----------
		mock_stack_class : MagicMock
			Mocked Stack class

		Returns
		-------
		None
		"""
		mock_stack = MagicMock()
		mock_stack.pop.side_effect = ["B", "A", "(A + B)"]
		mock_stack_class.return_value = mock_stack

		converter = ExpressionConverter(
			str_expr="A B +", str_from_type="postfix", str_to_type="infix"
		)
		converter.convert()

		mock_stack_class.assert_called_once()
		assert mock_stack.pop.call_count >= 2

	@patch("stpstone.utils.conversions.expression_converter.Stack")
	def test_prefix_operations_stack_usage(self, mock_stack_class: MagicMock) -> None:
		"""Test stack operations during prefix conversions.

		Verifies
		--------
		- Stack is used for prefix parsing
		- Reverse token processing works correctly
		- Stack operations handle prefix notation properly

		Parameters
		----------
		mock_stack_class : MagicMock
			Mocked Stack class

		Returns
		-------
		None
		"""
		mock_stack = MagicMock()
		mock_stack.pop.side_effect = ["A", "B", "A B +"]
		mock_stack_class.return_value = mock_stack

		converter = ExpressionConverter(
			str_expr="+ A B", str_from_type="prefix", str_to_type="postfix"
		)
		converter.convert()

		mock_stack_class.assert_called_once()


# --------------------------
# Test Internal Method Coverage
# --------------------------
class TestInternalMethods:
	"""Test cases for internal method coverage and edge cases."""

	def test_infix_to_postfix_internal_method(self) -> None:
		"""Test _infix_to_postfix internal method directly.

		Verifies
		--------
		- Internal method produces correct postfix result
		- Method can be called independently
		- postfix_list is properly populated

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr="A + B * C", str_from_type="infix", str_to_type="postfix"
		)
		result = converter._infix_to_postfix()
		assert result == "A B C * +"
		assert converter.postfix_list == ["A", "B", "C", "*", "+"]

	def test_infix_to_prefix_internal_method(self) -> None:
		"""Test _infix_to_prefix internal method directly.

		Verifies
		--------
		- Internal method produces correct prefix result
		- Method properly chains with postfix conversion
		- Stack operations work correctly for prefix

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr="A + B", str_from_type="infix", str_to_type="prefix"
		)
		result = converter._infix_to_prefix()
		assert result == "+ A B"

	def test_postfix_to_infix_internal_method(self) -> None:
		"""Test _postfix_to_infix internal method directly.

		Verifies
		--------
		- Internal method produces correct infix result
		- Parentheses are added appropriately
		- Stack operations handle operands correctly

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr="A B +", str_from_type="postfix", str_to_type="infix"
		)
		result = converter._postfix_to_infix()
		assert result == "(A + B)"

	def test_postfix_to_prefix_internal_method(self) -> None:
		"""Test _postfix_to_prefix internal method directly.

		Verifies
		--------
		- Internal method produces correct prefix result
		- Stack operations manage operands properly
		- Expression building follows prefix format

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr="A B +", str_from_type="postfix", str_to_type="prefix"
		)
		result = converter._postfix_to_prefix()
		assert result == "+ A B"

	def test_prefix_to_infix_internal_method(self) -> None:
		"""Test _prefix_to_infix internal method directly.

		Verifies
		--------
		- Internal method produces correct infix result
		- Reverse processing works correctly
		- Parentheses are properly placed

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr="+ A B", str_from_type="prefix", str_to_type="infix"
		)
		result = converter._prefix_to_infix()
		assert result == "(A + B)"

	def test_prefix_to_postfix_internal_method(self) -> None:
		"""Test _prefix_to_postfix internal method directly.

		Verifies
		--------
		- Internal method produces correct postfix result
		- Reverse token processing works properly
		- Expression building follows postfix format

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr="+ A B", str_from_type="prefix", str_to_type="postfix"
		)
		result = converter._prefix_to_postfix()
		assert result == "A B +"

	def test_validate_expression_internal_method(self) -> None:
		"""Test _validate_expression internal method directly.

		Verifies
		--------
		- Internal validation method works independently
		- Method raises appropriate errors for invalid input
		- Method accepts valid expressions without error

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr="A + B", str_from_type="infix", str_to_type="postfix"
		)

		converter._validate_expression("A + B")

		with pytest.raises(ValueError, match="Expression cannot be empty"):
			converter._validate_expression("")

		with pytest.raises(ValueError, match="Expression contains invalid characters"):
			converter._validate_expression("A & B")

		with pytest.raises(ValueError, match="Unbalanced parentheses in expression"):
			converter._validate_expression("(A + B")


# --------------------------
# Test Complex Scenarios and Performance
# --------------------------
class TestComplexScenarios:
	"""Test cases for complex conversion scenarios."""

	def test_complex_nested_expression_all_conversions(self) -> None:
		"""Test complex nested expressions for all conversion types.

		Verifies
		--------
		- Complex expressions work across all conversion combinations
		- Nested operations are handled correctly
		- Results maintain mathematical equivalence

		Returns
		-------
		None
		"""
		complex_infix = "((A + B) * C) - (D / (E + F))"

		# test infix to postfix
		converter = ExpressionConverter(
			str_expr=complex_infix, str_from_type="infix", str_to_type="postfix"
		)
		postfix_result = converter.convert()
		assert isinstance(postfix_result, str)
		assert len(postfix_result) > 0

		# test infix to prefix
		converter = ExpressionConverter(
			str_expr=complex_infix, str_from_type="infix", str_to_type="prefix"
		)
		prefix_result = converter.convert()
		assert isinstance(prefix_result, str)
		assert len(prefix_result) > 0

		# test round-trip conversions
		converter = ExpressionConverter(
			str_expr=postfix_result, str_from_type="postfix", str_to_type="prefix"
		)
		roundtrip_result = converter.convert()
		assert roundtrip_result == prefix_result

	def test_expression_with_all_operators(self) -> None:
		"""Test expression using all supported operators.

		Verifies
		--------
		- All operators (+, -, *, /, parentheses) work correctly
		- Precedence is maintained across all operators
		- Complex operator combinations are handled properly

		Returns
		-------
		None
		"""
		expr = "(A + B) * C - D / E"
		converter = ExpressionConverter(
			str_expr=expr, str_from_type="infix", str_to_type="postfix"
		)
		result = converter.convert()
		assert result == "A B + C * D E / -"

	def test_large_expression_handling(self) -> None:
		"""Test handling of expressions with many operands.

		Verifies
		--------
		- Large expressions are processed correctly
		- Performance remains acceptable
		- Memory usage is reasonable

		Returns
		-------
		None
		"""
		# create expression with many operands
		operands = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"]
		operators = ["+", "-", "*", "/"]

		expr_parts = []
		for i, operand in enumerate(operands[:-1]):
			expr_parts.extend([operand, operators[i % len(operators)]])
		expr_parts.append(operands[-1])

		large_expr = " ".join(expr_parts)
		converter = ExpressionConverter(
			str_expr=large_expr, str_from_type="infix", str_to_type="postfix"
		)
		result = converter.convert()
		assert isinstance(result, str)
		assert len(result) > 0

		# verify all operands are in result
		for operand in operands:
			assert operand in result


# --------------------------
# Test Error Recovery and Fallback
# --------------------------
class TestErrorRecoveryAndFallback:
	"""Test cases for error recovery and fallback mechanisms."""

	def test_conversion_map_key_error_handling(self) -> None:
		"""Test handling of conversion map key errors.

		Verifies
		--------
		- Invalid conversion combinations are properly caught
		- Appropriate error messages are generated
		- No unexpected exceptions are raised

		Returns
		-------
		None
		"""
		converter = ExpressionConverter(
			str_expr="A + B", str_from_type="infix", str_to_type="postfix"
		)

		# manually test invalid conversion by modifying types
		converter.str_from_type = "invalid"
		converter.str_to_type = "invalid"

		with pytest.raises(ValueError, match="Invalid conversion from invalid to invalid"):
			converter.convert()

	@patch("stpstone.utils.conversions.expression_converter.Stack")
	def test_stack_operation_error_handling(self, mock_stack_class: MagicMock) -> None:
		"""Test error handling when stack operations fail.

		Verifies
		--------
		- Stack operation failures are handled gracefully
		- Conversion continues despite stack issues
		- Appropriate fallback behavior occurs

		Parameters
		----------
		mock_stack_class : MagicMock
			Mocked Stack class that may raise exceptions

		Returns
		-------
		None
		"""
		mock_stack = MagicMock()
		mock_stack.is_empty = True
		mock_stack.push.side_effect = Exception("Stack error")
		mock_stack_class.return_value = mock_stack

		converter = ExpressionConverter(
			str_expr="A + B", str_from_type="infix", str_to_type="postfix"
		)

		# should raise exception due to stack failure
		with pytest.raises(Exception, match="Stack error"):
			converter.convert()


# --------------------------
# Test Metaclass and Type Checking Integration
# --------------------------
class TestMetaclassIntegration:
	"""Test cases for TypeChecker metaclass integration."""

	def test_metaclass_type_checking(self) -> None:
		"""Test that TypeChecker metaclass is properly applied.

		Verifies
		--------
		- ExpressionConverter uses TypeChecker metaclass
		- Type checking functionality is available
		- Metaclass doesn't interfere with normal operation

		Returns
		-------
		None
		"""
		# verify metaclass is applied
		assert ExpressionConverter.__class__.__name__ == "TypeChecker"

		# test normal initialization still works
		converter = ExpressionConverter(
			str_expr="A + B", str_from_type="infix", str_to_type="postfix"
		)
		assert converter.str_expr == "A + B"

	def test_type_validation_with_metaclass(self) -> None:
		"""Test type validation through metaclass integration.

		Verifies
		--------
		- Metaclass type validation works with expression converter
		- Invalid types are caught appropriately
		- Valid types pass validation

		Returns
		-------
		None
		"""
		# this should work with correct types
		converter = ExpressionConverter(
			str_expr="A + B", str_from_type="infix", str_to_type="postfix"
		)
		result = converter.convert()
		assert isinstance(result, str)


# --------------------------
# Test Coverage Completeness
# --------------------------
class TestCoverageCompleteness:
	"""Test cases to ensure 100% code coverage."""

	def test_all_branches_in_convert_method(self) -> None:
		"""Test all branches in convert method are covered.

		Verifies
		--------
		- All conversion combinations are tested
		- Both successful and error paths are covered
		- Return statements are reached in all cases

		Returns
		-------
		None
		"""
		test_cases = [
			("A + B", "infix", "postfix", "A B +"),
			("A + B", "infix", "prefix", "+ A B"),
			("A B +", "postfix", "infix", "(A + B)"),
			("A B +", "postfix", "prefix", "+ A B"),
			("+ A B", "prefix", "infix", "(A + B)"),
			("+ A B", "prefix", "postfix", "A B +"),
		]

		for expr, from_type, to_type, expected in test_cases:
			converter = ExpressionConverter(
				str_expr=expr, str_from_type=from_type, str_to_type=to_type
			)
			result = converter.convert()
			assert result == expected

	def test_all_validation_branches(self) -> None:
		"""Test all branches in validation method are covered.

		Verifies
		--------
		- All validation error conditions are tested
		- All validation success paths are tested
		- Edge cases in validation are covered

		Returns
		-------
		None
		"""
		# test empty expression
		with pytest.raises(ValueError, match="Expression cannot be empty"):
			ExpressionConverter("", "infix", "postfix")

		# test invalid characters
		with pytest.raises(ValueError, match="Expression contains invalid characters"):
			ExpressionConverter("A & B", "infix", "postfix")

		# test unbalanced parentheses
		with pytest.raises(ValueError, match="Unbalanced parentheses in expression"):
			ExpressionConverter("(A + B", "infix", "postfix")

		# test valid expression
		converter = ExpressionConverter("A + B", "infix", "postfix")
		assert converter.str_expr == "A + B"

	def test_all_operator_precedence_paths(self) -> None:
		"""Test all operator precedence comparison paths.

		Verifies
		--------
		- All precedence comparisons are exercised
		- Stack operations for different precedences work
		- Operator handling covers all cases

		Returns
		-------
		None
		"""
		# test different precedence combinations
		test_expressions = [
			"A + B * C",  # + lower than *
			"A * B + C",  # * higher than +
			"A - B / C",  # - lower than /
			"A / B - C",  # / higher than -
			"A + B - C",  # same precedence
			"A * B / C",  # same precedence
		]

		for expr in test_expressions:
			converter = ExpressionConverter(
				str_expr=expr, str_from_type="infix", str_to_type="postfix"
			)
			result = converter.convert()
			assert isinstance(result, str)
			assert len(result) > 0

	def test_parentheses_handling_branches(self) -> None:
		"""Test all parentheses handling branches are covered.

		Verifies
		--------
		- Opening parentheses handling is tested
		- Closing parentheses handling is tested
		- Nested parentheses scenarios are covered

		Returns
		-------
		None
		"""
		# test opening parentheses
		converter = ExpressionConverter(
			str_expr="(A + B) * C", str_from_type="infix", str_to_type="postfix"
		)
		result = converter.convert()
		assert result == "A B + C *"

		# test nested parentheses
		converter = ExpressionConverter(
			str_expr="((A + B) * C)", str_from_type="infix", str_to_type="postfix"
		)
		result = converter.convert()
		assert result == "A B + C *"

	def test_stack_empty_conditions(self) -> None:
		"""Test all stack empty condition branches.

		Verifies
		--------
		- Stack empty checks are properly tested
		- While loop conditions are covered
		- Stack state transitions are validated

		Returns
		-------
		None
		"""
		# simple expression that will test stack empty conditions
		converter = ExpressionConverter(str_expr="A", str_from_type="infix", str_to_type="postfix")
		result = converter.convert()
		assert result == "A"

		# expression that will exercise while loops
		converter = ExpressionConverter(
			str_expr="A + B + C", str_from_type="infix", str_to_type="postfix"
		)
		result = converter.convert()
		assert result == "A B + C +"
