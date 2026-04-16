"""Expression conversion utilities between infix, postfix and prefix notations.

This module provides a class for converting mathematical expressions between
different notation types (infix, postfix, prefix) using stack operations.
"""

import re
from typing import Literal

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.dsa.stacks.simple_stack import Stack


class ExpressionConverter(metaclass=TypeChecker):
	"""Class for converting mathematical expressions between notation types.

	Parameters
	----------
	str_expr : str
		The expression to convert
	str_from_type : Literal['infix', 'postfix', 'prefix']
		Source notation type
	str_to_type : Literal['infix', 'postfix', 'prefix']
		Target notation type

	Attributes
	----------
	prec : dict
		Operator precedence mapping
	postfix_list : list
		Temporary storage for postfix conversion
	token_list : list
		Tokenized input expression
	str_operands : str
		Valid operand characters
	str_operators : str
		Valid operator characters
	"""

	def _tokenize_expression(self, expr: str) -> list[str]:
		"""Tokenize the expression into individual components.

		Parameters
		----------
		expr : str
			Expression to tokenize

		Returns
		-------
		List[str]
			List of tokens (operands, operators, parentheses)
		"""
		# remove extra spaces and convert to uppercase
		expr = re.sub(r"\s+", " ", expr.strip().upper())

		# use regex to split on operators and parentheses while keeping them
		tokens = re.findall(r"[A-Z0-9]+|[+\-*/()]", expr)

		return [token for token in tokens if token]

	def _validate_expression(self, expr: str) -> None:
		"""Validate the input expression.

		Parameters
		----------
		expr : str
			Expression to validate

		Raises
		------
		ValueError
			If expression is empty
			If expression contains invalid characters
			If expression is malformed
		"""
		# check if expression is empty or only whitespace
		if not expr or not expr.strip():
			raise ValueError("Expression cannot be empty")

		# tokenize to check for valid components
		tokens = self._tokenize_expression(expr)
		if not tokens:
			raise ValueError("Expression cannot be empty")

		# check for invalid characters
		if not all(char in self.str_operands + self.str_operators + " " for char in expr.upper()):
			raise ValueError("Expression contains invalid characters")

		# check balanced parentheses
		if expr.count("(") != expr.count(")"):
			raise ValueError("Unbalanced parentheses in expression")

		# check for expressions with only operators (no operands)  # noqa: ERA001
		operand_count = sum(1 for token in tokens if token in self.str_operands)
		operator_count = sum(1 for token in tokens if token in "+-*/")  # noqa S105

		if operand_count == 0 and operator_count > 0:
			raise ValueError("Expression contains only operators")

	def __init__(
		self,
		str_expr: str,
		str_from_type: Literal["infix", "postfix", "prefix"],
		str_to_type: Literal["infix", "postfix", "prefix"],
	) -> None:
		"""Initialize the expression converter with input parameters.

		Parameters
		----------
		str_expr : str
			The expression to convert
		str_from_type : Literal['infix', 'postfix', 'prefix']
			Source notation type
		str_to_type : Literal['infix', 'postfix', 'prefix']
			Target notation type

		Returns
		-------
		None
		"""
		self.str_expr = str_expr
		self.str_from_type = str_from_type
		self.str_to_type = str_to_type
		self.prec = {"*": 3, "/": 3, "+": 2, "-": 2, "(": 1}
		self.postfix_list = []
		self.str_operands = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		self.str_operators = "+-*/()"

		self._validate_expression(str_expr)
		self.token_list = self._tokenize_expression(str_expr)

	def _infix_to_postfix(self) -> str:
		"""Convert infix notation to postfix notation.

		Returns
		-------
		str
			Expression in postfix notation

		Notes
		-----
		Uses the Shunting-yard algorithm for conversion.
		"""
		cls_stack = Stack()
		self.postfix_list = []

		for token in self.token_list:
			if token in self.str_operands:
				self.postfix_list.append(token)
			elif token == "(":  # noqa S105: possible hardcoded password
				cls_stack.push(token)
			elif token == ")":  # noqa S105: possible hardcoded password
				top_token = cls_stack.pop()
				while top_token != "(":  # noqa S105: possible hardcoded password
					self.postfix_list.append(top_token)
					top_token = cls_stack.pop()
			else:
				while (not cls_stack.is_empty) and (self.prec[cls_stack.peek] >= self.prec[token]):
					self.postfix_list.append(cls_stack.pop())
				cls_stack.push(token)

		while not cls_stack.is_empty:
			self.postfix_list.append(cls_stack.pop())
		return " ".join(self.postfix_list)

	def _infix_to_prefix(self) -> str:
		"""Convert infix notation to prefix notation.

		Returns
		-------
		str
			Expression in prefix notation
		"""
		postfix_expr = self._infix_to_postfix()
		cls_stack = Stack()
		for token in postfix_expr.split(" "):
			if token in self.str_operands:
				cls_stack.push(token)
			else:
				right = cls_stack.pop()
				left = cls_stack.pop()
				expr = f"{token} {left} {right}"
				cls_stack.push(expr)
		return cls_stack.pop()

	def _postfix_to_infix(self) -> str:
		"""Convert postfix notation to infix notation.

		Returns
		-------
		str
			Expression in infix notation
		"""
		cls_stack = Stack()
		for token in self.token_list:
			if token in self.str_operands:
				cls_stack.push(token)
			else:
				right = cls_stack.pop()
				left = cls_stack.pop()
				expr = f"({left} {token} {right})"
				cls_stack.push(expr)
		return cls_stack.pop()

	def _postfix_to_prefix(self) -> str:
		"""Convert postfix notation to prefix notation.

		Returns
		-------
		str
			Expression in prefix notation
		"""
		cls_stack = Stack()
		for token in self.token_list:
			if token in self.str_operands:
				cls_stack.push(token)
			else:
				right = cls_stack.pop()
				left = cls_stack.pop()
				expr = f"{token} {left} {right}"
				cls_stack.push(expr)
		return cls_stack.pop()

	def _prefix_to_infix(self) -> str:
		"""Convert prefix notation to infix notation.

		Returns
		-------
		str
			Expression in infix notation
		"""
		cls_stack = Stack()
		for token in reversed(self.token_list):
			if token in self.str_operands:
				cls_stack.push(token)
			else:
				left = cls_stack.pop()
				right = cls_stack.pop()
				expr = f"({left} {token} {right})"
				cls_stack.push(expr)
		return cls_stack.pop()

	def _prefix_to_postfix(self) -> str:
		"""Convert prefix notation to postfix notation.

		Returns
		-------
		str
			Expression in postfix notation
		"""
		cls_stack = Stack()
		for token in reversed(self.token_list):
			if token in self.str_operands:
				cls_stack.push(token)
			else:
				left = cls_stack.pop()
				right = cls_stack.pop()
				expr = f"{left} {right} {token}"
				cls_stack.push(expr)
		return cls_stack.pop()

	def convert(self) -> str:
		"""Convert expression between notation types.

		Returns
		-------
		str
			Converted expression

		Raises
		------
		ValueError
			If conversion type combination is invalid
		"""
		conversion_map = {
			("infix", "postfix"): self._infix_to_postfix,
			("infix", "prefix"): self._infix_to_prefix,
			("postfix", "infix"): self._postfix_to_infix,
			("postfix", "prefix"): self._postfix_to_prefix,
			("prefix", "infix"): self._prefix_to_infix,
			("prefix", "postfix"): self._prefix_to_postfix,
		}
		key = (self.str_from_type, self.str_to_type)
		if key in conversion_map:
			return conversion_map[key]()
		raise ValueError(f"Invalid conversion from {self.str_from_type} to {self.str_to_type}")
