"""Bracket balancing utilities using a stack-based approach.

This module provides a class for checking the balance of brackets in a given expression,
utilizing a stack data structure to ensure matching pairs of brackets.
"""

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.dsa.stacks.simple_stack import Stack


class BalanceBrackets(metaclass=TypeChecker):
	"""Check balance of brackets in expressions using a stack."""

	def _validate_expression(self, expression: str) -> None:
		"""Validate the input expression.

		Parameters
		----------
		expression : str
			The expression to validate

		Raises
		------
		ValueError
			If expression is empty
			If expression is not a string
		"""
		if not expression:
			raise ValueError("Expression cannot be empty")
		if not isinstance(expression, str):
			raise ValueError("Expression must be a string")

	def is_balanced(self, expression: str) -> bool:
		"""Check if brackets in the expression are balanced.

		Parameters
		----------
		expression : str
			The input string containing brackets to check

		Returns
		-------
		bool
			True if brackets are balanced, False otherwise

		Examples
		--------
		>>> balancer = BalanceBrackets()
		>>> balancer.is_balanced("({[]})")
		True
		>>> balancer.is_balanced("({[})")
		False
		"""
		self._validate_expression(expression)
		bracket_map: dict[str, str] = {")": "(", "}": "{", "]": "["}
		open_brackets: set[str] = set(bracket_map.values())
		cls_stack: Stack = Stack()
		for char in expression:
			if char in open_brackets:
				cls_stack.push(char)
			elif char in bracket_map and (
				cls_stack.is_empty or cls_stack.pop() != bracket_map[char]
			):
				return False
		return cls_stack.is_empty
