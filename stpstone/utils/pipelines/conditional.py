"""Conditional function application pipeline.

This module provides a utility for applying functions to data conditionally based on
predicate evaluations. Useful for rule-based processing systems like fraud detection.
"""

from typing import Any, Callable

from stpstone.transformations.validation.metaclass_type_checker import type_checker


@type_checker
def conditional_pipeline(
	data: Any,  # noqa ANN401: typing.Any is not allowed
	functions: list[tuple[Callable[[Any], bool], Callable[[Any], Any]]],
) -> Any:  # noqa ANN401: typing.Any is not allowed
	"""Apply functions conditionally based on predicate evaluations.

	Parameters
	----------
	data : Any
		Initial input data to be processed
	functions : list[tuple[Callable[[Any], bool], Callable[[Any], Any]]]
		list of (predicate, function) tuples where:
		- predicate: Callable that returns bool when applied to data
		- function: Callable to apply if predicate returns True

	Returns
	-------
	Any
		Processed data after applying all applicable functions

	Raises
	------
	ValueError
		If any of the pipeline elements are not callable
	TypeError
		If any of the conditions do not return bool

	Examples
	--------
	>>> def is_even(x):
	...     return x % 2 == 0
	>>> def double(x):
	...     return x * 2
	>>> def triple(x):
	...     return x * 3
	>>> steps = [(is_even, double), (lambda x: x > 10, triple)]
	>>> result = conditional_pipeline(6, steps)
	>>> print(result)
	12
	"""
	if not functions:
		return data

	for condition, func in functions:
		if not callable(condition) or not callable(func):
			raise ValueError("All pipeline elements must be callable (condition, function) pairs")

		condition_result = condition(data)
		if not isinstance(condition_result, bool):
			raise TypeError(f"Condition must return bool, got {type(condition_result)}")

		if condition_result:
			data = func(data)

	return data
