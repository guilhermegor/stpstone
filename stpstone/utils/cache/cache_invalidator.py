"""Cache invalidation utilities based on external conditions.

This module provides a class for invalidating caches using configurable clearing methods
with optional conditional execution based on external triggers.
"""

from typing import Any, Callable, Optional

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class CacheInvalidator(metaclass=TypeChecker):
	"""Utility to invalidate caches based on external conditions.

	This class provides a mechanism to invalidate caches by calling a specified
	cache-clearing method on an instance, optionally conditioned on an external
	condition function.
	"""

	def __init__(self, cache_clear_method: str) -> None:
		"""Initialize CacheInvalidator with specified cache clearing method.

		Parameters
		----------
		cache_clear_method : str
			Name of the method to call for cache clearing on target instances

		Returns
		-------
		None
		"""
		self._validate_cache_clear_method(cache_clear_method)
		self.cache_clear_method = cache_clear_method

	def invalidate(
		self,
		instance: Any,  # noqa ANN401: typing.Any is not allowed
		condition: Optional[Callable[[], bool]] = None,
	) -> None:
		"""Invalidate cache if condition is met or unconditionally.

		Parameters
		----------
		instance : Any
			Object instance containing the cache clearing method
		condition : Optional[Callable[[], bool]]
			Optional condition function that must return True to proceed with invalidation.
			If None, invalidation occurs unconditionally.

		Returns
		-------
		None

		Raises
		------
		AttributeError
			If the specified cache clearing method is not found or not callable on the instance
		ValueError
			If instance is None or condition function returns non-boolean value
		"""
		if instance is None:
			raise ValueError("Instance cannot be None")
		self._validate_condition(condition)

		if condition is None or condition():
			if condition is not None and not isinstance(condition(), bool):
				raise ValueError("Condition function must return a boolean")
			clear_method = getattr(instance, self.cache_clear_method, None)
			if callable(clear_method):
				clear_method()
			else:
				raise AttributeError(
					f"Cache-clearing method '{self.cache_clear_method}' not found or not callable"
				)

	def _validate_cache_clear_method(self, cache_clear_method: str) -> None:
		"""Validate cache clearing method parameter.

		Parameters
		----------
		cache_clear_method : str
			Method name to validate

		Returns
		-------
		None

		Raises
		------
		ValueError
			If cache_clear_method is empty
		"""
		if not cache_clear_method:
			raise ValueError("Cache clearing method cannot be empty")

	def _validate_condition(self, condition: Optional[Callable[[], bool]]) -> None:
		"""Validate condition function parameter.

		Parameters
		----------
		condition : Optional[Callable[[], bool]]
			Condition function to validate

		Returns
		-------
		None

		Raises
		------
		ValueError
			If condition is provided but not callable
		"""
		if condition is not None and not callable(condition):
			raise ValueError("Condition must be a callable function or None")
