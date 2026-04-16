"""Utilities for cache management in calendar operations.

This module provides decorators and utilities for managing cache operations in calendar-based
applications. It includes class-based decorators for enforcing cache clearing before method
execution and automatic application of cache reset behavior to specified methods.
"""

from functools import wraps
from typing import Any, Callable

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker, type_checker


class CacheResetDecorator(metaclass=TypeChecker):
	"""A decorator class to enforce calling a specified cache-clearing method before execution.

	This decorator ensures that specified cache-clearing methods are called before the decorated
	method executes, providing automatic cache invalidation for calendar operations.
	"""

	def __init__(self, cache_clear_method: str, force_refresh: bool = True) -> None:
		"""Initialize the decorator with the cache-clearing method name.

		Parameters
		----------
		cache_clear_method : str
			Name of the method to call for clearing the cache
		force_refresh : bool
			If True, clears cache on every call; if False, skips clearing, default=True

		Returns
		-------
		None
		"""
		self._validate_cache_clear_method(cache_clear_method)
		self.cache_clear_method = cache_clear_method
		self.force_refresh = force_refresh

	def _validate_cache_clear_method(self, cache_clear_method: str) -> None:
		"""Validate cache clear method parameter.

		Parameters
		----------
		cache_clear_method : str
			Name of the cache clearing method to validate

		Raises
		------
		ValueError
			If cache_clear_method is empty or whitespace-only
		"""
		if not cache_clear_method.strip():
			raise ValueError("cache_clear_method cannot be empty")

	def __call__(self, method: Callable) -> Callable:
		"""Apply the decorator to a method.

		Parameters
		----------
		method : Callable
			Method to be decorated

		Returns
		-------
		Callable
			Wrapped method with cache reset logic

		Raises
		------
		AttributeError
			If the cache-clearing method is not found or not callable
		"""

		@type_checker
		@wraps(method)
		def wrapper(
			*args: Any,  # noqa ANN401: typing.Any is not allowed
			**kwargs: Any,  # noqa ANN401: typing.Any is not allowed
		) -> Any:  # noqa ANN401: typing.Any is not allowed
			"""Wrap cache reset logic.

			Parameters
			----------
			*args : Any
				Variable-length argument list
			**kwargs : Any
				Arbitrary keyword arguments

			Returns
			-------
			Any
				The result of the method call

			Raises
			------
			AttributeError
				If the cache-clearing method is not found or not callable
			"""
			instance = args[0] if args else None

			if self.force_refresh:
				try:
					clear_method = getattr(instance, self.cache_clear_method, None)
					if callable(clear_method):
						clear_method()
					else:
						raise AttributeError(
							f"Cache-clearing method '{self.cache_clear_method}' "
							"not found or not callable"
						)
				except Exception as err:
					raise AttributeError(
						"Failed to access cache-clearing method "
						f"'{self.cache_clear_method}': {str(err)}"
					) from err
			# Pass args and kwargs as-is, but ensure instance method binding
			return method.__get__(instance, type(instance))(*args[1:], **kwargs)

		return wrapper


@type_checker
def clear_multiple_caches(method: Callable, cache_clear_methods: list[str]) -> Callable:
	"""Clear multiple caches before executing a method.

	Parameters
	----------
	method : Callable
		Method to be decorated
	cache_clear_methods : list[str]
		List of cache clearing method names to call before the decorated method executes

	Returns
	-------
	Callable
		Wrapped method with cache reset logic

	Raises
	------
	AttributeError
		If any cache-clearing method is not found or not callable
	"""
	_validate_cache_clear_methods(cache_clear_methods)

	@type_checker
	@wraps(method)
	def wrapper(
		*args: Any,  # noqa ANN401: typing.Any is not allowed
		**kwargs: Any,  # noqa ANN401: typing.Any is not allowed
	) -> Any:  # noqa ANN401: typing.Any is not allowed
		"""Clear multiple caches before method execution.

		Parameters
		----------
		*args : Any
			Variable-length argument list
		**kwargs : Any
			Arbitrary keyword arguments

		Returns
		-------
		Any
			The result of the method call

		Raises
		------
		AttributeError
			If any cache-clearing method is not found or not callable
		"""
		instance = args[0] if args else None
		for cache_clear_method in cache_clear_methods:
			try:
				clear_method = getattr(instance, cache_clear_method, None)
				if callable(clear_method):
					clear_method()
				else:
					raise AttributeError(
						f"Cache-clearing method '{cache_clear_method}' not found or not callable"
					)
			except Exception as err:
				raise AttributeError(
					f"Failed to access cache-clearing method '{cache_clear_method}': {str(err)}"
				) from err
		return method.__get__(instance, type(instance))(*args[1:], **kwargs)

	return wrapper


@type_checker
def _validate_cache_clear_methods(cache_clear_methods: list[str]) -> None:
	"""Validate cache clear methods list.

	Parameters
	----------
	cache_clear_methods : list[str]
		List of cache clearing method names to validate

	Raises
	------
	ValueError
		If cache_clear_methods is empty
		If any method name is empty or whitespace-only
	"""
	if not cache_clear_methods:
		raise ValueError("cache_clear_methods cannot be empty")

	for i, method_name in enumerate(cache_clear_methods):
		if not method_name.strip():
			raise ValueError(f"Method name at index {i} cannot be empty")


@type_checker
def auto_cache_reset_methods(
	method_cache_pairs: list[tuple[str, list[str]]],
) -> Callable[[type], type]:
	"""Class decorator to apply CacheResetDecorator to specified methods.

	This decorator applies CacheResetDecorator to specified methods in a class, with their
	respective cache-clearing methods. It provides automatic cache management for calendar
	operations by ensuring cache invalidation occurs before method execution.

	Parameters
	----------
	method_cache_pairs : list[tuple[str, list[str]]]
		List of tuples, each containing a method name and a list of cache-clearing
		method names to apply to that method

	Returns
	-------
	Callable[[type], type]
		The class decorator function

	Examples
	--------
	>>> method_cache_pairs = [
	...     ("holidays", ["clear_holidays_cache"]),
	...     ("holidays_in_year", ["clear_holidays_cache"]),
	...     ("working_days_range", ["clear_holidays_cache", "clear_working_days_cache"])
	... ]
	>>> @auto_cache_reset_methods(method_cache_pairs)
	... class CalendarManager:
	...     pass
	"""
	_validate_method_cache_pairs(method_cache_pairs)

	@type_checker
	def decorator(cls: type) -> type:
		"""Apply CacheResetDecorator or clear_multiple_caches to specified methods.

		Parameters
		----------
		cls : type
			The class to decorate

		Returns
		-------
		type
			The decorated class
		"""
		for method_name, cache_clear_methods in method_cache_pairs:
			if hasattr(cls, method_name):
				original_method = getattr(cls, method_name)
				if len(cache_clear_methods) == 1:
					decorated_method = CacheResetDecorator(cache_clear_methods[0])(original_method)
				else:
					decorated_method = clear_multiple_caches(original_method, cache_clear_methods)
				setattr(cls, method_name, decorated_method)
		return cls

	return decorator


@type_checker
def _validate_method_cache_pairs(method_cache_pairs: list[tuple[str, list[str]]]) -> None:
	"""Validate method cache pairs parameter.

	Parameters
	----------
	method_cache_pairs : list[tuple[str, list[str]]]
		List of method-cache pairs to validate

	Raises
	------
	ValueError
		If method_cache_pairs is empty
		If any pair has empty or whitespace-only method names or cache methods
	"""
	if not method_cache_pairs:
		raise ValueError("method_cache_pairs cannot be empty")

	for i, pair in enumerate(method_cache_pairs):
		method_name, cache_methods = pair

		if not method_name.strip():
			raise ValueError(f"Method name at index {i} cannot be empty")

		if not cache_methods:
			raise ValueError(f"Cache methods list at index {i} cannot be empty")

		for j, cache_method in enumerate(cache_methods):
			if not cache_method.strip():
				raise ValueError(f"Cache method at index {i}, {j} cannot be empty")
