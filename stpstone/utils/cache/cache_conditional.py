"""Conditional cache management utilities.

This module provides a decorator class for clearing cache based on specified conditions,
such as file modification times, with robust input validation and type checking.
"""

from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Optional

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker, type_checker


class ConditionalCacheReset(metaclass=TypeChecker):
	"""Decorator to clear cache if a condition (e.g., file modification) is met.

	Parameters
	----------
	cache_clear_method : str
		Name of the method to clear cache
	condition_file : Optional[str]
		Path to file whose modification triggers cache reset (default: None)

	Attributes
	----------
	last_modified : Optional[datetime]
		Last modification time of the condition file
	"""

	def __init__(self, cache_clear_method: str, condition_file: Optional[str] = None) -> None:
		"""Initialize the cache reset decorator.

		Parameters
		----------
		cache_clear_method : str
			Name of the method to clear cache
		condition_file : Optional[str]
			Path to file whose modification triggers cache reset (default: None)

		Returns
		-------
		None
		"""
		self._validate_inputs(cache_clear_method, condition_file)
		self.cache_clear_method = cache_clear_method
		self.condition_file = condition_file
		self.last_modified: Optional[datetime] = None

	def _validate_inputs(self, cache_clear_method: str, condition_file: Optional[str]) -> None:
		"""Validate initialization inputs.

		Parameters
		----------
		cache_clear_method : str
			Name of the method to clear cache
		condition_file : Optional[str]
			Path to file whose modification triggers cache reset

		Raises
		------
		ValueError
			If cache_clear_method is empty or whitespace-only
		TypeError
			If cache_clear_method is not a string or condition_file is not a string or None
		"""
		if not isinstance(cache_clear_method, str):
			raise TypeError(
				f"cache_clear_method must be of type str, got {type(cache_clear_method).__name__}"
			)
		if not cache_clear_method.strip():
			raise ValueError("cache_clear_method cannot be empty")
		if condition_file is not None and not isinstance(condition_file, str):
			raise TypeError(
				"condition_file must be one of types: "
				f"str, NoneType, got {type(condition_file).__name__}"
			)

	def __call__(self, method: Callable[..., Any]) -> Callable[..., Any]:
		"""Decorate a method to clear cache based on file modification.

		Parameters
		----------
		method : Callable[..., Any]
			Method to be decorated

		Returns
		-------
		Callable[..., Any]
			Wrapped method with cache reset logic
		"""

		@type_checker
		@wraps(method)
		def wrapper(
			instance: Any,  # noqa ANN401: typing.Any is not allowed
			*args: Any,  # noqa ANN401: typing.Any is not allowed
			**kwargs: Any,  # noqa ANN401: typing.Any is not allowed
		) -> Any:  # noqa ANN401: typing.Any is not allowed
			"""Wrap cache reset logic.

			Parameters
			----------
			instance : Any
				Instance of the decorated class
			*args : Any
				Variable-length argument list
			**kwargs : Any
				Arbitrary keyword arguments

			Returns
			-------
			Any
				The result of the method call
			"""
			if self.condition_file:
				self._validate_file_path()
				file_path = Path(self.condition_file)
				try:
					if file_path.exists():
						current_modified = datetime.fromtimestamp(file_path.stat().st_mtime)
						if self.last_modified is None or current_modified > self.last_modified:
							clear_method = getattr(instance, self.cache_clear_method, None)
							if callable(clear_method):
								clear_method()
							self.last_modified = current_modified
				except OSError:
					pass  # Ignore file access errors and proceed with method execution
			return method(instance, *args, **kwargs)

		return wrapper

	def _validate_file_path(self) -> None:
		"""Validate condition file path.

		Raises
		------
		ValueError
			If condition_file is empty or not a string
		"""
		if not self.condition_file:
			raise ValueError("condition_file cannot be empty")
		if not isinstance(self.condition_file, str):
			raise ValueError(
				f"condition_file must be a string, got {type(self.condition_file).__name__}"
			)
