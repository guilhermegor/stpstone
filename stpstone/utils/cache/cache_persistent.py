"""Utilities for caching method results to disk using pickle.

This module provides a decorator class to cache method results in memory and optionally persist
them to a file using pickle serialization. It ensures thread-safe operations and integrates with
custom logging and pickle utilities from the stpstone package.
"""

from functools import wraps
from logging import Logger
from pathlib import Path
import pickle
from threading import Lock
from typing import Any, Callable, Optional, Union

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker, type_checker
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.pickle import PickleFiles


class PersistentCacheDecorator(metaclass=TypeChecker):
	"""Decorator for caching method results to a file with thread safety.

	Parameters
	----------
	path_cache : str
		Path to the cache file (will be given a .pkl extension)
	cache_key : str
		Key to identify the cached method
	bool_persist_cache : bool
		If True, saves cache to disk; if False, uses in-memory cache only (default: True)
	logger : Optional[Logger]
		Logger for error messages (default: None)

	Attributes
	----------
	path_cache : Path
		Resolved path to the cache file
	cache_key : str
		Key for cache identification
	bool_persist_cache : bool
		Flag to control cache persistence
	cache : dict
		In-memory cache dictionary
	_lock : Lock
		Thread lock for safe cache operations
	logger : Optional[Logger]
		Logger instance for error logging
	"""

	def __init__(
		self,
		path_cache: Union[Path, str],
		cache_key: str,
		bool_persist_cache: bool = True,
		bool_thread_safe: bool = False,
		logger: Optional[Logger] = None,
	) -> None:
		"""Initialize the cache decorator with file path, key, and persistence settings.

		Parameters
		----------
		path_cache : Union[Path, str]
			Path to the cache file (will be given a .pkl extension)
		cache_key : str
			Key to identify the cached method
		bool_persist_cache : bool
			If True, saves cache to disk; if False, uses in-memory cache only (default: True)
		bool_thread_safe : bool
			If True, uses a thread-safe lock for cache operations (default: False)
		logger : Optional[Logger]
			Logger for error messages (default: None)
		"""
		self._validate_path_cache(path_cache)
		self._validate_cache_key(cache_key)
		self.path_cache = Path(path_cache).with_suffix(".pkl")
		self.cache_key = cache_key
		self.bool_persist_cache = bool_persist_cache
		self.bool_thread_safe = bool_thread_safe
		self._lock = Lock()
		self.logger = logger
		self.cache = self._load_cache() or {}

	def _validate_cache_key(self, cache_key: str) -> None:
		"""Validate the cache key.

		Parameters
		----------
		cache_key : str
			Cache key to validate

		Raises
		------
		TypeError
			If cache key is empty or contains only whitespace
		"""
		if not cache_key or cache_key.isspace():
			raise TypeError("cache_key must be a non-empty string")

	def _validate_path_cache(self, path_cache: Union[Path, str]) -> None:
		"""Validate the path to the cache file.

		Parameters
		----------
		path_cache : Union[Path, str]
			Path to the cache file to validate

		Raises
		------
		TypeError
			If path_cache is empty or not a string
		"""
		if not path_cache or not isinstance(path_cache, str) or path_cache.isspace():
			raise TypeError("path_cache must be a non-empty string")

	def _load_cache(self) -> dict:
		"""Load the cache from the file if it exists.

		Returns
		-------
		dict
			Loaded cache dictionary or empty dictionary if file does not exist
		"""
		if self.bool_thread_safe:
			with self._lock:
				return self._load_cache_unsafe()
		return self._load_cache_unsafe()

	def _load_cache_unsafe(self) -> dict:
		"""Load the cache from the file if it exists.

		Returns
		-------
		dict
			Loaded cache dictionary or empty dictionary if file does not exist

		Raises
		------
		ValueError
			If cache file cannot be loaded due to pickle errors
		"""
		if self.path_cache.exists():
			try:
				cache = PickleFiles().load_message(self.path_cache)
				if cache is None:
					CreateLog().log_message(
						self.logger,
						f"Warning: PickleFiles.load_message returned None for {self.path_cache}",
						"warning",
					)
					return {}
				if not isinstance(cache, dict):
					CreateLog().log_message(
						self.logger,
						f"Warning: Cache loaded from {self.path_cache} "
						f"is not a dict, got {type(cache)}",
						"warning",
					)
					return {}
				return cache
			except (pickle.PickleError, EOFError, AttributeError) as err:
				CreateLog().log_message(
					self.logger, f"Failed to load cache from {self.path_cache}: {err}", "error"
				)
				raise ValueError(f"Failed to load cache from {self.path_cache}: {err}") from err
		return {}

	def _save_cache(self, cache: dict) -> bool:
		"""Save the cache to the file if persistence is enabled.

		Parameters
		----------
		cache : dict
			Cache dictionary to save

		Returns
		-------
		bool
			True if cache is saved successfully, False otherwise
		"""
		if not self.bool_persist_cache:
			return False
		if not self.path_cache.parent.exists():
			self.path_cache.parent.mkdir(parents=True, exist_ok=True)
		if self.bool_thread_safe:
			with self._lock:
				return self._save_cache_unsafe(cache)
		return self._save_cache_unsafe(cache)

	def _save_cache_unsafe(self, cache: dict) -> bool:
		"""Save the cache to the file.

		Parameters
		----------
		cache : dict
			Cache dictionary to save

		Returns
		-------
		bool
			True if cache is saved successfully, False otherwise

		Raises
		------
		ValueError
			If cache cannot be saved due to pickle errors
		"""
		try:
			return PickleFiles().dump_message(cache, self.path_cache)
		except pickle.PickleError as err:
			CreateLog().log_message(
				self.logger,
				f"Warning: Failed to save cache to {self.path_cache}: {err}",
				"error",
			)
			raise ValueError(f"Failed to save cache to {self.path_cache}: {err}") from err

	def clear_cache(self) -> None:
		"""Clear the in-memory and file-based cache.

		Raises
		------
		ValueError
			If cache file path is invalid or cannot be deleted
		"""
		with self._lock:
			self.cache = {}
			if self.path_cache.exists():
				try:
					self.path_cache.unlink()
				except Exception as err:
					CreateLog().log_message(
						self.logger,
						f"Warning: Failed to delete cache file {self.path_cache}: {err}",
						"error",
					)
					raise ValueError(
						f"Failed to delete cache file {self.path_cache}: {err}"
					) from err

	def __call__(self, method: Callable[..., Any]) -> Callable[..., Any]:
		"""Decorate a method to cache its results.

		Parameters
		----------
		method : Callable[..., Any]
			Method to decorate

		Returns
		-------
		Callable[..., Any]
			Wrapped method with caching functionality
		"""

		@type_checker
		@wraps(method)
		def wrapper(
			*args: Any,  # noqa ANN401: typing.Any is not allowed
			**kwargs: Any,  # noqa ANN401: typing.Any is not allowed
		) -> Any:  # noqa ANN401: typing.Any is not allowed
			"""Wrap caching functionality.

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
			"""
			persist = getattr(args[0], "bool_persist_cache", self.bool_persist_cache)
			key = f"{self.cache_key}:{args[1:]}:{kwargs}"
			if self.bool_thread_safe:
				with self._lock:
					if key in self.cache:
						return self.cache[key]
					result = method(args[0], *args[1:], **kwargs)
					self.cache[key] = result
					if persist:
						self._save_cache(self.cache)
					return result
			else:
				if key in self.cache:
					return self.cache[key]
				result = method(args[0], *args[1:], **kwargs)
				self.cache[key] = result
				if persist:
					self._save_cache(self.cache)
				return result

		return wrapper
