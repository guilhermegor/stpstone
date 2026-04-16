"""Logging utilities with enhanced functionality.

This module provides a class for creating and managing log files with customizable
formats and levels, along with utility decorators for timing function execution.
"""

import inspect
import logging
import os
import time
from typing import Any, Callable, Literal, Optional, TypedDict

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker, type_checker


class CreateLog(metaclass=TypeChecker):
	"""Class for creating and managing log files with customizable formats."""

	def _validate_path(self, path: str) -> None:
		"""Validate path format and existence.

		Parameters
		----------
		path : str
			Path to validate

		Raises
		------
		ValueError
			If path is empty
			If path is not a string
		"""
		if not path:
			raise ValueError("Path cannot be empty")
		if not isinstance(path, str):
			raise ValueError("Path must be a string")

	def creating_parent_folder(self, new_path: str) -> bool:
		"""Create parent folder if it doesn't exist.

		Parameters
		----------
		new_path : str
			Path to create

		Returns
		-------
		bool
			True if folder was created, False if it already existed
		"""
		self._validate_path(new_path)
		if not os.path.exists(new_path):
			os.makedirs(new_path)
			return True
		return False

	def basic_conf(
		self, complete_path: str, basic_level: Literal["info", "debug"] = "info"
	) -> logging.Logger:
		"""Configure basic logging settings.

		Parameters
		----------
		complete_path : str
			Full path to log file
		basic_level : Literal['info', 'debug']
			Logging level (default: "info")

		Returns
		-------
		logging.Logger
			Configured logger instance

		Raises
		------
		ValueError
			If invalid logging level is provided
		"""
		self._validate_path(complete_path)

		level_mapping = {"info": logging.INFO, "debug": logging.DEBUG}

		try:
			level = level_mapping[basic_level]
		except KeyError as err:
			raise ValueError("Level was not properly defined in basic config of logging") from err

		logger = logging.getLogger(__name__)
		logger.setLevel(level)
		handler = logging.FileHandler(complete_path)
		handler.setFormatter(
			logging.Formatter(
				"%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s",
				datefmt="%Y-%m-%d,%H:%M:%S",
			)
		)
		logger.handlers.clear()
		logger.addHandler(handler)
		return logger

	def log_message(
		self,
		logger: Optional[logging.Logger],
		message: str,
		log_level: Literal["info", "warning", "error", "critical"],
	) -> None:
		"""Log a message with caller context information.

		Parameters
		----------
		logger : Optional[logging.Logger]
			Logger instance or None for console output
		message : str
			Message to log
		log_level : Literal['info', 'warning', 'error', 'critical']
			Logging level

		Raises
		------
		ValueError
			If log_level is invalid or empty
		"""
		if not log_level:
			raise ValueError("log_level cannot be empty")

		frame = inspect.currentframe()
		class_name = "UnknownClass"
		method_name = "unknown_method"
		set_skip_modules = {
			"pydantic",
			"typing",
			"inspect",
			"logging",
			"stpstone.transformations.validation",
		}

		while frame:
			frame = frame.f_back
			if not frame:
				break
			module_name = frame.f_globals.get("__name__", "UnknownModule")
			if any(module_name.startswith(prefix) for prefix in set_skip_modules):
				continue
			self_potential_cls = frame.f_locals.get("self")
			if self_potential_cls is not None and not isinstance(
				self_potential_cls, self.__class__
			):
				class_name = self_potential_cls.__class__.__name__
				method_name = frame.f_code.co_name
				break
			method_name = frame.f_code.co_name

		formatted_message = f"[{class_name}.{method_name}] {message}"

		if logger is not None:
			log_method = getattr(logger, log_level, None)
			if log_method is None:
				raise ValueError(f"Invalid log level: {log_level}")
			log_method(formatted_message)
		else:
			level = log_level.upper()
			timestamp = (
				f"{time.strftime('%Y-%m-%d,%H:%M:%S')}.{int(time.time() * 1000) % 1000:03d}"
			)
			print(f"{timestamp} {level} {{{class_name}}} [{method_name}] {message}")


class ReturnTimeit(TypedDict):
	"""Return type for timeit decorator.

	Parameters
	----------
	result : Any
		Original function return value
	execution_time_ms : float
		Execution time in milliseconds
	"""

	result: object
	execution_time_ms: float


@type_checker
def timeit(method: Callable) -> Callable[..., ReturnTimeit]:
	"""Decorate to measure function execution time.

	Parameters
	----------
	method : Callable
		Function to time

	Returns
	-------
	Callable[..., ReturnTimeit]
		Wrapped function with timing
	"""

	@type_checker
	def timed(
		*args: Any,  # noqa ANN401: typing.Any is not allowed
		**kw: Any,  # noqa ANN401: typing.Any is not allowed
	) -> ReturnTimeit:
		"""Decorate function with timing.

		Parameters
		----------
		*args : Any
			Positional arguments
		**kw : Any
			Keyword arguments

		Returns
		-------
		ReturnTimeit
			Wrapped function with timing
		"""
		time_start = time.time()
		result = method(*args, **kw)
		time_end = time.time()
		execution_time = (time_end - time_start) * 1000

		if "log_time" in kw:
			name = kw.get("log_name", method.__name__.upper())
			kw["log_time"][name] = int(execution_time)
		else:
			print(f"{method.__name__!r} {execution_time:2.2f} ms")

		return {"result": result, "execution_time_ms": execution_time}

	return timed


@type_checker
def conditional_timeit(bool_use_timer: bool) -> Callable:
	"""Conditionally apply timeit decorator based on parameter.

	Parameters
	----------
	bool_use_timer : bool
		Whether to apply timing

	Returns
	-------
	Callable
		Decorated function if bool_use_timer is True, else original function
	"""

	@type_checker
	def decorator(method: Callable) -> Callable:
		"""Apply timeit decorator if bool_use_timer is True.

		Parameters
		----------
		method : Callable
			Function to decorate

		Returns
		-------
		Callable
			Decorated function if bool_use_timer is True, else original function
		"""
		return timeit(method) if bool_use_timer else method

	return decorator
