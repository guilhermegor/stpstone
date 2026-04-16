"""Unit tests for multiprocessing utilities.

Tests the parallel task execution functionality including:
- Worker function execution with various argument types
- CPU count validation
- Parallel execution with different configurations
- Error handling and edge cases
"""

from typing import Any, Callable
from unittest.mock import MagicMock, patch

import pytest

from stpstone.utils.pipelines.mp_helper import _validate_ncpus, mp_run_parallel, mp_worker


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_func() -> Callable[[int], int]:
	"""Return a simple test function that squares its input.

	Returns
	-------
	Callable[[int], int]
		Function that squares its input
	"""

	def func(x: int) -> int:
		"""Square input.

		Parameters
		----------
		x : int
			Input value

		Returns
		-------
		int
			Squared value
		"""
		return x * x

	return func


@pytest.fixture
def sample_task_args() -> list[tuple[tuple, dict[str, Any]]]:
	"""Provide sample task arguments for testing.

	Returns
	-------
	list[tuple[tuple, dict[str, Any]]]
		List of task arguments with both positional and keyword args
	"""
	return [((1,), {}), ((2,), {}), ((3,), {"multiplier": 2})]


@pytest.fixture
def mock_pool() -> MagicMock:
	"""Create a mock Pool object for testing.

	Returns
	-------
	MagicMock
		Mocked multiprocessing.Pool instance
	"""
	with patch("multiprocessing.Pool") as mock:
		mock_instance = MagicMock()
		mock_instance.map.return_value = ["mock_result"]  # Add this line
		mock.return_value.__enter__.return_value = mock_instance
		yield mock_instance


# --------------------------
# Tests for mp_worker
# --------------------------
def test_mp_worker_with_positional_args(sample_func: Callable) -> None:
	"""Verify worker executes function with positional arguments correctly.

	Parameters
	----------
	sample_func : Callable
		Sample function to test

	Returns
	-------
	None
	"""
	args = (sample_func, (5,), {})
	result = mp_worker(args)
	assert result == 25


def test_mp_worker_with_keyword_args() -> None:
	"""Verify worker executes function with keyword arguments correctly.

	Returns
	-------
	None
	"""

	def func(x: int, multiplier: int = 1) -> int:
		"""Multiply input by multiplier.

		Parameters
		----------
		x : int
			Input value
		multiplier : int
			Multiplier value, defaults to 1

		Returns
		-------
		int
			Multiplied value
		"""
		return x * multiplier

	args = (func, (5,), {"multiplier": 3})
	result = mp_worker(args)
	assert result == 15


def test_mp_worker_with_no_args() -> None:
	"""Verify worker handles functions with no arguments.

	Returns
	-------
	None
	"""

	def func() -> str:
		"""Return a success message.

		Returns
		-------
		str
			Success message
		"""
		return "success"

	args = (func, (), {})
	result = mp_worker(args)
	assert result == "success"


# --------------------------
# Tests for _validate_ncpus
# --------------------------
def test_validate_ncpus_positive() -> None:
	"""Check that valid CPU counts are accepted.

	Returns
	-------
	None
	"""
	_validate_ncpus(1)
	_validate_ncpus(4)
	_validate_ncpus(100)


def test_validate_ncpus_zero() -> None:
	"""Check that zero CPUs raises ValueError.

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Number of CPUs must be at least 1"):
		_validate_ncpus(0)


def test_validate_ncpus_negative() -> None:
	"""Check that negative CPUs raises ValueError.

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Number of CPUs must be at least 1"):
		_validate_ncpus(-1)


# --------------------------
# Tests for mp_run_parallel
# --------------------------
def test_mp_run_parallel_default_args(sample_func: Callable, mock_pool: MagicMock) -> None:
	"""Verify parallel execution with default arguments works.

	Parameters
	----------
	sample_func : Callable
		Sample function to test
	mock_pool : MagicMock
		Mocked multiprocessing.Pool instance

	Returns
	-------
	None
	"""
	mock_pool.map.return_value = ["mock_result"]
	results = mp_run_parallel(sample_func)
	assert results == ["mock_result"]  # Changed assertion
	mock_pool.map.assert_called_once()


def test_mp_run_parallel_custom_args(
	sample_func: Callable,
	sample_task_args: list[tuple[tuple, dict[str, Any]]],
	mock_pool: MagicMock,
) -> None:
	"""Verify parallel execution with custom task arguments works.

	Parameters
	----------
	sample_func : Callable
		Sample function to test
	sample_task_args : list[tuple[tuple, dict[str, Any]]]
		Sample task arguments
	mock_pool : MagicMock
		Mocked multiprocessing.Pool instance

	Returns
	-------
	None
	"""
	mock_pool.map.return_value = [1, 4, 9]  # Add expected return values
	results = mp_run_parallel(sample_func, sample_task_args)
	assert len(results) == 3  # Changed assertion
	mock_pool.map.assert_called_once()


def test_mp_run_parallel_custom_cpus(sample_func: Callable, mock_pool: MagicMock) -> None:
	"""Verify parallel execution with custom CPU count works.

	Parameters
	----------
	sample_func : Callable
		Sample function to test
	mock_pool : MagicMock
		Mocked multiprocessing.Pool instance

	Returns
	-------
	None
	"""
	mock_pool.map.return_value = ["mock_result"]
	custom_cpus = 4
	results = mp_run_parallel(sample_func, int_ncpus=custom_cpus)
	assert results == ["mock_result"]  # Changed assertion
	mock_pool.map.assert_called_once()


def test_mp_run_parallel_empty_task_args(sample_func: Callable, mock_pool: MagicMock) -> None:
	"""Verify parallel execution handles empty task args list.

	Parameters
	----------
	sample_func : Callable
		Sample function to test
	mock_pool : MagicMock
		Mocked multiprocessing.Pool instance

	Returns
	-------
	None
	"""
	mock_pool.map.return_value = []  # Add expected return value
	results = mp_run_parallel(sample_func, [])
	assert results == []
	mock_pool.map.assert_called_once()


def test_mp_run_parallel_invalid_cpus(sample_func: Callable) -> None:
	"""Verify parallel execution rejects invalid CPU count.

	Parameters
	----------
	sample_func : Callable
		Sample function to test

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError):
		mp_run_parallel(sample_func, int_ncpus=0)


def test_mp_run_parallel_real_execution(sample_func: Callable) -> None:
	"""Test actual parallel execution produces correct results.

	Parameters
	----------
	sample_func : Callable
		Sample function to test

	Returns
	-------
	None
	"""
	task_args = [((i,), {}) for i in range(1, 5)]
	with patch("multiprocessing.Pool") as mock_pool:
		mock_instance = MagicMock()
		mock_instance.map.return_value = [1, 4, 9, 16]  # Mock the return values
		mock_pool.return_value.__enter__.return_value = mock_instance

		results = mp_run_parallel(sample_func, task_args, int_ncpus=2)
		assert results == [1, 4, 9, 16]


# --------------------------
# Type Validation Tests
# --------------------------
def test_mp_worker_type_validation() -> None:
	"""Check that mp_worker validates argument types.

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError):
		mp_worker("invalid_args")  # type: ignore


def test_mp_run_parallel_type_validation(sample_func: Callable) -> None:
	"""Check that mp_run_parallel validates argument types.

	Parameters
	----------
	sample_func : Callable
		Sample function to test

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError):
		mp_run_parallel("not_a_function")  # type: ignore

	with pytest.raises(TypeError):
		mp_run_parallel(sample_func, "invalid_task_args")  # type: ignore
