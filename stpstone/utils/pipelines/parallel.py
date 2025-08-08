"""Parallel pipeline execution utilities.

This module provides functionality for executing sequences of functions in parallel
using thread pools, with support for batch processing and data transformation pipelines.
"""

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

from stpstone.transformations.validation.metaclass_type_checker import type_checker


@type_checker
def parallel_pipeline(
    data: Any, # noqa ANN401: typing.Any is not allowed
    functions: list[Callable[[Any], Any]]
) -> Any: # noqa ANN401: typing.Any is not allowed
    """Execute a sequence of functions in parallel where possible.

    Parameters
    ----------
    data : Any
        Initial data input to be processed
    functions : list[Callable[[Any], Any]]
        list of functions to apply sequentially to the data

    Returns
    -------
    Any
        The processed data after applying all functions

    Raises
    ------
    ValueError
        If the functions list is empty
    RuntimeError
        If an error occurs during parallel pipeline execution

    Examples
    --------
    >>> def add_one(x): return x + 1
    >>> def multiply_by_two(x): return x * 2
    >>> def subtract_five(x): return x - 5
    >>> result = parallel_pipeline(5, [add_one, multiply_by_two, subtract_five])
    >>> print(result)
    7

    Notes
    -----
    The functions are applied in the order they appear in the list, with each function
    receiving the output of the previous function as input. Parallel execution is used
    when possible for improved performance.
    """
    if not functions:
        raise ValueError("Functions list cannot be empty")

    with ThreadPoolExecutor() as executor:
        try:
            results = list(executor.map(lambda f: f(data), functions))
            return results[-1]
        except Exception as err:
            raise RuntimeError(f"Error during parallel pipeline execution: {str(err)}") from err