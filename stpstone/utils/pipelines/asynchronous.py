"""Asynchronous function pipeline execution utilities.

This module provides functionality for executing sequences of asynchronous functions
on input data, with error handling capabilities. Designed for use with async workflows
like API requests, web scraping, and database queries.
"""

import inspect
from typing import Any, Callable


async def asyncpipeline(
    data: Any, # noqa ANN401: typing.Any is not allowed
    functions: list[Callable[[Any], Any]]
) -> Any: # noqa ANN401: typing.Any is not allowed
    """Execute a sequence of asynchronous functions on input data.

    Processes input data through a series of async functions sequentially.
    If any function fails, processing stops and the error is logged.

    Parameters
    ----------
    data : Any
        The initial input data to process
    functions : list[Callable[[Any], Any]]
        list of async functions to execute in order.
        Each function should accept and return data.

    Returns
    -------
    Any
        The processed data after applying all functions,
        or data up to the point of failure.

    Raises
    ------
    TypeError
        If `functions` is not a list or contains non-callable or non-async functions.

    Examples
    --------
    >>> async def async_step_1(data):
    ...     await asyncio.sleep(1)
    ...     return data * 2
    >>> async def async_step_2(data):
    ...     await asyncio.sleep(1)
    ...     return data + 10
    >>> async def main():
    ...     result = await asyncpipeline(5, [async_step_1, async_step_2])
    ...     print(result)  # Output: 20
    >>> asyncio.run(main())
    20
    """
    if not isinstance(functions, list):
        raise TypeError(f"Expected list[Callable], got {type(functions).__name__}")
    
    for func in functions:
        if not callable(func):
            raise TypeError(f"Expected callable, got non-callable: {func}")
        if not inspect.iscoroutinefunction(func):
            raise TypeError(f"Expected async function, got synchronous function: {func.__name__}")
    
    for func in functions:
        try:
            data = await func(data)
        except Exception as err:
            print(f"Error in {func.__name__}: {err}")
            break
    return data