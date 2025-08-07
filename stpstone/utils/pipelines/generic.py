"""Generic data processing pipeline.

This module defines a utility function to apply a sequence of transformations
to a given data object, including pandas DataFrames and other data types.
"""

from typing import Any, Callable

from stpstone.transformations.validation.metaclass_type_checker import type_checker


@type_checker
def generic_pipeline(
    data: Any, # noqa ANN401: typing.Any is not allowed
    functions: list[Callable[..., Any]]
) -> Any: # noqa ANN401: typing.Any is not allowed
    """Apply a sequence of functions to a given data object.

    Parameters
    ----------
    data : Any
        Initial data input (pandas DataFrame, string, number, list, etc.)
    functions : list[Callable[..., Any]]
        A list of functions to apply sequentially

    Returns
    -------
    Any
        The final processed data after applying all functions

    Raises
    ------
    ValueError
        If the functions list is empty
    TypeError
        If any of the functions in the list are not callable
    RuntimeError
        If any of the functions in the list raise an exception

    Examples
    --------
    >>> def add_one(x): return x + 1
    >>> def square(x): return x * x
    >>> generic_pipeline(2, [add_one, square])
    9
    """
    if not functions:
        raise ValueError("Functions list cannot be empty")
    
    processed_data = data
    for func in functions:
        if not callable(func):
            raise TypeError(f"All pipeline items must be callable, got {type(func)}")
        
        try:
            processed_data = func(processed_data)
        except Exception as e:
            raise RuntimeError(f"Error in {func.__name__}: {e}") from e
    return processed_data
