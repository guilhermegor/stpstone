"""Conditional function application pipeline.

This module provides a utility for applying functions to data conditionally based on
predicate evaluations. Useful for rule-based processing systems like fraud detection.
"""

from typing import Any, Callable, List, Tuple

from stpstone.transformations.validation.metaclass_type_checker import type_check


@type_check
def conditional_pipeline(
    data: Any, # noqa ANN401: typing.Any is not allowed
    functions: List[Tuple[Callable[[Any], bool], Callable[[Any], Any]]]
) -> Any: # noqa ANN401: typing.Any is not allowed
    """Apply functions conditionally based on predicate evaluations.

    Parameters
    ----------
    data : Any
        Initial input data to be processed
    functions : List[Tuple[Callable[[Any], bool], Callable[[Any], Any]]]
        List of (predicate, function) tuples where:
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
        
        if condition(data):
            data = func(data)
    
    return data