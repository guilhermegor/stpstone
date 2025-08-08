"""Stream processing pipeline utilities.

This module provides functionality for processing data streams through a sequence
of transformation functions in a memory-efficient manner.
"""

from collections.abc import Iterable, Iterator
from typing import Callable

from stpstone.transformations.validation.metaclass_type_checker import type_checker


@type_checker
def streaming_pipeline(
    generator: Iterable[str],
    functions: list[Callable[[str], str]]
) -> Iterator[str]:
    """Process a generator stream through a sequence of functions.

    Parameters
    ----------
    generator : Iterable[str]
        A data stream yielding string elements
    functions : list[Callable[[str], str]]
        A list of functions to apply sequentially to each element

    Yields
    ------
    Iterator[str]
        Processed elements from the stream after applying all functions

    Raises
    ------
    TypeError
        If any of the transformation functions are not callable

    Examples
    --------
    >>> def to_uppercase(text): return text.upper()
    >>> def add_exclamation(text): return text + '!'
    >>> stream = iter(['hello', 'world'])
    >>> processed_stream = streamingpipeline(stream, [to_uppercase, add_exclamation])
    >>> for item in processed_stream:
    ...     print(item)
    HELLO!
    WORLD!
    """
    for func in functions:
        if not callable(func):
            raise TypeError(
                "All transformation functions must be callable, "
                f"but got {type(func).__name__}"
            )
    
    for data in generator:
        for func in functions:
            result = func(data)
            if not isinstance(result, str):
                raise TypeError(
                    f"Function {func.__name__} must return a str, "
                    f"but got {type(result).__name__}"
                )
            data = result
        yield data
