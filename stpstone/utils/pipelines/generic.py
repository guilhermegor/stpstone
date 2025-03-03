### GENERIC PIPELINES ###

from typing import Any, Callable, List

import pandas as pd


def generic_pipeline(data: Any, functions: List[Callable]) -> Any:
    """
    Applies a sequence of functions to a given data object.
    Args:
        data (Any): Initial data input (pandas DataFrame, string, number, list, etc.).
        functions (List[Callable]): A list of functions to apply sequentially.
    Returns:
        Any: The final processed data after applying all functions.
    """
    for func in functions:
        try:
            #   check if the function is designed for DataFrame and if data is a DataFrame
            if isinstance(data, pd.DataFrame) and callable(func):
                data = func(data)
            elif not isinstance(data, pd.DataFrame):
                data = func(data)
        except Exception as e:
            print(f"Error in {func.__name__}: {e}")
            break
    return data
