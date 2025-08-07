"""Multiprocessing utilities for parallel task execution.

This module provides functions for running tasks in parallel using Python's multiprocessing.
It handles function execution with both positional and keyword arguments across multiple processes.
"""

import multiprocessing as mp
from typing import Any, Callable, Optional, Union

from stpstone.transformations.validation.metaclass_type_checker import type_checker


@type_checker
def mp_worker(
    args: tuple[Callable, tuple, dict[str, Any]]
) -> Any: # noqa ANN401: typing.Any is not allowed
    """Generalized worker function to call any method or function with its arguments.

    Parameters
    ----------
    args : tuple[Callable, tuple, dict[str, Any]]
        Contains:
        - callable: The function or method to be called
        - positional_args: The arguments to be passed to the function or method
        - keyword_args: A dictionary of keyword arguments

    Returns
    -------
    Any
        The result of the callable

    References
    ----------
    .. [1] https://chatgpt.com/share/6737ddcc-8564-800c-908f-9e36c311a834
    """
    func, positional_args, keyword_args = args
    return func(*positional_args, **keyword_args)

@type_checker
def _validate_ncpus(int_ncpus: int) -> None:
    """Validate number of CPUs for multiprocessing.

    Parameters
    ----------
    int_ncpus : int
        Number of CPUs to use

    Raises
    ------
    ValueError
        If number of CPUs is less than 1
    """
    if int_ncpus < 1:
        raise ValueError("Number of CPUs must be at least 1")

@type_checker
def mp_run_parallel(
    func: Callable,
    list_task_args: Optional[list[Union[tuple[tuple, dict[str, Any]], tuple[tuple]]]] = None,
    int_ncpus: int = mp.cpu_count() - 2 if mp.cpu_count() > 2 else 1
) -> list[Any]:
    """Run worker parallelized with multiprocessing.

    Handles the pickling requirement of multiprocessing:
    - Relies on pickling to serialize objects when sending them to worker processes
    - Instance methods are not pickable by default
    - On Windows, essential to protect entry point with if __name__ == '__main__'

    Parameters
    ----------
    func : Callable
        Function or method to execute in parallel
    list_task_args : Optional[list[Union[tuple[tuple, dict[str, Any]], tuple[tuple]]]]
        list of task arguments (positional and keyword) for each execution
        Defaults to [((), {})] if None
    int_ncpus : int
        Number of CPUs to use (default: cpu_count - 2 if > 2 else 1)

    Returns
    -------
    list[Any]
        list of results from each task execution

    References
    ----------
    .. [1] https://chatgpt.com/share/6737ddcc-8564-800c-908f-9e36c311a834
    """
    _validate_ncpus(int_ncpus)
    
    if list_task_args is None:
        list_task_args = [((), {})]

    args_list = [
        (func, pos_args, kw_args if kw_args else {})
        for pos_args, *kw_args in list_task_args
    ]

    with mp.Pool(processes=int_ncpus) as pool:
        list_results = pool.map(mp_worker, args_list)

    return list_results