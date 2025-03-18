### PARALLEL PIPELINES ###

from concurrent.futures import ThreadPoolExecutor
from typing import Callable, List, Any


def parallelpipeline(data:Any, functions:List[Callable]) -> Any:
    """
    Executes a sequence of functions in parallel where possible
    Examples of usage:
        Batch processing, image transformation
    Args:
        data (Any): Initial data input
        functions (List[Callable]): A list of functions to apply
    Returns:
        Any: The processed data
    """
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(lambda f: f(data), functions))
    return results[-1]  # Return last transformation


if __name__ == "__main__":
    def add_one(x): return x + 1
    def multiply_by_two(x): return x * 2
    def subtract_five(x): return x - 5

    result = parallelpipeline(5, [add_one, multiply_by_two, subtract_five])
    print(result)
