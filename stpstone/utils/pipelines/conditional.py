### CONDITIONAL PIPELINES ###

from typing import Any, List


def conditionalpipeline(data: Any, functions: List[tuple]) -> Any:
    """
    Applies functions conditionally based on a predicate
    Examples of usage:
        Fraud detection, rule-based processing
    Args:
        data (Any): Initial input
        functions (List[tuple]): List of (predicate, function) tuples
    Returns:
        Any: Processed data
    """
    for condition, func in functions:
        if condition(data):
            data = func(data)
    return data


if __name__ == "__main__":

    def is_even(x):
        return x % 2 == 0

    def double(x):
        return x * 2

    def triple(x):
        return x * 3

    steps = [(is_even, double), (lambda x: x > 10, triple)]
    result = conditionalpipeline(6, steps)
    print(result)
