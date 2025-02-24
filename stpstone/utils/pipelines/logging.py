### LOGGING PIPELINE ###

# pypi.org libs
from logging import Logger
from typing import Any, Callable, List
# local libs
from stpstone.utils.loggs.create_logs import CreateLog


def loggingpipeline(data:Any, functions:List[Callable], logger:Logger) -> Any:
    """
    Executes a sequence of functions while logging each step.
    """
    for func in functions:
        try:
            data = func(data)
            CreateLog().info(f'Applied {func.__name__}, result: {data}')
        except Exception as e:
            logger.error(f'Error in {func.__name__}: {e}')
            break
    return data


if __name__ == '__main__':
    def square(x): return x ** 2
    def halve(x): return x / 2

    result = loggingpipeline(4, [square, halve])
    # Logs: Applied square, result: 16 → Applied halve, result: 8
