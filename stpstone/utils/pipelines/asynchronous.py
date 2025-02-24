### ASYNCHRONOUS PIPELINES ###

import asyncio
from typing import Callable, List, Any


async def asyncpipeline(data:Any, functions:List[Callable]) -> Any:
    """
    Executes a sequence of asynchronous functions on a given input
    Examples of usage:
        API requests, web scraping, database queries
    Args:
        data (Any): The initial input data
        functions (List[Callable]): A list of async functions
    Returns:
        Any: The processed data.
    """
    for func in functions:
        try:
            data = await func(data)
        except Exception as e:
            print(f"Error in {func.__name__}: {e}")
            break
    return data


if __name__ == "__main__":

    async def async_step_1(data):
        await asyncio.sleep(1)
        return data * 2

    async def async_step_2(data):
        await asyncio.sleep(1)
        return data + 10

    async def main():
        result = await asyncpipeline(5, [async_step_1, async_step_2])
        print(result)  # Output: 20

    asyncio.run(main())
