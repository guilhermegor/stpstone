"""Unit tests for asynchronous pipeline execution.

Tests the asyncpipeline function with various input scenarios including:
- Successful execution with multiple functions
- Error handling during pipeline execution
- Edge cases with empty or invalid inputs
- Type validation for function parameters
"""

import asyncio
from typing import Any, Callable
from unittest.mock import patch

import pytest

from stpstone.utils.pipelines.asynchronous import asyncpipeline


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_async_functions() -> list[Callable[[int], int]]:
    """Fixture providing sample async functions for testing.

    Returns
    -------
    list[Callable[[int], int]]
        list of three async functions that:
        1. Multiplies input by 2
        2. Adds 10 to input
        3. Subtracts 5 from input
    """
    async def func1(x: int) -> int:
        """Multiply input by 2.
        
        Parameters
        ----------
        x : int
            Input value

        Returns
        -------
        int
            Multiplied value
        """
        await asyncio.sleep(0.01)
        return x * 2

    async def func2(x: int) -> int:
        """Add 10 to input.
        
        Parameters
        ----------
        x : int
            Input value

        Returns
        -------
        int
            Summed value
        """
        await asyncio.sleep(0.01)
        return x + 10

    async def func3(x: int) -> int:
        """Subtract 5 from input.
        
        Parameters
        ----------
        x : int
            Input value

        Returns
        -------
        int
            Subtracted value
        """
        await asyncio.sleep(0.01)
        return x - 5

    return [func1, func2, func3]


@pytest.fixture
def failing_async_function() -> Callable[[Any], Any]:
    """Fixture providing an async function that raises ValueError.

    Returns
    -------
    Callable[[Any], Any]
        Async function that raises ValueError with test message

    Raises
    ------
    ValueError
        Test error
    """
    async def func(
        x: Any # noqa ANN401: typing.Any is not allowed
    ) -> Any: # noqa ANN401: typing.Any is not allowed
        """Async function that raises ValueError.
        
        Parameters
        ----------
        x : Any
            Input value

        Returns
        -------
        Any
            Input value

        Raises
        ------
        ValueError
            Test error
        """
        await asyncio.sleep(0.01)
        raise ValueError("Test error")

    return func


# --------------------------
# Tests
# --------------------------
class TestAsyncPipelineSuccess:
    """Tests for successful pipeline execution scenarios."""

    async def test_normal_execution(
        self, sample_async_functions: list[Callable[[int], int]]
    ) -> None:
        """Test pipeline with valid functions and input.

        Verifies
        --------
        - Pipeline executes all functions in order
        - Returns correct final result
        - Maintains proper type through transformations

        Parameters
        ----------
        sample_async_functions : list[Callable[[int], int]]
            list of sample async functions

        Returns
        -------
        None
        """
        result = await asyncpipeline(5, sample_async_functions)
        assert result == 15  # ((5 * 2) + 10) - 5
        assert isinstance(result, int)

    async def test_single_function(
        self, sample_async_functions: list[Callable[[int], int]]
    ) -> None:
        """Test pipeline with single function.

        Verifies
        --------
        - Pipeline works with single function list
        - Returns correct transformed value

        Parameters
        ----------
        sample_async_functions : list[Callable[[int], int]]
            list of sample async functions

        Returns
        -------
        None
        """
        result = await asyncpipeline(5, [sample_async_functions[0]])
        assert result == 10

    async def test_string_data(self) -> None:
        """Test pipeline with string data transformation.

        Verifies
        --------
        - Pipeline works with non-numeric data
        - String operations execute correctly

        Returns
        -------
        None
        """
        async def append_hello(s: str) -> str:
            """Append 'hello' to input string.
            
            Parameters
            ----------
            s : str
                Input string

            Returns
            -------
            str
                String with 'hello' appended
            """
            await asyncio.sleep(0.01)
            return s + " hello"

        async def append_world(s: str) -> str:
            """Append 'world' to input string.
            
            Parameters
            ----------
            s : str
                Input string

            Returns
            -------
            str
                String with 'world' appended
            """
            await asyncio.sleep(0.01)
            return s + " world"

        result = await asyncpipeline("test", [append_hello, append_world])
        assert result == "test hello world"


class TestAsyncPipelineEdgeCases:
    """Tests for edge cases and special scenarios."""

    async def test_empty_function_list(self) -> None:
        """Test pipeline with empty functions list.

        Verifies
        --------
        - Returns original input unchanged
        - No errors raised with empty list

        Returns
        -------
        None
        """
        result = await asyncpipeline(5, [])
        assert result == 5

    async def test_none_input(self, sample_async_functions: list[Callable[[int], int]]
    ) -> None:
        """Test pipeline with None as input.

        Verifies
        --------
        - None propagates through functions unchanged
        - No errors raised with None input

        Parameters
        ----------
        sample_async_functions : list[Callable[[int], int]]
            list of sample async functions

        Returns
        -------
        None
        """
        result = await asyncpipeline(None, sample_async_functions)
        assert result is None

    async def test_large_input(self, sample_async_functions: list[Callable[[int], int]]
    ) -> None:
        """Test pipeline with large numeric input.

        Verifies
        --------
        - Handles large numbers correctly
        - No overflow or precision issues

        Parameters
        ----------
        sample_async_functions : list[Callable[[int], int]]
            list of sample async functions

        Returns
        -------
        None
        """
        large_num = 10**18
        result = await asyncpipeline(large_num, [sample_async_functions[0]])
        assert result == large_num * 2


class TestAsyncPipelineErrorHandling:
    """Tests for error handling scenarios."""

    async def test_function_error(
        self,
        sample_async_functions: list[Callable[[int], int]],
        failing_async_function: Callable[[Any], Any]
    ) -> None:
        """Test pipeline with failing function.

        Verifies
        --------
        - Pipeline stops at failing function
        - Returns partial result from successful functions
        - Error message is printed

        Parameters
        ----------
        sample_async_functions : list[Callable[[int], int]]
            list of sample async functions
        failing_async_function : Callable[[Any], Any]
            Async function that raises ValueError

        Returns
        -------
        None
        """
        functions = sample_async_functions[:1] + [failing_async_function] \
            + sample_async_functions[1:]
        
        with patch("builtins.print") as mock_print:
            result = await asyncpipeline(5, functions)
            
        assert result == 10  # only first function executed
        mock_print.assert_called_once()
        assert "Error in func" in mock_print.call_args[0][0]
        assert "Test error" in mock_print.call_args[0][0]

    async def test_all_functions_fail(
        self,
        failing_async_function: Callable[[Any], Any]
    ) -> None:
        """Test pipeline where all functions fail.

        Verifies
        --------
        - Returns original input when all functions fail
        - First error is caught and printed

        Parameters
        ----------
        failing_async_function : Callable[[Any], Any]
            Async function that raises ValueError

        Returns
        -------
        None
        """
        with patch("builtins.print") as mock_print:
            result = await asyncpipeline(5, [failing_async_function, failing_async_function])
            
        assert result == 5
        mock_print.assert_called_once()


class TestAsyncPipelineTypeValidation:
    """Tests for input type validation."""

    async def test_non_callable_in_list(self) -> None:
        """Test pipeline with non-callable in functions list.

        Verifies
        --------
        - Raises TypeError when non-callable is provided
        - Error message identifies the invalid item

        Parameters
        ----------
        sample_async_functions : list[Callable[[int], int]]
            list of sample async functions

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="Expected async function, got synchronous function"):
            await asyncpipeline(5, [lambda x: x, "not a function"])

    async def test_non_list_functions(self) -> None:
        """Test pipeline with non-list functions parameter.

        Verifies
        --------
        - Raises TypeError when functions is not a list
        - Error message indicates expected list type
        
        Returns
        -------
        None
        """
        with pytest.raises(TypeError) as excinfo:
            await asyncpipeline(5, "not a list")  # type: ignore
        assert "list[Callable]" in str(excinfo.value)

    async def test_non_async_function(self) -> None:
        """Test pipeline with synchronous function.

        Verifies
        --------
        - Raises TypeError when sync function is provided
        - Error message indicates async requirement

        Returns
        -------
        None
        """
        def sync_func(x: int) -> int:
            """Test Synchronous function.
            
            Parameters
            ----------
            x : int
                Input value

            Returns
            -------
            int
                Summed value
            """
            return x + 1

        with pytest.raises(TypeError) as excinfo:
            await asyncpipeline(5, [sync_func])  # type: ignore
        assert "async function" in str(excinfo.value)


class TestAsyncPipelineExamples:
    """Tests based on examples from docstring."""

    async def test_docstring_example(self) -> None:
        """Verify the example from function docstring works correctly.
        
        Returns
        -------
        None
        """
        async def async_step_1(data: int) -> int:
            """Multiply input by 2.
            
            Parameters
            ----------
            data : int
                Input value

            Returns
            -------
            int
                Multiplied value
            """
            await asyncio.sleep(0.01)
            return data * 2

        async def async_step_2(data: int) -> int:
            """Add 10 to input.
            
            Parameters
            ----------
            data : int
                Input value

            Returns
            -------
            int
                Summed value
            """
            await asyncio.sleep(0.01)
            return data + 10

        async def main() -> int:
            """Test main function.
            
            Returns
            -------
            int
                Final result
            """
            return await asyncpipeline(5, [async_step_1, async_step_2])

        result = await main()
        assert result == 20