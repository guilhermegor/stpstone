"""Test the performance of a CPU-bound operation.

This test simulates a CPU-bound operation and measures its performance.
It ensures that the operation completes within a specified time threshold.
"""
import time

import pytest


def heavy_computation(n: int) -> int:
    """Simulate a CPU-bound operation by summing squared integers.

    Parameters
    ----------
    n : int
        Upper bound (exclusive) for the range of integers to square and sum.

    Returns
    -------
    int
        Sum of squares of integers from 0 to n-1.
    """
    total = 0
    for i in range(n):
        total += i * i
    return total


@pytest.mark.performance
@pytest.mark.parametrize("n, max_seconds", [
    (10_000, 0.01),
    (100_000, 0.1),
])
def test_heavy_computation_performance(n: int, max_seconds: float) -> None:
    """Test that heavy_computation completes within the given time threshold.

    Parameters
    ----------
    n : int
        Upper bound passed to heavy_computation.
    max_seconds : float
        Maximum allowed wall-clock time in seconds.
    """
    start = time.perf_counter()
    result = heavy_computation(n)
    elapsed = time.perf_counter() - start

    assert isinstance(result, int)

    assert elapsed < max_seconds, (
        f"Execution took {elapsed:.4f}s, expected < {max_seconds:.4f}s"
    )
