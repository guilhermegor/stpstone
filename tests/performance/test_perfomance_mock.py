"""Test the performance of a CPU-bound operation.

This test simulates a CPU-bound operation and measures its performance.
It ensures that the operation completes within a specified time threshold.
"""
import time

import pytest


def heavy_computation(n: int) -> int:
    """Simulates a CPU-bound operation."""
    total = 0
    for i in range(n):
        total += i * i
    return total


@pytest.mark.performance
@pytest.mark.parametrize("n, max_seconds", [
    (10_000, 0.01),
    (100_000, 0.1),
])
def test_heavy_computation_performance(n, max_seconds):
    start = time.perf_counter()
    result = heavy_computation(n)
    elapsed = time.perf_counter() - start

    assert isinstance(result, int)

    assert elapsed < max_seconds, (
        f"Execution took {elapsed:.4f}s, expected < {max_seconds:.4f}s"
    )
