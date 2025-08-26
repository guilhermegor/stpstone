"""Time-to-live cache decorator implementation.

This module provides a decorator class for caching method results with expiration
based on time-to-live (TTL) in seconds. It uses datetime for time tracking and
functools for proper method wrapping.
"""

from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Callable

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker, type_checker


class TTLCacheDecorator(metaclass=TypeChecker):
    """Decorator to cache method results with a time-to-live expiration.

    This decorator caches the results of method calls and returns the cached
    value if it is still valid based on the specified TTL. Invalid cache entries
    are automatically refreshed by calling the original method.

    Parameters
    ----------
    ttl_seconds : int
        Time-to-live for cache entries in seconds (must be positive)
    cache_key : str
        Base key for cache entries (must not be empty)

    Raises
    ------
    ValueError
        If ttl_seconds is not positive
        If cache_key is empty or not a string
    """

    def __init__(
        self, 
        ttl_seconds: int, 
        cache_key: str
    ) -> None:
        """Initialize the TTLCacheDecorator.
        
        Parameters
        ----------
        ttl_seconds : int
            Time-to-live for cache entries in seconds (must be positive)
        cache_key : str
            Base key for cache entries (must not be empty)
        
        Returns
        -------
        None
        """
        self._validate_ttl_seconds(ttl_seconds)
        self._validate_cache_key(cache_key)
        self.ttl_seconds = ttl_seconds
        self.cache_key = cache_key
        self.cache: dict[str, Any] = {}
        self.last_updated: dict[str, datetime] = {}

    def __call__(self, method: Callable) -> Callable:
        """Wrap the method with TTL caching functionality.

        Parameters
        ----------
        method : Callable
            Method to be decorated

        Returns
        -------
        Callable
            Wrapped method with caching behavior
        """
        @type_checker
        @wraps(method)
        def wrapper(
            self_instance: Any, # noqa ANN401: typing.Any is not allowed
            *args: Any, # noqa ANN401: typing.Any is not allowed
            **kwargs: Any # noqa ANN401: typing.Any is not allowed
        ) -> Any: # noqa ANN401: typing.Any is not allowed
            """Implement TTL caching behavior.

            Parameters
            ----------
            self_instance : Any
                Instance of the class containing the decorated method
            *args : Any
                Positional arguments passed to the method
            **kwargs : Any
                Keyword arguments passed to the method

            Returns
            -------
            Any
                Cached result if valid, otherwise newly computed result
            """
            key = f"{self.cache_key}:{args}:{kwargs}"
            current_time = datetime.now()

            if key in self.cache:
                time_since_update = current_time - self.last_updated[key]
                if time_since_update < timedelta(seconds=self.ttl_seconds):
                    return self.cache[key]

            result = method(self_instance, *args, **kwargs)
            self.cache[key] = result
            self.last_updated[key] = current_time
            return result

        return wrapper

    def _validate_ttl_seconds(self, ttl_seconds: int) -> None:
        """Validate that TTL seconds is a positive integer.

        Parameters
        ----------
        ttl_seconds : int
            Time-to-live value to validate

        Raises
        ------
        ValueError
            If ttl_seconds is not positive
            If ttl_seconds is not an integer
        """
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")

    def _validate_cache_key(self, cache_key: str) -> None:
        """Validate that cache key is a non-empty string.

        Parameters
        ----------
        cache_key : str
            Cache key to validate

        Raises
        ------
        ValueError
            If cache_key is empty
            If cache_key is not a string
        """
        if not cache_key.strip():
            raise ValueError("cache_key cannot be empty")