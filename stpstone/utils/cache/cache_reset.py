"""Utilities for cache management in calendar operations.

This module provides decorators and utilities for managing cache operations in calendar-based
applications. It includes class-based decorators for enforcing cache clearing before method
execution and automatic application of cache reset behavior to specified methods.
"""

from functools import wraps
from typing import Any, Callable


class CacheResetDecorator:
    """A decorator class to enforce calling a specified cache-clearing method before execution.
    
    This decorator ensures that specified cache-clearing methods are called before the decorated
    method executes, providing automatic cache invalidation for calendar operations.
    """
    
    def __init__(
        self, 
        cache_clear_method: str, 
        force_refresh: bool = True
    ) -> None:
        """Initialize the decorator with the cache-clearing method name.
        
        Parameters
        ----------
        cache_clear_method : str
            Name of the method to call for clearing the cache
        force_refresh : bool, default=True
            If True, clears cache on every call; if False, skips clearing
        
        Raises
        ------
        ValueError
            If cache_clear_method is empty or not a string
        """
        self._validate_cache_clear_method(cache_clear_method)
        self.cache_clear_method = cache_clear_method
        self.force_refresh = force_refresh
    
    def _validate_cache_clear_method(
        self, 
        cache_clear_method: str
    ) -> None:
        """Validate cache clear method parameter.
        
        Parameters
        ----------
        cache_clear_method : str
            Name of the cache clearing method to validate
        
        Raises
        ------
        ValueError
            If cache_clear_method is empty
            If cache_clear_method is not a string
        """
        if not isinstance(cache_clear_method, str):
            raise ValueError("cache_clear_method must be a string")
        if not cache_clear_method.strip():
            raise ValueError("cache_clear_method cannot be empty")
    
    def __call__(self, method: Callable) -> Callable:
        """Decorate the method to call the cache-clearing method first.
        
        Parameters
        ----------
        method : Callable
            The method to decorate
        
        Returns
        -------
        Callable
            The decorated method
        
        Raises
        ------
        AttributeError
            If cache-clearing method is not found on the instance
        """
        @wraps(method)
        def wrapper(
            instance: Any, # noqa ANN401: typing.Any is not allowed
            *args: Any, # noqa ANN401: typing.Any is not allowed
            **kwargs: Any # noqa ANN401: typing.Any is not allowed
        ) -> Any: # noqa ANN401: typing.Any is not allowed
            """Decorate the method to call the cache-clearing method first.
            
            Parameters
            ----------
            instance : Any
                The instance on which the method is called
            args : Any
                Positional arguments passed to the method
            kwargs : Any
                Keyword arguments passed to the method
            
            Returns
            -------
            Any
                The result of the decorated method
            
            Raises
            ------
            AttributeError
                If cache-clearing method is not found on the instance
            """
            if self.force_refresh:
                try:
                    clear_method = getattr(instance, self.cache_clear_method, None)
                    if callable(clear_method):
                        clear_method()
                    else:
                        raise AttributeError(
                            f"Cache-clearing method '{self.cache_clear_method}' "
                            "not found or not callable"
                        )
                except Exception as err:
                    raise AttributeError(
                        "Failed to access cache-clearing "
                        f"method '{self.cache_clear_method}': {str(err)}") from err
            return method(instance, *args, **kwargs)
        return wrapper


def clear_multiple_caches(
    method: Callable, 
    cache_clear_methods: list[str]
) -> Callable:
    """Decorator to enforce calling multiple cache-clearing methods before execution.
    
    Parameters
    ----------
    method : Callable
        The method to decorate
    cache_clear_methods : list[str]
        Names of the methods to call for clearing caches
    
    Returns
    -------
    Callable
        The decorated method
    
    Raises
    ------
    ValueError
        If cache_clear_methods is empty or contains invalid method names
    AttributeError
        If any cache-clearing method is not found on the instance
    """
    _validate_cache_clear_methods(cache_clear_methods)
    
    @wraps(method)
    def wrapper(
        instance: Any, # noqa ANN401: typing.Any is not allowed
        *args: Any, # noqa ANN401: typing.Any is not allowed
        **kwargs: Any # noqa ANN401: typing.Any is not allowed
    ) -> Any: # noqa ANN401: typing.Any is not allowed
        """Decorate the method to call multiple cache-clearing methods.
        
        Parameters
        ----------
        instance : Any
            The instance on which the method is called
        args : Any
            Positional arguments passed to the method
        kwargs : Any
            Keyword arguments passed to the method
        
        Returns
        -------
        Any
            The result of the decorated method
        
        Raises
        ------
        AttributeError
            If any cache-clearing method is not found on the instance
        """
        for cache_clear_method in cache_clear_methods:
            try:
                clear_method = getattr(instance, cache_clear_method, None)
                if callable(clear_method):
                    clear_method()
                else:
                    raise AttributeError(
                        f"Cache-clearing method '{cache_clear_method}' not found or not callable"
                    )
            except Exception as err:
                raise AttributeError(
                    f"Failed to access cache-clearing method '{cache_clear_method}': {str(err)}"
                ) from err
        return method(instance, *args, **kwargs)
    return wrapper


def _validate_cache_clear_methods(cache_clear_methods: list[str]) -> None:
    """Validate cache clear methods list.
    
    Parameters
    ----------
    cache_clear_methods : list[str]
        List of cache clearing method names to validate
    
    Raises
    ------
    ValueError
        If cache_clear_methods is not a list
        If cache_clear_methods is empty
        If any method name is not a string or is empty
    """
    if not isinstance(cache_clear_methods, list):
        raise ValueError("cache_clear_methods must be a list")
    if not cache_clear_methods:
        raise ValueError("cache_clear_methods cannot be empty")
    
    for i, method_name in enumerate(cache_clear_methods):
        if not isinstance(method_name, str):
            raise ValueError(f"Method name at index {i} must be a string")
        if not method_name.strip():
            raise ValueError(f"Method name at index {i} cannot be empty")


def auto_cache_reset_methods(
    method_cache_pairs: list[tuple[str, list[str]]]
) -> Callable[[type], type]:
    """Class decorator to apply CacheResetDecorator to specified methods.

    This decorator applies CacheResetDecorator to specified methods in a class, with their 
    respective cache-clearing methods. It provides automatic cache management for calendar
    operations by ensuring cache invalidation occurs before method execution.
    
    Parameters
    ----------
    method_cache_pairs : list[tuple[str, list[str]]]
        List of tuples, each containing a method name and a list of cache-clearing 
        method names to apply to that method
    
    Returns
    -------
    Callable[[type], type]
        The class decorator function
    
    Raises
    ------
    ValueError
        If method_cache_pairs is invalid or contains empty method names
    
    Examples
    --------
    >>> method_cache_pairs = [
    ...     ("holidays", ["clear_holidays_cache"]),
    ...     ("holidays_in_year", ["clear_holidays_cache"]),
    ...     ("working_days_range", ["clear_holidays_cache", "clear_working_days_cache"])
    ... ]
    >>> @auto_cache_reset_methods(method_cache_pairs)
    ... class CalendarManager:
    ...     pass
    """
    _validate_method_cache_pairs(method_cache_pairs)
    
    def decorator(cls: type) -> type:
        """Apply CacheResetDecorator or clear_multiple_caches to specified methods.
        
        Parameters
        ----------
        cls : type
            The class to decorate
        
        Returns
        -------
        type
            The decorated class
        """
        for method_name, cache_clear_methods in method_cache_pairs:
            if hasattr(cls, method_name):
                original_method = getattr(cls, method_name)
                if len(cache_clear_methods) == 1:
                    decorated_method = CacheResetDecorator(cache_clear_methods[0])(original_method)
                else:
                    decorated_method = clear_multiple_caches(original_method, cache_clear_methods)
                setattr(cls, method_name, decorated_method)
        return cls
    return decorator


def _validate_method_cache_pairs(method_cache_pairs: list[tuple[str, list[str]]]) -> None:
    """Validate method cache pairs parameter.
    
    Parameters
    ----------
    method_cache_pairs : list[tuple[str, list[str]]]
        List of method-cache pairs to validate
    
    Raises
    ------
    ValueError
        If method_cache_pairs is not a list
        If method_cache_pairs is empty
        If any pair is not a tuple or has incorrect structure
        If method names or cache method names are invalid
    """
    if not isinstance(method_cache_pairs, list):
        raise ValueError("method_cache_pairs must be a list")
    if not method_cache_pairs:
        raise ValueError("method_cache_pairs cannot be empty")
    
    for i, pair in enumerate(method_cache_pairs):
        if not isinstance(pair, tuple) or len(pair) != 2:
            raise ValueError(f"Pair at index {i} must be a tuple of length 2")
        
        method_name, cache_methods = pair
        
        if not isinstance(method_name, str):
            raise ValueError(f"Method name at index {i} must be a string")
        if not method_name.strip():
            raise ValueError(f"Method name at index {i} cannot be empty")
        
        if not isinstance(cache_methods, list):
            raise ValueError(f"Cache methods at index {i} must be a list")
        if not cache_methods:
            raise ValueError(f"Cache methods list at index {i} cannot be empty")
        
        for j, cache_method in enumerate(cache_methods):
            if not isinstance(cache_method, str):
                raise ValueError(f"Cache method at index {i}, {j} must be a string")
            if not cache_method.strip():
                raise ValueError(f"Cache method at index {i}, {j} cannot be empty")