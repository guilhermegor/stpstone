"""Utilities for cache management in calendar operations."""

from functools import wraps
from typing import Any, Callable, Type


class CacheResetDecorator:
    """A decorator class to enforce calling a specified cache-clearing method before execution.
    
    Attributes:
        cache_clear_method (str): Name of the method to call for clearing the cache.
        force_refresh (bool): Whether to enforce cache clearing on every call (default: True).
    """
    
    def __init__(self, cache_clear_method: str, force_refresh: bool = True) -> None:
        """Initialize the decorator with the cache-clearing method name.
        
        Args:
            cache_clear_method (str): Name of the method to call for clearing the cache.
            force_refresh (bool): If True, clears cache on every call; if False, skips clearing.
        """
        self.cache_clear_method = cache_clear_method
        self.force_refresh = force_refresh
    
    def __call__(self, method: Callable) -> Callable:
        """Decorate the method to call the cache-clearing method first.
        
        Args:
            method (Callable): The method to decorate.
        
        Returns:
            Callable: The decorated method.
        """
        @wraps(method)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            if self.force_refresh:
                clear_method = getattr(self, self.cache_clear_method, None)
                if callable(clear_method):
                    clear_method()
                else:
                    raise AttributeError(f"Cache-clearing method '{self.cache_clear_method}' not found")
            return method(self, *args, **kwargs)
        return wrapper


def clear_multiple_caches(method: Callable, cache_clear_methods: list[str]) -> Callable:
    """Decorator to enforce calling multiple cache-clearing methods before execution.
    
    Args:
        method (Callable): The method to decorate.
        cache_clear_methods (list[str]): Names of the methods to call for clearing caches.
    
    Returns:
        Callable: The decorated method.
    """
    @wraps(method)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        for cache_clear_method in cache_clear_methods:
            clear_method = getattr(self, cache_clear_method, None)
            if callable(clear_method):
                clear_method()
            else:
                raise AttributeError(f"Cache-clearing method '{cache_clear_method}' not found")
        return method(self, *args, **kwargs)
    return wrapper

def auto_cache_reset_methods(
    method_cache_pairs: list[tuple[str, list[str]]]
) -> Callable[[Type], Type]:
    """Class decorator to apply CacheResetDecorator to specified methods.

    This decorator applies CacheResetDecorator to specified methods in a class, with their 
    respective cache-clearing methods.
    
    Args:
        method_cache_pairs (list[tuple[str, list[str]]]): list of tuples, each containing a method name
            and a list of cache-clearing method names to apply to that method.
    
    Returns:
        Callable[[Type], Type]: The class decorator.
    
    Example:
        method_cache_pairs = [
            ("holidays", ["clear_holidays_cache"]),
            ("holidays_in_year", ["clear_holidays_cache"]),
            ("working_days_range", ["clear_holidays_cache", "clear_working_days_cache"])
        ]
    """
    def decorator(cls: Type) -> Type:
        """Apply CacheResetDecorator or clear_multiple_caches to specified methods.
        
        Parameters
        ----------
        cls : Type
            The class to decorate.
        
        Returns
        -------
        Type
            The decorated class.
        """
        for method_name, cache_clear_methods in method_cache_pairs:
            if hasattr(cls, method_name):
                original_method = getattr(cls, method_name)
                if len(cache_clear_methods) == 1:
                    setattr(cls, method_name, CacheResetDecorator(cache_clear_methods[0])(original_method))
                else:
                    setattr(cls, method_name, clear_multiple_caches(original_method, cache_clear_methods))
        return cls
    return decorator