from typing import Any, Callable, Optional


class CacheInvalidator:
    """Utility to invalidate caches based on external conditions."""
    
    def __init__(self, cache_clear_method: str) -> None:
        self.cache_clear_method = cache_clear_method
    
    def invalidate(self, instance: Any, condition: Optional[Callable[[], bool]] = None) -> None:
        """Invalidate cache if condition is met or unconditionally."""
        if condition is None or condition():
            clear_method = getattr(instance, self.cache_clear_method, None)
            if callable(clear_method):
                clear_method()
            else:
                raise AttributeError(f"Cache-clearing method '{self.cache_clear_method}' not found")