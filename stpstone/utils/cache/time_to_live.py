from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Callable


class TTLCacheDecorator:
    """Decorator to cache method results with a time-to-live expiration."""
    
    def __init__(self, ttl_seconds: int, cache_key: str) -> None:
        self.ttl_seconds = ttl_seconds
        self.cache_key = cache_key
        self.cache = {}
        self.last_updated = {}
    
    def __call__(self, method: Callable) -> Callable:
        @wraps(method)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            key = f"{self.cache_key}:{args}:{kwargs}"
            current_time = datetime.now()
            if key in self.cache and \
                (current_time - self.last_updated.get(key, datetime.min)) \
                    < timedelta(seconds=self.ttl_seconds):
                return self.cache[key]
            result = method(self, *args, **kwargs)
            self.cache[key] = result
            self.last_updated[key] = current_time
            return result
        return wrapper