from functools import wraps
import json
from pathlib import Path
from typing import Any, Callable


class PersistentCacheDecorator:
    """Decorator to cache method results to a file."""
    
    def __init__(self, cache_file: str, cache_key: str) -> None:
        self.cache_file = Path(cache_file)
        self.cache_key = cache_key
        self.cache = self._load_cache()
    
    def _load_cache(self) -> dict:
        if self.cache_file.exists():
            with self.cache_file.open("r") as f:
                return json.load(f)
        return {}
    
    def _save_cache(self, cache: dict) -> None:
        with self.cache_file.open("w") as f:
            json.dump(cache, f)
    
    def __call__(self, method: Callable) -> Callable:
        @wraps(method)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            key = f"{self.cache_key}:{args}:{kwargs}"
            if key in self.cache:
                return self.cache[key]
            result = method(self, *args, **kwargs)
            self.cache[key] = result
            self._save_cache(self.cache)
            return result
        return wrapper