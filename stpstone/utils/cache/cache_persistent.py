from functools import wraps
from logging import Logger
from pathlib import Path
import pickle
from threading import Lock
from typing import Any, Callable, Optional

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.pickle import PickleFiles


class PersistentCacheDecorator(metaclass=TypeChecker):
    """Decorator to cache method results to a file."""
    
    def __init__(
        self, 
        path_cache: str, 
        cache_key: str, 
        bool_persist_cache: bool = True, 
        logger: Optional[Logger] = None
    ) -> None:
        self.path_cache = Path(path_cache).with_suffix(".pkl")
        self.cache_key = cache_key
        self.bool_persist_cache = bool_persist_cache
        self.cache = self._load_cache()
        self._lock = Lock()
        self.logger = logger
    
    def _load_cache(self) -> dict:
        if self.path_cache.exists():
            try:
                return PickleFiles().load_message(self.path_cache)
            except (pickle.PickleError, EOFError, AttributeError) as err:
                CreateLog().log_message(self.logger, f"Failed to load cache: {err}", "error")
                raise ValueError(f"Failed to load cache: {err}")
        return {}
    
    def _save_cache(self, cache: dict) -> None:
        if not self.path_cache.parent.exists():
            self.path_cache.parent.mkdir(parents=True)
        if self.bool_persist_cache:
            with self._lock:
                try:
                    return PickleFiles().dump_message(cache, self.cache)
                except pickle.PickleError as e:
                    CreateLog().log_message(
                        self.logger, 
                        f"Warning: Failed to save cache to {self.path_cache}: {e}", 
                        "error"
                    )
                    raise ValueError(f"Failed to save cache to {self.path_cache}: {e}")

    def clear_cache(self) -> None:
        """Clear the in-memory and file-based cache."""
        with self._lock:
            self.cache = {}
            if self.path_cache.exists():
                try:
                    self.path_cache.unlink()
                except Exception as e:
                    print(f"Warning: Failed to delete cache file {self.path_cache}: {e}")
    
    def __call__(self, method: Callable) -> Callable:
        @wraps(method)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            # check self.bool_persist_cache if available, otherwise use decorator's bool_persist_cache
            persist = getattr(self, 'bool_persist_cache', self.bool_persist_cache)
            key = f"{self.cache_key}:{args}:{kwargs}"
            with self._lock:
                if key in self.cache:
                    return self.cache[key]
                result = method(self, *args, **kwargs)
                self.cache[key] = result
                if persist:
                    self._save_cache(self.cache)
                return result
        return wrapper