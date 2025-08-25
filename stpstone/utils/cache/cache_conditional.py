from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Optional


class ConditionalCacheReset:
    """Decorator to clear cache if a condition (e.g., file modification) is met."""
    
    def __init__(self, cache_clear_method: str, condition_file: Optional[str] = None) -> None:
        self.cache_clear_method = cache_clear_method
        self.condition_file = condition_file
        self.last_modified = None
    
    def __call__(self, method: Callable) -> Callable:
        @wraps(method)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            if self.condition_file:
                file_path = Path(self.condition_file)
                if file_path.exists():
                    current_modified = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if self.last_modified is None or current_modified > self.last_modified:
                        clear_method = getattr(self, self.cache_clear_method, None)
                        if callable(clear_method):
                            clear_method()
                        self.last_modified = current_modified
            return method(self, *args, **kwargs)
        return wrapper