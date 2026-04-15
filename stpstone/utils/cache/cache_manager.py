"""Cache manager for caching expensive data fetches.

This module provides a class for caching method results in memory and optionally persisting
them to a file using pickle serialization.

Example
-------
```python
from logging import Logger
from typing import Optional
import pandas as pd
from stpstone.utils.cache.cache_manager import CacheManager

class DataProcessor:
    def __init__(self, cache_dir: Optional[str] = None, logger: Optional[Logger] = None):
        self.cls_cache_manager = CacheManager(
            bool_persist_cache=True,
            bool_reuse_cache=True,
            int_days_cache_expiration=1,
            int_cache_ttl_days=30,
            path_cache_dir=cache_dir,
            logger=logger
        )

    @CacheManager.cache_df(key="raw_data_fetched")
    def fetch_data(self) -> pd.DataFrame:
        return pd.DataFrame({"data": [1, 2, 3]})

processor = DataProcessor(cache_dir="/tmp/cache")
df_ = processor.fetch_data()  # fetches and caches data
df_cached = processor.fetch_data()  # returns cached data
```
"""

from collections.abc import Callable
from datetime import datetime, timedelta
from functools import wraps
from logging import Logger
import os
from pathlib import Path
import pickle
import platform
from typing import Any, Optional, TypeVar

import pandas as pd

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker, type_checker
from stpstone.utils.calendars.calendar_abc import ABCCalendarOperations
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.pickle import PickleFiles


T = TypeVar("T")


class CacheManager(metaclass=TypeChecker):
    """A class for caching method results in memory and optionally persisting them to a file."""

    def __init__(
        self, 
        bool_persist_cache: bool = True, 
        bool_reuse_cache: bool = True,
        int_days_cache_expiration: int = 1,
        int_cache_ttl_days: int = 30,
        path_cache_dir: Optional[str] = None,
        logger: Optional[Logger] = None
    ) -> None:
        """Initialize the CacheManager class.
        
        Parameters
        ----------
        bool_persist_cache : bool
            If True, saves cache to disk; if False, uses in-memory cache only (default: True)
        bool_reuse_cache : bool
            If True, caches in-memory; if False, does not cache in-memory (default: True)
        int_days_cache_expiration : int
            Number of days after which the cache expires (default: 1)
        int_cache_ttl_days : int
            Number of days after which the cache is considered expired (default: 30)
        path_cache_dir : Optional[str]
            Path to the cache directory (default: None)
        logger : Optional[Logger]
            The logger to use (default: None)
        
        Returns
        -------
        None
        
        Notes
        -----
        [1] The cache directory is created if it does not exist
        [2] TTL stands for time-to-live, or retention time period for a given resource
        """
        self.bool_persist_cache = bool_persist_cache
        self.bool_reuse_cache = bool_reuse_cache
        self.logger = logger
        self._cache = {}
        self._path_cache_dir = self._get_cache_dir_path(path_cache_dir)
        self.timedelta_cache_expiry = timedelta(days=int_days_cache_expiration)
        self.timedelta_cache_ttl_days = timedelta(days=int_cache_ttl_days)
        self.cls_create_log = CreateLog()

    def _get_cache_dir_path(self, path_cache_dir: Optional[str]) -> str:
        """Get the path to the cache directory.
        
        Parameters
        ----------
        path_cache_dir : Optional[str]
            Path to the cache directory
        
        Returns
        -------
        str
            Path to the cache directory
        """
        if path_cache_dir:
            path_cache_dir = Path(path_cache_dir)
        else:
            if platform.system() == "Windows":
                path_cache_dir = Path(os.getenv("APPDATA")) / "stpstone_calendar_cache"
            else:
                path_cache_dir = Path.home() / ".cache" / "stpstone_calendar_cache"
        path_cache_dir.mkdir(parents=True, exist_ok=True)
        return path_cache_dir

    @staticmethod
    def cache_df(key: str) -> Callable:
        """Decorate a function to cache its output as a DataFrame.
        
        Parameters
        ----------
        key : str
            Key to use for caching
        
        Returns
        -------
        Callable
            Decorated function
        
        Raises
        ------
        ValueError
            If there is an error generating the cache key from callable.
        AttributeError
            If the instance does not have a 'cls_cache_manager' attribute
        """
        @type_checker
        def decorator(func: Callable) -> Callable:
            """Decorate a function to cache its output as a DataFrame.

            Parameters
            ----------
            func : Callable
                The function to decorate

            Returns
            -------
            Callable
                Decorated function

            Raises
            ------
            ValueError
                If there is an error generating the cache key from callable.
            AttributeError
                If the instance does not have a 'cls_cache_manager' attribute
            """
            @type_checker
            @wraps(func)
            def wrapper(
                self: Any, # noqa ANN401: typing.Any is not allowed
                *args: Any, # noqa ANN401: typing.Any is not allowed
                **kwargs: Any # noqa ANN401: typing.Any is not allowed
            ) -> pd.DataFrame:
                """Wrap the function to cache its output as a DataFrame.

                Parameters
                ----------
                self : Any
                    Instance of the class
                *args : Any
                    Variable-length argument list
                **kwargs : Any
                    Arbitrary keyword arguments

                Returns
                -------
                pd.DataFrame
                    Cached DataFrame

                Raises
                ------
                ValueError
                    If there is an error generating the cache key from callable.
                AttributeError
                    If the instance does not have a 'cls_cache_manager' attribute
                """
                cls_cache_manager = getattr(self, 'cls_cache_manager', None)
                if cls_cache_manager is None:
                    raise AttributeError("Instance must have 'cls_cache_manager' attribute")
                
                if callable(key):
                    try:
                        cache_key = key(self)
                    except Exception as e:
                        raise ValueError(f"Error generating cache key from callable: {e}") from e
                else:
                    cache_key = key
                
                df_cached = cls_cache_manager._load_cache(key=cache_key)
                if df_cached is not None and not df_cached.empty \
                    and cls_cache_manager.bool_reuse_cache:
                    cls_cache_manager.cls_create_log.log_message(
                        cls_cache_manager.logger,
                        f"Using cached holidays from {cache_key}. "
                        f"Path: {cls_cache_manager._path_cache_dir}", 
                        "info"
                    )
                    return df_cached
                cls_cache_manager.cls_create_log.log_message(
                    cls_cache_manager.logger,
                    f"Fetching holidays from {cache_key}", 
                    "info"
                )
                df_ = func(self, *args, **kwargs)
                cls_cache_manager._save_cache(key=cache_key, df_=df_)
                cls_cache_manager._save_cache(
                    key=f"{cache_key}_{ABCCalendarOperations().current_timestamp_string()}", 
                    df_=df_
                )
                cls_cache_manager._clean_old_cache()
                return df_
            return wrapper
        return decorator
    
    @staticmethod
    def cache_value(key: str) -> Callable:
        """Decorate a function to cache its output as a generic value.

        Parameters
        ----------
        key : str
            Key to use for caching

        Returns
        -------
        Callable
            Decorated function

        Raises
        ------
        ValueError
            If there is an error generating the cache key from callable.
        AttributeError
            If the instance does not have a 'cls_cache_manager' attribute
        """
        @type_checker
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            """Decorate a function to cache its output as a generic value.

            Parameters
            ----------
            func : Callable[..., T]
                The function to decorate

            Returns
            -------
            Callable[..., T]
                Decorated function

            Raises
            ------
            ValueError
                If there is an error generating the cache key from callable.
            AttributeError
                If the instance does not have a 'cls_cache_manager' attribute
            """
            @type_checker
            @wraps(func)
            def wrapper(
                self: Any, # noqa ANN401: typing.Any is not allowed
                *args: Any, # noqa ANN401: typing.Any is not allowed
                **kwargs: Any # noqa ANN401: typing.Any is not allowed
            ) -> T:
                """Wrap the function to cache its output as a generic value.

                Parameters
                ----------
                self : Any
                    Instance of the class
                *args : Any
                    Variable-length argument list
                **kwargs : Any
                    Arbitrary keyword arguments

                Returns
                -------
                T
                    Cached value

                Raises
                ------
                ValueError
                    If there is an error generating the cache key from callable.
                AttributeError
                    If the instance does not have a 'cls_cache_manager' attribute
                """
                cls_cache_manager = getattr(self, 'cls_cache_manager', None)
                if cls_cache_manager is None:
                    raise AttributeError("Instance must have 'cls_cache_manager' attribute")
                
                if callable(key):
                    try:
                        cache_key = key(self)
                    except Exception as e:
                        raise ValueError(f"Error generating cache key from callable: {e}") from e
                else:
                    cache_key = key
                
                cached_value = cls_cache_manager._load_cache_value(key=cache_key)
                if cached_value is not None and cls_cache_manager.bool_reuse_cache:
                    cls_cache_manager.cls_create_log.log_message(
                        cls_cache_manager.logger,
                        f"Using cached value from {cache_key}. "
                        f"Path: {cls_cache_manager._path_cache_dir}", 
                        "debug"
                    )
                    return cached_value
                
                cls_cache_manager.cls_create_log.log_message(
                    cls_cache_manager.logger,
                    f"Fetching value from {cache_key}", 
                    "info"
                )
                value = func(self, *args, **kwargs)
                cls_cache_manager._save_cache_value(key=cache_key, value=value)
                cls_cache_manager._save_cache_value(
                    key=f"{cache_key}_{ABCCalendarOperations().current_timestamp_string()}", 
                    value=value
                )
                cls_cache_manager._clean_old_cache()
                return value
            return wrapper
        return decorator
    
    def _load_cache(self, key: str) -> Optional[pd.DataFrame]:
        """Load a DataFrame from the cache file for a given key.
        
        Parameters
        ----------
        key : str
            Key for the cache file
        
        Returns
        -------
        Optional[pd.DataFrame]
            DataFrame loaded from the cache file

        Raises
        ------
        ValueError
            If there is an error loading the cache
        """
        if self.bool_reuse_cache:
            df_cached = self._cache.get(key)
            if df_cached is not None and self._validate_cached_dataframe(df_cached):
                return df_cached

        if not self.bool_persist_cache:
            return None
        
        path_cache_file = self._get_cache_file_path(key)
        if path_cache_file.exists():
            try:
                cache_data = PickleFiles().load_message(path_cache_file, bool_trusted_file=True)
                datetime_creation, df_ = cache_data
                if datetime.now() - datetime_creation < self.timedelta_cache_expiry:
                    if self._validate_cached_dataframe(df_):
                        return df_
                else:
                    path_cache_file.unlink()
            except (pickle.PickleError, EOFError, FileNotFoundError) as err:
                path_cache_file.unlink(missing_ok=True)
                raise ValueError (
                    f"Warning: Failed to load cache from {path_cache_file}: {err}") from err
                
        return None
    
    def _get_cache_file_path(self, key: str) -> Path:
        """Get the path to the cache file for a given key.
        
        Parameters
        ----------
        key : str
            Key for the cache file
        
        Returns
        -------
        Path
            Path to the cache file
        """
        safe_key = "".join(c if c.isalnum() else "_" for c in key)
        return self._path_cache_dir / f"{safe_key}.pkl"

    def _validate_cached_dataframe(self, df_: pd.DataFrame) -> bool:
        """Validate a cached DataFrame.
        
        Parameters
        ----------
        df_ : pd.DataFrame
            DataFrame to validate
        
        Returns
        -------
        bool
            True if the DataFrame is valid, False otherwise

        Raises
        ------
        ValueError
            If the DataFrame is invalid
        """
        try:
            if df_ is None or not isinstance(df_, pd.DataFrame) or df_.empty:
                return False
            return not len(df_) < 1
        except Exception as err:
            raise ValueError(f"Warning: cache validation failed. Error: {err}") from err

    def _save_cache(self, key: str, df_: pd.DataFrame) -> None:
        """Save a DataFrame to the cache (in-memory and disk).
        
        Parameters
        ----------
        key : str
            Key for the cache file
        df_ : pd.DataFrame
            DataFrame to save to the cache file

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If there is an error saving the cache
        """
        if self._validate_cached_dataframe(df_):
            self._cache[key] = df_
            if self.bool_persist_cache:
                path_cache_file = self._get_cache_file_path(key)
                try:
                    with open(path_cache_file, "wb") as f:
                        pickle.dump((datetime.now(), df_), f)
                except Exception as err:
                    raise ValueError(
                        f"Warning: Failed to save cache to {path_cache_file}: {err}") from err
    
    def _clean_old_cache(self) -> None:
        """Clean old cache files.
        
        Returns
        -------
        None

        Raises
        ------
        ValueError
            If there is an error cleaning the cache
        """
        if not self.bool_persist_cache:
            return
        datetime_now = datetime.now()
        for cache_file in self._path_cache_dir.glob("*.pkl"):
            try:
                datetime_creation, _ = PickleFiles().load_message(
                    cache_file, bool_trusted_file=True)
                if datetime_now - datetime_creation > self.timedelta_cache_ttl_days:
                    cache_file.unlink()
            except (pickle.PickleError, EOFError, FileNotFoundError) as err:
                cache_file.unlink(missing_ok=True)
                raise ValueError(
                    f"Warning: Failed to load cache from {cache_file}: {err}") from err

    def clear_caches(self) -> None:
        """Clear the in-memory and disk caches.
        
        Returns
        -------
        None

        Raises
        ------
        ValueError
            If there is an error clearing the cache
        """
        self._cache.clear()
        if self.bool_persist_cache:
            for cache_file in self._path_cache_dir.glob("*.pkl"):
                try:
                    cache_file.unlink()
                except Exception as err:
                    raise ValueError(
                        f"Warning: Failed to clear cache file {cache_file}: {err}") from err