"""Metaclass for automatic type checking of methods in a class."""

from functools import wraps
import inspect
from io import BufferedIOBase, BytesIO, RawIOBase
from logging import Logger
from numbers import Number
from typing import (
    IO,
    Any,
    BinaryIO,
    Callable,
    Literal,
    Protocol,
    Union,
    get_args,
    get_origin,
    get_type_hints,
    runtime_checkable,
)
from unittest.mock import Mock

import numpy as np
import pandas as pd
from psycopg.sql import Composable
from pydantic_core import core_schema


@runtime_checkable
class SQLComposable(Protocol):
    """Database-agnostic protocol for SQL composable objects."""

    def __str__(self) -> str: 
        """Return a string representation of the SQL composable object."""
        ...

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: type[Any],
        _handler: Callable[[Any], core_schema.CoreSchema],
    ) -> core_schema.CoreSchema:
        """Pydantic of SQL composable objects."""
        return core_schema.union_schema(
            [
                core_schema.str_schema(),
                core_schema.is_instance_schema(cls),
                core_schema.is_instance_schema(Composable),
            ]
        )


def validate_type(value: type[Any], expected_type: type[Any], param_name: str) -> None:
    """Validate that a value matches the expected type."""
    # skip type checking for Mock objects during testing
    if isinstance(value, Mock):
        return
    
    # handle numpy integer types as equivalent to Python int
    if expected_type is int and hasattr(value, 'dtype') and value.dtype.kind in ('i', 'u'):
        return
    
    origin = get_origin(expected_type)
    
    # handle Literal types
    if origin is Literal:
        args = get_args(expected_type)
        if value not in args:
            allowed_values = ", ".join(repr(arg) for arg in args)
            raise TypeError(
                f"{param_name} must be one of: {allowed_values}, got {repr(value)}"
            )
        return

    # handle Union types (including Optional)
    if origin is Union:
        args = get_args(expected_type)
        # check if any of the union types match
        for arg in args:
            if arg is type(None) and value is None:
                return
            try:
                validate_type(value, arg, param_name)
                return
            except TypeError:
                continue
        # if none matched, raise error
        type_names = [getattr(arg, '__name__', str(arg)) for arg in args]
        raise TypeError(f"{param_name} must be one of types: {', '.join(type_names)}, "
                        + f"got {type(value).__name__}")
    
    # handle generic types like List, dict, etc.
    elif origin is not None:
        if not isinstance(value, origin):
            raise TypeError(f"{param_name} must be of type {expected_type}, "
                            + f"got {type(value).__name__}")
        return
    
    # handle regular types
    elif isinstance(expected_type, type):
        if not isinstance(value, expected_type):
            raise TypeError(f"{param_name} must be of type {expected_type.__name__}, "
                            + f"got {type(value).__name__}")
        return
    
    # handle special cases or protocols
    else:
        # for protocols and other complex types, try isinstance
        try:
            if not isinstance(value, expected_type):
                raise TypeError(f"{param_name} must be of type {expected_type}, "
                                + f"got {type(value).__name__}")
        except TypeError:
            # if isinstance fails, skip validation for complex types
            pass


def create_type_checked_method(original_method: Callable[..., Any]) -> Callable[..., Any]:
    """Create a type-checked wrapper for a method."""
    @wraps(original_method)
    def wrapper(*args: type[Any], **kwargs: type[Any]) -> type[Any]:
        # get type hints for the method
        try:
            type_hints = get_type_hints(original_method)
        except (NameError, AttributeError):
            # if we can't get type hints, just call the original method
            return original_method(*args, **kwargs)
        
        # get parameter names
        sig = inspect.signature(original_method)
        param_names = list(sig.parameters.keys())
        
        # validate positional arguments
        for _, (arg, param_name) in enumerate(zip(args, param_names)):
            if param_name in type_hints and param_name != 'self':
                validate_type(arg, type_hints[param_name], param_name)
        
        # validate keyword arguments
        for param_name, value in kwargs.items():
            if param_name in type_hints:
                validate_type(value, type_hints[param_name], param_name)
        
        return original_method(*args, **kwargs)
    
    return wrapper


class TypeChecker(type):
    """Metaclass that automatically adds runtime type checking to methods."""
    
    def __new__(
        cls: type["TypeChecker"], name: str, bases: tuple, dict_: dict[str, Any]
    ) -> "TypeChecker":
        """Create a new class with type-checked methods."""
        # types that should be ignored for type checking
        tup_types_ignore = (
            pd.DataFrame,
            pd.Series,
            np.ndarray,
            list,
            Logger,
            Number,
        )

        tup_special_types = ( # noqa: F841 - local variable is assigned but not used
            *tup_types_ignore,
            IO,
            BinaryIO,
            RawIOBase,
            BufferedIOBase,
            BytesIO,
            SQLComposable,
            Composable,
        )

        # process all methods in the class
        for attr_name, attr_value in dict_.items():
            if callable(attr_value) and not attr_name.startswith("__"):
                # Add type checking wrapper
                dict_[attr_name] = create_type_checked_method(attr_value)
        
        # special handling for __init__ method
        if "__init__" in dict_:
            original_init = dict_["__init__"]
            dict_["__init__"] = create_type_checked_method(original_init)

        return super().__new__(cls, name, bases, dict_)


# Enhanced version with additional features
class AdvancedTypeChecker(type):
    """Advanced metaclass with more features for type checking."""
    
    def __new__(
        cls: type["AdvancedTypeChecker"], name: str, bases: tuple, dict_: dict[str, Any]
    ) -> "AdvancedTypeChecker":
        """Create a new class with advanced type-checked methods."""
        # store type checking configuration
        type_check_config = dict_.get('_type_check_config', {})
        strict_mode = type_check_config.get('strict', True)
        check_return_types = type_check_config.get('check_returns', False)
        excluded_methods = type_check_config.get('exclude', set())
        
        def create_advanced_type_checked_method(
            original_method: Callable[..., Any],
            method_name: str
        ) -> Callable[..., Any]:
            """Create an advanced type-checked wrapper for a method."""
            @wraps(original_method)
            def wrapper(*args: type[Any], **kwargs: type[Any]) -> type[Any]:
                if method_name in excluded_methods:
                    return original_method(*args, **kwargs)
                
                # get type hints for the method
                try:
                    type_hints = get_type_hints(original_method)
                except (NameError, AttributeError) as err:
                    if strict_mode:
                        msg = f"Could not get type hints for {method_name}"
                        raise RuntimeError(msg) from err
                    return original_method(*args, **kwargs)
                
                # get parameter names
                sig = inspect.signature(original_method)
                param_names = list(sig.parameters.keys())
                
                # validate positional arguments
                for _, (arg, param_name) in enumerate(zip(args, param_names)):
                    if param_name in type_hints and param_name != 'self':
                        try:
                            validate_type(arg, type_hints[param_name], param_name)
                        except TypeError as e:
                            if strict_mode:
                                msg = f"In method {method_name}: {e}"
                                raise TypeError(msg) from e
                            # In non-strict mode, just issue a warning
                            print(f"Warning in {method_name}: {e}")
                
                # validate keyword arguments
                for param_name, value in kwargs.items():
                    if param_name in type_hints:
                        try:
                            validate_type(value, type_hints[param_name], param_name)
                        except TypeError as e:
                            if strict_mode:
                                msg = f"In method {method_name}: {e}"
                                raise TypeError(msg) from e
                            print(f"Warning in {method_name}: {e}")
                
                # call the original method
                result = original_method(*args, **kwargs)
                
                # validate return type if requested
                if check_return_types and 'return' in type_hints:
                    try:
                        validate_type(result, type_hints['return'], 'return value')
                    except TypeError as e:
                        if strict_mode:
                            msg = f"In method {method_name} return: {e}"
                            raise TypeError(msg) from e
                        print(f"Warning in {method_name} return: {e}")
                
                return result
            
            return wrapper

        # process all methods in the class
        for attr_name, attr_value in dict_.items():
            if callable(attr_value) and not attr_name.startswith("__"):
                dict_[attr_name] = create_advanced_type_checked_method(attr_value, attr_name)
        
        # special handling for __init__ method
        if "__init__" in dict_:
            original_init = dict_["__init__"]
            dict_["__init__"] = create_advanced_type_checked_method(original_init, "__init__")

        return super().__new__(cls, name, bases, dict_)


# decorator version for individual methods
def type_check(func: Callable[..., Any]) -> Callable[..., Any]:
    """Add type checking to individual methods."""
    return create_type_checked_method(func)


# example usage with configuration
class ConfigurableTypeChecker(type):
    """Metaclass that respects configuration for type checking behavior."""
    
    def __new__(
        cls: type["ConfigurableTypeChecker"], name: str, bases: tuple, dict_: dict[str, Any]
    ) -> "ConfigurableTypeChecker":
        """Create a new class with configurable type checking."""
        # Configuration options
        config = dict_.get('__type_check_config__', {})
        enabled = config.get('enabled', True)
        strict = config.get('strict', True)
        exclude_methods = config.get('exclude_methods', set())
        include_private = config.get('include_private', False)
        
        if not enabled:
            return super().__new__(cls, name, bases, dict_)
        
        def should_check_method(method_name: str) -> bool:
            if method_name in exclude_methods:
                return False
            return include_private or not method_name.startswith('_')
        
        def create_configurable_type_checked_method(
            original_method: Callable[..., Any],
            method_name: str
        ) -> Callable[..., Any]:
            """Create a configurable type-checked wrapper for a method."""
            @wraps(original_method)
            def wrapper(*args: type[Any], **kwargs: type[Any]) -> type[Any]:
                if not should_check_method(method_name):
                    return original_method(*args, **kwargs)
                
                # Get type hints for the method
                try:
                    type_hints = get_type_hints(original_method)
                except (NameError, AttributeError) as err:
                    if strict:
                        msg = f"Could not get type hints for {method_name}"
                        raise RuntimeError(msg) from err
                    return original_method(*args, **kwargs)
                
                # Get parameter names
                sig = inspect.signature(original_method)
                param_names = list(sig.parameters.keys())
                
                # validate positional arguments
                for _, (arg, param_name) in enumerate(zip(args, param_names)):
                    if param_name in type_hints and param_name != 'self':
                        try:
                            validate_type(arg, type_hints[param_name], param_name)
                        except TypeError as e:
                            if strict:
                                msg = f"In method {method_name}: {e}"
                                raise TypeError(msg) from e
                            # In non-strict mode, just issue a warning
                            print(f"Warning in {method_name}: {e}")
                
                # validate keyword arguments
                for param_name, value in kwargs.items():
                    if param_name in type_hints:
                        try:
                            validate_type(value, type_hints[param_name], param_name)
                        except TypeError as e:
                            if strict:
                                msg = f"In method {method_name}: {e}"
                                raise TypeError(msg) from e
                            print(f"Warning in {method_name}: {e}")
                
                # Call the original method
                result = original_method(*args, **kwargs)
                
                return result
            
            return wrapper
        
        # process methods
        for attr_name, attr_value in dict_.items():
            if callable(attr_value) and should_check_method(attr_name):
                dict_[attr_name] = create_configurable_type_checked_method(attr_value, attr_name)
        
        # special handling for __init__ method if it should be checked
        if "__init__" in dict_ and should_check_method("__init__"):
            original_init = dict_["__init__"]
            dict_["__init__"] = create_configurable_type_checked_method(original_init, "__init__")

        return super().__new__(cls, name, bases, dict_)