"""Metaclass for automatic type checking of methods in a class.

This metaclass provides automatic type checking for methods in a class, 
ensuring that the types of the arguments and return values match the specified 
types. It also allows for configuration of the type checking behavior.

Usage
-----
class MyClass(metaclass=TypeChecker):
    def add_numbers(self, x: int, y: int) -> int:
        return x + y

my_instance = MyClass()
my_instance.add_numbers(1, 2)  # This will raise a TypeError if the types don't match

@type_checker
def add_numbers(self, x: int, y: int) -> int:
    return x + y

add_numbers(1, 2)  # This will raise a TypeError if the types don't match
"""

from abc import ABCMeta
from functools import wraps
import inspect
from io import BufferedIOBase, BytesIO, RawIOBase
from logging import Logger
from numbers import Number
from pathlib import Path
from typing import (
    IO,
    Any,
    BinaryIO,
    Callable,
    Literal,
    Optional,
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
        """Return a string representation of the SQL composable object.
        
        Returns
        -------
        str
            String representation of the SQL composable object.
        """
        ...

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        _source_type: Any, # noqa ANN401: typing.Any is not allowed
        _handler: Any, # noqa ANN401: typing.Any is not allowed
    ) -> core_schema.CoreSchema:
        """Tell Pydantic how to handle this protocol.
        
        Parameters
        ----------
        cls : type
            Class of the SQL composable object.
        _source_type : Any
            Source type of the SQL composable object.
        _handler : Any
            Handler function for the SQL composable object.
        
        Returns
        -------
        core_schema.CoreSchema
            Core schema for SQL composable objects.
        """
        return core_schema.union_schema(
            [core_schema.str_schema(), core_schema.is_instance_schema(cls)]
        )


@runtime_checkable
class DbCursor(Protocol):
    """Protocol defining a generic database cursor interface."""

    def execute(
        self, 
        query: Union[str, SQLComposable], 
        params: Optional[Any] = None # noqa ANN401: typing.Any is not allowed
    ) -> Any: # noqa ANN401: typing.Any is not allowed
        """Execute a SQL query.

        Parameters
        ----------
        query : Union[str, SQLComposable]
            SQL query to execute
        params : Optional[Any]
            Parameters to pass to the query, defaults to None

        Returns
        -------
        Any
            Result of the query
        """
        ...
    
    def fetchone(self) -> Any: # noqa ANN401: typing.Any is not allowed
        """Fetch a single row from the query result.

        Returns
        -------
        Any
            Single row of the query result
        """
        ...

    def fetchall(self) -> list[Any]: 
        """Fetch all rows from the query result.

        Returns
        -------
        list[Any]
            List of all rows in the query result
        """
        ...
        
    def close(self) -> None: 
        """Close the cursor.
        
        Returns
        -------
        None
        """
        ...


@runtime_checkable
class DbConnection(Protocol):
    """Protocol defining a generic database connection interface."""

    def cursor(self) -> DbCursor:
        """Create a database cursor.

        Returns
        -------
        DbCursor
            Database cursor
        """
        ...
    
    def commit(self) -> None:
        """Commit changes to the database.
        
        Returns
        -------
        None
        """
        ...
    
    def rollback(self) -> None:
        """Rollback changes to the database.

        Returns
        -------
        None
        """
        ...
    
    def close(self) -> None:
        """Close the connection.
        
        Returns
        -------
        None
        """
        ...


def validate_type(
    value: Any, # noqa ANN401: typing.Any is not allowed
    expected_type: type, 
    param_name: str
) -> None:
    """Validate that a value matches the expected type.
    
    Parameters
    ----------
    value : Any
        The value to validate.
    expected_type : type
        The expected type of the value.
    param_name : str
        The name of the parameter being validated.

    Returns
    -------
    None
    
    Raises
    ------
    TypeError
        If the value does not match the expected type.
    """
    # skip type checking for Mock objects during testing
    if isinstance(value, Mock):
        return

    # handle numpy integer types as equivalent to Python int
    if expected_type is int and hasattr(value, 'dtype') and value.dtype.kind in ('i', 'u'):
        return

    # allow int where float is expected
    if expected_type is float and isinstance(value, (int, float)):
        return

    origin = get_origin(expected_type)

    # allow pathlib.Path for parameters expecting str when they represent file paths
    if expected_type is str and isinstance(value, Path) \
            and param_name in ('file_path', 'path_cache'):
        return

    # allow None or non-string types for specific parameters to be validated by
    # _validate_email_params
    if param_name in ('str_sender', 'subject', 'html_body', 'token') and expected_type is str:
        return

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

    # handle generic types like List[Callable[...]]
    elif origin is list:
        if not isinstance(value, list):
            raise TypeError(f"{param_name} must be of type list, "
                            + f"got {type(value).__name__}")
        element_type = get_args(expected_type)[0] if get_args(expected_type) else Any
        for i, elem in enumerate(value):
            if get_origin(element_type) is Callable:
                if not callable(elem):
                    raise TypeError(
                        f"{param_name}[{i}] must be of type {element_type.__name__}, "
                        f"got {type(elem).__name__}"
                    )
            else:
                validate_type(elem, element_type, f"{param_name}[{i}]")
        return

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
    """Create a type-checked wrapper for a method.
    
    Parameters
    ----------
    original_method : Callable[..., Any]
        The original method to wrap.
    
    Returns
    -------
    Callable[..., Any]
        The type-checked wrapper for the method.
    """
    @wraps(original_method)
    def wrapper(
        *args: Any, # noqa ANN401: typing.Any is not allowed
        **kwargs: Any # noqa ANN401: typing.Any is not allowed
    ) -> Any: # noqa ANN401: typing.Any is not allowed
        """Type-checked wrapper for the original method.
        
        Parameters
        ----------
        *args : Any
            Positional arguments to the original method.
        **kwargs : Any
            Keyword arguments to the original method.
        
        Returns
        -------
        Any
            The result of the original method.
        """
        # get type hints for the method
        try:
            type_hints = get_type_hints(original_method)
        except (NameError, AttributeError):
            # if we can't get type hints, just call the original method
            return original_method(*args, **kwargs)
        
        # get parameter names and inspect signature
        sig = inspect.signature(original_method)
        params = sig.parameters
        
        # validate positional arguments
        pos_arg_idx = 0
        for param_name, param in params.items():
            if param_name == 'self':
                pos_arg_idx += 1
                continue
            if param.kind == inspect.Parameter.VAR_POSITIONAL:
                # validate all remaining positional args against varargs type
                varargs_type = type_hints.get(param_name, Any)
                varargs_type = get_args(varargs_type)[0] if get_origin(varargs_type) is list \
                    else varargs_type
                while pos_arg_idx < len(args):
                    validate_type(args[pos_arg_idx], varargs_type, f"{param_name}[{pos_arg_idx}]")
                    pos_arg_idx += 1
            elif param.kind in (inspect.Parameter.POSITIONAL_ONLY, 
                                inspect.Parameter.POSITIONAL_OR_KEYWORD):
                if pos_arg_idx < len(args):
                    if param_name in type_hints:
                        validate_type(args[pos_arg_idx], type_hints[param_name], param_name)
                    pos_arg_idx += 1
        
        # validate keyword arguments
        for param_name, value in kwargs.items():
            if param_name in type_hints:
                validate_type(value, type_hints[param_name], param_name)
        
        return original_method(*args, **kwargs)
    
    return wrapper


# * decorator version for individual methods / functions
def type_checker(func: Callable[..., Any]) -> Callable[..., Any]:
    """Add type checking to individual methods.
    
    Parameters
    ----------
    func : Callable[..., Any]
        The method to add type checking to.
    
    Returns
    -------
    Callable[..., Any]
        The type-checked method.
    """
    return create_type_checked_method(func)


# * metaclass version for entire classes
class TypeChecker(type):
    """Metaclass that automatically adds runtime type checking to methods."""
    
    def __new__(
        cls: "TypeChecker", 
        name: str, 
        bases: tuple, 
        dict_: dict[str, Any]
    ) -> "TypeChecker":
        """Create a new class with type-checked methods.
        
        Parameters
        ----------
        cls : 'TypeChecker'
            The metaclass.
        name : str
            The name of the class.
        bases : tuple
            The base classes of the class.
        dict_ : dict[str, Any]
            The class dictionary.
        
        Returns
        -------
        'TypeChecker'
            The type-checked class.
        """
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


class ABCTypeCheckerMeta(ABCMeta, TypeChecker):
    """Meta class for type checking abstract base classes."""

    pass


class AdvancedTypeChecker(type):
    """Advanced metaclass with more features for type checking."""
    
    def __new__(
        cls: "AdvancedTypeChecker", 
        name: str, 
        bases: tuple, 
        dict_: dict[str, Any]
    ) -> "AdvancedTypeChecker":
        """Create a new class with advanced type-checked methods.
        
        Parameters
        ----------
        cls : 'AdvancedTypeChecker'
            The metaclass.
        name : str
            The name of the class.
        bases : tuple
            The base classes of the class.
        dict_ : dict[str, Any]
            The class dictionary.
        
        Returns
        -------
        'AdvancedTypeChecker'
            The advanced type-checked class.

        Raises
        ------
        RuntimeError
            If type hints cannot be obtained for a method.
        TypeError
            If type checking fails.
        """
        type_check_config = dict_.get('_type_check_config', {})
        strict_mode = type_check_config.get('strict', True)
        check_return_types = type_check_config.get('check_returns', False)
        excluded_methods = type_check_config.get('exclude', set())
        
        def create_advanced_type_checked_method(
            original_method: Callable[..., Any],
            method_name: str
        ) -> Callable[..., Any]:
            """Create an advanced type-checked wrapper for a method.
            
            Parameters
            ----------
            original_method : Callable[..., Any]
                The original method.
            method_name : str
                The name of the method.
            
            Returns
            -------
            Callable[..., Any]
                The advanced type-checked method.

            Raises
            ------
            RuntimeError
                If type hints cannot be obtained for the method.
            TypeError
                If type checking fails.
            """
            @wraps(original_method)
            def wrapper(
                *args: Any, # noqa ANN401: typing.Any is not allowed
                **kwargs: Any # noqa ANN401: typing.Any is not allowed
            ) -> Any: # noqa ANN401: typing.Any is not allowed
                """Wrap the original method with advanced type checking.

                Parameters
                ----------
                *args : Any
                    Positional arguments.
                **kwargs : Any
                    Keyword arguments.

                Returns
                -------
                Any
                    The result of the method.

                Raises
                ------
                RuntimeError
                    If type hints cannot be obtained for the method.
                TypeError
                    If type checking fails.
                """
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

# example usage with configuration
class ConfigurableTypeChecker(type):
    """Metaclass that respects configuration for type checking behavior."""
    
    def __new__(
        cls: "ConfigurableTypeChecker", 
        name: str, 
        bases: tuple, 
        dict_: dict[str, Any]
    ) -> "ConfigurableTypeChecker":
        """Create a new class with configurable type checking.
        
        Parameters
        ----------
        cls : 'ConfigurableTypeChecker'
            The metaclass.
        name : str
            The name of the class.
        bases : tuple
            The base classes of the class.
        dict_ : dict[str, Any]
            The class dictionary.
        
        Returns
        -------
        'ConfigurableTypeChecker'
            The configurable type-checked class.

        Raises
        ------
        RuntimeError
            If type hints cannot be obtained for a method.
        TypeError
            If type checking fails.
        """
        # configuration options
        config = dict_.get('__type_check_config__', {})
        enabled = config.get('enabled', True)
        strict = config.get('strict', True)
        exclude_methods = config.get('exclude_methods', set())
        include_private = config.get('include_private', False)
        
        if not enabled:
            return super().__new__(cls, name, bases, dict_)
        
        def should_check_method(method_name: str) -> bool:
            """Determine if a method should be type-checked.
            
            Parameters
            ----------
            method_name : str
                The name of the method.
            
            Returns
            -------
            bool
                True if the method should be type-checked, False otherwise.
            """
            if method_name in exclude_methods:
                return False
            return include_private or not method_name.startswith('_')
        
        def create_configurable_type_checked_method(
            original_method: Callable[..., Any],
            method_name: str
        ) -> Callable[..., Any]:
            """Create a configurable type-checked wrapper for a method.
            
            Parameters
            ----------
            original_method : Callable[..., Any]
                The original method to wrap.
            method_name : str
                The name of the method.
            
            Returns
            -------
            Callable[..., Any]
                The type-checked wrapper for the method.

            Raises
            ------
            RuntimeError
                If type hints cannot be obtained for the method.
            TypeError
                If type checking fails.
            """
            @wraps(original_method)
            def wrapper(
                *args: Any, # noqa ANN401: typing.Any is not allowed
                **kwargs: Any # noqa ANN401: typing.Any is not allowed
            ) -> Any: # noqa ANN401: typing.Any is not allowed
                """Wrap a method with configurable type checking.
                
                Parameters
                ----------
                *args : Any
                    Positional arguments.
                **kwargs : Any
                    Keyword arguments.
                
                Returns
                -------
                Any
                    The result of the method.

                Raises
                ------
                RuntimeError
                    If type hints cannot be obtained for the method.
                TypeError
                    If type checking fails.
                """
                if not should_check_method(method_name):
                    return original_method(*args, **kwargs)
                
                # get type hints for the method
                try:
                    type_hints = get_type_hints(original_method)
                except (NameError, AttributeError) as err:
                    if strict:
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
                            if strict:
                                msg = f"In method {method_name}: {e}"
                                raise TypeError(msg) from e
                            # in non-strict mode, just issue a warning
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
                
                # call the original method
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