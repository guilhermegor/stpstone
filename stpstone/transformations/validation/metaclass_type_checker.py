from io import BufferedIOBase, BytesIO, RawIOBase
from logging import Logger
from numbers import Number
from typing import (
    IO,
    Any,
    BinaryIO,
    Dict,
    Protocol,
    Type,
    Union,
    get_origin,
    get_type_hints,
    runtime_checkable,
)

import numpy as np
import pandas as pd
from psycopg.sql import Composable
from pydantic import ConfigDict, validate_arguments, validator
from pydantic_core import core_schema
from requests import Session
from typing_extensions import get_args as typing_get_args


@runtime_checkable
class SQLComposable(Protocol):
    """Database-agnostic protocol for SQL composable objects"""

    def __str__(self) -> str: ...

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        _handler: Any,
    ) -> core_schema.CoreSchema:
        """Pydantic of SQL composable objects"""
        return core_schema.union_schema(
            [
                core_schema.str_schema(),
                core_schema.is_instance_schema(cls),
                core_schema.is_instance_schema(Composable),
            ]
        )


def check_for_special_types(hint: Any, tup_special_types: tuple) -> bool:
    """
    Recursively check if a type hint contains any special types that require arbitrary_types_allowed

    Args:
        hint: The type hint to check
        tup_special_types: Tuple of special types that require arbitrary_types_allowed

    Returns:
        True if the hint contains any special types, False otherwise
    """
    if hint in tup_special_types:
        return True
    
    if isinstance(hint, type):
        for special_type in tup_special_types:
            try:
                if issubclass(hint, special_type):
                    return True
            except TypeError:
                continue

    # handle Union types
    origin = get_origin(hint)
    if origin is Union:
        args = typing_get_args(hint)
        for arg in args:
            if check_for_special_types(arg, tup_special_types):
                return True

    # handle other generic types (List, Dict, etc.)
    if origin is not None:
        args = typing_get_args(hint)
        for arg in args:
            if check_for_special_types(arg, tup_special_types):
                return True

    return False

def create_validator(field_name: str, expected_type: Any) -> classmethod:
    """Create a Pydantic validator for a specific field and type."""
    def validate_field(cls, value: Any) -> Any:
        if not isinstance(value, expected_type):
            raise TypeError(f"{field_name} must be of type {expected_type.__name__}")
        return value
    return validator(field_name, allow_reuse=True)(validate_field)


class TypeChecker(type):
    def __new__(
        cls: Type["TypeChecker"], name: str, bases: tuple, dict_: Dict[str, Any]
    ) -> "TypeChecker":
        tup_types_ignore = (
            pd.DataFrame,
            pd.Series,
            np.ndarray,
            list,
            Session,
            Logger,
            Number,
        )

        tup_special_types = (
            *tup_types_ignore,
            IO,
            BinaryIO,
            RawIOBase,
            BufferedIOBase,
            BytesIO,
            SQLComposable,
            Composable,
        )

        # Process all methods in the class
        for attr_name, attr_value in dict_.items():
            if callable(attr_value) and not attr_name.startswith("__"):
                bl_arbitrary_types = False
                type_hints = get_type_hints(attr_value, include_extras=True)

                # Check for special types
                for hint in type_hints.values():
                    if check_for_special_types(hint, tup_special_types):
                        bl_arbitrary_types = True
                        break

                # Apply validation
                dict_[attr_name] = validate_arguments(
                    attr_value,
                    config=ConfigDict(arbitrary_types_allowed=bl_arbitrary_types),
                )
        
        # process __init__ separately to add field validators
        if "__init__" in dict_:
            init_method = dict_["__init__"]
            type_hints = get_type_hints(init_method, include_extras=True)
            
            for param, hint in type_hints.items():
                if param == "return":
                    continue
                    
                # handle simple types (int, str, etc.)
                if isinstance(hint, type):
                    validator_method = create_validator(param, hint)
                    dict_[f"validate_{param}"] = validator_method
                
                # handle union types (including optional)
                elif get_origin(hint) is Union:
                    args = typing_get_args(hint)
                    # filter out NoneType for optional
                    type_args = [arg for arg in args if arg is not type(None)]
                    if len(type_args) == 1:
                        validator_method = create_validator(param, type_args[0])
                        dict_[f"validate_{param}"] = validator_method

        return super().__new__(cls, name, bases, dict_)
