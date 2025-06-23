import pandas as pd
import numpy as np
from requests import Session
from logging import Logger
from numbers import Number
from pydantic import validate_arguments, ConfigDict
from pydantic_core import core_schema
from psycopg.sql import Composable
from typing import (
    get_type_hints,
    get_origin,
    runtime_checkable,
    Type,
    Dict,
    Any,
    Union,
    BinaryIO,
    IO,
    Protocol,
)
from io import BytesIO, RawIOBase, BufferedIOBase
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
        """Tell Pydantic how to handle this protocol"""
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
    # Direct type check
    if hint in tup_special_types:
        return True

    # Check if it's a class and subclass of special types
    if isinstance(hint, type):
        for special_type in tup_special_types:
            try:
                if issubclass(hint, special_type):
                    return True
            except TypeError:
                # issubclass can raise TypeError for some types
                continue

    # Handle Union types
    origin = get_origin(hint)
    if origin is Union:
        args = typing_get_args(hint)
        for arg in args:
            if check_for_special_types(arg, tup_special_types):
                return True

    # Handle other generic types (List, Dict, etc.)
    if origin is not None:
        args = typing_get_args(hint)
        for arg in args:
            if check_for_special_types(arg, tup_special_types):
                return True

    return False


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
            Composable,  # This is the key addition
        )

        for attr_name, attr_value in dict_.items():
            if callable(attr_value) and not attr_name.startswith("__"):
                bl_arbitrary_types = False

                # Get type hints with error handling
                try:
                    type_hints = get_type_hints(attr_value, include_extras=True)
                except (NameError, AttributeError, TypeError):
                    # If we can't get type hints, assume we need arbitrary types
                    bl_arbitrary_types = True
                    type_hints = {}

                # Check each type hint for special types
                for hint in type_hints.values():
                    if check_for_special_types(hint, tup_special_types):
                        bl_arbitrary_types = True
                        break

                # Apply validation with appropriate config
                dict_[attr_name] = validate_arguments(
                    attr_value,
                    config=ConfigDict(
                        arbitrary_types_allowed=bl_arbitrary_types,
                    ),
                )

        return super().__new__(cls, name, bases, dict_)
