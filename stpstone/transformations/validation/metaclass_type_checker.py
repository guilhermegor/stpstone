import pandas as pd
import numpy as np
from requests import Session
from logging import Logger
from numbers import Number
from pydantic import validate_arguments, ConfigDict
from pydantic_core import core_schema
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
        _source_type: Any,
        _handler: Any,
    ) -> core_schema.CoreSchema:
        """Tell Pydantic how to handle this protocol"""
        return core_schema.union_schema(
            [core_schema.str_schema(), core_schema.is_instance_schema(cls)]
        )


class TypeChecker(type):
    def __new__(
        cls: Type["TypeChecker"], name: str, bases: tuple, dict_: Dict[str, Any]
    ) -> "TypeChecker":
        for attr_name, attr_value in dict_.items():
            if callable(attr_value) and not attr_name.startswith("__"):
                bl_arbitrary_types = False
                tup_types_ignore = (
                    pd.DataFrame,
                    np.ndarray,
                    pd.Series,
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
                )
                # use get_type_hints with include_extras=True for Python 3.9+ compatibility
                type_hints = get_type_hints(attr_value, include_extras=True)
                for hint in type_hints.values():
                    origin = get_origin(hint)
                    # check for direct matches
                    if any(
                        hint is typ
                        or (isinstance(hint, type) and issubclass(hint, typ))
                        for typ in tup_special_types
                    ):
                        bl_arbitrary_types = True
                        break
                    # handle Union types (Python 3.9+ compatible)
                    if origin is Union or (
                        hasattr(hint, "__origin__") and hint.__origin__ is Union
                    ):
                        args = typing_get_args(hint)
                        if any(
                            arg in tup_special_types
                            or (isinstance(arg, type) and issubclass(arg, Protocol))
                            for arg in args
                        ):
                            bl_arbitrary_types = True
                            break
                    # handle protocols
                    if isinstance(hint, type) and issubclass(hint, Protocol):
                        bl_arbitrary_types = True
                        break
                dict_[attr_name] = validate_arguments(
                    attr_value,
                    config=ConfigDict(
                        arbitrary_types_allowed=bl_arbitrary_types,
                        # for Python 3.9+ compatibility with protocols
                        arbitrary_types_allowed_for_protocols=True,
                    ),
                )
        return super().__new__(cls, name, bases, dict_)
