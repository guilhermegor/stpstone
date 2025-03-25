import pandas as pd
import numpy as np
from pydantic import validate_arguments, ConfigDict
from typing import get_type_hints, get_origin, get_args, Type, Dict, Any, Union, BinaryIO, IO
from io import BytesIO, RawIOBase, BufferedIOBase


class TypeChecker(type):
    def __new__(cls: Type["TypeChecker"], name: str, bases: tuple, dict_: Dict[str, Any]) \
        -> "TypeChecker":
        for attr_name, attr_value in dict_.items():
            if (callable(attr_value)) and (not attr_name.startswith("__")):
                bl_arbitrary_types = False
                tuple_types = (pd.DataFrame, np.ndarray, pd.Series, list)
                io_types = (IO, BinaryIO, RawIOBase, BufferedIOBase, BytesIO)
                type_hints = get_type_hints(attr_value)
                for hint in type_hints.values():
                    if hint in tuple_types:
                        bl_arbitrary_types = True
                        break
                    if hint in io_types:
                        bl_arbitrary_types = True
                        break
                    if get_origin(hint) is Union:
                        args = get_args(hint)
                        if any(arg in tuple_types for arg in args):
                            bl_arbitrary_types = True
                            break
                    if getattr(hint, "__origin__", None) in tuple_types:
                        bl_arbitrary_types = True
                        break
                dict_[attr_name] = validate_arguments(
                    attr_value,
                    config=ConfigDict(arbitrary_types_allowed=True) if bl_arbitrary_types else {},
                )
        return super().__new__(cls, name, bases, dict_)
