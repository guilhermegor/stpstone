### VALIDATES DATA FORMATS ###

from pydantic import validate_arguments, BaseModel, Field, ConfigDict
from abc import ABCMeta
from typing import get_type_hints
import pandas as pd

class DataFrameModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    df: pd.DataFrame = Field(..., description="Pandas DataFrame")

class TypeChecker(type):
    def __new__(cls, name, bases, dct):
        for attr_name, attr_value in dct.items():
            if callable(attr_value) and not attr_name.startswith("__"):
                #   use get_type_hints to inspect function signature
                type_hints = get_type_hints(attr_value)
                #   check if any argument or return type is a DataFrame
                has_dataframe = False
                for hint in type_hints.values():
                    #   handle generics
                    origin = getattr(hint, '__origin__', hint)
                    #   check dataframe or dataframe model
                    if origin is pd.DataFrame or origin is DataFrameModel:
                        has_dataframe = True
                        break
                if has_dataframe:
                  #   use pydantic validate_arguments
                  dct[attr_name] = validate_arguments(
                      attr_value, 
                      config={"arbitrary_types_allowed": True}
                    )
                else:
                  dct[attr_name] = validate_arguments(attr_value)
        return super().__new__(cls, name, bases, dct)


if __name__ == '__main__':

    class MyClass(metaclass=TypeChecker):

        def method_one(self, x: int, y: int) -> int:
            return x + y

        def method_two(self, z: str) -> str:
            return z.upper()

    # usage
    obj = MyClass()
    print(obj.method_one(3, 4))  # Outputs: 7
    try:
        print(obj.method_one("a", "b"))  # Raises ValidationError
    except Exception as e:
        print("Validation Error:", e)