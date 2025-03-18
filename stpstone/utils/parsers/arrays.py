import numpy as np
import pandas as pd
from typing import Union, List
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class Arrays(metaclass=TypeChecker):

    def to_array_matrice(self, array_data: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
        if len(array_data) == 0:
            raise ValueError("Return array is empty.")
        return array_data if isinstance(array_data, np.ndarray) else (
            array_data.to_numpy() if isinstance(array_data, pd.DataFrame) else None
        )

    def to_array_vector(self, array_data: Union[np.ndarray, pd.Series, List[float]]) -> np.ndarray:
        if len(array_data) == 0:
            raise ValueError("Return array is empty.")
        return array_data if isinstance(array_data, np.ndarray) else (
            np.array(array_data) if isinstance(array_data, list)
            else (array_data.to_numpy() if isinstance(array_data, pd.Series) else None)
        )

    def to_array(self, array_data: Union[np.ndarray, pd.DataFrame, pd.Series, List[float]]) \
        -> np.ndarray:
        if isinstance(array_data, (np.ndarray, pd.DataFrame)):
            return self.to_array_matrice(array_data)
        elif isinstance(array_data, (pd.Series, list, np.ndarray)):
            return self.to_array_vector(array_data)
        else:
            raise ValueError(f"Unsupported data type: {type(array_data)}")
