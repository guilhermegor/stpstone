"""Dictionary manipulation utilities.

This module provides various operations for working with dictionaries including:
- Finding min/max values
- Merging dictionaries
- Sorting and filtering dictionary lists
- Adding/updating keys
- Pairing data with keys
- Placeholder replacement in dictionaries
"""

from collections import Counter, OrderedDict, defaultdict
from functools import cmp_to_key
from heapq import nlargest, nsmallest
from itertools import groupby
from operator import itemgetter
import re
from typing import Any, Callable, Literal, Optional, TypeVar, Union

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


TypeFilter = TypeVar("TypeFilter", bound=Literal["equal", "not_equal", "less_than", 
                                                 "greater_than", "less_than_or_equal_to", 
                                                 "greater_than_or_equal_to", "isin"])

class HandlingDicts(metaclass=TypeChecker):
    """Collection of dictionary manipulation utilities."""

    def _validate_dict_not_empty(self, dict_: dict) -> None:
        """Validate that dictionary is not empty.
        
        Parameters
        ----------
        dict_ : dict
            Dictionary to validate
            
        Raises
        ------
        ValueError
            If dictionary is empty
        """
        if not dict_:
            raise ValueError("Input dictionary cannot be empty")

    def min_val_key(self, dict_: dict) -> dict[str, Any]:
        """Get the key-value pair with the minimum value in a dictionary.
        
        Parameters
        ----------
        dict_ : dict
            Input dictionary to search
            
        Returns
        -------
        dict[str, Any]
            Tuple containing (key, value) of minimum value

        Raises
        ------
        ValueError
            If dictionary is empty or contains only None values
            
        Examples
        --------
        >>> hd = HandlingDicts()
        >>> hd.min_val_key({"a": 1, "b": 2})
        ("a", 1)
        """
        self._validate_dict_not_empty(dict_)
        list_filt_items = [(k, v) for k, v in dict_.items() if v is not None]
        if not list_filt_items:
            raise ValueError("Dictionary contains only None values")
        return min(list_filt_items, key=itemgetter(1))

    def max_val_key(self, dict_: dict) -> dict[str, Any]:
        """Get the key-value pair with the maximum value in a dictionary.
        
        Parameters
        ----------
        dict_ : dict
            Input dictionary to search
            
        Returns
        -------
        dict[str, Any]
            Tuple containing (key, value) of maximum value

        Raises
        ------
        ValueError
            If dictionary is empty or contains only None values
        """
        self._validate_dict_not_empty(dict_)
        list_filt_items = [(k, v) for k, v in dict_.items() if v is not None]
        if not list_filt_items:
            raise ValueError("Dictionary contains only None values")
        return max(list_filt_items, key=itemgetter(1))

    def merge_n_dicts(self, *dicts: dict) -> dict[str, Any]:
        """Merge multiple dictionaries into one.
        
        Parameters
        ----------
        *dicts : dict
            Variable number of dictionaries to merge
            
        Returns
        -------
        dict[str, Any]
            Merged dictionary containing all key-value pairs
            
        Notes
        -----
        Later dictionaries will overwrite earlier ones for duplicate keys
        """
        dict_xpt = dict()
        for dict_ in dicts:
            dict_xpt = {**dict_xpt, **dict_}
        return dict_xpt

    def cmp(
        self, 
        x: Any, # noqa ANN401: typing.Any is not allowed
        y: Any # noqa ANN401: typing.Any is not allowed
    ) -> int:
        """Compare two objects and return comparison result.
        
        Parameters
        ----------
        x : Any
            First object to compare
        y : Any
            Second object to compare
            
        Returns
        -------
        int
            Negative if x < y, 0 if x == y, positive if x > y
            
        References
        ----------
        .. [1] https://portingguide.readthedocs.io/en/latest/comparisons.html#the-cmp-function
        """
        return (x > y) - (x < y)

    def multikeysort(
        self, 
        items: list[dict], 
        columns: list[str]
    ) -> list[dict]:
        """Sort a list of dictionaries by multiple keys.
        
        Parameters
        ----------
        items : list[dict]
            List of dictionaries to sort
        columns : list[str]
            List of column names to sort by. Prefix with "-" for descending order.
            
        Returns
        -------
        list[dict]
            Sorted list of dictionaries
            
        References
        ----------
        .. [1] https://stackoverflow.com/questions/1143671/how-to-sort-objects-by-multiple-keys
        .. [2] https://stackoverflow.com/questions/28502774/typeerror-cmp-is-an-invalid-keyword-argument
        """
        list_comparers = [
            (
                (itemgetter(col[1:].strip()), -1)
                if col.startswith("-")
                else (itemgetter(col.strip()), 1)
            )
            for col in columns
        ]
        
        def comparer(left: dict, right: dict) -> int:
            """Compare two dictionaries based on multiple keys.
            
            Parameters
            ----------
            left : dict
                First dictionary to compare
            right : dict
                Second dictionary to compare
            
            Returns
            -------
            int
                Negative if left < right, 0 if left == right, positive if left > right
            """
            comparer_iter = (
                self.cmp(fn(left), fn(right)) * mult for fn, mult in list_comparers
            )
            return next((result for result in comparer_iter if result), 0)
            
        return sorted(items, key=cmp_to_key(comparer))

    def sum_values_selected_keys(
        self,
        list_ser: list[dict],
        list_keys_merge: Optional[list[str]] = None,
        bool_sum_values_key: bool = True
    ) -> dict:
        """Merge dictionaries by summing values for selected keys.
        
        Parameters
        ----------
        list_ser : list[dict]
            List of dictionaries to process
        list_keys_merge : Optional[list[str]]
            Keys to merge (sum values). If None, sum all values.
        bool_sum_values_key : bool
            Whether to sum values (True) or keep as lists (False)
            
        Returns
        -------
        dict
            Resulting dictionary with merged values
        """
        dict_ = defaultdict(list)
        
        if list_keys_merge is not None:
            for dict_aux in list_ser:
                for key, value in dict_aux.items():
                    if key in list_keys_merge:
                        if isinstance(dict_[key], list):
                            dict_[key].append(value)
                    else:
                        dict_[key] = value
                        
            if bool_sum_values_key:
                return {
                    k: (sum(v) if isinstance(v, list) else v)
                    for k, v in dict_.items()
                }
            return dict_
            
        list_counter_dicts = [Counter(dict_) for dict_ in list_ser]
        return dict(sum(list_counter_dicts))

    def filter_list_ser(
        self,
        list_ser: list[dict[str, Any]],
        foreigner_key: str,
        k_value: Any, # noqa ANN401: typing.Any is not allowed
        str_filter_type: TypeFilter = "equal",
    ) -> list[dict[str, Any]]:
        """Filter list of dictionaries based on key condition.
        
        Parameters
        ----------
        list_ser : list[dict[str, Any]]
            List of dictionaries to filter
        foreigner_key : str
            Key to filter on
        k_value : Any
            Value to filter on
        str_filter_type : TypeFilter
            Type of comparison to perform
            
        Returns
        -------
        list[dict[str, Any]]
            Filtered list of dictionaries
            
        Raises
        ------
        ValueError
            If invalid filter type specified
        """
        filter_ops = {
            "equal": lambda x: x == k_value,
            "not_equal": lambda x: x != k_value,
            "less_than": lambda x: x < k_value,
            "greater_than": lambda x: x > k_value,
            "less_than_or_equal_to": lambda x: x <= k_value,
            "greater_than_or_equal_to": lambda x: x >= k_value,
            "isin": lambda x: x in k_value,
        }
        
        if str_filter_type not in filter_ops:
            raise ValueError(
                f"Invalid filter type: {str_filter_type}. "
                "Must be one of: {list(filter_ops.keys())}"
            )
            
        return [
            dict_ for dict_ in list_ser 
            if filter_ops[str_filter_type](dict_[foreigner_key])
        ]

    def merge_values_foreigner_keys(
        self,
        list_ser: list[dict],
        foreigner_key: str,
        list_keys_merge_dict: list[str]
    ) -> list[dict]:
        """Merge dictionaries grouped by a foreign key.
        
        Parameters
        ----------
        list_ser : list[dict]
            List of dictionaries to merge
        foreigner_key : str
            Key to group by
        list_keys_merge_dict : list[str]
            Keys whose values should be merged
            
        Returns
        -------
        list[dict]
            List of merged dictionaries
            
        References
        ----------
        .. [1] https://stackoverflow.com/questions/50167565/python-how-to-merge-dict-in-list-of-dicts
        """
        list_foreinger_keys = list({dict_[foreigner_key] for dict_ in list_ser})
        return [
            self.sum_values_selected_keys(
                self.filter_list_ser(list_ser, foreigner_key, key),
                list_keys_merge_dict
            )
            for key in list_foreinger_keys
        ]

    def n_smallest(
        self, 
        list_ser: list[dict], 
        key_: str, 
        n: int
    ) -> list[dict]:
        """Get n dictionaries with smallest values for given key.
        
        Parameters
        ----------
        list_ser : list[dict]
            List of dictionaries to process
        key_ : str
            Key to compare values
        n : int
            Number of items to return
            
        Returns
        -------
        list[dict]
            List of n smallest dictionaries
        """
        return nsmallest(n, list_ser, key=lambda dict_: dict_[key_])

    def n_largest(
        self, 
        list_ser: list[dict], 
        key_: str, 
        n: int
    ) -> list[dict]:
        """Get n dictionaries with largest values for given key.
        
        Parameters
        ----------
        list_ser : list[dict]
            List of dictionaries to process
        key_ : str
            Key to compare values
        n : int
            Number of items to return
            
        Returns
        -------
        list[dict]
            List of n largest dictionaries
        """
        return nlargest(n, list_ser, key=lambda dict_: dict_[key_])

    def order_dict(self, dict_: dict) -> OrderedDict:
        """Return an ordered dictionary sorted by keys.
        
        Parameters
        ----------
        dict_ : dict
            Dictionary to order
            
        Returns
        -------
        OrderedDict
            Dictionary sorted by keys
        """
        return OrderedDict(sorted(dict_.items()))

    def group_by_dicts(self, list_ser: list[dict]) -> groupby:
        """Group dictionaries by date key.
        
        Parameters
        ----------
        list_ser : list[dict]
            List of dictionaries containing "date" key
            
        Returns
        -------
        groupby
            Iterator yielding consecutive groups by date
            
        Notes
        -----
        Input list must contain "date" key and be pre-sorted by date
        """
        list_ser.sort(key=itemgetter("date"))
        return groupby(list_ser, key=itemgetter("date"))

    def add_key_value_to_dicts(
        self,
        list_ser: list[dict[str, Union[int, float, str]]],
        key: Union[str, list[dict[str, Union[int, float, str]]]],
        value: Optional[Union[
            Callable[..., Union[int, float, str]], Union[int, float, str]
        ]] = None,
        list_keys_for_function: Optional[list[str]] = None,
        kwargs_static: Optional[dict[str, Union[int, float, str, None]]] = None,
    ) -> list[dict[str, Union[int, float, str]]]:
        """Add key-value pairs to dictionaries in a list.
        
        Parameters
        ----------
        list_ser : list[dict[str, Union[int, float, str]]]
            List of dictionaries to modify
        key : Union[str, list[dict[str, Union[int, float, str]]]]
            Either a single key or list of dicts with key-value pairs
        value : Optional[Union[Callable[..., Union[int, float, str]], Union[int, float, str]]]
            Value to add (static or callable)
        list_keys_for_function : Optional[list[str]]
            Keys to extract for callable value
        kwargs_static : Optional[dict[str, Union[int, float, str, None]]]
            Additional kwargs for callable value
            
        Returns
        -------
        list[dict[str, Union[int, float, str]]]
            Modified list of dictionaries
            
        Raises
        ------
        ValueError
            If key is string but value is None
        TypeError
            If key is neither string nor list of dicts
        """
        if isinstance(key, str):
            if value is None:
                raise ValueError("If key is a string, value must be provided.")
                
            for dict_ in list_ser:
                if isinstance(dict_, dict):
                    if callable(value):
                        args = (
                            [dict_.get(k) for k in list_keys_for_function]
                            if list_keys_for_function is not None
                            else []
                        )
                        dict_[key] = (
                            value(*args, **kwargs_static)
                            if kwargs_static is not None
                            else value(*args)
                        )
                    else:
                        dict_[key] = value
                        
        elif isinstance(key, list):
            for dict_ in list_ser:
                if isinstance(dict_, dict):
                    for kv_pair in key:
                        if isinstance(kv_pair, dict):
                            dict_.update(kv_pair)
        else:
            raise TypeError("key must be a string or a list of dictionaries.")
            
        return list_ser

    def pair_keys_with_values(
        self, 
        list_keys: list[str], 
        list_lists: list[list[Any]]
    ) -> list[dict[str, Any]]:
        """Pair keys with values from sublists to create dictionaries.
        
        Parameters
        ----------
        list_keys : list[str]
            List of keys for dictionary
        list_lists : list[list[Any]]
            List of value lists to pair with keys
            
        Returns
        -------
        list[dict[str, Any]]
            List of dictionaries with paired keys/values
            
        Raises
        ------
        ValueError
            If sublist length doesn't match keys length
        """
        list_ser = []
        for sublist in list_lists:
            if len(sublist) != len(list_keys):
                raise ValueError(
                    f"Sublist length {len(sublist)} != keys length {len(list_keys)}"
                )
            list_ser.append(dict(zip(list_keys, sublist)))
        return list_ser

    def pair_headers_with_data(
        self, 
        list_headers: list[str], 
        list_data: list[Any]
    ) -> list[dict[str, Any]]:
        """Pair headers with data to create list of dictionaries.
        
        Parameters
        ----------
        list_headers : list[str]
            List of header/field names
        list_data : list[Any]
            Flat list of data values
            
        Returns
        -------
        list[dict[str, Any]]
            List of dictionaries with header-value pairs
            
        Raises
        ------
        ValueError
            If data length isn't multiple of headers length
        """
        if len(list_data) % len(list_headers) != 0:
            raise ValueError(
                f"Data length {len(list_data)} not multiple of headers {len(list_headers)}"
            )
            
        return [
            {list_headers[j]: list_data[i + j] 
             for j in range(len(list_headers))}
            for i in range(0, len(list_data), len(list_headers))
        ]

    def fill_placeholders(
        self, 
        dict_base: dict[str, Any], 
        dict_replacer: dict[str, Any]
    ) -> dict[str, Any]:
        """Replace {{placeholder}} patterns in dictionary values.
        
        Parameters
        ----------
        dict_base : dict[str, Any]
            Dictionary containing placeholders
        dict_replacer : dict[str, Any]
            Dictionary with replacement values
            
        Returns
        -------
        dict[str, Any]
            Dictionary with placeholders replaced
        """
        placeholder_pattern = re.compile(r"\{\{\s*(\w+)\s*\}\}")
        
        def replace_value(
            value: Any # noqa ANN401: typing.Any is not allowed
        ) -> Any: # noqa ANN401: typing.Any is not allowed
            """Recursively replace placeholders in value.
            
            Parameters
            ----------
            value : Any
                Value to process
            
            Returns
            -------
            Any
                Processed value with placeholders replaced
            """
            if isinstance(value, dict):
                return {k: replace_value(v) for k, v in value.items()}
            if isinstance(value, str):
                return placeholder_pattern.sub(
                    lambda m: str(dict_replacer.get(m.group(1), m.group(0))), 
                    value
                )
            if isinstance(value, list):
                return [replace_value(v) for v in value]
            return value
            
        return {key: replace_value(value) for key, value in dict_base.items()}