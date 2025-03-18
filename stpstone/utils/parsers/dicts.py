### HANDLING DICTIONARIES ISSUES ###

import re
from collections import Counter, OrderedDict, defaultdict
from functools import cmp_to_key
from heapq import nlargest, nsmallest
from itertools import groupby
from numbers import Number
from operator import itemgetter
from typing import Any, Callable, Dict, List, Optional, Union


class HandlingDicts:

    def min_val_key(self, dict_active):
        """
        DOCSTRING: MINIMUN VALUE FOR A GIVEN SET OF VALUES IN A DICTIONARY
        INPUTS: ACTIVE DICTIONARY
        OUTPUTS: KEY, VALUE
        """
        return min(dict_active.items(), key=itemgetter(1))

    def max_val_key(self, dict_active):
        """
        DOCSTRING: MAXIMUN VALUE FOR A GIVEN SET OF VALUES IN A DICTIONARY
        INPUTS: ACTIVE DICTIONARY
        OUTPUTS: KEY, VALUE
        """
        return max(dict_active.items(), key=itemgetter(1))

    def merge_n_dicts(self, *dicts):
        """
        DOCSTRING: MERGE DICTIONARIES, FOR PYTHON 3.5+
        INPUTS: DICTIONARIES
        OUTPUTS: DICTIONARY
        """
        dict_xpt = dict()
        for dict_ in dicts:
            dict_xpt = {**dict_xpt, **dict_}
        return dict_xpt

    def cmp(self, x, y):
        """
        Replacement for built-in function cmp that was removed in Python 3
        Compare the two objects x and y and return an integer according to
        the outcome. The return value is negative if x < y, zero if x == y
        and strictly positive if x > y.
        https://portingguide.readthedocs.io/en/latest/comparisons.html#the-cmp-function
        """
        return (x > y) - (x < y)

    def multikeysort(self, items, columns):
        """
        REFERENCES: https://stackoverflow.com/questions/1143671/how-to-sort-objects-by-multiple-keys-in-python,
            https://stackoverflow.com/questions/28502774/typeerror-cmp-is-an-invalid-keyword-argument-for-this-function
        DOCSTRING: SORT A LIST OF DICTIONARIES
        INPUTS: LIST OF DICTS AND LIST OF COLUMNS, IF THERE IS A NEGATIVE (-) SIGN ON KEY,
            IT WIL BE ORDERED IN REVERSE
        OUTPUTS: LIST OF DICTIONARIES
        """
        comparers = [
            (
                (itemgetter(col[1:].strip()), -1)
                if col.startswith("-")
                else (itemgetter(col.strip()), 1)
            )
            for col in columns
        ]
        def comparer(left, right):
            comparer_iter = (
                self.cmp(fn(left), fn(right)) * mult for fn, mult in comparers
            )
            return next((result for result in comparer_iter if result), 0)
        return sorted(items, key=cmp_to_key(comparer))

    def sum_values_selected_keys(self, list_ser, list_keys_merge=None, bl_sum_values_key=True):
        """
        DOCSTRING: MERGE DICTS FOR EVERY KEY REPETITION
        INPUTS: FOREIGNER KEY, DICTS
        OUTPUTS: DICTIONARY
        """
        # setting default variables
        dict_export = defaultdict(list)
        list_counter_dicts = list()
        # if list of keys to merge is none, return a list of every values for the same key
        if list_keys_merge != None:
            # iterating through dictionaries of interest an merging accordingly to foreigner key
            for dict_ in list_ser:
                for key, value in dict_.items():
                    if key in list_keys_merge:
                        dict_export[key].append(value)
                    else:
                        dict_export[key] = value
            if bl_sum_values_key == True:
                return {
                    k: (sum(v) if isinstance(v, list) else v)
                    for k, v in dict_export.items()
                }
            else:
                return dict_export
        else:
            for dict_ in list_ser:
                list_counter_dicts.append(Counter(dict_))
            return dict(sum(list_counter_dicts))

    def filter_list_ser(
        self,
        list_ser: List[Dict[str, Any]],
        foreigner_key: str,
        k_value: Number,
        str_filter_type: str = "equal",
    ) -> List[Dict[str, Any]]:
        """
        Filter list of dictionaries
        Args:
            list_ser (List[Dict[str, Any]]): List of dictionaries to filter
            foreigner_key (str): Foreigner key to filter
            k_value (Number): Value of foreigner key of interest
            str_filter_type (str): Type of filter to apply
        Returns:
            List[Dict[str, Any]]: Filtered list of dictionaries
        """
        if str_filter_type == "equal":
            return [dict_ for dict_ in list_ser if dict_[foreigner_key] == k_value]
        elif str_filter_type == "not_equal":
            return [dict_ for dict_ in list_ser if dict_[foreigner_key] != k_value]
        elif str_filter_type == "less_than":
            return [dict_ for dict_ in list_ser if dict_[foreigner_key] < k_value]
        elif str_filter_type == "greater_than":
            return [dict_ for dict_ in list_ser if dict_[foreigner_key] > k_value]
        elif str_filter_type == "less_than_or_equal_to":
            return [dict_ for dict_ in list_ser if dict_[foreigner_key] <= k_value]
        elif str_filter_type == "greater_than_or_equal_to":
            return [dict_ for dict_ in list_ser if dict_[foreigner_key] >= k_value]
        elif str_filter_type == "isin":
            return [dict_ for dict_ in list_ser if dict_[foreigner_key] in k_value]
        else:
            raise ValueError(
                'str_filter_type must be "equal", "not_equal", "less_than" or "greater_than"'
            )

    def merge_values_foreigner_keys(
        self, list_ser, foreigner_key, list_keys_merge_dict
    ):
        """
        REFERECES: https://stackoverflow.com/questions/50167565/python-how-to-merge-dict-in-list-of-dicts-based-on-value
        DOCSTRING: MERGE DICTS ACCORDINGLY TO A FOREIGNER KEY IN THE LIST OF DICTS
        INPUTS: LIST OF DICTS, FOREIGNER KEY AND LIST OF KEYS TO MERGE IN A GIVEN SET O DICTS
        OUTPUTS: LIST OF DICTS
        """
        # setting default variables
        list_ser_export = list()
        list_foreinger_keys = list()
        list_ser_export = list()
        # get values from foreinger key
        list_foreinger_keys = list(set([dict_[foreigner_key] for dict_ in list_ser]))
        # iterating through list of foreigner key and merging values of interest
        for key in list_foreinger_keys:
            # filter dicts for the given foreinger key
            list_filtered_dicts = self.filter_list_ser(list_ser, foreigner_key, key)
            # merge dictionaries accordingly to given keys
            list_ser_export.append(
                self.sum_values_selected_keys(list_filtered_dicts, list_keys_merge_dict)
            )
        # return final result
        return list_ser_export

    def n_smallest(self, list_ser, key_, n):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return nsmallest(n, list_ser, key=lambda dict_: dict_[key_])

    def n_largest(self, list_ser, key_, n):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return nlargest(n, list_ser, key=lambda dict_: dict_[key_])

    def order_dict(self, dict_):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return OrderedDict(sorted(dict_.items()))

    def group_by_dicts(self, list_ser):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # sort by the desired field first
        list_ser.sort(key=itemgetter("date"))
        # return iteration in groups
        return groupby(list_ser, key=itemgetter("date"))

    def add_key_value_to_dicts(
        self,
        list_ser: List[Dict[str, Union[int, float, str]]],
        key: Union[str, List[Dict[str, Union[int, float, str]]]],
        value: Union[
            Callable[..., Union[int, float, str]], Union[int, float, str], None
        ] = None,
        list_keys_for_function: Optional[List[str]] = None,
        kwargs_static: Optional[Dict[str, Union[int, float, str, None]]] = None,
    ) -> List[Dict[str, Union[int, float, str]]]:
        """
        Adds a key and value (or multiple key-value pairs) to every dictionary in a list.

        Args:
            list_ser (List[Dict[str, Union[int, float, str]]]): A list of dictionaries to be updated.
            key (Union[str, List[Dict[str, Union[int, float, str]]]]):
                - If a string, the key to add to each dictionary
                - If a list of dictionaries, each dictionary contains key-value pairs to add
            value (Union[Callable[..., Union[int, float, str]], Union[int, float, str], None]):
                - The value to associate with the key (if key is a string)
                - Can be a static value or a function to compute the value dynamically
                - Ignored if key is a list of dictionaries
            list_keys_for_function (Optional[List[str]]):
                - Keys to extract from the dictionary for the value function (if callable)
                - Required if value is a function
            kwargs_static (Optional[Dict[str, Union[int, float, str, None]]]):
                - Static keyword arguments to pass to the function (if callable)

        Returns:
            List[Dict[str, Union[int, float, str]]]: The updated list of dictionaries.

        Raises:
            ValueError: if key is a string and value is None.
            TypeError: if key is neither a string nor a list of dictionaries.
        """
        # if key is a string, treat it as a single key-value pair
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
                        if kwargs_static is not None:
                            dict_[key] = value(*args, **kwargs_static)
                        else:
                            dict_[key] = value(*args)
                    else:
                        dict_[key] = value
        # if key is a list of dictionaries, treat it as multiple key-value pairs
        elif isinstance(key, list):
            for dict_ in list_ser:
                if isinstance(dict_, dict):
                    for kv_pair in key:
                        if isinstance(kv_pair, dict):
                            for k, v in kv_pair.items():
                                dict_[k] = v
        else:
            raise TypeError("key must be a string or a list of dictionaries.")
        return list_ser

    def pair_keys_with_values(
        self, list_keys: List[str], list_lists: List[List[Any]]
    ) -> List[Dict[str, Any]]:
        """
        Pair each sublist with the list of keys and return a list of dictionaries
        Args:
            list_keys (List[str]): The list of keys to pair with the sublists
            list_lists (List[List[Any]]): The list of sublists to pair with the keys
        Returns:
            List[Dict[str, Any]]: A list of dictionaries, where each dictionary is a pairing of
                a sublist with the list of keys
        Raises:
            ValueError: If the length of a sublist does not match the length of list_keys
        """
        list_ser = []
        for sublist in list_lists:
            if len(sublist) != len(list_keys):
                raise ValueError(
                    f"The length of the sublist {sublist} does not match the length of list_keys."
                )
            # Pair keys with values from the sublist
            dict_entry = {key: value for key, value in zip(list_keys, sublist)}
            list_ser.append(dict_entry)
        return list_ser

    def pair_headers_with_data(
        self, list_headers: List[str], list_data: List[Any]
    ) -> List[Dict[str, Any]]:
        """
        DOCSTRING: PAIR HEADERS AND DATA AS KEYS AND VALUES IN A SERIALIZED LIST
            - FOR EXAMPLE, IF LIST_HEADERS IS ['NAME', 'AGE'] AND LIST_DATA IS
                ['JOHN', 25, 'ALICE', 30], THE FUNCTION WILL RETURN [{'NAME': 'JOHN', 'AGE': 25},
                {'NAME': 'ALICE', 'AGE': 30}]
        INPUTS: LIST HEADERS, LIST DATA
        OUTPUTS: LIST
        """
        # setting variables
        list_ser = list()
        # ensuring the list_data length is a multiple of list_headers length
        if len(list_data) % len(list_headers) != 0:
            raise ValueError(
                "The length of list_data is not a multiple of the length of list_headers."
            )
        # iterate over the list_data in chunks equal to the length of list_headers
        for i in range(0, len(list_data), len(list_headers)):
            # create a dictionary for each chunk
            entry = {
                list_headers[j]: list_data[i + j] for j in range(len(list_headers))
            }
            list_ser.append(entry)
        # returning list of dictionaries
        return list_ser

    def fill_placeholders(
        self, dict_base: Dict[str, Any], dict_replacer: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Replaces placeholders in the base dictionary with values from the replacement dictionary
        Args:
            dict_base (Dict[str, Any]): The dictionary containing placeholders
            dict_replacer (Dict[str, Any]): The dictionary providing replacement values
        Returns:
            Dict[str, Any]: The updated dictionary with placeholders replaced.
        """
        placeholder_pattern = re.compile(r"\{\{\s*(\w+)\s*\}\}")
        def replace_value(value):
            if isinstance(value, dict):
                return {k: replace_value(v) for k, v in value.items()}
            elif isinstance(value, str):
                return placeholder_pattern.sub(
                    lambda m: str(dict_replacer.get(m.group(1), m.group(0))), value
                )
            elif isinstance(value, list):
                return [replace_value(v) for v in value]
            return value
        return {key: replace_value(value) for key, value in dict_base.items()}
