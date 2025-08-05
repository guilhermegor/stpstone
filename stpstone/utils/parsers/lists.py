"""List manipulation utilities with advanced search, sorting, and validation capabilities.

This module provides comprehensive list handling functions including:
- Occurrence searching (first, closest, regex matches)
- Bounds calculation (lower/upper/middle bounds)
- List operations (deduplication, chunking, flattening)
- Sorting (alphanumeric, pairwise)
- Frequency analysis
- String manipulation within lists

References
----------
.. [1] https://docs.python.org/3/library/itertools.html#itertools.pairwise
.. [2] https://stackoverflow.com/questions/2669059/how-to-sort-alpha-numeric-set-in-python
"""

import bisect
from collections import Counter, OrderedDict
from collections.abc import Iterable
from itertools import chain, product, tee
from logging import Logger
from numbers import Number
import re
from typing import Any, Optional, TypedDict

import numpy as np
from numpy.typing import NDArray

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.numbers import NumHandler
from stpstone.utils.parsers.str import StrHandler


class ReturnGetLowerUpperBound(TypedDict):
    """Return type for get_lower_upper_bound method."""

    lower_bound: Number
    upper_bound: Number


class ReturnGetLowerMidUpperBound(TypedDict):
    """Return type for get_lower_mid_upper_bound method."""

    lower_bound: Number
    middle_bound: Number
    upper_bound: Number
    end_of_list: bool


class ListHandler(metaclass=TypeChecker):
    """Handler class for advanced list operations and manipulations."""

    def __init__(self) -> None:
        """Initialize ListHandler instance."""
        self.str_handler = StrHandler()

    def _validate_list_not_empty(self, list_: list) -> None:
        """Validate that input list is not empty.
        
        Parameters
        ----------
        list_ : list
            List to validate
            
        Raises
        ------
        ValueError
            If input list is empty
        """
        if not list_:
            raise ValueError("Input list cannot be empty")

    def get_first_occurrence_within_list(
        self,
        list_: list[str],
        obj_occurrence: Optional[str] = None,
        bool_uppercase: bool = False,
        bool_last_uppercase_before_capitalized: bool = False,
        int_error: int = -1,
        int_error_obj_occurrence: int = -2,
        bool_regex_alphanumeric_false: bool = False,
        bool_ignore_sole_letter: bool = True,
    ) -> int:
        """Get index of first occurrence matching specified criteria.
        
        Parameters
        ----------
        list_ : list[str]
            List to search through
        obj_occurrence : Optional[str]
            String pattern to match (case insensitive)
        bool_uppercase : bool
            Find first all-uppercase string
        bool_last_uppercase_before_capitalized : bool
            Find last uppercase num_before capitalized/lowercase
        int_error : int
            Return value if no uppercase found
        int_error_obj_occurrence : int
            Return value if object occurrence not found
        bool_regex_alphanumeric_false : bool
            Find first non-alphanumeric string
        bool_ignore_sole_letter : bool
            Ignore single-letter uppercase strings
        str_original_replace_1 : str
            First character to replace in strings
        str_original_replace_2 : str
            Second character to replace in strings
        str_result_replace : str
            Replacement character
            
        Returns
        -------
        int
            Index of first matching element or error code

        Raises
        ------
        ValueError
            If no valid search criteria provided
        """
        self._validate_list_not_empty(list_)
        
        if bool_uppercase:
            try:
                return list_.index(next(obj for obj in list_ if obj.isupper()))
            except StopIteration:
                return int_error
                
        if obj_occurrence is not None:
            obj_lower = self.str_handler.remove_diacritics(obj_occurrence).lower()
            for i, el in enumerate(list_):
                if self.str_handler.match_string_like(
                    self.str_handler.remove_diacritics(el).lower(), obj_lower
                ):
                    return i
            return int_error_obj_occurrence
            
        if bool_last_uppercase_before_capitalized:
            first_el = list_[0].replace(",", "")
            if self.str_handler.is_capitalized(first_el) or first_el.islower():
                return int_error
                
            for i in range(len(list_) - 2):
                current = list_[i].replace(",", "")
                next_el = list_[i + 1].replace(",", "")
                if (current.isupper() and 
                    (self.str_handler.is_capitalized(next_el) or next_el.islower())):
                    if bool_ignore_sole_letter:
                        if len(self.str_handler.remove_non_alphanumeric_chars(current)) == 1:
                            return i - 1
                        return i
                    return i
            return int_error
            
        if bool_regex_alphanumeric_false:
            for i, el in enumerate(list_):
                if not el.isalnum():
                    return i
            return int_error
            
        raise ValueError(
            "No valid search criteria provided - must specify either uppercase search, "
            "object occurrence, or regex alphanumeric check"
        )

    def get_list_until_invalid_occurrences(
        self, 
        list_: list[Any], 
        list_invalid_values: list[Any]
    ) -> list[Any]:
        """Get sublist until first invalid occurrence.
        
        Parameters
        ----------
        list_ : list[Any]
            Input list to process
        list_invalid_values : list[Any]
            Values that should terminate processing
            
        Returns
        -------
        list[Any]
            Sublist containing elements until first invalid value
        """
        self._validate_list_not_empty(list_)
        
        result = []
        for el in list_:
            el_processed = self.str_handler.remove_diacritics(str(el)).lower()
            if any(self.str_handler.match_string_like(
                el_processed, 
                self.str_handler.remove_diacritics(str(inv)).lower()
            ) for inv in list_invalid_values):
                break
            result.append(el)
        return result

    def first_numeric(
        self, 
        list_: list[Any] # noqa ANN401: typing.Any is not allowed 
    ) -> Any: # noqa ANN401: typing.Any is not allowed 
        """Get first numeric value in list.
        
        Parameters
        ----------
        list_ : list[Any]
            Input list to search
            
        Returns
        -------
        Any
            First numeric value or False if none found
        """
        self._validate_list_not_empty(list_)
        try:
            return next(el for el in list_ if str(el).isnumeric())
        except StopIteration:
            return False

    def get_lower_upper_bound(
        self, 
        sorted_list: list[Number], 
        value_to_put_in_between: Number
    ) -> ReturnGetLowerUpperBound:
        """Get lower and upper bounds around a value in sorted list.
        
        Parameters
        ----------
        sorted_list : list[Number]
            List sorted in ascending order
        value_to_put_in_between : Number
            Value to find bounds for
            
        Returns
        -------
        ReturnGetLowerUpperBound
            Dictionary with lower and upper bounds
            
        Raises
        ------
        ValueError
            If value is outside list bounds
        """
        self._validate_list_not_empty(sorted_list)
        
        if value_to_put_in_between in sorted_list and sorted_list[-1] != value_to_put_in_between:
            idx = bisect.bisect_left(sorted_list, value_to_put_in_between)
            bounds = [idx, idx + 1]
        else:
            idx = bisect.bisect_left(sorted_list, value_to_put_in_between)
            bounds = [idx - 1, idx]
            
        if not all(0 <= i < len(sorted_list) for i in bounds):
            raise ValueError(
                f"{value_to_put_in_between} value is outside the bounds of {sorted_list}"
            )
            
        return {
            "lower_bound": sorted_list[bounds[0]],
            "upper_bound": sorted_list[bounds[1]]
        }

    def get_lower_mid_upper_bound(
        self, 
        sorted_list: list[Number], 
        value_to_put_in_between: Number
    ) -> ReturnGetLowerMidUpperBound:
        """Get lower, middle and upper bounds around a value in sorted list.
        
        Parameters
        ----------
        sorted_list : list[Number]
            List sorted in ascending order
        value_to_put_in_between : Number
            Value to find bounds for
            
        Returns
        -------
        ReturnGetLowerMidUpperBound
            Dictionary with bounds and end of list flag
            
        Raises
        ------
        ValueError
            If value is outside list bounds or list too small
        """
        self._validate_list_not_empty(sorted_list)
        if len(sorted_list) <= 2:
            raise ValueError("Input list must have more than 2 elements")
            
        bounds = self.get_lower_upper_bound(sorted_list, value_to_put_in_between)
        lower_idx = sorted_list.index(bounds["lower_bound"])
        
        try:
            return {
                "lower_bound": bounds["lower_bound"],
                "middle_bound": bounds["upper_bound"],
                "upper_bound": sorted_list[lower_idx + 2],
                "end_of_list": False
            }
        except IndexError:
            return {
                "lower_bound": sorted_list[lower_idx - 1],
                "middle_bound": bounds["lower_bound"],
                "upper_bound": bounds["upper_bound"],
                "end_of_list": True
            }

    def closest_bound(
        self, 
        sorted_list: list[Number], 
        value_to_put_in_between: Number
    ) -> Number:
        """Find closest value in sorted list to given value.
        
        Parameters
        ----------
        sorted_list : list[Number]
            Sorted input list
        value_to_put_in_between : Number
            Value to compare against
            
        Returns
        -------
        Number
            Closest value in list
        """
        self._validate_list_not_empty(sorted_list)
        idx = bisect.bisect_left(sorted_list, value_to_put_in_between)
        if idx == 0:
            return sorted_list[0]
        if idx == len(sorted_list):
            return sorted_list[-1]
        num_before = sorted_list[idx-1]
        num_after = sorted_list[idx]
        if abs(num_after - value_to_put_in_between) <= abs(value_to_put_in_between - num_before):
            return num_after
        return num_before

    def closest_number_within_list(
        self, 
        list_: list[Number], 
        number: Number
    ) -> Number:
        """Find closest number in unsorted list to given number.
        
        Parameters
        ----------
        list_ : list[Number]
            Input list (doesn't need to be sorted)
        number : Number
            Number to compare against
            
        Returns
        -------
        Number
            Closest number in list
        """
        self._validate_list_not_empty(list_)
        return min(list_, key=lambda x: abs(x - number))

    def first_occurrence_like(self, list_: list[str], str_like: str) -> int:
        """Find index of first string matching pattern.
        
        Parameters
        ----------
        list_ : list[str]
            List to search
        str_like : str
            Pattern to match
            
        Returns
        -------
        int
            Index of first match
            
        Raises
        ------
        ValueError
            If no match found
        """
        self._validate_list_not_empty(list_)
        str_like_lower = str_like.lower()
        try:
            return list_.index(next(
                x for x in list_ 
                if x.lower() == str_like_lower
            ))
        except StopIteration as err:
            raise ValueError(f"No match found for pattern: {str_like}") from err

    def remove_duplicates(
        self, 
        list_interest: list[Any] # noqa ANN401: typing.Any is not allowed 
    ) -> list[Any]: # noqa ANN401: typing.Any is not allowed 
        """Remove duplicates while preserving order.
        
        Parameters
        ----------
        list_interest : list[Any]
            Input list with possible duplicates
            
        Returns
        -------
        list[Any]
            List with duplicates removed
        """
        return list(OrderedDict.fromkeys(list_interest))

    def nth_smallest_numbers(
        self, 
        list_numbers: list[Number], 
        nth_smallest: int
    ) -> NDArray[np.float64]:
        """Get n smallest numbers from list.
        
        Parameters
        ----------
        list_numbers : list[Number]
            Input numbers
        nth_smallest : int
            Number of smallest values to return
            
        Returns
        -------
        NDArray[np.float64]
            Array of smallest numbers
        """
        self._validate_list_not_empty(list_numbers)
        return np.sort(np.array(list_numbers))[:nth_smallest]

    def extend_lists(
        self, 
        *lists: list[Any], # noqa ANN401: typing.Any is not allowed 
        bool_remove_duplicates: bool = True
    ) -> list[Any]: # noqa ANN401: typing.Any is not allowed
        """Combine multiple lists with optional deduplication.
        
        Parameters
        ----------
        *lists : list[Any]
            Variable number of lists to combine
        bool_remove_duplicates : bool
            Whether to remove duplicates
            
        Returns
        -------
        list[Any]
            Combined list
        """
        combined = list(chain.from_iterable(lists))
        return self.remove_duplicates(combined) if bool_remove_duplicates else combined

    def chunk_list(
        self,
        list_to_chunk: list[Any], # noqa ANN401: typing.Any is not allowed 
        str_join_char: Optional[str] = " ",
        int_chunk: int = 150,
        bool_remove_duplicates: bool = True
    ) -> list[Any]: # noqa ANN401: typing.Any is not allowed 
        """Split list into chunks with optional joining.
        
        Parameters
        ----------
        list_to_chunk : list[Any]
            List to split
        str_join_char : Optional[str]
            Joining character (None for sublists)
        int_chunk : int
            Maximum chunk size
        bool_remove_duplicates : bool
            Whether to remove duplicates first
            
        Returns
        -------
        list[Any]
            List of chunks (joined strings or sublists)
        """
        if str_join_char is None:
            return [
                list_to_chunk[x: x + int_chunk] 
                for x in range(0, len(list_to_chunk), int_chunk)
            ]

        list_chunked = list()
        if bool_remove_duplicates:
            list_to_chunk = self.remove_duplicates(list_to_chunk)
        list_position_chunks = NumHandler().multiples(int_chunk, len(list_to_chunk))
        inf_limit = list_position_chunks[0]
        sup_limit = list_position_chunks[1]

        if len(list_position_chunks) > 2:
            for lim in list_position_chunks[2:]:
                if str_join_char is None:
                    list_chunked.append(list_to_chunk[inf_limit: sup_limit])
                else:
                    list_chunked.append(str_join_char.join(
                        list_to_chunk[inf_limit: sup_limit]))
                inf_limit = sup_limit
                sup_limit = lim
            if str_join_char is None:
                list_chunked.append(list_to_chunk[
                    list_position_chunks[-2]: list_position_chunks[-1]])
            else:
                list_chunked.append(str_join_char.join(list_to_chunk[
                    list_position_chunks[-2]: list_position_chunks[-1]]))
        else:
            list_chunked.append(str_join_char.join(list_to_chunk[
                inf_limit: sup_limit]))
        list_chunked = self.remove_duplicates(list_chunked)
        return list_chunked

    def cartesian_product(
        self, 
        list_lists: list[list[Any]], # noqa ANN401: typing.Any is not allowed 
        int_break_n_n: Optional[int] = None
    ) -> list[Any]: # noqa ANN401: typing.Any is not allowed
        """Compute Cartesian product of input lists.
        
        Parameters
        ----------
        list_lists : list[list[Any]]
            Lists to compute product of
        int_break_n_n : Optional[int]
            Truncate tuples to this length
            
        Returns
        -------
        list[Any]
            List of product tuples
        """
        products = list(product(*list_lists))
        if int_break_n_n is None:
            return products
            
        result = []
        for tup in products:
            truncated = tup[:int_break_n_n]
            if (truncated not in result) and all(
                truncated[i] != truncated[i - 1] 
                for i in range(1, len(truncated))
            ):
                result.append(truncated)
        return result

    def sort_alphanumeric(self, list_: list[str]) -> list[str]:
        """Sort list alphanumerically.
        
        Parameters
        ----------
        list_ : list[str]
            List to sort
            
        Returns
        -------
        list[str]
            Sorted list

        References
        ----------
        .. [1] https://stackoverflow.com/questions/2669059/how-to-sort-alpha-numeric-set-in-python
        """
        def convert(text: str) -> Any: # noqa ANN401: typing.Any is not allowed 
            """Convert text to integer if numeric, else return as is.
            
            Parameters
            ----------
            text : str
                Text to convert
            
            Returns
            -------
            Any
                Converted text
            """
            return int(text) if text.isdigit() else text
            
        def alphanum_key(
            key: str
        ) -> list[Any]: # noqa ANN401: typing.Any is not allowed 
            """Split key into alphanumeric components for sorting.
            
            Parameters
            ----------
            key : str
                Key to split
            
            Returns
            -------
            list[Any]
                List of alphanumeric components
            """
            return [convert(c) for c in re.split("([0-9]+)", key)]
            
        return sorted(list_, key=alphanum_key)

    def pairwise(
        self, 
        iterable: Iterable[Any] # noqa ANN401: typing.Any is not allowed 
    ) -> list[tuple[Any, Any]]: # noqa ANN401: typing.Any is not allowed 
        """Return successive overlapping pairs from iterable.
        
        Parameters
        ----------
        iterable : Iterable[Any]
            Input sequence
            
        Returns
        -------
        list[tuple[Any, Any]]
            List of adjacent pairs

        References
        ----------
        .. [1] https://docs.python.org/3/library/itertools.html#itertools.pairwise
        """
        a, b = tee(iterable)
        next(b, None)
        return list(zip(a, b))

    def discard_from_list(
        self, 
        list_: list[Any], # noqa ANN401: typing.Any is not allowed 
        list_items_remove: list[Any] # noqa ANN401: typing.Any is not allowed 
    ) -> list[Any]: # noqa ANN401: typing.Any is not allowed 
        """Remove specified items from list.
        
        Parameters
        ----------
        list_ : list[Any]
            List to modify
        list_items_remove : list[Any]
            Items to remove
            
        Returns
        -------
        list[Any]
            Modified list
        """
        return [item for item in list_ if item not in list_items_remove]

    def absolute_frequency(
        self, 
        list_: list[Any] # noqa ANN401: typing.Any is not allowed 
    ) -> Counter:
        """Compute frequency count of list items.
        
        Parameters
        ----------
        list_ : list[Any]
            Input list
            
        Returns
        -------
        Counter
            Frequency count dictionary
        """
        return Counter(list_)

    def flatten_list(
        self, 
        list_: list[list[Any]] # noqa ANN401: typing.Any is not allowed 
    ) -> list[Any]: # noqa ANN401: typing.Any is not allowed 
        """Flatten nested list one level.
        
        Parameters
        ----------
        list_ : list[list[Any]]
            Nested list to flatten
            
        Returns
        -------
        list[Any]
            Flattened list
        """
        return [item for sublist in list_ for item in sublist]

    def remove_consecutive_duplicates(
        self, 
        list_: list[Any] # noqa ANN401: typing.Any is not allowed 
    ) -> list[Any]: # noqa ANN401: typing.Any is not allowed 
        """Remove consecutive duplicate items.
        
        Parameters
        ----------
        list_ : list[Any]
            List to process
            
        Returns
        -------
        list[Any]
            List with consecutive duplicates removed
        """
        if not list_:
            return []
            
        result = [list_[0]]
        for item in list_[1:]:
            if item != result[-1]:
                result.append(item)
        return result

    def replace_first_occurrence(
        self,
        list_: list[str],
        str_old_value: str,
        str_new_value: str,
        logger: Optional[Logger] = None
    ) -> list[str]:
        """Replace first occurrence of value in list.
        
        Parameters
        ----------
        list_ : list[str]
            List to modify
        str_old_value : str
            Value to replace
        str_new_value : str
            New value
        logger : Optional[Logger]
            Logger for warnings
            
        Returns
        -------
        list[str]
            Modified list
        """
        if str_old_value in list_:
            list_[list_.index(str_old_value)] = str_new_value
        else:
            CreateLog().log_message(
                logger, 
                f"Value {str_old_value} not found in list", 
                "warning"
            )
        return list_

    def replace_last_occurrence(
        self,
        list_: list[str],
        str_old_value: str,
        str_new_value: str,
        logger: Optional[Logger] = None
    ) -> list[str]:
        """Replace last occurrence of value in list.
        
        Parameters
        ----------
        list_ : list[str]
            List to modify
        str_old_value : str
            Value to replace
        str_new_value : str
            New value
        logger : Optional[Logger]
            Logger for warnings
            
        Returns
        -------
        list[str]
            Modified list
        """
        try:
            idx = len(list_) - 1 - list_[::-1].index(str_old_value)
            list_[idx] = str_new_value
        except ValueError:
            CreateLog().log_message(
                logger,
                f"Value {str_old_value} not found in list",
                "warning"
            )
        return list_