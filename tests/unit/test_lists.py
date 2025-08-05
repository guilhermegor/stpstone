"""Unit tests for ListHandler class.

Tests the list manipulation functionality including:
- Occurrence searching (first, closest, regex matches)
- Bounds calculation (lower/upper/middle bounds)
- List operations (deduplication, chunking, flattening)
- Sorting (alphanumeric, pairwise)
- Frequency analysis
- String manipulation within lists
"""

from logging import Logger
from typing import Any

import numpy as np
import pytest

from stpstone.utils.parsers.lists import ListHandler


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def list_handler() -> Any: # noqa ANN401: typing.Any is not allowed 
    """Fixture providing ListHandler instance.

    Returns
    -------
    Any
        Instance of ListHandler
    """
    return ListHandler()


@pytest.fixture
def sample_str_list() -> list[str]:
    """Fixture providing sample string list.

    Returns
    -------
    list[str]
        List with mixed case strings
    """
    return ["apple", "Banana", "CHERRY", "date", "Fig", "GRAPE"]


@pytest.fixture
def sample_num_list() -> list[int]:
    """Fixture providing sample number list.

    Returns
    -------
    list[int]
        List of numbers in random order
    """
    return [5, 2, 8, 1, 9, 3]


@pytest.fixture
def sample_sorted_num_list() -> list[int]:
    """Fixture providing sorted number list.

    Returns
    -------
    list[int]
        List of numbers in ascending order
    """
    return [1, 2, 3, 5, 8, 9]


@pytest.fixture
def sample_nested_list() -> list[list[int]]:
    """Fixture providing nested list.

    Returns
    -------
    list[list[int]]
        List containing sublists
    """
    return [[1, 2], [3, 4, 5], [6]]


@pytest.fixture
def sample_duplicates_list() -> list[str]:
    """Fixture providing list with duplicates.

    Returns
    -------
    list[str]
        List with duplicate values
    """
    return ["a", "b", "a", "c", "b", "d"]


@pytest.fixture
def sample_consecutive_duplicates() -> list[int]:
    """Fixture providing list with consecutive duplicates.

    Returns
    -------
    list[int]
        List with consecutive duplicate values
    """
    return [1, 1, 2, 2, 3, 4, 4, 5]


# --------------------------
# Tests for _validate_list_not_empty
# --------------------------
def test_validate_list_not_empty_with_empty_list(
    list_handler: Any # noqa ANN401: typing.Any is not allowed 
) -> None:
    """Test raises ValueError when list is empty.

    Verifies
    --------
    - The method raises ValueError when input list is empty
    - The error message is correct

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    
    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Input list cannot be empty"):
        list_handler._validate_list_not_empty([])


def test_validate_list_not_empty_with_valid_list(
    list_handler: Any # noqa ANN401: typing.Any is not allowed 
) -> None:
    """Test passes when list is not empty.

    Verifies
    --------
    - The method doesn't raise exception with non-empty list

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test

    Returns
    -------
    None
    """
    list_handler._validate_list_not_empty([1, 2, 3])


# --------------------------
# Tests for get_first_occurrence_within_list
# --------------------------
def test_get_first_uppercase(
    list_handler: Any, # noqa ANN401: typing.Any is not allowed 
    sample_str_list: list[str]
) -> None:
    """Test finds first uppercase string.

    Verifies
    --------
    - Correctly identifies first all-uppercase string
    - Returns correct index

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    sample_str_list : list[str]
        Sample list containing mixed case strings

    Returns
    -------
    None
    """
    assert list_handler.get_first_occurrence_within_list(
        sample_str_list, bool_uppercase=True) == 2


def test_get_first_uppercase_not_found(
    list_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test returns error code when no uppercase found.

    Verifies
    --------
    - Returns specified error code when no uppercase strings

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test

    Returns
    -------
    None
    """
    assert list_handler.get_first_occurrence_within_list(
        ["apple", "banana"], bool_l_uppercase=True) == -1


def test_get_first_object_occurrence(
    list_handler: Any, # noqa ANN401: typing.Any is not allowed 
    sample_str_list: list[str]
) -> None:
    """Test finds first matching string.

    Verifies
    --------
    - Correctly finds first case-insensitive match
    - Returns correct index

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    sample_str_list : list[str]
        Sample list containing mixed case strings

    Returns
    -------
    None
    """
    assert list_handler.get_first_occurrence_within_list(
        sample_str_list, obj_occurrence="banana") == 1


def test_get_first_object_occurrence_not_found(
    list_handler: Any, # noqa ANN401: typing.Any is not allowed 
    sample_str_list: list[str]
) -> None:
    """Test returns error code when no match found.

    Verifies
    --------
    - Returns specified error code when no match

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    sample_str_list : list[str]
        Sample list containing mixed case strings

    Returns
    -------
    None
    """
    assert list_handler.get_first_occurrence_within_list(
        sample_str_list, obj_occurrence="melon") == -2


def test_get_first_non_alphanumeric(
    list_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test finds first non-alphanumeric string.

    Verifies
    --------
    - Correctly identifies first non-alphanumeric string
    - Returns correct index

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test

    Returns
    -------
    None
    """
    test_list = ["abc", "123", "a1b2", "a@b", "x_y"]
    assert list_handler.get_first_occurrence_within_list(
        test_list, bool_l_regex_alphanumeric_false=True) == 3


def test_get_first_non_alphanumeric_not_found(
    list_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test returns error code when all alphanumeric.

    Verifies
    --------
    - Returns specified error code when all strings are alphanumeric

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test

    Returns
    -------
    None
    """
    assert list_handler.get_first_occurrence_within_list(
        ["abc", "123"], bool_l_regex_alphanumeric_false=True) == -1


def test_get_first_occurrence_invalid_args(
    list_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test raises ValueError with invalid arguments.

    Verifies
    --------
    - Raises ValueError when no search criteria specified
    - Error message is correct

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="No valid search criteria provided"):
        list_handler.get_first_occurrence_within_list(["a", "b"])


# --------------------------
# Tests for get_list_until_invalid_occurrences
# --------------------------
def test_get_list_until_invalid(
    list_handler: Any, # noqa ANN401: typing.Any is not allowed 
    sample_str_list: list[str]
) -> None:
    """Test gets sublist until invalid value.

    Verifies
    --------
    - Correctly stops at first invalid occurrence
    - Returns correct sublist

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    sample_str_list : list[str]
        Sample list containing mixed case strings

    Returns
    -------
    None
    """
    invalid = ["cherry"]
    result = list_handler.get_list_until_invalid_occurrences(
        sample_str_list, invalid)
    assert result == ["apple", "Banana"]


def test_get_list_until_invalid_not_found(
    list_handler: Any, # noqa ANN401: typing.Any is not allowed 
    sample_str_list: list[str]
) -> None:
    """Test returns full list when no invalid values.

    Verifies
    --------
    - Returns complete list when no invalid values found

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    sample_str_list : list[str]
        Sample list containing mixed case strings

    Returns
    -------
    None
    """
    invalid = ["melon"]
    result = list_handler.get_list_until_invalid_occurrences(
        sample_str_list, invalid)
    assert result == sample_str_list


# --------------------------
# Tests for first_numeric
# --------------------------
def test_first_numeric_found(
    list_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test finds first numeric string.

    Verifies
    --------
    - Correctly identifies first numeric string
    - Returns the numeric value

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test

    Returns
    -------
    None
    """
    assert list_handler.first_numeric(["a", "1", "b", "2"]) == "1"


def test_first_numeric_not_found(
    list_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test returns False when no numeric found.

    Verifies
    --------
    - Returns False when no numeric strings in list

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test

    Returns
    -------
    None
    """
    assert list_handler.first_numeric(["a", "b"]) is False


# --------------------------
# Tests for get_lower_upper_bound
# --------------------------
def test_get_lower_upper_bound(
    list_handler: Any, # noqa ANN401: typing.Any is not allowed 
    sample_sorted_num_list: list[int]
) -> None:
    """Test gets correct bounds for value in middle.

    Verifies
    --------
    - Correct lower and upper bounds for value in middle
    - Return type matches expected TypedDict

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    sample_sorted_num_list : list[int]
        Sample sorted list of numbers

    Returns
    -------
    None
    """
    result = list_handler.get_lower_upper_bound(sample_sorted_num_list, 4)
    assert result["lower_bound"] == 3
    assert result["upper_bound"] == 5


def test_get_lower_upper_bound_exact_match(
    list_handler: Any, # noqa ANN401: typing.Any is not allowed 
    sample_sorted_num_list: list[int]
) -> None:
    """Test gets correct bounds for existing value.

    Verifies
    --------
    - Correct bounds when value exists in list
    - Handles exact matches properly

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    sample_sorted_num_list : list[int]
        Sample sorted list of numbers

    Returns
    -------
    None
    """
    result = list_handler.get_lower_upper_bound(sample_sorted_num_list, 5)
    assert result["lower_bound"] == 5
    assert result["upper_bound"] == 8


def test_get_lower_upper_bound_out_of_range(
    list_handler: Any, # noqa ANN401: typing.Any is not allowed 
    sample_sorted_num_list: list[int]
) -> None:
    """Test raises error for out of range value.

    Verifies
    --------
    - Raises ValueError when value outside list range
    - Error message contains correct information

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    sample_sorted_num_list : list[int]
        Sample sorted list of numbers

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="10 value is outside"):
        list_handler.get_lower_upper_bound(sample_sorted_num_list, 10)


# --------------------------
# Tests for get_lower_mid_upper_bound
# --------------------------
def test_get_lower_mid_upper_bound(
    list_handler: Any, # noqa ANN401: typing.Any is not allowed 
    sample_sorted_num_list: list[int]
) -> None:
    """Test gets correct bounds for value in middle.

    Verifies
    --------
    - Correct lower, middle and upper bounds
    - end_of_list flag is False

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    sample_sorted_num_list : list[int]
        Sample sorted list of numbers

    Returns
    -------
    None
    """
    result = list_handler.get_lower_mid_upper_bound(sample_sorted_num_list, 4)
    assert result["lower_bound"] == 3
    assert result["middle_bound"] == 5
    assert result["upper_bound"] == 8
    assert result["end_of_list"] is False


def test_get_lower_mid_upper_bound_end_of_list(
    list_handler: Any, # noqa ANN401: typing.Any is not allowed 
    sample_sorted_num_list: list[int]
) -> None:
    """Test handles end of list case.

    Verifies
    --------
    - Correct bounds when near end of list
    - end_of_list flag is True

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    sample_sorted_num_list : list[int]
        Sample sorted list of numbers

    Returns
    -------
    None
    """
    result = list_handler.get_lower_mid_upper_bound(sample_sorted_num_list, 8)
    assert result["lower_bound"] == 5
    assert result["middle_bound"] == 8
    assert result["upper_bound"] == 9
    assert result["end_of_list"] is True


def test_get_lower_mid_upper_bound_small_list(
    list_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test raises error for small lists.

    Verifies
    --------
    - Raises ValueError when list has <= 2 elements
    - Error message is correct

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Input list must have more than 2"):
        list_handler.get_lower_mid_upper_bound([1, 2], 1.5)


# --------------------------
# Tests for closest_bound
# --------------------------
def test_closest_bound(
    list_handler: Any, # noqa ANN401: typing.Any is not allowed 
    sample_sorted_num_list: list[int]
) -> None:
    """Test finds closest value in sorted list.

    Verifies
    --------
    - Correctly identifies closest value
    - Returns the closest value

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    sample_sorted_num_list : list[int]
        Sample sorted list of numbers

    Returns
    -------
    None
    """
    assert list_handler.closest_bound(sample_sorted_num_list, 4) == 5


# --------------------------
# Tests for closest_number_within_list
# --------------------------
def test_closest_number_within_list(
    list_handler: Any, # noqa ANN401: typing.Any is not allowed 
    sample_num_list: list[int]
) -> None:
    """Test finds closest number in unsorted list.

    Verifies
    --------
    - Correctly identifies closest number
    - Works with unsorted lists

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    sample_num_list : list[int]
        Sample list of numbers in random order

    Returns
    -------
    None
    """
    assert list_handler.closest_number_within_list(sample_num_list, 4) == 5


# --------------------------
# Tests for first_occurrence_like
# --------------------------
def test_first_occurrence_like(
    list_handler: Any, # noqa ANN401: typing.Any is not allowed 
    sample_str_list: list[str]
) -> None:
    """Test finds first matching string.

    Verifies
    --------
    - Correctly finds first case-insensitive match
    - Returns correct index

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    sample_str_list : list[str]
        Sample list containing mixed case strings

    Returns
    -------
    None
    """
    assert list_handler.first_occurrence_like(sample_str_list, "banana") == 1


def test_first_occurrence_like_not_found(
    list_handler: Any, # noqa ANN401: typing.Any is not allowed 
    sample_str_list: list[str]
) -> None:
    """Test raises error when no match found.

    Verifies
    --------
    - Raises ValueError when no match
    - Error message contains pattern

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    sample_str_list : list[str]
        Sample list containing mixed case strings

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="No match found for pattern"):
        list_handler.first_occurrence_like(sample_str_list, "melon")


# --------------------------
# Tests for remove_duplicates
# --------------------------
def test_remove_duplicates(
    list_handler: Any, # noqa ANN401: typing.Any is not allowed 
    sample_duplicates_list: list[str]
) -> None:
    """Test removes duplicates while preserving order.

    Verifies
    --------
    - Removes duplicate values
    - Preserves order of first occurrences

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    sample_duplicates_list : list[str]
        Sample list containing duplicate values

    Returns
    -------
    None
    """
    result = list_handler.remove_duplicates(sample_duplicates_list)
    assert result == ["a", "b", "c", "d"]


# --------------------------
# Tests for nth_smallest_numbers
# --------------------------
def test_nth_smallest_numbers(
    list_handler: Any, # noqa ANN401: typing.Any is not allowed 
    sample_num_list: list[int]
) -> None:
    """Test gets n smallest numbers.

    Verifies
    --------
    - Correctly identifies n smallest numbers
    - Returns as numpy array

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    sample_num_list : list[int]
        Sample list of numbers in random order

    Returns
    -------
    None
    """
    result = list_handler.nth_smallest_numbers(sample_num_list, 3)
    assert isinstance(result, np.ndarray)
    assert list(result) == [1, 2, 3]


# --------------------------
# Tests for extend_lists
# --------------------------
def test_extend_lists_no_duplicates(
    list_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test combines lists without duplicates.

    Verifies
    --------
    - Correctly combines multiple lists
    - Removes duplicates when requested

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test

    Returns
    -------
    None
    """
    list1 = [1, 2]
    list2 = [2, 3]
    result = list_handler.extend_lists(list1, list2, bool_l_remove_duplicates=True)
    assert result == [1, 2, 3]


def test_extend_lists_with_duplicates(
    list_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test combines lists keeping duplicates.

    Verifies
    --------
    - Correctly combines multiple lists
    - Preserves duplicates when requested

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test

    Returns
    -------
    None
    """
    list1 = [1, 2]
    list2 = [2, 3]
    result = list_handler.extend_lists(list1, list2, bool_l_remove_duplicates=False)
    assert result == [1, 2, 2, 3]


# --------------------------
# Tests for chunk_list
# --------------------------
def test_chunk_list_with_joining(
    list_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test chunks list with string joining.

    Verifies
    --------
    - Correctly splits list into chunks
    - Joins chunks with specified delimiter

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test

    Returns
    -------
    None
    """
    test_list = ["a", "b", "c", "d", "e"]
    result = list_handler.chunk_list(test_list, " ", 2)
    assert result == ["a b", "c d", "e"]


def test_chunk_list_without_joining(
    list_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test chunks list into sublists.

    Verifies
    --------
    - Correctly splits list into sublists
    - Returns list of lists when no join char specified

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test

    Returns
    -------
    None
    """
    test_list = ["a", "b", "c", "d", "e"]
    result = list_handler.chunk_list(test_list, None, 2)
    assert result == [["a", "b"], ["c", "d"], ["e"]]


# --------------------------
# Tests for cartesian_product
# --------------------------
def test_cartesian_product_full(
    list_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test computes full Cartesian product.

    Verifies
    --------
    - Correctly computes product of multiple lists
    - Returns all combinations

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test

    Returns
    -------
    None
    """
    list1 = [1, 2]
    list2 = ["a", "b"]
    result = list_handler.cartesian_product([list1, list2])
    assert result == [(1, "a"), (1, "b"), (2, "a"), (2, "b")]


def test_cartesian_product_truncated(
    list_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test computes truncated Cartesian product.

    Verifies
    --------
    - Correctly truncates product tuples
    - Filters duplicate prefixes

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test

    Returns
    -------
    None
    """
    list1 = [1, 1, 2]
    list2 = ["a", "b", "c"]
    result = list_handler.cartesian_product([list1, list2], int_break_n_n=1)
    assert result == [(1,), (2,)]


# --------------------------
# Tests for sort_alphanumeric
# --------------------------
def test_sort_alphanumeric(
    list_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test sorts alphanumeric strings naturally.

    Verifies
    --------
    - Correctly sorts mixed alphanumeric strings
    - Handles numeric portions properly

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test

    Returns
    -------
    None
    """
    test_list = ["item2", "item10", "item1"]
    result = list_handler.sort_alphanumeric(test_list)
    assert result == ["item1", "item2", "item10"]


# --------------------------
# Tests for pairwise
# --------------------------
def test_pairwise(
    list_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test generates successive pairs.

    Verifies
    --------
    - Correctly generates overlapping pairs
    - Handles even and odd length lists

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test

    Returns
    -------
    None
    """
    result = list_handler.pairwise([1, 2, 3, 4])
    assert result == [(1, 2), (2, 3), (3, 4)]


# --------------------------
# Tests for discard_from_list
# --------------------------
def test_discard_from_list(
    list_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test removes specified items.

    Verifies
    --------
    - Correctly removes all specified items
    - Preserves order of remaining items

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test

    Returns
    -------
    None
    """
    test_list = [1, 2, 3, 4, 5]
    result = list_handler.discard_from_list(test_list, [2, 4])
    assert result == [1, 3, 5]


# --------------------------
# Tests for absolute_frequency
# --------------------------
def test_absolute_frequency(
    list_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test computes frequency counts.

    Verifies
    --------
    - Correctly counts occurrences of each item
    - Returns Counter object

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test

    Returns
    -------
    None
    """
    test_list = ["a", "b", "a", "c"]
    result = list_handler.absolute_frequency(test_list)
    assert result == {"a": 2, "b": 1, "c": 1}


# --------------------------
# Tests for flatten_list
# --------------------------
def test_flatten_list(
    list_handler: Any, # noqa ANN401: typing.Any is not allowed 
    sample_nested_list: list[list[int]]
) -> None:
    """Test flattens nested list.

    Verifies
    --------
    - Correctly flattens one level of nesting
    - Preserves order of elements

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    sample_nested_list : list[list[int]]
        Sample nested list containing sublists

    Returns
    -------
    None
    """
    result = list_handler.flatten_list(sample_nested_list)
    assert result == [1, 2, 3, 4, 5, 6]


# --------------------------
# Tests for remove_consecutive_duplicates
# --------------------------
def test_remove_consecutive_duplicates(
    list_handler: Any, # noqa ANN401: typing.Any is not allowed 
    sample_consecutive_duplicates: list[int]
) -> None:
    """Test removes consecutive duplicates.

    Verifies
    --------
    - Removes consecutive duplicate values
    - Preserves non-consecutive duplicates

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    sample_consecutive_duplicates : list[int]
        Sample list containing consecutive duplicates

    Returns
    -------
    None
    """
    result = list_handler.remove_consecutive_duplicates(sample_consecutive_duplicates)
    assert result == [1, 2, 3, 4, 5]


# --------------------------
# Tests for replace_first_occurrence
# --------------------------
def test_replace_first_occurrence(
    list_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test replaces first occurrence.

    Verifies
    --------
    - Correctly replaces first matching value
    - Returns modified list

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test

    Returns
    -------
    None
    """
    test_list = ["a", "b", "c", "b"]
    result = list_handler.replace_first_occurrence(test_list, "b", "x")
    assert result == ["a", "x", "c", "b"]


def test_replace_first_occurrence_not_found(
    list_handler: Any, # noqa ANN401: typing.Any is not allowed 
    mocker: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test logs warning when value not found.

    Verifies
    --------
    - Logs warning when value not in list
    - Returns original list unchanged

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    mocker : Any
        Mocking library to simulate logging

    Returns
    -------
    None
    """
    mock_logger = mocker.Mock(spec=Logger)
    test_list = ["a", "b"]
    result = list_handler.replace_first_occurrence(test_list, "c", "x", mock_logger)
    assert result == test_list
    mock_logger.warning.assert_called_once()


# --------------------------
# Tests for replace_last_occurrence
# --------------------------
def test_replace_last_occurrence(
    list_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test replaces last occurrence.

    Verifies
    --------
    - Correctly replaces last matching value
    - Returns modified list

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test

    Returns
    -------
    None
    """
    test_list = ["a", "b", "c", "b"]
    result = list_handler.replace_last_occurrence(test_list, "b", "x")
    assert result == ["a", "b", "c", "x"]


def test_replace_last_occurrence_not_found(
    list_handler: Any, # noqa ANN401: typing.Any is not allowed 
    mocker: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test logs warning when value not found.

    Verifies
    --------
    - Logs warning when value not in list
    - Returns original list unchanged

    Parameters
    ----------
    list_handler : Any
        Instance of ListHandler to test
    mocker : Any
        Mocking library to simulate logging

    Returns
    -------
    None
    """
    mock_logger = mocker.Mock(spec=Logger)
    test_list = ["a", "b"]
    result = list_handler.replace_last_occurrence(
        test_list, "c", "x", mock_logger)
    assert result == test_list
    mock_logger.warning.assert_called_once()