"""Unit tests for HandlingDicts class.

Tests the dictionary manipulation utilities including:
- Finding min/max values in dictionaries
- Merging and sorting dictionaries
- Filtering and grouping operations
- Key-value pair manipulations
- Placeholder replacement functionality
"""

from typing import Any, Union

import pytest

from stpstone.utils.parsers.dicts import HandlingDicts


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def handler() -> HandlingDicts:
    """Fixture providing a HandlingDicts instance for testing.
    
    Returns
    -------
    HandlingDicts
        Instance of HandlingDicts class
    """
    return HandlingDicts()


@pytest.fixture
def sample_dict() -> dict[str, int]:
    """Fixture providing a sample dictionary for testing.
    
    Returns
    -------
    dict[str, int]
        Dictionary with string keys and integer values
    """
    return {"a": 1, "b": 2, "c": 3, "d": 0}


@pytest.fixture
def sample_dict_list() -> list[dict[str, Union[int, str]]]:
    """Fixture providing a list of dictionaries for testing.
    
    Returns
    -------
    list[dict[str, Union[int, str]]]
        list of dictionaries with mixed types
    """
    return [
        {"id": 1, "value": 10, "category": "A"},
        {"id": 2, "value": 20, "category": "B"},
        {"id": 3, "value": 15, "category": "A"},
        {"id": 4, "value": 5, "category": "C"},
    ]


@pytest.fixture
def placeholder_dict() -> dict[str, str]:
    """Fixture providing a dictionary with placeholders.
    
    Returns
    -------
    dict[str, str]
        Dictionary containing {{placeholder}} patterns
    """
    return {
        "name": "{{first}} {{last}}",
        "address": "{{street}}, {{city}}",
        "nested": {
            "greeting": "Hello {{first}}!",
            "list": ["{{first}}", "{{last}}"]
        }
    }


# --------------------------
# Tests for Basic Dictionary Operations
# --------------------------
class TestBasicOperations:
    """Tests for basic dictionary operations."""
    
    def test_min_val_key(self, handler: HandlingDicts, sample_dict: dict[str, int]) -> None:
        """Test finding key with minimum value.
        
        Verifies
        --------
        - Correctly identifies key-value pair with minimum value
        - Returns tuple in (key, value) format
        - Works with different value types

        Parameters
        ----------
        handler : HandlingDicts
            Instance of HandlingDicts class
        sample_dict : dict[str, int]
            Sample dictionary with string keys and integer values

        Returns
        -------
        None
        """
        result = handler.min_val_key(sample_dict)
        assert result == ("d", 0)
        
        # Test with float values
        float_dict = {"x": 1.5, "y": 0.5, "z": 2.0}
        assert handler.min_val_key(float_dict) == ("y", 0.5)
        
    def test_min_val_key_empty_dict(self, handler: HandlingDicts) -> None:
        """Test min_val_key with empty dictionary raises ValueError.
        
        Verifies
        --------
        - Properly raises ValueError when input dict is empty
        - Error message is appropriate

        Parameters
        ----------
        handler : HandlingDicts
            Instance of HandlingDicts class

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Input dictionary cannot be empty"):
            handler.min_val_key({})
            
    def test_max_val_key(self, handler: HandlingDicts, sample_dict: dict[str, int]) -> None:
        """Test finding key with maximum value.
        
        Verifies
        --------
        - Correctly identifies key-value pair with maximum value
        - Returns tuple in (key, value) format
        - Works with different value types

        Parameters
        ----------
        handler : HandlingDicts
            Instance of HandlingDicts class
        sample_dict : dict[str, int]
            Sample dictionary with string keys and integer values

        Returns
        -------
        None
        """
        assert handler.max_val_key(sample_dict) == ("c", 3)
        
        # Test with negative values
        neg_dict = {"x": -1, "y": -5, "z": -3}
        assert handler.max_val_key(neg_dict) == ("x", -1)
        
    def test_merge_n_dicts(self, handler: HandlingDicts) -> None:
        """Test merging multiple dictionaries.
        
        Verifies
        --------
        - Correctly merges multiple dictionaries
        - Later dictionaries overwrite earlier ones
        - Works with empty dict inputs

        Parameters
        ----------
        handler : HandlingDicts
            Instance of HandlingDicts class

        Returns
        -------
        None
        """
        dict1 = {"a": 1, "b": 2}
        dict2 = {"b": 3, "c": 4}
        dict3 = {"c": 5, "d": 6}
        
        result = handler.merge_n_dicts(dict1, dict2, dict3)
        assert result == {"a": 1, "b": 3, "c": 5, "d": 6}
        
        # Test with empty dict
        assert handler.merge_n_dicts({}, dict1) == dict1
        assert handler.merge_n_dicts(dict1, {}) == dict1


# --------------------------
# Tests for Sorting and Filtering
# --------------------------
class TestSortingFiltering:
    """Tests for dictionary sorting and filtering operations."""
    
    def test_multikeysort(self, handler: HandlingDicts, sample_dict_list: list[dict]) -> None:
        """Test sorting by multiple keys.
        
        Verifies
        --------
        - Correctly sorts by multiple keys
        - Handles ascending and descending order
        - Maintains stable sort

        Parameters
        ----------
        handler : HandlingDicts
            Instance of HandlingDicts class
        sample_dict_list : list[dict]
            List of dictionaries to sort

        Returns
        -------
        None
        """
        # Single key sort
        sorted_by_id = handler.multikeysort(sample_dict_list, ["id"])
        assert [d["id"] for d in sorted_by_id] == [1, 2, 3, 4]
        
        # Descending sort
        sorted_by_id_desc = handler.multikeysort(sample_dict_list, ["-id"])
        assert [d["id"] for d in sorted_by_id_desc] == [4, 3, 2, 1]
        
        # Multiple key sort
        sorted_by_category_value = handler.multikeysort(sample_dict_list, ["category", "value"])
        assert sorted_by_category_value[0]["category"] == "A"
        assert sorted_by_category_value[0]["value"] == 10
        
    def test_filter_list_ser(self, handler: HandlingDicts, sample_dict_list: list[dict]) -> None:
        """Test filtering dictionary list by key condition.
        
        Verifies
        --------
        - Correctly filters based on different comparison types
        - Handles numeric comparisons
        - Raises ValueError for invalid filter types

        Parameters
        ----------
        handler : HandlingDicts
            Instance of HandlingDicts class
        sample_dict_list : list[dict]
            List of dictionaries to filter

        Returns
        -------
        None
        """
        # Equality filter
        filtered = handler.filter_list_ser(sample_dict_list, "category", "A", "equal")
        assert len(filtered) == 2
        assert all(d["category"] == "A" for d in filtered)
        
        # Greater than filter
        filtered = handler.filter_list_ser(sample_dict_list, "value", 15, "greater_than")
        assert len(filtered) == 1
        assert filtered[0]["value"] == 20
        
        # Invalid filter type
        with pytest.raises(ValueError, match="Invalid filter type"):
            handler.filter_list_ser(sample_dict_list, "value", 10, "invalid_type")


# --------------------------
# Tests for Dictionary Manipulation
# --------------------------
class TestDictionaryManipulation:
    """Tests for advanced dictionary manipulation methods."""
    
    def test_add_key_value_to_dicts(
        self, 
        handler: HandlingDicts, 
        sample_dict_list: list[dict]
    ) -> None:
        """Test adding key-value pairs to dictionaries.
        
        Verifies
        --------
        - Adds static values correctly
        - Handles callable values
        - Processes list of key-value dicts
        - Raises appropriate errors

        Parameters
        ----------
        handler : HandlingDicts
            Instance of HandlingDicts class
        sample_dict_list : list[dict]
            List of dictionaries to modify

        Returns
        -------
        None
        """
        # add static value
        result = handler.add_key_value_to_dicts(sample_dict_list, "new_key", "static_value")
        assert all("new_key" in d for d in result)
        assert all(d["new_key"] == "static_value" for d in result)
        
        # add computed value
        def multiply(x: int, y: int = 2) -> int:
            """Multiply x by y.
            
            Parameters
            ----------
            x : int
                Value to multiply
            y : int, optional
                Multiplier, default is 2
            
            Returns
            -------
            int
                The product of x and y
            """
            return x * y
            
        result = handler.add_key_value_to_dicts(
            sample_dict_list, 
            "doubled", 
            multiply, 
            ["value"],
            {"y": 2}
        )
        assert result[0]["doubled"] == 20  # 10 * 2
        
        # Test error cases
        with pytest.raises(ValueError, match="value must be provided"):
            handler.add_key_value_to_dicts(sample_dict_list, "key", None)
            
        with pytest.raises(TypeError, match="must be one of types"):
            handler.add_key_value_to_dicts(sample_dict_list, 123, "value")  # type: ignore
            
    def test_fill_placeholders(
        self, 
        handler: HandlingDicts, 
        placeholder_dict: dict[str, Any]
    ) -> None:
        """Test placeholder replacement in dictionaries.
        
        Verifies
        --------
        - Replaces placeholders in strings
        - Handles nested dictionaries
        - Processes lists containing placeholders
        - Leaves unmatched placeholders intact

        Parameters
        ----------
        handler : HandlingDicts
            Instance of HandlingDicts class
        placeholder_dict : dict[str, Any]
            Dictionary containing placeholders to replace

        Returns
        -------
        None
        """
        replacements = {
            "first": "John",
            "last": "Doe",
            "street": "123 Main",
            "city": "Anytown"
        }
        
        result = handler.fill_placeholders(placeholder_dict, replacements)
        
        assert result["name"] == "John Doe"
        assert result["address"] == "123 Main, Anytown"
        assert result["nested"]["greeting"] == "Hello John!"
        assert result["nested"]["list"] == ["John", "Doe"]
        
        # Test with missing replacement
        result = handler.fill_placeholders(placeholder_dict, {"first": "Jane"})
        assert "{{last}}" in result["name"]


# --------------------------
# Tests for Special Cases
# --------------------------
class TestSpecialCases:
    """Tests for edge cases and error conditions."""
    
    def test_empty_inputs(self, handler: HandlingDicts) -> None:
        """Test methods with empty inputs.
        
        Verifies
        --------
        - Appropriate handling of empty dictionaries
        - Proper error messages

        Parameters
        ----------
        handler : HandlingDicts
            Instance of HandlingDicts class

        Returns
        -------
        None
        """
        # Test empty dict for min/max
        with pytest.raises(ValueError):
            handler.min_val_key({})
            
        with pytest.raises(ValueError):
            handler.max_val_key({})
            
        # Test empty list for filtering
        assert handler.filter_list_ser([], "key", 1, "equal") == []
        
    def test_invalid_types(self, handler: HandlingDicts) -> None:
        """Test type validation in methods.
        
        Verifies
        --------
        - Proper type checking for inputs
        - Appropriate error messages

        Parameters
        ----------
        handler : HandlingDicts
            Instance of HandlingDicts class

        Returns
        -------
        None
        """
        # Test non-dict input for merge
        with pytest.raises(TypeError, match="must be of type"):
            handler.merge_n_dicts("not a dict")  # type: ignore
            
        # Test invalid key types for pair_keys_with_values
        with pytest.raises(ValueError):
            handler.pair_keys_with_values(["a", "b"], [[1]])  # Length mismatch
            
    def test_none_values(self, handler: HandlingDicts) -> None:
        """Test handling of None values.
        
        Verifies
        --------
        - Methods handle None values appropriately
        - None comparisons work as expected

        Parameters
        ----------
        handler : HandlingDicts
            Instance of HandlingDicts class

        Returns
        -------
        None
        """
        dict_with_none = {"a": None, "b": 1, "c": None}
        
        # Min/max should skip None values
        assert handler.min_val_key(dict_with_none) == ("b", 1)
        assert handler.max_val_key(dict_with_none) == ("b", 1)
        
        # Filtering with None
        list_with_none = [{"x": 1}, {"x": None}, {"x": 2}]
        filtered = handler.filter_list_ser(list_with_none, "x", None, "equal")
        assert len(filtered) == 1
        assert filtered[0]["x"] is None


# --------------------------
# Tests for Data Structure Operations
# --------------------------
class TestDataStructureOperations:
    """Tests for operations involving complex data structures."""
    
    def test_pair_keys_with_values(self, handler: HandlingDicts) -> None:
        """Test pairing keys with values to create dictionaries.
        
        Verifies
        --------
        - Correct pairing of keys with sublist values
        - Proper handling of different value types
        - Length validation

        Parameters
        ----------
        handler : HandlingDicts
            Instance of HandlingDicts class

        Returns
        -------
        None
        """
        keys = ["id", "name", "value"]
        values = [
            [1, "Alice", 100],
            [2, "Bob", 200],
            [3, "Charlie", 300]
        ]
        
        result = handler.pair_keys_with_values(keys, values)
        assert len(result) == 3
        assert result[0] == {"id": 1, "name": "Alice", "value": 100}
        assert result[1]["name"] == "Bob"
        
        # Test length mismatch
        with pytest.raises(ValueError):
            handler.pair_keys_with_values(keys, [[1, "Alice"]])
            
    def test_pair_headers_with_data(self, handler: HandlingDicts) -> None:
        """Test pairing headers with flat data list.
        
        Verifies
        --------
        - Correct chunking of flat list
        - Proper dictionary construction
        - Length validation

        Parameters
        ----------
        handler : HandlingDicts
            Instance of HandlingDicts class

        Returns
        -------
        None
        """
        headers = ["id", "name"]
        data = [1, "Alice", 2, "Bob", 3, "Charlie"]
        
        result = handler.pair_headers_with_data(headers, data)
        assert len(result) == 3
        assert result[1] == {"id": 2, "name": "Bob"}
        
        # Test length mismatch
        with pytest.raises(ValueError):
            handler.pair_headers_with_data(headers, [1, "Alice", 2])


# --------------------------
# Tests for Grouping and Aggregation
# --------------------------
class TestGroupingAggregation:
    """Tests for dictionary grouping and aggregation methods."""
    
    def test_sum_values_selected_keys(
        self, 
        handler: HandlingDicts, 
        sample_dict_list: list[dict]
    ) -> None:
        """Test summing values for selected keys.
        
        Verifies
        --------
        - Correct summation of numeric values
        - Proper handling of selected keys
        - Non-selected keys remain unchanged

        Parameters
        ----------
        handler : HandlingDicts
            Instance of HandlingDicts class
        sample_dict_list : list[dict]
            List of dictionaries to sum values from

        Returns
        -------
        None
        """
        # sum selected keys
        result = handler.sum_values_selected_keys(
            sample_dict_list, 
            ["value"], 
            True
        )
        assert result["value"] == 50  # 10 + 20 + 15 + 5
        assert "category" in result  # Other keys should be present
        
        # without summing (keep as lists)
        result = handler.sum_values_selected_keys(
            sample_dict_list,
            ["value"],
            False
        )
        assert isinstance(result["value"], list)
        assert len(result["value"]) == 4
        
    def test_merge_values_foreigner_keys(
        self, 
        handler: HandlingDicts, 
        sample_dict_list: list[dict]
    ) -> None:
        """Test merging dictionaries by foreign key.
        
        Verifies
        --------
        - Correct grouping by foreign key
        - Proper merging of specified keys
        - Returns expected number of groups

        Parameters
        ----------
        handler : HandlingDicts
            Instance of HandlingDicts class
        sample_dict_list : list[dict]
            List of dictionaries to merge

        Returns
        -------
        None
        """
        result = handler.merge_values_foreigner_keys(
            sample_dict_list,
            "category",
            ["value"]
        )
        
        # should have one dict per unique category
        categories = {d["category"] for d in sample_dict_list}
        assert len(result) == len(categories)
        
        # check summed values
        category_a = next(d for d in result if d["category"] == "A")
        assert category_a["value"] == 25  # 10 + 15


# --------------------------
# Tests for N-smallest/largest
# --------------------------
class TestNSmallestLargest:
    """Tests for finding n smallest/largest dictionaries by key."""
    
    def test_n_smallest(
        self, 
        handler: HandlingDicts, 
        sample_dict_list: list[dict]
    ) -> None:
        """Test finding n smallest dictionaries by key.
        
        Verifies
        --------
        - Correct ordering by specified key
        - Returns exactly n items
        - Handles edge cases (n > length, n = 0)

        Parameters
        ----------
        handler : HandlingDicts
            Instance of HandlingDicts class
        sample_dict_list : list[dict]
            List of dictionaries to find smallest items from

        Returns
        -------
        None
        """
        # normal case
        result = handler.n_smallest(sample_dict_list, "value", 2)
        assert len(result) == 2
        assert result[0]["value"] == 5
        assert result[1]["value"] == 10
        
        # edge cases
        assert len(handler.n_smallest(sample_dict_list, "value", 10)) == 4
        assert len(handler.n_smallest(sample_dict_list, "value", 0)) == 0
        
    def test_n_largest(
        self, 
        handler: HandlingDicts, 
        sample_dict_list: list[dict]
    ) -> None:
        """Test finding n largest dictionaries by key.
        
        Verifies
        --------
        - Correct ordering by specified key
        - Returns exactly n items
        - Handles edge cases (n > length, n = 0)

        Parameters
        ----------
        handler : HandlingDicts
            Instance of HandlingDicts class
        sample_dict_list : list[dict]
            List of dictionaries to find largest items from

        Returns
        -------
        None
        """
        # normal case
        result = handler.n_largest(sample_dict_list, "value", 2)
        assert len(result) == 2
        assert result[0]["value"] == 20
        assert result[1]["value"] == 15
        
        # edge cases
        assert len(handler.n_largest(sample_dict_list, "value", 10)) == 4
        assert len(handler.n_largest(sample_dict_list, "value", 0)) == 0