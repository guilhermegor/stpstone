"""Unit tests for YAML file parsing utilities.

Tests the functionality for reading and parsing YAML files, including:
- Successful parsing of valid YAML files
- Proper error handling for missing files
- Correct exception raising for malformed YAML
- Type validation of input parameters
"""

from pathlib import Path

import pytest
import yaml

from stpstone.utils.parsers.yaml import reading_yaml


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def valid_yaml_file(tmp_path: Path) -> Path:
	"""Fixture providing a temporary valid YAML file.

	Parameters
	----------
	tmp_path : Path
		Pytest temporary directory fixture

	Returns
	-------
	Path
		Path to the created YAML file
	"""
	yaml_path = tmp_path / "test.yaml"
	yaml_content = """
    key1: value1
    key2:
      - item1
      - item2
    key3:
      subkey: subvalue
    """
	yaml_path.write_text(yaml_content)
	return yaml_path


@pytest.fixture
def invalid_yaml_file(tmp_path: Path) -> Path:
	"""Fixture providing a temporary invalid YAML file.

	Parameters
	----------
	tmp_path : Path
		Pytest temporary directory fixture

	Returns
	-------
	Path
		Path to the created invalid YAML file
	"""
	yaml_path = tmp_path / "invalid.yaml"
	yaml_content = """
    key1: value1
      key2: [item1, item2
    """
	yaml_path.write_text(yaml_content)
	return yaml_path


# --------------------------
# Tests
# --------------------------
def test_reading_valid_yaml(valid_yaml_file: Path) -> None:
	"""Test reading a valid YAML file.

	Verifies
	--------
	- The function correctly parses a valid YAML file
	- The returned structure matches the expected content
	- The returned types are correct

	Parameters
	----------
	valid_yaml_file : Path
		Path to valid YAML file fixture
	"""
	result = reading_yaml(str(valid_yaml_file))

	assert isinstance(result, dict)
	assert result["key1"] == "value1"
	assert isinstance(result["key2"], list)
	assert result["key2"] == ["item1", "item2"]
	assert isinstance(result["key3"], dict)
	assert result["key3"]["subkey"] == "subvalue"


def test_file_not_found() -> None:
	"""Test behavior with non-existent file path.

	Verifies
	--------
	- The function raises FileNotFoundError for missing files
	- The error message contains the missing file path
	"""
	non_existent_path = "/nonexistent/path/to/file.yaml"
	with pytest.raises(FileNotFoundError) as excinfo:
		reading_yaml(non_existent_path)
	assert non_existent_path in str(excinfo.value)


def test_invalid_yaml(invalid_yaml_file: Path) -> None:
	"""Test behavior with invalid YAML content.

	Verifies
	--------
	- The function raises yaml.YAMLError for malformed YAML
	- The error message indicates a parsing error

	Parameters
	----------
	invalid_yaml_file : Path
		Path to invalid YAML file fixture

	Returns
	-------
	None
	"""
	with pytest.raises(yaml.YAMLError) as excinfo:
		reading_yaml(str(invalid_yaml_file))
	assert "Error parsing YAML file" in str(excinfo.value)


def test_non_string_input() -> None:
	"""Test behavior with non-string input path.

	Verifies
	--------
	- The function raises TypeError for non-string inputs

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError):
		reading_yaml(123)  # type: ignore
	with pytest.raises(TypeError):
		reading_yaml(["path"])  # type: ignore


def test_empty_file(tmp_path: Path) -> None:
	"""Test behavior with empty YAML file.

	Verifies
	--------
	- The function handles empty YAML files correctly
	- Returns None for empty files (PyYAML behavior)

	Parameters
	----------
	tmp_path : Path
		Pytest temporary directory fixture

	Returns
	-------
	None
	"""
	empty_path = tmp_path / "empty.yaml"
	empty_path.write_text("")

	result = reading_yaml(str(empty_path))
	assert result is None


def test_nested_structures(tmp_path: Path) -> None:
	"""Test parsing of nested YAML structures.

	Verifies
	--------
	- The function correctly handles nested dictionaries and lists
	- Complex structures are properly parsed

	Parameters
	----------
	tmp_path : Path
		Pytest temporary directory fixture

	Returns
	-------
	None
	"""
	nested_path = tmp_path / "nested.yaml"
	nested_content = """
    outer:
      inner:
        - name: item1
          value: 1
        - name: item2
          value: 2
    """
	nested_path.write_text(nested_content)

	result = reading_yaml(str(nested_path))

	assert isinstance(result, dict)
	assert isinstance(result["outer"], dict)
	assert isinstance(result["outer"]["inner"], list)
	assert len(result["outer"]["inner"]) == 2
	assert result["outer"]["inner"][0]["name"] == "item1"
	assert result["outer"]["inner"][1]["value"] == 2


def test_special_characters(tmp_path: Path) -> None:
	"""Test parsing of YAML with special characters.

	Verifies
	--------
	- The function handles special characters correctly
	- Unicode characters are properly preserved

	Parameters
	----------
	tmp_path : Path
		Pytest temporary directory fixture

	Returns
	-------
	None
	"""
	special_path = tmp_path / "special.yaml"
	special_content = """
    special: "üñîçø∂é"
    emoji: "😊"
    """
	special_path.write_text(special_content)

	result = reading_yaml(str(special_path))

	assert result["special"] == "üñîçø∂é"
	assert result["emoji"] == "😊"
