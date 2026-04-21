"""YAML file parsing utilities.

This module provides functionality for reading and parsing YAML files using PyYAML.
It includes proper file handling and YAML loading with safe defaults.
"""

from typing import Any

from yaml import YAMLError, safe_load

from stpstone.transformations.validation.metaclass_type_checker import type_checker


@type_checker
def reading_yaml(file_path: str) -> Any:  # noqa ANN401: typing.Any is not allowed
	"""Read and parse a YAML file.

	Parameters
	----------
	file_path : str
		Path to the YAML file to be read

	Returns
	-------
	Any
		Parsed YAML content as Python objects

	Raises
	------
	TypeError
		If file_path is not a string
	FileNotFoundError
		If the specified file does not exist
	YAMLError
		If the YAML file is malformed

	References
	----------
	.. [1] https://pyyaml.org/wiki/PyYAMLDocumentation
	"""
	if not isinstance(file_path, str):
		raise TypeError("file_path must be a string")

	try:
		with open(file_path, encoding="utf-8") as f:
			return safe_load(f)
	except FileNotFoundError as err:
		raise FileNotFoundError(f"YAML file not found: {file_path}") from err
	except YAMLError as err:
		raise YAMLError(f"Error parsing YAML file: {str(err)}") from err
