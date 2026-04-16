"""Text file handling utilities.

This module provides a class for reading, writing, and processing text files with various
operations including line manipulation, content validation, and file writing.
"""

from re import sub
from typing import Literal, Optional

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.folders import DirFilesManagement


class HandlingTXTFiles(metaclass=TypeChecker):
	"""Class for handling text file operations."""

	def __init__(self) -> None:
		"""Initialize the HandlingTXTFiles class."""
		self.cls_dir_files = DirFilesManagement()

	def _validate_file_path(self, file_path: str) -> None:
		"""Validate file path exists and is accessible.

		Parameters
		----------
		file_path : str
			Path to the file to validate

		Raises
		------
		ValueError
			If file path is empty
			If file path is not a string
			If file does not exist
		"""
		if not file_path:
			raise ValueError("File path cannot be empty")
		if not isinstance(file_path, str):
			raise ValueError("File path must be a string")
		if not self.cls_dir_files.object_exists(file_path):
			raise ValueError(f"File does not exist at path: {file_path}")

	def read_file(self, complete_path: str, method: Literal["r", "w"] = "r") -> str:
		"""Read content from a text file.

		Parameters
		----------
		complete_path : str
			Complete path to the text file
		method : Literal['r', 'w']
			File access method (default: 'r')

		Returns
		-------
		str
			Content of the text file
		"""
		self._validate_file_path(complete_path)
		with open(complete_path, method) as f:
			return f.read()

	def generator_file(
		self,
		complete_path: str,
		method: Literal["r", "w"] = "r",
		regex: str = r"[^A-Za-z0-9-.*-:-<-=-\s]+",
		non_matching_regex_characaters: str = "|",
	) -> list[str]:
		"""Generate processed lines from a text file using regex substitution.

		Parameters
		----------
		complete_path : str
			Complete path to the text file
		method : Literal['r', 'w']
			File access method (default: 'r')
		regex : str
			Regular expression pattern for substitution
		non_matching_regex_characaters : str
			Replacement string for non-matching characters (default: '|')

		Returns
		-------
		list[str]
			list of processed lines
		"""
		self._validate_file_path(complete_path)
		with open(complete_path, method, encoding="ascii", errors="replace") as f:
			return [
				sub(regex, non_matching_regex_characaters, line) for line in f.read().splitlines()
			]

	def read_first_line(self, complete_path: str, method: Literal["r", "w"] = "r") -> str:
		"""Read the first line of a text file.

		Parameters
		----------
		complete_path : str
			Complete path to the text file
		method : Literal['r', 'w']
			File access method (default: 'r')

		Returns
		-------
		str
			First line of the text file
		"""
		self._validate_file_path(complete_path)
		with open(complete_path, method) as f:
			return f.readline().rstrip()

	def remove_first_n_lines(self, complete_path: str, n: int = 1) -> None:
		"""Remove the first n lines from a text file.

		Parameters
		----------
		complete_path : str
			Complete path to the text file
		n : int
			Number of lines to remove (default: 1)

		Raises
		------
		ValueError
			If n is negative
		"""
		self._validate_file_path(complete_path)
		if n < 0:
			raise ValueError("Number of lines to remove must be non-negative")

		with open(complete_path) as file_in:
			data = file_in.read().splitlines(True)
		with open(complete_path, "w") as f:
			f.writelines(data[n:])

	def write_file(
		self, file_complete_path: str, data_content: str, method: Literal["r", "w"] = "w"
	) -> bool:
		"""Write content to a text file.

		Parameters
		----------
		file_complete_path : str
			Complete path to the text file
		data_content : str
			Content to write to the file
		method : Literal['r', 'w']
			File access method (default: 'w')

		Returns
		-------
		bool
			True if file was successfully written, False otherwise

		Raises
		------
		ValueError
			If file path is empty
			If file path is not a string
		"""
		if not file_complete_path:
			raise ValueError("File path cannot be empty")
		if not isinstance(file_complete_path, str):
			raise ValueError("File path must be a string")

		with open(file_complete_path, method) as f:
			f.write(data_content)

		if not self.cls_dir_files.object_exists(file_complete_path):
			raise ValueError(f"Failed to create file at path: {file_complete_path}")

		return True

	def check_separator_consistency(
		self, file_path: str, list_sep: Optional[list[str]] = None
	) -> bool:
		"""Check if a text file has consistent field separators.

		Parameters
		----------
		file_path : str
			Path to the text file
		list_sep : Optional[list[str]]
			list of possible separators

		Returns
		-------
		bool
			True if consistent separator found, False otherwise

		Raises
		------
		ValueError
			If file path is invalid
			If list_sep is empty
		"""
		if list_sep is None:
			list_sep = [",", ";", "\t"]

		self._validate_file_path(file_path)
		if not list_sep:
			raise ValueError("Separator list cannot be empty")

		with open(file_path) as file:
			list_lines = file.readlines()

		for sep in list_sep:
			field_counts = [len(line.split(sep)) for line in list_lines]
			if len(set(field_counts)) == 1:
				print(f"The file has a consistent separator: '{sep}'")
				return True
		print("The file does not have a consistent separator.")
		return False
