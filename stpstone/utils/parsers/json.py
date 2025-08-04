"""JSON file handling utilities.

This module provides functionality for working with JSON files, including loading, saving,
and converting between different formats. It handles various JSON operations with robust
error handling and type validation.
"""

import ast
import json
import os
from typing import Any, Optional, Union

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.str import StrHandler


class JsonFiles(metaclass=TypeChecker):
    """Handles JSON file operations including loading, saving, and format conversion."""

    def _validate_message_dict(self, message: Any) -> None:
        """Validate that input is a dictionary.

        Raises
        ------
        TypeError
            If message is not a dictionary
        """
        if not isinstance(message, dict):
            raise TypeError("Message must be a dictionary")

    def _validate_file_path(self, file_path: str) -> None:
        """Validate file path.

        Raises
        ------
        ValueError
            If file_path is empty or invalid
        """
        if not file_path or not isinstance(file_path, str):
            raise ValueError("Invalid file path")

    def _validate_file_exists(self, file_path: str) -> None:
        """Validate that file exists.

        Raises
        ------
        FileNotFoundError
            If file doesn't exist
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

    def _validate_json_list(self, json_data: Any) -> None:
        """Validate input is a non-empty list of dictionaries.

        Raises
        ------
        TypeError
            If input is not a list of dictionaries
        ValueError
            If list is empty
        """
        if not isinstance(json_data, list):
            raise TypeError("Input must be a list")
        if not json_data:
            raise ValueError("Input list cannot be empty")
        if not all(isinstance(item, dict) for item in json_data):
            raise TypeError("All list items must be dictionaries")

    def dump_message(self, message: dict, json_file: str) -> bool:
        """Dump message and save to specific directory in the network.

        Parameters
        ----------
        message : dict
            Message in dictionary format to be saved
        json_file : str
            Complete filename with .json extension for saving

        Returns
        -------
        bool
            True if file saving was successful, False otherwise

        Raises
        ------
        TypeError
            If message is not a dictionary
        ValueError
            If json_file is not a valid path
        """
        self._validate_message_dict(message)
        self._validate_file_path(json_file)

        with open(json_file, "w") as write_file:
            json.dump(message, write_file)
        
        return os.path.exists(json_file)

    def load_message(
        self,
        json_file: str,
        errors: str = "ignore",
        encoding: Optional[str] = None,
        decoding: Optional[str] = None
    ) -> Union[dict, list]:
        """Load message from JSON file.

        Parameters
        ----------
        json_file : str
            JSON file to load
        errors : str, optional
            How to handle encoding errors (default: "ignore")
        encoding : Optional[str], optional
            File encoding (default: None)
        decoding : Optional[str], optional
            Decoding to apply (default: None)

        Returns
        -------
        Union[dict, list]
            Loaded JSON data

        Raises
        ------
        FileNotFoundError
            If json_file doesn't exist
        ValueError
            If file contains invalid JSON
        """
        self._validate_file_exists(json_file)

        if encoding is None:
            with open(json_file, "r", errors=errors) as read_file:
                return json.load(read_file)
        else:
            with open(json_file, errors=errors, encoding=encoding) as read_file:
                return json.loads(read_file.read().encode(encoding).decode(decoding))

    def loads_message_like(self, message: Union[str, bytes]) -> Union[dict, list]:
        """Load a representation in str from a valid message type.

        Parameters
        ----------
        message : Union[str, bytes]
            Message to convert to JSON

        Returns
        -------
        Union[dict, list]
            Parsed JSON data

        Raises
        ------
        ValueError
            If message is not valid JSON
        """
        return json.loads(message)

    def dict_to_json(self, message: dict) -> str:
        """Convert dictionary to JSON string.

        Parameters
        ----------
        message : dict
            Dictionary to convert

        Returns
        -------
        str
            JSON string representation

        Raises
        ------
        TypeError
            If message is not a dictionary
        """
        self._validate_message_dict(message)
        return json.dumps(message)

    def send_json(self, message: dict) -> dict:
        """Send JSON in memory (e.g., for API responses).

        Parameters
        ----------
        message : dict
            Dictionary to convert

        Returns
        -------
        dict
            JSON-compatible dictionary

        Raises
        ------
        TypeError
            If message is not JSON-serializable
        """
        return json.loads(json.dumps(message))

    def byte_to_json(self, byte_message: bytes) -> dict:
        """Decrypt byte format to JSON.

        Parameters
        ----------
        byte_message : bytes
            Byte message to convert

        Returns
        -------
        dict
            Decoded JSON message
        """
        jsonify_message = ast.literal_eval(StrHandler().get_between(
            str(byte_message), "b'", "'"))
        return self.send_json(jsonify_message)

    def normalize_json_keys(self, json_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize JSON keys across dictionaries in a list.

        Parameters
        ----------
        json_data : list[dict[str, Any]]
            list of dictionaries to normalize

        Returns
        -------
        list[dict[str, Any]]
            list with all dictionaries containing the same keys

        Raises
        ------
        TypeError
            If input is not a list of dictionaries
        ValueError
            If input list is empty
        """
        self._validate_json_list(json_data)

        list_keys = set()
        for item in json_data:
            list_keys.update(item.keys())

        for item in json_data:
            for key in list_keys:
                if key not in item:
                    item[key] = 0
        return json_data