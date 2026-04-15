"""JSON file handling utilities.

This module provides functionality for working with JSON files, including loading, saving,
and converting between different formats. It handles various JSON operations with robust
error handling and type validation.
"""

import json
import os
from typing import Any, Optional, Union

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class JsonFiles(metaclass=TypeChecker):
    """Handles JSON file operations including loading, saving, and format conversion."""

    def _validate_message_dict(
        self, 
        message: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Validate that input is a dictionary.

        Parameters
        ----------
        message : Any
            Input message to validate

        Returns
        -------
        None

        Raises
        ------
        TypeError
            If message is not a dictionary
        """
        if not isinstance(message, dict):
            raise TypeError("Message must be a dictionary")

    def _validate_file_path(self, file_path: str) -> None:
        """Validate file path.

        Parameters
        ----------
        file_path : str
            Path to the file to validate

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If file_path is empty or invalid
        """
        if not file_path or not isinstance(file_path, str):
            raise ValueError("Invalid file path")

    def _validate_file_exists(self, file_path: str) -> None:
        """Validate that file exists.

        Parameters
        ----------
        file_path : str
            Path to the file to check

        Returns
        -------
        None

        Raises
        ------
        FileNotFoundError
            If file doesn't exist
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

    def _validate_json_list(
        self, 
        json_data: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Validate input is a non-empty list of dictionaries.

        Parameters
        ----------
        json_data : Any
            Input data to validate

        Returns
        -------
        None

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
        errors : str
            How to handle decode errors (default: "ignore")
        encoding : Optional[str]
            File encoding (default: None)
        decoding : Optional[str]
            Decoding to apply (default: None)

        Returns
        -------
        Union[dict, list]
            Loaded JSON data
        """
        self._validate_file_exists(json_file)

        if encoding is None:
            with open(json_file, errors=errors) as read_file:
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

        Raises
        ------
        ValueError
            If byte message cannot be decoded or parsed as JSON
        """
        try:
            str_message = byte_message.decode('utf-8')
            if str_message.startswith("b'") and str_message.endswith("'"):
                str_message = str_message[2:-1]
            json_data = json.loads(str_message)
            return self.send_json(json_data)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise ValueError(f"Failed to convert byte message to JSON: {str(e)}") from e

    def normalize_json_keys(
        self, 
        json_data: list[dict[str, Any]] # noqa ANN401: typing.Any is not allowed
    ) -> list[dict[str, Any]]: # noqa ANN401: typing.Any is not allowed
        """Normalize JSON keys across dictionaries in a list.

        Parameters
        ----------
        json_data : list[dict[str, Any]]
            list of dictionaries to normalize

        Returns
        -------
        list[dict[str, Any]]
            list with all dictionaries containing the same keys
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