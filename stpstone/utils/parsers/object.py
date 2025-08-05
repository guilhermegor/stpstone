"""Utilities for handling objects of various datatypes.

This module provides a class for parsing and converting objects to their inherent Python types
using ast.literal_eval for safe evaluation, with optional string boundary extraction.
"""

import ast
from typing import Any, Optional

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.str import StrHandler


class HandlingObjects(metaclass=TypeChecker):
    """Class for handling and converting objects to their inherent types."""

    def _validate_bounds(
        self, 
        str_left_bound: Optional[str], 
        str_right_bound: Optional[str]
    ) -> None:
        """Validate string boundary parameters.

        Parameters
        ----------
        str_left_bound : Optional[str]
            Left boundary string for extraction
        str_right_bound : Optional[str]
            Right boundary string for extraction

        Raises
        ------
        ValueError
            If only one boundary is provided (both must be None or both provided)
        """
        if (str_left_bound is None) != (str_right_bound is None):
            raise ValueError(
                "Both str_left_bound and str_right_bound must be provided or both None")

    def _validate_data_object(
        self, 
        data_object: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Validate the input data object.

        Parameters
        ----------
        data_object : Any
            Object to validate

        Raises
        ------
        ValueError
            If data_object is None
            If data_object is an empty string
        """
        if data_object is None:
            raise ValueError("Data object cannot be None")
        if isinstance(data_object, str) and not data_object.strip():
            raise ValueError("Data object cannot be an empty string")

    def literal_eval_data(
        self,
        data_object: Any, # noqa ANN401: typing.Any is not allowed
        str_left_bound: Optional[str] = None,
        str_right_bound: Optional[str] = None
    ) -> Any: # noqa ANN401: typing.Any is not allowed
        """Convert an object to its inherent Python type using safe evaluation.

        Parameters
        ----------
        data_object : Any
            Object to be converted (typically a string representation)
        str_left_bound : Optional[str], optional
            Left boundary string for extraction (default: None)
        str_right_bound : Optional[str], optional
            Right boundary string for extraction (default: None)

        Returns
        -------
        Any
            Converted Python object (e.g., list, dict, int)

        Raises
        ------
        SyntaxError
            If ast.literal_eval fails to parse the input

        Notes
        -----
        Uses ast.literal_eval for safe evaluation of strings to Python objects.
        If boundaries are provided, extracts substring before evaluation.

        References
        ----------
        .. [1] https://docs.python.org/3/library/ast.html#ast.literal_eval
        """
        self._validate_data_object(data_object)
        self._validate_bounds(str_left_bound, str_right_bound)

        try:
            if str_left_bound is None and str_right_bound is None:
                return ast.literal_eval(data_object)
            return ast.literal_eval(
                StrHandler().get_between(str(data_object), str_left_bound, str_right_bound)
            )
        except Exception as err:
            raise SyntaxError(f"Failed to evaluate data object: {str(err)}") from err