"""Module for handling pickle file operations.

This module provides functionality for working with pickle files, including loading, 
saving, and validating pickle files for security and integrity.
"""

import os
from pathlib import Path
import pickle
import pickletools
from typing import Any, Union

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class PickleFiles(metaclass=TypeChecker):
    """Class for handling pickle file operations."""

    def _validate_file_path(self, file_path: Union[str, Path]) -> None:
        """Validate file path for pickle operations.

        Parameters
        ----------
        file_path : Union[str, Path]
            Path to validate

        Raises
        ------
        ValueError
            If file path is empty
            If file path does not end with .pickle
        """
        file_path_str = str(file_path)
        if not file_path_str:
            raise ValueError("File path cannot be empty")
        if not (file_path_str.endswith(".pickle") or file_path_str.endswith(".pkl")):
            raise ValueError("File path must end with .pickle extension")

    def is_safe_pickle(self, file_path: Union[str, Path]) -> bool:
        """Check if a pickle file contains only safe opcodes.

        Parameters
        ----------
        file_path : Union[str, Path]
            Path to the pickle file to analyze

        Returns
        -------
        bool
            True if the pickle file contains only safe opcodes, False otherwise

        Notes
        -----
        The pickle module is inherently insecure, as it allows arbitrary code execution 
        during deserialization, posing a security risk if the pickled file comes from an 
        untrusted source.

        This method analyzes the pickle file's opcodes using pickletools to detect
        potentially dangerous operations that could allow arbitrary code execution.
        Unsafe opcodes include GLOBAL, REDUCE, INST, OBJ, BUILD, and NEWOBJ, which
        can invoke arbitrary Python functions or instantiate objects.

        Opcodes (short for "operation codes") are compact, machine-readable instructions that 
        the pickle modules uses to represent operations like creating objects, setting attributes, 
        or calling functions during serialization and deserialization. Each opcode corresponds to 
        a specific action, such as storing a string, building a dictionary, or importing a module.

        Unsafe opcodes tracked:
        - GLOBAL: can import and call arbitrary Python functions
        - REDUCE: can call arbitrary callable objects
        - INST: can instantiate arbitrary classes
        - OBJ: similar to INST, can instantiate objects
        - BUILD: can modify object state in potentially unsafe ways
        - NEWOBJ: can create new instances of arbitrary classes

        References
        ----------
        .. [1] https://docs.python.org/3/library/pickletools.html
        """
        dict_unsafe_opcodes = {
            'GLOBAL',
            'REDUCE',
            'INST',
            'OBJ',
            'BUILD',
            'NEWOBJ',
        }

        try:
            with open(file_path, 'rb') as f:
                for opcode, _, _ in pickletools.genops(f):
                    if opcode.name in dict_unsafe_opcodes:
                        return False
            return True
        except Exception:
            return False

    def dump_message(
        self,
        message: dict[str, Any],
        file_path: Union[str, Path],
        pickle_protocol: int = pickle.HIGHEST_PROTOCOL,
        pickle_protocol_error: int = -1
    ) -> bool:
        """Serialize and save a message to a pickle file.

        Parameters
        ----------
        message : dict[str, Any]
            Dictionary message to serialize
        file_path : Union[str, Path]
            Complete file path with .pickle extension
        pickle_protocol : int
            Preferred pickle protocol (default: HIGHEST_PROTOCOL)
        pickle_protocol_error : int
            Fallback pickle protocol if preferred fails (default: -1)

        Returns
        -------
        bool
            True if file was successfully saved, False otherwise

        Raises
        ------
        ValueError
            If file path is invalid

        References
        ----------
        .. [1] https://stackoverflow.com/questions/11218477/how-can-i-use-pickle-to-save-a-dict-or-any-other-python-object
        """
        self._validate_file_path(file_path)

        try:
            with open(file_path, 'wb') as write_file:
                try:
                    pickle.dump(message, write_file, protocol=pickle_protocol)
                except Exception:
                    pickle.dump(message, write_file, protocol=pickle_protocol_error)
        except Exception as err:
            raise ValueError(f"Failed to write pickle file: {str(err)}") from err

        return os.path.exists(file_path)

    def load_message(
        self,
        file_path: Union[str, Path],
        bool_trusted_file: bool = False,
    ) -> Any: # noqa ANN401: typing.Any is not allowed
        """Load and deserialize a message from a pickle file.

        Parameters
        ----------
        file_path : Union[str, Path]
            Complete file path with .pickle extension
        bool_trusted_file : bool
            Whether the pickle file is trusted (default: False)

        Returns
        -------
        Any
            Deserialized Python object from pickle file

        Raises
        ------
        ValueError
            If file does not exist
            If file contains unsafe pickle opcodes
            If decoding is not specified when encoding is provided
            If failed to load pickle file

        References
        ----------
        .. [1] https://stackoverflow.com/questions/11218477/how-can-i-use-pickle-to-save-a-dict-or-any-other-python-object
        """
        self._validate_file_path(file_path)

        if not os.path.exists(file_path):
            raise ValueError(f"File not found: {file_path}")

        if not self.is_safe_pickle(file_path) and not bool_trusted_file:
            raise ValueError("Pickle file contains potentially unsafe data")

        try:
            with open(file_path, 'rb') as read_file:
                return pickle.load(read_file) # noqa S301: potentially unsafe when deserializing untrusted data, mitigated by is_safe_pickle method
        except Exception as err:
            raise ValueError(f"Failed to load pickle file: {str(err)}") from err