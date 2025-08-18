"""Module for handling TGZ file download and extraction operations.

This module provides a class for handling TGZ file download and extraction operations.
It includes validation, directory creation, file download, local file handling, and TGZ extraction.
"""

import os
import tarfile
from typing import Optional, TypedDict

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.folders import DirFilesManagement


class ReturnFetchTgzFiles(TypedDict, metaclass=TypeChecker):
    """Return type for fetch_tgz_files method.

    Attributes
    ----------
    blame_download_tgz_file : str
        Download status or 'n/a' if using local file
    dir_path : str
        Directory path where files were extracted
    extracted_files_names : list[str]
        Names of extracted files from the TGZ archive
    """

    blame_download_tgz_file: str
    dir_path: str
    extracted_files_names: list[str]


class HandlingTGZFiles(metaclass=TypeChecker):
    """Class for handling TGZ file download and extraction operations."""

    def _validate_input_paths(
        self,
        dir_exporting_path: str,
        url_source: Optional[str] = None,
        complete_source_path: Optional[str] = None
    ) -> None:
        """Validate input paths for TGZ file operations.

        Parameters
        ----------
        dir_exporting_path : str
            Directory path to export files
        url_source : Optional[str]
            URL source for downloading TGZ file
        complete_source_path : Optional[str]
            Complete local path to existing TGZ file

        Raises
        ------
        ValueError
            If neither url_source nor complete_source_path is provided
            If dir_exporting_path is empty
        """
        if not dir_exporting_path:
            raise ValueError("Directory exporting path cannot be empty")
        if all([x is None for x in [url_source, complete_source_path]]):
            raise ValueError(
                "Either url_source or complete_source_path must be provided"
            )

    def _is_safe_path(self, basedir: str, path: str) -> bool:
        """Check if a path is safe to extract (prevents path traversal).

        Parameters
        ----------
        basedir : str
            The base directory where extraction is allowed
        path : str
            The path to check

        Returns
        -------
        bool
            True if the path is safe, False otherwise
        """
        # convert to absolute paths and normalize to remove '..' or similar
        abs_basedir = os.path.abspath(basedir)
        abs_path = os.path.abspath(os.path.join(basedir, path))
        # check if the path is within the base directory
        return abs_path.startswith(abs_basedir)

    def fetch_tgz_files(
        self,
        dir_exporting_path: str,
        tgz_exporting_name: str,
        url_source: Optional[str] = None,
        complete_source_path: Optional[str] = None
    ) -> ReturnFetchTgzFiles:
        """Extract files from TGZ compression.

        Downloads from URL or uses local file, then extracts contents to specified directory.

        Parameters
        ----------
        dir_exporting_path : str
            Directory path to export files
        tgz_exporting_name : str
            Name for the TGZ file
        url_source : Optional[str]
            URL to download TGZ file from (optional)
        complete_source_path : Optional[str]
            Complete local path to existing TGZ file (optional)

        Returns
        -------
        ReturnFetchTgzFiles
            Dictionary containing download status, directory path, and extracted filenames

        Raises
        ------
        ValueError
            If input validation fails or source file not found
            If TGZ file operations fail or path traversal detected

        References
        ----------
        .. [1] "Hands-On Machine Learning with Scikit-Learn, Keras, and TensorFlow,
            2nd Edition, by Aurélien Géron (O'Reilly). Copyright 2019 Kiwisoft S.A.S.,
            978-1-492-03264-9."
        """
        self._validate_input_paths(dir_exporting_path, url_source, complete_source_path)

        if not DirFilesManagement().object_exists(dir_exporting_path):
            DirFilesManagement().mk_new_directory(dir_exporting_path)

        tgz_path = os.path.join(dir_exporting_path, tgz_exporting_name)

        try:
            if url_source is not None:
                blame_download_tgz_file = DirFilesManagement().download_web_file(
                    url_source, tgz_path
                )
            else:
                blame_download_tgz_file = "n/a"
                if not os.path.exists(complete_source_path):
                    raise ValueError(f"Source file not found: {complete_source_path}")
                tgz_path = complete_source_path

            extracted_files_names = []
            with tarfile.open(tgz_path) as tgz_file:
                for member in tgz_file.getmembers():
                    if not self._is_safe_path(dir_exporting_path, member.name):
                        raise ValueError(f"Unsafe path detected in tarball: {member.name}")
                    tgz_file.extract(member, path=dir_exporting_path)
                    extracted_files_names.append(member.name)

            return {
                "blame_download_tgz_file": blame_download_tgz_file,
                "dir_path": dir_exporting_path,
                "extracted_files_names": extracted_files_names
            }
        except Exception as err:
            raise ValueError(f"Failed to process TGZ file: {str(err)}") from err