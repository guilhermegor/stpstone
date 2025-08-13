"""OneDrive synchronization status monitoring utilities.

This module provides functionality to check the synchronization status of Microsoft OneDrive
by examining log files in the local machine's Business1 logs directory.
"""

import platform


if platform.system() != "Windows":
    raise OSError("This module requires a Windows operating system to function properly.")

from datetime import datetime
from getpass import getuser
from typing import Union

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.folders import DirFilesManagement


class OneDrive(metaclass=TypeChecker):
    """Class for monitoring OneDrive synchronization status."""

    def _validate_dir_path(self, dir_path: str) -> None:
        """Validate the directory path format.

        Parameters
        ----------
        dir_path : str
            Directory path to validate

        Raises
        ------
        ValueError
            If directory path is empty
            If directory path does not contain '{}' for user formatting
        """
        if not dir_path:
            raise ValueError("Directory path cannot be empty")
        if "{}" not in dir_path:
            raise ValueError("Directory path must contain '{}' for user formatting")

    def check_sync_status(
        self,
        dir_path_business: str = \
            'C:\\Users\\{}\\AppData\\Local\\Microsoft\\OneDrive\\logs\\Business1\\',
        name_like: str = 'SyncEngine-*',
        bool_to_datetime: bool = True
    ) -> Union[datetime, bool]:
        """Check whether OneDrive sync service is alive by examining log files.

        Parameters
        ----------
        dir_path_business : str
            Path template to OneDrive logs directory (default: standard Business1 path)
        name_like : str
            Pattern to match sync log files (default: 'SyncEngine-*')
        bool_to_datetime : bool
            Flag to determine return type (datetime if True, bool if False)

        Returns
        -------
        Union[datetime, bool]
            Last edition time of the most recent sync log file, either as datetime object or 
            boolean

        Raises
        ------
        ValueError
            If directory path is invalid
            If no matching log files are found
        """
        self._validate_dir_path(dir_path_business)
        formatted_dir_path = dir_path_business.format(getuser())

        complete_status_file_path = DirFilesManagement().choose_last_saved_file_w_rule(
            formatted_dir_path, name_like
        )
        if not complete_status_file_path:
            raise ValueError("No matching log files found in directory")

        return DirFilesManagement().time_last_edition(
            complete_status_file_path,
            bool_to_datetime=bool_to_datetime
        )