"""Database logging utilities for audit tracking.

This module provides a class for adding audit-related information to pandas DataFrames,
including URL, timestamp, user, host, error messages, and action logging. It ensures
robust validation and type safety for all operations.
"""

from datetime import date
import socket
from typing import Literal, Optional

import pandas as pd

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.calendars.calendar_br import DatesBRAnbima


class DBLogs(metaclass=TypeChecker):
    """Class for handling database audit logging operations."""

    def _validate_dataframe(self, df_: pd.DataFrame) -> None:
        """Validate DataFrame input.

        Parameters
        ----------
        df_ : pd.DataFrame
            DataFrame to validate

        Raises
        ------
        ValueError
            If DataFrame is empty or None
        """
        if df_.empty:
            raise ValueError("DataFrame cannot be empty")

    def _validate_string(self, value: Optional[str], name: str) -> None:
        """Validate string input.

        Parameters
        ----------
        value : Optional[str]
            String to validate
        name : str
            Name of the variable for error messages

        Raises
        ------
        ValueError
            If string is empty
        """
        if not value or len(value) == 0:
            raise ValueError(f"{name} cannot be empty")

    def audit_log(
        self,
        df_: pd.DataFrame,
        url: Optional[str],
        dt_db_ref: date,
        bool_format_log_as_str: bool = True,
    ) -> pd.DataFrame:
        """Add audit columns to the DataFrame for logging.

        Parameters
        ----------
        df_ : pd.DataFrame
            DataFrame to update
        url : Optional[str]
            URL to insert into the DataFrame
        dt_db_ref : date
            Date of the database reference
        bool_format_log_as_str : bool, optional
            Whether to format timestamp as string (default: True)

        Returns
        -------
        pd.DataFrame
            Updated DataFrame with audit columns
        """
        self._validate_dataframe(df_)
        if "URL" not in df_.columns:
            self._validate_string(url, "url")
            df_["URL"] = url
        df_["REF_DATE"] = dt_db_ref
        log_ts = DatesBRAnbima().utc_log_ts()
        df_["LOG_TIMESTAMP"] = (
            log_ts.strftime("%Y-%m-%d") if bool_format_log_as_str else log_ts
        )
        return df_

    def insert_user_info(self, df_: pd.DataFrame, user_id: str) -> pd.DataFrame:
        """Insert user information into the DataFrame for traceability.

        Parameters
        ----------
        df_ : pd.DataFrame
            DataFrame to update
        user_id : str
            ID of the user responsible for the update

        Returns
        -------
        pd.DataFrame
            Updated DataFrame with user information
        """
        self._validate_dataframe(df_)
        self._validate_string(user_id, "user_id")

        df_["USER"] = user_id
        return df_

    def insert_host_info(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Insert host name (machine) where data is processed into the DataFrame.

        Parameters
        ----------
        df_ : pd.DataFrame
            DataFrame to update

        Returns
        -------
        pd.DataFrame
            Updated DataFrame with host information
        """
        self._validate_dataframe(df_)
        
        df_["HOST"] = socket.gethostname()
        return df_

    def insert_error_info(self, df_: pd.DataFrame, error_msg: str) -> pd.DataFrame:
        """Insert error information into the DataFrame to track issues.

        Parameters
        ----------
        df_ : pd.DataFrame
            DataFrame to update
        error_msg : str
            Error message to insert

        Returns
        -------
        pd.DataFrame
            Updated DataFrame with error information
        """
        self._validate_dataframe(df_)
        self._validate_string(error_msg, "error_msg")
        
        df_["ERROR_MSG"] = error_msg
        return df_

    def log_data_insert(
        self, 
        df_: pd.DataFrame, 
        action_type: Literal["insert", "update", "delete"], 
        dt_action: date
    ) -> pd.DataFrame:
        """Insert logging information for data actions into the DataFrame.

        Parameters
        ----------
        df_ : pd.DataFrame
            DataFrame to update
        action_type : Literal['insert', 'update', 'delete']
            Type of action
        dt_action : date
            Date of the action

        Returns
        -------
        pd.DataFrame
            Updated DataFrame with action logging information
        """
        self._validate_dataframe(df_)
        self._validate_string(action_type, "action_type")
        
        df_["ACTION_TYPE"] = action_type
        df_["ACTION_TIMESTAMP"] = dt_action
        return df_