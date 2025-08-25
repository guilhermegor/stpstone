"""Database logging utilities for audit tracking.

This module provides a class for adding audit-related information to pandas DataFrames,
including URL, timestamp, user, host, error messages, and action logging. It ensures
robust validation and type safety for all operations.
"""

from datetime import datetime
import socket
from typing import Literal

import pandas as pd

from stpstone._config.global_slots import YAML_GEN
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.cals.cal_abc import DatesBR


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

    def _validate_string(self, value: str, name: str) -> None:
        """Validate string input.

        Parameters
        ----------
        value : str
            String to validate
        name : str
            Name of the variable for error messages

        Raises
        ------
        ValueError
            If string is empty
        """
        if not value:
            raise ValueError(f"{name} cannot be empty")

    def audit_log(
        self,
        df_: pd.DataFrame,
        url: str,
        dt_db_ref: datetime,
        ts_log_str: bool = True,
    ) -> pd.DataFrame:
        """Add audit columns to the DataFrame for logging.

        Parameters
        ----------
        df_ : pd.DataFrame
            DataFrame to update
        url : str
            URL to insert into the DataFrame
        dt_db_ref : datetime
            Timestamp of the database reference
        ts_log_str : bool, optional
            Whether to format timestamp as string (default: True)

        Returns
        -------
        pd.DataFrame
            Dictionary containing the updated DataFrame
        """
        self._validate_dataframe(df_)
        self._validate_string(url, "url")
        
        df_[YAML_GEN["audit_log_cols"]["url"]] = url
        df_[YAML_GEN["audit_log_cols"]["ref_date"]] = dt_db_ref
        log_ts = DatesBR().utc_log_ts()
        df_[YAML_GEN["audit_log_cols"]["log_timestamp"]] = (
            log_ts.strftime("%Y-%m-%d %H:%M:%S.%f%z") if ts_log_str else log_ts
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
            Dictionary containing the updated DataFrame
        """
        self._validate_dataframe(df_)
        self._validate_string(user_id, "user_id")

        df_[YAML_GEN["audit_log_cols"]["user"]] = user_id
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
            Dictionary containing the updated DataFrame
        """
        self._validate_dataframe(df_)
        
        df_[YAML_GEN["audit_log_cols"]["host"]] = socket.gethostname()
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
            Dictionary containing the updated DataFrame
        """
        self._validate_dataframe(df_)
        self._validate_string(error_msg, "error_msg")
        
        df_[YAML_GEN["audit_log_cols"]["error_msg"]] = error_msg
        return df_

    def log_data_insert(
        self, 
        df_: pd.DataFrame, 
        action_type: Literal["insert", "update", "delete"], 
        dt_action: datetime
    ) -> pd.DataFrame:
        """Insert logging information for data actions into the DataFrame.

        Parameters
        ----------
        df_ : pd.DataFrame
            DataFrame to update
        action_type : Literal['insert', 'update', 'delete']
            Type of action
        dt_action : datetime
            Timestamp of the action

        Returns
        -------
        pd.DataFrame
            Dictionary containing the updated DataFrame
        """
        self._validate_dataframe(df_)
        self._validate_string(action_type, "action_type")
        
        df_[YAML_GEN["audit_log_cols"]["action_type"]] = action_type
        df_[YAML_GEN["audit_log_cols"]["action_timestamp"]] = dt_action
        return df_