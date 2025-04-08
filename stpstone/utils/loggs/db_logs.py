### DATABASE LOGGING ###

# pypi.org libs
import socket
from datetime import datetime

import pandas as pd

# local libs
from stpstone._config.global_slots import YAML_GEN
from stpstone.utils.cals.handling_dates import DatesBR


class DBLogs:

    def audit_log(
        self, df_: pd.DataFrame, str_url: str, dt_db_ref: datetime, bl_ts_log_str: bool = True
    ) -> pd.DataFrame:
        """
        Adds audit columns to the DataFrame for logging.

        Args:
            - df_ (pandas.DataFrame): DataFrame to update.
            - str_url (str): URL to insert into the DataFrame.
            - dt_last_update (str): Timestamp of the last update.

        Returns:
            - pd.DataFrame: DataFrame with audit log information.
        """
        df_[YAML_GEN["audit_log_cols"]["url"]] = str_url
        df_[YAML_GEN["audit_log_cols"]["ref_date"]] = dt_db_ref
        if bl_ts_log_str == True:
            df_[YAML_GEN["audit_log_cols"]["log_timestamp"]] = DatesBR().utc_log_ts.strftime(
                "%Y-%m-%d %H:%M:%S.%f%z")
        else:
            df_[YAML_GEN["audit_log_cols"]["log_timestamp"]] = DatesBR().utc_log_ts
        return df_

    def insert_user_info(self, df_: pd.DataFrame, user_id: str) -> pd.DataFrame:
        """
        Inserts user information into the dataframe for traceability.

        Args:
            df_ (pandas.DataFrame): DataFrame to update.
            user_id (str): ID of the user responsible for the update.

        Returns:
            pd.DataFrame: Updated DataFrame with user information.
        """
        df_[YAML_GEN["audit_log_cols"]["user"]] = user_id
        return df_

    def insert_host_info(self, df_: pd.DataFrame) -> pd.DataFrame:
        """
        Inserts the host name (machine) where the data is being processed into the dataframe.

        Args:
            df_ (pandas.DataFrame): DataFrame to update.

        Returns:
            pd.DataFrame: Updated DataFrame with host information.
        """
        df_[YAML_GEN["audit_log_cols"]["host"]] = socket.gethostname()
        return df_

    def insert_error_info(self, df_: pd.DataFrame, error_msg: str) -> pd.DataFrame:
        """
        Inserts error information into the dataframe to track the issues.

        Args:
            df_ (pandas.DataFrame): DataFrame to update.
            error_msg (str): Error message to insert.

        Returns:
            pd.DataFrame: Updated DataFrame with error information.
        """
        df_[YAML_GEN["audit_log_cols"]["error_msg"]] = error_msg
        return df_

    def log_data_insert(
        self, df_: pd.DataFrame, action_type: str, dt_action: str
    ) -> pd.DataFrame:
        """
        Inserts logging information for data insertions into the dataframe.

        Args:
            df_ (pandas.DataFrame): DataFrame to update.
            action_type (str): Type of action (insert, update, delete).
            dt_action (str): Timestamp of the action.

        Returns:
            pd.DataFrame: Updated DataFrame with logging information.
        """
        df_[YAML_GEN["audit_log_cols"]["action_type"]] = action_type
        df_[YAML_GEN["audit_log_cols"]["action_timestamp"]] = dt_action
        return df_
