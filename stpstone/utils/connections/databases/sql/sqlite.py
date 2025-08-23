"""SQLite database operations handler.

This module provides a class for managing SQLite database connections and operations,
including query execution, data insertion, reading, and backup functionality. It ensures
robust error handling and type safety throughout database interactions.
"""

from logging import Logger
import os
import platform
import shutil
import sqlite3
import subprocess
from typing import Any, Optional, Union

import pandas as pd

from stpstone.transformations.validation.metaclass_type_checker import SQLComposable
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.databases.sql.database_abc import ABCDatabase
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.json import JsonFiles


class SQLiteDB(ABCDatabase):
    """SQLite database handler with connection management and operations."""

    def __init__(
        self,
        db_path: str,
        logger: Optional[Logger] = None,
        bool_singleton: bool = False,
    ) -> None:
        """Initialize SQLite database connection.
        
        Parameters
        ----------
        db_path : str
            Path to SQLite database file
        logger : Optional[Logger], optional
            Logger instance (default: None)
        bool_singleton : bool, optional
            Whether to use a singleton connection (default: False)
        
        Returns
        -------
        None

        Raises
        ------
        ConnectionError
            If database connection fails
        """
        self.db_path = db_path
        self.logger = logger
        self.bool_singleton = bool_singleton
        self._validate_db_path()
        try:
            self.conn: sqlite3.Connection = sqlite3.connect(self.db_path)
            self.cursor: sqlite3.Cursor = self.conn.cursor()
            self.execute("SELECT 1")
        except Exception as err:
            error_msg = f"Error connecting to database: {str(err)}"
            CreateLog().log_message(self.logger, error_msg, "error")
            raise ConnectionError(error_msg) from err

    def _validate_db_path(self) -> None:
        """Validate database path parameter.

        Raises
        ------
        ValueError
            If db_path is empty
            If db_path is not a string
        """
        if not self.db_path:
            raise ValueError("Database path cannot be empty")
        if not isinstance(self.db_path, str):
            raise ValueError("Database path must be a string")

    def execute(
        self, 
        str_query: Union[str, SQLComposable]
    ) -> None:
        """Execute a SQL query.

        Parameters
        ----------
        str_query : Union[str, SQLComposable]
            SQL query to execute

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If query is empty
        """
        if not str_query:
            raise ValueError("Query cannot be empty")
        self.cursor.execute(str_query)

    def read(
        self,
        str_query: Union[str, SQLComposable],
        dict_type_cols: Optional[dict[str, Any]] = None,
        list_cols_dt: Optional[list[str]] = None,
        str_fmt_dt: Optional[str] = None,
    ) -> pd.DataFrame:
        """Read data from database into DataFrame.

        Parameters
        ----------
        str_query : Union[str, SQLComposable]
            SQL query to execute
        dict_type_cols : Optional[dict[str, Any]], optional
            Dictionary for column type conversion (default: None)
        list_cols_dt : Optional[list[str]], optional
            List of date columns to convert (default: None)
        str_fmt_dt : Optional[str], optional
            Date format string (default: None)

        Returns
        -------
        pd.DataFrame
            DataFrame containing query results

        Raises
        ------
        ValueError
            If query is empty or invalid parameters provided
        """
        if not str_query:
            raise ValueError("Query cannot be empty")
        if (list_cols_dt is not None and str_fmt_dt is None) \
            or (list_cols_dt is None and str_fmt_dt is not None):
            raise ValueError("Both list_cols_dt and str_fmt_dt must be provided or None")

        df_ = pd.read_sql_query(str_query, self.conn)
        
        if not df_.empty:
            if dict_type_cols is not None:
                df_ = df_.astype(dict_type_cols)

            if list_cols_dt is not None and str_fmt_dt is not None:
                for col_ in list_cols_dt:
                    df_[col_] = [
                        DatesBR().str_date_to_datetime(d, str_fmt_dt) for d in df_[col_]
                    ]

        return df_

    def insert(
        self,
        json_data: list[dict[str, Any]],
        str_table_name: str,
        bool_insert_or_ignore: bool = False,
    ) -> None:
        """Insert data into database table.

        Parameters
        ----------
        json_data : list[dict[str, Any]]
            List of dictionaries containing data to insert
        str_table_name : str
            Target table name
        bool_insert_or_ignore : bool, optional
            Whether to use INSERT OR IGNORE (default: False)

        Raises
        ------
        ValueError
            If table name is empty or json_data is empty
            If insertion fails
        """
        if not str_table_name:
            raise ValueError("Table name cannot be empty")
        if not json_data:
            return

        json_data = JsonFiles().normalize_json_keys(json_data)
        columns = json_data[0].keys()
        placeholders = ", ".join(["?" for _ in columns])
        columns_str = ", ".join(columns)

        if bool_insert_or_ignore:
            query = \
                f"INSERT OR IGNORE INTO {str_table_name} ({columns_str}) VALUES ({placeholders})" # noqa S608: possible sql injection
        else:
            query = f"INSERT INTO {str_table_name} ({columns_str}) VALUES ({placeholders})" # noqa S608: possible sql injection

        try:
            records = [tuple(record[col] for col in columns) for record in json_data]
            self.cursor.executemany(query, records)
            self.conn.commit()

            CreateLog().log_message(
                self.logger,
                f"Successful commit in db {self.db_path} "
                + f"/ table {str_table_name}.",
                "info"
            )
        except Exception as err:
            self.conn.rollback()
            self.close()
            CreateLog().log_message(
                self.logger,
                "Error while inserting data\n"
                + f"DB_PATH: {self.db_path}\n"
                + f"TABLE_NAME: {str_table_name}\n"
                + f"JSON_DATA: {json_data}\n"
                + f"ERROR_MESSAGE: {err}",
                "error"
            )
            raise ValueError(
                "Error while inserting data\n"
                + f"DB_PATH: {self.db_path}\n"
                + f"TABLE_NAME: {str_table_name}\n"
                + f"JSON_DATA: {json_data}\n"
                + f"ERROR_MESSAGE: {err}"
            ) from err

    def close(self) -> None:
        """Close database connection.
        
        Returns
        -------
        None
        """
        self.conn.close()

    def backup(
        self, 
        str_backup_dir: str, 
        str_bkp_name: Optional[str] = None
    ) -> str:
        """Create database backup.

        Parameters
        ----------
        str_backup_dir : str
            Backup directory path
        str_bkp_name : Optional[str], optional
            Backup file name (default: None)

        Returns
        -------
        str
            Backup status message

        Raises
        ------
        ValueError
            If backup directory is empty
        RuntimeError
            If backup tool is not available
        """
        if not str_backup_dir:
            raise ValueError("Backup directory cannot be empty")

        if not self.check_bkp_tool():
            error_msg = "Backup tool is required for backups, but it is not available and " \
            "could not be installed automatically"
            CreateLog().log_message(self.logger, error_msg, "error")
            raise RuntimeError(error_msg)

        try:
            os.makedirs(str_backup_dir, exist_ok=True)
            backup_file = os.path.join(str_backup_dir, str_bkp_name \
                                       or f"{os.path.basename(self.db_path)}.backup")
            
            command = ["sqlite3", self.db_path, f".backup '{backup_file}'"]
            subprocess.run(command, check=True, capture_output=True, text=True)  # noqa S603: check for execution of untrusted input
            
            return f"Backup successful! File saved at: {backup_file}"
        except subprocess.CalledProcessError as err:
            return f"Backup failed: {err}"
        except Exception as err:
            return f"An error occurred: {err}"
        
    def check_bkp_tool(self) -> bool:
        """Check if backup tool is available.

        Returns
        -------
        bool
            True if backup tool is available, False otherwise
        """
        if shutil.which("sqlite3"):
            return True

        error_msg = "sqlite3 not found. Attempting to install..."
        CreateLog().log_message(self.logger, error_msg, "warning")

        system = platform.system().lower()
        try:
            if system == "linux":
                subprocess.run(["apt-get", "update"], check=True)  # noqa S603: check for execution of untrusted input
                subprocess.run(
                    ["apt-get", "install", "-y", "sqlite3"],  # noqa S607: starting a process with a partial executable path
                    check=True  # noqa S603: check for execution of untrusted input
                )
            elif system == "darwin":
                if shutil.which("brew"):
                    subprocess.run(
                        ["brew", "install", "sqlite"],  # noqa S607: starting a process with a partial executable path
                        check=True  # noqa S603: check for execution of untrusted input
                    )
                else:
                    error_msg = (
                        "Homebrew not found. Please install Homebrew and then "
                        "run 'brew install sqlite' to install sqlite3."
                    )
                    CreateLog().log_message(self.logger, error_msg, "error")
                    return False
            elif system == "windows":
                error_msg = (
                    "sqlite3 not found. Please install SQLite from "
                    "https://sqlite.org/download.html and ensure "
                    "sqlite3 is in your system PATH."
                )
                CreateLog().log_message(self.logger, error_msg, "error")
                return False
            else:
                error_msg = (
                    f"Unsupported operating system: {system}. Please install "
                    "sqlite3 manually."
                )
                CreateLog().log_message(self.logger, error_msg, "error")
                return False

            if shutil.which("sqlite3"):
                success_msg = "sqlite3 successfully installed."
                CreateLog().log_message(self.logger, success_msg, "info")
                return True
            else:
                error_msg = "Failed to install sqlite3."
                CreateLog().log_message(self.logger, error_msg, "error")
                return False

        except subprocess.CalledProcessError as err:
            error_msg = f"Failed to install sqlite3: {err}"
            CreateLog().log_message(self.logger, error_msg, "error")
            return False