"""PostgreSQL database operations handler.

This module provides a class for managing PostgreSQL database connections and operations,
including query execution, data insertion, reading, and backup functionality. It ensures
robust error handling and type safety throughout database interactions.
"""

from logging import Logger
import os
import platform
import shutil
import subprocess
from typing import Any, Optional, Union

import pandas as pd
from psycopg import Connection, Cursor, connect
from psycopg.rows import dict_row
from psycopg.sql import SQL, Composable, Identifier

from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.databases.sql.database_abc import ABCDatabase
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.json import JsonFiles


class PostgreSQLDB(ABCDatabase):
    """PostgreSQL database handler with connection management and operations."""

    def __init__(
        self,
        dbname: str,
        user: str,
        password: str,
        host: str,
        port: int,
        str_schema: str = "public",
        logger: Optional[Logger] = None,
        bool_singleton: bool = False,
    ) -> None:
        """Initialize PostgreSQL database connection.
        
        Parameters
        ----------
        dbname : str
            Database name
        user : str
            Database user
        password : str
            Database password
        host : str
            Database host
        port : int
            Database port
        str_schema : str, optional
            Database schema (default: "public")
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
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.str_schema = str_schema
        self.logger = logger
        self.bool_singleton = bool_singleton
        self.dict_db_config = {
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "port": self.port,
        }
        self._validate_db_config()
        try:
            self.conn: Connection = connect(**self.dict_db_config, row_factory=dict_row)
            self.cursor: Cursor = self.conn.cursor()
            self.execute("SELECT 1")
            self.execute(
                SQL("SET search_path TO {}").format(Identifier(self.str_schema))
            )
        except Exception as err:
            error_msg = f"Error connecting to database: {str(err)}"
            CreateLog().log_message(self.logger, error_msg, "error")
            raise ConnectionError(error_msg) from err

    def _validate_db_config(self) -> None:
        """Validate database configuration parameters.

        Raises
        ------
        ValueError
            If dbname is empty
            If user is empty
            If password is empty
            If host is empty
            If port is not a positive integer
            If schema is empty
        """
        if not self.dbname:
            raise ValueError("Database name cannot be empty")
        if not self.user:
            raise ValueError("Database user cannot be empty")
        if not self.password:
            raise ValueError("Database password cannot be empty")
        if not self.host:
            raise ValueError("Database host cannot be empty")
        if self.port <= 0:
            raise ValueError("Database port must be a positive integer")
        if not self.str_schema:
            raise ValueError("Database schema cannot be empty")

    def execute(
        self, 
        str_query: Union[str, Composable]
    ) -> None:
        """Execute a SQL query.

        Parameters
        ----------
        str_query : Union[str, Composable]
            SQL query to execute

        Returns
        -------
        None
        """
        self.cursor.execute(str_query)

    def read(
        self,
        str_query: Union[str, Composable],
        dict_type_cols: Optional[dict[str, Any]] = None,
        list_cols_dt: Optional[list[str]] = None,
        str_fmt_dt: Optional[str] = None,
    ) -> pd.DataFrame:
        """Read data from database into DataFrame.

        Parameters
        ----------
        str_query : Union[str, Composable]
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

        self.cursor.execute(str_query)
        data = self.cursor.fetchall()
        df_ = pd.DataFrame(data)

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
        Exception
            If insertion fails
        """
        if not str_table_name:
            raise ValueError("Table name cannot be empty")
        if not json_data:
            return

        json_data = JsonFiles().normalize_json_keys(json_data)
        columns = json_data[0].keys()
        placeholders = SQL(",").join([SQL("%s") for _ in columns])
        table = Identifier(str_table_name)
        cols = SQL(",").join(map(Identifier, columns))

        if bool_insert_or_ignore:
            query = SQL("""
                INSERT INTO {table} ({cols})
                VALUES ({placeholders})
                ON CONFLICT DO NOTHING
            """).format(table=table, cols=cols, placeholders=placeholders)
        else:
            query = SQL("""
                INSERT INTO {table} ({cols})
                VALUES ({placeholders})
            """).format(table=table, cols=cols, placeholders=placeholders)

        try:
            records = [tuple(record[col] for col in columns) for record in json_data]
            self.cursor.executemany(query, records)
            self.conn.commit()

            CreateLog().log_message(
                self.logger,
                f"Successful commit in db {self.dict_db_config['dbname']} "
                + f"/ table {str_table_name}.",
                "info"
            )
        except Exception as err:
            self.conn.rollback()
            self.close()
            CreateLog().log_message(
                self.logger,
                "Error while inserting data\n"
                + f"DB_CONFIG: {self.dict_db_config}\n"
                + f"TABLE_NAME: {str_table_name}\n"
                + f"JSON_DATA: {json_data}\n"
                + f"ERROR_MESSAGE: {err}",
                "error"
            )
            raise Exception(
                "Error while inserting data\n"
                + f"DB_CONFIG: {self.dict_db_config}\n"
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
            backup_file = os.path.join(str_backup_dir, str_bkp_name)
            env = os.environ.copy()
            env["PGPASSWORD"] = self.password

            command = [
                "pg_dump",
                "-h",
                self.host,
                "-p",
                str(self.port),
                "-U",
                self.user,
                "-F",
                "c",
                "-b",
                "-f",
                backup_file,
                self.dbname,
            ]
            subprocess.run(command, check=True, env=env) # noqa S603: check for execution of untrusted input
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
        if shutil.which("pg_dump"):
            return True

        error_msg = "pg_dump not found. Attempting to install..."
        CreateLog().log_message(self.logger, error_msg, "warning")

        system = platform.system().lower()
        try:
            if system == "linux":
                subprocess.run(["apt-get", "update"], check=True)  # noqa S603: check for execution of untrusted input
                subprocess.run(
                    ["apt-get", "install", "-y", "postgresql-client"], # noqa S607: starting a process with a partial executable path
                    check=True  # noqa S603: check for execution of untrusted input
                )
            elif system == "darwin":
                if shutil.which("brew"):
                    subprocess.run(
                        ["brew", "install", "postgresql"], # noqa S607: starting a process with a partial executable path
                        check=True  # noqa S603: check for execution of untrusted input
                    )
                else:
                    error_msg = (
                        "Homebrew not found. Please install Homebrew and then "
                        "run 'brew install postgresql' to install pg_dump."
                    )
                    CreateLog().log_message(self.logger, error_msg, "error")
                    return False
            elif system == "windows":
                error_msg = (
                    "pg_dump not found. Please install PostgreSQL from "
                    "https://www.postgresql.org/download/windows/ and ensure "
                    "pg_dump is in your system PATH."
                )
                CreateLog().log_message(self.logger, error_msg, "error")
                return False
            else:
                error_msg = (
                    f"Unsupported operating system: {system}. Please install "
                    "pg_dump manually."
                )
                CreateLog().log_message(self.logger, error_msg, "error")
                return False

            if shutil.which("pg_dump"):
                success_msg = "pg_dump successfully installed."
                CreateLog().log_message(self.logger, success_msg, "info")
                return True
            else:
                error_msg = "Failed to install pg_dump."
                CreateLog().log_message(self.logger, error_msg, "error")
                return False

        except subprocess.CalledProcessError as err:
            error_msg = f"Failed to install pg_dump: {err}"
            CreateLog().log_message(self.logger, error_msg, "error")
            return False