"""MySQL database operations handler.

This module provides a class for managing MySQL database connections and operations,
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
import pymysql
from sqlalchemy import create_engine

from stpstone.transformations.validation.metaclass_type_checker import SQLComposable
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.databases.sql.database_abc import ABCDatabase
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.json import JsonFiles


class MySQLDatabase(ABCDatabase):
    """MySQL database handler with connection management and operations."""

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
        """Initialize MySQL database connection.

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
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.dbname,
        }
        self._validate_db_config()
        try:
            self.conn = pymysql.connect(**self.dict_db_config)
            self.cursor = self.conn.cursor()
            self.execute("SELECT 1")
            if self.str_schema != "public":
                self.execute(f"USE {self.str_schema}")
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
        """
        self.cursor.execute(str_query)

    def read_sql(
        self,
        str_query: Union[str, SQLComposable],
        timeout: int = 7200,
    ) -> pd.DataFrame:
        """Read data from MySQL database using SQLAlchemy engine.

        Parameters
        ----------
        str_query : Union[str, SQLComposable]
            SQL query to execute
        timeout : int, optional
            Connection timeout in seconds (default: 7200)

        Returns
        -------
        pd.DataFrame
            DataFrame containing query results

        Raises
        ------
        ValueError
            If query is empty or execution fails
        """
        if not str_query:
            raise ValueError("Query cannot be empty")

        try:
            conn_engine = create_engine(
                f"mysql+pymysql://{self.user}:{self.password}@{self.host}:"
                f"{self.port}/{self.dbname}",
                connect_args={"connect_timeout": timeout}
            )
            return pd.read_sql(str_query, con=conn_engine)
        except Exception as err:
            raise ValueError(f"Failed to read SQL query: {str(err)}") from err

    def read(
        self,
        str_query: str,
        dict_type_cols: Optional[dict[str, Any]] = None,
        list_cols_dt: Optional[list[str]] = None,
        str_fmt_dt: Optional[str] = None,
    ) -> pd.DataFrame:
        """Read data from database into DataFrame.

        Parameters
        ----------
        str_query : str
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
        
        if self.cursor.description:
            columns = [desc[0] for desc in self.cursor.description]
        else:
            columns = [f"col_{i}" for i in range(len(data[0]))] if data and len(data) > 0 else []
        df_ = pd.DataFrame(data, columns=columns)

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
            Whether to use INSERT IGNORE (default: False)

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
        columns = list(json_data[0].keys())

        insert_type = "INSERT IGNORE INTO" if bool_insert_or_ignore else "INSERT INTO"
        placeholders = ", ".join(["%s" for _ in columns])
        column_names = ", ".join(columns)

        query = f"{insert_type} {str_table_name} ({column_names}) VALUES ({placeholders})"

        try:
            records = [tuple(record[col] for col in columns) for record in json_data]
            self.cursor.executemany(query, records)
            self.conn.commit()

            CreateLog().log_message(
                self.logger,
                f"Successful commit in db {self.dict_db_config['database']} "
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
        if hasattr(self, "cursor") and self.cursor is not None:
            self.cursor.close()
        if hasattr(self, "conn") and self.conn is not None:
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
            
            if str_bkp_name is None:
                str_bkp_name = f"{self.dbname}_backup.sql"
            
            backup_file = os.path.join(str_backup_dir, str_bkp_name)
            env = os.environ.copy()
            env["MYSQL_PWD"] = self.password

            command = [
                "mysqldump",
                "-h",
                self.host,
                "-P",
                str(self.port),
                "-u",
                self.user,
                self.dbname,
            ]
            
            with open(backup_file, "w", encoding="utf-8") as f:
                subprocess.run(command, stdout=f, check=True, env=env) # noqa S603: check for execution of untrusted input
                
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
        if shutil.which("mysqldump"):
            return True

        error_msg = "mysqldump not found. Attempting to install..."
        CreateLog().log_message(self.logger, error_msg, "warning")

        system = platform.system().lower()
        try:
            if system == "linux":
                subprocess.run(["apt-get", "update"], check=True)  # noqa S603: check for execution of untrusted input
                subprocess.run(
                    ["apt-get", "install", "-y", "mysql-client"], # noqa S607: starting a process with a partial executable path
                    check=True  # noqa S603: check for execution of untrusted input
                )
            elif system == "darwin":
                if shutil.which("brew"):
                    subprocess.run(
                        ["brew", "install", "mysql-client"], # noqa S607: starting a process with a partial executable path
                        check=True  # noqa S603: check for execution of untrusted input
                    )
                else:
                    error_msg = (
                        "Homebrew not found. Please install Homebrew and then "
                        "run 'brew install mysql-client' to install mysqldump."
                    )
                    CreateLog().log_message(self.logger, error_msg, "error")
                    return False
            elif system == "windows":
                error_msg = (
                    "mysqldump not found. Please install MySQL from "
                    "https://dev.mysql.com/downloads/mysql/ and ensure "
                    "mysqldump is in your system PATH."
                )
                CreateLog().log_message(self.logger, error_msg, "error")
                return False
            else:
                error_msg = (
                    f"Unsupported operating system: {system}. Please install "
                    "mysqldump manually."
                )
                CreateLog().log_message(self.logger, error_msg, "error")
                return False

            if shutil.which("mysqldump"):
                success_msg = "mysqldump successfully installed."
                CreateLog().log_message(self.logger, success_msg, "info")
                return True
            else:
                error_msg = "Failed to install mysqldump."
                CreateLog().log_message(self.logger, error_msg, "error")
                return False

        except subprocess.CalledProcessError as err:
            error_msg = f"Failed to install mysqldump: {err}"
            CreateLog().log_message(self.logger, error_msg, "error")
            return False