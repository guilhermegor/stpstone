"""Oracle database operations handler.

This module provides a class for managing Oracle database connections and operations,
including query execution, data insertion, reading, and backup functionality. It ensures
robust error handling and type safety throughout database interactions.
"""

from logging import Logger
import os
import platform
import shutil
import subprocess
from typing import Any, Optional, Union

import oracledb
from oracledb import Connection, Cursor
import pandas as pd

from stpstone.transformations.validation.metaclass_type_checker import Composable
from stpstone.utils.cals.cal_abc import DatesBR
from stpstone.utils.connections.databases.sql.database_abc import ABCDatabase
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.json import JsonFiles


class OracleDB(ABCDatabase):
    """Oracle database handler with connection management and operations."""

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
        """Initialize Oracle database connection.
        
        Parameters
        ----------
        dbname : str
            Database name or service name
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
        super().__init__(dbname, user, password, host, port, str_schema, logger, bool_singleton)
        self.dict_db_config = {
            "user": self.user,
            "password": self.password,
            "dsn": f"{self.host}:{self.port}/{self.dbname}",
        }
        self._validate_db_config()
        try:
            oracledb.init_oracle_client()
            self.conn: Connection = oracledb.connect(**self.dict_db_config)
            self.cursor: Cursor = self.conn.cursor()
            self.execute("SELECT 1 FROM DUAL")
            if self.str_schema:
                self.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {self.str_schema}")
        except oracledb.Error as err:
            error_msg = f"Error connecting to Oracle database: {str(err)}"
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
        self._validate_query(str_query)
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
        self._validate_query(str_query)
        if (list_cols_dt is not None and str_fmt_dt is None) \
            or (list_cols_dt is None and str_fmt_dt is not None):
            raise ValueError("Both list_cols_dt and str_fmt_dt must be provided or None")

        self.cursor.execute(str_query)
        columns = [desc[0].lower() for desc in self.cursor.description]
        data = self.cursor.fetchall()
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
            Whether to use INSERT OR IGNORE (default: False)

        Raises
        ------
        ValueError
            If table name is empty or json_data is empty
        """
        self._validate_insert_data(json_data, str_table_name)

        json_data = JsonFiles().normalize_json_keys(json_data)
        columns = json_data[0].keys()
        placeholders = ", ".join([":" + str(i + 1) for i in range(len(columns))])
        cols = ", ".join(columns)

        if bool_insert_or_ignore:
            query = f"""
                MERGE INTO {str_table_name} dest
                USING (SELECT :1 {columns[0]} FROM DUAL) src
                ON (dest.{columns[0]} = src.{columns[0]})
                WHEN NOT MATCHED THEN
                INSERT ({cols})
                VALUES ({placeholders})
            """ # noqa S608: possible SQL injection
        else:
            query = f"""
                INSERT INTO {str_table_name} ({cols})
                VALUES ({placeholders})
            """ # noqa S608: possible SQL injection

        try:
            records = [tuple(record[col] for col in columns) for record in json_data]
            self.cursor.executemany(query, records)
            self.conn.commit()

            CreateLog().log_message(
                self.logger,
                f"Successful commit in db {self.dict_db_config['dsn']} "
                + f"/ table {str_table_name}.",
                "info"
            )
        except oracledb.Error as err:
            self.conn.rollback()
            self.close()
            error_msg = (
                "Error while inserting data\n"
                + f"DB_CONFIG: {self.dict_db_config}\n"
                + f"TABLE_NAME: {str_table_name}\n"
                + f"JSON_DATA: {json_data}\n"
                + f"ERROR_MESSAGE: {err}"
            )
            CreateLog().log_message(self.logger, error_msg, "error")
            raise ValueError(error_msg) from err

    def close(self) -> None:
        """Close database connection.
        
        Returns
        -------
        None
        """
        try:
            self.cursor.close()
            self.conn.close()
        except oracledb.Error as err:
            CreateLog().log_message(
                self.logger,
                f"Error closing connection: {str(err)}",
                "error"
            )
            raise

    def backup(
        self, 
        str_backup_dir: str, 
        str_bkp_name: Optional[str] = None
    ) -> str:
        """Create database backup using Oracle Data Pump.

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
        RuntimeError
            If backup tool is not available
        """
        self._validate_backup_params(str_backup_dir)

        if not self.check_bkp_tool():
            error_msg = "Oracle Data Pump (expdp) is required for backups, but it is not available"
            CreateLog().log_message(self.logger, error_msg, "error")
            raise RuntimeError(error_msg)

        try:
            os.makedirs(str_backup_dir, exist_ok=True)
            backup_file = os.path.join(str_backup_dir, str_bkp_name or f"{self.dbname}_backup.dmp")
            env = os.environ.copy()
            env["ORACLE_SID"] = self.dbname
            env["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"

            command = [
                "expdp",
                f"{self.user}/{self.password}@{self.host}:{self.port}/{self.dbname}",
                "DIRECTORY=DATA_PUMP_DIR",
                f"DUMPFILE={os.path.basename(backup_file)}",
                f"LOGFILE={os.path.basename(backup_file)}.log",
            ]
            subprocess.run(command, check=True, env=env) # noqa S603: possible subprocess injection
            return f"Backup successful! File saved at: {backup_file}"
        except subprocess.CalledProcessError as err:
            return f"Backup failed: {err}"
        except Exception as err:
            return f"An error occurred: {err}"

    def check_bkp_tool(self) -> bool:
        """Check if Oracle Data Pump (expdp) is available.

        Returns
        -------
        bool
            True if backup tool is available, False otherwise
        """
        if shutil.which("expdp"):
            return True

        error_msg = "expdp not found. Attempting to locate Oracle client..."
        CreateLog().log_message(self.logger, error_msg, "warning")

        system = platform.system().lower()
        try:
            if system == "linux":
                if os.path.exists("/u01/app/oracle/product"):
                    os.environ["ORACLE_HOME"] = "/u01/app/oracle/product"
                    os.environ["PATH"] = f"{os.environ['ORACLE_HOME']}/bin:{os.environ['PATH']}"
            elif system == "windows":
                if os.path.exists("C:\\app\\oracle\\product"):
                    os.environ["ORACLE_HOME"] = "C:\\app\\oracle\\product"
                    os.environ["PATH"] = f"{os.environ['ORACLE_HOME']}\\bin;{os.environ['PATH']}"
            elif system == "darwin":
                error_msg = "Oracle Data Pump is not supported on macOS. Please install manually."
                CreateLog().log_message(self.logger, error_msg, "error")
                return False

            if shutil.which("expdp"):
                success_msg = "expdp successfully located."
                CreateLog().log_message(self.logger, success_msg, "info")
                return True
            else:
                error_msg = "Failed to locate expdp. Please ensure Oracle Client is installed."
                CreateLog().log_message(self.logger, error_msg, "error")
                return False

        except Exception as err:
            error_msg = f"Failed to locate expdp: {err}"
            CreateLog().log_message(self.logger, error_msg, "error")
            return False