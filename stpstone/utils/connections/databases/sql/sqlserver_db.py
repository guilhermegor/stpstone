"""SQL Server database operations handler.

This module provides a class for managing SQL Server database connections and operations,
including query execution and data reading functionality. It ensures robust error handling
and type safety throughout database interactions.
"""

from logging import Logger
from typing import Any, Optional, Union

import pandas as pd
from pyodbc import Connection, connect

from stpstone.transformations.validation.metaclass_type_checker import SQLComposable
from stpstone.utils.calendars.calendar_abc import ABCCalendarOperations
from stpstone.utils.connections.databases.sql.database_abc import ABCDatabase, TypeDateFormatInput
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.json import JsonFiles


class SqlServerDB(ABCDatabase):
	"""SQL Server database handler with connection management and operations."""

	def __init__(
		self,
		driver_sql: str,
		server: str,
		port: int,
		database: str,
		user_id: str,
		password: str,
		timeout: int = 7200,
		logger: Optional[Logger] = None,
		bool_singleton: bool = False,
	) -> None:
		"""Initialize SQL Server database connection.

		Parameters
		----------
		driver_sql : str
			SQL Server driver name
		server : str
			Database server hostname
		port : int
			Database server port
		database : str
			Database name
		user_id : str
			Database username
		password : str
			Database password
		timeout : int
			Connection timeout in seconds (default: 7200)
		logger : Optional[Logger]
			Logger instance (default: None)
		bool_singleton : bool
			Whether to use a singleton connection (default: False)

		Returns
		-------
		None

		Raises
		------
		ConnectionError
			If database connection fails
		"""
		self.driver_sql = driver_sql
		self.server = server
		self.port = port
		self.database = database
		self.user_id = user_id
		self.password = password
		self.timeout = timeout
		self.logger = logger
		self.bool_singleton = bool_singleton

		self._validate_db_config()

		try:
			self.conn: Connection = self._create_connection()
			self.cursor = self.conn.cursor()
			self.execute("SELECT 1")
		except Exception as err:
			error_msg = f"Error connecting to database: {str(err)}"
			CreateLog().log_message(self.logger, error_msg, "error")
			raise ConnectionError(error_msg) from err
		self.cls_calendar_operations = ABCCalendarOperations()

	def _validate_db_config(self) -> None:
		"""Validate database configuration parameters.

		Raises
		------
		ValueError
			If driver_sql is not supported
			If server is empty
			If port is not a positive integer
			If database is empty
			If user_id is empty
			If password is empty
			If timeout is not a positive integer
		"""
		valid_drivers = ["{SQL Server}", "{ODBC Driver 17 for SQL Server}"]
		if self.driver_sql not in valid_drivers:
			raise ValueError(f"Driver must be one of {valid_drivers}, got {self.driver_sql}")
		if not self.server:
			raise ValueError("Server cannot be empty")
		if self.port <= 0:
			raise ValueError("Port must be a positive integer")
		if not self.database:
			raise ValueError("Database cannot be empty")
		if not self.user_id:
			raise ValueError("User ID cannot be empty")
		if not self.password:
			raise ValueError("Password cannot be empty")
		if self.timeout <= 0:
			raise ValueError("Timeout must be a positive integer")

	def _create_connection(self) -> Connection:
		"""Create SQL Server database connection.

		Returns
		-------
		Connection
			pyodbc Connection object

		Raises
		------
		ValueError
			If driver is not recognized
		"""
		if self.driver_sql == "{SQL Server}":
			str_conex = (
				f"Driver={self.driver_sql};"
				f"Server={self.server};"
				f"Database={self.database};"
				f"Uid={self.user_id};"
				f"Pwd={self.password}"
			)
		elif self.driver_sql == "{ODBC Driver 17 for SQL Server}":
			str_conex = (
				f"Driver={self.driver_sql};"
				f"Server={self.server};"
				f"PORT={self.port};"
				f"Database={self.database};"
				f"Trusted_Connection=no;"
				f"Encrypt=yes;"
				f"TrustServerCertificate=no;"
				f"UID={self.user_id};"
				f"PWD={self.password}"
			)
		else:
			raise ValueError(f"Driver SQL not identified: {self.driver_sql}")

		return connect(str_conex, autocommit=True, timeout=self.timeout)

	def execute(self, str_query: Union[str, SQLComposable]) -> None:
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
		str_fmt_dt: Optional[TypeDateFormatInput] = None,
	) -> pd.DataFrame:
		"""Read data from database into DataFrame.

		Parameters
		----------
		str_query : Union[str, SQLComposable]
			SQL query to execute
		dict_type_cols : Optional[dict[str, Any]]
			Dictionary for column type conversion (default: None)
		list_cols_dt : Optional[list[str]]
			List of date columns to convert (default: None)
		str_fmt_dt : Optional[TypeDateFormatInput]
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
		if (list_cols_dt is not None and str_fmt_dt is None) or (
			list_cols_dt is None and str_fmt_dt is not None
		):
			raise ValueError("Both list_cols_dt and str_fmt_dt must be provided or None")

		df_ = pd.read_sql(str_query, con=self.conn)

		if dict_type_cols is not None:
			df_ = df_.astype(dict_type_cols)

		if list_cols_dt is not None and str_fmt_dt is not None:
			for col_ in list_cols_dt:
				df_[col_] = [
					self.cls_calendar_operations.str_date_to_datetime(d, str_fmt_dt)
					for d in df_[col_]
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
		bool_insert_or_ignore : bool
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
		df_data = pd.DataFrame(json_data)

		try:
			if bool_insert_or_ignore:
				existing_query = f"SELECT * FROM {str_table_name} WHERE 1=0"  # noqa S608: possible SQL injection
				existing_df = pd.read_sql(existing_query, self.conn)
				df_data = df_data[~df_data.isin(existing_df.to_dict("list")).all(axis=1)]

			df_data.to_sql(str_table_name, self.conn, if_exists="append", index=False)
			self.conn.commit()

			if self.logger is not None:
				CreateLog().log_message(
					self.logger,
					f"Successful commit in db {self.database} / table {str_table_name}.",
					"info",
				)
		except Exception as err:
			self.conn.rollback()
			self.close()
			if self.logger is not None:
				CreateLog().log_message(
					self.logger,
					"Error while inserting data\n"
					+ f"DATABASE: {self.database}\n"
					+ f"TABLE_NAME: {str_table_name}\n"
					+ f"ERROR_MESSAGE: {err}",
					"error",
				)
			raise ValueError(
				"Error while inserting data\n"
				+ f"DATABASE: {self.database}\n"
				+ f"TABLE_NAME: {str_table_name}\n"
				+ f"ERROR_MESSAGE: {err}"
			) from err

	def close(self) -> None:
		"""Close database connection.

		Returns
		-------
		None
		"""
		self.conn.close()

	def backup(self, str_backup_dir: str, str_bkp_name: Optional[str] = None) -> str:
		"""Create database backup.

		Parameters
		----------
		str_backup_dir : str
			Backup directory path
		str_bkp_name : Optional[str]
			Backup file name (default: None)

		Returns
		-------
		str
			Backup status message

		Raises
		------
		NotImplementedError
			Backup functionality not implemented for SQL Server
		"""
		raise NotImplementedError("Backup functionality not implemented for SQL Server")

	def check_bkp_tool(self) -> bool:
		"""Check if backup tool is available.

		Returns
		-------
		bool
			Always False for SQL Server (backup not implemented)
		"""
		return False
