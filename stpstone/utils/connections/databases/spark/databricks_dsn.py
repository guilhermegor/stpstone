"""Databricks SQL connection and query execution utilities.

This module provides a class for handling Databricks SQL connections and query execution,
including connection retries, timeout handling, and comprehensive error logging.
It ensures robust error handling and type safety throughout database interactions.
"""

import threading
from typing import Optional, Union

import pandas as pd
import pyodbc as pyo

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker, type_checker
from stpstone.utils.loggs.create_logs import CreateLog


class Databricks(metaclass=TypeChecker):
	"""Class for handling Databricks SQL connections and query execution.

	This class provides methods to connect to Databricks using ODBC DSNs,
	execute SQL queries with timeout handling, and manage connection retries
	with comprehensive error logging.
	"""

	def __init__(
		self,
		str_query: str,
		list_dsns: list[str],
		logger: Optional[object] = None,
		max_error_attempts: int = 3,
	) -> None:
		"""Initialize Databricks connection handler.

		Parameters
		----------
		str_query : str
			SQL query string to execute
		list_dsns : list[str]
			list of DSN names to attempt connection
		logger : Optional[object]
			Logger object for logging messages (default: None)
		max_error_attempts : int
			Maximum number of connection retry attempts (default: 3)

		Returns
		-------
		None
		"""
		self._validate_init_params(str_query, list_dsns, max_error_attempts)
		self.str_query = str_query
		self.list_dsns = list_dsns
		self.logger = logger
		self.max_error_attempts = max_error_attempts

	def _validate_init_params(
		self, str_query: str, list_dsns: list[str], max_error_attempts: int
	) -> None:
		"""Validate initialization parameters.

		Parameters
		----------
		str_query : str
			SQL query string
		list_dsns : list[str]
			list of DSN names
		max_error_attempts : int
			Maximum error attempts

		Raises
		------
		ValueError
			If str_query is empty or not a string
			If list_dsns is empty or not a list
			If max_error_attempts is not positive
		"""
		if not str_query or not isinstance(str_query, str):
			raise ValueError("str_query must be a non-empty string")
		if not list_dsns or not isinstance(list_dsns, list):
			raise ValueError("list_dsns must be a non-empty list")
		if max_error_attempts <= 0:
			raise ValueError("max_error_attempts must be a positive integer")

	def _validate_dsn_conn(self, dsn_conn: str) -> None:
		"""Validate DSN connection string.

		Parameters
		----------
		dsn_conn : str
			DSN connection string

		Raises
		------
		ValueError
			If dsn_conn is empty or not a string
		"""
		if not dsn_conn or not isinstance(dsn_conn, str):
			raise ValueError("dsn_conn must be a non-empty string")

	def _validate_int_timeout(self, int_timeout: int) -> None:
		"""Validate timeout value.

		Parameters
		----------
		int_timeout : int
			Timeout value in seconds

		Raises
		------
		ValueError
			If int_timeout is not positive
		"""
		if int_timeout <= 0:
			raise ValueError("int_timeout must be a positive integer")

	def conn_dsn_databricks(
		self, dsn_conn: str, int_timeout: int = 108000, bool_autocommit: bool = True
	) -> pyo.Connection:
		"""Establish connection with Databricks using DSN.

		Parameters
		----------
		dsn_conn : str
			DSN connection string name
		int_timeout : int
			Connection timeout in seconds (default: 108000)
		bool_autocommit : bool
			Enable autocommit mode (default: True)

		Returns
		-------
		pyo.Connection
			PyODBC connection object

		"""
		self._validate_dsn_conn(dsn_conn)
		self._validate_int_timeout(int_timeout)

		try:
			return pyo.connect(f"DSN={dsn_conn}", autocommit=bool_autocommit, timeout=int_timeout)
		except pyo.Error as err:
			raise pyo.Error(f"Failed to connect to DSN {dsn_conn}: {str(err)}") from err

	def fetch_data_from_databricks(self, connection: pyo.Connection) -> pd.DataFrame:
		"""Execute SQL query and fetch data from Databricks.

		Parameters
		----------
		connection : pyo.Connection
			Active PyODBC connection object

		Returns
		-------
		pd.DataFrame
			Pandas DataFrame with query results

		Raises
		------
		ValueError
			If connection is invalid or query execution fails
		"""
		if not connection:
			raise ValueError("Connection object cannot be None")

		try:
			return pd.read_sql(self.str_query, connection)
		except pyo.Error as err:
			raise pyo.Error(f"Failed to execute query: {str(err)}") from err
		except Exception as err:
			raise ValueError(f"Unexpected error during query execution: {str(err)}") from err

	def conn_databricks(
		self, int_max_wait: int = 480, bool_kill_process_when_databricks_down: bool = False
	) -> Union[pd.DataFrame, str]:
		"""Attempt connection with Databricks using available DSNs.

		Parameters
		----------
		int_max_wait : int
			Maximum wait time for query execution in seconds (default: 480)
		bool_kill_process_when_databricks_down : bool
			Whether to raise exception on complete failure (default: False)

		Returns
		-------
		Union[pd.DataFrame, str]
			DataFrame with query results or error message string

		Raises
		------
		ValueError
			If bool_kill_process_when_databricks_down is True and all connections fail
		TimeoutError
			If query execution times out
		"""
		self._validate_int_timeout(int_max_wait)
		bool_conn = False
		df_databricks = pd.DataFrame()
		str_error = "CONNECTION TIMEOUT EXPIRED"

		class TimeoutError(Exception, metaclass=TypeChecker):
			"""Timeout error class."""

			pass

		@type_checker
		def timeout_handler() -> None:
			"""Timeout handler function.

			Raises
			------
			TimeoutError
				If query execution times out
			"""
			raise TimeoutError("Query execution timed out")

		for dsn in self.list_dsns:
			try:
				connection = self.conn_dsn_databricks(dsn)
				timer = threading.Timer(int_max_wait, timeout_handler)
				timer.start()
				try:
					df_databricks = self.fetch_data_from_databricks(connection)
					bool_conn = True
					break
				finally:
					timer.cancel()
			except TimeoutError:
				if self.logger is not None:
					CreateLog().log_message(
						self.logger,
						f"Connection could not be established in the DSN {dsn} due to timeout",
						"warning",
					)
			except Exception as err:
				if self.logger is not None:
					CreateLog().log_message(
						self.logger,
						f"Connection could not be established in the DSN {dsn}. Error: {str(err)}",
						"warning",
					)

		if not bool_conn:
			error_msg = (
				"Connection to Databricks could not be established in any of the DSNs, "
				f"please validate the stability of the service. "
				f"list of DSNs configured: {self.list_dsns}"
			)
			if self.logger is not None:
				CreateLog().log_message(self.logger, error_msg, "warning")
			if bool_kill_process_when_databricks_down:
				raise ValueError(error_msg)
			return str_error

		return df_databricks

	def retrieve_query_data(self, int_max_wait: int = 480) -> Union[pd.DataFrame, str]:
		"""Retrieve query data with retry mechanism.

		Parameters
		----------
		int_max_wait : int
			Maximum wait time for each attempt in seconds (default: 480)

		Returns
		-------
		Union[pd.DataFrame, str]
			DataFrame with query results or error message after max attempts
		"""
		self._validate_int_timeout(int_max_wait)
		df_databricks = ""
		i = 0

		while (isinstance(df_databricks, str)) and (i <= self.max_error_attempts):
			df_databricks = self.conn_databricks(int_max_wait=int_max_wait)

			log_msg = f"#{i} Attempting connection with DSNs to Databricks"
			if self.logger is None:
				print(log_msg)
			else:
				CreateLog().log_message(self.logger, log_msg, "info")

			i += 1

		return df_databricks
