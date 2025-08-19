"""Abstract base classes for database operations with protocol definitions.

This module provides abstract base classes and protocols for database operations,
including connection management, query execution, and data insertion. It supports
both regular instances and singleton patterns for database connections.
"""

from abc import ABC, ABCMeta, abstractmethod
from logging import Logger
from typing import Any, Literal, Optional, Union

import pandas as pd

from stpstone.transformations.validation.metaclass_type_checker import (
    DbConnection,
    DbCursor,
    SQLComposable,
    TypeChecker,
)
from stpstone.utils.loggs.create_logs import CreateLog


class ABCTypeCheckerMeta(ABCMeta, TypeChecker):
    """Meta class for type checking abstract base classes."""

    pass


class ABCDatabase(ABC, metaclass=ABCTypeCheckerMeta):
    """Abstract base class for database connections.
    
    Enforces:
    - self.conn (DbConnection) and self.cursor (DbCursor) must be created in __init__
    - Standard database operations interface

    References
    ----------
    .. [1] https://docs.python.org/3/library/abc.html
    .. [2] https://peps.python.org/pep-0544/
    """

    # class variables for type hinting (enforces instance attributes)
    conn: DbConnection
    cursor: DbCursor
    _instances: dict[type, Any] = {}

    @abstractmethod
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
        """
        Initialize database connection.
        
        Concrete implementations MUST:
        1. Create self.conn (DbConnection)
        2. Create self.cursor (DbCursor)
        3. Initialize other necessary attributes

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
            Schema name, defaults to 'public'
        logger : Optional[Logger], optional
            Logger instance, defaults to None
        bool_singleton : bool, optional
            Whether to use singleton pattern, defaults to False

        Returns
        -------
        None
        """
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.str_schema = str_schema
        self.logger = logger
        self.bool_singleton = bool_singleton

    def _validate_connection_params(self) -> None:
        """Validate database connection parameters.

        Raises
        ------
        ValueError
            If database name, user, password, host, or port are empty
            If database port is not a positive integer
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
            raise ValueError("Schema name cannot be empty")

    def _validate_query(
        self, 
        str_query: Union[str, SQLComposable]
    ) -> None:
        """Validate SQL query parameters.

        Parameters
        ----------
        str_query : Union[str, SQLComposable]
            SQL query to validate

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If query is empty
        """
        if not str_query:
            raise ValueError("SQL query cannot be empty")

    def _validate_insert_data(
        self, 
        json_data: list[dict[str, Any]], 
        str_table_name: str
    ) -> None:
        """Validate data insertion parameters.

        Parameters
        ----------
        json_data : list[dict[str, Any]]
            Data to insert
        str_table_name : str
            Target table name

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If json_data or str_table_name is empty
        """
        if not json_data:
            raise ValueError("Insert data cannot be empty")
        if not str_table_name:
            raise ValueError("Table name cannot be empty")

    def _validate_backup_params(
        self, 
        str_backup_dir: str
    ) -> None:
        """Validate backup parameters.

        Parameters
        ----------
        str_backup_dir : str
            Backup directory path
        
        Returns
        -------
        None

        Raises
        ------
        ValueError
            If str_backup_dir is empty
        """
        if not str_backup_dir:
            raise ValueError("Backup directory cannot be empty")

    def _validate_format_query_params(
        self, 
        query_path: str, 
        dict_params: dict[str, Any]
    ) -> None:
        """Validate format query parameters.

        Parameters
        ----------
        query_path : str
            Path to SQL query file
        dict_params : dict[str, Any]
            Parameters for query formatting

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If query_path or dict_params is empty
        """
        if not query_path:
            raise ValueError("Query path cannot be empty")
        if not dict_params:
            raise ValueError("Parameters dictionary cannot be empty")

    @classmethod
    def _get_singleton_instance(
        cls, 
        *args: Any, # noqa ANN401: typing.Any is not allowed
        **kwargs: Any # noqa ANN401: typing.Any is not allowed
    ) -> "ABCDatabase":
        """Get singleton instance of the database class.

        Parameters
        ----------
        *args : Any
            Positional arguments for initialization
        **kwargs : Any
            Keyword arguments for initialization

        Returns
        -------
        'ABCDatabase'
            Singleton instance of the database class
        """
        if cls not in cls._instances:
            # temporarily set bool_singleton to False to avoid recursion
            singleton_kwargs = kwargs.copy()
            singleton_kwargs["bool_singleton"] = False
            # use super().__new__ to bypass the singleton check in __new__
            instance = super().__new__(cls)
            instance.__init__(*args, **singleton_kwargs)
            # restore the original bool_singleton value
            instance.bool_singleton = kwargs.get("bool_singleton", False)
            cls._instances[cls] = instance
        return cls._instances[cls]

    def format_query(
        self, 
        query_path: str, 
        dict_params: dict[str, Any]
    ) -> str:
        """Format a SQL query with parameters using f-strings.

        Parameters
        ----------
        query_path : str
            Path to SQL query file
        dict_params : dict[str, Any]
            Dictionary of parameters for formatting

        Returns
        -------
        str
            Formatted SQL query

        Raises
        ------
        ValueError
            If file cannot be read or parameters are invalid
        FileNotFoundError
            If query file does not exist
        """
        self._validate_format_query_params(query_path, dict_params)
        
        try:
            with open(query_path, encoding="utf-8") as query_file:
                query_read = query_file.read()
        except FileNotFoundError as err:
            raise FileNotFoundError(f"Query file not found: {query_path}") from err
        except OSError as err:
            raise ValueError(f"Error reading query file: {str(err)}") from err
        
        try:
            return query_read.format(**dict_params)
        except KeyError as err:
            raise ValueError(f"Missing parameter in query: {str(err)}") from err
        except ValueError as err:
            raise ValueError(f"Error formatting query: {str(err)}") from err

    @abstractmethod
    def execute(
        self, 
        str_query: Union[str, SQLComposable]
    ) -> None:
        """
        Execute a SQL query without returning results.

        Parameters
        ----------
        str_query : Union[str, SQLComposable]
            SQL query to execute
            
        Returns
        -------
        None
        """
        pass

    @abstractmethod
    def read(
        self,
        str_query: str,
        dict_type_cols: Optional[dict[str, Any]] = None,
        list_cols_dt: Optional[list[str]] = None,
        str_fmt_dt: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Execute a query and return results as DataFrame.

        Parameters
        ----------
        str_query : str
            SQL query to execute
        dict_type_cols : Optional[dict[str, Any]], optional
            Column type mapping, defaults to None
        list_cols_dt : Optional[list[str]], optional
            Date columns to parse, defaults to None
        str_fmt_dt : Optional[str], optional
            Date format string, defaults to None

        Returns
        -------
        pd.DataFrame
            Query results
        """
        pass

    @abstractmethod
    def insert(
        self,
        json_data: list[dict[str, Any]],
        str_table_name: str,
        bool_insert_or_ignore: bool = False,
    ) -> None:
        """
        Insert data into a table.

        Parameters
        ----------
        json_data : list[dict[str, Any]]
            Data to insert (list of dicts)
        str_table_name : str
            Target table name
        bool_insert_or_ignore : bool, optional
            If True, ignore duplicates, defaults to False

        Returns
        -------
        None
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the database connection.

        Returns
        -------
        None
        """
        pass

    @abstractmethod
    def backup(
        self, 
        str_backup_dir: str, 
        str_bkp_name: Optional[str] = None
    ) -> str:
        """
        Create database backup.

        Parameters
        ----------
        str_backup_dir : str
            Backup directory path
        str_bkp_name : Optional[str], optional
            Custom backup filename, defaults to None

        Returns
        -------
        str
            Backup status message
        """
        pass

    def __new__(
        cls, 
        *args: Any, # noqa ANN401: typing.Any is not allowed
        **kwargs: Any # noqa ANN401: typing.Any is not allowed
    ) -> "ABCDatabase":
        """Handle singleton pattern during instance creation.

        Parameters
        ----------
        *args : Any
            Positional arguments
        **kwargs : Any
            Keyword arguments

        Returns
        -------
        'ABCDatabase'
            New instance or existing singleton instance
        """
        bool_singleton = kwargs.get("bool_singleton", False)
        if bool_singleton:
            return cls._get_singleton_instance(*args, **kwargs)
        return super().__new__(cls)

    def __enter__(self) -> "ABCDatabase":
        """Support context manager protocol.

        Returns
        -------
        'ABCDatabase'
            Self instance
        """
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any], # noqa ANN401: typing.Any is not allowed
    ) -> Literal[False]:
        """Context manager exit handler.
        
        Closes the connection and:
        - Returns False to let exceptions propagate
        - Logs any errors during closing

        Parameters
        ----------
        exc_type : Optional[type[BaseException]]
            Exception type
        exc_val : Optional[BaseException]
            Exception value
        exc_tb : Optional[Any]
            Exception traceback

        Returns
        -------
        Literal[False]
            Always returns False to propagate exceptions
        """
        try:
            self.close()
            if exc_type is not None and self.logger is not None:
                CreateLog().log_message(
                    self.logger,
                    f"Context exited with exception: {exc_type.__name__}: {exc_val}",
                    "error",
                )
        except Exception as err:
            if self.logger is not None:
                CreateLog().log_message(
                    self.logger, f"Error closing connection: {str(err)}", "error"
                )
            raise
        return False