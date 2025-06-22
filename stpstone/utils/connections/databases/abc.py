from abc import ABC, ABCMeta, abstractmethod
from logging import Logger
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

import pandas as pd

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.loggs.create_logs import CreateLog


@runtime_checkable
class SQLComposable(Protocol):
    """Protocol for database-agnostic SQL composable objects"""

    def __str__(self) -> str: ...


@runtime_checkable
class DbCursor(Protocol):
    """Protocol defining a generic database cursor interface."""

    def execute(self, query: str, params: Any = None) -> Any: ...
    def fetchone(self) -> Any: ...
    def fetchall(self) -> List[Any]: ...
    def close(self) -> None: ...


@runtime_checkable
class DbConnection(Protocol):
    """Protocol defining a generic database connection interface."""

    def cursor(self) -> DbCursor: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...


class ABCTypeCheckerMeta(ABCMeta, TypeChecker):
    pass


class ABCDatabase(ABC, metaclass=ABCTypeCheckerMeta):
    """
    Abstract base class for database connections that enforces:
    - self.conn (DbConnection) and self.cursor (DbCursor) must be created in __init__
    - Standard database operations interface
    """

    # class variables for type hinting (enforces instance attributes)
    conn: DbConnection
    cursor: DbCursor

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
    ) -> None:
        """
        Initialize database connection. Concrete implementations MUST:
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
        """
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.str_schema = str_schema
        self.logger = logger

    @abstractmethod
    def execute(self, str_query: str | SQLComposable) -> None:
        """
        Execute a SQL query without returning results.

        Parameters
        ----------
        str_query : str
            SQL query to execute
        """
        pass

    @abstractmethod
    def read(
        self,
        str_query: str,
        dict_type_cols: Optional[Dict[str, Any]] = None,
        list_cols_dt: Optional[List[str]] = None,
        str_fmt_dt: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Execute a query and return results as DataFrame.

        Parameters
        ----------
        str_query : str
            SQL query to execute
        dict_type_cols : Optional[Dict[str, Any]], optional
            Column type mapping, defaults to None
        list_cols_dt : Optional[List[str]], optional
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
        json_data: List[Dict[str, Any]],
        str_table_name: str,
        bl_insert_or_ignore: bool = False,
    ) -> None:
        """
        Insert data into a table.

        Parameters
        ----------
        json_data : List[Dict[str, Any]]]
            Data to insert (list of dicts)
        str_table_name : str
            Target table name
        bl_insert_or_ignore : bool, optional
            If True, ignore duplicates, defaults to False
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the database connection."""
        pass

    @abstractmethod
    def backup(self, str_backup_dir: str, str_bkp_name: Optional[str] = None) -> str:
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

    def __enter__(self):
        """Support context manager protocol."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit handler. Closes the connection and:
        - Returns False/None to let exceptions propagate
        - Logs any errors during closing
        """
        try:
            self.close()
            if exc_type is not None and self.logger is not None:
                CreateLog().error(
                    self.logger,
                    f"Context exited with exception: {exc_type.__name__}: {exc_val}",
                )
        except Exception as e:
            if self.logger is not None:
                CreateLog().error(self.logger, f"Error closing connection: {str(e)}")
            raise
        return None
