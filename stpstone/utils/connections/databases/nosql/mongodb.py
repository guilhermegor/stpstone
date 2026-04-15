"""MongoDB connection and data handling utilities.

This module provides a singleton class for managing MongoDB connections and operations
using pymongo for database interactions and pandas for data handling.
"""

from __future__ import annotations

from logging import Logger
from typing import Any, Optional

import pandas as pd
from pymongo import MongoClient

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.loggs.create_logs import CreateLog


class MongoConn(metaclass=TypeChecker):
    """Singleton class for MongoDB connection management.

    This class implements the singleton pattern to ensure only one MongoDB connection
    instance exists, providing methods for data operations and connection management.

    Attributes
    ----------
    str_host : str
        MongoDB server host address
    int_port : int
        MongoDB server port number
    str_dbname : str
        Database name
    str_collection : str
        Collection name
    logger : Optional[Logger]
        Logger instance for logging operations
    """

    _instance: Optional[MongoConn] = None # noqa UP045: use X | None for type annotations
    _initialized: bool = False

    def __new__(
        cls: type[MongoConn], 
        *args: Any, # noqa ANN401: typing.Any is not allowed
        **kwargs: Any # noqa ANN401: typing.Any is not allowed
    ) -> MongoConn:
        """Create singleton instance of MongoConn.

        Ensures that only one instance of the class is created (singleton pattern).

        Parameters
        ----------
        cls : type[MongoConn]
            Class object
        *args: Any
            Variable length argument list
        **kwargs: Any
            Arbitrary keyword arguments

        Returns
        -------
        MongoConn
            Singleton instance of MongoConn class
        """
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        str_host: str = "localhost",
        int_port: int = 27017,
        str_dbname: str = "test",
        str_collection: str = "data",
        logger: Optional[Logger] = None # noqa UP045: use X | None for type annotations
    ) -> None:
        """Initialize MongoDB connection parameters.
        
        Parameters
        ----------
        str_host : str
            Address of the MongoDB server (default: "localhost")
        int_port : int
            Port of the MongoDB server (default: 27017)
        str_dbname : str
            Name of the database (default: "test")
        str_collection : str
            Name of the collection (default: "data")
        logger : Optional[Logger]
            Logger instance for logging (default: None)
        """
        # Always validate inputs to ensure they are valid, even if not used
        self._validate_host(str_host)
        self._validate_port(int_port)
        self._validate_dbname(str_dbname)
        self._validate_collection(str_collection)
        
        # Only initialize if not already initialized (singleton pattern)
        if not MongoConn._initialized:
            # Set attributes from the FIRST initialization (singleton pattern)
            self.str_host = str_host
            self.int_port = int_port
            self.str_dbname = str_dbname
            self.str_collection = str_collection
            self.logger = logger
            
            # Initialize connection components
            self._client = None
            self._db = None
            self._collection = None
            self._connect()
            MongoConn._initialized = True

    def _validate_host(self, str_host: str) -> None:
        """Validate MongoDB host address.

        Parameters
        ----------
        str_host : str
            Host address to validate

        Raises
        ------
        ValueError
            If host is empty or not a string
        """
        if not isinstance(str_host, str):
            raise ValueError("Host must be a string")
        if not str_host:
            raise ValueError("Host cannot be empty")

    def _validate_port(self, int_port: int) -> None:
        """Validate MongoDB port number.

        Parameters
        ----------
        int_port : int
            Port number to validate

        Raises
        ------
        ValueError
            If port is not an integer or out of valid range (1-65535)
        """
        if not isinstance(int_port, int):
            raise ValueError("Port must be an integer")
        if not 1 <= int_port <= 65535:
            raise ValueError("Port must be between 1 and 65535")

    def _validate_dbname(self, str_dbname: str) -> None:
        """Validate database name.

        Parameters
        ----------
        str_dbname : str
            Database name to validate

        Raises
        ------
        ValueError
            If database name is empty or not a string
        """
        if not isinstance(str_dbname, str):
            raise ValueError("Database name must be a string")
        if not str_dbname:
            raise ValueError("Database name cannot be empty")

    def _validate_collection(self, str_collection: str) -> None:
        """Validate collection name.

        Parameters
        ----------
        str_collection : str
            Collection name to validate

        Raises
        ------
        ValueError
            If collection name is empty or not a string
        """
        if not isinstance(str_collection, str):
            raise ValueError("Collection name must be a string")
        if not str_collection:
            raise ValueError("Collection name cannot be empty")

    def _connect(self) -> None:
        """Establish connection to MongoDB server.

        Initializes the database and collection objects for subsequent operations.

        Raises
        ------
        ConnectionError
            If connection to MongoDB server fails
        """
        try:
            self._client = MongoClient(self.str_host, self.int_port)
            self._db = self._client[self.str_dbname]
            self._collection = self._db[self.str_collection]
        except Exception as err:
            raise ConnectionError(f"Failed to connect to MongoDB: {str(err)}") from err

    def save_df(self, df_: pd.DataFrame) -> None:
        """Save pandas DataFrame to MongoDB collection.

        Parameters
        ----------
        df_ : pd.DataFrame
            DataFrame to be saved to MongoDB

        Raises
        ------
        RuntimeError
            If data insertion fails
        """
        self._validate_dataframe(df_)
        
        data = df_.to_dict(orient="records")
        try:
            self._collection.insert_many(data)
        except Exception as err:
            if self.logger is not None:
                CreateLog().log_message(
                    self.logger,
                    f"Error: {err}; mongoDB injestion aborted",
                    "info"
                )
            raise RuntimeError(f"Failed to insert data into MongoDB: {str(err)}") from err

    def _validate_dataframe(self, df_: pd.DataFrame) -> None:
        """Validate input DataFrame.

        Parameters
        ----------
        df_ : pd.DataFrame
            DataFrame to validate

        Raises
        ------
        ValueError
            If input is not a pandas DataFrame or is empty
        """
        if not isinstance(df_, pd.DataFrame):
            raise ValueError("The provided data is not a pandas DataFrame")
        if df_.empty:
            raise ValueError("DataFrame cannot be empty")

    def close(self) -> None:
        """Close connection to MongoDB server.

        Safely closes the client connection if it exists.
        """
        if self._client:
            self._client.close()

    def __enter__(self) -> MongoConn:
        """Return instance for use in context manager.

        Returns
        -------
        MongoConn
            Current instance for context management
        """
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]], # noqa UP045: use X | None for type annotations
        exc_val: Optional[BaseException], # noqa UP045: use X | None for type annotations
        exc_tb: Optional[BaseException] # noqa UP045: use X | None for type annotations
    ) -> None:
        """Close connection when exiting context.

        Parameters
        ----------
        exc_type : Optional[type[BaseException]]
            Exception type if any
        exc_val : Optional[BaseException]
            Exception value if any
        exc_tb : Optional[BaseException]
            Traceback if any
        """
        self.close()

    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton instance for testing purposes."""
        cls._instance = None
        cls._initialized = False