"""Redis client singleton for managing Redis connections.

This module provides a singleton class for managing Redis client connections
using the redis-py library with configurable connection parameters.
"""

from typing import TypedDict

import redis

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class ReturnLoadConfig(TypedDict):
    """Configuration dictionary for Redis connection.

    Parameters
    ----------
    host : str
        Redis server hostname
    port : int
        Redis server port number
    decode_responses : bool
        Whether to decode responses from bytes to strings
    """

    host: str
    port: int
    decode_responses: bool


class RedisClient(metaclass=TypeChecker):
    """Singleton Redis client for managing connections.

    This class implements the singleton pattern to ensure only one Redis
    connection instance exists per application, with configurable connection
    parameters.

    Parameters
    ----------
    str_host : str
        Redis server hostname (default: 'localhost')
    int_port : int
        Redis server port number (default: 6379)
    bl_decode_resp : bool
        Whether to decode responses from bytes to strings (default: True)

    References
    ----------
    .. [1] https://redis-py.readthedocs.io/en/stable/
    .. [2] https://en.wikipedia.org/wiki/Singleton_pattern
    """

    _instance = None

    def __init__(self, str_host: str = "localhost", int_port: int = 6379, bl_decode_resp: bool = True) -> None:
        """Store the connection configuration.

        Parameters
        ----------
        str_host : str
            Redis server hostname
        int_port : int
            Redis server port number
        bl_decode_resp : bool
            Whether to decode responses from bytes to strings
        """
        self._validate_host(str_host)
        self._validate_port(int_port)
        self._validate_decode_resp(bl_decode_resp)

        self.str_host = str_host
        self.int_port = int_port
        self.bl_decode_resp = bl_decode_resp

    def __new__(cls, *args, **kwargs) -> "RedisClient":
        """Create or retrieve the singleton instance of RedisClient.

        Returns
        -------
        RedisClient
            Singleton instance of RedisClient class

        Raises
        ------
        RuntimeError
            If Redis connection fails during instance creation
        """
        if not cls._instance:
            try:
                cls._instance = super(RedisClient, cls).__new__(cls)
                # Initialize with provided arguments or defaults
                host = kwargs.get('str_host', 'localhost') if kwargs else (args[0] if len(args) > 0 else 'localhost')
                port = kwargs.get('int_port', 6379) if kwargs else (args[1] if len(args) > 1 else 6379)
                decode_resp = kwargs.get('bl_decode_resp', True) if kwargs else (args[2] if len(args) > 2 else True)
                
                cls._instance._redis_client = cls._connect(host, port, decode_resp)
                # Store the actual configuration used
                cls._instance.str_host = host
                cls._instance.int_port = port
                cls._instance.bl_decode_resp = decode_resp
            except Exception as err:
                raise RuntimeError("Failed to create Redis client instance") from err
        return cls._instance

    def _validate_host(self, str_host: str) -> None:
        """Validate Redis host parameter.

        Parameters
        ----------
        str_host : str
            Redis server hostname to validate

        Raises
        ------
        ValueError
            If host is empty or not a string
        """
        if not isinstance(str_host, str):
            raise ValueError("Redis host must be a string")
        if not str_host or not str_host.strip():
            raise ValueError("Redis host cannot be empty or whitespace-only")

    def _validate_port(self, int_port: int) -> None:
        """Validate Redis port parameter.

        Parameters
        ----------
        int_port : int
            Redis server port number to validate

        Raises
        ------
        ValueError
            If port is not an integer or out of valid range (1-65535)
        """
        if not isinstance(int_port, int):
            raise ValueError("Redis port must be an integer")
        if not 1 <= int_port <= 65535:
            raise ValueError("Redis port must be between 1 and 65535")

    def _validate_decode_resp(self, bl_decode_resp: bool) -> None:
        """Validate decode responses parameter.

        Parameters
        ----------
        bl_decode_resp : bool
            Decode responses flag to validate

        Raises
        ------
        ValueError
            If decode_resp is not a boolean
        """
        if not isinstance(bl_decode_resp, bool):
            raise ValueError("decode_responses must be a boolean")

    @staticmethod
    def _load_config(str_host: str, int_port: int, bl_decode_resp: bool) -> ReturnLoadConfig:
        """Load Redis configuration parameters.

        Parameters
        ----------
        str_host : str
            Redis server hostname
        int_port : int
            Redis server port number
        bl_decode_resp : bool
            Whether to decode responses from bytes to strings

        Returns
        -------
        ReturnLoadConfig
            Configuration dictionary for Redis connection
        """
        return {
            "host": str_host,
            "port": int_port,
            "decode_responses": bl_decode_resp
        }

    @classmethod
    def _connect(cls, str_host: str, int_port: int, bl_decode_resp: bool) -> redis.StrictRedis:
        """Establish a connection to Redis.

        Parameters
        ----------
        str_host : str
            Redis server hostname
        int_port : int
            Redis server port number
        bl_decode_resp : bool
            Whether to decode responses from bytes to strings

        Returns
        -------
        redis.StrictRedis
            Redis client instance

        Raises
        ------
        redis.RedisError
            If connection to Redis server fails
        """
        config = cls._load_config(str_host, int_port, bl_decode_resp)
        try:
            return redis.StrictRedis(**config)
        except Exception as err:
            raise redis.RedisError(f"Failed to connect to Redis at {str_host}:{int_port}") from err

    @classmethod
    def get(cls) -> redis.StrictRedis:
        """Retrieve the Redis client instance.

        Returns
        -------
        redis.StrictRedis
            Singleton Redis client instance

        Raises
        ------
        RuntimeError
            If Redis client instance is not initialized
        """
        instance = cls()
        if not hasattr(instance, "_redis_client") or instance._redis_client is None:
            raise RuntimeError("Redis client not initialized")
        return instance._redis_client