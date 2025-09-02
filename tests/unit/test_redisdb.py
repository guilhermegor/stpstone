"""Unit tests for RedisClient class.

Tests the Redis connection management functionality including:
- Singleton pattern implementation
- Connection configuration validation
- Redis client initialization
- Error handling and edge cases
"""

from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest
import redis

from stpstone.utils.connections.databases.nosql.redisdb import RedisClient, ReturnLoadConfig


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def default_config() -> ReturnLoadConfig:
    """Fixture providing default Redis configuration.

    Returns
    -------
    ReturnLoadConfig
        Default configuration with localhost:6379 and decode_responses=True
    """
    return {
        "host": "localhost",
        "port": 6379,
        "decode_responses": True
    }


@pytest.fixture
def mock_redis_client() -> Mock:
    """Fixture providing a mock Redis client.

    Returns
    -------
    Mock
        Mock object simulating redis.StrictRedis
    """
    return Mock(spec=redis.StrictRedis)


@pytest.fixture
def redis_client_instance(default_config: ReturnLoadConfig) -> RedisClient:
    """Fixture providing a RedisClient instance with default config.

    Parameters
    ----------
    default_config : ReturnLoadConfig
        Default Redis configuration

    Returns
    -------
    RedisClient
        RedisClient instance with default configuration
    """
    # Clear any existing instance
    RedisClient._instance = None
    return RedisClient(
        str_host=default_config["host"],
        int_port=default_config["port"],
        bl_decode_resp=default_config["decode_responses"]
    )


@pytest.fixture
def redis_client_custom_config() -> RedisClient:
    """Fixture providing RedisClient with custom configuration.

    Returns
    -------
    RedisClient
        RedisClient instance with custom host, port, and decode_responses=False
    """
    RedisClient._instance = None
    return RedisClient(str_host="redis.example.com", int_port=6380, bl_decode_resp=False)


# --------------------------
# Tests for Validation Methods
# --------------------------
class TestValidationMethods:
    """Test cases for RedisClient validation methods."""

    @pytest.mark.parametrize("invalid_host", [None, "", "   ", 123, []])
    def test_validate_host_invalid(self, invalid_host: Any) -> None:
        """Test _validate_host with invalid inputs.

        Verifies
        --------
        That invalid host values raise ValueError with appropriate message.

        Parameters
        ----------
        invalid_host : Any
            Invalid host values to test

        Returns
        -------
        None
        """
        if invalid_host in ["", "   "]:
            with pytest.raises(ValueError, match="Redis host cannot be empty or whitespace-only"):
                RedisClient._validate_host(None, invalid_host)
        else:
            with pytest.raises(TypeError, match="must be of type"):
                RedisClient._validate_host(None, invalid_host)

    def test_validate_host_valid(self) -> None:
        """Test _validate_host with valid input.

        Verifies
        --------
        That valid host string passes validation without raising exceptions.

        Returns
        -------
        None
        """
        # Should not raise any exception
        RedisClient._validate_host(None, "localhost")
        RedisClient._validate_host(None, "redis.example.com")
        RedisClient._validate_host(None, "192.168.1.1")

    @pytest.mark.parametrize("invalid_port", [None, "6379", 0, 65536, -1, 70000])
    def test_validate_port_invalid(self, invalid_port: Any) -> None:
        """Test _validate_port with invalid inputs.

        Verifies
        --------
        That invalid port values raise ValueError with appropriate message.

        Parameters
        ----------
        invalid_port : Any
            Invalid port values to test

        Returns
        -------
        None
        """
        if isinstance(invalid_port, str) or invalid_port is None:
            with pytest.raises(TypeError, match="must be of type"):
                RedisClient._validate_port(None, invalid_port)
        else:
            with pytest.raises(ValueError, match="Redis port must be between 1 and"):
                RedisClient._validate_port(None, invalid_port)

    @pytest.mark.parametrize("valid_port", [1, 6379, 8080, 65535])
    def test_validate_port_valid(self, valid_port: int) -> None:
        """Test _validate_port with valid inputs.

        Verifies
        --------
        That valid port integers pass validation without raising exceptions.

        Parameters
        ----------
        valid_port : int
            Valid port numbers to test

        Returns
        -------
        None
        """
        # Should not raise any exception
        RedisClient._validate_port(None, valid_port)

    @pytest.mark.parametrize("invalid_decode_resp", [None, "true", 1, 0, "False"])
    def test_validate_decode_resp_invalid(self, invalid_decode_resp: Any) -> None:
        """Test _validate_decode_resp with invalid inputs.

        Verifies
        --------
        That invalid decode_responses values raise ValueError with appropriate message.

        Parameters
        ----------
        invalid_decode_resp : Any
            Invalid decode_responses values to test

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be of type"):
            RedisClient._validate_decode_resp(None, invalid_decode_resp)

    @pytest.mark.parametrize("valid_decode_resp", [True, False])
    def test_validate_decode_resp_valid(self, valid_decode_resp: bool) -> None:
        """Test _validate_decode_resp with valid inputs.

        Verifies
        --------
        That valid boolean values pass validation without raising exceptions.

        Parameters
        ----------
        valid_decode_resp : bool
            Valid boolean values to test

        Returns
        -------
        None
        """
        # Should not raise any exception
        RedisClient._validate_decode_resp(None, valid_decode_resp)


# --------------------------
# Tests for Configuration Methods
# --------------------------
class TestConfigurationMethods:
    """Test cases for RedisClient configuration methods."""

    def test_load_config_default(self) -> None:
        """Test _load_config with default parameters.

        Verifies
        --------
        That _load_config returns correct configuration dictionary
        with default parameter values.

        Returns
        -------
        None
        """
        config = RedisClient._load_config("localhost", 6379, True)
        expected = {
            "host": "localhost",
            "port": 6379,
            "decode_responses": True
        }
        assert config == expected
        assert isinstance(config, dict)

    def test_load_config_custom(self) -> None:
        """Test _load_config with custom parameters.

        Verifies
        --------
        That _load_config returns correct configuration dictionary
        with custom parameter values.

        Returns
        -------
        None
        """
        config = RedisClient._load_config("redis.example.com", 6380, False)
        expected = {
            "host": "redis.example.com",
            "port": 6380,
            "decode_responses": False
        }
        assert config == expected


# --------------------------
# Tests for Connection Methods
# --------------------------
class TestConnectionMethods:
    """Test cases for RedisClient connection methods."""

    @patch("stpstone.utils.connections.databases.nosql.redisdb.redis.StrictRedis")
    def test_connect_success(self, mock_strict_redis: Mock, default_config: ReturnLoadConfig) -> None:
        """Test _connect method with successful connection.

        Verifies
        --------
        That _connect method successfully creates Redis client
        with correct configuration parameters.

        Parameters
        ----------
        mock_strict_redis : Mock
            Mock for redis.StrictRedis class
        default_config : ReturnLoadConfig
            Default Redis configuration

        Returns
        -------
        None
        """
        mock_client = Mock()
        mock_strict_redis.return_value = mock_client

        result = RedisClient._connect(
            default_config["host"],
            default_config["port"],
            default_config["decode_responses"]
        )

        mock_strict_redis.assert_called_once_with(**default_config)
        assert result == mock_client

    @patch("stpstone.utils.connections.databases.nosql.redisdb.redis.StrictRedis")
    def test_connect_failure(self, mock_strict_redis: Mock) -> None:
        """Test _connect method with connection failure.

        Verifies
        --------
        That _connect method raises redis.RedisError when connection fails.

        Parameters
        ----------
        mock_strict_redis : Mock
            Mock for redis.StrictRedis class configured to raise exception

        Returns
        -------
        None
        """
        mock_strict_redis.side_effect = redis.ConnectionError("Connection failed")

        with pytest.raises(redis.RedisError, match="Failed to connect to Redis"):
            RedisClient._connect("invalid-host", 6379, True)


# --------------------------
# Tests for Singleton Pattern
# --------------------------
class TestSingletonPattern:
    """Test cases for RedisClient singleton pattern implementation."""

    def test_singleton_creation(self, redis_client_instance: RedisClient) -> None:
        """Test that only one instance is created.

        Verifies
        --------
        That RedisClient follows singleton pattern and returns
        the same instance on subsequent calls.

        Parameters
        ----------
        redis_client_instance : RedisClient
            RedisClient instance from fixture

        Returns
        -------
        None
        """
        instance1 = RedisClient()
        instance2 = RedisClient()
        instance3 = RedisClient("different-host", 6380, False)

        assert instance1 is instance2
        assert instance2 is instance3
        assert instance1 is redis_client_instance

    def test_singleton_different_config_ignored(self, redis_client_instance: RedisClient) -> None:
        """Test that different configs are ignored for singleton.

        Verifies
        --------
        That subsequent RedisClient creations with different configuration
        parameters return the original instance with original config.

        Parameters
        ----------
        redis_client_instance : RedisClient
            RedisClient instance from fixture with default config

        Returns
        -------
        None
        """
        # Create instance with default config
        default_instance = RedisClient()

        # Try to create with different config - should return same instance
        different_instance = RedisClient("different-host", 9999, False)

        assert default_instance is different_instance
        # Configuration should remain as original (first created)
        assert default_instance.str_host == "localhost"
        assert default_instance.int_port == 6379
        assert default_instance.bl_decode_resp is True


# --------------------------
# Tests for Initialization
# --------------------------
class TestInitialization:
    """Test cases for RedisClient initialization."""

    def test_init_with_valid_parameters(self, default_config: ReturnLoadConfig) -> None:
        """Test initialization with valid parameters.

        Verifies
        --------
        That RedisClient can be initialized with valid parameters
        and stores them correctly in instance attributes.

        Parameters
        ----------
        default_config : ReturnLoadConfig
            Default Redis configuration

        Returns
        -------
        None
        """
        RedisClient._instance = None
        client = RedisClient(
            str_host=default_config["host"],
            int_port=default_config["port"],
            bl_decode_resp=default_config["decode_responses"]
        )

        assert client.str_host == default_config["host"]
        assert client.int_port == default_config["port"]
        assert client.bl_decode_resp == default_config["decode_responses"]

    def test_init_with_custom_parameters(self) -> None:
        """Test initialization with custom parameters.

        Verifies
        --------
        That RedisClient can be initialized with custom parameters
        and stores them correctly in instance attributes.

        Returns
        -------
        None
        """
        RedisClient._instance = None
        client = RedisClient(str_host="redis.example.com", int_port=6380, bl_decode_resp=False)

        assert client.str_host == "redis.example.com"
        assert client.int_port == 6380
        assert client.bl_decode_resp is False

    @pytest.mark.parametrize("invalid_host", [None, "", 123])
    def test_init_invalid_host_raises_error(self, invalid_host: Any) -> None:
        """Test initialization with invalid host raises error.

        Verifies
        --------
        That RedisClient initialization with invalid host parameter
        raises ValueError during validation.

        Parameters
        ----------
        invalid_host : Any
            Invalid host values to test

        Returns
        -------
        None
        """
        RedisClient._instance = None
        if invalid_host in ["", "   "]:
            with pytest.raises(ValueError, match="Redis host cannot be empty or whitespace-only"):
                RedisClient(str_host=invalid_host, int_port=6379, bl_decode_resp=True)
        else:
            with pytest.raises(TypeError, match="must be of type"):
                RedisClient(str_host=invalid_host, int_port=6379, bl_decode_resp=True)

    @pytest.mark.parametrize("invalid_port", [None, "6379", 0, 65536])
    def test_init_invalid_port_raises_error(self, invalid_port: Any) -> None:
        """Test initialization with invalid port raises error.

        Verifies
        --------
        That RedisClient initialization with invalid port parameter
        raises ValueError during validation.

        Parameters
        ----------
        invalid_port : Any
            Invalid port values to test

        Returns
        -------
        None
        """
        RedisClient._instance = None
        if isinstance(invalid_port, str) or invalid_port is None:
            with pytest.raises(TypeError, match="must be of type"):
                RedisClient(str_host="localhost", int_port=invalid_port, bl_decode_resp=True)
        else:
            with pytest.raises(ValueError, match="Redis port must be between 1 and"):
                RedisClient(str_host="localhost", int_port=invalid_port, bl_decode_resp=True)

    @pytest.mark.parametrize("invalid_decode_resp", [None, "true", 1])
    def test_init_invalid_decode_resp_raises_error(self, invalid_decode_resp: Any) -> None:
        """Test initialization with invalid decode_resp raises error.

        Verifies
        --------
        That RedisClient initialization with invalid decode_responses parameter
        raises ValueError during validation.

        Parameters
        ----------
        invalid_decode_resp : Any
            Invalid decode_responses values to test

        Returns
        -------
        None
        """
        RedisClient._instance = None
        with pytest.raises(TypeError, match="must be of type"):
            RedisClient(str_host="localhost", int_port=6379, bl_decode_resp=invalid_decode_resp)


# --------------------------
# Tests for Get Method
# --------------------------
class TestGetMethod:
    """Test cases for RedisClient get method."""

    def test_get_returns_redis_client(self, redis_client_instance: RedisClient, mock_redis_client: Mock) -> None:
        """Test that get method returns Redis client instance.

        Verifies
        --------
        That get method returns the underlying redis.StrictRedis client
        when it has been properly initialized.

        Parameters
        ----------
        redis_client_instance : RedisClient
            RedisClient instance from fixture
        mock_redis_client : Mock
            Mock Redis client

        Returns
        -------
        None
        """
        # Set the mock client on the instance
        redis_client_instance._redis_client = mock_redis_client

        result = RedisClient.get()

        assert result == mock_redis_client
        assert hasattr(result, "ping")  # Verify it has Redis client methods

    def test_get_without_initialization_raises_error(self) -> None:
        """Test get method raises error when client not initialized.

        Verifies
        --------
        That get method raises RuntimeError when Redis client
        has not been properly initialized.

        Returns
        -------
        None
        """
        # Clear instance and ensure no client exists
        RedisClient._instance = None
        
        # Create a minimal instance without calling _connect
        instance = RedisClient.__new__(RedisClient)
        RedisClient._instance = instance
        # Don't set _redis_client to simulate initialization failure

        with pytest.raises(RuntimeError, match="Redis client not initialized"):
            RedisClient.get()

    def test_get_with_none_client_raises_error(self, redis_client_instance: RedisClient) -> None:
        """Test get method raises error when client is None.

        Verifies
        --------
        That get method raises RuntimeError when _redis_client
        attribute exists but is None.

        Parameters
        ----------
        redis_client_instance : RedisClient
            RedisClient instance from fixture

        Returns
        -------
        None
        """
        redis_client_instance._redis_client = None

        with pytest.raises(RuntimeError, match="Redis client not initialized"):
            RedisClient.get()


# --------------------------
# Tests for Error Conditions
# --------------------------
class TestErrorConditions:
    """Test cases for RedisClient error conditions."""

    @patch("stpstone.utils.connections.databases.nosql.redisdb.redis.StrictRedis")
    def test_new_connection_failure_raises_runtime_error(self, mock_strict_redis: Mock) -> None:
        """Test that connection failure in __new__ raises RuntimeError.

        Verifies
        --------
        That when _connect fails during singleton creation,
        a RuntimeError is raised with appropriate message.

        Parameters
        ----------
        mock_strict_redis : Mock
            Mock for redis.StrictRedis configured to raise exception

        Returns
        -------
        None
        """
        mock_strict_redis.side_effect = redis.ConnectionError("Connection refused")

        RedisClient._instance = None

        with pytest.raises(RuntimeError, match="Failed to create Redis client instance"):
            RedisClient()

    def test_singleton_instance_persistence_across_tests(self) -> None:
        """Test that singleton instance persists across test runs.

        Verifies
        --------
        That the singleton instance behavior works correctly
        when tested multiple times.

        Returns
        -------
        None
        """
        # Clear instance first
        RedisClient._instance = None

        instance1 = RedisClient()
        instance2 = RedisClient()

        assert instance1 is instance2

    @patch("stpstone.utils.connections.databases.nosql.redisdb.redis.StrictRedis")
    def test_connection_error_chaining(self, mock_strict_redis: Mock) -> None:
        """Test that original exception is chained in RuntimeError.

        Verifies
        --------
        That when connection fails, the original redis.RedisError
        is properly chained in the raised RuntimeError.

        Parameters
        ----------
        mock_strict_redis : Mock
            Mock for redis.StrictRedis configured to raise exception

        Returns
        -------
        None
        """
        original_error = redis.ConnectionError("Connection refused")
        mock_strict_redis.side_effect = original_error

        RedisClient._instance = None

        with pytest.raises(RuntimeError, match="Failed to create Redis client instance") as exc_info:
            RedisClient("localhost", 6379, True)  # Provide all required arguments

        assert exc_info.value.__cause__ is original_error

# --------------------------
# Tests for Edge Cases
# --------------------------
class TestEdgeCases:
    """Test cases for RedisClient edge cases."""

    def test_extreme_port_values(self) -> None:
        """Test initialization with extreme port values.

        Verifies
        --------
        That RedisClient handles port number boundary conditions correctly.

        Returns
        -------
        None
        """
        RedisClient._instance = None

        # Test minimum valid port
        client_min = RedisClient(str_host="localhost", int_port=1, bl_decode_resp=True)
        assert client_min.int_port == 1

        RedisClient._instance = None

        # Test maximum valid port
        client_max = RedisClient(str_host="localhost", int_port=65535, bl_decode_resp=True)
        assert client_max.int_port == 65535

    def test_unicode_hostname(self) -> None:
        """Test initialization with Unicode hostname.

        Verifies
        --------
        That RedisClient handles Unicode characters in hostnames correctly.

        Returns
        -------
        None
        """
        RedisClient._instance = None

        unicode_host = "rédis.example.com"
        client = RedisClient(str_host=unicode_host, int_port=6379, bl_decode_resp=True)
        assert client.str_host == unicode_host

    def test_boolean_edge_cases(self) -> None:
        """Test initialization with boolean edge cases.

        Verifies
        --------
        That RedisClient handles both True and False values correctly
        for decode_responses parameter.

        Returns
        -------
        None
        """
        RedisClient._instance = None

        client_true = RedisClient(str_host="localhost", int_port=6379, bl_decode_resp=True)
        assert client_true.bl_decode_resp is True

        RedisClient._instance = None

        client_false = RedisClient(str_host="localhost", int_port=6379, bl_decode_resp=False)
        assert client_false.bl_decode_resp is False


# --------------------------
# Tests for Type Validation
# --------------------------
class TestTypeValidation:
    """Test cases for RedisClient type validation."""

    def test_instance_type(self, redis_client_instance: RedisClient) -> None:
        """Test that created instance is of correct type.

        Verifies
        --------
        That RedisClient instance is properly typed and has expected attributes.

        Parameters
        ----------
        redis_client_instance : RedisClient
            RedisClient instance from fixture

        Returns
        -------
        None
        """
        assert isinstance(redis_client_instance, RedisClient)
        assert hasattr(redis_client_instance, "str_host")
        assert hasattr(redis_client_instance, "int_port")
        assert hasattr(redis_client_instance, "bl_decode_resp")
        assert hasattr(redis_client_instance, "_redis_client")

    def test_config_type(self) -> None:
        """Test that _load_config returns correct type.

        Verifies
        --------
        That _load_config method returns a dictionary with correct
        types for all values.

        Returns
        -------
        None
        """
        config = RedisClient._load_config("localhost", 6379, True)

        assert isinstance(config, dict)
        assert isinstance(config["host"], str)
        assert isinstance(config["port"], int)
        assert isinstance(config["decode_responses"], bool)