"""Unit tests for S3Client class in AWS S3 client interface.

This module tests the S3Client functionality including initialization, file uploads,
downloads, object listing, and error handling with various input scenarios and edge cases.
"""

from io import BytesIO
from logging import Logger
import sys
from typing import Optional
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError
import pytest
from pytest_mock import MockerFixture

from stpstone.utils.connections.clouds.aws_s3 import ReturnDownloadFile, S3Client
from stpstone.utils.loggs.create_logs import CreateLog


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_boto3_client(mocker: MockerFixture) -> Mock:
    """Mock boto3.client for S3 operations.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    Mock
        Mocked boto3 client
    """
    mock_client = Mock()
    mocker.patch("boto3.client", return_value=mock_client)
    return mock_client


@pytest.fixture
def s3_client() -> S3Client:
    """Fixture providing S3Client instance with valid credentials.

    Returns
    -------
    S3Client
        Instance initialized with test credentials
    """
    return S3Client(
        aws_access_key_id="test_key",
        aws_secret_access_key="test_secret", # noqa S106: Test credentials, not actual secrets
        s3_bucket="test_bucket",
        region_name="us-west-1"
    )


@pytest.fixture
def sample_data() -> BytesIO:
    """Fixture providing sample BytesIO data for testing.

    Returns
    -------
    BytesIO
        Sample binary data
    """
    return BytesIO(b"test data")


@pytest.fixture
def mock_logger(mocker: MockerFixture) -> Logger:
    """Fixture providing a mock logger instance.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    Logger
        Mocked logger instance
    """
    return mocker.create_autospec(Logger)


# --------------------------
# Tests for Initialization
# --------------------------
def test_init_valid_credentials(mocker: Mock) -> None:
    """Test S3Client initialization with valid credentials.

    Verifies
    --------
    - Instance is created successfully
    - boto3.client is called with correct parameters
    - Internal _envs dictionary is set correctly

    Parameters
    ----------
    mocker : Mock
        Mocked boto3 client

    Returns
    -------
    None
    """
    # Mock boto3.client and capture the mock
    mock_boto3_client_func = mocker.patch("boto3.client")
    
    # noqa S106: Test credentials, not actual secrets
    client = S3Client(
        aws_access_key_id="key",
        aws_secret_access_key="secret", # noqa S106: Test credentials, not actual secrets
        s3_bucket="bucket",
        region_name="us-west-1",
        datalake_path="path",
        bool_debug_mode=True
    )
    
    assert client._envs["aws_access_key_id"] == "key"
    # noqa S105: Test secret, not actual credential
    assert client._envs["aws_secret_access_key"] == "secret" # noqa S106
    assert client._envs["s3_bucket"] == "bucket"
    assert client._envs["region_name"] == "us-west-1"
    assert client._envs["datalake"] == "path"
    assert client.bool_debug_mode is True
    
    # Assert that boto3.client was called with correct parameters
    mock_boto3_client_func.assert_called_once_with(
        "s3",
        aws_access_key_id="key",
        aws_secret_access_key="secret", # noqa S106: Test credentials, not actual secrets
        region_name="us-west-1"
    )


@pytest.mark.parametrize(
    "key,secret,bucket,type_error,error_msg",
    [
        ("", "secret", "bucket", ValueError, "Missing required credential"),
        ("key", "", "bucket", ValueError, "Missing required credential"),
        ("key", "secret", "", ValueError, "Missing required credential"),
        (None, "secret", "bucket", TypeError, "must be of type"),
        ("key", None, "bucket", TypeError, "must be of type"),
        ("key", "secret", None, TypeError, "must be of type"),
    ]
)
def test_init_missing_credentials(
    key: Optional[str],
    secret: Optional[str],
    bucket: Optional[str],
    type_error: type,
    error_msg: str
) -> None:
    """Test S3Client initialization with missing credentials raises ValueError or TypeError.

    Verifies
    --------
    - Raises ValueError for empty credentials
    - Raises TypeError for None credentials
    - Error message contains the expected text

    Parameters
    ----------
    key : Optional[str]
        AWS access key ID
    secret : Optional[str]
        AWS secret access key
    bucket : Optional[str]
        S3 bucket name
    type_error : type
        Expected exception type
    error_msg : str
        Expected error message substring

    Returns
    -------
    None
    """
    # noqa S106: Test credentials, not actual secrets
    with pytest.raises(type_error, match=error_msg):
        S3Client(
            aws_access_key_id=key,
            aws_secret_access_key=secret,
            s3_bucket=bucket
        )


# --------------------------
# Tests for _validate_credentials
# --------------------------
def test_validate_credentials_valid(s3_client: S3Client) -> None:
    """Test _validate_credentials with valid inputs.

    Verifies
    --------
    - No exception is raised when all credentials are provided
    - Method executes without errors

    Parameters
    ----------
    s3_client : S3Client
        S3Client instance from fixture

    Returns
    -------
    None
    """
    s3_client._validate_credentials()  # Should not raise


@pytest.mark.parametrize(
    "env_dict",
    [
        {"aws_access_key_id": "", "aws_secret_access_key": "secret", "s3_bucket": "bucket"},
        {"aws_access_key_id": "key", "aws_secret_access_key": "", "s3_bucket": "bucket"},
        {"aws_access_key_id": "key", "aws_secret_access_key": "secret", "s3_bucket": ""}
    ]
)
def test_validate_credentials_invalid(s3_client: S3Client, env_dict: dict[str, str]) -> None:
    """Test _validate_credentials with invalid credentials raises ValueError.

    Verifies
    --------
    - Raises ValueError for empty credentials
    - Error message contains the missing credential name

    Parameters
    ----------
    s3_client : S3Client
        S3Client instance from fixture
    env_dict : dict[str, str]
        Dictionary with invalid credential values

    Returns
    -------
    None
    """
    s3_client._envs = env_dict
    with pytest.raises(ValueError, match="Missing required credential"):
        s3_client._validate_credentials()


# --------------------------
# Tests for handle_error
# --------------------------
@pytest.mark.parametrize(
    "exception,expected_message",
    [
        (
            NoCredentialsError(),
            "Credentials not found, please reconfigure your AWS credentials properly."
        ),
        (
            ClientError({"Error": {"Code": "NoSuchKey"}}, "operation"),
            "The file test_key was not found in bucket test_bucket."
        ),
        (
            ClientError({"Error": {"Code": "OtherError"}}, "operation"),
            "AWS Client Error: "
        ),
        (
            EndpointConnectionError(endpoint_url="url"),
            "Failed to connect to AWS endpoint. Check internet connection."
        ),
        (
            FileNotFoundError("file.txt"),
            "The file test_key was not found locally."
        ),
        (
            ValueError("generic error"),
            "Unexpected error during test_action: generic error"
        )
    ]
)
def test_handle_error(
    s3_client: S3Client,
    mock_logger: Logger,
    exception: Exception,
    expected_message: str
) -> None:
    """Test handle_error method with various exception types.

    Verifies
    --------
    - Correct error message is generated for each exception type
    - Logger is called with correct message and level
    - Handles both specific and generic exceptions

    Parameters
    ----------
    s3_client : S3Client
        S3Client instance from fixture
    mock_logger : Logger
        Mocked logger instance from fixture
    exception : Exception
        Exception to test
    expected_message : str
        Expected error message substring

    Returns
    -------
    None
    """
    s3_client.logger = mock_logger
    with patch.object(CreateLog, "log_message") as mock_log:
        s3_client.handle_error(exception, action="test_action", s3_key="test_key")
        mock_log.assert_called_once()
        args, _ = mock_log.call_args
        assert args[0] == mock_logger
        assert args[2] == "critical"
        assert expected_message in args[1]


# --------------------------
# Tests for upload_file
# --------------------------
def test_upload_file_success(
    s3_client: S3Client,
    sample_data: BytesIO,
    mock_boto3_client: Mock
) -> None:
    """Test successful file upload to S3.

    Verifies
    --------
    - Upload returns True
    - boto3 put_object is called with correct parameters
    - No errors are logged

    Parameters
    ----------
    s3_client : S3Client
        S3Client instance from fixture
    sample_data : BytesIO
        Sample data for upload
    mock_boto3_client : Mock
        Mocked boto3 client

    Returns
    -------
    None
    """
    s3_client.s3 = mock_boto3_client
    result = s3_client.upload_file(sample_data, "test_key")
    assert result is True
    mock_boto3_client.put_object.assert_called_once_with(
        Body=sample_data.getvalue(),
        Bucket="test_bucket",
        Key="test_key"
    )


def test_upload_file_error(
    s3_client: S3Client,
    sample_data: BytesIO,
    mock_boto3_client: Mock
) -> None:
    """Test upload_file with error handling.

    Verifies
    --------
    - Returns False on error
    - Error is handled through handle_error
    - boto3 put_object raises exception

    Parameters
    ----------
    s3_client : S3Client
        S3Client instance from fixture
    sample_data : BytesIO
        Sample data for upload
    mock_boto3_client : Mock
        Mocked boto3 client

    Returns
    -------
    None
    """
    mock_boto3_client.put_object.side_effect = ClientError(
        {"Error": {"Code": "Error"}},
        "put_object"
    )
    s3_client.s3 = mock_boto3_client
    with patch.object(s3_client, "handle_error") as mock_handle:
        result = s3_client.upload_file(sample_data, "test_key")
        assert result is False
        mock_handle.assert_called_once()


# --------------------------
# Tests for download_file
# --------------------------
def test_download_file_success(s3_client: S3Client, mock_boto3_client: Mock) -> None:
    """Test successful file download from S3.

    Verifies
    --------
    - Returns correct file data and metadata
    - boto3 get_object is called with correct parameters
    - Debug print occurs when debug mode is enabled

    Parameters
    ----------
    s3_client : S3Client
        S3Client instance from fixture
    mock_boto3_client : Mock
        Mocked boto3 client

    Returns
    -------
    None
    """
    s3_client.bool_debug_mode = True
    s3_client.s3 = mock_boto3_client
    expected_result: ReturnDownloadFile = {"Body": BytesIO(b"data"), "Metadata": {}}
    mock_boto3_client.get_object.return_value = expected_result
    with patch("builtins.print") as mock_print:
        result = s3_client.download_file("test_key")
        assert result == expected_result
        mock_boto3_client.get_object.assert_called_once_with(
            Bucket="test_bucket",
            Key="test_key"
        )
        mock_print.assert_called_once_with("Successfully downloaded test_key")


def test_download_file_error(s3_client: S3Client, mock_boto3_client: Mock) -> None:
    """Test download_file with error handling.

    Verifies
    --------
    - Returns None on error
    - Error is handled through handle_error
    - boto3 get_object raises exception

    Parameters
    ----------
    s3_client : S3Client
        S3Client instance from fixture
    mock_boto3_client : Mock
        Mocked boto3 client

    Returns
    -------
    None
    """
    mock_boto3_client.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey"}},
        "get_object"
    )
    s3_client.s3 = mock_boto3_client
    with patch.object(s3_client, "handle_error") as mock_handle:
        result = s3_client.download_file("test_key")
        assert result is None
        mock_handle.assert_called_once()


# --------------------------
# Tests for list_objects
# --------------------------
def test_list_objects_success(s3_client: S3Client, mock_boto3_client: Mock) -> None:
    """Test successful listing of S3 objects.

    Verifies
    --------
    - Returns list of objects
    - boto3 list_objects is called with correct parameters
    - Returns empty list if no Contents in response

    Parameters
    ----------
    s3_client : S3Client
        S3Client instance from fixture
    mock_boto3_client : Mock
        Mocked boto3 client

    Returns
    -------
    None
    """
    s3_client.s3 = mock_boto3_client
    expected_result = [{"Key": "file1"}, {"Key": "file2"}]
    mock_boto3_client.list_objects.return_value = {"Contents": expected_result}
    result = s3_client.list_objects("prefix/")
    assert result == expected_result
    mock_boto3_client.list_objects.assert_called_once_with(
        Bucket="test_bucket",
        Prefix="prefix/"
    )


def test_list_objects_no_contents(s3_client: S3Client, mock_boto3_client: Mock) -> None:
    """Test list_objects when no objects are found.

    Verifies
    --------
    - Returns empty list when Contents is missing
    - boto3 list_objects is called with correct parameters

    Parameters
    ----------
    s3_client : S3Client
        S3Client instance from fixture
    mock_boto3_client : Mock
        Mocked boto3 client

    Returns
    -------
    None
    """
    s3_client.s3 = mock_boto3_client
    mock_boto3_client.list_objects.return_value = {}
    result = s3_client.list_objects("prefix/")
    assert result == []
    mock_boto3_client.list_objects.assert_called_once_with(
        Bucket="test_bucket",
        Prefix="prefix/"
    )


def test_list_objects_error(s3_client: S3Client, mock_boto3_client: Mock) -> None:
    """Test list_objects with error handling.

    Verifies
    --------
    - Returns None on error
    - Error is handled through handle_error
    - boto3 list_objects raises exception

    Parameters
    ----------
    s3_client : S3Client
        S3Client instance from fixture
    mock_boto3_client : Mock
        Mocked boto3 client

    Returns
    -------
    None
    """
    mock_boto3_client.list_objects.side_effect = ClientError(
        {"Error": {"Code": "Error"}},
        "list_objects"
    )
    s3_client.s3 = mock_boto3_client
    with patch.object(s3_client, "handle_error") as mock_handle:
        result = s3_client.list_objects("prefix/")
        assert result is None
        mock_handle.assert_called_once()


# --------------------------
# Edge Cases and Type Validation
# --------------------------
def test_upload_file_empty_data(s3_client: S3Client, mock_boto3_client: Mock) -> None:
    """Test upload_file with empty BytesIO data.

    Verifies
    --------
    - Upload succeeds with empty data
    - boto3 put_object is called with empty bytes

    Parameters
    ----------
    s3_client : S3Client
        S3Client instance from fixture
    mock_boto3_client : Mock
        Mocked boto3 client

    Returns
    -------
    None
    """
    s3_client.s3 = mock_boto3_client
    empty_data = BytesIO(b"")
    result = s3_client.upload_file(empty_data, "test_key")
    assert result is True
    mock_boto3_client.put_object.assert_called_once_with(
        Body=b"",
        Bucket="test_bucket",
        Key="test_key"
    )


def test_download_file_empty_key(s3_client: S3Client, mock_boto3_client: Mock) -> None:
    """Test download_file with empty key.

    Verifies
    --------
    - Returns None for empty key
    - Error is handled through handle_error

    Parameters
    ----------
    s3_client : S3Client
        S3Client instance from fixture
    mock_boto3_client : Mock
        Mocked boto3 client

    Returns
    -------
    None
    """
    mock_boto3_client.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey"}},
        "get_object"
    )
    s3_client.s3 = mock_boto3_client
    with patch.object(s3_client, "handle_error") as mock_handle:
        result = s3_client.download_file("")
        assert result is None
        mock_handle.assert_called_once()


def test_list_objects_empty_prefix(s3_client: S3Client, mock_boto3_client: Mock) -> None:
    """Test list_objects with empty prefix.

    Verifies
    --------
    - Returns list of objects for empty prefix
    - boto3 list_objects is called with empty prefix

    Parameters
    ----------
    s3_client : S3Client
        S3Client instance from fixture
    mock_boto3_client : Mock
        Mocked boto3 client

    Returns
    -------
    None
    """
    s3_client.s3 = mock_boto3_client
    expected_result = [{"Key": "file1"}]
    mock_boto3_client.list_objects.return_value = {"Contents": expected_result}
    result = s3_client.list_objects("")
    assert result == expected_result
    mock_boto3_client.list_objects.assert_called_once_with(
        Bucket="test_bucket",
        Prefix=""
    )


# --------------------------
# Reload Logic
# --------------------------
def test_module_reload(mocker: MockerFixture) -> None:
    """Test module reloading behavior.

    Verifies
    --------
    - Module can be reloaded without errors
    - S3Client class is available after reload
    - Instance can be created after reload

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    import importlib
    # noqa S106: Test credentials, not actual secrets
    mocker.patch("boto3.client", return_value=Mock())
    importlib.reload(sys.modules["stpstone.utils.connections.clouds.aws_s3"])
    from stpstone.utils.connections.clouds.aws_s3 import S3Client
    client = S3Client("key", "secret", "bucket")
    assert isinstance(client, S3Client)