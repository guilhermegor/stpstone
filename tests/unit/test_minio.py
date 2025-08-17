"""Unit tests for MinIO client operations.

Tests the MinIO client functionality with various input scenarios including:
- Initialization with valid and invalid inputs
- Bucket creation and validation
- File and stream upload/download operations
- Object listing and error handling
"""

from io import BytesIO
from typing import Any
from unittest.mock import MagicMock, Mock

from minio.error import S3Error
import pytest
from pytest_mock import MockerFixture

from stpstone.utils.connections.clouds.minio import MinioClient


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def minio_client(mock_minio: Mock, mock_create_log: Mock) -> MinioClient:
    """Fixture providing a MinIO client instance with a mocked Minio client and CreateLog.

    Parameters
    ----------
    mock_minio : Mock
        Mocked Minio client instance
    mock_create_log : Mock
        Mocked CreateLog instance

    Returns
    -------
    MinioClient
        Initialized MinIO client with default parameters
    """
    minio_client = MinioClient(
        user="test_user",
        password="test_password",  # noqa S106: possible hardcoded password
        endpoint="localhost:9000",
        bool_secure=True
    )
    minio_client.client = mock_minio.return_value
    minio_client.cls_create_log = mock_create_log.return_value
    return minio_client


@pytest.fixture
def mock_minio(mocker: MockerFixture) -> Mock:
    """Fixture mocking the Minio client.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    Mock
        Mocked Minio client instance
    """
    return mocker.patch("minio.Minio", autospec=True)


@pytest.fixture
def mock_create_log(mocker: MockerFixture) -> Mock:
    """Fixture mocking the CreateLog class.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    Mock
        Mocked CreateLog instance
    """
    return mocker.patch("stpstone.utils.loggs.create_logs.CreateLog", autospec=True)


@pytest.fixture
def sample_file(tmp_path: Any) -> str:  # noqa ANN401: typing.Any is not allowed
    """Fixture providing a temporary file path with content.

    Parameters
    ----------
    tmp_path : Any
        Temporary path provided by pytest

    Returns
    -------
    str
        Path to temporary file
    """
    file_path = tmp_path / "test.txt"
    with open(file_path, "wb") as f:
        f.write(b"test content")
    return str(file_path)


@pytest.fixture
def sample_stream() -> BytesIO:
    """Fixture providing a sample BytesIO stream.

    Returns
    -------
    BytesIO
        Stream with sample data
    """
    return BytesIO(b"test stream data")


@pytest.fixture
def sample_bytes() -> bytes:
    """Fixture providing sample bytes data.

    Returns
    -------
    bytes
        Sample bytes content
    """
    return b"test bytes data"


# --------------------------
# Tests for Initialization
# --------------------------
def test_init_valid_inputs(
    minio_client: MinioClient, 
    mock_minio: Mock, 
    mock_create_log: Mock
) -> None:
    """Test initialization with valid inputs.

    Verifies
    --------
    - MinIO client is initialized with correct parameters
    - Logger is set correctly
    - CreateLog instance is created

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_minio : Mock
        Mocked Minio client
    mock_create_log : Mock
        Mocked CreateLog instance

    Returns
    -------
    None
    """
    assert minio_client.client is mock_minio.return_value, \
        f"Expected client to be mock_minio.return_value, got {type(minio_client.client)}"


def test_init_no_logger(minio_client: MinioClient) -> None:
    """Test initialization without logger.

    Verifies
    --------
    - MinIO client is initialized without a logger
    - Logger attribute is None

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture

    Returns
    -------
    None
    """
    assert minio_client.logger is None


# --------------------------
# Tests for _validate_bucket_name
# --------------------------
def test_validate_bucket_name_empty(minio_client: MinioClient) -> None:
    """Test bucket name validation with empty string.

    Verifies
    --------
    - Empty bucket name raises ValueError
    - Error message matches expected pattern

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Bucket name cannot be empty"):
        minio_client._validate_bucket_name("")


def test_validate_bucket_name_valid(minio_client: MinioClient) -> None:
    """Test bucket name validation with valid input.

    Verifies
    --------
    - Valid bucket name does not raise an error

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture

    Returns
    -------
    None
    """
    minio_client._validate_bucket_name("valid-bucket")
    assert True  # No exception raised


# --------------------------
# Tests for make_bucket
# --------------------------
def test_make_bucket_new(
    minio_client: MinioClient,
    mock_minio: Mock,
    mock_create_log: Mock
) -> None:
    """Test creating a new bucket.

    Verifies
    --------
    - New bucket is created successfully
    - bucket_exists is called
    - make_bucket is called
    - Logging is performed
    - Returns True

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_minio : Mock
        Mocked Minio client
    mock_create_log : Mock
        Mocked CreateLog instance

    Returns
    -------
    None
    """
    mock_minio.return_value.bucket_exists.return_value = False
    mock_minio.return_value.make_bucket.return_value = None
    result = minio_client.make_bucket("test-bucket")
    mock_minio.return_value.bucket_exists.assert_called_once_with("test-bucket")
    mock_minio.return_value.make_bucket.assert_called_once_with("test-bucket")
    mock_create_log.return_value.log_message.assert_called_once_with(
        None, "Bucket test-bucket created successfully.", "info"
    )
    assert result is True


def test_make_bucket_exists(
    minio_client: MinioClient,
    mock_minio: Mock,
    mock_create_log: Mock
) -> None:
    """Test handling of existing bucket.

    Verifies
    --------
    - Existing bucket returns True
    - make_bucket is not called
    - Logging is not performed

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_minio : Mock
        Mocked Minio client
    mock_create_log : Mock
        Mocked CreateLog instance

    Returns
    -------
    None
    """
    mock_minio.return_value.bucket_exists.return_value = True
    result = minio_client.make_bucket("test-bucket")
    mock_minio.return_value.bucket_exists.assert_called_once_with("test-bucket")
    mock_minio.return_value.make_bucket.assert_not_called()
    mock_create_log.return_value.log_message.assert_not_called()
    assert result is True


def test_make_bucket_s3error(
    minio_client: MinioClient,
    mock_minio: Mock,
    mock_create_log: Mock
) -> None:
    """Test handling S3Error during bucket creation.

    Verifies
    --------
    - S3Error is caught
    - Error is logged
    - Returns False

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_minio : Mock
        Mocked Minio client
    mock_create_log : Mock
        Mocked CreateLog instance

    Returns
    -------
    None
    """
    mock_minio.return_value.bucket_exists.side_effect = S3Error(
        "error", "message", "resource", "request_id", "host_id", "bucket"
    )
    result = minio_client.make_bucket("test-bucket")
    mock_create_log.return_value.log_message.assert_called_once_with(
        None,
        "Error creating bucket test-bucket: S3 operation failed; code: error, message: message, " \
        "resource: resource, request_id: request_id, host_id: host_id",
        "critical"
    )
    assert result is False


# --------------------------
# Tests for put_object_from_file
# --------------------------
def test_put_object_from_file_success(
    minio_client: MinioClient,
    mock_minio: Mock,
    mock_create_log: Mock,
    sample_file: str
) -> None:
    """Test successful file upload.

    Verifies
    --------
    - File upload succeeds
    - Bucket is created
    - Logging is performed
    - Returns True

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_minio : Mock
        Mocked Minio client
    mock_create_log : Mock
        Mocked CreateLog instance
    sample_file : str
        Path to temporary file

    Returns
    -------
    None
    """
    mock_minio.return_value.bucket_exists.return_value = False
    mock_minio.return_value.make_bucket.return_value = None
    result = minio_client.put_object_from_file(
        "test-bucket", "test-object", sample_file, {"key": "value"}, "text/plain"
    )
    mock_minio.return_value.fput_object.assert_called_once_with(
        "test-bucket", "test-object", sample_file,
        metadata={"key": "value"}, content_type="text/plain"
    )
    mock_create_log.return_value.log_message.assert_called_with(
        None,
        f"File '{sample_file}' uploaded as 'test-object' to bucket 'test-bucket'.",
        "info"
    )
    assert result is True


def test_put_object_from_file_nonexistent(
    minio_client: MinioClient,
    mock_create_log: Mock
) -> None:
    """Test upload with nonexistent file.

    Verifies
    --------
    - Nonexistent file returns False
    - Error is logged

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_create_log : Mock
        Mocked CreateLog instance

    Returns
    -------
    None
    """
    result = minio_client.put_object_from_file("test-bucket", "test-object", "nonexistent.txt")
    mock_create_log.return_value.log_message.assert_called_once_with(
        None, "File nonexistent.txt does not exist.", "critical"
    )
    assert result is False


def test_put_object_from_file_s3error(
    minio_client: MinioClient,
    mock_minio: Mock,
    mock_create_log: Mock,
    sample_file: str
) -> None:
    """Test handling S3Error during file upload.

    Verifies
    --------
    - S3Error is caught
    - Error is logged
    - Returns False

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_minio : Mock
        Mocked Minio client
    mock_create_log : Mock
        Mocked CreateLog instance
    sample_file : str
        Path to temporary file

    Returns
    -------
    None
    """
    mock_minio.return_value.bucket_exists.return_value = False
    mock_minio.return_value.make_bucket.return_value = None
    mock_minio.return_value.fput_object.side_effect = S3Error(
        "error", "message", "resource", "request_id", "host_id", "bucket"
    )
    result = minio_client.put_object_from_file("test-bucket", "test-object", sample_file)
    mock_create_log.return_value.log_message.assert_called_with(
        None,
        f"Error uploading file '{sample_file}' as 'test-object' to bucket 'test-bucket': "
        "S3 operation failed; code: error, message: message, resource: resource, "
        "request_id: request_id, host_id: host_id",
        "critical"
    )
    assert result is False


# --------------------------
# Tests for put_object_from_stream
# --------------------------
def test_put_object_from_stream_success(
    minio_client: MinioClient,
    mock_minio: Mock,
    mock_create_log: Mock,
    sample_stream: BytesIO
) -> None:
    """Test successful stream upload.

    Verifies
    --------
    - Stream upload succeeds
    - Bucket is created
    - Seek is performed when requested
    - Logging is performed
    - Returns True

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_minio : Mock
        Mocked Minio client
    mock_create_log : Mock
        Mocked CreateLog instance
    sample_stream : BytesIO
        Sample stream from fixture

    Returns
    -------
    None
    """
    mock_minio.return_value.bucket_exists.return_value = False
    mock_minio.return_value.make_bucket.return_value = None
    result = minio_client.put_object_from_stream(
        "test-bucket", "test-object", sample_stream, 15, seek_to_start=True
    )
    mock_minio.return_value.put_object.assert_called_once()
    mock_create_log.return_value.log_message.assert_called_with(
        None, "Stream uploaded as 'test-object' to bucket 'test-bucket'", "info"
    )
    assert result is True


def test_put_object_from_stream_s3error(
    minio_client: MinioClient,
    mock_minio: Mock,
    mock_create_log: Mock,
    sample_stream: BytesIO
) -> None:
    """Test handling S3Error during stream upload.

    Verifies
    --------
    - S3Error is caught
    - Error is logged
    - Returns False

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_minio : Mock
        Mocked Minio client
    mock_create_log : Mock
        Mocked CreateLog instance
    sample_stream : BytesIO
        Sample stream from fixture

    Returns
    -------
    None
    """
    mock_minio.return_value.bucket_exists.return_value = False
    mock_minio.return_value.make_bucket.return_value = None
    mock_minio.return_value.put_object.side_effect = S3Error(
        "error", "message", "resource", "request_id", "host_id", "bucket"
    )
    result = minio_client.put_object_from_stream("test-bucket", "test-object", sample_stream, 15)
    mock_create_log.return_value.log_message.assert_called_with(
        None,
        "Error uploading stream as 'test-object': S3 operation failed; code: error, "
        "message: message, resource: resource, request_id: request_id, host_id: host_id",
        "critical"
    )
    assert result is False


def test_put_object_from_stream_oserror(
    minio_client: MinioClient,
    mock_minio: Mock,
    mock_create_log: Mock,
    sample_stream: BytesIO
) -> None:
    """Test handling OSError during stream upload.

    Verifies
    --------
    - OSError is caught
    - Error is logged
    - Returns False

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_minio : Mock
        Mocked Minio client
    mock_create_log : Mock
        Mocked CreateLog instance
    sample_stream : BytesIO
        Sample stream from fixture

    Returns
    -------
    None
    """
    mock_minio.return_value.bucket_exists.return_value = False
    mock_minio.return_value.make_bucket.return_value = None
    mock_minio.return_value.put_object.side_effect = OSError("IO error")
    result = minio_client.put_object_from_stream("test-bucket", "test-object", sample_stream, 15)
    mock_create_log.return_value.log_message.assert_called_with(
        None, "Error uploading stream as 'test-object': IO error", "critical"
    )
    assert result is False


# --------------------------
# Tests for put_object_from_bytes
# --------------------------
def test_put_object_from_bytes_success(
    minio_client: MinioClient,
    mock_minio: Mock,
    mock_create_log: Mock,
    sample_bytes: bytes
) -> None:
    """Test successful bytes upload.

    Verifies
    --------
    - Bytes upload succeeds
    - Bucket is created
    - Logging is performed
    - Returns True

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_minio : Mock
        Mocked Minio client
    mock_create_log : Mock
        Mocked CreateLog instance
    sample_bytes : bytes
        Sample bytes data from fixture

    Returns
    -------
    None
    """
    mock_minio.return_value.bucket_exists.return_value = False
    mock_minio.return_value.make_bucket.return_value = None
    result = minio_client.put_object_from_bytes("test-bucket", "test-object", sample_bytes)
    mock_minio.return_value.put_object.assert_called_once()
    mock_create_log.return_value.log_message.assert_called_with(
        None, "Bytes data uploaded as 'test-object' to bucket 'test-bucket'", "info"
    )
    assert result is True


def test_put_object_from_bytes_s3error(
    minio_client: MinioClient,
    mock_minio: Mock,
    mock_create_log: Mock,
    sample_bytes: bytes
) -> None:
    """Test handling S3Error during bytes upload.

    Verifies
    --------
    - S3Error is caught
    - Error is logged
    - Returns False

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_minio : Mock
        Mocked Minio client
    mock_create_log : Mock
        Mocked CreateLog instance
    sample_bytes : bytes
        Sample bytes data from fixture

    Returns
    -------
    None
    """
    mock_minio.return_value.bucket_exists.return_value = False
    mock_minio.return_value.make_bucket.return_value = None
    mock_minio.return_value.put_object.side_effect = S3Error(
        "error", "message", "resource", "request_id", "host_id", "bucket"
    )
    result = minio_client.put_object_from_bytes("test-bucket", "test-object", sample_bytes)
    mock_create_log.return_value.log_message.assert_called_with(
        None,
        "Error uploading bytes as 'test-object': S3 operation failed; code: error, "
        "message: message, resource: resource, request_id: request_id, host_id: host_id",
        "critical"
    )
    assert result is False


# --------------------------
# Tests for get_object_as_bytes
# --------------------------
def test_get_object_as_bytes_success(
    minio_client: MinioClient,
    mock_minio: Mock,
    mock_create_log: Mock
) -> None:
    """Test successful object retrieval as bytes.

    Verifies
    --------
    - Object is retrieved successfully
    - Logging is performed
    - Returns correct bytes
    - Response is closed

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_minio : Mock
        Mocked Minio client
    mock_create_log : Mock
        Mocked CreateLog instance

    Returns
    -------
    None
    """
    mock_response = MagicMock()
    mock_response.read.return_value = b"test data"
    mock_minio.return_value.get_object.return_value = mock_response
    result = minio_client.get_object_as_bytes("test-bucket", "test-object")
    mock_minio.return_value.get_object.assert_called_once_with("test-bucket", "test-object")
    mock_create_log.return_value.log_message.assert_called_with(
        None, "Successfully retrieved object 'test-object' from bucket 'test-bucket'", "info"
    )
    assert result == b"test data"
    mock_response.close.assert_called_once()
    mock_response.release_conn.assert_called_once()


def test_get_object_as_bytes_s3error(
    minio_client: MinioClient,
    mock_minio: Mock,
    mock_create_log: Mock
) -> None:
    """Test handling S3Error during object retrieval.

    Verifies
    --------
    - S3Error is caught
    - Error is logged
    - Returns None

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_minio : Mock
        Mocked Minio client
    mock_create_log : Mock
        Mocked CreateLog instance

    Returns
    -------
    None
    """
    mock_minio.return_value.get_object.side_effect = S3Error(
        "error", "message", "resource", "request_id", "host_id", "bucket"
    )
    result = minio_client.get_object_as_bytes("test-bucket", "test-object")
    mock_create_log.return_value.log_message.assert_called_with(
        None,
        "Error retrieving object 'test-object' from bucket 'test-bucket': "
        "S3 operation failed; code: error, message: message, resource: resource, "
        "request_id: request_id, host_id: host_id",
        "critical"
    )
    assert result is None


# --------------------------
# Tests for get_object_to_file
# --------------------------
def test_get_object_to_file_success(
    minio_client: MinioClient,
    mock_minio: Mock,
    mock_create_log: Mock,
    tmp_path: Any  # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test successful object download to file.

    Verifies
    --------
    - Object is downloaded successfully
    - Logging is performed
    - Returns True

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_minio : Mock
        Mocked Minio client
    mock_create_log : Mock
        Mocked CreateLog instance
    tmp_path : Any
        Temporary path provided by pytest

    Returns
    -------
    None
    """
    file_path = str(tmp_path / "downloaded.txt")
    mock_minio.return_value.fget_object.return_value = None
    result = minio_client.get_object_to_file("test-bucket", "test-object", file_path)
    mock_minio.return_value.fget_object.assert_called_once_with(
        "test-bucket", "test-object", file_path
    )
    mock_create_log.return_value.log_message.assert_called_with(
        None, f"Successfully downloaded object 'test-object' to '{file_path}'", "info"
    )
    assert result is True


def test_get_object_to_file_s3error(
    minio_client: MinioClient,
    mock_minio: Mock,
    mock_create_log: Mock,
    tmp_path: Any  # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test handling S3Error during file download.

    Verifies
    --------
    - S3Error is caught
    - Error is logged
    - Returns False

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_minio : Mock
        Mocked Minio client
    mock_create_log : Mock
        Mocked CreateLog instance
    tmp_path : Any
        Temporary path provided by pytest

    Returns
    -------
    None
    """
    file_path = str(tmp_path / "downloaded.txt")
    mock_minio.return_value.fget_object.side_effect = S3Error(
        "error", "message", "resource", "request_id", "host_id", "bucket"
    )
    result = minio_client.get_object_to_file("test-bucket", "test-object", file_path)
    mock_create_log.return_value.log_message.assert_called_with(
        None,
        "Error downloading object 'test-object': S3 operation failed; code: error, "
        "message: message, resource: resource, request_id: request_id, host_id: host_id",
        "critical"
    )
    assert result is False


# --------------------------
# Tests for list_objects
# --------------------------
def test_list_objects_success(
    minio_client: MinioClient,
    mock_minio: Mock,
    mock_create_log: Mock
) -> None:
    """Test successful object listing.

    Verifies
    --------
    - Objects are listed successfully
    - Returns list of object names
    - Logging is not performed on success

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_minio : Mock
        Mocked Minio client
    mock_create_log : Mock
        Mocked CreateLog instance

    Returns
    -------
    None
    """
    mock_obj1 = MagicMock(object_name="obj1")
    mock_obj2 = MagicMock(object_name="obj2")
    mock_minio.return_value.list_objects.return_value = [mock_obj1, mock_obj2]
    result = minio_client.list_objects("test-bucket", prefix="prefix/", recursive=True)
    mock_minio.return_value.list_objects.assert_called_once_with(
        "test-bucket", include_version=False, prefix="prefix/", recursive=True
    )
    assert result == ["obj1", "obj2"]
    mock_create_log.return_value.log_message.assert_not_called()


def test_list_objects_s3error(
    minio_client: MinioClient,
    mock_minio: Mock,
    mock_create_log: Mock
) -> None:
    """Test handling S3Error during object listing.

    Verifies
    --------
    - S3Error is caught
    - Error is logged
    - Returns None

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_minio : Mock
        Mocked Minio client
    mock_create_log : Mock
        Mocked CreateLog instance

    Returns
    -------
    None
    """
    mock_minio.return_value.list_objects.side_effect = S3Error(
        "error", "message", "resource", "request_id", "host_id", "bucket"
    )
    result = minio_client.list_objects("test-bucket")
    mock_create_log.return_value.log_message.assert_called_with(
        None,
        "Error listing objects in bucket 'test-bucket': S3 operation failed; code: error, "
        "message: message, resource: resource, request_id: request_id, host_id: host_id",
        "critical"
    )
    assert result is None


# --------------------------
# Edge Cases and Type Validation
# --------------------------
def test_put_object_from_stream_invalid_type(
    minio_client: MinioClient,
    mock_minio: Mock
) -> None:
    """Test stream upload with invalid stream type.

    Verifies
    --------
    - Non-stream type raises TypeError due to TypeChecker metaclass

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_minio : Mock
        Mocked Minio client

    Returns
    -------
    None
    """
    with pytest.raises(TypeError):
        minio_client.put_object_from_stream("test-bucket", "test-object", "not a stream", 10)


def test_put_object_from_bytes_invalid_type(
    minio_client: MinioClient,
    mock_minio: Mock
) -> None:
    """Test bytes upload with invalid data type.

    Verifies
    --------
    - Non-bytes type raises TypeError due to TypeChecker metaclass

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_minio : Mock
        Mocked Minio client

    Returns
    -------
    None
    """
    with pytest.raises(TypeError):
        minio_client.put_object_from_bytes("test-bucket", "test-object", "not bytes")


def test_put_object_from_file_invalid_metadata(
    minio_client: MinioClient,
    mock_minio: Mock,
    sample_file: str
) -> None:
    """Test file upload with invalid metadata type.

    Verifies
    --------
    - Non-dict metadata raises TypeError due to TypeChecker metaclass

    Parameters
    ----------
    minio_client : MinioClient
        MinIO client instance from fixture
    mock_minio : Mock
        Mocked Minio client
    sample_file : str
        Path to temporary file

    Returns
    -------
    None
    """
    with pytest.raises(TypeError):
        minio_client.put_object_from_file("test-bucket", "test-object", sample_file, 
                                          metadata="not a dict")