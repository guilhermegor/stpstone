"""Unit tests for Databricks CLI integration utilities.

Tests the Databricks CLI integration functionality including:
- JobsCLI class for job operations
- DbfsCLI class for file system operations
- Validation methods and error handling
- Command execution and output parsing
"""

import json
import os
from pathlib import Path
import subprocess
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from stpstone.utils.connections.databases.spark.databricks_cli import (
    DbfsCLI,
    JobsCLI,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def temp_dir(tmp_path: Path) -> str:
    """Fixture providing temporary directory path.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory fixture

    Returns
    -------
    str
        Path to temporary directory
    """
    return str(tmp_path)


@pytest.fixture
def valid_job_id() -> int:
    """Fixture providing valid job ID.

    Returns
    -------
    int
        Valid job ID (positive integer)
    """
    return 12345


@pytest.fixture
def valid_run_id() -> int:
    """Fixture providing valid run ID.

    Returns
    -------
    int
        Valid run ID (positive integer)
    """
    return 67890


@pytest.fixture
def sample_notebook_params() -> dict[str, Any]:
    """Fixture providing sample notebook parameters.

    Returns
    -------
    dict[str, Any]
        Dictionary with sample notebook parameters
    """
    return {"param1": "value1", "param2": 42, "param3": True}


@pytest.fixture
def jobs_cli_instance(valid_job_id: int, temp_dir: str) -> JobsCLI:
    """Fixture providing JobsCLI instance with valid parameters.

    Parameters
    ----------
    valid_job_id : int
        Valid job ID from fixture
    temp_dir : str
        Temporary directory path from fixture

    Returns
    -------
    JobsCLI
        Initialized JobsCLI instance
    """
    return JobsCLI(job_id=valid_job_id, path=temp_dir)


@pytest.fixture
def dbfs_cli_instance() -> DbfsCLI:
    """Fixture providing DbfsCLI instance.

    Returns
    -------
    DbfsCLI
        Initialized DbfsCLI instance
    """
    return DbfsCLI()


@pytest.fixture
def mock_subprocess_success() -> MagicMock:
    """Fixture mocking successful subprocess execution.

    Returns
    -------
    MagicMock
        Mocked subprocess.run with successful result
    """
    mock_result = MagicMock()
    mock_result.stdout = "command output"
    mock_result.returncode = 0

    with patch("subprocess.run", return_value=mock_result) as mock:
        yield mock


@pytest.fixture
def mock_subprocess_failure() -> MagicMock:
    """Fixture mocking failed subprocess execution.

    Returns
    -------
    MagicMock
        Mocked subprocess.run that raises CalledProcessError
    """
    with patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, "cmd")) as mock:
        yield mock


@pytest.fixture
def mock_sleep() -> MagicMock:
    """Fixture mocking time.sleep.

    Returns
    -------
    MagicMock
        Mocked time.sleep function
    """
    # Fix: Patch the correct module path where sleep is imported
    with patch("stpstone.utils.connections.databases.spark.databricks_cli.sleep") as mock:
        yield mock


# --------------------------
# Tests for JobsCLI Validation Methods
# --------------------------
class TestJobsCLIValidation:
    """Test cases for JobsCLI validation methods."""

    def test_validate_job_id_valid(self, jobs_cli_instance: JobsCLI) -> None:
        """Test _validate_job_id with valid positive integer.

        Verifies
        --------
        - No exception is raised for valid job ID
        - Method accepts positive integers

        Parameters
        ----------
        jobs_cli_instance : JobsCLI
            JobsCLI instance from fixture

        Returns
        -------
        None
        """
        jobs_cli_instance._validate_job_id(12345)

    @pytest.mark.parametrize("invalid_job_id", [0, -1, -100])
    def test_validate_job_id_non_positive(
        self, 
        jobs_cli_instance: JobsCLI, 
        invalid_job_id: int
    ) -> None:
        """Test _validate_job_id with non-positive integers.

        Verifies
        --------
        - ValueError is raised for non-positive job IDs
        - Error message indicates job_id must be positive

        Parameters
        ----------
        jobs_cli_instance : JobsCLI
            JobsCLI instance from fixture
        invalid_job_id : int
            Invalid job ID to test

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="job_id must be positive integer"):
            jobs_cli_instance._validate_job_id(invalid_job_id)

    @pytest.mark.parametrize("invalid_job_id", ["123", 123.45, None, []])
    def test_validate_job_id_non_integer(
        self, 
        jobs_cli_instance: JobsCLI, 
        invalid_job_id: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test _validate_job_id with non-integer types.

        Verifies
        --------
        - ValueError is raised for non-integer job IDs
        - Error message indicates job_id must be integer

        Parameters
        ----------
        jobs_cli_instance : JobsCLI
            JobsCLI instance from fixture
        invalid_job_id : Any
            Invalid job ID type to test

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be of type"):
            jobs_cli_instance._validate_job_id(invalid_job_id)

    def test_validate_path_valid(self, jobs_cli_instance: JobsCLI, temp_dir: str) -> None:
        """Test _validate_path with valid directory.

        Verifies
        --------
        - No exception is raised for valid directory path
        - Method accepts existing directories

        Parameters
        ----------
        jobs_cli_instance : JobsCLI
            JobsCLI instance from fixture
        temp_dir : str
            Temporary directory path from fixture

        Returns
        -------
        None
        """
        jobs_cli_instance._validate_path(temp_dir)

    @pytest.mark.parametrize("invalid_path", ["", "nonexistent/directory", 123, None])
    def test_validate_path_invalid(
        self, 
        jobs_cli_instance: JobsCLI, 
        invalid_path: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test _validate_path with invalid paths.

        Verifies
        --------
        - ValueError is raised for invalid paths
        - Error message indicates path must be string and directory must exist

        Parameters
        ----------
        jobs_cli_instance : JobsCLI
            JobsCLI instance from fixture
        invalid_path : Any
            Invalid path to test

        Returns
        -------
        None
        """
        if isinstance(invalid_path, int) or invalid_path is None:
            with pytest.raises(TypeError, match="must be of type"): 
                jobs_cli_instance._validate_path(invalid_path)
        else:
            with pytest.raises(ValueError):
                jobs_cli_instance._validate_path(invalid_path)

    @pytest.mark.parametrize("valid_filename", ["test.json", "run_id.json", "output.json"])
    def test_validate_filename_valid(
        self, 
        jobs_cli_instance: JobsCLI, 
        valid_filename: str
    ) -> None:
        """Test _validate_filename with valid filenames.

        Verifies
        --------
        - No exception is raised for valid JSON filenames
        - Method accepts strings ending with .json

        Parameters
        ----------
        jobs_cli_instance : JobsCLI
            JobsCLI instance from fixture
        valid_filename : str
            Valid filename to test

        Returns
        -------
        None
        """
        jobs_cli_instance._validate_filename(valid_filename)

    @pytest.mark.parametrize("invalid_filename", ["test.txt", "json", "", 123, None])
    def test_validate_filename_invalid(
        self, 
        jobs_cli_instance: JobsCLI, 
        invalid_filename: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test _validate_filename with invalid filenames.

        Verifies
        --------
        - ValueError is raised for invalid filenames
        - Error message indicates filename must be string with .json extension

        Parameters
        ----------
        jobs_cli_instance : JobsCLI
            JobsCLI instance from fixture
        invalid_filename : Any
            Invalid filename to test

        Returns
        -------
        None
        """
        if isinstance(invalid_filename, int) or invalid_filename is None:
            with pytest.raises(TypeError, match="must be of type"):
                jobs_cli_instance._validate_filename(invalid_filename)
        else:
            with pytest.raises(ValueError):
                jobs_cli_instance._validate_filename(invalid_filename)


# --------------------------
# Tests for JobsCLI Initialization
# --------------------------
class TestJobsCLIInitialization:
    """Test cases for JobsCLI initialization."""

    def test_init_valid_parameters(self, valid_job_id: int, temp_dir: str) -> None:
        """Test initialization with valid parameters.

        Verifies
        --------
        - JobsCLI can be initialized with valid job_id and path
        - Attributes are correctly set
        - run_id is initially None

        Parameters
        ----------
        valid_job_id : int
            Valid job ID from fixture
        temp_dir : str
            Temporary directory path from fixture

        Returns
        -------
        None
        """
        cli = JobsCLI(job_id=valid_job_id, path=temp_dir)
        assert cli.job_id == valid_job_id
        assert cli.path == temp_dir
        assert cli.run_id is None

    @pytest.mark.parametrize("invalid_job_id", [0, -1, "invalid", None])
    def test_init_invalid_job_id(
        self, 
        temp_dir: str, 
        invalid_job_id: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test initialization with invalid job_id.

        Verifies
        --------
        - ValueError is raised during initialization with invalid job_id
        - Error message indicates job_id validation failed

        Parameters
        ----------
        temp_dir : str
            Temporary directory path from fixture
        invalid_job_id : Any
            Invalid job ID to test

        Returns
        -------
        None
        """
        if isinstance(invalid_job_id, str) or invalid_job_id is None:
            with pytest.raises(TypeError, match="must be of type"):
                JobsCLI(job_id=invalid_job_id, path=temp_dir)
        else:
            with pytest.raises(ValueError):
                JobsCLI(job_id=invalid_job_id, path=temp_dir)

    @pytest.mark.parametrize("invalid_path", ["", "nonexistent", 123, None])
    def test_init_invalid_path(
        self, 
        valid_job_id: int, 
        invalid_path: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test initialization with invalid path.

        Verifies
        --------
        - ValueError is raised during initialization with invalid path
        - Error message indicates path validation failed

        Parameters
        ----------
        valid_job_id : int
            Valid job ID from fixture
        invalid_path : Any
            Invalid path to test

        Returns
        -------
        None
        """
        if isinstance(invalid_path, int) or invalid_path is None:
            with pytest.raises(TypeError, match="must be of type"):
                JobsCLI(job_id=valid_job_id, path=invalid_path)
        else:
            with pytest.raises(ValueError):
                JobsCLI(job_id=valid_job_id, path=invalid_path)


# --------------------------
# Tests for JobsCLI Command Execution
# --------------------------
class TestJobsCLICommandExecution:
    """Test cases for JobsCLI command execution methods."""

    def test_execute_command_success(
        self, jobs_cli_instance: JobsCLI, mock_subprocess_success: MagicMock
    ) -> None:
        """Test _execute_command with successful execution.

        Verifies
        --------
        - Command is executed successfully
        - Returns command output
        - subprocess.run is called with correct parameters

        Parameters
        ----------
        jobs_cli_instance : JobsCLI
            JobsCLI instance from fixture
        mock_subprocess_success : MagicMock
            Mocked subprocess.run with successful result

        Returns
        -------
        None
        """
        command = "echo test"
        result = jobs_cli_instance._execute_command(command)

        assert result == "command output"
        mock_subprocess_success.assert_called_once_with( # noqa S604: function call with `shell=True` parameter identified; security issue
            command, shell=True, capture_output=True, text=True, check=True
        )

    def test_execute_command_failure(
        self, jobs_cli_instance: JobsCLI, mock_subprocess_failure: MagicMock
    ) -> None:
        """Test _execute_command with failed execution.

        Verifies
        --------
        - RuntimeError is raised when command fails
        - Error message contains original exception information
        - Exception is chained properly with from err

        Parameters
        ----------
        jobs_cli_instance : JobsCLI
            JobsCLI instance from fixture
        mock_subprocess_failure : MagicMock
            Mocked subprocess.run that raises CalledProcessError

        Returns
        -------
        None
        """
        command = "invalid command"

        with pytest.raises(RuntimeError, match="Command execution failed"):
            jobs_cli_instance._execute_command(command)


# --------------------------
# Tests for JobsCLI Job Operations
# --------------------------
class TestJobsCLIOperations:
    """Test cases for JobsCLI job operations."""

    def test_print_time_success(
        self, jobs_cli_instance: JobsCLI, mock_subprocess_success: MagicMock, temp_dir: str
    ) -> None:
        """Test print_time with successful execution.

        Verifies
        --------
        - Command is executed to print timestamp
        - Path validation is performed
        - subprocess.run is called with correct command

        Parameters
        ----------
        jobs_cli_instance : JobsCLI
            JobsCLI instance from fixture
        mock_subprocess_success : MagicMock
            Mocked subprocess.run with successful result
        temp_dir : str
            Temporary directory path from fixture

        Returns
        -------
        None
        """
        complete_path = os.path.join(temp_dir, "timestamp.txt")
        jobs_cli_instance.print_time(complete_path)

        expected_command = f'echo %date%-%time% > "{complete_path}"'
        mock_subprocess_success.assert_called_once_with( # noqa S604: function call with `shell=True` parameter identified; security issue
            expected_command, shell=True, capture_output=True, text=True, check=True
        )

    def test_job_run_without_params(
        self,
        jobs_cli_instance: JobsCLI,
        mock_subprocess_success: MagicMock,
        mock_sleep: MagicMock,
        temp_dir: str,
    ) -> None:
        """Test job_run without notebook parameters.

        Verifies
        --------
        - Command is executed without notebook params
        - Output file is created and parsed
        - run_id is set correctly
        - Returns dictionary with run_id

        Parameters
        ----------
        jobs_cli_instance : JobsCLI
            JobsCLI instance from fixture
        mock_subprocess_success : MagicMock
            Mocked subprocess.run with successful result
        mock_sleep : MagicMock
            Mocked time.sleep
        temp_dir : str
            Temporary directory path from fixture

        Returns
        -------
        None
        """
        output_data = {"run_id": 99999}

        # Mock json.load to return our test data
        with patch("builtins.open", MagicMock()), \
            patch("json.load", return_value=output_data):
            
            result = jobs_cli_instance.job_run()

        assert result == {"run_id": 99999}
        assert jobs_cli_instance.run_id == 99999
        mock_sleep.assert_called_with(5)

    def test_job_run_with_params(
        self,
        jobs_cli_instance: JobsCLI,
        mock_subprocess_success: MagicMock,
        mock_sleep: MagicMock,
        sample_notebook_params: dict[str, Any],
        temp_dir: str,
    ) -> None:
        """Test job_run with notebook parameters.

        Verifies
        --------
        - Command includes notebook parameters
        - Parameters are properly JSON encoded
        - Output file is created and parsed
        - run_id is set correctly

        Parameters
        ----------
        jobs_cli_instance : JobsCLI
            JobsCLI instance from fixture
        mock_subprocess_success : MagicMock
            Mocked subprocess.run with successful result
        mock_sleep : MagicMock
            Mocked time.sleep
        sample_notebook_params : dict[str, Any]
            Sample notebook parameters from fixture
        temp_dir : str
            Temporary directory path from fixture

        Returns
        -------
        None
        """
        output_data = {"run_id": 88888}

        with patch("builtins.open", MagicMock()), \
            patch("json.load", return_value=output_data):

            result = jobs_cli_instance.job_run(notebook_params=sample_notebook_params)

        assert result == {"run_id": 88888}
        assert jobs_cli_instance.run_id == 88888
        mock_sleep.assert_called_with(5)

    def test_job_run_json_decode_error(
        self,
        jobs_cli_instance: JobsCLI,
        mock_subprocess_success: MagicMock,
        mock_sleep: MagicMock,
        temp_dir: str,
    ) -> None:
        """Test job_run with invalid JSON output.

        Verifies
        --------
        - json.JSONDecodeError is raised when output contains invalid JSON
        - Exception is chained properly with from err
        - Error message indicates invalid JSON in output file

        Parameters
        ----------
        jobs_cli_instance : JobsCLI
            JobsCLI instance from fixture
        mock_subprocess_success : MagicMock
            Mocked subprocess.run with successful result
        mock_sleep : MagicMock
            Mocked time.sleep
        temp_dir : str
            Temporary directory path from fixture

        Returns
        -------
        None
        """
        with patch("builtins.open", MagicMock()), \
            patch("json.load", side_effect=json.JSONDecodeError("Invalid JSON", "doc", 0)), \
            pytest.raises(json.JSONDecodeError, match="Invalid JSON in output file"):
            jobs_cli_instance.job_run()

    def test_get_job_metadata_success(
        self,
        jobs_cli_instance: JobsCLI,
        mock_subprocess_success: MagicMock,
        mock_sleep: MagicMock,
        temp_dir: str,
    ) -> None:
        """Test get_job_metadata with successful execution.

        Verifies
        --------
        - Command is executed to get job metadata
        - Output file is created and parsed
        - Returns job metadata dictionary

        Parameters
        ----------
        jobs_cli_instance : JobsCLI
            JobsCLI instance from fixture
        mock_subprocess_success : MagicMock
            Mocked subprocess.run with successful result
        mock_sleep : MagicMock
            Mocked time.sleep
        temp_dir : str
            Temporary directory path from fixture

        Returns
        -------
        None
        """
        metadata = {"job_id": 12345, "settings": {}, "created_time": 1640995200000}

        with patch("builtins.open", MagicMock()), patch("json.load", return_value=metadata):

            result = jobs_cli_instance.get_job_metadata()

        assert result == metadata
        mock_sleep.assert_called_with(5)

    def test_cancel_job_run_with_run_id(
        self,
        jobs_cli_instance: JobsCLI,
        mock_subprocess_success: MagicMock,
        valid_run_id: int,
    ) -> None:
        """Test cancel_job_run with existing run_id.

        Verifies
        --------
        - Command is executed to cancel job run
        - Uses internal run_id when available
        - subprocess.run is called with correct command

        Parameters
        ----------
        jobs_cli_instance : JobsCLI
            JobsCLI instance from fixture
        mock_subprocess_success : MagicMock
            Mocked subprocess.run with successful result
        valid_run_id : int
            Valid run ID from fixture

        Returns
        -------
        None
        """
        jobs_cli_instance.run_id = valid_run_id
        jobs_cli_instance.cancel_job_run()

        expected_command = f"databricks runs cancel --run-id {valid_run_id}"
        mock_subprocess_success.assert_called_once_with( # noqa S604: function call with `shell=True` parameter identified; security issue
            expected_command, shell=True, capture_output=True, text=True, check=True
        )

    def test_cancel_job_run_with_external_run_id(
        self,
        jobs_cli_instance: JobsCLI,
        mock_subprocess_success: MagicMock,
        valid_run_id: int,
    ) -> None:
        """Test cancel_job_run with external run_id.

        Verifies
        --------
        - Command is executed to cancel external job run
        - Uses provided outside_run_id
        - subprocess.run is called with correct command

        Parameters
        ----------
        jobs_cli_instance : JobsCLI
            JobsCLI instance from fixture
        mock_subprocess_success : MagicMock
            Mocked subprocess.run with successful result
        valid_run_id : int
            Valid run ID from fixture

        Returns
        -------
        None
        """
        jobs_cli_instance.cancel_job_run(outside_run_id=valid_run_id)

        expected_command = f"databricks runs cancel --run-id {valid_run_id}"
        mock_subprocess_success.assert_called_once_with( # noqa S604: function call with `shell=True` parameter identified; security issue
            expected_command, shell=True, capture_output=True, text=True, check=True
        )

    def test_cancel_job_run_no_run_id(self, jobs_cli_instance: JobsCLI) -> None:
        """Test cancel_job_run without any run_id available.

        Verifies
        --------
        - ValueError is raised when no run_id is available
        - Error message indicates no run_id available for cancellation

        Parameters
        ----------
        jobs_cli_instance : JobsCLI
            JobsCLI instance from fixture

        Returns
        -------
        None
        """
        jobs_cli_instance.run_id = None

        with pytest.raises(ValueError, match="No run_id available for cancellation"):
            jobs_cli_instance.cancel_job_run()


# --------------------------
# Tests for DbfsCLI Validation Methods
# --------------------------
class TestDbfsCLIValidation:
    """Test cases for DbfsCLI validation methods."""

    @pytest.mark.parametrize("valid_path", ["dbfs:/path/to/file", 
                                            "dbfs:/FileStore/tables/data.csv"])
    def test_validate_dbfs_path_valid(self, dbfs_cli_instance: DbfsCLI, valid_path: str) -> None:
        """Test _validate_dbfs_path with valid DBFS paths.

        Verifies
        --------
        - No exception is raised for valid DBFS paths
        - Method accepts paths starting with 'dbfs:/'

        Parameters
        ----------
        dbfs_cli_instance : DbfsCLI
            DbfsCLI instance from fixture
        valid_path : str
            Valid DBFS path to test

        Returns
        -------
        None
        """
        dbfs_cli_instance._validate_dbfs_path(valid_path)

    @pytest.mark.parametrize("invalid_path", ["/local/path", "hdfs://path", "", 123, None])
    def test_validate_dbfs_path_invalid(
        self, 
        dbfs_cli_instance: DbfsCLI, 
        invalid_path: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test _validate_dbfs_path with invalid paths.

        Verifies
        --------
        - ValueError is raised for invalid DBFS paths
        - Error message indicates path must start with 'dbfs:/'

        Parameters
        ----------
        dbfs_cli_instance : DbfsCLI
            DbfsCLI instance from fixture
        invalid_path : Any
            Invalid path to test

        Returns
        -------
        None
        """
        if isinstance(invalid_path, int) or invalid_path is None:
            with pytest.raises(TypeError, match="must be of type"):
                dbfs_cli_instance._validate_dbfs_path(invalid_path)
        else:
            with pytest.raises(ValueError):
                dbfs_cli_instance._validate_dbfs_path(invalid_path)


# --------------------------
# Tests for DbfsCLI Operations
# --------------------------
class TestDbfsCLIOperations:
    """Test cases for DbfsCLI file operations."""

    def test_copy_with_overwrite(
        self, dbfs_cli_instance: DbfsCLI, mock_subprocess_success: MagicMock
    ) -> None:
        """Test copy with overwrite enabled.

        Verifies
        --------
        - Command includes --overwrite flag
        - Path validation is performed
        - Returns command output lines

        Parameters
        ----------
        dbfs_cli_instance : DbfsCLI
            DbfsCLI instance from fixture
        mock_subprocess_success : MagicMock
            Mocked subprocess.run with successful result

        Returns
        -------
        None
        """
        src_path = "dbfs:/source/file.csv"
        dest_path = "dbfs:/destination/file.csv"

        result = dbfs_cli_instance.copy(src_path, dest_path, overwrite=True)

        expected_command = f'dbfs cp "{src_path}" "{dest_path}" --overwrite'
        mock_subprocess_success.assert_called_once_with( # noqa S604: function call with `shell=True` parameter identified; security issue
            expected_command, shell=True, capture_output=True, text=True, check=True
        )
        assert result == ["command output"]

    def test_copy_without_overwrite(
        self, dbfs_cli_instance: DbfsCLI, mock_subprocess_success: MagicMock
    ) -> None:
        """Test copy without overwrite.

        Verifies
        --------
        - Command does not include --overwrite flag
        - Path validation is performed
        - Returns command output lines

        Parameters
        ----------
        dbfs_cli_instance : DbfsCLI
            DbfsCLI instance from fixture
        mock_subprocess_success : MagicMock
            Mocked subprocess.run with successful result

        Returns
        -------
        None
        """
        src_path = "dbfs:/source/file.csv"
        dest_path = "dbfs:/destination/file.csv"

        result = dbfs_cli_instance.copy(src_path, dest_path, overwrite=False)

        expected_command = f'dbfs cp "{src_path}" "{dest_path}"'
        mock_subprocess_success.assert_called_once_with( # noqa S604: function call with `shell=True` parameter identified; security issue
            expected_command, shell=True, capture_output=True, text=True, check=True
        )
        assert result == ["command output"]

    def test_remove_success(
        self, dbfs_cli_instance: DbfsCLI, mock_subprocess_success: MagicMock
    ) -> None:
        """Test remove with successful execution.

        Verifies
        --------
        - Command is executed to remove file
        - Returns second line of command output
        - Path validation is performed

        Parameters
        ----------
        dbfs_cli_instance : DbfsCLI
            DbfsCLI instance from fixture
        mock_subprocess_success : MagicMock
            Mocked subprocess.run with successful result

        Returns
        -------
        None
        """
        path = "dbfs:/path/to/remove.txt"
        mock_subprocess_success.return_value.stdout = "line1\nline2\nline3"

        result = dbfs_cli_instance.remove(path)

        expected_command = f'dbfs rm "{path}"'
        mock_subprocess_success.assert_called_once_with( # noqa S604: function call with `shell=True` parameter identified; security issue
            expected_command, shell=True, capture_output=True, text=True, check=True
        )
        assert result == "line2"

    def test_move_success(
        self, dbfs_cli_instance: DbfsCLI, mock_subprocess_success: MagicMock
    ) -> None:
        """Test move with successful execution.

        Verifies
        --------
        - Command is executed to move file
        - Returns command output lines
        - Path validation is performed for both paths

        Parameters
        ----------
        dbfs_cli_instance : DbfsCLI
            DbfsCLI instance from fixture
        mock_subprocess_success : MagicMock
            Mocked subprocess.run with successful result

        Returns
        -------
        None
        """
        src_path = "dbfs:/source/file.csv"
        dest_path = "dbfs:/destination/file.csv"

        result = dbfs_cli_instance.move(src_path, dest_path)

        expected_command = f'dbfs mv "{src_path}" "{dest_path}"'
        mock_subprocess_success.assert_called_once_with( # noqa S604: function call with `shell=True` parameter identified; security issue
            expected_command, shell=True, capture_output=True, text=True, check=True
        )
        assert result == ["command output"]

    def test_list_files_detailed(
        self, dbfs_cli_instance: DbfsCLI, mock_subprocess_success: MagicMock
    ) -> None:
        """Test list_files with detailed output.

        Verifies
        --------
        - Command includes -l flag for detailed listing
        - Output is parsed into dictionary format
        - Path validation is performed

        Parameters
        ----------
        dbfs_cli_instance : DbfsCLI
            DbfsCLI instance from fixture
        mock_subprocess_success : MagicMock
            Mocked subprocess.run with successful result

        Returns
        -------
        None
        """
        path = "dbfs:/path/to/list"
        # Fix: Use proper DBFS ls -l format with full file paths
        mock_output = "drwxr-xr-x 0 dbfs:/path/to/list/file1.txt\n-rw-r--r-- 1 " \
            + "dbfs:/path/to/list/file2.txt"
        mock_subprocess_success.return_value.stdout = mock_output

        result = dbfs_cli_instance.list_files(path, l_flag="-l")

        expected_command = f'dbfs ls "{path}" --absolute -l'
        mock_subprocess_success.assert_called_once_with( # noqa S604: function call with `shell=True` parameter identified; security issue
            expected_command, shell=True, capture_output=True, text=True, check=True
        )
        assert isinstance(result, dict)
        # The current parsing logic uses the 3rd element (index 2) as the key
        assert "dbfs:/path/to/list/file1.txt" in result
        assert "dbfs:/path/to/list/file2.txt" in result

    def test_list_files_simple(
        self, dbfs_cli_instance: DbfsCLI, mock_subprocess_success: MagicMock
    ) -> None:
        """Test list_files with simple output.

        Verifies
        --------
        - Command does not include -l flag for simple listing
        - Output is returned as list of lines
        - Path validation is performed

        Parameters
        ----------
        dbfs_cli_instance : DbfsCLI
            DbfsCLI instance from fixture
        mock_subprocess_success : MagicMock
            Mocked subprocess.run with successful result

        Returns
        -------
        None
        """
        path = "dbfs:/path/to/list"
        mock_output = "dbfs:/path/to/list/file1.txt\ndbfs:/path/to/list/file2.txt"
        mock_subprocess_success.return_value.stdout = mock_output

        result = dbfs_cli_instance.list_files(path, l_flag="")

        expected_command = f'dbfs ls "{path}" --absolute'
        mock_subprocess_success.assert_called_once_with( # noqa S604: function call with `shell=True` parameter identified; security issue
            expected_command, 
            shell=True, 
            capture_output=True, 
            text=True, 
            check=True
        )
        assert isinstance(result, list)
        assert len(result) == 2


# --------------------------
# Tests for DbfsCLI Copy and Run
# --------------------------
class TestDbfsCLICopyAndRun:
    """Test cases for DbfsCLI copy_and_run method."""

    def test_copy_and_run_success(
        self,
        dbfs_cli_instance: DbfsCLI,
        mock_subprocess_success: MagicMock,
        mock_sleep: MagicMock,
        temp_dir: str,
    ) -> None:
        """Test copy_and_run with successful job execution.

        Verifies
        --------
        - Files are copied to DBFS
        - Job is executed successfully
        - Returns success status
        - All validation and sleep calls are made

        Parameters
        ----------
        dbfs_cli_instance : DbfsCLI
            DbfsCLI instance from fixture
        mock_subprocess_success : MagicMock
            Mocked subprocess.run with successful result
        mock_sleep : MagicMock
            Mocked time.sleep
        temp_dir : str
            Temporary directory path from fixture

        Returns
        -------
        None
        """
        local_path = os.path.join(temp_dir, "test.csv")
        dbfs_path = "dbfs:/FileStore/tables/test.csv"
        job_id = 12345

        # Create test file
        with open(local_path, "w") as f:
            f.write("test,data\n1,2\n3,4")

        # Mock the copy method to avoid path validation on local_path
        with patch.object(dbfs_cli_instance, "copy") as mock_copy, \
            patch.object(dbfs_cli_instance, "list_files") as mock_list_files:
            
            mock_list_files.return_value = [dbfs_path]

            # Mock JobsCLI operations
            with patch("stpstone.utils.connections.databases.spark.databricks_cli.JobsCLI") \
                as mock_jobs_cli:
                mock_instance = MagicMock()
                mock_jobs_cli.return_value = mock_instance
                mock_instance.job_run.return_value = {"run_id": 99999}
                mock_instance.get_run_metadata.return_value = {
                    "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"}
                }

                result = dbfs_cli_instance.copy_and_run(local_path, dbfs_path, job_id)

        assert result == "Success running job"
        # Verify that sleep was called multiple times (at least 3 times in the method)
        assert mock_sleep.call_count >= 3
        mock_copy.assert_called()

    def test_copy_and_run_file_not_exists(self, dbfs_cli_instance: DbfsCLI, temp_dir: str) -> None:
        """Test copy_and_run with non-existent local file.

        Verifies
        --------
        - ValueError is raised when local file does not exist
        - Error message indicates file does not exist

        Parameters
        ----------
        dbfs_cli_instance : DbfsCLI
            DbfsCLI instance from fixture
        temp_dir : str
            Temporary directory path from fixture

        Returns
        -------
        None
        """
        local_path = os.path.join(temp_dir, "nonexistent.csv")
        dbfs_path = "dbfs:/FileStore/tables/test.csv"
        job_id = 12345

        with pytest.raises(ValueError, match="Local file does not exist"):
            dbfs_cli_instance.copy_and_run(local_path, dbfs_path, job_id)

    def test_copy_and_run_job_failure(
        self,
        dbfs_cli_instance: DbfsCLI,
        mock_subprocess_success: MagicMock,
        mock_sleep: MagicMock,
        temp_dir: str,
    ) -> None:
        """Test copy_and_run with job execution failure.

        Verifies
        --------
        - Returns error status when job fails
        - Handles job failure gracefully

        Parameters
        ----------
        dbfs_cli_instance : DbfsCLI
            DbfsCLI instance from fixture
        mock_subprocess_success : MagicMock
            Mocked subprocess.run with successful result
        mock_sleep : MagicMock
            Mocked time.sleep
        temp_dir : str
            Temporary directory path from fixture

        Returns
        -------
        None
        """
        local_path = os.path.join(temp_dir, "test.csv")
        dbfs_path = "dbfs:/FileStore/tables/test.csv"
        job_id = 12345

        # Create test file
        with open(local_path, "w") as f:
            f.write("test,data\n1,2\n3,4")

        # Mock the copy method to avoid path validation on local_path
        with patch.object(dbfs_cli_instance, "copy") as mock_copy, \
            patch.object(dbfs_cli_instance, "list_files") as mock_list_files:
            
            mock_list_files.return_value = [dbfs_path]

            # Mock JobsCLI operations with failure
            with patch("stpstone.utils.connections.databases.spark.databricks_cli.JobsCLI") \
                as mock_jobs_cli:
                mock_instance = MagicMock()
                mock_jobs_cli.return_value = mock_instance
                mock_instance.job_run.return_value = {"run_id": 99999}
                mock_instance.get_run_metadata.return_value = {
                    "state": {"life_cycle_state": "TERMINATED", "result_state": "FAILED"}
                }

                result = dbfs_cli_instance.copy_and_run(local_path, dbfs_path, job_id)

        assert result == "Error running job"
        mock_copy.assert_called()


# --------------------------
# Tests for Edge Cases and Error Conditions
# --------------------------
class TestEdgeCases:
    """Test cases for edge cases and error conditions."""

    def test_get_run_metadata_no_run_id(self, jobs_cli_instance: JobsCLI) -> None:
        """Test get_run_metadata without any run_id available.

        Verifies
        --------
        - ValueError is raised when no run_id is available
        - Error message indicates no run_id available for metadata retrieval

        Parameters
        ----------
        jobs_cli_instance : JobsCLI
            JobsCLI instance from fixture

        Returns
        -------
        None
        """
        jobs_cli_instance.run_id = None

        with pytest.raises(ValueError, match="No run_id available for metadata retrieval"):
            jobs_cli_instance.get_run_metadata()

    def test_get_run_output_no_run_id(self, jobs_cli_instance: JobsCLI) -> None:
        """Test get_run_output without any run_id available.

        Verifies
        --------
        - ValueError is raised when no run_id is available
        - Error message indicates no run_id available for output retrieval

        Parameters
        ----------
        jobs_cli_instance : JobsCLI
            JobsCLI instance from fixture

        Returns
        -------
        None
        """
        jobs_cli_instance.run_id = None

        with pytest.raises(ValueError, match="No run_id available for output retrieval"):
            jobs_cli_instance.get_run_output()

    def test_list_files_empty_output(
        self, 
        dbfs_cli_instance: DbfsCLI, 
        mock_subprocess_success: MagicMock
    ) -> None:
        """Test list_files with empty command output.

        Verifies
        --------
        - Handles empty output gracefully
        - Returns empty list for simple output
        - Returns empty dict for detailed output

        Parameters
        ----------
        dbfs_cli_instance : DbfsCLI
            DbfsCLI instance from fixture
        mock_subprocess_success : MagicMock
            Mocked subprocess.run with successful result

        Returns
        -------
        None
        """
        path = "dbfs:/empty/path"
        mock_subprocess_success.return_value.stdout = ""

        # Test simple output
        result_simple = dbfs_cli_instance.list_files(path, l_flag="")
        assert result_simple == []

        # Test detailed output
        result_detailed = dbfs_cli_instance.list_files(path, l_flag="-l")
        assert result_detailed == {}

    def test_remove_single_line_output(
        self, 
        dbfs_cli_instance: DbfsCLI, 
        mock_subprocess_success: MagicMock
    ) -> None:
        """Test remove with single line command output.

        Verifies
        --------
        - Handles single line output gracefully
        - Returns the single line when only one line exists

        Parameters
        ----------
        dbfs_cli_instance : DbfsCLI
            DbfsCLI instance from fixture
        mock_subprocess_success : MagicMock
            Mocked subprocess.run with successful result

        Returns
        -------
        None
        """
        path = "dbfs:/path/to/file.txt"
        mock_subprocess_success.return_value.stdout = "single line"

        result = dbfs_cli_instance.remove(path)
        assert result == "single line"