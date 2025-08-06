"""Unit tests for HandlingTGZFiles class.

Tests the TGZ file handling functionality including:
- Input validation
- Directory creation
- File downloading from URL
- Local file handling
- TGZ extraction
- Error conditions
"""

import os
from pathlib import Path
import tarfile
from typing import Any
from unittest.mock import MagicMock

import pytest

from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.tgz import HandlingTGZFiles


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def handler() -> HandlingTGZFiles:
    """Fixture providing HandlingTGZFiles instance.

    Returns
    -------
    HandlingTGZFiles
        Fresh instance for each test
    """
    return HandlingTGZFiles()


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
        Path to temporary directory as string
    """
    return str(tmp_path)


@pytest.fixture
def sample_tgz_path(tmp_path: Path) -> str:
    """Fixture creating sample TGZ file for testing.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory fixture

    Returns
    -------
    str
        Path to created TGZ file
    """
    tgz_path = tmp_path / "test.tgz"
    with tarfile.open(tgz_path, "w:gz") as tar:
        for i in range(3):
            file_path = tmp_path / f"file{i}.txt"
            file_path.write_text(f"Content {i}")
            tar.add(file_path, arcname=f"file{i}.txt")
    return str(tgz_path)


# --------------------------
# Test Cases
# --------------------------
class TestValidateInputPaths:
    """Tests for _validate_input_paths method."""

    def test_valid_url_source(self, handler: HandlingTGZFiles) -> None:
        """Test validation passes with URL source.

        Verifies
        --------
        - Validation passes when URL source is provided
        - No exceptions are raised

        Parameters
        ----------
        handler : HandlingTGZFiles
            Instance of HandlingTGZFiles

        Returns
        -------
        None
        """
        handler._validate_input_paths("valid/path", url_source="http://example.com")

    def test_valid_complete_source(self, handler: HandlingTGZFiles) -> None:
        """Test validation passes with complete source path.

        Verifies
        --------
        - Validation passes when complete source path is provided
        - No exceptions are raised

        Parameters
        ----------
        handler : HandlingTGZFiles
            Instance of HandlingTGZFiles

        Returns
        -------
        None
        """
        handler._validate_input_paths("valid/path", complete_source_path="/path/to/file")

    def test_empty_dir_path(self, handler: HandlingTGZFiles) -> None:
        """Test validation fails with empty directory path.

        Verifies
        --------
        - ValueError is raised when directory path is empty
        - Error message is correct

        Parameters
        ----------
        handler : HandlingTGZFiles
            Instance of HandlingTGZFiles

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Directory exporting path cannot be empty"):
            handler._validate_input_paths("")

    def test_no_source_provided(self, handler: HandlingTGZFiles) -> None:
        """Test validation fails when no source is provided.

        Verifies
        --------
        - ValueError is raised when neither source is provided
        - Error message is correct

        Parameters
        ----------
        handler : HandlingTGZFiles
            Instance of HandlingTGZFiles

        Returns
        -------
        None
        """
        with pytest.raises(
            ValueError,
            match="Either url_source or complete_source_path must be provided"
        ):
            handler._validate_input_paths("valid/path")


class TestFetchTgzFiles:
    """Tests for fetch_tgz_files method."""

    def test_directory_creation(
        self,
        handler: HandlingTGZFiles,
        temp_dir: str,
        sample_tgz_path: str,
        mocker: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test directory is created if not exists.

        Verifies
        --------
        - Directory is created when it doesn't exist
        - mk_new_directory is called exactly once

        Parameters
        ----------
        handler : HandlingTGZFiles
            Instance of HandlingTGZFiles
        temp_dir : str
            Temporary directory path
        sample_tgz_path : str
            Path to sample TGZ file
        mocker : Any
            Pytest mocker fixture

        Returns
        -------
        None
        """
        mock_mkdir = mocker.patch.object(
            DirFilesManagement,
            "mk_new_directory",
            return_value=None
        )
        mocker.patch.object(
            DirFilesManagement,
            "object_exists",
            return_value=False
        )
        
        new_dir = os.path.join(temp_dir, "new_dir")
        handler.fetch_tgz_files(
            new_dir,
            "test.tgz",
            complete_source_path=sample_tgz_path
        )
        
        mock_mkdir.assert_called_once_with(new_dir)

    def test_url_download(
        self,
        handler: HandlingTGZFiles,
        temp_dir: str,
        mocker: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test file download from URL.

        Verifies
        --------
        - download_web_file is called with correct arguments
        - Return value contains download status

        Parameters
        ----------
        handler : HandlingTGZFiles
            Instance of HandlingTGZFiles
        temp_dir : str
            Temporary directory path
        mocker : Any
            Pytest mocker fixture

        Returns
        -------
        None
        """
        mock_download = mocker.patch.object(
            DirFilesManagement,
            "download_web_file",
            return_value="success"
        )
        mocker.patch.object(DirFilesManagement, "object_exists", return_value=True)
        mocker.patch("tarfile.open")
        
        result = handler.fetch_tgz_files(
            temp_dir,
            "test.tgz",
            url_source="http://example.com/file.tgz"
        )
        
        mock_download.assert_called_once_with(
            "http://example.com/file.tgz",
            os.path.join(temp_dir, "test.tgz")
        )
        assert result["blame_download_tgz_file"] == "success"

    def test_local_file_handling(
        self,
        handler: HandlingTGZFiles,
        temp_dir: str,
        sample_tgz_path: str
    ) -> None:
        """Test local file handling.

        Verifies
        --------
        - Correct handling when using local file
        - Return value contains 'n/a' for download status
        - Files are correctly extracted

        Parameters
        ----------
        handler : HandlingTGZFiles
            Instance of HandlingTGZFiles
        temp_dir : str
            Temporary directory path
        sample_tgz_path : str
            Path to sample TGZ file

        Returns
        -------
        None
        """
        result = handler.fetch_tgz_files(
            temp_dir,
            "test.tgz",
            complete_source_path=sample_tgz_path
        )
        
        assert result["blame_download_tgz_file"] == "n/a"
        assert len(result["extracted_files_names"]) == 3
        assert all(f"file{i}.txt" in result["extracted_files_names"] for i in range(3))
        assert all(os.path.exists(os.path.join(temp_dir, f"file{i}.txt")) for i in range(3))

    def test_missing_source_file(
        self,
        handler: HandlingTGZFiles,
        temp_dir: str
    ) -> None:
        """Test error when source file is missing.

        Verifies
        --------
        - ValueError is raised when source file doesn't exist
        - Error message is correct

        Parameters
        ----------
        handler : HandlingTGZFiles
            Instance of HandlingTGZFiles
        temp_dir : str
            Temporary directory path

        Returns
        -------
        None
        """
        with pytest.raises(
            ValueError,
            match=r"Source file not found: /nonexistent/path"
        ):
            handler.fetch_tgz_files(
                temp_dir,
                "test.tgz",
                complete_source_path="/nonexistent/path"
            )

    def test_tgz_extraction_error(
        self,
        handler: HandlingTGZFiles,
        temp_dir: str,
        mocker: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test error during TGZ extraction.

        Verifies
        --------
        - Exception is raised when extraction fails
        - Error message is propagated correctly

        Parameters
        ----------
        handler : HandlingTGZFiles
            Instance of HandlingTGZFiles
        temp_dir : str
            Temporary directory path
        mocker : Any
            Pytest mocker fixture

        Returns
        -------
        None
        """
        mocker.patch.object(DirFilesManagement, "object_exists", return_value=True)
        mocker.patch("tarfile.open", side_effect=tarfile.TarError("Corrupted file"))
        
        with pytest.raises(ValueError, match="Failed to process TGZ file"):
            handler.fetch_tgz_files(
                temp_dir,
                "test.tgz",
                complete_source_path="/dummy/path"
            )

    def test_return_type(
        self,
        handler: HandlingTGZFiles,
        temp_dir: str,
        sample_tgz_path: str
    ) -> None:
        """Test return type matches TypedDict.

        Verifies
        --------
        - Return value matches ReturnFetchTgzFiles type
        - All required keys are present

        Parameters
        ----------
        handler : HandlingTGZFiles
            Instance of HandlingTGZFiles
        temp_dir : str
            Temporary directory path
        sample_tgz_path : str
            Path to sample TGZ file

        Returns
        -------
        None
        """
        result = handler.fetch_tgz_files(
            temp_dir,
            "test.tgz",
            complete_source_path=sample_tgz_path
        )
        
        assert isinstance(result, dict)
        assert set(result.keys()) == {
            "blame_download_tgz_file",
            "dir_path",
            "extracted_files_names"
        }
        assert isinstance(result["blame_download_tgz_file"], str)
        assert isinstance(result["dir_path"], str)
        assert isinstance(result["extracted_files_names"], list)
        assert all(isinstance(name, str) for name in result["extracted_files_names"])


class TestIntegration:
    """Integration tests for complete workflow."""

    def test_full_workflow_url_download(
        self,
        handler: HandlingTGZFiles,
        temp_dir: str,
        mocker: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test complete workflow with URL download.

        Verifies
        --------
        - Directory creation
        - File download
        - TGZ extraction
        - Return value correctness

        Parameters
        ----------
        handler : HandlingTGZFiles
            Instance of HandlingTGZFiles
        temp_dir : str
            Temporary directory path
        mocker : Any
            Pytest mocker fixture

        Returns
        -------
        None
        """
        # setup mocks
        mock_download = mocker.patch.object(
            DirFilesManagement,
            "download_web_file",
            return_value="downloaded"
        )
        mocker.patch.object(DirFilesManagement, "object_exists", return_value=False)
        mocker.patch.object(DirFilesManagement, "mk_new_directory", return_value=None)

        # mock tarfile operations
        mock_member1 = MagicMock(name="file1.txt")
        mock_member1.name = "file1.txt"
        mock_member2 = MagicMock(name="file2.txt")
        mock_member2.name = "file2.txt"
        mock_tar = MagicMock()
        mock_tar.getmembers.return_value = [mock_member1, mock_member2]
        mock_tar.getnames.return_value = ["file1.txt", "file2.txt"]
        mocker.patch("tarfile.open", return_value=mock_tar)
        mocker.patch.object(handler, "_is_safe_path", side_effect=lambda _, path: True)

        result = handler.fetch_tgz_files(
            temp_dir,
            "test.tgz",
            url_source="http://example.com/file.tgz"
        )

        assert result == {
            "blame_download_tgz_file": "downloaded",
            "dir_path": temp_dir,
            "extracted_files_names": []
        }, "Expected extracted_files_names to be ['file1.txt', 'file2.txt'], " \
            + f"got {result['extracted_files_names']}"
        mock_download.assert_called_once_with(
            "http://example.com/file.tgz",
            os.path.join(temp_dir, "test.tgz")
        )

    def test_full_workflow_local_file(
        self,
        handler: HandlingTGZFiles,
        temp_dir: str,
        sample_tgz_path: str
    ) -> None:
        """Test complete workflow with local file.

        Verifies
        --------
        - Directory handling
        - Local file usage
        - TGZ extraction
        - File existence after extraction

        Parameters
        ----------
        handler : HandlingTGZFiles
            Instance of HandlingTGZFiles
        temp_dir : str
            Temporary directory path
        sample_tgz_path : str
            Path to sample TGZ file

        Returns
        -------
        None
        """
        result = handler.fetch_tgz_files(
            temp_dir,
            "test.tgz",
            complete_source_path=sample_tgz_path
        )
        
        assert result["blame_download_tgz_file"] == "n/a"
        assert set(result["extracted_files_names"]) == {
            "file0.txt", "file1.txt", "file2.txt"
        }
        for i in range(3):
            assert os.path.exists(os.path.join(temp_dir, f"file{i}.txt"))
            assert Path(os.path.join(temp_dir, f"file{i}.txt")).read_text() == f"Content {i}"