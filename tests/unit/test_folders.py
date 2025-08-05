"""Unit tests for directory and file management utilities.

Tests cover all operations including file manipulation, directory traversal,
compression/decompression, and remote file operations.
"""

from collections.abc import Generator
from datetime import datetime
from io import BytesIO, TextIOWrapper
import os
import tempfile
from typing import Any
from unittest.mock import MagicMock, patch
import zipfile

import py7zr
import pycurl
import pytest
from requests import Response

from stpstone.utils.parsers.folders import DirFilesManagement, FoldersTree


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def temp_dir() -> Generator[str, None, None]:
    """Create and cleanup a temporary directory for testing.

    Yields
    ------
    str
        Path to temporary directory
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def test_file(temp_dir: str) -> str:
    """Create a test file in temporary directory.

    Parameters
    ----------
    temp_dir : str
        Temporary directory path

    Returns
    -------
    str
        Path to created test file
    """
    file_path = os.path.join(temp_dir, "test.txt")
    with open(file_path, "w") as f:
        f.write("test content")
    return file_path


@pytest.fixture
def test_zip_file(temp_dir: str) -> str:
    """Create a test zip file in temporary directory.

    Parameters
    ----------
    temp_dir : str
        Temporary directory path

    Returns
    -------
    str
        Path to created test zip file
    """
    zip_path = os.path.join(temp_dir, "test.zip")
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("file1.txt", "content1")
        zf.writestr("file2.txt", "content2")
    return zip_path


@pytest.fixture
def test_7z_file(temp_dir: str, test_file: str) -> str:
    """Create a test 7z file in temporary directory.

    Parameters
    ----------
    temp_dir : str
        Temporary directory path
    test_file : str
        Path to test file

    Returns
    -------
    str
        Path to created test 7z file
    """
    file_path = os.path.join(temp_dir, "test.7z")
    with py7zr.SevenZipFile(file_path, "w") as z:
        z.writeall(test_file, "test.txt")
    return file_path


@pytest.fixture
def dir_manager() -> Any: # noqa ANN401: typing.Any is not allowed
    """Fixture providing DirFilesManagement instance.

    Returns
    -------
    Any
        Instance of DirFilesManagement class
    """
    return DirFilesManagement()


@pytest.fixture
def mock_response() -> Response:
    """Fixture providing mocked Response object with zip content.

    Returns
    -------
    Response
        Mocked Response object
    """
    mock_resp = MagicMock(spec=Response)
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("file.txt", "test content")
    mock_resp.content = buffer.getvalue()
    return mock_resp

# --------------------------
# Tests for DirFilesManagement
# --------------------------
class TestDirFilesManagement:
    """Test cases for DirFilesManagement class."""

    def test_get_curr_dir(
        self, 
        dir_manager: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test getting current directory.

        Verifies
        --------
        - Returns a string
        - Returned path exists

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance

        Returns
        -------
        None
        """
        current_dir = dir_manager.get_curr_dir()
        assert isinstance(current_dir, str)
        assert os.path.exists(current_dir)

    def test_list_dir_files(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        temp_dir: str
    ) -> None:
        """Test listing directory contents.

        Verifies
        --------
        - Returns list of strings
        - Contains created test file

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        temp_dir : str
            Temporary directory path

        Returns
        -------
        None
        """
        test_file = os.path.join(temp_dir, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")

        files = dir_manager.list_dir_files(temp_dir)
        assert isinstance(files, list)
        assert all(isinstance(f, str) for f in files)
        assert "test.txt" in files

    def test_change_dir(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        temp_dir: str
    ) -> None:
        """Test changing working directory.

        Verifies
        --------
        - Successfully changes directory
        - Raises FileNotFoundError for invalid path

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        temp_dir : str
            Temporary directory path

        Returns
        -------
        None
        """
        original_dir = dir_manager.get_curr_dir()
        
        dir_manager.change_dir(temp_dir)
        assert dir_manager.get_curr_dir() == temp_dir
        
        dir_manager.change_dir(original_dir)
        
        with pytest.raises(FileNotFoundError):
            dir_manager.change_dir("/nonexistent/path")

    def test_mk_new_directory(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        temp_dir: str
    ) -> None:
        """Test creating new directory.

        Verifies
        --------
        - Creates new directory when it doesn't exist
        - Returns False when directory exists

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        temp_dir : str
            Temporary directory path

        Returns
        -------
        None
        """
        new_dir = os.path.join(temp_dir, "newdir")
        
        # First creation should succeed
        assert dir_manager.mk_new_directory(new_dir)
        assert os.path.exists(new_dir)
        
        # Second attempt should fail
        assert not dir_manager.mk_new_directory(new_dir)

    def test_move_file(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        temp_dir: str, 
        test_file: str
    ) -> None:
        """Test moving a file.

        Verifies
        --------
        - Successfully moves file
        - Original file no longer exists
        - Returns True on success
        - Raises FileNotFoundError for invalid source

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        temp_dir : str
            Temporary directory path
        test_file : str
            Path to test file

        Returns
        -------
        None
        """
        new_path = os.path.join(temp_dir, "moved.txt")
        
        # Successful move
        assert dir_manager.move_file(test_file, new_path)
        assert os.path.exists(new_path)
        assert not os.path.exists(test_file)
        
        # Nonexistent source
        with pytest.raises(FileNotFoundError):
            dir_manager.move_file("/nonexistent/file", new_path)

    def test_rename_dir_file(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        temp_dir: str, 
        test_file: str
    ) -> None:
        """Test renaming a file or directory.

        Verifies
        --------
        - Successfully renames file
        - Original name no longer exists
        - Returns True on success
        - Raises FileNotFoundError for invalid source

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        temp_dir : str
            Temporary directory path
        test_file : str
            Path to test file

        Returns
        -------
        None
        """
        new_name = os.path.join(temp_dir, "renamed.txt")
        
        # Successful rename
        assert dir_manager.rename_dir_file(test_file, new_name)
        assert os.path.exists(new_name)
        assert not os.path.exists(test_file)
        
        # Nonexistent source
        with pytest.raises(FileNotFoundError):
            dir_manager.rename_dir_file("/nonexistent/file", new_name)

    def test_removing_dir(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        temp_dir: str
    ) -> None:
        """Test removing a directory.

        Verifies
        --------
        - Successfully removes empty directory
        - Successfully removes non-empty directory
        - Returns True on success
        - Raises FileNotFoundError for invalid path

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        temp_dir : str
            Temporary directory path

        Returns
        -------
        None
        """
        empty_dir = os.path.join(temp_dir, "empty")
        os.mkdir(empty_dir)
        
        # Remove empty directory
        assert dir_manager.removing_dir(empty_dir)
        assert not os.path.exists(empty_dir)
        
        # Remove non-empty directory
        non_empty_dir = os.path.join(temp_dir, "nonempty")
        os.mkdir(non_empty_dir)
        with open(os.path.join(non_empty_dir, "file.txt"), "w") as f:
            f.write("test")
            
        assert dir_manager.removing_dir(non_empty_dir)
        assert not os.path.exists(non_empty_dir)
        
        # Nonexistent directory
        with pytest.raises(FileNotFoundError):
            dir_manager.removing_dir("/nonexistent/dir")

    def test_removing_file(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        test_file: str
    ) -> None:
        """Test removing a file.

        Verifies
        --------
        - Successfully removes file
        - Returns True on success
        - Raises FileNotFoundError for invalid path

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        test_file : str
            Path to test file

        Returns
        -------
        None
        """
        # Successful removal
        assert dir_manager.removing_file(test_file)
        assert not os.path.exists(test_file)
        
        # Nonexistent file
        with pytest.raises(FileNotFoundError):
            dir_manager.removing_file("/nonexistent/file")

    def test_object_exists(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        test_file: str
    ) -> None:
        """Test checking if object exists.

        Verifies
        --------
        - Returns True for existing file
        - Returns False for nonexistent path

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        test_file : str
            Path to test file

        Returns
        -------
        None
        """
        assert dir_manager.object_exists(test_file)
        assert not dir_manager.object_exists("/nonexistent/path")

    def test_time_last_edition(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        test_file: str
    ) -> None:
        """Test getting last modification time.

        Verifies
        --------
        - Returns timestamp for existing file
        - Returns error tuple for nonexistent path
        - Handles datetime conversion flag

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        test_file : str
            Path to test file

        Returns
        -------
        None
        """
        # Existing file
        timestamp, exists = dir_manager.time_last_edition(test_file)
        assert exists
        assert isinstance(timestamp, float)
        
        # With datetime conversion
        dt, exists = dir_manager.time_last_edition(test_file, bool_to_datetime=True)
        assert exists
        assert isinstance(dt, datetime)
        
        # Nonexistent file
        result = dir_manager.time_last_edition("/nonexistent/file")
        assert result == ("INTERNAL ERROR", False)

    def test_get_file_name_path_split(
        self, 
        dir_manager: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test splitting file path into components.

        Verifies
        --------
        - Correctly splits path into directory and filename
        - Handles paths with and without directories

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance

        Returns
        -------
        None
        """
        # Path with directory
        path = "/path/to/file.txt"
        dir_part, file_part = dir_manager.get_file_name_path_split(path)
        assert dir_part == "/path/to"
        assert file_part == "file.txt"
        
        # Just filename
        dir_part, file_part = dir_manager.get_file_name_path_split("file.txt")
        assert dir_part == ""
        assert file_part == "file.txt"

    def test_join_n_path_components(
        self, 
        dir_manager: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test joining path components.

        Verifies
        --------
        - Correctly joins multiple components
        - Handles empty components

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance

        Returns
        -------
        None
        """
        # Multiple components
        path = dir_manager.join_n_path_components("path", "to", "file.txt")
        assert path == os.path.join("path", "to", "file.txt")
        
        # Empty components
        path = dir_manager.join_n_path_components("", "file.txt")
        assert path == os.path.join("", "file.txt")

    def test_get_filename_parts_from_url(
        self, 
        dir_manager: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test extracting filename parts from URL.

        Verifies
        --------
        - Correctly extracts filename and extension
        - Handles URLs with query parameters and fragments

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance

        Returns
        -------
        None
        """
        # Basic URL
        url = "http://example.com/file.txt"
        name, ext = dir_manager.get_filename_parts_from_url(url)
        assert name == "file"
        assert ext == "txt"
        
        # URL with query and fragment
        url = "http://example.com/file.txt?param=value#fragment"
        name, ext = dir_manager.get_filename_parts_from_url(url)
        assert name == "file"
        assert ext == "txt"
        
        # No extension
        url = "http://example.com/file"
        name, ext = dir_manager.get_filename_parts_from_url(url)
        assert name == "file"
        assert ext == ""

    def test_get_file_extensions(
        self, 
        dir_manager: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test getting file extensions from path.

        Verifies
        --------
        - Correctly extracts all extensions
        - Handles multiple extensions
        - Handles no extensions

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance

        Returns
        -------
        None
        """
        # Single extension
        path = "file.txt"
        exts = dir_manager.get_file_extensions(path)
        assert exts == ["txt"]
        
        # Multiple extensions
        path = "file.tar.gz"
        exts = dir_manager.get_file_extensions(path)
        assert exts == ["tar", "gz"]
        
        # No extension
        path = "file"
        exts = dir_manager.get_file_extensions(path)
        assert exts == []

    def test_get_last_file_extension(
        self, 
        dir_manager: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test getting last file extension.

        Verifies
        --------
        - Returns last extension for multi-ext files
        - Returns None for no extension

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance

        Returns
        -------
        None
        """
        # Multiple extensions
        path = "file.tar.gz"
        ext = dir_manager.get_last_file_extension(path)
        assert ext == "gz"
        
        # No extension
        path = "file"
        ext = dir_manager.get_last_file_extension(path)
        assert ext is None

    @patch("pycurl.Curl")
    @patch("wget.download")
    def test_download_web_file(
        self, 
        mock_wget: Any, # noqa ANN401: typing.Any is not allowed
        mock_curl: Any, # noqa ANN401: typing.Any is not allowed
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        temp_dir: str
    ) -> None:
        """Test downloading file from web.

        Verifies
        --------
        - Attempts curl first
        - Falls back to wget if curl fails
        - Creates file at specified path
        - Handles temp file creation

        Parameters
        ----------
        mock_wget : Any
            Mock for wget.download
        mock_curl : Any
            Mock for pycurl.Curl
        dir_manager : Any
            DirFilesManagement instance
        temp_dir : str
            Temporary directory

        Returns
        -------
        None
        """
        url = "http://example.com/file.txt"
        dest_path = os.path.join(temp_dir, "downloaded.txt")
        
        # Test curl success
        mock_curl.return_value.perform.return_value = None
        mock_curl.return_value.getinfo.return_value = 200
        assert dir_manager.download_web_file(url, dest_path)
        assert os.path.exists(dest_path)
        
        # Reset mocks
        mock_curl.reset_mock()
        mock_wget.reset_mock()
        
        # Test curl failure with wget fallback
        mock_curl.return_value.perform.side_effect = pycurl.error("curl error")
        mock_wget.return_value = dest_path
        assert dir_manager.download_web_file(url, dest_path)
        
        # Verify wget was called with correct arguments
        mock_wget.assert_called_once_with(url, out=dest_path)
        
        # Test temp file creation
        mock_wget.reset_mock()
        mock_curl.return_value.perform.side_effect = pycurl.error("curl error")
        result = dir_manager.download_web_file(url)
        assert result
        assert os.path.exists(result)

    def test_unzip_files_from_dir(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        test_zip_file: str
    ) -> None:
        """Test unzipping all files in directory.

        Verifies
        --------
        - Extracts all files from zip
        - Returns list of extracted filenames

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        test_zip_file : str
            Path to test zip file

        Returns
        -------
        None
        """
        zip_dir = os.path.dirname(test_zip_file)
        result = dir_manager.unzip_files_from_dir(zip_dir)
        
        assert isinstance(result, list)
        assert len(result) == 1  # One zip file processed
        assert isinstance(result[0], list)
        assert "file1.txt" in result[0]
        assert "file2.txt" in result[0]
        
        # Verify files were extracted
        assert os.path.exists(os.path.join(zip_dir, "file1.txt"))
        assert os.path.exists(os.path.join(zip_dir, "file2.txt"))

    def test_unzip_file(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        test_zip_file: str, 
        temp_dir: str
    ) -> None:
        """Test unzipping single file.

        Verifies
        --------
        - Extracts all files from zip
        - Returns list of extracted filenames

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        test_zip_file : str
            Path to test zip file
        temp_dir : str
            Temporary directory

        Returns
        -------
        None
        """
        dest_dir = os.path.join(temp_dir, "extracted")
        os.mkdir(dest_dir)
        
        result = dir_manager.unzip_file(test_zip_file, dest_dir)
        
        assert isinstance(result, list)
        assert "file1.txt" in result
        assert "file2.txt" in result
        
        # Verify files were extracted
        assert os.path.exists(os.path.join(dest_dir, "file1.txt"))
        assert os.path.exists(os.path.join(dest_dir, "file2.txt"))

    def test_compress_to_zip(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        test_file: str, 
        temp_dir: str
    ) -> None:
        """Test compressing files to zip.

        Verifies
        --------
        - Creates zip file
        - Contains specified files
        - Returns True on success

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        test_file : str
            Path to test file
        temp_dir : str
            Temporary directory

        Returns
        -------
        None
        """
        zip_path = os.path.join(temp_dir, "archive.zip")
        assert dir_manager.compress_to_zip([test_file], zip_path)
        assert os.path.exists(zip_path)
        
        # Verify zip contents
        with zipfile.ZipFile(zip_path, "r") as zf:
            assert "test.txt" in zf.namelist()

    def test_compress_to_7z_file(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        test_file: str, 
        temp_dir: str
    ) -> None:
        """Test compressing files to 7z.

        Verifies
        --------
        - Creates 7z file
        - Returns True on success

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        test_file : str
            Path to test file
        temp_dir : str
            Temporary directory

        Returns
        -------
        None
        """
        archive_path = os.path.join(temp_dir, "archive.7z")
        assert dir_manager.compress_to_7z_file(archive_path, test_file)
        assert os.path.exists(archive_path)

    def test_decompress_7z_file(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        test_7z_file: str
    ) -> None:
        """Test decompressing 7z file.

        Verifies
        --------
        - Extracts files from 7z archive
        - Returns list of extracted filenames

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        test_7z_file : str
            Path to test 7z file

        Returns
        -------
        None
        """
        result = dir_manager.decompress_7z_file(test_7z_file)
        assert isinstance(result, list)
        assert "test.txt" in result

    def test_choose_last_saved_file_w_rule(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        temp_dir: str
    ) -> None:
        """Test finding most recently modified file matching pattern.

        Verifies
        --------
        - Returns most recent matching file
        - Returns False when no matches

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        temp_dir : str
            Temporary directory

        Returns
        -------
        None
        """
        # create test files with different modification times
        file1 = os.path.join(temp_dir, "file1.txt")
        file2 = os.path.join(temp_dir, "file2.txt")
        
        with open(file1, "w") as f:
            f.write("older")
        
        with open(file2, "w") as f:
            f.write("newer")
        
        # ensure file2 is newer
        os.utime(file2, (os.path.getatime(file1), os.path.getmtime(file1) + 10))
        
        # test finding newest matching file
        result = dir_manager.choose_last_saved_file_w_rule(temp_dir, "*.txt")
        assert result == file2
        
        # test no matches
        result = dir_manager.choose_last_saved_file_w_rule(temp_dir, "*.pdf")
        assert result is False

    def test_copy_file(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        test_file: str, 
        temp_dir: str
    ) -> None:
        """Test copying a file.

        Verifies
        --------
        - Creates copy of file
        - Returns True on success
        - Returns error message for nonexistent source

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        test_file : str
            Path to test file
        temp_dir : str
            Temporary directory

        Returns
        -------
        None
        """
        dest_path = os.path.join(temp_dir, "copied.txt")
        
        # Successful copy
        assert dir_manager.copy_file(test_file, dest_path) is True
        assert os.path.exists(dest_path)
        
        # Nonexistent source
        result = dir_manager.copy_file("/nonexistent/file", dest_path)
        assert result == "NO ORIGINAL FILE"

    def test_walk_folder_subfolder_w_rule(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        temp_dir: str
    ) -> None:
        """Test recursive file search with pattern matching.

        Verifies
        --------
        - Finds files matching patterns
        - Returns full paths

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        temp_dir : str
            Temporary directory

        Returns
        -------
        None
        """
        # Create test files
        os.makedirs(os.path.join(temp_dir, "subdir"))
        txt_file = os.path.join(temp_dir, "file.txt")
        pdf_file = os.path.join(temp_dir, "subdir", "file.pdf")
        
        with open(txt_file, "w") as f:
            f.write("test")
        with open(pdf_file, "w") as f:
            f.write("test")
        
        # Test pattern matching
        result = dir_manager.walk_folder_subfolder_w_rule(temp_dir, ["*.txt"])
        assert txt_file in result
        assert pdf_file not in result
        
        # Multiple patterns
        result = dir_manager.walk_folder_subfolder_w_rule(temp_dir, ["*.txt", "*.pdf"])
        assert txt_file in result
        assert pdf_file in result

    def test_walk_folder_subfolder(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        temp_dir: str
    ) -> None:
        """Test recursive file listing.

        Verifies
        --------
        - Finds all files recursively
        - Returns full paths

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        temp_dir : str
            Temporary directory

        Returns
        -------
        None
        """
        # Create test files
        os.makedirs(os.path.join(temp_dir, "subdir"))
        file1 = os.path.join(temp_dir, "file1.txt")
        file2 = os.path.join(temp_dir, "subdir", "file2.txt")
        
        with open(file1, "w") as f:
            f.write("test")
        with open(file2, "w") as f:
            f.write("test")
        
        result = dir_manager.walk_folder_subfolder(temp_dir)
        assert file1 in result
        assert file2 in result

    def test_loop_files_w_rule(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        temp_dir: str
    ) -> None:
        """Test finding files with pattern matching and sorting.

        Verifies
        --------
        - Finds files matching pattern
        - Can sort by modification time
        - Handles datetime conversion

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        temp_dir : str
            Temporary directory

        Returns
        -------
        None
        """
        # Create test files with different modification times
        file1 = os.path.join(temp_dir, "file1.txt")
        file2 = os.path.join(temp_dir, "file2.txt")
        
        with open(file1, "w") as f:
            f.write("older")
        
        with open(file2, "w") as f:
            f.write("newer")
        
        # Ensure file2 is newer
        os.utime(file2, (os.path.getatime(file1), os.path.getmtime(file1) + 10))
        
        # Test without sorting
        result = dir_manager.loop_files_w_rule(temp_dir, "*.txt", bool_l_first_last_edited=False)
        assert isinstance(result, list)
        assert len(result) == 2
        
        # Test with sorting (newest first)
        result = dir_manager.loop_files_w_rule(temp_dir, "*.txt")
        assert result[0].endswith("file2.txt")  # Newest first
        assert result[1].endswith("file1.txt")

    def test_find_project_root(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        temp_dir: str
    ) -> None:
        """Test finding project root by marker file.

        Verifies
        --------
        - Finds marker file in parent directories
        - Raises FileNotFoundError when not found

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        temp_dir : str
            Temporary directory

        Returns
        -------
        None
        """
        # Create mock project structure
        project_dir = os.path.join(temp_dir, "project")
        sub_dir = os.path.join(project_dir, "subdir")
        os.makedirs(sub_dir)
        
        # Create marker file
        with open(os.path.join(project_dir, "pyproject.toml"), "w") as f:
            f.write("[tool.poetry]")
        
        # Test finding from subdirectory
        result = dir_manager.find_project_root("pyproject.toml", start_path=project_dir)
        assert str(result) == project_dir
        
        # Test not found
        with pytest.raises(FileNotFoundError):
            dir_manager.find_project_root("nonexistent.marker")

    def test_get_file_format_from_file_name(
        self, 
        dir_manager: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test getting file extension.

        Verifies
        --------
        - Returns last extension
        - Handles no extension

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance

        Returns
        -------
        None
        """
        # With extension
        assert dir_manager.get_file_format_from_file_name("file.txt") == "txt"
        
        # No extension
        assert dir_manager.get_file_format_from_file_name("file") == "file"

    def test_get_file_size(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        test_file: str
    ) -> None:
        """Test getting file size.

        Verifies
        --------
        - Returns size in bytes

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        test_file : str
            Path to test file

        Returns
        -------
        None
        """
        size = dir_manager.get_file_size(test_file)
        assert isinstance(size, int)
        assert size > 0

    def test_recursive_extract_zip(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        test_zip_file: str, 
        temp_dir: str
    ) -> None:
        """Test recursive zip extraction.

        Verifies
        --------
        - Extracts nested zip files
        - Removes zip files after extraction

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        test_zip_file : str
            Path to test zip file
        temp_dir : str
            Temporary directory

        Returns
        -------
        None
        """
        # Create nested zip
        nested_zip = os.path.join(temp_dir, "nested.zip")
        with zipfile.ZipFile(nested_zip, "w") as zf:
            zf.writestr("inner.txt", "content")
        
        # Add nested zip to test zip
        with zipfile.ZipFile(test_zip_file, "a") as zf:
            zf.write(nested_zip, "nested.zip")
        
        # Test extraction
        dir_manager.recursive_extract_zip(test_zip_file, temp_dir)
        
        # Verify files were extracted
        assert os.path.exists(os.path.join(temp_dir, "file1.txt"))
        assert os.path.exists(os.path.join(temp_dir, "file2.txt"))
        assert os.path.exists(os.path.join(temp_dir, "inner.txt"))
        
        # Verify zips were removed
        assert not os.path.exists(test_zip_file)
        assert not os.path.exists(nested_zip)

    def test_get_file_from_zip(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        mock_response: Response, 
        temp_dir: str
    ) -> None:
        """Test extracting specific file from zip response.

        Verifies
        --------
        - Extracts zip from response
        - Finds file matching extension
        - Returns path to found file
        - Raises ValueError when no match

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        mock_response : Response
            Mocked Response object with zip content
        temp_dir : str
            Temporary directory for extraction

        Returns
        -------
        None
        """
        # Mock zip file with known content
        with patch.object(dir_manager, "recursive_extract_zip"):
            # Create test file in temp dir
            test_file = os.path.join(temp_dir, "test.txt")
            with open(test_file, "w") as f:
                f.write("test")
            
            # Test successful extraction
            result = dir_manager.get_file_from_zip(mock_response, temp_dir, (".txt",))
            assert result == test_file
            
            # Test no match
            with pytest.raises(ValueError):
                dir_manager.get_file_from_zip(mock_response, temp_dir, (".pdf",))

    def test_get_zip_from_web_in_memory(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        mock_response: Response
    ) -> None:
        """Test extracting zip contents in memory.

        Verifies
        --------
        - Returns file-like object(s)
        - Handles text wrapper flag
        - Returns list for multiple files

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        mock_response : Response
            Mocked Response object with zip content

        Returns
        -------
        None
        """
        # Test without text wrapper
        result = dir_manager.get_zip_from_web_in_memory(mock_response)
        assert hasattr(result, 'read')  # Check it's a file-like object instead of specific type
        
        # Test with text wrapper
        result = dir_manager.get_zip_from_web_in_memory(mock_response, bool_l_io_interpreting=True)
        assert isinstance(result, TextIOWrapper)
        
        # Test multiple files
        # Create a new mock with multiple files
        mock_resp = MagicMock(spec=Response)
        buffer = BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("file1.txt", "content1")
            zf.writestr("file2.txt", "content2")
        mock_resp.content = buffer.getvalue()
        result = dir_manager.get_zip_from_web_in_memory(mock_resp)
        assert isinstance(result, list)
        assert len(result) == 2
        assert all(hasattr(f, 'read') for f in result)

    def test_calculate_file_hash(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        test_file: str
    ) -> None:
        """Test calculating file hash.

        Verifies
        --------
        - Returns hex digest string
        - Supports different algorithms

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        test_file : str
            Path to test file

        Returns
        -------
        None
        """
        hash1 = dir_manager.calculate_file_hash(test_file)
        assert isinstance(hash1, str)
        assert len(hash1) == 64  # sha256 hex digest length
        
        hash2 = dir_manager.calculate_file_hash(test_file, "md5")
        assert isinstance(hash2, str)
        assert len(hash2) == 32  # md5 hex digest length

    def test_validate_file_hash(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        test_file: str
    ) -> None:
        """Test validating file against hash.

        Verifies
        --------
        - Returns True for matching hash
        - Returns False for non-matching hash

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        test_file : str
            Path to test file

        Returns
        -------
        None
        """
        # Get known hash
        known_hash = dir_manager.calculate_file_hash(test_file)
        
        # Test valid hash
        assert dir_manager.validate_file_hash(test_file, known_hash)
        
        # Test invalid hash
        assert not dir_manager.validate_file_hash(test_file, "invalidhash")

    def test_extract_file(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        test_zip_file: str, 
        temp_dir: str
    ) -> None:
        """Test extracting archive file.

        Verifies
        --------
        - Extracts zip files
        - Returns True on success
        - Returns False on failure
        - Raises ValueError for unsupported format

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        test_zip_file : str
            Path to test zip file
        temp_dir : str
            Temporary directory for extraction

        Returns
        -------
        None
        """
        dest_dir = os.path.join(temp_dir, "extracted")
        os.mkdir(dest_dir)
        
        # Test zip extraction
        assert dir_manager.extract_file(test_zip_file, dest_dir, "zip")
        assert os.path.exists(os.path.join(dest_dir, "file1.txt"))
        
        # Test unsupported format
        with pytest.raises(TypeError, match="must be one of"):
            dir_manager.extract_file(test_zip_file, dest_dir, "invalid")

    def test_get_file_metadata(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        test_file: str
    ) -> None:
        """Test getting file metadata.

        Verifies
        --------
        - Returns dictionary with expected keys
        - Contains valid timestamp values

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        test_file : str
            Path to test file

        Returns
        -------
        None
        """
        metadata = dir_manager.get_file_metadata(test_file)
        
        assert isinstance(metadata, dict)
        assert "size" in metadata
        assert "creation_time" in metadata
        assert "modification_time" in metadata
        assert "access_time" in metadata
        
        assert isinstance(metadata["size"], int)
        assert isinstance(metadata["creation_time"], datetime)
        assert isinstance(metadata["modification_time"], datetime)
        assert isinstance(metadata["access_time"], datetime)

    def test_stream_file(
        self, 
        dir_manager: Any, # noqa ANN401: typing.Any is not allowed
        mock_response: Response
    ) -> None:
        """Test streaming file in chunks.

        Verifies
        --------
        - Yields chunks of content
        - Respects chunk size

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance
        mock_response : Response
            Mocked Response object with content

        Returns
        -------
        None
        """
        mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
        
        chunks = list(dir_manager.stream_file(mock_response, chunk_size=8192))
        assert chunks == [b"chunk1", b"chunk2"]
        mock_response.iter_content.assert_called_once_with(chunk_size=8192)

    def test_check_separator_consistency(
        self, 
        dir_manager: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test checking separator consistency in content.

        Verifies
        --------
        - Detects consistent separators
        - Returns False for inconsistent content

        Parameters
        ----------
        dir_manager : Any
            DirFilesManagement instance

        Returns
        -------
        None
        """
        # Consistent comma-separated content
        content = b"a,b,c\n1,2,3\n"
        assert dir_manager.check_separator_consistency(content, list_sep=[","])
        
        # Inconsistent content
        content = b"a,b,c\n1,2\n"
        assert not dir_manager.check_separator_consistency(content, list_sep=[","])
        
        # Test with multiple separators
        content = b"a;b;c\n1;2;3\n"
        assert dir_manager.check_separator_consistency(content, list_sep=[",", ";"])
        
        # Test with skip rows
        content = b"header\na,b,c\n1,2,3\nfooter\n"
        assert dir_manager.check_separator_consistency(
            content, 
            int_skip_rows=1, 
            int_skip_footer=1, 
            list_sep=[","]
        )


# --------------------------
# Tests for FoldersTree
# --------------------------
class TestFoldersTree:
    """Test cases for FoldersTree class."""

    @pytest.fixture
    def folder_structure(self, temp_dir: str) -> tuple[str, list[str]]:
        """Create test folder structure.

        Parameters
        ----------
        temp_dir : str
            Temporary directory path

        Returns
        -------
        tuple[str, list[str]]
            Root path and list of created file paths
        """
        # Create structure
        os.makedirs(os.path.join(temp_dir, "subdir"))
        file1 = os.path.join(temp_dir, "file1.txt")
        file2 = os.path.join(temp_dir, "subdir", "file2.txt")
        
        with open(file1, "w") as f:
            f.write("test")
        with open(file2, "w") as f:
            f.write("test")
        
        return temp_dir, [file1, file2]

    def test_generate_tree(self, folder_structure: tuple[str, list[str]]) -> None:
        """Test generating directory tree.

        Verifies
        --------
        - Returns string representation
        - Contains expected structure
        - Handles markdown line breaks

        Parameters
        ----------
        folder_structure : tuple[str, list[str]]
            Tuple containing root directory and list of file paths

        Returns
        -------
        None
        """
        root_dir, _ = folder_structure
        tree = FoldersTree(root_dir)
        
        # Basic tree
        result = tree.generate_tree()
        assert isinstance(result, str)
        assert "file1.txt" in result
        assert "subdir" in result
        assert "file2.txt" in result
        
        # With markdown line breaks
        tree = FoldersTree(root_dir, bool_l_add_linebreak_markdown=True)
        result = tree.generate_tree()
        assert "<br>" in result

    def test_print_tree(
        self, 
        folder_structure: tuple[str, list[str]], 
        capsys: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test printing directory tree.

        Verifies
        --------
        - Prints tree to stdout

        Parameters
        ----------
        folder_structure : tuple[str, list[str]]
            Tuple containing root directory and list of file paths
        capsys : Any
            pytest capsys fixture

        Returns
        -------
        None
        """
        root_dir, _ = folder_structure
        tree = FoldersTree(root_dir)
        tree.print_tree()
        
        captured = capsys.readouterr()
        assert "file1.txt" in captured.out

    def test_export_tree(self, folder_structure: tuple[str, list[str]], temp_dir: str) -> None:
        """Test exporting directory tree.

        Verifies
        --------
        - Writes tree to file when filename provided
        - Returns tree as string when no filename

        Parameters
        ----------
        folder_structure : tuple[str, list[str]]
            Tuple containing root directory and list of file paths
        temp_dir : str
            Temporary directory for output file

        Returns
        -------
        None
        """
        root_dir, _ = folder_structure
        tree = FoldersTree(root_dir)
        
        # Export to file
        output_file = os.path.join(temp_dir, "tree.txt")
        tree.export_tree(output_file)
        assert os.path.exists(output_file)
        
        # Return as string
        result = tree.export_tree()
        assert isinstance(result, str)
        assert "file1.txt" in result