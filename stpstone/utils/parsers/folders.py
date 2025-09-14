"""Directory and file management utilities.

This module provides classes for handling file system operations including directory traversal,
file manipulation, compression/decompression, and remote file operations.
"""

from collections.abc import Iterable
from datetime import datetime
import fnmatch
import hashlib
from io import BufferedReader, BytesIO, StringIO, TextIOWrapper
import os
from pathlib import Path
import shutil
import tarfile
import tempfile
from typing import Literal, Optional, TypedDict, Union
from zipfile import ZIP_DEFLATED, ZipExtFile, ZipFile

import chardet
import py7zr
import pycurl
from requests import Response
import wget

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.str import StrHandler


class ReturnGetFileMetadata(TypedDict):
    """Typed dictionary for file metadata return values.

    Attributes
    ----------
    size : int
        File size in bytes
    creation_time : datetime
        File creation timestamp
    modification_time : datetime
        Last modification timestamp
    access_time : datetime
        Last access timestamp
    """

    size: int
    creation_time: datetime
    modification_time: datetime
    access_time: datetime


class DirFilesManagement(metaclass=TypeChecker):
    """Class for directory and file management operations."""

    def _validate_safe_path(self, base_path: str, file_path: str) -> bool:
        """Validate that a file path is safe to extract.

        Parameters
        ----------
        base_path : str
            The base directory where files should be extracted
        file_path : str
            The file path to validate

        Returns
        -------
        bool
            True if the path is safe, False otherwise
        """
        base_path = os.path.abspath(base_path)
        file_path = os.path.abspath(os.path.join(base_path, file_path))
        return file_path.startswith(base_path)

    def get_curr_dir(self) -> str:
        """Get current working directory.

        Returns
        -------
        str
            Current working directory path
        """
        return os.getcwd()

    def list_dir_files(self, dir_path: Optional[str] = None) -> list[str]:
        """List files and subdirectories in given directory.

        Parameters
        ----------
        dir_path : Optional[str]
            Directory path (defaults to current directory if None)

        Returns
        -------
        list[str]
            List of files and subdirectories
        """
        return os.listdir(dir_path if dir_path else self.get_curr_dir)

    def change_dir(self, dir_path: str) -> None:
        """Change current working directory.

        Parameters
        ----------
        dir_path : str
            Target directory path

        Raises
        ------
        FileNotFoundError
            If directory doesn't exist
        """
        if not os.path.exists(dir_path):
            raise FileNotFoundError(f"Directory not found: {dir_path}")
        os.chdir(dir_path)

    def mk_new_directory(self, dir_path: str) -> bool:
        """Create new directory.

        Parameters
        ----------
        dir_path : str
            Path of directory to create

        Returns
        -------
        bool
            True if created, False if already existed
        """
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)
            return True
        return False

    def move_file(self, old_file_name: str, new_file_name: str) -> bool:
        """Move/rename a file.

        Parameters
        ----------
        old_file_name : str
            Source file path
        new_file_name : str
            Destination file path

        Returns
        -------
        bool
            True if successful, False otherwise
        """
        self._validate_file_exists(old_file_name)
        shutil.move(old_file_name, new_file_name)
        return self.object_exists(new_file_name)

    def rename_dir_file(self, old_object_name: str, new_object_name: str) -> bool:
        """Rename a file or directory.

        Parameters
        ----------
        old_object_name : str
            Current path
        new_object_name : str
            New path

        Returns
        -------
        bool
            True if successful, False otherwise
        """
        self._validate_object_exists(old_object_name)
        os.rename(old_object_name, new_object_name)
        return self.object_exists(new_object_name)

    def removing_dir(self, dir_path: str) -> bool:
        """Remove a directory.

        Parameters
        ----------
        dir_path : str
            Directory path to remove

        Returns
        -------
        bool
            True if successful, False otherwise
        """
        self._validate_object_exists(dir_path)
        if len(self.list_dir_files(dir_path)) == 0:
            os.rmdir(dir_path)
        else:
            shutil.rmtree(dir_path)
        return not os.path.exists(dir_path)

    def removing_file(self, file_path: str) -> bool:
        """Remove a file.

        Parameters
        ----------
        file_path : str
            File path to remove

        Returns
        -------
        bool
            True if successful, False otherwise
        """
        self._validate_file_exists(file_path)
        os.remove(file_path)
        return not os.path.exists(file_path)

    def object_exists(self, object_path: str) -> bool:
        """Check if file/directory exists.

        Parameters
        ----------
        object_path : str
            Path to check

        Returns
        -------
        bool
            True if exists, False otherwise
        """
        return os.path.exists(object_path)

    def time_last_edition(self, object_path: str, bool_to_datetime: bool = False) -> tuple:
        """Get last modification time of file/directory.

        Parameters
        ----------
        object_path : str
            Path to check
        bool_to_datetime : bool
            Whether to return as datetime object

        Returns
        -------
        tuple
            (timestamp, exists_flag)
        """
        if not self.object_exists(object_path):
            return ("INTERNAL ERROR", False)
        timestamp = os.path.getmtime(object_path)
        return (datetime.fromtimestamp(timestamp) if bool_to_datetime else timestamp, True)

    def time_creation(self, object_path: str) -> tuple:
        """Get creation time of file/directory.

        Parameters
        ----------
        object_path : str
            Path to check

        Returns
        -------
        tuple
            (timestamp, exists_flag)
        """
        if not self.object_exists(object_path):
            return ("INTERNAL ERROR", False)
        return (os.path.getctime(object_path), True)

    def time_last_access(self, object_path: str) -> tuple:
        """Get last access time of file/directory.

        Parameters
        ----------
        object_path : str
            Path to check

        Returns
        -------
        tuple
            (timestamp, exists_flag)
        """
        if not self.object_exists(object_path):
            return ("INTERNAL ERROR", False)
        return (os.path.getatime(object_path), True)

    def get_file_name_path_split(self, complete_file_name: str) -> tuple[str, str]:
        """Split file path into directory and filename components.

        Parameters
        ----------
        complete_file_name : str
            Full file path

        Returns
        -------
        tuple[str, str]
            (directory_path, filename)
        """
        return os.path.split(complete_file_name)

    def join_n_path_components(self, *path_components: str) -> str:
        """Join multiple path components.

        Parameters
        ----------
        *path_components : str
            Path components to join

        Returns
        -------
        str
            Combined path
        """
        path_output = ""
        for path_component in path_components:
            path_output = os.path.join(path_output, path_component)
        return path_output

    def get_filename_parts_from_url(self, url: str) -> list[str]:
        """Extract filename parts from URL.

        Parameters
        ----------
        url : str
            URL to parse

        Returns
        -------
        list[str]
            [filename_without_ext, extension]
        """
        fullname = url.split("/")[-1].split("#")[0].split("?")[0]
        parts = list(os.path.splitext(fullname))
        if parts[1]:
            parts[1] = parts[1][1:]
        return parts

    def get_file_extensions(self, file_path: str) -> list[str]:
        """Get all extensions from a file path.

        Parameters
        ----------
        file_path : str
            File path to parse

        Returns
        -------
        list[str]
            List of file extensions
        """
        parts = os.path.basename(file_path).split('.')
        return parts[1:] if len(parts) > 1 else []

    def get_last_file_extension(self, file_path: str) -> Optional[str]:
        """Get the last extension from a file path.

        Parameters
        ----------
        file_path : str
            File path to parse

        Returns
        -------
        Optional[str]
            Last extension or None if no extensions
        """
        extensions = self.get_file_extensions(file_path)
        return extensions[-1] if extensions else None

    def download_web_file(self, url: str, filepath: Optional[str] = None) -> bool:
        """Download file from web.

        Parameters
        ----------
        url : str
            URL to download from
        filepath : Optional[str]
            Destination path (uses temp file if None)

        Returns
        -------
        bool
            True if successful, False otherwise


        Raises
        ------
        pycurl.error
            If pycurl fails
        """
        if filepath and self.object_exists(filepath):
            self.removing_file(filepath)

        if not filepath:
            _, suffix = self.get_filename_parts_from_url(url)
            with tempfile.NamedTemporaryFile(suffix=f".{suffix}", delete=False) as f:
                filepath = f.name

        try:
            with pycurl.Curl() as c:
                c.setopt(pycurl.URL, str(url))
                with open(filepath, "wb") as f:
                    c.setopt(pycurl.WRITEFUNCTION, f)
                    c.perform()
                # Check HTTP status code
                if c.getinfo(pycurl.HTTP_CODE) != 200:
                    raise pycurl.error(f"HTTP error: {c.getinfo(pycurl.HTTP_CODE)}")
        except pycurl.error:
            try:
                # Use wget as fallback
                wget.download(url, out=filepath)
            except Exception:
                return False

        return self.object_exists(filepath)

    def unzip_files_from_dir(self, destination_path: str) -> list[list[str]]:
        """Unzip all zip files in a directory.

        Parameters
        ----------
        destination_path : str
            Directory containing zip files

        Returns
        -------
        list[list[str]]
            List of lists of extracted filenames

        Raises
        ------
        ValueError
            If path traversal is detected
        """
        list_files_unz = []
        for file in os.listdir(destination_path):
            if file.endswith(".zip"):
                file_path = os.path.join(destination_path, file)
                with ZipFile(file_path) as zip_file:
                    list_files_unz.append(zip_file.namelist())
                    for name in zip_file.namelist():
                        if not self._validate_safe_path(destination_path, name):
                            raise ValueError(f"Path traversal detected in zip file: {name}")
                        zip_file.extract(name, destination_path)
        return list_files_unz

    def unzip_file(self, zippedfile_path: str, dir_destiny: str) -> list[str]:
        """Unzip a single zip file.

        Parameters
        ----------
        zippedfile_path : str
            Path to zip file
        dir_destiny : str
            Destination directory

        Returns
        -------
        list[str]
            List of extracted filenames

        Raises
        ------
        ValueError
            If path traversal is detected
        """
        with ZipFile(zippedfile_path, "r") as zipobj:
            list_zip_files = zipobj.namelist()
            for name in list_zip_files:
                if not self._validate_safe_path(dir_destiny, name):
                    raise ValueError(f"Path traversal detected in zip file: {name}")
                zipobj.extract(name, dir_destiny)
        return list_zip_files

    def compress_to_zip(self, list_files_archive: list[str], zfilename: str) -> bool:
        """Compress files to zip archive.

        Parameters
        ----------
        list_files_archive : list[str]
            List of files to compress
        zfilename : str
            Output zip filename

        Returns
        -------
        bool
            True if successful
        """
        with ZipFile(zfilename, "w", ZIP_DEFLATED) as zout:
            for fname in list_files_archive:
                zout.write(fname, arcname=os.path.basename(fname))
        return True

    def compress_to_7z_file(
        self, 
        file_path_7z: str, 
        object_to_compress: str, 
        method: str = "w"
    ) -> bool:
        """Compress files to 7z archive.

        Parameters
        ----------
        file_path_7z : str
            Output 7z filename
        object_to_compress : str
            Path to file/directory to compress
        method : str
            Write mode (default: "w")

        Returns
        -------
        bool
            True if successful
        """
        with py7zr.SevenZipFile(file_path_7z, mode=method) as archive:
            archive.writeall(object_to_compress)
        return self.object_exists(file_path_7z)

    def decompress_7z_file(self, file_path_7z: str, method: str = "r") -> list[str]:
        """Decompress 7z archive.

        Parameters
        ----------
        file_path_7z : str
            7z file to extract
        method : str
            Read mode (default: "r")

        Returns
        -------
        list[str]
            List of extracted filenames

        Raises
        ------
        ValueError
            If path traversal is detected
        """
        with py7zr.SevenZipFile(file_path_7z, mode=method) as archive:
            list_file_names = archive.getnames()
            for name in list_file_names:
                if not self._validate_safe_path(os.path.dirname(file_path_7z), name):
                    raise ValueError(f"Path traversal detected in 7z file: {name}")
                archive.extract(path=os.path.dirname(file_path_7z), targets=[name])
        return list_file_names

    def choose_last_saved_file_w_rule(self, parent_dir: str, name_like: str) -> Union[str, bool]:
        """Find most recently modified file matching pattern.

        Parameters
        ----------
        parent_dir : str
            Directory to search
        name_like : str
            Filename pattern to match

        Returns
        -------
        Union[str, bool]
            Path of matching file or False if none found
        """
        file_name_return = None
        for file_dir in os.listdir(parent_dir):
            if fnmatch.fnmatch(file_dir, name_like):
                if file_name_return is None:
                    file_name_return = file_dir
                else:
                    current_mtime = os.path.getmtime(os.path.join(parent_dir, file_dir))
                    stored_mtime = os.path.getmtime(os.path.join(parent_dir, file_name_return))
                    if current_mtime > stored_mtime:
                        file_name_return = file_dir
        return os.path.join(parent_dir, file_name_return) if file_name_return else False

    def copy_file(self, org_file_path: str, dest_direcory: str) -> Union[bool, str]:
        """Copy file to destination.

        Parameters
        ----------
        org_file_path : str
            Source file path
        dest_direcory : str
            Destination directory

        Returns
        -------
        Union[bool, str]
            True if successful, "NO ORIGINAL FILE" if source doesn't exist
        """
        if not self.object_exists(org_file_path):
            return "NO ORIGINAL FILE"
        shutil.copy(org_file_path, dest_direcory)
        return True

    def walk_folder_subfolder_w_rule(
        self, 
        root_directory: str, 
        list_name_like: list[str]
    ) -> list[str]:
        """Recursively find files matching patterns.

        Parameters
        ----------
        root_directory : str
            Directory to search
        list_name_like : list[str]
            List of filename patterns to match

        Returns
        -------
        list[str]
            List of matching file paths
        """
        list_paths = []
        for directory, _, files in os.walk(root_directory):
            for file in files:
                if any(fnmatch.fnmatch(file, name_like) for name_like in list_name_like):
                    list_paths.append(os.path.join(directory, file))
        return list_paths

    def walk_folder_subfolder(self, root_directory: str) -> list[str]:
        """Recursively list all files in directory.

        Parameters
        ----------
        root_directory : str
            Directory to search

        Returns
        -------
        list[str]
            List of all file paths
        """
        return [
            os.path.join(directory, file)
            for directory, _, files in os.walk(root_directory)
            for file in files
        ]

    def loop_files_w_rule(
        self,
        directory: str,
        name_like: str,
        bool_first_last_edited: bool = True,
        bool_to_datetime: bool = True,
        key_file_name: str = "file_name",
        key_file_last_edition: str = "file_last_edition"
    ) -> list[str]:
        """Get files matching pattern, optionally sorted by modification time.

        Parameters
        ----------
        directory : str
            Directory to search
        name_like : str
            Filename pattern to match
        bool_first_last_edited : bool
            Whether to sort by modification time
        bool_to_datetime : bool
            Whether to return timestamps as datetime objects
        key_file_name : str
            Dictionary key for filename
        key_file_last_edition : str
            Dictionary key for modification time

        Returns
        -------
        list[str]
            List of matching file paths
        """
        list_files_names_like = [
            file_name for file_name in os.listdir(directory)
            if StrHandler().match_string_like(file_name, name_like)
        ]

        if not bool_first_last_edited:
            return list_files_names_like

        list_files_last_edition = [
            self.time_last_edition(
                os.path.join(directory, file_name),
                bool_to_datetime=bool_to_datetime
            )
            for file_name in list_files_names_like
        ]

        list_ser_file_name_last_edition = [
            {
                key_file_name: list_files_names_like[i],
                key_file_last_edition: list_files_last_edition[i][0]
            }
            for i in range(len(list_files_last_edition))
        ]

        return [
            os.path.join(directory, dict_[key_file_name])
            for dict_ in HandlingDicts().multikeysort(
                list_ser_file_name_last_edition,
                [f"-{key_file_last_edition}"]
            )
        ]

    def find_project_root(
        self, 
        marker: str = "pyproject.toml", 
        start_path: Optional[str] = None
    ) -> Path:
        """Find project root directory by marker file.

        Parameters
        ----------
        marker : str
            Filename to identify project root
        start_path : Optional[str]
            Directory to start search

        Returns
        -------
        Path
            Path to project root

        Raises
        ------
        FileNotFoundError
            If marker file not found
        """
        current_path = Path(start_path) if start_path else Path(__file__).resolve()
        while current_path != current_path.parent:
            if (current_path / marker).exists():
                return current_path
            current_path = current_path.parent
        raise FileNotFoundError(f"Could not find project root with marker: {marker}")

    def get_file_format_from_file_name(self, filename: str) -> str:
        """Get file extension from filename.

        Parameters
        ----------
        filename : str
            Filename to parse

        Returns
        -------
        str
            File extension
        """
        return filename.split(".")[-1]

    def get_file_size(self, filename: str) -> int:
        """Get file size in bytes.

        Parameters
        ----------
        filename : str
            File to check

        Returns
        -------
        int
            File size in bytes
        """
        return os.path.getsize(filename)

    def recursive_extract_zip(self, zip_file_path: str, extract_dir: str) -> None:
        """Recursively extract zip files including nested zips.

        Parameters
        ----------
        zip_file_path : str
            Zip file to extract
        extract_dir : str
            Destination directory

        Raises
        ------
        ValueError
            If path traversal is detected
        """
        with ZipFile(zip_file_path, "r") as zip_ref:
            for name in zip_ref.namelist():
                if not self._validate_safe_path(extract_dir, name):
                    raise ValueError(f"Path traversal detected in zip file: {name}")
                zip_ref.extract(name, extract_dir)
        os.remove(zip_file_path)
        for root_path, _, files in os.walk(extract_dir):
            for file in files:
                if file.endswith(".zip"):
                    nested_zip_path = os.path.join(root_path, file)
                    self.recursive_extract_zip(nested_zip_path, root_path)

    def _validate_file_exists(self, file_path: str) -> None:
        """Validate that a file exists.

        Parameters
        ----------
        file_path : str
            Path to validate

        Raises
        ------
        FileNotFoundError
            If file doesn't exist
        """
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

    def _validate_object_exists(self, object_path: str) -> None:
        """Validate that a file/directory exists.

        Parameters
        ----------
        object_path : str
            Path to validate

        Raises
        ------
        FileNotFoundError
            If object doesn't exist
        """
        if not os.path.exists(object_path):
            raise FileNotFoundError(f"Path not found: {object_path}")

    def get_file_from_zip(
        self,
        resp_req: Response,
        path_dir: Union[str, tempfile.TemporaryDirectory, Path],
        tup_endswith: tuple[str]
    ) -> str:
        """Extract file from zip response matching extensions.

        Parameters
        ----------
        resp_req : Response
            HTTP response containing zip file
        path_dir : Union[str, tempfile.TemporaryDirectory, Path]
            Extraction directory
        tup_endswith : tuple[str]
            Tuple of valid file extensions

        Returns
        -------
        str
            Path to extracted file

        Raises
        ------
        ValueError
            If no matching file found
        """
        zip_file_path = os.path.join(path_dir, "archive.zip")
        with open(zip_file_path, "wb") as zip_file:
            zip_file.write(resp_req.content)
        self.recursive_extract_zip(zip_file_path, path_dir)

        for root_path, _, list_files in os.walk(path_dir):
            for file in list_files:
                if file.endswith(tup_endswith):
                    return os.path.join(root_path, file)

        raise ValueError(
            "No file found in the extracted .zip archive. "
            f"- considerered extensions: {tup_endswith}"
        )

    def get_zip_from_web_in_memory(
        self,
        resp_req: Response,
        bool_io_interpreting: bool = False
    ) -> Union[TextIOWrapper, BufferedReader, list[BufferedReader]]:
        """Extract zip contents from web response in memory.

        Parameters
        ----------
        resp_req : Response
            HTTP response containing zip file
        bool_io_interpreting : bool
            Whether to return as text wrapper

        Returns
        -------
        Union[TextIOWrapper, BufferedReader, list[BufferedReader]]
            Extracted file object(s)
        """
        zipfile = ZipFile(BytesIO(resp_req.content))
        zip_names = zipfile.namelist()
        if len(zip_names) == 1:
            file_name = zip_names.pop()
            extracted_file = zipfile.open(file_name)
            return TextIOWrapper(extracted_file) if bool_io_interpreting else extracted_file
        return [zipfile.open(file_name) for file_name in zip_names]
    
    def recursive_unzip_in_memory(
        self, 
        file: Union[BytesIO, ZipExtFile]
    ) -> list[tuple[Union[StringIO, BytesIO], str]]:
        """Recursive unzip a zip file in memory.
        
        Parameters
        ----------
        file : Union[BytesIO, ZipExtFile]
            Zip file to unzip

        Returns
        -------
        list[tuple[Union[StringIO, BytesIO], str]]
            List of tuples containing (file_content, filename)
        """
        list_: list[tuple[Union[StringIO, BytesIO], str]] = []
        zipfile = ZipFile(file)

        for file_name in zipfile.namelist():
            if file_name.endswith(".zip"):
                list_.extend(self.recursive_unzip_in_memory(zipfile.open(file_name)))
                continue
            
            content_bytes = zipfile.read(file_name)

            if file_name.lower().endswith((".xlsx", ".xls", ".xlsm", ".xlsb")):
                list_.append((BytesIO(content_bytes), file_name))
            elif file_name.lower().endswith((".txt", ".csv", ".json", ".html", ".py", ".js")):
                try:
                    content = content_bytes.decode("utf-8")
                    list_.append((StringIO(content), file_name))
                except UnicodeDecodeError:
                    try:
                        content = content_bytes.decode("latin-1")
                        list_.append((StringIO(content), file_name))
                    except UnicodeDecodeError:
                        list_.append((BytesIO(content_bytes), file_name))
            else:
                list_.append((BytesIO(content_bytes), file_name))
        
        return list_

    def calculate_file_hash(self, file_path: str, algorithm: str = "sha256") -> str:
        """Calculate file hash.

        Parameters
        ----------
        file_path : str
            File to hash
        algorithm : str
            Hashing algorithm (default: "sha256")

        Returns
        -------
        str
            Hex digest of file hash
        """
        hash_func = getattr(hashlib, algorithm)()
        with open(file_path, "rb") as file:
            for chunk in iter(lambda: file.read(4096), b""):
                hash_func.update(chunk)
        return hash_func.hexdigest()

    def validate_file_hash(
        self,
        file_path: Union[str, Path],
        expected_hash: str,
        algorithm: str = "sha256"
    ) -> bool:
        """Validate file against expected hash.

        Parameters
        ----------
        file_path : Union[str, Path]
            File to validate
        expected_hash : str
            Expected hash value
        algorithm : str
            Hashing algorithm (default: "sha256")

        Returns
        -------
        bool
            True if hash matches, False otherwise
        """
        return self.calculate_file_hash(file_path, algorithm) == expected_hash

    def extract_file(
        self,
        archive_path: Union[str, Path],
        extract_dir: Union[str, Path],
        format: Literal["zip", "tar", "7z"] = "zip"
    ) -> bool:
        """Extract archive file.

        Parameters
        ----------
        archive_path : Union[str, Path]
            Archive file path
        extract_dir : Union[str, Path]
            Extraction directory
        format : Literal['zip', 'tar', '7z']
            Archive format (default: "zip")

        Returns
        -------
        bool
            True if successful, False otherwise

        Raises
        ------
        ValueError
            For unsupported archive formats
        """
        try:
            extract_dir = str(extract_dir)
            if format == "zip":
                with ZipFile(archive_path, "r") as zip_ref:
                    for name in zip_ref.namelist():
                        if not self._validate_safe_path(extract_dir, name):
                            raise ValueError(f"Path traversal detected in zip file: {name}")
                        zip_ref.extract(name, extract_dir)
            elif format == "tar":
                with tarfile.open(archive_path, "r:*") as tar_ref:
                    for member in tar_ref.getmembers():
                        if not self._validate_safe_path(extract_dir, member.name):
                            raise ValueError(f"Path traversal detected in tar file: {member.name}")
                        tar_ref.extract(member, extract_dir)
            elif format == "7z":
                with py7zr.SevenZipFile(archive_path, mode="r") as seven_zip_ref:
                    for name in seven_zip_ref.getnames():
                        if not self._validate_safe_path(extract_dir, name):
                            raise ValueError(f"Path traversal detected in 7z file: {name}")
                        seven_zip_ref.extract(path=extract_dir, targets=[name])
            else:
                raise ValueError(f"Unsupported archive format: {format}")
            return True
        except Exception as e:
            print(f"Failed to extract archive: {e}")
            return False

    def get_file_metadata(self, file_path: Union[str, Path]) -> ReturnGetFileMetadata:
        """Get file metadata.

        Parameters
        ----------
        file_path : Union[str, Path]
            File to inspect

        Returns
        -------
        ReturnGetFileMetadata
            Dictionary containing file metadata
        """
        stat = os.stat(file_path)
        creation_time = stat.st_ctime  # fallback for platforms without st_birthtime
        if hasattr(stat, 'st_birthtime'):
            creation_time = stat.st_birthtime
        return {
            "size": stat.st_size,
            "creation_time": datetime.fromtimestamp(creation_time),
            "modification_time": datetime.fromtimestamp(stat.st_mtime),
            "access_time": datetime.fromtimestamp(stat.st_atime),
        }

    def stream_file(self, resp_req: Response, chunk_size: int = 8192) -> Iterable[bytes]:
        """Stream file from response in chunks.

        Parameters
        ----------
        resp_req : Response
            HTTP response to stream
        chunk_size : int
            Chunk size in bytes (default: 8192)

        Yields
        ------
        Iterable[bytes]
            File chunks
        """
        yield from resp_req.iter_content(chunk_size=chunk_size)

    def check_separator_consistency(
        self,
        req_content: bytes,
        int_skip_rows: int = 0,
        int_skip_footer: int = 0,
        list_sep: Optional[list[str]] = None
    ) -> bool:
        """Check if content uses consistent separators.

        Parameters
        ----------
        req_content : bytes
            Content to check
        int_skip_rows : int
            Rows to skip at start (default: 0)
        int_skip_footer : int
            Rows to skip at end (default: 0)
        list_sep : Optional[list[str]]
            List of possible separators

        Returns
        -------
        bool
            True if consistent separators found
        """
        if list_sep is None:
            list_sep = [",", ";", "\t"]

        result = chardet.detect(req_content)
        encoding = result["encoding"] if result["encoding"] is not None else "latin-1"
        decoded_content = req_content.decode(encoding)
        list_lines = decoded_content.splitlines()
        int_skip_footer = len(list_lines) - int_skip_footer
        list_lines = list_lines[int_skip_rows:int_skip_footer]

        for sep in list_sep:
            list_sep_counts = [len(line.split(sep)) for line in list_lines]
            if len(set(list_sep_counts)) == 1 and all(x > 1 for x in list_sep_counts):
                return True
        return False


class FoldersTree(metaclass=TypeChecker):
    """Class for generating directory tree structures."""

    def __init__(
        self,
        str_path: str,
        bool_ignore_dot_folders: bool = False,
        list_ignored_folders: Optional[list[str]] = None,
        bool_add_linebreak_markdown: bool = False
    ) -> None:
        """Initialize FoldersTree instance.

        Parameters
        ----------
        str_path : str
            Root directory path
        bool_ignore_dot_folders : bool
            Whether to ignore dot folders (default: False)
        list_ignored_folders : Optional[list[str]]
            List of folders to ignore (default: ["__pycache__"])
        bool_add_linebreak_markdown : bool
            Whether to add markdown line breaks (default: False)

        Returns
        -------
        None
        """
        self.str_path = str_path
        self.bool_ignore_dot_folders = bool_ignore_dot_folders
        self.list_ignored_folders = list_ignored_folders or ["__pycache__"]
        self.bool_add_linebreak_markdown = bool_add_linebreak_markdown

    def generate_tree(
        self,
        str_curr_path: Optional[str] = None,
        str_prefix: str = "",
        bool_include_root: bool = True,
        str_tree_structure: str = ""
    ) -> str:
        """Generate directory tree structure.

        Parameters
        ----------
        str_curr_path : Optional[str]
            Current directory path (default: root path)
        str_prefix : str
            Prefix for tree lines (default: "")
        bool_include_root : bool
            Whether to include root directory (default: True)
        str_tree_structure : str
            Accumulated tree structure (default: "")

        Returns
        -------
        str
            Generated tree structure
        """
        if str_curr_path is None:
            str_curr_path = self.str_path

        str_linebreak_md = "<br>" if self.bool_add_linebreak_markdown else ""

        if bool_include_root:
            str_tree_structure += f"{os.path.basename(self.str_path)}{str_linebreak_md}\n"
            str_prefix = ""

        list_entries = sorted(os.listdir(str_curr_path))

        for idx, str_entry in enumerate(list_entries):
            str_entry_path = os.path.join(str_curr_path, str_entry)

            if self.bool_ignore_dot_folders and str_entry.startswith("."):
                continue
            if str_entry in self.list_ignored_folders:
                continue

            bool_is_directory = os.path.isdir(str_entry_path)
            bool_is_last_entry = idx == len(list_entries) - 1
            str_branch_prefix = "└── " if bool_is_last_entry else "├── "
            str_tree_structure += f"{str_prefix}{str_branch_prefix}{str_entry}{str_linebreak_md}\n"

            if bool_is_directory:
                str_new_prefix = str_prefix + ("    " if bool_is_last_entry else "│   ")
                str_tree_structure += self.generate_tree(
                    str_entry_path,
                    str_prefix=str_new_prefix,
                    bool_include_root=False
                )

        return str_tree_structure
    
    def print_tree(self) -> None:
        """Print generated tree structure.
        
        Returns
        -------
        None
        """
        print(self.generate_tree())

    def export_tree(self, filename: Optional[str] = None) -> Optional[str]:
        """Export tree structure to file or return as string.

        Parameters
        ----------
        filename : Optional[str]
            File to write to (returns string if None)

        Returns
        -------
        Optional[str]
            Tree structure if filename is None
        """
        str_tree_structure = self.generate_tree()
        if filename:
            with open(filename, "w", encoding="utf-8") as file:
                file.write(str_tree_structure)
            print(f"Tree structure has been written to {filename}")
        else:
            return str_tree_structure