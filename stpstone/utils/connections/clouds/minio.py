"""MinIO client for object storage operations.

This module provides a client class for interacting with MinIO object storage,
supporting operations like bucket creation, file upload/download, and object listing.
"""

from io import BufferedIOBase, BytesIO, RawIOBase
from logging import Logger
import os
from typing import Any, BinaryIO, Optional, Union

from minio import Minio
from minio.error import S3Error

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.loggs.create_logs import CreateLog


class MinioClient(metaclass=TypeChecker):
    """Client for MinIO object storage operations."""

    def __init__(
        self,
        user: str,
        password: str,
        endpoint: str = "localhost:9000",
        bool_secure: bool = True,
        logger: Optional[Logger] = None
    ) -> None:
        """Initialize MinIO client.

        Parameters
        ----------
        user : str
            MinIO access key
        password : str
            MinIO secret key
        endpoint : str, optional
            MinIO server endpoint (default: "localhost:9000")
        bool_secure : bool, optional
            Whether to use HTTPS (default: True)
        logger : Optional[Logger], optional
            Logger instance for logging (default: None)
        """
        self.client = Minio(
            endpoint,
            access_key=user,
            secret_key=password,
            secure=bool_secure
        )
        self.logger = logger
        self.cls_create_log = CreateLog()

    def _log_info(self, message: str) -> None:
        """Log an info message.

        Parameters
        ----------
        message : str
            The message to log
        """
        self.cls_create_log.log_message(self.logger, message, "info")

    def _log_critical(self, message: str) -> None:
        """Log a critical message.

        Parameters
        ----------
        message : str
            The message to log
        """
        self.cls_create_log.log_message(self.logger, message, "critical")

    def _validate_bucket_name(self, bucket_name: str) -> None:
        """Validate bucket name.

        Parameters
        ----------
        bucket_name : str
            Name of the bucket to validate

        Raises
        ------
        ValueError
            If bucket name is empty
        """
        if not bucket_name:
            raise ValueError("Bucket name cannot be empty")

    def make_bucket(self, bucket_name: str) -> bool:
        """Create a new bucket if it doesn't exist.

        Parameters
        ----------
        bucket_name : str
            Name of the bucket to create

        Returns
        -------
        bool
            True if bucket exists or was created successfully, False otherwise
        """
        self._validate_bucket_name(bucket_name)
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                self._log_info(f"Bucket {bucket_name} created successfully.")
            return True
        except S3Error as err:
            self._log_critical(f"Error creating bucket {bucket_name}: {err}")
            return False

    def put_object_from_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str,
        dict_metadata: Optional[dict[str, Any]] = None,
        content_type: Optional[str] = None
    ) -> bool:
        """Upload a file to MinIO storage.

        Parameters
        ----------
        bucket_name : str
            Target bucket name
        object_name : str
            Name for the stored object
        file_path : str
            Path to local file to upload
        dict_metadata : Optional[dict[str, Any]], optional
            Metadata for the object (default: None)
        content_type : Optional[str], optional
            Content type of the object (default: None)

        Returns
        -------
        bool
            True if upload succeeded, False otherwise
        """
        self._validate_bucket_name(bucket_name)
        if not os.path.exists(file_path):
            self._log_critical(f"File {file_path} does not exist.")
            return False
        if not self.make_bucket(bucket_name):
            return False
        try:
            self.client.fput_object(
                bucket_name,
                object_name,
                file_path,
                metadata=dict_metadata,
                content_type=content_type
            )
            self._log_info(
                f"File '{file_path}' uploaded as '{object_name}' to bucket '{bucket_name}'."
            )
            return True
        except S3Error as err:
            self._log_critical(
                f"Error uploading file '{file_path}' as '{object_name}' "
                f"to bucket '{bucket_name}': {err}"
            )
            return False

    def put_object_from_stream(
        self,
        bucket_name: str,
        object_name: str,
        stream: Union[BinaryIO, RawIOBase, BufferedIOBase],
        int_lenght: int,
        dict_metadata: Optional[dict[str, Any]] = None,
        content_type: Optional[str] = None,
        seek_to_start: bool = False
    ) -> bool:
        """Upload data from a stream to MinIO storage.

        Parameters
        ----------
        bucket_name : str
            Target bucket name
        object_name : str
            Name for the stored object
        stream : Union[BinaryIO, RawIOBase, BufferedIOBase]
            Stream containing data to upload
        int_lenght : int
            Length of the data in bytes
        dict_metadata : Optional[dict[str, Any]], optional
            Metadata for the object (default: None)
        content_type : Optional[str], optional
            Content type of the object (default: None)
        seek_to_start : bool, optional
            Whether to seek to start of stream before upload (default: False)

        Returns
        -------
        bool
            True if upload succeeded, False otherwise
        """
        self._validate_bucket_name(bucket_name)
        if not self.make_bucket(bucket_name):
            return False
        try:
            if seek_to_start and hasattr(stream, "seekable") and stream.seekable():
                stream.seek(0)
            self.client.put_object(
                bucket_name,
                object_name,
                stream,
                int_lenght,
                metadata=dict_metadata,
                content_type=content_type
            )
            self._log_info(f"Stream uploaded as '{object_name}' to bucket '{bucket_name}'")
            return True
        except (S3Error, OSError) as err:
            self._log_critical(f"Error uploading stream as '{object_name}': {err}")
            return False

    def put_object_from_bytes(
        self,
        bucket_name: str,
        object_name: str,
        data: bytes,
        dict_metadata: Optional[dict[str, Any]] = None,
        content_type: Optional[str] = None
    ) -> bool:
        """Upload bytes data to MinIO storage.

        Parameters
        ----------
        bucket_name : str
            Target bucket name
        object_name : str
            Name for the stored object
        data : bytes
            Bytes data to upload
        dict_metadata : Optional[dict[str, Any]], optional
            Metadata for the object (default: None)
        content_type : Optional[str], optional
            Content type of the object (default: None)

        Returns
        -------
        bool
            True if upload succeeded, False otherwise
        """
        self._validate_bucket_name(bucket_name)
        if not self.make_bucket(bucket_name):
            return False
        try:
            with BytesIO(data) as stream:
                self.client.put_object(
                    bucket_name,
                    object_name,
                    stream,
                    len(data),
                    metadata=dict_metadata,
                    content_type=content_type
                )
                self._log_info(f"Bytes data uploaded as '{object_name}' to bucket '{bucket_name}'")
                return True
        except S3Error as err:
            self._log_critical(f"Error uploading bytes as '{object_name}': {err}")
            return False

    def get_object_as_bytes(
        self,
        bucket_name: str,
        object_name: str,
    ) -> Optional[bytes]:
        """Download an object from MinIO as bytes.

        Parameters
        ----------
        bucket_name : str
            Source bucket name
        object_name : str
            Name of the object to download

        Returns
        -------
        Optional[bytes]
            Object data as bytes if successful, None otherwise
        """
        self._validate_bucket_name(bucket_name)
        try:
            response = self.client.get_object(bucket_name, object_name)
            data = response.read()
            self._log_info(
                f"Successfully retrieved object '{object_name}' from bucket '{bucket_name}'"
            )
            return data
        except S3Error as err:
            self._log_critical(
                f"Error retrieving object '{object_name}' from bucket '{bucket_name}': {err}"
            )
            return None
        finally:
            if "response" in locals():
                response.close()
                response.release_conn()

    def get_object_to_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str,
    ) -> bool:
        """Download an object from MinIO to a local file.

        Parameters
        ----------
        bucket_name : str
            Source bucket name
        object_name : str
            Name of the object to download
        file_path : str
            Path to save the downloaded file

        Returns
        -------
        bool
            True if download succeeded, False otherwise
        """
        self._validate_bucket_name(bucket_name)
        try:
            self.client.fget_object(bucket_name, object_name, file_path)
            self._log_info(
                f"Successfully downloaded object '{object_name}' to '{file_path}'"
            )
            return True
        except S3Error as err:
            self._log_critical(f"Error downloading object '{object_name}': {err}")
            return False

    def list_objects(
        self,
        bucket_name: str,
        bool_include_version: bool = False,
        prefix: Optional[str] = None,
        recursive: bool = False
    ) -> Optional[list[str]]:
        """List objects in a bucket.

        Parameters
        ----------
        bucket_name : str
            Bucket to list objects from
        bool_include_version : bool, optional
            Whether to include object versions (default: False)
        prefix : Optional[str], optional
            Prefix filter for object names (default: None)
        recursive : bool, optional
            Whether to list recursively (default: False)

        Returns
        -------
        Optional[list[str]]
            List of object names if successful, None otherwise
        """
        self._validate_bucket_name(bucket_name)
        try:
            objects = self.client.list_objects(
                bucket_name,
                include_version=bool_include_version,
                prefix=prefix,
                recursive=recursive
            )
            return [obj.object_name for obj in objects]
        except S3Error as err:
            self._log_critical(f"Error listing objects in bucket '{bucket_name}': {err}")
            return None