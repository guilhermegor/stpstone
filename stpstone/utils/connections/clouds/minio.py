import os
from io import BytesIO, RawIOBase, BufferedIOBase
from typing import BinaryIO, Optional, Dict, Any, Union, IO, AnyStr
from minio import Minio
from minio.error import S3Error
from logging import Logger
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class MinioClient:

    def __init__(
        self,
        user: str,
        password: str,
        endpoint: Optional[str] = "localhost:9000",
        bl_secure: Optional[bool] = True,
        logger: Optional[Logger] = None
    ):
        self.client = Minio(
            endpoint,
            access_key=user,
            secret_key=password,
            secure=bl_secure
        )
        self.logger = logger

    def _log_info(self, message: str):
        if self.logger is not None:
            CreateLog().info(self.logger, message)
        else:
            print(f"INFO: {message}")

    def _log_critical(self, message: str):
        if self.logger is not None:
            CreateLog().critical(self.logger, message)
        else:
            print(f"CRITICAL: {message}")

    def make_bucket(self, bucket_name: str) -> bool:
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                self._log_info(f"Bucket {bucket_name} created successfully.")
            return True
        except S3Error as e:
            self._log_critical(f"Error creating bucket {bucket_name}: {e}")
            return False

    def put_object_from_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str,
        dict_metadata: Optional[Dict[str, Any]] = None,
        content_type: Optional[str] = None
    ) -> bool:
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
                f"File '{file_path}' uploaded as '{object_name}' to bucket '{bucket_name}'.")
            return True
        except S3Error as e:
            self._log_critical(
                f"Error uploading file '{file_path}' as '{object_name}' to bucket '{bucket_name}': {e}")
            return False

    def put_object_from_stream(
        self,
        bucket_name: str,
        object_name: str,
        stream: Union[IO[AnyStr], BinaryIO, RawIOBase, BufferedIOBase],
        int_lenght: int,
        dict_metadata: Optional[Dict[str, Any]] = None,
        content_type: Optional[str] = None,
        seek_to_start: bool = None
    ) -> bool:
        if not self.make_bucket(bucket_name):
            return False
        try:
            if seek_to_start and hasattr(stream, 'seekable') and stream.seekable():
                stream.seek(0)
            self.client.put_object(bucket_name, object_name, stream, int_lenght,
                                metadata=dict_metadata, content_type=content_type)
            self._log_info(f"Stream uploaded as '{object_name}' to bucket '{bucket_name}'")
            return True
        except (S3Error, OSError) as e:
            self._log_critical(f"Error uploading stream as '{object_name}': {e}")
            return False

    def put_object_from_bytes(
        self,
        bucket_name: str,
        object_name: str,
        data: bytes,
        dict_metadata: Optional[Dict[str, Any]] = None,
        content_type: Optional[str] = None
    ) -> bool:
        if not self.make_bucket(bucket_name):
            return False
        try:
            with BytesIO(data) as stream:
                self.client.put_object(bucket_name, object_name, stream, len(data),
                                       metadata=dict_metadata, content_type=content_type)
                self._log_info(f"Bytes data uploaded as '{object_name}' to bucket '{bucket_name}'")
                return True
        except S3Error as e:
            self._log_critical(f"Error uploading bytes as '{object_name}': {e}")
            return False

    def get_object_as_bytes(
        self,
        bucket_name: str,
        object_name: str,
    ) -> Optional[bytes]:
        try:
            response = self.client.get_object(bucket_name, object_name)
            data = response.read()
            self._log_info(f"Successfully retrieved object '{object_name}' from bucket '{bucket_name}'")
            return data
        except S3Error as e:
            self._log_critical(f"Error retrieving object '{object_name}' from bucket '{bucket_name}': {e}")
            return None
        finally:
            if 'response' in locals():
                response.close()
                response.release_conn()

    def get_object_to_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str,
    ) -> bool:
        try:
            self.client.fget_object(bucket_name, object_name, file_path)
            self._log_info(f"Successfully downloaded object '{object_name}' to '{file_path}'")
            return True
        except S3Error as e:
            self._log_critical(f"Error downloading object '{object_name}': {e}")
            return False

    def list_objects(
        self,
        bucket_name: str,
        bl_include_version: bool = False,
        prefix: Optional[str] = None,
        recursive: bool = False
    ) -> Optional[list]:
        try:
            objects = self.client.list_objects(bucket_name, include_version=bl_include_version,
                                               prefix=prefix, recursive=recursive)
            return [obj.object_name for obj in objects]
        except S3Error as e:
            self._log_critical(f"Error listing objects in bucket '{bucket_name}': {e}")
            return None
