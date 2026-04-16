"""AWS S3 client interface for file operations.

This module provides a class for interacting with AWS S3 services, including file uploads,
downloads, and object listing operations. It handles credential management and error
handling for common S3 operations.
"""

from io import BytesIO
from logging import Logger
from typing import Any, Optional, TypedDict

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.loggs.create_logs import CreateLog


class ReturnDownloadFile(TypedDict):
	"""Typed dictionary for download file return structure."""

	Body: BytesIO
	Metadata: dict[str, Any]


class S3Client(metaclass=TypeChecker):
	"""Client for AWS S3 operations with error handling and logging."""

	def __init__(
		self,
		aws_access_key_id: str,
		aws_secret_access_key: str,
		s3_bucket: str,
		region_name: str = "us-west-1",
		datalake_path: Optional[str] = None,
		logger: Optional[Logger] = None,
		bool_debug_mode: bool = False,
	) -> None:
		"""Initialize S3 client with credentials and configuration.

		Parameters
		----------
		aws_access_key_id : str
			AWS access key ID
		aws_secret_access_key : str
			AWS secret access key
		s3_bucket : str
			Name of the S3 bucket
		region_name : str
			AWS region name (default: 'us-west-1')
		datalake_path : Optional[str]
			Path to datalake in S3 (default: None)
		logger : Optional[Logger]
			Logger instance for error logging (default: None)
		bool_debug_mode : bool
			Flag to enable debug mode printing (default: False)
		"""
		self._envs = {
			"aws_access_key_id": aws_access_key_id,
			"aws_secret_access_key": aws_secret_access_key,
			"region_name": region_name,
			"s3_bucket": s3_bucket,
			"datalake": datalake_path,
		}

		self._validate_credentials()
		self.s3 = boto3.client(
			"s3",
			aws_access_key_id=self._envs["aws_access_key_id"],
			aws_secret_access_key=self._envs["aws_secret_access_key"],
			region_name=self._envs["region_name"],
		)
		self.logger = logger
		self.bool_debug_mode = bool_debug_mode

	def _validate_credentials(self) -> None:
		"""Validate required AWS credentials are provided.

		Raises
		------
		ValueError
			If any required credential is empty or None
		"""
		required_vars = ["aws_access_key_id", "aws_secret_access_key", "s3_bucket"]
		for var in required_vars:
			if not self._envs[var]:
				raise ValueError(f"Missing required credential: {var}")

	def handle_error(
		self, err: Exception, action: Optional[str] = None, s3_key: Optional[str] = None
	) -> None:
		"""Handle and log S3 operation errors.

		Parameters
		----------
		err : Exception
			Exception object to handle
		action : Optional[str]
			Description of the operation being performed (default: None)
		s3_key : Optional[str]
			S3 key/path involved in the operation (default: None)

		Notes
		-----
		Handles specific AWS error types with appropriate messages:
		- NoCredentialsError: Missing AWS credentials
		- ClientError: Various AWS client errors including missing keys
		- EndpointConnectionError: Connection issues
		- FileNotFoundError: Local file missing
		"""
		if isinstance(err, NoCredentialsError):
			message = "Credentials not found, please reconfigure your AWS credentials properly."
		elif isinstance(err, ClientError):
			if err.response["Error"]["Code"] == "NoSuchKey":
				message = f"The file {s3_key} was not found in bucket {self._envs['s3_bucket']}."
			else:
				message = f"AWS Client Error: {err}"
		elif isinstance(err, EndpointConnectionError):
			message = "Failed to connect to AWS endpoint. Check internet connection."
		elif isinstance(err, FileNotFoundError):
			message = f"The file {s3_key} was not found locally."
		else:
			message = f"Unexpected error during {action or 'an operation'}: {err}"

		CreateLog().log_message(self.logger, message, "critical")

	def upload_file(self, data: BytesIO, s3_key: str) -> bool:
		"""Upload file data to S3 bucket.

		Parameters
		----------
		data : BytesIO
			File data to upload
		s3_key : str
			Destination key/path in S3

		Returns
		-------
		bool
			True if upload succeeded, False otherwise
		"""
		try:
			self.s3.put_object(Body=data.getvalue(), Bucket=self._envs["s3_bucket"], Key=s3_key)
			return True
		except Exception as err:
			self.handle_error(err, action="uploading file", s3_key=s3_key)
			return False

	def download_file(self, s3_key: str) -> Optional[ReturnDownloadFile]:
		"""Download file from S3 bucket.

		Parameters
		----------
		s3_key : str
			Source key/path in S3

		Returns
		-------
		Optional[ReturnDownloadFile]
			Downloaded file data and metadata if successful, None otherwise
		"""
		try:
			file = self.s3.get_object(Bucket=self._envs["s3_bucket"], Key=s3_key)
			if self.bool_debug_mode:
				print(f"Successfully downloaded {s3_key}")
			return file
		except Exception as err:
			self.handle_error(err, action="downloading file", s3_key=s3_key)
			return None

	def list_objects(self, prefix: str) -> Optional[list[dict[str, Any]]]:
		"""List objects in S3 bucket with given prefix.

		Parameters
		----------
		prefix : str
			Prefix filter for object keys

		Returns
		-------
		Optional[list[dict[str, Any]]]
			List of objects if successful, None otherwise
		"""
		try:
			response = self.s3.list_objects(Bucket=self._envs["s3_bucket"], Prefix=prefix)
			return response.get("Contents", [])
		except Exception as err:
			self.handle_error(err, action="listing objects")
			return None
