### AWS S3 CLIENT ###

# pypi.org libs
import boto3
import sys
from botocore.exceptions import NoCredentialsError, ClientError, EndpointConnectionError
from keyring import get_password
from logging import Logger
from io import BytesIO
from typing import Optional, List, Dict, Any
# local libs
from stpstone.utils.loggs.create_logs import CreateLog


class S3Client:

    def __init__(self, str_default_region:str='us-west-1', logger:Optional[Logger]=None,
                 bl_debug_mode:bool=False) -> None:
        """
        DOCSTRING:
        INPUTS:
        OUTPUT:S
        """
        self._envs = {
            'aws_access_key_id': get_password('AWS_ACCESS', 'KEY_ID'),
            'aws_secret_access_key': get_password('AWS_ACCESS', 'PASSWORD'),
            'region_name': get_password('AWS_ACCESS', 'REGION_NAME') \
                if get_password('AWS_ACCESS', 'REGION_NAME') is not None \
                else str_default_region,
            's3_bucket': get_password('S3_BUCKET_NAME'),
            'datalake': get_password('DELTA_LAKE_S3_PATH')
        }
        for var in self._envs:
            if self._envs[var] is None:
                print(f'A variável de ambiente {var} não está definida.')
                sys.exit(1)
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=self._envs['aws_access_key_id'],
            aws_secret_access_key=self._envs['aws_secret_access_key'],
            region_name=self._envs['region_name']
        )
        self.logger = logger
        self.bl_debug_mode = bl_debug_mode

    def handle_error(self, e:Exception, action:Optional[str]=None, s3_key:Optional[str]=None) \
        -> None:
        """
        DOCSTRING: GENERIC ERROR HANDLER
        INPUTS: EXCEPTION OBJECT, ACTION (OPTIONAL), S3 KEY (OPTIONAL)
        OUTPUTS: LOGGER/PRINT
        """
        if isinstance(e, NoCredentialsError):
            message = 'Credentials not found, please reconfigure your AWS credentials properly.'
        elif isinstance(e, ClientError):
            if e.response['Error']['Code'] == "NoSuchKey":
                message = f'The file {s3_key} was not found in the bucket {self._envs["s3_bucket"]}.'
            else:
                message = f'AWS Client Error: {e}'
        elif isinstance(e, EndpointConnectionError):
            message = 'Failed to connect to the AWS endpoint. Please check your internet connection.'
        elif isinstance(e, FileNotFoundError):
            message = f'The file {s3_key} was not found locally.'
        else:
            message = f'An unexpected error occurred during {action or "an operation"}: {e}'
        # logging or printing, in case of debug mode
        if self.logger:
            CreateLog().critical(self.logger, message)
        if self.bl_debug_mode:
            print(message)

    def upload_file(self, data:BytesIO, s3_key:str) -> None:
        """
        DOCSTRING: UPLOAD FILES TO AWS CLOUD
        INPUTS: DATA, S3 KEY
        OUTPUTS:
        """
        try:
            self.s3.put_object(
                Body=data.getvalue(),
                Bucket=self._envs['s3_bucket'],
                Key=s3_key
            )
        except Exception as e:
            self.handle_error(e, action='uploading a file', s3_key=s3_key)

    def download_file(self, s3_key:str) -> Optional[Dict[str, Any]]:
        """
        DOCSTRING: DOWNLOAD FILE FROM S3
        INPUTS: S3 KEY
        OUTPUTS: DOWNLOADED FILE
        """
        try:
            file = self.s3.get_object(Bucket=self._envs['s3_bucket'], Key=s3_key)
            print(f'Successfully downloaded {s3_key}')
            return file
        except Exception as e:
            self.handle_error(e, action='downloading a file', s3_key=s3_key)

    def list_object(self, prefix:str) -> Optional[List[Dict[str, Any]]]:
        """
        DOCSTRING: LIST OBJECTS IN S3
        INPUTS: PREFIX
        OUTPUTS: LIST OF OBJECTS
        """
        try:
            return self.s3.list_objects(Bucket=self._envs['s3_bucket'], Prefix=prefix)['Contents']
        except Exception as e:
            self.handle_error(e, action='listing objects')
