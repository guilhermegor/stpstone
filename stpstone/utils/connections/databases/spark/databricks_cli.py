"""Databricks CLI integration utilities.

This module provides classes for interacting with Databricks jobs and DBFS operations
using the Databricks CLI. It includes functionality for job execution, metadata retrieval,
and file system operations with proper error handling and validation.
"""

import json
from logging import Logger
import os
import subprocess
from time import sleep
from typing import Any, Literal, Optional, TypedDict, Union

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.loggs.create_logs import CreateLog


class ReturnJobRun(TypedDict):
    """Return type for job_run method.

    Attributes
    ----------
    run_id : int
        The ID of the executed job run
    """

    run_id: int


class ReturnGetJobMetadata(TypedDict):
    """Return type for get_job_metadata method.

    Attributes
    ----------
    job_id : int
        The job ID
    settings : dict
        Job settings and configuration
    created_time : int
        Job creation timestamp
    """

    job_id: int
    settings: dict
    created_time: int


class ReturnGetRunMetadata(TypedDict):
    """Return type for get_run_metadata method.

    Attributes
    ----------
    run_id : int
        The run ID
    job_id : int
        The job ID
    state : dict
        Current state of the run
    """

    run_id: int
    job_id: int
    state: dict


class ReturnGetRunOutput(TypedDict):
    """Return type for get_run_output method.

    Attributes
    ----------
    run_id : int
        The run ID
    output : dict
        Output data from the run
    """

    run_id: int
    output: dict


class JobsCLI(metaclass=TypeChecker):
    """Class for handling Databricks jobs operations using CLI.

    This class provides methods to interact with Databricks jobs including
    running jobs, retrieving metadata, and managing job executions.
    """

    def __init__(self, job_id: int, path: str = "C:\\Temp") -> None:
        r"""Initialize JobsCLI instance.

        Parameters
        ----------
        job_id : int
            Databricks job ID
        path : str
            Temporary file path for storing CLI outputs (default: "C:\\Temp")

        Returns
        -------
        None
        """
        self._validate_job_id(job_id)
        self._validate_path(path)
        self.job_id = job_id
        self.path = path
        self.run_id: Optional[int] = None

    def _validate_job_id(self, job_id: int) -> None:
        """Validate job ID is positive integer.

        Parameters
        ----------
        job_id : int
            Job ID to validate

        Raises
        ------
        ValueError
            If job_id is not positive integer
        """
        if not isinstance(job_id, int):
            raise ValueError("job_id must be an integer")
        if job_id <= 0:
            raise ValueError("job_id must be positive integer")

    def _validate_path(self, path: str) -> None:
        """Validate path is string and directory exists.

        Parameters
        ----------
        path : str
            Path to validate

        Raises
        ------
        ValueError
            If path is not string
            If directory does not exist
        """
        if not isinstance(path, str):
            raise ValueError("path must be a string")
        if not os.path.isdir(path):
            raise ValueError(f"Directory does not exist: {path}")

    def _validate_filename(self, filename: str) -> None:
        """Validate filename is string with valid extension.

        Parameters
        ----------
        filename : str
            Filename to validate

        Raises
        ------
        ValueError
            If filename is not string
            If filename does not have .json extension
        """
        if not isinstance(filename, str):
            raise ValueError("filename must be a string")
        if not filename.endswith(".json"):
            raise ValueError("filename must have .json extension")

    def _execute_command(self, command: str) -> str:
        """Execute shell command and return output.

        Parameters
        ----------
        command : str
            Command to execute

        Returns
        -------
        str
            Command output

        Raises
        ------
        RuntimeError
            If command execution fails
        """
        try:
            result = subprocess.run( # noqa S602: subprocess called with shell=True
                command, 
                shell=True,
                capture_output=True, 
                text=True, 
                check=True
            )
            return result.stdout
        except subprocess.CalledProcessError as err:
            raise RuntimeError(f"Command execution failed: {str(err)}") from err

    def print_time(self, complete_path: str) -> None:
        """Print current timestamp to file.

        Parameters
        ----------
        complete_path : str
            Full path to output file
            
        Returns
        -------
        None
        """
        self._validate_path(os.path.dirname(complete_path))
        command = f'echo %date%-%time% > "{complete_path}"'
        self._execute_command(command)

    def job_run(
        self, filename: str = "run_id.json", notebook_params: Optional[dict[str, Any]] = None
    ) -> ReturnJobRun:
        """Execute Databricks job run and store run ID.

        Parameters
        ----------
        filename : str
            Output filename for run ID (default: "run_id.json")
        notebook_params : Optional[dict[str, Any]]
            Notebook parameters for job execution

        Returns
        -------
        ReturnJobRun
            Dictionary containing run_id

        """
        self._validate_filename(filename)
        complete_path = os.path.join(self.path, filename)

        if notebook_params:
            params_json = json.dumps(notebook_params)
            command = (
                f"databricks jobs run-now --job-id {self.job_id} "
                f"--notebook-params '{params_json}' > \"{complete_path}\""
            )
        else:
            command = f"databricks jobs run-now --job-id {self.job_id} > \"{complete_path}\""

        self._execute_command(command)
        sleep(5)

        try:
            with open(complete_path, encoding="utf-8") as f:
                dict_metadata = json.load(f)
        except json.JSONDecodeError as err:
            raise json.JSONDecodeError(
                f"Invalid JSON in output file: {complete_path}", err.doc, err.pos
            ) from err

        self.run_id = dict_metadata["run_id"]
        return {"run_id": self.run_id}

    def get_job_metadata(self, filename: str = "job_metadata.json") -> ReturnGetJobMetadata:
        """Retrieve job metadata from Databricks.

        Parameters
        ----------
        filename : str
            Output filename for job metadata (default: "job_metadata.json")

        Returns
        -------
        ReturnGetJobMetadata
            Dictionary containing job metadata

        """
        self._validate_filename(filename)
        complete_path = os.path.join(self.path, filename)
        command = f"databricks jobs get --job-id {self.job_id} > \"{complete_path}\""
        self._execute_command(command)
        sleep(5)

        try:
            with open(complete_path, encoding="utf-8") as f:
                dict_metadata = json.load(f)
        except json.JSONDecodeError as err:
            raise json.JSONDecodeError(
                f"Invalid JSON in output file: {complete_path}", err.doc, err.pos
            ) from err

        return dict_metadata

    def cancel_job_run(self, outside_run_id: Optional[int] = None) -> None:
        """Cancel specified job run.

        Parameters
        ----------
        outside_run_id : Optional[int]
            External run ID to cancel, uses internal run_id if not provided

        Raises
        ------
        ValueError
            If no run_id available and outside_run_id not provided
        """
        run_id_to_cancel = outside_run_id if outside_run_id is not None else self.run_id
        if run_id_to_cancel is None:
            raise ValueError("No run_id available for cancellation")
        command = f"databricks runs cancel --run-id {run_id_to_cancel}"
        self._execute_command(command)

    def get_run_metadata(
        self, 
        filename: str = "run_metadata.json", 
        outside_run_id: Optional[int] = None
    ) -> ReturnGetRunMetadata:
        """Retrieve run metadata from Databricks.

        Parameters
        ----------
        filename : str
            Output filename for run metadata (default: "run_metadata.json")
        outside_run_id : Optional[int]
            External run ID to query, uses internal run_id if not provided

        Returns
        -------
        ReturnGetRunMetadata
            Dictionary containing run metadata

        Raises
        ------
        ValueError
            If no run_id available and outside_run_id not provided
        """
        self._validate_filename(filename)
        run_id_to_query = outside_run_id if outside_run_id is not None else self.run_id
        if run_id_to_query is None:
            raise ValueError("No run_id available for metadata retrieval")

        complete_path = os.path.join(self.path, filename)
        command = f"databricks runs get --run-id {run_id_to_query} > \"{complete_path}\""
        self._execute_command(command)
        sleep(5)

        try:
            with open(complete_path, encoding="utf-8") as f:
                dict_metadata = json.load(f)
        except json.JSONDecodeError as err:
            raise json.JSONDecodeError(
                f"Invalid JSON in output file: {complete_path}", err.doc, err.pos
            ) from err

        return dict_metadata

    def get_run_output(
        self, 
        filename: str = "run_output_metadata.json", 
        outside_run_id: Optional[int] = None
    ) -> ReturnGetRunOutput:
        """Retrieve run output from Databricks.

        Parameters
        ----------
        filename : str
            Output filename for run output (default: "run_output_metadata.json")
        outside_run_id : Optional[int]
            External run ID to query, uses internal run_id if not provided

        Returns
        -------
        ReturnGetRunOutput
            Dictionary containing run output

        Raises
        ------
        ValueError
            If run_id_to_query not available
        """
        self._validate_filename(filename)
        run_id_to_query = outside_run_id if outside_run_id is not None else self.run_id
        if run_id_to_query is None:
            raise ValueError("No run_id available for output retrieval")

        complete_path = os.path.join(self.path, filename)
        command = f"databricks runs get-output --run-id {run_id_to_query} > \"{complete_path}\""
        self._execute_command(command)
        sleep(5)

        try:
            with open(complete_path, encoding="utf-8") as f:
                dict_metadata = json.load(f)
        except json.JSONDecodeError as err:
            raise json.JSONDecodeError(
                f"Invalid JSON in output file: {complete_path}", err.doc, err.pos
            ) from err

        return dict_metadata


class DbfsCLI(metaclass=TypeChecker):
    """Class for handling DBFS operations using CLI.

    This class provides methods to interact with Databricks File System (DBFS)
    including file copying, removal, movement, and listing.
    """

    def __init__(self, logger: Optional[Logger] = None) -> None:
        """Initialize DbfsCLI.
        
        Parameters
        ----------
        logger : Optional[Logger]
            Logger instance (default: None)
        
        Returns
        -------
        None
        """
        self.logger = logger
        self.cls_create_log = CreateLog()

    def _validate_dbfs_path(self, path: str) -> None:
        """Validate DBFS path format.

        Parameters
        ----------
        path : str
            DBFS path to validate

        Raises
        ------
        ValueError
            If path is not string
            If path does not start with 'dbfs:/'
        """
        if not isinstance(path, str):
            raise ValueError("DBFS path must be a string")
        if not path.startswith("dbfs:/"):
            raise ValueError("DBFS path must start with 'dbfs:/'")

    def copy(self, path_ori: str, path_dest: str, overwrite: bool = True) -> list[str]:
        """Copy file between DBFS paths.

        Parameters
        ----------
        path_ori : str
            Source DBFS path
        path_dest : str
            Destination DBFS path
        overwrite : bool
            Whether to overwrite existing files (default: True)

        Returns
        -------
        list[str]
            Command output lines
        """
        self._validate_dbfs_path(path_ori)
        self._validate_dbfs_path(path_dest)

        overwrite_flag = "--overwrite" if overwrite else ""
        command = f'dbfs cp "{path_ori}" "{path_dest}" {overwrite_flag}'.strip()
        output = self._execute_command(command)
        return [line.strip() for line in output.splitlines() if line.strip()]

    def remove(self, path: str) -> str:
        """Remove file from DBFS.

        Parameters
        ----------
        path : str
            DBFS path to remove

        Returns
        -------
        str
            Command output
        """
        self._validate_dbfs_path(path)
        command = f'dbfs rm "{path}"'
        output = self._execute_command(command)
        output_lines = [line.strip() for line in output.splitlines() if line.strip()]
        return output_lines[1] if len(output_lines) > 1 else output_lines[0]

    def move(self, path_ori: str, path_dest: str) -> list[str]:
        """Move file between DBFS paths.

        Parameters
        ----------
        path_ori : str
            Source DBFS path
        path_dest : str
            Destination DBFS path

        Returns
        -------
        list[str]
            Command output lines
        """
        self._validate_dbfs_path(path_ori)
        self._validate_dbfs_path(path_dest)
        command = f'dbfs mv "{path_ori}" "{path_dest}"'
        output = self._execute_command(command)
        return [line.strip() for line in output.splitlines() if line.strip()]

    def list_files(
        self,
        path: str,
        absolute: Literal['--absolute', ''] = "--absolute",
        l_flag: Literal['-l', ''] = "-l"
    ) -> Union[dict[str, dict[str, str]], list[str]]:
        """List files in DBFS directory.

        Parameters
        ----------
        path : str
            DBFS directory path
        absolute : Literal['--absolute', '']
            Whether to show absolute paths (default: "--absolute")
        l_flag : Literal['-l', '']
            Whether to show detailed listing (default: "-l")

        Returns
        -------
        Union[dict[str, dict[str, str]], list[str]]
            Detailed file information or simple file list
        """
        self._validate_dbfs_path(path)
        command = f'dbfs ls "{path}" {absolute} {l_flag}'.strip()
        output = self._execute_command(command)
        output_lines = [line.strip() for line in output.splitlines() if line.strip()]

        if l_flag == "-l":
            parsed_output = [line.split() for line in output_lines]
            for item in parsed_output:
                while "" in item:
                    item.remove("")
            return {item[2]: {"tipo": item[0], "tamanho": item[1]} for item in parsed_output}
        else:
            return output_lines

    def _execute_command(self, command: str) -> str:
        """Execute shell command and return output.

        Parameters
        ----------
        command : str
            Command to execute

        Returns
        -------
        str
            Command output

        Raises
        ------
        RuntimeError
            If command execution fails
        """
        try:
            result = subprocess.run( # noqa S602: subprocess called with shell=True
                command, 
                shell=True,
                capture_output=True, 
                text=True, 
                check=True
            )
            return result.stdout
        except subprocess.CalledProcessError as err:
            raise RuntimeError(f"DBFS command execution failed: {str(err)}") from err

    def copy_and_run(
        self, local_path: str, dbfs_path: str, job_id: int, int_seconds_wait: int = 10
    ) -> Literal['Success running job', 'Error running job']:
        """Copy CSV files to DBFS and run job to upload data.

        Parameters
        ----------
        local_path : str
            Local file path to copy
        dbfs_path : str
            Destination DBFS path
        job_id : int
            Databricks job ID to execute
        int_seconds_wait : int
            Wait time between operations in seconds (default: 10)

        Returns
        -------
        Literal['Success running job', 'Error running job']
            Job execution status

        Raises
        ------
        ValueError
            If path validation fails
        """
        self._validate_dbfs_path(dbfs_path)
        if not os.path.exists(local_path):
            raise ValueError(f"Local file does not exist: {local_path}")

        self.copy(local_path, dbfs_path)
        tables = self.list_files("dbfs:/FileStore/tables")

        while dbfs_path not in tables:
            self.copy(local_path, dbfs_path)
            tables = self.list_files("dbfs:/FileStore/tables")
            sleep(int_seconds_wait)

        sleep(int_seconds_wait)
        db_cli = JobsCLI(job_id)
        db_cli.job_run()
        sleep(int_seconds_wait)

        try:
            run_state = db_cli.get_run_metadata()
        except json.JSONDecodeError:
            self.cls_create_log.log_message(
                self.logger, 
                f"Unable to fetch metadata from run: {db_cli.run_id}", 
                "error"
            )
            return "Error running job"

        sleep(int_seconds_wait)

        while run_state["state"]["life_cycle_state"] != "TERMINATED":
            try:
                run_state = db_cli.get_run_metadata()
            except json.JSONDecodeError:
                self.cls_create_log.log_message(
                    self.logger, 
                    f"Unable to fetch metadata from run: {db_cli.run_id}", 
                    "error"
                )
                return "Error running job"
            sleep(int_seconds_wait)

        if run_state["state"]["result_state"] != "SUCCESS":
            return "Error running job"
        else:
            return "Success running job"