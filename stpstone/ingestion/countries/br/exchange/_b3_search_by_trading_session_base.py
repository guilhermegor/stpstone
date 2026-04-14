"""Shared base class for B3 Search by Trading Session ingestion."""

from abc import abstractmethod
from datetime import date
from io import BytesIO, StringIO
from logging import Logger
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Optional, Union

import backoff
import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import requests
from requests import Response, Session
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.abc.ingestion_abc import (
	ABCIngestionOperations,
	ContentParser,
	CoreIngestion,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.str import TypeCaseFrom, TypeCaseTo
from stpstone.utils.parsers.xml import XMLFiles


class ABCB3SearchByTradingSession(ABCIngestionOperations):
	"""Ingestion concrete class."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
		url: str = "FILL_ME",
	) -> None:
		"""Initialize the ingestion class.

		Parameters
		----------
		date_ref : Optional[date], optional
		    The date of reference, by default None.
		logger : Optional[Logger], optional
		    The logger, by default None.
		cls_db : Optional[Session], optional
		    The database session, by default None.
		url : str, optional
		    The url of the website, by default "FILL_ME".

		Returns
		-------
		None
		"""
		super().__init__(cls_db=cls_db)
		CoreIngestion.__init__(self)
		ContentParser.__init__(self)

		self.logger = logger
		self.cls_db = cls_db
		self.cls_dir_files_management = DirFilesManagement()
		self.cls_dates_current = DatesCurrent()
		self.cls_create_log = CreateLog()
		self.cls_dates_br = DatesBRAnbima()
		self.cls_xml_handler = XMLFiles()
		self.date_ref = date_ref or self.cls_dates_br.add_working_days(
			self.cls_dates_current.curr_date(), -1
		)
		self.url = url.format(self.date_ref.strftime("%y%m%d"))

	def run(
		self,
		dict_dtypes: dict[str, Union[str, int, float]],
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_fmt_dt: str = "YYYY-MM-DD",
		cols_from_case: Optional[TypeCaseFrom] = None,
		cols_to_case: Optional[TypeCaseTo] = None,
		str_table_name: str = "<COUNTRY>_<SOURCE>_<TABLE_NAME>",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		dict_dtypes : dict[str, Union[str, int, float]]
		    The data types of the columns.
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		    The timeout, by default (12.0, 21.0).
		bool_verify : bool, optional
		    Whether to verify the SSL certificate, by default True.
		bool_insert_or_ignore : bool, optional
		    Whether to insert or ignore the data, by default False.
		str_fmt_dt : str, optional
		    The format of the date, by default "YYYY-MM-DD".
		cols_from_case : Optional[TypeCaseFrom], optional
		    The case of the columns, by default None.
		cols_to_case : Optional[TypeCaseTo], optional
		    The case of the columns, by default None.
		str_table_name : str, optional
		    The name of the table, by default "<COUNTRY>_<SOURCE>_<TABLE_NAME>".

		Returns
		-------
		Optional[pd.DataFrame]
		    The transformed DataFrame.
		"""
		resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
		file, file_name = self.parse_raw_file(resp_req)
		df_ = self.transform_data(file=file, file_name=file_name)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes=dict_dtypes,
			str_fmt_dt=str_fmt_dt,
			url=self.url,
			cols_from_case=cols_from_case,
			cols_to_case=cols_to_case,
		)
		if self.cls_db:
			self.insert_table_db(
				cls_db=self.cls_db,
				str_table_name=str_table_name,
				df_=df_,
				bool_insert_or_ignore=bool_insert_or_ignore,
			)
		else:
			return df_

	@backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_time=60)
	def get_response(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Return a list of response objects.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		    The timeout, by default (12.0, 21.0).
		bool_verify : bool, optional
		    Verify the SSL certificate, by default True.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
		    A list of response objects.
		"""
		resp_req = requests.get(
			self.url,
			timeout=timeout,
			verify=bool_verify,
		)
		resp_req.raise_for_status()
		return resp_req

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
	) -> tuple[StringIO, str]:
		"""Parse the raw file content.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
		    The response object.

		Returns
		-------
		tuple[StringIO, str]
		    The parsed content and the name of the file.

		Raises
		------
		ValueError
		    If no files found in the downloaded content.
		"""
		files_list = self.cls_dir_files_management.recursive_unzip_in_memory(
			BytesIO(resp_req.content)
		)

		if not files_list:
			raise ValueError("No files found in the downloaded content")

		file_content, filename = files_list[0]

		if isinstance(file_content, BytesIO):
			try:
				content_str = file_content.getvalue().decode("utf-8")
				file_content = StringIO(content_str)
			except UnicodeDecodeError:
				try:
					content_str = file_content.getvalue().decode("latin-1")
					file_content = StringIO(content_str)
				except UnicodeDecodeError:
					try:
						content_str = file_content.getvalue().decode("cp1252")
						file_content = StringIO(content_str)
					except UnicodeDecodeError:
						content_str = file_content.getvalue().decode("utf-8", errors="replace")
						file_content = StringIO(content_str)
		elif isinstance(file_content, str):
			file_content = StringIO(file_content)
		elif not isinstance(file_content, StringIO):
			file_content = StringIO(str(file_content))

		return file_content, filename

	def parse_raw_ex_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
		prefix: str,
		file_name: str,
	) -> tuple[StringIO, str]:
		"""Parse the raw file content.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
		    The response object.
		prefix : str
		    The prefix for the temporary directory.
		file_name : str
		    The name of the file.

		Returns
		-------
		tuple[StringIO, str]
		    The parsed content and the name of the file.

		Raises
		------
		ValueError
		    If no files found in the downloaded content.
		RuntimeError
		    If no .ex_ file found in the downloaded content.
		"""
		files_list = self.cls_dir_files_management.recursive_unzip_in_memory(
			BytesIO(resp_req.content)
		)

		ex_file_content = None
		ex_file_name = None
		for file_content, filename in files_list:
			if filename.lower().endswith(".ex_"):
				ex_file_content = file_content
				ex_file_name = filename
				break

		if ex_file_content is None:
			raise ValueError("No .ex_ file found in the downloaded ZIP")

		temp_dir = Path(tempfile.mkdtemp(prefix=prefix))

		try:
			self.cls_create_log.log_message(
				self.logger,
				f"Created temporary directory: {temp_dir}",
				"info",
			)

			exe_filename = f"{file_name}_{self.date_ref.strftime('%y%m%d')}.exe"
			exe_path = temp_dir / exe_filename

			if hasattr(ex_file_content, "getvalue"):
				ex_content = ex_file_content.getvalue()
			else:
				ex_content = ex_file_content

			if isinstance(ex_content, str):
				ex_content = ex_content.encode("latin-1")

			with open(exe_path, "wb") as f:
				f.write(ex_content)

			self.cls_create_log.log_message(
				self.logger,
				f"Saved executable to: {exe_path}",
				"info",
			)

			os.chmod(exe_path, 0o755)  # noqa S103

			original_cwd = os.getcwd()
			os.chdir(temp_dir)

			try:
				self.cls_create_log.log_message(
					self.logger,
					f"Executing Wine command: wine {exe_filename}",
					"info",
				)

				result = subprocess.run(  # noqa S603
					["wine", exe_filename],  # noqa S607
					capture_output=True,
					text=True,
					timeout=300,
					check=False,
				)

				if result.returncode != 0:
					self.cls_create_log.log_message(
						self.logger,
						f"Wine execution failed with return code {result.returncode}: "
						+ f"{result.stderr}",
						"warning",
					)

				self.cls_create_log.log_message(
					self.logger,
					f"Wine execution completed. Stdout: {result.stdout[:200]}...",
					"info",
				)

			finally:
				os.chdir(original_cwd)

			output_patterns = [
				"*.txt",
				"*.csv",
				"*.dat",
				"*.out",
				"*.xlsx",
				"*.xls",
				f"*{self.date_ref.strftime('%y%m%d')}*",
				"premiums*",
				"option*",
				"PE*",
			]

			output_files = []
			for pattern in output_patterns:
				output_files.extend(temp_dir.glob(pattern))

			output_files = [f for f in output_files if f != exe_path]

			if not output_files:
				all_files = list(temp_dir.iterdir())
				list_ = [f.name for f in all_files]
				self.cls_create_log.log_message(
					self.logger,
					f"No output files found. All files in temp dir: {list_}",
					"error",
				)
				raise RuntimeError(
					f"No output file generated after Wine execution. "
					f"Wine stderr: {result.stderr if 'result' in locals() else 'N/A'}"
				)

			if len(output_files) > 1:
				output_file = max(output_files, key=lambda f: f.stat().st_size)
				self.cls_create_log.log_message(
					self.logger,
					f"Multiple output files found, using largest: {output_file.name}",
					"info",
				)
			else:
				output_file = output_files[0]

			self.cls_create_log.log_message(
				self.logger,
				f"Reading output file: {output_file}",
				"info",
			)

			try:
				with open(output_file, encoding="utf-8") as f:
					content = f.read()
			except UnicodeDecodeError:
				with open(output_file, encoding="latin-1") as f:
					content = f.read()

			return StringIO(content), ex_file_name

		except Exception as e:
			self.cls_create_log.log_message(
				self.logger,
				f"Error in parse_raw_ex_file: {str(e)}",
				"error",
			)
			raise

		finally:
			try:
				if temp_dir.exists():
					shutil.rmtree(temp_dir)
					self.cls_create_log.log_message(
						self.logger,
						f"Cleaned up temporary directory: {temp_dir}",
						"info",
					)
			except Exception as cleanup_error:
				self.cls_create_log.log_message(
					self.logger,
					f"Failed to cleanup temp directory: {cleanup_error}",
					"warning",
				)

	@abstractmethod
	def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
		"""Transform a list of response objects into a DataFrame.

		Parameters
		----------
		file : StringIO
		    The parsed content.
		file_name : str
		    The name of the file.

		Returns
		-------
		pd.DataFrame
		    The transformed DataFrame.
		"""
		return pd.read_csv(file, sep=";")
