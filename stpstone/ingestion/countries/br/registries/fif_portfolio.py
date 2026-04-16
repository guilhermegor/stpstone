"""Implementation of CVM FIF Portfolio Composition ingestion."""

from datetime import date
from io import BytesIO, StringIO
from logging import Logger
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


class FIFPortfolio(ABCIngestionOperations):
	"""CVM FIF Portfolio Composition Data - concrete implementation.

	This class handles the ingestion of portfolio composition data for investment funds
	from the Brazilian Securities and Exchange Commission (CVM). The data includes
	detailed breakdown of fund investments across different asset types, showing the
	percentage allocation of each asset class in the fund's portfolio.
	"""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the FIF Portfolio ingestion class.

		Parameters
		----------
		date_ref : Optional[date]
			The date of reference for data retrieval. If None, defaults to 22
			working days before current date, by default None.
		logger : Optional[Logger]
			Logger instance for tracking operations, by default None.
		cls_db : Optional[Session]
			Database session for data persistence, by default None.

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
		self.date_ref = date_ref or self.cls_dates_br.add_working_days(
			self.cls_dates_current.curr_date(), -22
		)

		str_yearmonth = self.date_ref.strftime("%Y%m")
		self.url = (
			f"https://dados.cvm.gov.br/dados/FI/DOC/LAMINA/DADOS/lamina_fi_{str_yearmonth}.zip"
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_cvm_fif_portfolio",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process for portfolio composition data.

		If a database session is provided, data is inserted into the database.
		Otherwise, the transformed DataFrame is returned for further processing.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			Request timeout in seconds. Can be a single value or tuple of
			(connect, read) timeouts, by default (12.0, 21.0).
		bool_verify : bool
			Whether to verify SSL certificates, by default True.
		bool_insert_or_ignore : bool
			If True, uses INSERT OR IGNORE for database operations,
			by default False.
		str_table_name : str
			Target database table name, by default "br_cvm_fif_portfolio".

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame if no database session is provided,
			otherwise None.
		"""
		resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
		file, _ = self.parse_raw_file(resp_req)
		df_ = self.transform_data(file=file)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"TP_FUNDO_CLASSE": str,
				"CNPJ_FUNDO_CLASSE": str,
				"ID_SUBCLASSE": str,
				"DENOM_SOCIAL": str,
				"DT_COMPTC": "date",
				"TP_ATIVO": str,
				"PR_PL_ATIVO": float,
				"FILE_NAME": "category",
			},
			str_fmt_dt="YYYY-MM-DD",
			url=self.url,
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

	@backoff.on_exception(
		backoff.expo,
		requests.exceptions.HTTPError,
		max_time=60,
	)
	def get_response(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Fetch ZIP file containing portfolio data from CVM website.

		Performs HTTP GET request with exponential backoff retry logic for
		handling transient network errors.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			Request timeout in seconds, by default (12.0, 21.0).
		bool_verify : bool
			Whether to verify SSL certificates, by default True.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
			The HTTP response object containing ZIP data.
		"""
		self.cls_create_log.log_message(
			self.logger,
			f"Fetching portfolio data from URL: {self.url}",
			"info",
		)

		resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
		resp_req.raise_for_status()

		self.cls_create_log.log_message(
			self.logger,
			f"Successfully fetched {len(resp_req.content)} bytes from CVM",
			"info",
		)

		return resp_req

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
	) -> tuple[StringIO, str]:
		"""Parse raw ZIP file and extract only the lamina_fi_carteira_*.csv file.

		Extracts only the portfolio composition CSV file (lamina_fi_carteira_*.csv) from
		the ZIP archive and converts it to StringIO object with proper
		encoding handling for Brazilian Portuguese text.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The HTTP response object containing ZIP data.

		Returns
		-------
		tuple[StringIO, str]
			A tuple containing:
			- StringIO object with decoded CSV content
			- Filename string for tracking purposes

		Raises
		------
		ValueError
			If no files found in ZIP or portfolio CSV file not present.
		"""
		self.cls_create_log.log_message(
			self.logger,
			"Extracting lamina_fi_carteira_*.csv file from ZIP archive in memory",
			"info",
		)

		files_list = self.cls_dir_files_management.recursive_unzip_in_memory(
			BytesIO(resp_req.content)
		)

		if not files_list:
			raise ValueError("No files found in the downloaded ZIP content")

		portfolio_csv_file = None
		portfolio_filename = None

		str_yearmonth = self.date_ref.strftime("%Y%m")
		expected_portfolio_filename = f"lamina_fi_carteira_{str_yearmonth}.csv"

		for file_content, filename in files_list:
			if filename == expected_portfolio_filename:
				portfolio_csv_file = file_content
				portfolio_filename = filename
				self.cls_create_log.log_message(
					self.logger,
					f"Found portfolio composition file: {filename}",
					"info",
				)
				break

		if portfolio_csv_file is None:
			for file_content, filename in files_list:
				if filename.lower().endswith(".csv") and "carteira" in filename.lower():
					portfolio_csv_file = file_content
					portfolio_filename = filename
					self.cls_create_log.log_message(
						self.logger,
						f"Using alternative portfolio composition file: {filename}",
						"info",
					)
					break

		if portfolio_csv_file is None:
			raise ValueError(
				f"Portfolio composition file {expected_portfolio_filename} "
				"not found in the downloaded ZIP"
			)

		csv_string_io = self._decode_csv_content(portfolio_csv_file, portfolio_filename)

		self.cls_create_log.log_message(
			self.logger,
			f"Successfully extracted and decoded portfolio CSV file: {portfolio_filename}",
			"info",
		)

		return csv_string_io, portfolio_filename

	def _decode_csv_content(
		self,
		file_content: Union[BytesIO, str, StringIO],
		filename: str,
	) -> StringIO:
		"""Decode CSV content with proper encoding handling.

		Tries multiple encodings (UTF-8, Latin-1, CP1252) to properly decode
		Brazilian Portuguese text content.

		Parameters
		----------
		file_content : Union[BytesIO, str, StringIO]
			The file content in various possible formats.
		filename : str
			The filename for logging purposes.

		Returns
		-------
		StringIO
			StringIO object with decoded content.
		"""
		if isinstance(file_content, StringIO):
			return file_content

		if isinstance(file_content, str):
			return StringIO(file_content)

		if isinstance(file_content, BytesIO):
			for encoding in ("utf-8", "latin-1", "cp1252"):
				try:
					content_str = file_content.getvalue().decode(encoding)
					self.cls_create_log.log_message(
						self.logger,
						f"Successfully decoded {filename} with {encoding} encoding",
						"info",
					)
					return StringIO(content_str)
				except UnicodeDecodeError:
					continue

			self.cls_create_log.log_message(
				self.logger,
				f"Using UTF-8 with error replacement for {filename}",
				"warning",
			)
			return StringIO(file_content.getvalue().decode("utf-8", errors="replace"))

		return StringIO(str(file_content))

	def transform_data(
		self,
		file: StringIO,
	) -> pd.DataFrame:
		"""Transform CSV content into structured DataFrame.

		Reads semicolon-separated CSV data with robust error handling for
		malformed lines and adds metadata column for tracking data source.

		Parameters
		----------
		file : StringIO
			StringIO object containing CSV content.

		Returns
		-------
		pd.DataFrame
			Transformed DataFrame with CVM portfolio composition data and metadata.
		"""
		self.cls_create_log.log_message(
			self.logger,
			"Transforming portfolio CSV data into DataFrame",
			"info",
		)

		try:
			df_ = pd.read_csv(file, sep=";")
		except pd.errors.ParserError as e:
			self.cls_create_log.log_message(
				self.logger,
				f"Parser error encountered: {str(e)}. Trying with error handling.",
				"warning",
			)
			file.seek(0)
			try:
				df_ = pd.read_csv(file, sep=";", on_bad_lines="skip")
				self.cls_create_log.log_message(
					self.logger,
					"Successfully loaded CSV with error handling (skipping bad lines)",
					"info",
				)
			except Exception as exc:
				self.cls_create_log.log_message(
					self.logger,
					f"Failed to load CSV even with error handling: {str(exc)}",
					"error",
				)
				raise

		self.cls_create_log.log_message(
			self.logger,
			f"Successfully loaded {len(df_)} rows and {len(df_.columns)} columns",
			"info",
		)

		file_yearmonth_str = self.date_ref.strftime("%Y%m")
		df_["FILE_NAME"] = f"lamina_fi_carteira_{file_yearmonth_str}.csv"

		return df_
