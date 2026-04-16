"""Implementation of CVM FIF CDA (Composição e Diversificação das Aplicações) ingestion."""

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


class FIFCDA(ABCIngestionOperations):
	"""CVM FIF CDA (Composição e Diversificação das Aplicações) Data.

	This class handles the ingestion of CDA data for investment funds from the
	Brazilian Securities and Exchange Commission (CVM). The data includes
	detailed composition and diversification of fund investments across
	different asset types. The source ZIP file contains multiple CSV files
	that are consolidated into a single DataFrame.
	"""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the FIF CDA ingestion class.

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

		str_date_fmt = self.date_ref.strftime("%Y%m")
		self.url = f"https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/cda_fi_{str_date_fmt}.zip"

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_cvm_fif_cda",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process for CDA data.

		If a database session is provided, data is inserted into the database.
		Otherwise, the consolidated DataFrame is returned for further processing.

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
			Target database table name, by default "br_cvm_fif_cda".

		Returns
		-------
		Optional[pd.DataFrame]
			The consolidated DataFrame if no database session is provided,
			otherwise None.
		"""
		resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
		files_list = self.parse_raw_file(resp_req)
		df_ = self.transform_data(files_list=files_list)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"TP_FUNDO_CLASSE": str,
				"CNPJ_FUNDO_CLASSE": str,
				"DENOM_SOCIAL": str,
				"DT_COMPTC": "date",
				"ID_DOC": str,
				"TP_APLIC": str,
				"TP_ATIVO": str,
				"EMISSOR_LIGADO": str,
				"TP_NEGOC": str,
				"QT_VENDA_NEGOC": float,
				"VL_VENDA_NEGOC": float,
				"QT_AQUIS_NEGOC": float,
				"VL_AQUIS_NEGOC": float,
				"QT_POS_FINAL": float,
				"VL_MERC_POS_FINAL": float,
				"VL_CUSTO_POS_FINAL": float,
				"DT_CONFID_APLIC": str,
				"TP_TITPUB": str,
				"CD_ISIN": str,
				"CD_SELIC": str,
				"DT_EMISSAO": str,
				"DT_VENC": str,
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
		"""Fetch ZIP file containing CDA data from CVM website.

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
			f"Fetching CDA data from URL: {self.url}",
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
	) -> list[tuple[StringIO, str]]:
		"""Parse raw ZIP file and extract all CSV files.

		Extracts all CSV files from the ZIP archive and converts them to
		StringIO objects with proper encoding handling for Brazilian
		Portuguese text.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The HTTP response object containing ZIP data.

		Returns
		-------
		list[tuple[StringIO, str]]
			List of tuples, each containing:
			- StringIO object with decoded CSV content
			- Filename string for tracking purposes

		Raises
		------
		ValueError
			If no files found in ZIP or no CSV files present.
		"""
		self.cls_create_log.log_message(
			self.logger,
			"Extracting CSV files from ZIP archive in memory",
			"info",
		)

		files_list = self.cls_dir_files_management.recursive_unzip_in_memory(
			BytesIO(resp_req.content)
		)

		if not files_list:
			raise ValueError("No files found in the downloaded ZIP content")

		csv_files = []
		for file_content, filename in files_list:
			if not filename.lower().endswith(".csv"):
				self.cls_create_log.log_message(
					self.logger,
					f"Skipping non-CSV file: {filename}",
					"info",
				)
				continue

			self.cls_create_log.log_message(
				self.logger,
				f"Processing CSV file: {filename}",
				"info",
			)

			csv_string_io = self._decode_csv_content(file_content, filename)
			csv_files.append((csv_string_io, filename))

		if not csv_files:
			raise ValueError("No CSV files found in the downloaded ZIP")

		self.cls_create_log.log_message(
			self.logger,
			f"Successfully extracted {len(csv_files)} CSV files from ZIP",
			"info",
		)

		return csv_files

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
		files_list: list[tuple[StringIO, str]],
	) -> pd.DataFrame:
		"""Transform multiple CSV files into consolidated DataFrame.

		Reads all CSV files, adds file metadata, and consolidates them into
		a single DataFrame. Handles files with different column structures by
		using union of all columns and filling missing values.

		Parameters
		----------
		files_list : list[tuple[StringIO, str]]
			List of tuples containing StringIO objects and filenames.

		Returns
		-------
		pd.DataFrame
			Consolidated DataFrame with CVM CDA data from all CSV files.

		Raises
		------
		ValueError
			If no valid data could be loaded from any file.
		"""
		self.cls_create_log.log_message(
			self.logger,
			f"Consolidating {len(files_list)} CSV files into single DataFrame",
			"info",
		)

		dataframes = []
		total_rows = 0

		for file_io, filename in files_list:
			try:
				df_temp = pd.read_csv(file_io, sep=";")
				df_temp["FILE_NAME"] = filename
				dataframes.append(df_temp)
				total_rows += len(df_temp)

				self.cls_create_log.log_message(
					self.logger,
					f"Loaded {len(df_temp)} rows from {filename}",
					"info",
				)
			except pd.errors.EmptyDataError:
				self.cls_create_log.log_message(
					self.logger,
					f"Skipping empty file: {filename}",
					"warning",
				)
			except pd.errors.ParserError as e:
				self.cls_create_log.log_message(
					self.logger,
					f"Error parsing {filename}: {str(e)}",
					"error",
				)
			except Exception as e:
				self.cls_create_log.log_message(
					self.logger,
					f"Unexpected error processing {filename}: {str(e)}",
					"error",
				)

		if not dataframes:
			raise ValueError("No valid data could be loaded from any CSV file")

		self.cls_create_log.log_message(
			self.logger,
			"Concatenating all dataframes",
			"info",
		)

		df_consolidated = pd.concat(dataframes, ignore_index=True, sort=False)

		self.cls_create_log.log_message(
			self.logger,
			f"Successfully consolidated {total_rows} total rows into "
			f"DataFrame with {len(df_consolidated.columns)} columns",
			"info",
		)

		return df_consolidated
