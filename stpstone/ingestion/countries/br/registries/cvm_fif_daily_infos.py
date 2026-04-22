"""Implementation of CVM FIF Daily Infos ingestion."""

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


class CvmFIFDailyInfos(ABCIngestionOperations):
	"""Liquid funds daily infos from CVM Data - concrete implementation."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the ingestion class.

		Parameters
		----------
		date_ref : Optional[date]
			The date of reference, by default None.
		logger : Optional[Logger]
			The logger, by default None.
		cls_db : Optional[Session]
			The database session, by default None.

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
			self.cls_dates_current.curr_date(), -4
		)

		str_date_fmt = self.date_ref.strftime("%Y%m")
		self.url = (
			"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_"
			+ f"{str_date_fmt}.zip"
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_cvm_data",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Whether to verify the SSL certificate, by default True.
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False.
		str_table_name : str
			The name of the table, by default "br_cvm_data".

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
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
				"DT_COMPTC": "date",
				"VL_TOTAL": float,
				"VL_QUOTA": float,
				"VL_PATRIM_LIQ": float,
				"CAPTC_DIA": float,
				"RESG_DIA": float,
				"NR_COTST": int,
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
		"""Return a response object with the ZIP file content.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Verify the SSL certificate, by default True.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
			The response object.
		"""
		self.cls_create_log.log_message(
			self.logger,
			f"Fetching data from URL: {self.url}",
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
		"""Parse the raw ZIP file content in memory and extract CSV.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The response object.

		Returns
		-------
		tuple[StringIO, str]
			The parsed CSV content as StringIO and the filename.

		Raises
		------
		ValueError
			If no CSV files found in the ZIP archive.
		"""
		self.cls_create_log.log_message(
			self.logger,
			"Extracting CSV from ZIP file in memory",
			"info",
		)

		files_list = self.cls_dir_files_management.recursive_unzip_in_memory(
			BytesIO(resp_req.content)
		)

		if not files_list:
			raise ValueError("No files found in the downloaded ZIP content")

		csv_file_content = None
		csv_filename = None

		for file_content, filename in files_list:
			if filename.lower().endswith(".csv"):
				csv_file_content = file_content
				csv_filename = filename
				break

		if csv_file_content is None:
			raise ValueError("No CSV file found in the downloaded ZIP")

		self.cls_create_log.log_message(
			self.logger,
			f"Found CSV file: {csv_filename}",
			"info",
		)

		if isinstance(csv_file_content, BytesIO):
			for encoding in ("utf-8", "latin-1", "cp1252"):
				try:
					csv_file_content = StringIO(csv_file_content.getvalue().decode(encoding))
					break
				except UnicodeDecodeError:
					continue
			else:
				csv_file_content = StringIO(
					csv_file_content.getvalue().decode("utf-8", errors="replace")
				)
		elif isinstance(csv_file_content, str):
			csv_file_content = StringIO(csv_file_content)
		elif not isinstance(csv_file_content, StringIO):
			csv_file_content = StringIO(str(csv_file_content))

		self.cls_create_log.log_message(
			self.logger,
			f"Successfully parsed CSV content from {csv_filename}",
			"info",
		)

		return csv_file_content, csv_filename

	def transform_data(
		self,
		file: StringIO,
	) -> pd.DataFrame:
		"""Transform the CSV content into a DataFrame.

		Parameters
		----------
		file : StringIO
			The parsed CSV content.

		Returns
		-------
		pd.DataFrame
			The transformed DataFrame with CVM fund data.
		"""
		self.cls_create_log.log_message(
			self.logger,
			"Transforming CSV data into DataFrame",
			"info",
		)

		df_ = pd.read_csv(file, sep=";")

		self.cls_create_log.log_message(
			self.logger,
			f"Successfully loaded {len(df_)} rows and {len(df_.columns)} columns",
			"info",
		)

		file_date_str = self.date_ref.strftime("%Y%m")
		df_["FILE_NAME"] = f"inf_diario_fi_{file_date_str}.csv"

		return df_
