"""Brazilian IRS (Receita Federal) NCEA (Cnaes) open data ingestion.

Fetches the single ZIP file (Cnaes.zip) from the Receita Federal NextCloud
share for a given reference month, extracts the semicolon-delimited CSV inside
the archive, and returns a DataFrame with standardised column names and types.
"""

from datetime import date
from io import BytesIO, StringIO
from logging import Logger
from typing import Optional, Union
from zipfile import ZipFile

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


_NUM_FILES: int = 1

_FILE_PREFIX: str = "Cnaes"

_COLUMN_NAMES: list[str] = [
	"CODE",
	"DESCRIPTION",
]

_COLUMN_DTYPES: dict[str, type] = {
	"CODE": str,
	"DESCRIPTION": str,
}

_BASE_SHARE_URL: str = "https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9"

_DEFAULT_HEADERS: dict[str, str] = {
	"Accept": (
		"text/html,application/xhtml+xml,application/xml;q=0.9,"
		"image/avif,image/webp,image/apng,*/*;q=0.8,"
		"application/signed-exchange;v=b3;q=0.7"
	),
	"Accept-Language": "pt-BR,pt;q=0.9",
	"Connection": "keep-alive",
	"Sec-Fetch-Dest": "document",
	"Sec-Fetch-Mode": "navigate",
	"Sec-Fetch-Site": "same-origin",
	"Sec-Fetch-User": "?1",
	"Upgrade-Insecure-Requests": "1",
	"User-Agent": (
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
		"AppleWebKit/537.36 (KHTML, like Gecko) "
		"Chrome/132.0.0.0 Safari/537.36"
	),
	"sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
	"sec-ch-ua-mobile": "?0",
	"sec-ch-ua-platform": '"Windows"',
}


class IRSBRNCEA(ABCIngestionOperations):
	"""Brazilian IRS (Receita Federal) NCEA (Cnaes) open data ingestion class."""

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
			self.cls_dates_current.curr_date(), -1
		)
		self.year_month = self.date_ref.strftime("%Y-%m")
		self.dir_url = f"{_BASE_SHARE_URL}?dir=/{self.year_month}"

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = False,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_irs_ncea",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		Fetches ``Cnaes.zip``, parses the CSV inside, and returns the
		resulting DataFrame. If a database session is provided the data is
		inserted into the database; otherwise the transformed DataFrame is
		returned.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Whether to verify the SSL certificate, by default False.
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False.
		str_table_name : str
			The name of the table, by default 'br_irs_ncea'.

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame when no database session is provided;
			``None`` otherwise.
		"""
		filename = f"{_FILE_PREFIX}.zip"
		url = f"{_BASE_SHARE_URL}/download?path=/{self.year_month}&files={filename}"
		try:
			resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify, url=url)
		except requests.exceptions.HTTPError:
			return pd.DataFrame()
		file = self.parse_raw_file(resp_req=resp_req)
		df_ = self.transform_data(file=file)
		if df_.empty:
			return pd.DataFrame()
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes=_COLUMN_DTYPES,
			str_fmt_dt="YYYY-MM-DD",
			url=self.dir_url,
			logger=self.logger,
		)
		if self.cls_db:
			self.insert_table_db(
				cls_db=self.cls_db,
				str_table_name=str_table_name,
				df_=df_,
				bool_insert_or_ignore=bool_insert_or_ignore,
			)
			return None
		return df_

	@backoff.on_exception(
		backoff.expo,
		requests.exceptions.HTTPError,
		max_time=60,
	)
	def get_response(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = False,
		url: Optional[str] = None,
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Fetch the raw HTTP response for a given URL.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Whether to verify the SSL certificate, by default False.
		url : Optional[str]
			The URL to fetch. When ``None``, uses ``self.dir_url``,
			by default None.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
			The HTTP response object.
		"""
		target_url = url or self.dir_url
		headers = {
			**_DEFAULT_HEADERS,
			"Referer": self.dir_url,
		}
		resp_req = requests.get(
			target_url,
			timeout=timeout,
			verify=bool_verify,
			headers=headers,
		)
		resp_req.raise_for_status()
		return resp_req

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
	) -> BytesIO:
		"""Parse the raw ZIP response into an in-memory bytes stream.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The HTTP response object.

		Returns
		-------
		BytesIO
			The response content as a bytes stream.
		"""
		return BytesIO(resp_req.content)

	def transform_data(
		self,
		file: Optional[Union[StringIO, BytesIO]] = None,
	) -> pd.DataFrame:
		"""Extract CSV data from a ZIP archive and return a DataFrame.

		The ZIP file is expected to contain a single CSV file encoded in
		``latin-1`` with semicolon separators and no header row. Column
		names are taken from the module-level ``_COLUMN_NAMES`` constant.

		Parameters
		----------
		file : Optional[Union[StringIO, BytesIO]], optional
			The bytes stream of the ZIP file, by default None.

		Returns
		-------
		pd.DataFrame
			The parsed DataFrame.
		"""
		if file is None:
			return pd.DataFrame()
		try:
			with ZipFile(file) as zf:
				list_frames: list[pd.DataFrame] = []
				for str_name in zf.namelist():
					if not str_name.lower().endswith(".csv"):
						continue
					with zf.open(str_name) as csv_file:
						df_ = pd.read_csv(
							csv_file,
							sep=";",
							header=None,
							names=_COLUMN_NAMES,
							encoding="latin-1",
							dtype=str,
							on_bad_lines="skip",
						)
						list_frames.append(df_)
				if not list_frames:
					return pd.DataFrame()
				return pd.concat(list_frames, ignore_index=True)
		except Exception:
			self.cls_create_log.log_message(
				logger=self.logger,
				message="Failed to extract CSV from ZIP archive — returning empty DataFrame.",
				log_level="warning",
			)
			return pd.DataFrame()
