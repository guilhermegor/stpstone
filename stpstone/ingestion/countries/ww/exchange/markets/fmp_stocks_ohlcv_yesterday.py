"""FMP batch stock OHLCV quotes ingestion."""

from datetime import date
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


class FMPStocksOhlcvYesterday(ABCIngestionOperations):
	"""FMP batch stock OHLCV quotes ingestion class."""

	_BASE_URL = "https://financialmodelingprep.com/stable/"
	_CHUNK_SIZE = 10

	def __init__(
		self,
		token: str,
		list_slugs: list,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the ingestion class.

		Parameters
		----------
		token : str
			FMP API key.
		list_slugs : list
			List of stock symbols to fetch.
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

		self.token = token
		self.list_slugs = list_slugs
		self.logger = logger
		self.cls_db = cls_db
		self.cls_dir_files_management = DirFilesManagement()
		self.cls_dates_current = DatesCurrent()
		self.cls_create_log = CreateLog()
		self.cls_dates_br = DatesBRAnbima()
		self.date_ref = date_ref or \
			self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
		self.url = self._BASE_URL

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = False,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "ww_fmp_stocks_ohlcv_yesterday",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Whether to verify the SSL certificate, by default False.
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False.
		str_table_name : str
			The name of the table, by default 'ww_fmp_stocks_ohlcv_yesterday'.

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		list_frames: list[pd.DataFrame] = []
		for i in range(0, len(self.list_slugs), self._CHUNK_SIZE):
			chunk = ",".join(self.list_slugs[i:i + self._CHUNK_SIZE])
			self.url = f"{self._BASE_URL}batch-quote?symbols={chunk}&apikey={self.token}"
			resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
			list_frames.append(self.transform_data(resp_req=resp_req))
		df_ = pd.concat(list_frames, ignore_index=True)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"SYMBOL": str,
				"NAME": str,
				"PRICE": float,
				"CHANGE_PERCENTAGE": float,
				"CHANGE": float,
				"VOLUME": float,
				"DAY_LOW": float,
				"DAY_HIGH": float,
				"YEAR_HIGH": float,
				"YEAR_LOW": float,
				"MARKET_CAP": float,
				"PRICE_AVG_50": float,
				"PRICE_AVG_200": float,
				"EXCHANGE": str,
				"OPEN": float,
				"PREVIOUS_CLOSE": float,
				"TIMESTAMP": int,
			},
			cols_from_case="camel",
			cols_to_case="upper_constant",
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

	@backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_time=60)
	def get_response(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = False,
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Return a response object from FMP.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Verify the SSL certificate, by default False.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
			A response object.
		"""
		resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
		resp_req.raise_for_status()
		return resp_req

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Pass through the response object for JSON parsing in transform_data.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The response object.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
			The same response object, unchanged.
		"""
		return resp_req

	def transform_data(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
	) -> pd.DataFrame:
		"""Transform a FMP JSON response into a DataFrame.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The response object with a JSON array body.

		Returns
		-------
		pd.DataFrame
			The transformed DataFrame.
		"""
		return pd.DataFrame(resp_req.json())
