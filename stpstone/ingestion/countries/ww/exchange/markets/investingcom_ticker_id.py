"""Investing.com ticker ID lookup ingestion."""

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


class InvestingComTickerId(ABCIngestionOperations):
	"""Investing.com ticker ID lookup ingestion class."""

	_BASE_URL = "https://tvc4.investing.com/"
	_HEADERS = {
		"User-Agent": (
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
			"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
		),
		"Referer": "https://www.investing.com/",
		"Accept": "application/json, text/plain, */*",
		"Accept-Language": "en-US,en;q=0.9",
		"Accept-Encoding": "gzip, deflate, br",
		"Connection": "keep-alive",
	}

	def __init__(
		self,
		str_ticker: str = "PETR4",
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the ingestion class.

		Parameters
		----------
		str_ticker : str
			The ticker symbol to look up, by default 'PETR4'.
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

		self.str_ticker = str_ticker
		self.logger = logger
		self.cls_db = cls_db
		self.cls_dir_files_management = DirFilesManagement()
		self.cls_dates_current = DatesCurrent()
		self.cls_create_log = CreateLog()
		self.cls_dates_br = DatesBRAnbima()
		self.date_ref = date_ref or \
			self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
		self.url = (
			f"{self._BASE_URL}725910b675af9252224ca6069a1e73cc/1631836267/1/1/8"
			f"/symbols?symbol={self.str_ticker}"
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = False,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "ww_investingcom_ticker_id",
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
			The name of the table, by default 'ww_investingcom_ticker_id'.

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
		df_ = self.transform_data(resp_req=resp_req)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"NAME": str,
				"EXCHANGE_TRADED": str,
				"EXCHANGE_LISTED": str,
				"TIMEZONE": str,
				"PRICESCALE": int,
				"POINTVALUE": int,
				"TICKER": str,
				"TYPE": str,
			},
			cols_from_case="lower_constant",
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
		"""Return a response object from Investing.com.

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
		resp_req = requests.get(
			self.url,
			headers=self._HEADERS,
			timeout=timeout,
			verify=bool_verify,
		)
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
		"""Transform an Investing.com symbols JSON response into a DataFrame.

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
