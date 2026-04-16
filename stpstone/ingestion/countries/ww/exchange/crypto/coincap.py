"""CoinCap cryptocurrency market data ingestion."""

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


class CoinCap(ABCIngestionOperations):
	"""CoinCap cryptocurrency OHLCV latest data ingestion class."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
		token: Optional[str] = None,
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
		token : Optional[str]
			Bearer token for CoinCap API authentication, by default None.

		Returns
		-------
		None
		"""
		super().__init__(cls_db=cls_db)
		CoreIngestion.__init__(self)
		ContentParser.__init__(self)

		self.logger = logger
		self.cls_db = cls_db
		self.token = token
		self.cls_dir_files_management = DirFilesManagement()
		self.cls_dates_current = DatesCurrent()
		self.cls_create_log = CreateLog()
		self.cls_dates_br = DatesBRAnbima()
		self.date_ref = date_ref or self.cls_dates_br.add_working_days(
			self.cls_dates_current.curr_date(), -1
		)
		self.base_url = "https://api.coincap.io/v2/"
		self.slugs = ["bitcoin", "solaris", "ethereum"]
		self.url = self.base_url

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 12.0),
		bool_verify: bool = False,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "ww_coincap_ohlcv_latest",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		Fetches OHLCV-equivalent asset data for each configured slug and combines
		results into a single DataFrame. If a database session is provided the data
		is persisted; otherwise the DataFrame is returned.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 12.0).
		bool_verify : bool
			Whether to verify the SSL certificate, by default False.
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False.
		str_table_name : str
			The name of the table, by default 'ww_coincap_ohlcv_latest'.

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame, or None when a database session is provided.
		"""
		list_dfs: list[pd.DataFrame] = []
		for slug in self.slugs:
			self.url = f"{self.base_url}assets/{slug}"
			resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
			raw = self.parse_raw_file(resp_req)
			df_slug = self.transform_data(raw)
			df_slug = self.standardize_dataframe(
				df_=df_slug,
				date_ref=self.date_ref,
				dict_dtypes={
					"ID": str,
					"RANK": int,
					"SYMBOL": str,
					"NAME": str,
					"SUPPLY": float,
					"MAX_SUPPLY": float,
					"MARKET_CAP_USD": float,
					"VOLUME_USD_24_HR": float,
					"PRICE_USD": float,
					"CHANGE_PERCENT_24_HR": float,
					"VWAP_24_HR": float,
					"EXPLORER": str,
				},
				cols_from_case="camel",
				cols_to_case="upper_constant",
				str_fmt_dt="YYYY-MM-DD",
				type_error_action="raise",
				strategy_keep_when_dupl="first",
				url=self.url,
			)
			list_dfs.append(df_slug)
		df_ = pd.concat(list_dfs, ignore_index=True)
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
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 12.0),
		bool_verify: bool = False,
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Return a response object from the CoinCap API.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 12.0).
		bool_verify : bool
			Verify the SSL certificate, by default False.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
			The HTTP response object.
		"""
		headers: dict[str, str] = {"Accept-Encoding": "gzip"}
		if self.token:
			headers["Authorization"] = f"Bearer {self.token}"
		resp_req = requests.get(
			self.url,
			headers=headers,
			timeout=timeout,
			verify=bool_verify,
		)
		resp_req.raise_for_status()
		return resp_req

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
	) -> dict:
		"""Parse the raw JSON response from CoinCap.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The response object.

		Returns
		-------
		dict
			The parsed JSON payload.
		"""
		return resp_req.json()

	def transform_data(self, raw: dict) -> pd.DataFrame:
		"""Transform the parsed JSON payload into a DataFrame.

		Parameters
		----------
		raw : dict
			The parsed JSON payload containing a 'data' key.

		Returns
		-------
		pd.DataFrame
			A single-row DataFrame with the asset fields.
		"""
		return pd.DataFrame([raw["data"]])
