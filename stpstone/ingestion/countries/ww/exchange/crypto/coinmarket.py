"""CoinMarketCap cryptocurrency listings ingestion."""

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


class CoinMarket(ABCIngestionOperations):
	"""CoinMarketCap OHLCV latest listings ingestion class."""

	def __init__(
		self,
		api_key: str,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the ingestion class.

		Parameters
		----------
		api_key : str
			CoinMarketCap Pro API key used in the X-CMC_PRO_API_KEY header.
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

		self.api_key = api_key
		self.logger = logger
		self.cls_db = cls_db
		self.cls_dir_files_management = DirFilesManagement()
		self.cls_dates_current = DatesCurrent()
		self.cls_create_log = CreateLog()
		self.cls_dates_br = DatesBRAnbima()
		self.date_ref = date_ref or self.cls_dates_br.add_working_days(
			self.cls_dates_current.curr_date(), -1
		)
		self.url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
		self.headers = {
			"Accepts": "application/json",
			"X-CMC_PRO_API_KEY": self.api_key,
		}
		self.params = {
			"start": "1",
			"limit": "5000",
			"convert": "USD",
		}

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 12.0),
		bool_verify: bool = False,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "ww_coinmarket_ohlcv_latest",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 12.0).
		bool_verify : bool
			Whether to verify the SSL certificate, by default False.
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False.
		str_table_name : str
			The name of the table, by default 'ww_coinmarket_ohlcv_latest'.

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
		file = self.parse_raw_file(resp_req)
		df_ = self.transform_data(file=file)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"ID": str,
				"NAME": str,
				"SYMBOL": str,
				"PRICE": float,
				"MARKET_CAP": float,
				"VOLUME": float,
				"SLUG": str,
				"TOTAL_SUPPLY": float,
				"CMC_RANK": int,
				"NUM_MARKET_PAIRS": int,
				"LAST_UPDATE": str,
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

	@backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_time=60)
	def get_response(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 12.0),
		bool_verify: bool = False,
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Return a response object from the CoinMarketCap API.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 12.0).
		bool_verify : bool
			Verify the SSL certificate, by default False.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
			A response object.
		"""
		resp_req = requests.get(
			self.url,
			headers=self.headers,
			params=self.params,
			timeout=timeout,
			verify=bool_verify,
		)
		resp_req.raise_for_status()
		return resp_req

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
	) -> Response:
		"""Return the raw response for JSON extraction.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The response object.

		Returns
		-------
		Response
			The original response object, passed through for JSON parsing.
		"""
		return resp_req

	def transform_data(self, file: Response) -> pd.DataFrame:
		"""Transform the JSON API response into a DataFrame.

		Parameters
		----------
		file : Response
			The response object containing the JSON payload.

		Returns
		-------
		pd.DataFrame
			The transformed DataFrame with one row per cryptocurrency listing.
		"""
		list_ser = []
		json_ = file.json()
		for dict_ in json_["data"]:
			list_ser.append(
				{
					"ID": dict_["id"],
					"NAME": dict_["name"],
					"SYMBOL": dict_["symbol"],
					"PRICE": dict_["quote"]["USD"]["price"],
					"MARKET_CAP": dict_["quote"]["USD"]["market_cap"],
					"VOLUME": dict_["quote"]["USD"]["volume_24h"],
					"SLUG": dict_["slug"],
					"TOTAL_SUPPLY": dict_["total_supply"],
					"CMC_RANK": dict_["cmc_rank"],
					"NUM_MARKET_PAIRS": dict_["num_market_pairs"],
					"LAST_UPDATE": dict_["last_updated"],
				}
			)
		return pd.DataFrame(list_ser)
