"""Tiingo US daily adjusted OHLCV ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from time import sleep
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


# Tiingo free tier recommends a small delay between requests to avoid rate-limiting.
RATE_LIMIT_SLEEP_SECONDS: int = 10

_BASE_URL = "https://api.tiingo.com/tiingo"


class TiingoUS(ABCIngestionOperations):
	"""Tiingo daily adjusted OHLCV ingestion for US equities."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
		token: Optional[str] = None,
		list_slugs: Optional[list[str]] = None,
		date_start: Optional[date] = None,
		date_end: Optional[date] = None,
	) -> None:
		"""Initialize the ingestion class.

		Parameters
		----------
		date_ref : Optional[date]
			Reference date for the ingestion run, by default None (yesterday).
		logger : Optional[Logger]
			Logger instance, by default None.
		cls_db : Optional[Session]
			Database session for persistence, by default None.
		token : Optional[str]
			Tiingo API key, by default None.
		list_slugs : Optional[list[str]]
			Ticker symbols to fetch (e.g. ``["AAPL", "MSFT"]``), by default None.
		date_start : Optional[date]
			Start of the historical window to fetch; defaults to 52 working days before
			``date_ref`` when not provided.
		date_end : Optional[date]
			End of the historical window to fetch; defaults to ``date_ref`` when not
			provided.

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
		self.list_slugs = list_slugs or []
		self.cls_dir_files_management = DirFilesManagement()
		self.cls_dates_current = DatesCurrent()
		self.cls_create_log = CreateLog()
		self.cls_dates_br = DatesBRAnbima()
		self.date_ref = date_ref or self.cls_dates_br.add_working_days(
			self.cls_dates_current.curr_date(), -1
		)
		self.date_start = date_start or self.cls_dates_br.add_working_days(self.date_ref, -52)
		self.date_end = date_end or self.date_ref

	def _build_url(self, slug: str) -> str:
		"""Build the request URL for a single ticker symbol.

		Parameters
		----------
		slug : str
			Ticker symbol (e.g. ``"AAPL"``).

		Returns
		-------
		str
			Fully-qualified Tiingo price endpoint URL for the given symbol.
		"""
		date_start_str = self.date_start.strftime("%Y-%m-%d")
		date_end_str = self.date_end.strftime("%Y-%m-%d")
		return (
			f"{_BASE_URL}/daily/{slug}/prices"
			f"?startDate={date_start_str}"
			f"&endDate={date_end_str}"
			f"&format=json"
			f"&resampleFreq=daily"
		)

	def run(
		self,
		bool_verify: bool = False,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "us_tiingo_ohlcv_adjusted",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion pipeline over all configured ticker symbols.

		Iterates over ``list_slugs``, fetches and transforms adjusted OHLCV data for
		each symbol, concatenates all results, standardises types, and either
		persists to the database (when ``cls_db`` is set) or returns the combined
		DataFrame.

		Parameters
		----------
		bool_verify : bool
			Whether to verify the SSL certificate, by default False.
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			Request timeout passed to ``requests.get``, by default (12.0, 21.0).
		bool_insert_or_ignore : bool
			Whether to use INSERT OR IGNORE semantics when persisting, by default False.
		str_table_name : str
			Target database table name, by default ``"us_tiingo_ohlcv_adjusted"``.

		Returns
		-------
		Optional[pd.DataFrame]
			Combined adjusted OHLCV DataFrame when no database session is configured,
			otherwise ``None``.
		"""
		if not self.list_slugs:
			return None

		list_frames: list[pd.DataFrame] = []

		for slug in self.list_slugs:
			url = self._build_url(slug)
			resp_req = self.get_response(url=url, timeout=timeout, bool_verify=bool_verify)
			sleep(RATE_LIMIT_SLEEP_SECONDS)
			df_slug = self.transform_data(resp_req=resp_req, slug=slug)
			if df_slug is not None and not df_slug.empty:
				list_frames.append(df_slug)

		if not list_frames:
			return None

		df_ = pd.concat(list_frames, ignore_index=True)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"DATE": str,
				"TICKER": str,
				"CLOSE": float,
				"HIGH": float,
				"LOW": float,
				"OPEN": float,
				"VOLUME": float,
				"ADJ_CLOSE": float,
				"ADJ_HIGH": float,
				"ADJ_LOW": float,
				"ADJ_OPEN": float,
				"ADJ_VOLUME": float,
				"DIV_CASH": float,
				"SPLIT_FACTOR": float,
			},
			str_fmt_dt="YYYY-MM-DD",
			url=_BASE_URL,
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

	@backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_time=60)
	def get_response(
		self,
		url: str,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = False,
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Fetch the raw HTTP response for a single ticker symbol URL.

		Parameters
		----------
		url : str
			Fully-qualified Tiingo price endpoint URL (from ``_build_url``).
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			Request timeout, by default (12.0, 21.0).
		bool_verify : bool
			Whether to verify the SSL certificate, by default False.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
			HTTP response object.
		"""
		headers = {
			"Content-Type": "application/json",
			"Authorization": f"Token {self.token}",
		}
		resp_req = requests.get(url, headers=headers, timeout=timeout, verify=bool_verify)
		resp_req.raise_for_status()
		return resp_req

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
	) -> StringIO:
		"""Parse the raw response into a StringIO stream.

		This method satisfies the ``ABCIngestion`` contract. For Tiingo, the actual
		data extraction is JSON-based and handled by ``transform_data``.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			HTTP response object.

		Returns
		-------
		StringIO
			Response text wrapped in a StringIO buffer.
		"""
		return self.get_file(resp_req=resp_req)

	def transform_data(
		self,
		resp_req: Response,
		slug: str,
	) -> Optional[pd.DataFrame]:
		"""Parse the JSON response and build a flat adjusted OHLCV DataFrame.

		Parameters
		----------
		resp_req : Response
			HTTP response whose JSON body is a list of daily price records as returned
			by the Tiingo ``/daily/{ticker}/prices`` endpoint.
		slug : str
			Ticker symbol added as the ``TICKER`` column to each row.

		Returns
		-------
		Optional[pd.DataFrame]
			Flat adjusted OHLCV DataFrame for the given ticker, or ``None`` when the
			JSON payload is empty or cannot be decoded.
		"""
		try:
			json_ = resp_req.json()
		except Exception:
			self.cls_create_log.log_message(
				logger=self.logger,
				message=f"Tiingo: failed to decode JSON for slug '{slug}'",
				log_level="warning",
			)
			return None

		if not json_:
			self.cls_create_log.log_message(
				logger=self.logger,
				message=f"Tiingo: empty payload for slug '{slug}'",
				log_level="warning",
			)
			return None

		list_rows: list[dict] = [
			{
				"DATE": record.get("date"),
				"TICKER": slug,
				"CLOSE": record.get("close"),
				"HIGH": record.get("high"),
				"LOW": record.get("low"),
				"OPEN": record.get("open"),
				"VOLUME": record.get("volume"),
				"ADJ_CLOSE": record.get("adjClose"),
				"ADJ_HIGH": record.get("adjHigh"),
				"ADJ_LOW": record.get("adjLow"),
				"ADJ_OPEN": record.get("adjOpen"),
				"ADJ_VOLUME": record.get("adjVolume"),
				"DIV_CASH": record.get("divCash"),
				"SPLIT_FACTOR": record.get("splitFactor"),
			}
			for record in json_
		]

		return pd.DataFrame(list_rows)
