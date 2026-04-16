"""Mais Retorno Instruments Statistics."""

from datetime import date
from logging import Logger
from math import nan
from typing import Optional, Union

import pandas as pd
from requests import Session

from stpstone.ingestion.abc.ingestion_abc import (
	ABCIngestionOperations,
	ContentParser,
	CoreIngestion,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.numbers import NumHandler
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


class MaisRetornoStats(ABCIngestionOperations):
	"""Mais Retorno instrument statistics across multiple time windows."""

	_BASE_URL = "https://maisretorno.com/{}/{}"
	_XPATH_LIST_STAS = (
		'//th[@class="MuiTableCell-root MuiTableCell-body MuiTableCell-sizeMedium css-qixyx5"]'
	)
	_XPATH_LIST_TD = (
		'//section[@id="profitability-ratio"]'
		'//td[@class="MuiTableCell-root MuiTableCell-body MuiTableCell-sizeMedium'
		' css-qixyx5"]/span[@data-testid="negative-number"]'
	)
	_LIST_COLS = ["YTD", "MTD", "LTM", "L_24M", "L_36M", "L_48M", "L_60M", "SINCE_INCEPTION"]

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
		list_slugs: Optional[list] = None,
		instruments_class: Optional[str] = None,
		bool_headless: bool = True,
		int_wait_load_seconds: int = 60,
	) -> None:
		"""Initialize the Mais Retorno Stats ingestion class.

		Parameters
		----------
		date_ref : Optional[date]
			The date of reference, by default None.
		logger : Optional[Logger]
			The logger, by default None.
		cls_db : Optional[Session]
			The database session, by default None.
		list_slugs : Optional[list]
			List of instrument slug identifiers, by default None.
		instruments_class : Optional[str]
			URL segment for the instrument class (e.g. 'fii'), by default None.
		bool_headless : bool
			Whether to run the browser in headless mode, by default True.
		int_wait_load_seconds : int
			Seconds to wait for page elements to load, by default 60.

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
		self.cls_num_handler = NumHandler()
		self.cls_dict_handler = HandlingDicts()
		self.date_ref = date_ref or self.cls_dates_br.add_working_days(
			self.cls_dates_current.curr_date(), -1
		)
		self.list_slugs = list_slugs or ["aasl-fia"]
		self.instruments_class = instruments_class or "fundo"
		self.bool_headless = bool_headless
		self.int_wait_load_seconds = int_wait_load_seconds
		self.url = self._BASE_URL

	def run(
		self,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_mais_retorno_instruments_stats",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False.
		str_table_name : str
			The name of the table, by default 'br_mais_retorno_instruments_stats'.

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		scraper_playwright = self.parse_raw_file()
		df_ = self.transform_data(scraper_playwright=scraper_playwright)
		dict_dtypes: dict = {"INSTRUMENT": str, "STATISTIC": str}
		for col in self._LIST_COLS:
			dict_dtypes[col] = float
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes=dict_dtypes,
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

	def get_response(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
	) -> None:
		"""Return None — Playwright does not use HTTP responses.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Verify the SSL certificate, by default True.

		Returns
		-------
		None
		"""
		pass

	def parse_raw_file(self) -> PlaywrightScraper:
		"""Return a PlaywrightScraper instance for browser automation.

		Returns
		-------
		PlaywrightScraper
			Configured PlaywrightScraper instance.
		"""
		return PlaywrightScraper(
			bool_headless=self.bool_headless,
			int_default_timeout=self.int_wait_load_seconds * 1_000,
			logger=self.logger,
		)

	def transform_data(self, scraper_playwright: PlaywrightScraper) -> pd.DataFrame:
		"""Scrape statistics tables for all configured instrument slugs.

		Parameters
		----------
		scraper_playwright : PlaywrightScraper
			Configured Playwright scraper instance.

		Returns
		-------
		pd.DataFrame
			DataFrame with statistics for all instruments and time windows.
		"""
		list_ser = []
		with scraper_playwright.launch():
			for slug in self.list_slugs:
				page_url = self._BASE_URL.format(self.instruments_class, slug)
				if not scraper_playwright.navigate(page_url):
					self.cls_create_log.log_message(
						self.logger,
						f"Failed to navigate to instrument page '{slug}': {page_url}",
						"warning",
					)
					continue
				str_instrument = self.cls_dir_files_management.get_filename_parts_from_url(
					scraper_playwright.get_current_url()
				)[0].upper()
				list_stas = scraper_playwright.get_elements(
					self._XPATH_LIST_STAS, selector_type="xpath"
				)
				list_td = scraper_playwright.get_elements(
					self._XPATH_LIST_TD, selector_type="xpath"
				)
				list_td = self._convert_nums(list_td)
				list_rows = self.cls_dict_handler.pair_headers_with_data(self._LIST_COLS, list_td)
				df_slug = pd.DataFrame(list_rows)
				df_slug["STATISTIC"] = [d["text"] for d in list_stas]
				df_slug["INSTRUMENT"] = str_instrument
				ordered_cols = ["INSTRUMENT", "STATISTIC"] + self._LIST_COLS
				df_slug = df_slug[ordered_cols]
				list_ser.extend(df_slug.to_dict(orient="records"))
		return pd.DataFrame(list_ser)

	def _convert_nums(self, list_: list) -> list:
		"""Normalise a list of scraped cell values to floats or nan.

		Parameters
		----------
		list_ : list
			Raw list of element dicts returned by PlaywrightScraper.get_elements.

		Returns
		-------
		list
			Processed list with floats or nan for missing/dash values.
		"""
		list_ = [self.cls_num_handler.transform_to_float(d, int_precision=6) for d in list_]
		return [nan if x == "-" else x for x in list_]
