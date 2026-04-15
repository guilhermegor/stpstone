"""Mais Retorno Instruments Historical Rentability."""

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
from stpstone.utils.parsers.lists import ListHandler
from stpstone.utils.parsers.numbers import NumHandler
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


class MaisRetornoHistoricalRentability(ABCIngestionOperations):
	"""Mais Retorno monthly historical rentability grid per instrument."""

	_BASE_URL = "https://maisretorno.com/{}/{}"
	_XPATH_LIST_YEARS = (
		"//tbody[@class=\"css-cssveg\"]/tr"
		"/th[@class=\"__variable_a57643 MuiBox-root css-i9lh1x\"][not(*)]"
	)
	_XPATH_LIST_TD_RENTABILITIES = (
		"//div[@class=\"MuiStack-root css-j7qwjs\"]"
		"/span[@style=\"font-size: 0.7rem; color: rgb(107, 113, 137);"
		" font-weight: 400; line-height: 1.5;\"]/preceding-sibling::span"
	)
	_XPATH_LIST_TD_ALPHA = (
		"//div[@class=\"MuiStack-root css-j7qwjs\"]"
		"/span[@style=\"font-size: 0.7rem; color: rgb(107, 113, 137);"
		" font-weight: 400; line-height: 1.5;\"]"
	)
	_LIST_MONTHS = [
		"JAN", "FEB", "MAR", "APR", "MAY", "JUN",
		"JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
	]

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
		"""Initialize the Mais Retorno Historical Rentability ingestion class.

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
		self.cls_list_handler = ListHandler()
		self.cls_dict_handler = HandlingDicts()
		self.date_ref = date_ref or \
			self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
		self.list_slugs = list_slugs or ["aasl-fia"]
		self.instruments_class = instruments_class or "fundo"
		self.bool_headless = bool_headless
		self.int_wait_load_seconds = int_wait_load_seconds
		self.url = self._BASE_URL

	def run(
		self,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_mais_retorno_instruments_historic_rentability",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False.
		str_table_name : str
			The name of the table, by default
			'br_mais_retorno_instruments_historic_rentability'.

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		scraper_playwright = self.parse_raw_file()
		df_ = self.transform_data(scraper_playwright=scraper_playwright)
		dict_dtypes: dict = {
			"YEAR": int, "INSTRUMENT": str, "YTD": float, "SINCE_INCEPTION": float
		}
		for month in self._LIST_MONTHS:
			dict_dtypes[month] = float
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
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
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
		"""Scrape monthly rentability grids for all configured instrument slugs.

		Parameters
		----------
		scraper_playwright : PlaywrightScraper
			Configured Playwright scraper instance.

		Returns
		-------
		pd.DataFrame
			DataFrame with historical rentability data for all instruments.
		"""
		list_ser = []
		list_cols = self.cls_list_handler.extend_lists(
			["INSTRUMENT"], self._LIST_MONTHS, ["YTD", "SINCE_INCEPTION"]
		)
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
				)[0]
				list_years = scraper_playwright.get_elements(
					self._XPATH_LIST_YEARS, selector_type="xpath"
				)
				list_years = [int(d["text"]) for d in list_years]
				list_years = list_years * 2
				list_td_rentabilities = scraper_playwright.get_elements(
					self._XPATH_LIST_TD_RENTABILITIES, selector_type="xpath"
				)
				list_td_alpha = scraper_playwright.get_elements(
					self._XPATH_LIST_TD_ALPHA, selector_type="xpath"
				)
				list_td_rentabilities = self._convert_nums(list_td_rentabilities, str_instrument)
				list_td_alpha = self._convert_nums(list_td_alpha, str_instrument)
				list_combined = self.cls_list_handler.extend_lists(
					list_td_rentabilities, list_td_alpha, bool_remove_duplicates=False
				)
				list_rows = self.cls_dict_handler.pair_headers_with_data(
					list_cols, list_combined
				)
				df_slug = pd.DataFrame(list_rows)
				df_slug["YEAR"] = list_years
				list_ser.extend(df_slug.to_dict(orient="records"))
		return pd.DataFrame(list_ser)

	def _convert_nums(
		self,
		list_: list,
		str_instrument: Optional[str] = None,
	) -> list:
		"""Normalise a list of scraped cell values to floats or instrument-labelled strings.

		Parameters
		----------
		list_ : list
			Raw list of element dicts returned by PlaywrightScraper.get_elements.
		str_instrument : Optional[str]
			Instrument identifier prepended to benchmark strings (e.g. 'p.p.' values).

		Returns
		-------
		list
			Processed list with floats, nan, or labelled strings.
		"""
		list_ = [
			self.cls_num_handler.transform_to_float(d, int_precision=6) for d in list_
		]
		if str_instrument:
			list_ = [
				nan
				if x == "-"
				else str_instrument.upper() + " " + x
				if isinstance(x, str) and "p.p." in x
				else x
				for x in list_
			]
		else:
			list_ = [nan if x == "-" else x for x in list_]
		return list_
