"""Mais Retorno Instruments Consistency Metrics."""

from datetime import date
from logging import Logger
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
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.numbers import NumHandler
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


class MaisRetornoConsistency(ABCIngestionOperations):
	"""Mais Retorno instrument consistency metrics (positive/negative months, returns)."""

	_BASE_URL = "https://maisretorno.com/{}/{}"
	_XPATH_SPAN_POSITIVE_MONTHS = (
		"(//section[@id=\"consistency-table\"]"
		"//span[@data-testid=\"negative-number\"])[1]"
	)
	_XPATH_SPAN_NEGATIVE_MONTHS = (
		"(//section[@id=\"consistency-table\"]"
		"//span[@data-testid=\"negative-number\"])[3]"
	)
	_XPATH_SPAN_GREATEST_RETURN = (
		"(//section[@id=\"consistency-table\"]"
		"//span[@data-testid=\"negative-number\"])[5]"
	)
	_XPATH_SPAN_LEAST_RETURN = (
		"(//section[@id=\"consistency-table\"]"
		"//span[@data-testid=\"negative-number\"])[6]"
	)

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
		"""Initialize the Mais Retorno Consistency ingestion class.

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
		str_table_name: str = "br_mais_retorno_instruments_consistency",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False.
		str_table_name : str
			The name of the table, by default 'br_mais_retorno_instruments_consistency'.

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		scraper_playwright = self.parse_raw_file()
		df_ = self.transform_data(scraper_playwright=scraper_playwright)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"INSTRUMENT": str,
				"POSITIVE_MONTHS": int,
				"NEGATIVE_MONTHS": int,
				"GREATEST_RETURN": float,
				"LEAST_RETURN": float,
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
		"""Scrape consistency metrics for all configured instrument slugs.

		Parameters
		----------
		scraper_playwright : PlaywrightScraper
			Configured Playwright scraper instance.

		Returns
		-------
		pd.DataFrame
			DataFrame with consistency metrics for all instruments.
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
				list_ser.extend(
					self._extract_consistency(scraper_playwright, str_instrument)
				)
		return pd.DataFrame(list_ser)

	def _extract_consistency(
		self, scraper: PlaywrightScraper, str_instrument: str
	) -> list:
		"""Extract consistency metrics from the currently loaded instrument page.

		Parameters
		----------
		scraper : PlaywrightScraper
			Active PlaywrightScraper instance.
		str_instrument : str
			Normalised instrument identifier (uppercase slug from URL).

		Returns
		-------
		list
			Single-element list with a dict of consistency metric fields.
		"""
		span_positive_months = scraper.get_element(
			self._XPATH_SPAN_POSITIVE_MONTHS, selector_type="xpath"
		)
		span_negative_months = scraper.get_element(
			self._XPATH_SPAN_NEGATIVE_MONTHS, selector_type="xpath"
		)
		span_greatest_return = scraper.get_element(
			self._XPATH_SPAN_GREATEST_RETURN, selector_type="xpath"
		)
		span_least_return = scraper.get_element(
			self._XPATH_SPAN_LEAST_RETURN, selector_type="xpath"
		)
		raw_positive = span_positive_months["text"] if span_positive_months else None
		raw_negative = span_negative_months["text"] if span_negative_months else None
		raw_greatest = span_greatest_return["text"] if span_greatest_return else None
		raw_least = span_least_return["text"] if span_least_return else None
		return [
			{
				"INSTRUMENT": str_instrument,
				"POSITIVE_MONTHS": self.cls_num_handler.transform_to_float(
					raw_positive, int_precision=6
				)
				if raw_positive is not None
				else None,
				"NEGATIVE_MONTHS": self.cls_num_handler.transform_to_float(
					raw_negative, int_precision=6
				)
				if raw_negative is not None
				else None,
				"GREATEST_RETURN": self.cls_num_handler.transform_to_float(
					raw_greatest, int_precision=6
				)
				if raw_greatest is not None
				else None,
				"LEAST_RETURN": self.cls_num_handler.transform_to_float(
					raw_least, int_precision=6
				)
				if raw_least is not None
				else None,
			}
		]
