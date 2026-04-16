"""Mais Retorno Available Funds Listing."""

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
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


class MaisRetornoAvlFunds(ABCIngestionOperations):
	"""Mais Retorno Available Funds paginated listing."""

	_BASE_URL = "https://maisretorno.com/lista-fundos-investimentos/page/{}"
	_XPATH_P_CNPJ = (
		'//ul[@style="padding:0"]/li[{}]'
		'//p[@class="MuiTypography-root MuiTypography-body2 css-oc8vpl"]'
	)
	_XPATH_HREF_FUND = (
		'//ul[@style="padding:0"]/li[{}]'
		'//h2[@class="MuiTypography-root MuiTypography-body1 css-fuk7hj"]/a'
	)
	_XPATH_A_FUND_NAME = '//ul[@style="padding:0"]/li[{}]//a[@class="css-g90fw7"]'
	_XPATH_A_CATEGORY = '//ul[@style="padding:0"]/li[{}]//a[@class="css-3xq1wq"]'
	_XPATH_STATUS_FUND = (
		'//ul[@style="padding:0"]/li[{}]'
		'//span[@class="MuiChip-label MuiChip-labelMedium css-4qalxv"'
		' or @class="MuiChip-root MuiChip-outlined MuiChip-sizeMedium'
		' MuiChip-colorDefault MuiChip-outlinedDefault css-6ot3hj"]'
	)

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
		list_slugs: Optional[list] = None,
		bool_headless: bool = True,
		int_wait_load_seconds: int = 60,
	) -> None:
		"""Initialize the Mais Retorno Available Funds ingestion class.

		Parameters
		----------
		date_ref : Optional[date]
			The date of reference, by default None.
		logger : Optional[Logger]
			The logger, by default None.
		cls_db : Optional[Session]
			The database session, by default None.
		list_slugs : Optional[list]
			List of page number slugs to iterate, by default None.
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
		self.date_ref = date_ref or self.cls_dates_br.add_working_days(
			self.cls_dates_current.curr_date(), -1
		)
		self.list_slugs = list_slugs or list(range(1, 11))
		self.bool_headless = bool_headless
		self.int_wait_load_seconds = int_wait_load_seconds
		self.url = self._BASE_URL

	def run(
		self,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_mais_retorno_available_funds",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False.
		str_table_name : str
			The name of the table, by default 'br_mais_retorno_available_funds'.

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
				"CNPJ": str,
				"URL_FUND": str,
				"FUND_NAME": str,
				"CATEGORY": str,
				"STATUS_FUND": str,
				"PAGE_POSITION": int,
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
		"""Scrape paginated fund listings across all configured page slugs.

		Parameters
		----------
		scraper_playwright : PlaywrightScraper
			Configured Playwright scraper instance.

		Returns
		-------
		pd.DataFrame
			DataFrame with fund listing data across all pages.
		"""
		list_ser = []
		with scraper_playwright.launch():
			for slug in self.list_slugs:
				page_url = self._BASE_URL.format(slug)
				if not scraper_playwright.navigate(page_url):
					self.cls_create_log.log_message(
						self.logger,
						f"Failed to navigate to page {slug}: {page_url}",
						"warning",
					)
					continue
				i = 1
				while True:
					if not scraper_playwright.selector_exists(
						self._XPATH_P_CNPJ.format(i),
						selector_type="xpath",
						timeout=self.int_wait_load_seconds * 1_000,
					):
						break
					list_ser.append(self._extract_row(i, scraper_playwright))
					i += 1
		return pd.DataFrame(list_ser)

	def _extract_row(self, i: int, scraper: PlaywrightScraper) -> dict:
		"""Extract a single fund row from the current page.

		Parameters
		----------
		i : int
			One-based position of the list item on the page.
		scraper : PlaywrightScraper
			Active PlaywrightScraper instance.

		Returns
		-------
		dict
			Mapping of column names to scraped values.
		"""
		p_cnpj = scraper.get_element(self._XPATH_P_CNPJ.format(i), selector_type="xpath")
		href_fund = scraper.get_element_attrb(
			self._XPATH_HREF_FUND.format(i),
			str_attribute="href",
			selector_type="xpath",
		)
		a_fund_name = scraper.get_element(self._XPATH_A_FUND_NAME.format(i), selector_type="xpath")
		a_category = scraper.get_element(self._XPATH_A_CATEGORY.format(i), selector_type="xpath")
		status_fund = scraper.get_element(self._XPATH_STATUS_FUND.format(i), selector_type="xpath")
		return {
			"CNPJ": p_cnpj.get("text", None) if p_cnpj else None,
			"URL_FUND": "https://maisretorno.com/" + href_fund if href_fund is not None else None,
			"FUND_NAME": a_fund_name.get("text", None) if a_fund_name else None,
			"CATEGORY": a_category.get("text", None) if a_category else None,
			"STATUS_FUND": status_fund.get("text", None) if status_fund else None,
			"PAGE_POSITION": i,
		}
