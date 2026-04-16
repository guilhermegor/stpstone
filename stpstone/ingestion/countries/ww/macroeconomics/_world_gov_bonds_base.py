"""Base class for World Government Bonds sovereign data ingestion."""

from datetime import date
from logging import Logger
from typing import Any, Optional, Union

import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
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
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.numbers import NumHandler
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


_RATINGS_AGENCIES_LIST: list[str] = [
	"AAA",
	"AA+",
	"AA",
	"AA-",
	"A+",
	"A",
	"A-",
	"BBB+",
	"BBB",
	"BBB-",
	"BB+",
	"BB",
	"BB-",
	"B+",
	"B",
	"B-",
	"CCC+",
	"CCC",
	"CCC-",
	"CC",
	"C",
	"D",
	"SD",
	"NR",
	"Aaa",
	"Aa1",
	"Aa2",
	"Aa3",
	"A1",
	"A2",
	"A3",
	"Baa1",
	"Baa2",
	"Baa3",
	"Ba1",
	"Ba2",
	"Ba3",
	"B1",
	"B2",
	"B3",
	"Caa1",
	"Caa2",
	"Caa3",
	"Ca",
	"WR",
	"RD",
]


class _WorldGovBondsBase(ABCIngestionOperations):
	"""Base class with shared Playwright scraping logic for World Government Bonds sources."""

	_BASE_HOST = "https://www.worldgovernmentbonds.com/"
	_PATH: str = ""
	_TABLE_NAME: str = ""
	_DTYPES: dict[str, Any] = {}
	_XPATH: str = ""
	_TRIM_LAST: bool = False

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the base ingestion class.

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
		self.url = f"{self._BASE_HOST}{self._PATH}"

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = False,
		bool_insert_or_ignore: bool = False,
		str_table_name: Optional[str] = None,
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			Unused; kept for interface consistency, by default (12.0, 21.0).
		bool_verify : bool
			Unused; kept for interface consistency, by default False.
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False.
		str_table_name : Optional[str]
			The name of the table, by default the class-level _TABLE_NAME.

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		str_table_name = str_table_name or self._TABLE_NAME
		df_ = self.transform_data(resp_req=None)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes=self._DTYPES,
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

	def get_response(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = False,
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Not used directly; Playwright navigation occurs inside transform_data.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			Unused; kept for interface consistency, by default (12.0, 21.0).
		bool_verify : bool
			Unused; kept for interface consistency, by default False.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
			None (Playwright is managed inside transform_data).
		"""
		return None  # type: ignore[return-value]

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Pass through; scraping is handled entirely in transform_data.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			Ignored.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
			The same value, unchanged.
		"""
		return resp_req

	def transform_data(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
	) -> pd.DataFrame:
		"""Navigate with Playwright, scrape the target XPath, and build a DataFrame.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			Ignored; Playwright manages the browser session internally.

		Returns
		-------
		pd.DataFrame
			The scraped DataFrame.
		"""
		list_td: list[str] = []
		scraper = PlaywrightScraper(bool_headless=True, int_default_timeout=100_000)
		with scraper.launch():
			if scraper.navigate(self.url):
				list_td = scraper.get_list_data(self._XPATH, selector_type="xpath")
		list_td = self._treat_list_td(list_td)
		if self._TRIM_LAST:
			list_td = list_td[:-1]
		list_ser = HandlingDicts().pair_headers_with_data(list(self._DTYPES.keys()), list_td)
		return pd.DataFrame(list_ser)

	def _treat_list_td(self, list_td: list[str]) -> list[Any]:
		"""Insert 'N/A' placeholders for countries missing a credit rating.

		The rule: after a country name, if the next item is numeric (e.g. '9.88%'),
		insert 'N/A' between them to represent a missing rating.

		Parameters
		----------
		list_td : list[str]
			Raw list of table data cell strings.

		Returns
		-------
		list[Any]
			Processed list with 'N/A' inserted where ratings are absent.
		"""
		list_: list[Any] = []
		i = 0
		n = len(list_td)
		while i < n:
			item_curr = list_td[i]
			item_processed = NumHandler().transform_to_float(list_td[i], int_precision=6)
			list_.append(item_processed)
			bool_country_name = (
				isinstance(item_curr, str)
				and not any(c.isdigit() for c in item_curr)
				and not item_curr.endswith("%")
				and item_curr != "N/A"
				and item_curr not in _RATINGS_AGENCIES_LIST
			)
			if bool_country_name and i + 1 < n:
				next_item = list_td[i + 1]
				bool_next_numeric = (
					isinstance(next_item, str) and next_item.endswith("%")
				) or isinstance(
					NumHandler().transform_to_float(next_item, int_precision=6),
					(int, float),
				)
				if bool_next_numeric:
					list_.append("N/A")
			i += 1
		return list_
