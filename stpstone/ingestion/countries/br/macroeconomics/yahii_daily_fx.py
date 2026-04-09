"""Implementation of Yahii daily FX rates ingestion (USD/BRL and EUR/BRL)."""

from datetime import date
from logging import Logger
from typing import Any, Literal, Optional, Union

import backoff
from lxml.html import HtmlElement
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
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.str import StrHandler


URL_TEMPLATE_USD = "https://www.yahii.com.br/dolardiario{yy}.html"
URL_TEMPLATE_EUR = "https://www.yahii.com.br/eurodiario{yy}.html"
XPATH_TABLE_DATA = '//table[@cellspacing="3"]/tbody/tr//font'
COL_NAMES = ["DATA", "COMPRA", "VENDA"]
DICT_DTYPES: dict[str, Any] = {
	"DATA": "date",
	"COMPRA": float,
	"VENDA": float,
}
TABLE_NAME_USD = "br_yahii_daily_usdbrl"
TABLE_NAME_EUR = "br_yahii_daily_eurbrl"
DATE_FMT = "DD/MM/YYYY"


class YahiiDailyFX(ABCIngestionOperations):
	"""Ingestion class for Yahii daily FX rates (USD/BRL or EUR/BRL)."""

	def __init__(
		self,
		str_currency: Literal["usd", "eur"] = "usd",
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the ingestion class.

		Parameters
		----------
		str_currency : Literal["usd", "eur"], optional
			The currency pair to fetch, by default "usd".
		date_ref : Optional[date], optional
			The date of reference, by default None.
		logger : Optional[Logger], optional
			The logger, by default None.
		cls_db : Optional[Session], optional
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
		self.str_currency = str_currency
		year_yy = self.date_ref.strftime("%y")
		if str_currency == "eur":
			self.url = URL_TEMPLATE_EUR.format(yy=year_yy)
			self.table_name = TABLE_NAME_EUR
		else:
			self.url = URL_TEMPLATE_USD.format(yy=year_yy)
			self.table_name = TABLE_NAME_USD

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
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
			The timeout, by default (12.0, 21.0).
		bool_verify : bool, optional
			Whether to verify the SSL certificate, by default False.
		bool_insert_or_ignore : bool, optional
			Whether to insert or ignore the data, by default False.
		str_table_name : Optional[str], optional
			The name of the table, by default None (uses currency-specific name).

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		table_name = str_table_name or self.table_name
		resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
		list_td = self.parse_raw_file(resp_req=resp_req)
		df_ = self.transform_data(list_td=list_td)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes=DICT_DTYPES,
			str_fmt_dt=DATE_FMT,
			url=self.url,
		)
		if self.cls_db:
			self.insert_table_db(
				cls_db=self.cls_db,
				str_table_name=table_name,
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
		"""Return the HTTP response object.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
			The timeout, by default (12.0, 21.0).
		bool_verify : bool, optional
			Verify the SSL certificate, by default False.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
			Response object.
		"""
		resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
		resp_req.raise_for_status()
		return resp_req

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
	) -> list[str]:
		"""Parse the HTML response and extract table cell text via XPath.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The response object.

		Returns
		-------
		list[str]
			List of stripped text values from the HTML table cells.
		"""
		root: HtmlElement = HtmlHandler().lxml_parser(resp_req)
		elements = HtmlHandler().lxml_xpath(root, XPATH_TABLE_DATA)
		return [x.text.strip() for x in elements if x.text is not None]

	def transform_data(self, list_td: list[str]) -> pd.DataFrame:
		"""Transform the extracted text list into a DataFrame.

		Replaces commas with dots, filters to keep only date-like strings
		(DD/MM/YYYY) and numeric strings, then pairs with column headers.

		Parameters
		----------
		list_td : list[str]
			Raw text values extracted from the HTML table.

		Returns
		-------
		pd.DataFrame
			The transformed DataFrame.
		"""
		cls_str = StrHandler()
		filtered: list[str] = [
			x.replace(",", ".")
			for x in list_td
			if (cls_str.match_string_like(x, "*/*/*") and len(x) == 10)
			or "," in x
			or "." in x
			or (cls_str.match_string_like(x, "*/*") and len(x) == 9 and cls_str.has_no_letters(x))
		]
		list_ser = HandlingDicts().pair_headers_with_data(COL_NAMES, filtered)
		return pd.DataFrame(list_ser)
