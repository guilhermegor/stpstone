"""Implementation of Yahii minimum wage history ingestion."""

from datetime import date
from logging import Logger
from typing import Any, Optional, Union

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


URL_YAHII_MIN_WAGE = "https://www.yahii.com.br/salariomi.html"
XPATH_TABLE_DATA = '//table[@cellspacing="3"]/tbody/tr//font'
COL_NAMES = ["DISPOSITIVO_LEGAL", "DATA", "VALOR"]
DICT_DTYPES: dict[str, Any] = {
	"DISPOSITIVO_LEGAL": str,
	"DATA": "date",
	"VALOR": float,
}
TABLE_NAME = "br_yahii_min_wage"
DATE_FMT = "DD.MM.YY"
CURRENCY_PREFIXES = [
	"NCz$ ",
	"NCr$ ",
	"CR$",
	"R$",
	"Cr$",
	"NCz$",
	"$000",
	"Cz$ ",
]
ERA_SUFFIXES = [
	" (Abono)",
	" (URV) \u2013 (Real)",
	" (Cruzeiro Real)",
	" (Cruzeiro)",
	" (Cruzado)",
	" (Cruzeiro Novo)",
	" (Cruzado Novo)",
	" (R\u00e9is)",
]
MIN_DATE = date(2000, 1, 1)


class YahiiMinWage(ABCIngestionOperations):
	"""Ingestion class for Yahii minimum wage history."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the ingestion class.

		Parameters
		----------
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
		self.url = URL_YAHII_MIN_WAGE

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = False,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = TABLE_NAME,
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
		str_table_name : str, optional
			The name of the table, by default "br_yahii_min_wage".

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
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

		Cleans currency prefixes from value cells, strips era suffixes from
		date cells, filters out revoked entries, and restricts to dates
		between 2000-01-01 and the reference date.

		Parameters
		----------
		list_td : list[str]
			Raw text values extracted from the HTML table.

		Returns
		-------
		pd.DataFrame
			The transformed DataFrame.
		"""
		cleaned: list[Any] = []
		for td in list_td:
			if "DECRETO" not in td and "$" in td:
				value_str = td
				for prefix in CURRENCY_PREFIXES:
					value_str = value_str.replace(prefix, "")
				value_str = value_str.replace(".", "").replace(",", ".").strip()
				cleaned.append(float(value_str))
			elif "DECRETO" not in td and len(td) >= 8 and "." in td:
				date_str = td
				for suffix in ERA_SUFFIXES:
					date_str = date_str.replace(suffix, "")
				cleaned.append(date_str)
			else:
				cleaned.append(td)
		list_ser = HandlingDicts().pair_headers_with_data(COL_NAMES, cleaned)
		df_ = pd.DataFrame(list_ser)
		df_ = df_[df_["DATA"] != "REVOGADA"]
		df_["DATA"] = [self.cls_dates_br.str_date_to_datetime(d, DATE_FMT) for d in df_["DATA"]]
		min_dt = pd.Timestamp(MIN_DATE, tz="UTC")
		max_dt = pd.Timestamp(self.date_ref, tz="UTC")
		df_ = df_[(df_["DATA"] >= min_dt) & (df_["DATA"] <= max_dt)]
		return df_
