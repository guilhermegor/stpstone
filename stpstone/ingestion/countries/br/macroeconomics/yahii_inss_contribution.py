"""Implementation of Yahii INSS contribution brackets ingestion."""

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
from stpstone.utils.parsers.numbers import NumHandler
from stpstone.utils.parsers.str import StrHandler


URL_YAHII_INSS = "https://www.yahii.com.br/inss.html"
XPATH_TABLE_DATA = '//table[@cellspacing="3"]/tbody/tr//font'
COL_NAMES = ["SALARIO_CONTRIBUICAO", "ALIQUOTA_RECOLHIMENTO_INSS"]
DICT_DTYPES: dict[str, Any] = {
	"SALARIO_CONTRIBUICAO": str,
	"ALIQUOTA_RECOLHIMENTO_INSS": float,
	"SALARIO_INF": float,
	"SALARIO_SUP": float,
}
TABLE_NAME = "br_yahii_inss_contribution"
FILLNA_INF = 0.0
FILLNA_SUP = 1_000_000_000


class YahiiINSSContribution(ABCIngestionOperations):
	"""Ingestion class for Yahii INSS contribution brackets."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
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
		self.url = URL_YAHII_INSS

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
		bool_verify : bool
			Whether to verify the SSL certificate, by default False.
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False.
		str_table_name : str
			The name of the table, by default "br_yahii_inss_contribution".

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
			str_fmt_dt="DD/MM/YYYY",
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
		bool_verify : bool
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

		Cleans whitespace and percentage symbols, parses salary bracket
		ranges into SALARIO_INF and SALARIO_SUP columns, and converts
		percentage strings to decimal floats.

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
		cls_num = NumHandler()
		cleaned: list[Any] = [
			x.replace("\r\n                 ", "")
			.strip()
			.replace("%", "")
			.replace(",01,01", "")
			.replace("Ate  4.190,84 ", "De 4.190,84 ")
			.strip()
			for x in list_td
		]
		cleaned = [
			float(x.replace(",", ".")) / 100.0 if cls_num.is_numeric(x.replace(",", ".")) else x
			for x in cleaned
		]
		list_ser = HandlingDicts().pair_headers_with_data(COL_NAMES, cleaned)
		df_ = pd.DataFrame(list_ser)
		df_["SALARIO_INF"] = [
			cls_str.get_between(cls_str.remove_diacritics(x.lower()), "de", "ate").strip()
			if "de " in x.lower() and "acima de " not in x.lower()
			else cls_str.get_after(
				cls_str.remove_diacritics(x.lower().strip()), "acima de "
			).strip()
			if "acima de " in cls_str.remove_diacritics(x.lower().strip())
			else 0.0
			for x in df_["SALARIO_CONTRIBUICAO"]
		]
		df_["SALARIO_SUP"] = [
			cls_str.get_after(cls_str.remove_diacritics(x.lower()), "ate ").strip()
			if "ate " in cls_str.remove_diacritics(x.lower())
			else 0.0
			for x in df_["SALARIO_CONTRIBUICAO"]
		]
		df_["SALARIO_INF"] = pd.to_numeric(
			df_["SALARIO_INF"].str.replace(".", "").str.replace(",", "."),
			errors="coerce",
		)
		df_["SALARIO_SUP"] = pd.to_numeric(
			df_["SALARIO_SUP"].str.replace(".", "").str.replace(",", "."),
			errors="coerce",
		)
		df_["SALARIO_INF"] = df_["SALARIO_INF"].fillna(FILLNA_INF)
		df_["SALARIO_SUP"] = df_["SALARIO_SUP"].fillna(FILLNA_SUP)
		return df_
