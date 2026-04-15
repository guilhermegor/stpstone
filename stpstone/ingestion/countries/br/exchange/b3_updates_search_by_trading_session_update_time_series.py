"""B3 Update Time Series for Search by Trading Session ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

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
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.html import HtmlHandler


class B3UpdatesSearchByTradingSessionUpdateTimeSeries(ABCIngestionOperations):
	"""Update Time Series for B3 Search by Trading Session."""

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
		self.cls_html_handler = HtmlHandler()
		self.date_ref = date_ref or self.cls_dates_br.add_working_days(
			self.cls_dates_current.curr_date(), -1
		)
		self.url = (
			"https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/"
			"market-data/historico/boletins-diarios/pesquisa-por-pregao/pesquisa-por-pregao/"
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_b3_updates_time_series_search_by_trading_session",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Whether to verify the SSL certificate, by default True.
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False.
		str_table_name : str
			The name of the table, by default
			"br_b3_updates_time_series_search_by_trading_session".

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
		html_root = self.parse_raw_file(resp_req)
		df_ = self.transform_data(html_root=html_root)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"ARQUIVOS_CLEARING_B3": str,
				"DATA_ATUALIZACAO": str,
			},
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
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Return a list of response objects.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Verify the SSL certificate, by default True.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
			A list of response objects.
		"""
		resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
		resp_req.raise_for_status()
		return resp_req

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
	) -> StringIO:
		"""Parse the raw file content.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The response object.

		Returns
		-------
		StringIO
			The parsed content.
		"""
		return self.cls_html_handler.lxml_parser(resp_req=resp_req)

	def transform_data(
		self,
		html_root: HtmlElement,
	) -> pd.DataFrame:
		"""Transform a list of response objects into a DataFrame.

		Parameters
		----------
		html_root : HtmlElement
			The root element of the HTML document.

		Returns
		-------
		pd.DataFrame
			The transformed DataFrame.

		Raises
		------
		ValueError
			If the table number is unexpected.
		"""
		fstr_xpath_file_name_tbl1: str = (
			'//*[@id="Form_8A68812D56B339A00156B35D5C276FEC"]'
			"/section/div/div/div/table[1]/tbody/tr[{}]/td[2]/span//text()"
		)
		fstr_xpath_file_name_tbl2: str = (
			'//*[@id="Form_8A68812D56B339A00156B35D5C276FEC"]'
			"/section/div/div/div/table[2]/tbody/tr[{}]/td[2]//text()"
		)
		fstr_xpath_update_timestamp: str = (
			'//*[@id="Form_8A68812D56B339A00156B35D5C276FEC"]'
			"/section/div/div/div/table[{}]/tbody/tr[{}]/td[4]//text()"
		)
		list_th: list[str] = ["ARQUIVOS_CLEARING_B3", "DATA_ATUALIZACAO"]
		list_data: list[str] = []

		for int_table in [1, 2]:
			for int_tr in range(1, 100):
				if int_table == 1:
					fstr_xpath_file_name = fstr_xpath_file_name_tbl1.format(int_tr)
				elif int_table == 2:
					fstr_xpath_file_name = fstr_xpath_file_name_tbl2.format(int_tr)
				else:
					raise ValueError(f"Invalid table number: {int_table}")

				str_span_file_name = " ".join(
					self.cls_html_handler.lxml_xpath(
						html_content=html_root,
						str_xpath=fstr_xpath_file_name,
					)
				)
				str_span_update_timestamp = " ".join(
					self.cls_html_handler.lxml_xpath(
						html_content=html_root,
						str_xpath=fstr_xpath_update_timestamp.format(int_table, int_tr),
					)
				)
				list_data.extend([str_span_file_name, str_span_update_timestamp])

		list_ser = self.cls_handling_dicts.pair_headers_with_data(
			list_headers=list_th,
			list_data=list_data,
		)

		df_ = pd.DataFrame(list_ser)
		df_ = df_.dropna(how="all")
		return df_
