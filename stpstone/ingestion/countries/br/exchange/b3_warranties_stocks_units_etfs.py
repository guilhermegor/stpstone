"""B3 Warranties - Stocks, Units and ETFs."""

from datetime import date
from io import BytesIO, StringIO
from logging import Logger
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
from stpstone.utils.parsers.html import HtmlHandler


class B3WarrantiesStocksUnitsETFs(ABCIngestionOperations):
	"""B3 Warranties - Stocks, Units and ETFs."""

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
		self.url_host = "https://www.b3.com.br/"
		self.url = (
			"https://www.b3.com.br/pt_br/produtos-e-servicos/compensacao-e-liquidacao/"
			"clearing/administracao-de-riscos/garantias/limites-de-renda-variavel-e-fixa/"
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_b3_warranties_stocks_units_etfs",
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
			The name of the table, by default "br_b3_warranties_stocks_units_etfs".

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
		resp_req, url_href = self._get_href(
			resp_req=resp_req, timeout=timeout, bool_verify=bool_verify
		)
		list_tup_files = self.parse_raw_file(resp_req=resp_req)
		df_ = self.transform_data(list_tup_files=list_tup_files, url_href=url_href)
		df_ = super().standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"CODIGO": str,
				"ISIN": str,
				"LIMITE_QUANTIDADE": int,
				"NOME_PLANILHA": str,
				"NOME_PASTA_TRABALHO": str,
				"URL_HREF": str,
			},
			str_fmt_dt="D/M/YYYY",
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

	def _get_href(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
	) -> tuple[Union[Response, PlaywrightPage, SeleniumWebDriver], str]:
		"""Return href object, with stocks, units and ETFs accepted as warranties.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The response object.
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Verify the SSL certificate, by default True.

		Returns
		-------
		tuple[Union[Response, PlaywrightPage, SeleniumWebDriver], str]
			The href object file and the URL.
		"""
		xpath_href: str = '//*[@id="panel1a"]/ul/li[2]/a'

		html_root = self.cls_html_handler.lxml_parser(resp_req=resp_req)

		href = self.cls_html_handler.lxml_xpath(html_content=html_root, str_xpath=xpath_href)[
			0
		].get("href")
		url_href = href.replace("../../../../../../../", self.url_host)
		resp_req = requests.get(url_href, timeout=timeout, verify=bool_verify)
		resp_req.raise_for_status()
		return resp_req, url_href

	def parse_raw_file(
		self, resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
	) -> list[tuple[Union[StringIO, BytesIO], str]]:
		"""Parse the raw file content.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The response object.

		Returns
		-------
		list[tuple[Union[StringIO, BytesIO], str]]
			The parsed content.
		"""
		return self.cls_dir_files_management.recursive_unzip_in_memory(BytesIO(resp_req.content))

	def transform_data(
		self, list_tup_files: list[tuple[Union[StringIO, BytesIO], str]], url_href: str
	) -> pd.DataFrame:
		"""Transform a list of response objects into a DataFrame.

		Parameters
		----------
		list_tup_files : list[tuple[Union[StringIO, BytesIO], str]]
			The parsed content.
		url_href : str
			The URL.

		Returns
		-------
		pd.DataFrame
			The transformed DataFrame.
		"""
		list_ser: list[dict[str, Union[str, int, float]]] = []

		for file_content, filename in list_tup_files:
			excel_file = pd.ExcelFile(file_content)
			list_sheets = excel_file.sheet_names
			for sheet_name in list_sheets:
				df_ = excel_file.parse(sheet_name=sheet_name)
				df_ = df_.rename(
					columns={
						"Código": "CODIGO",
						"Limite (quantidade)": "LIMITE_QUANTIDADE",  # codespell:ignore
					}
				)
				df_["NOME_PLANILHA"] = sheet_name
				df_["NOME_PASTA_TRABALHO"] = filename
				df_["URL_HREF"] = url_href
				list_ser.extend(df_.to_dict(orient="records"))

		return pd.DataFrame(list_ser)
