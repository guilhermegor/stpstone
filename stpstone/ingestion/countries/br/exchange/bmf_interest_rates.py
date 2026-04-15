"""BMF Interest Rates.

This module provides an implementation of the BMF Interest Rates ingestion class.
It inherits from the ABCIngestionOperations class and implements the run method.
"""

from datetime import date
from io import StringIO
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
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.html import HtmlHandler


class BMFInterestRates(ABCIngestionOperations):
	"""BMF Interest Rates Ingestion Class."""

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
		self.cls_dict_handler = HandlingDicts()
		self.date_ref = date_ref or self.cls_dates_br.add_working_days(
			self.cls_dates_current.curr_date(), -1
		)
		self.url = (
			"https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/TxRef1.asp?"
			+ "Data={}".format(self.date_ref.strftime("%d/%m/%Y"))
			+ "&Data1={}&slcTaxa=TODOS".format(self.date_ref.strftime("%Y%m%d"))
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_b3_interest_rates",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
		    The timeout, by default (12.0, 21.0)
		bool_verify : bool
		    Whether to verify the SSL certificate, by default True
		bool_insert_or_ignore : bool
		    Whether to insert or ignore the data, by default False
		str_table_name : str
		    The name of the table, by default "br_b3_interest_rates"

		Returns
		-------
		Optional[pd.DataFrame]
		    The transformed DataFrame.
		"""
		resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
		list_ser = self.parse_raw_file(resp_req)
		df_ = self.transform_data(list_ser=list_ser)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"DIAS_CORRIDOS": "int",
				"DI_PRE_252": "float",
				"DI_PRE_360": "float",
				"SELIC_PRE_252": "float",
				"DI_TR_252": "float",
				"DI_TR_360": "float",
				"DOLAR_PRE_252": "float",
				"DOLAR_PRE_360": "float",
				"REAL_EURO_PRECO": "float",
				"DI_EURO_360": "float",
				"TBF_PRE_252": "float",
				"TBF_PRE_360": "float",
				"TR_PRE_252": "float",
				"TR_PRE_360": "float",
				"DI_DOLAR_360": "float",
				"CUPOM_CAMBIAL_OC1_360": "float",
				"CUPOM_LIMPO_360": "float",
				"REAL_DOLAR_PRECO": "float",
				"IBRX_50": "float",
				"IBOVESPA": "float",
				"DI_IGP_M_252": "float",
				"DI_IPCA_252": "float",
				"AJUSTE_PRE_252": "float",
				"AJUSTE_PRE_360": "float",
				"AJUSTE_CUPOM_360": "float",
				"REAL_IENE_PRECO": "float",
				"SPEAD_LIBOR_EURO_DOLAR_TAXA": "float",
				"LIBOR_360": "float",
			},
			str_fmt_dt="YYYY-MM-DD",
			url=self.url,
			list_cols_drop_dupl=["DIAS_CORRIDOS"],
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
		    The timeout, by default (12.0, 21.0)
		bool_verify : bool
		    Verify the SSL certificate, by default True

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
		    A list of response objects.
		"""
		resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
		resp_req.raise_for_status()
		return resp_req

	def parse_raw_file(
		self, resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
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
		list_ser: list[dict[str, float]] = []
		xpath_td: str = """
        //table[starts-with(@id, "tb_principal")]//td[contains(@class, "tabelaConteudo")]
        """

		html_root = self.cls_html_handler.lxml_parser(resp_req=resp_req)
		list_td = self.cls_html_handler.lxml_xpath(html_content=html_root, str_xpath=xpath_td)
		list_td = [float(x.text.strip().replace(",", ".")) for x in list_td]
		list_ser = self._pair_headers_with_data(list_data=list_td)
		return list_ser

	def _pair_headers_with_data(self, list_data: list[float]) -> list[dict[str, float]]:
		"""Pair extracted table data with their corresponding column headers.

		Parameters
		----------
		list_data : list[float]
			Flat list of numeric values scraped from the interest rate table.

		Returns
		-------
		list[dict[str, float]]
			List of dicts mapping column names to their float values.
		"""
		list_th: list[str] = [
			"DIAS_CORRIDOS",
			"DI_PRE_252",
			"DI_PRE_360",
			"SELIC_PRE_252",
			"DI_TR_252",
			"DI_TR_360",
			"DOLAR_PRE_252",
			"DOLAR_PRE_360",
			"REAL_EURO_PRECO",
		]
		list_td = list_data[0 : 1 * 9 * (281 - 3)]
		list_ser = self.cls_dict_handler.pair_headers_with_data(
			list_headers=list_th, list_data=list_td
		)
		df_1 = pd.DataFrame(list_ser)

		list_th: list[str] = [
			"DIAS_CORRIDOS",
			"DI_EURO_360",
			"TBF_PRE_252",
			"TBF_PRE_360",
			"TR_PRE_252",
			"TR_PRE_360",
			"DI_DOLAR_360",
			"CUPOM_CAMBIAL_OC1_360",
			"CUPOM_LIMPO_360",
		]
		list_td = list_data[1 * 9 * (281 - 3) : 2 * 9 * (281 - 3)]
		list_ser = self.cls_dict_handler.pair_headers_with_data(
			list_headers=list_th, list_data=list_td
		)
		df_2 = pd.DataFrame(list_ser)

		list_th: list[str] = [
			"DIAS_CORRIDOS",
			"REAL_DOLAR_PRECO",
			"IBRX_50",
			"IBOVESPA",
			"DI_IGP_M_252",
			"DI_IPCA_252",
			"AJUSTE_PRE_252",
			"AJUSTE_PRE_360",
			"AJUSTE_CUPOM_360",
		]
		list_td = list_data[2 * 9 * (281 - 3) : 3 * 9 * (281 - 3)]
		list_ser = self.cls_dict_handler.pair_headers_with_data(
			list_headers=list_th, list_data=list_td
		)
		df_3 = pd.DataFrame(list_ser)

		list_th: list[str] = [
			"DIAS_CORRIDOS",
			"REAL_IENE_PRECO",
			"SPEAD_LIBOR_EURO_DOLAR_TAXA",
			"LIBOR_360",
		]
		list_td = list_data[3 * 9 * (281 - 3) :]
		list_ser = self.cls_dict_handler.pair_headers_with_data(
			list_headers=list_th, list_data=list_td
		)
		df_4 = pd.DataFrame(list_ser)

		df_ = pd.merge(df_1, df_2, on="DIAS_CORRIDOS", how="left", suffixes=("", "_"))
		df_ = pd.merge(df_, df_3, on="DIAS_CORRIDOS", how="left", suffixes=("", "_"))
		df_ = pd.merge(df_, df_4, on="DIAS_CORRIDOS", how="left", suffixes=("", "_"))
		list_ser = df_.to_dict("records")
		return list_ser

	def transform_data(self, list_ser: list[dict[str, float]]) -> pd.DataFrame:
		"""Transform a list of response objects into a DataFrame.

		Parameters
		----------
		list_ser : list[dict[str, float]]
		    The list of dictionaries to transform.

		Returns
		-------
		pd.DataFrame
		    The transformed DataFrame.
		"""
		return pd.DataFrame(list_ser)
