"""B3 Derivatives Market Swap Market Rates ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
from requests import Response, Session
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.countries.br.exchange._b3_search_by_trading_session_base import (
	ABCB3SearchByTradingSession,
)


class B3DerivativesMarketSwapMarketRates(ABCB3SearchByTradingSession):
	"""B3 Derivatives Market - Swap Market Rates."""

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
		super().__init__(
			date_ref=date_ref,
			logger=logger,
			cls_db=cls_db,
			url="https://www.b3.com.br/pesquisapregao/download?filelist=TS{}.ex_",
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_fmt_dt: str = "YYYYMMDD",
		str_table_name: str = "br_b3_derivatives_market_swap_market_rates",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		    The timeout for the ingestion process, by default (12.0, 21.0).
		bool_verify : bool, optional
		    Whether to verify the ingestion process, by default True.
		bool_insert_or_ignore : bool, optional
		    Whether to insert or ignore the ingestion process, by default False.
		str_fmt_dt : str, optional
		    The format of the date, by default "YYYYMMDD".
		str_table_name : str, optional
		    The name of the table, by default "br_b3_derivatives_market_swap_market_rates".

		Returns
		-------
		Optional[pd.DataFrame]
		    The ingested data.
		"""
		return super().run(
			dict_dtypes={
				"ID_TRANSACAO": str,
				"COMPLEMENTO_TRANSACAO": str,
				"TIPO_REGISTRO": str,
				"DATA_GERACAO_ARQUIVO": str,
				"CODIGO_CURVAS_A_TERMO": str,
				"CODIGO_TAXA": str,
				"DESCRICAO_TAXA": str,
				"NUMERO_DIAS_CORRIDOS_TAXA_JURO": str,
				"NUMERO_SAQUES_TAXA_JURO": str,
				"SINAL_TAXA_TEORICA": str,
				"TAXA_TEORICA": str,
				"CARACTERISTICA_VERTICE": str,
				"CODIGO_VERTICE": str,
				"FILE_NAME": "category",
			},
			timeout=timeout,
			bool_verify=bool_verify,
			bool_insert_or_ignore=bool_insert_or_ignore,
			str_fmt_dt=str_fmt_dt,
			str_table_name=str_table_name,
		)

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
		prefix: str = "b3_derivatives_market_swap_market_rates_",
		file_name: str = "b3_derivatives_market_swap_market_rates",
	) -> tuple[StringIO, str]:
		"""Parse the raw file content by executing Windows executable with Wine.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
		    The response object.
		prefix : str, optional
		    The prefix for the temporary directory, by default
		    "b3_derivatives_market_swap_market_rates_".
		file_name : str, optional
		    The name of the file, by default "b3_derivatives_market_swap_market_rates".

		Returns
		-------
		tuple[StringIO, str]
		    The parsed content and file name.
		"""
		return self.parse_raw_ex_file(
			resp_req=resp_req,
			prefix=prefix,
			file_name=file_name,
		)

	def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
		"""Transform file content into a DataFrame.

		Parameters
		----------
		file : StringIO
		    The file content.
		file_name : str
		    The name of the file.

		Returns
		-------
		pd.DataFrame
		    The transformed DataFrame.
		"""
		colspecs = [
			(0, 6),
			(6, 9),
			(9, 11),
			(11, 19),
			(19, 21),
			(21, 26),
			(26, 41),
			(41, 46),
			(46, 51),
			(51, 52),
			(52, 66),
			(66, 67),
			(67, 72),
		]

		column_names = [
			"ID_TRANSACAO",
			"COMPLEMENTO_TRANSACAO",
			"TIPO_REGISTRO",
			"DATA_GERACAO_ARQUIVO",
			"CODIGO_CURVAS_A_TERMO",
			"CODIGO_TAXA",
			"DESCRICAO_TAXA",
			"NUMERO_DIAS_CORRIDOS_TAXA_JURO",
			"NUMERO_SAQUES_TAXA_JURO",
			"SINAL_TAXA_TEORICA",
			"TAXA_TEORICA",
			"CARACTERISTICA_VERTICE",
			"CODIGO_VERTICE",
		]

		df_ = pd.read_fwf(file, colspecs=colspecs, names=column_names, header=None)
		df_["FILE_NAME"] = file_name
		return df_
