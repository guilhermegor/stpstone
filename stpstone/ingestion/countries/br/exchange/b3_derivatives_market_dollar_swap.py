"""B3 Derivatives Market IDxUS Dollar Swap Mark-to-Market ingestion."""

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


class B3DerivativesMarketDollarSwap(ABCB3SearchByTradingSession):
	"""B3 Derivatives Market - IDxUS Dollar Swap - Mark-to-Market."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the B3DerivativesMarketDollarSwap class.

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
		super().__init__(
			date_ref=date_ref,
			logger=logger,
			cls_db=cls_db,
			url="https://www.b3.com.br/pesquisapregao/download?filelist=MM{}.ex_",
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
		str_table_name: str = "br_b3_derivatives_market_dollar_swap",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout value, by default (12.0, 21.0).
		bool_verify : bool
			Whether to verify the data, by default True.
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False.
		str_fmt_dt : str
			The format of the date, by default "YYYYMMDD".
		str_table_name : str
			The name of the table, by default "br_b3_derivatives_market_dollar_swap".

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
				"REFERENCIA": str,
				"CODIGO_PRODUTO": str,
				"SERIE": str,  # codespell:ignore
				"INDICADOR_AJUSTE_PERIODO": str,
				"DATA_VENCIMENTO": str,
				"PRAZO_DIAS_CORRIDOS": str,
				"SINAL_TAXA_MERCADO": str,
				"TAXA_MERCADO": str,
				"FATOR_DESCONTO": str,
				"VALOR_MERCADO": str,
				"FATOR_DESCONTO_TAXA_OPERACIONAL_BASICA": str,
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
		prefix: str = "b3_derivatives_market_dollar_swap_",
		file_name: str = "b3_derivatives_market_dollar_swap",
	) -> tuple[StringIO, str]:
		"""Parse the raw file content by executing Windows executable with Wine.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The response object.
		prefix : str
			The prefix of the file name, by default "b3_derivatives_market_dollar_swap_".
		file_name : str
			The name of the file, by default "b3_derivatives_market_dollar_swap".

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
			(19, 25),
			(25, 28),
			(28, 32),
			(32, 33),
			(33, 41),
			(41, 46),
			(46, 47),
			(47, 59),
			(59, 75),
			(75, 97),
			(97, 113),
		]

		column_names = [
			"ID_TRANSACAO",
			"COMPLEMENTO_TRANSACAO",
			"TIPO_REGISTRO",
			"DATA_GERACAO_ARQUIVO",
			"REFERENCIA",
			"CODIGO_PRODUTO",
			"SERIE",  # codespell:ignore
			"INDICADOR_AJUSTE_PERIODO",
			"DATA_VENCIMENTO",
			"PRAZO_DIAS_CORRIDOS",
			"SINAL_TAXA_MERCADO",
			"TAXA_MERCADO",
			"FATOR_DESCONTO",
			"VALOR_MERCADO",
			"FATOR_DESCONTO_TAXA_OPERACIONAL_BASICA",
		]

		df_ = pd.read_fwf(file, colspecs=colspecs, names=column_names, header=None)
		df_["FILE_NAME"] = file_name
		return df_
