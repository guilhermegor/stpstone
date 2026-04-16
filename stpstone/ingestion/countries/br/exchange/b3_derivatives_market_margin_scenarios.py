"""B3 Derivatives Market Margin Scenarios ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import pandas as pd
from requests import Session

from stpstone.ingestion.countries.br.exchange._b3_search_by_trading_session_base import (
	ABCB3SearchByTradingSession,
)


class B3DerivativesMarketMarginScenarios(ABCB3SearchByTradingSession):
	"""B3 Derivatives Market Margin Scenarios.

	Margin scenarios for liquid assets.
	"""

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
		super().__init__(
			date_ref=date_ref,
			logger=logger,
			cls_db=cls_db,
			url="https://www.b3.com.br/pesquisapregao/download?filelist=CN{}.zip",
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_fmt_dt: str = "YYYY-MM-DD",
		str_table_name: str = "br_b3_derivatives_market_margin_scenarios",
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
			The format of the date, by default "YYYY-MM-DD".
		str_table_name : str
			The name of the table, by default "br_b3_derivatives_market_margin_scenarios".

		Returns
		-------
		Optional[pd.DataFrame]
			The ingested data.
		"""
		return super().run(
			dict_dtypes={
				"TIPO_REGISTRO": str,
				"FATOR_PRIMITIVO_RISCO": str,
				"VERTICE_CODIGO_DISTRIBUICAO": str,
				"ID_CENARIO": str,
				"VALOR": float,
				"FATOR_CENARIO": str,
				"CHOQUE_CENARIO_POSITIVO": str,
				"CHOQUE_CENARIO_NEGATIVO": str,
				"TIPO_CHOQUE": str,
				"FILE_NAME": "category",
			},
			timeout=timeout,
			bool_verify=bool_verify,
			bool_insert_or_ignore=bool_insert_or_ignore,
			str_fmt_dt=str_fmt_dt,
			str_table_name=str_table_name,
		)

	def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
		"""Transform file content into a DataFrame.

		Parameters
		----------
		file : StringIO
			The file content.
		file_name : str
			The file name.

		Returns
		-------
		pd.DataFrame
			The transformed DataFrame.
		"""
		df_ = pd.read_csv(
			file,
			sep=";",
			skiprows=1,
			names=[
				"TIPO_REGISTRO",
				"FATOR_PRIMITIVO_RISCO",
				"VERTICE_CODIGO_DISTRIBUICAO",
				"ID_CENARIO",
				"VALOR",
				"FATOR_CENARIO",
				"CHOQUE_CENARIO_POSITIVO",
				"CHOQUE_CENARIO_NEGATIVO",
				"TIPO_CHOQUE",
			],
		)
		df_["FILE_NAME"] = file_name
		return df_
