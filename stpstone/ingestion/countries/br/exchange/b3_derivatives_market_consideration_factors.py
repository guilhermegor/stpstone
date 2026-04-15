"""B3 Derivatives Market Consideration Factors ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import pandas as pd
from requests import Session

from stpstone.ingestion.countries.br.exchange._b3_search_by_trading_session_base import (
	ABCB3SearchByTradingSession,
)


class B3DerivativesMarketConsiderationFactors(ABCB3SearchByTradingSession):
	"""B3 Derivatives Market - Consideration Factors."""

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
			url="https://www.b3.com.br/pesquisapregao/download?filelist=GL{}.zip",
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
		str_table_name: str = "br_b3_derivatives_market_consideration_factors",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		    The timeout value, by default (12.0, 21.0).
		bool_verify : bool
		    Whether to verify the data, by default True.
		bool_insert_or_ignore : bool
		    Whether to insert or ignore the data, by default False.
		str_fmt_dt : str
		    The format of the date, by default "YYYYMMDD".
		str_table_name : str
		    The name of the table, by default "br_b3_derivatives_market_consideration_factors".

		Returns
		-------
		Optional[pd.DataFrame]
		    The ingested data.
		"""
		return super().run(
			dict_dtypes={
				"DATA_BASE": str,
				"ATIVO_BASE_FPR": str,
				"DATA_FUTURA": str,
				"ATIVO_FPR": str,
				"TIPO_CENARIO_INDICADOR": str,
				"CODIGO_INTERNO": str,
				"VALOR_LIQUIDO_COMPENSADO_DOLAR": str,
				"VALOR_LIQUIDO_COMPENSADO_REAL": str,
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
		colspecs = [
			(0, 8),
			(8, 16),
			(16, 24),
			(24, 44),
			(44, 47),
			(47, 49),
			(49, 64),
			(64, 85),
		]

		column_names = [
			"DATA_BASE",
			"ATIVO_BASE_FPR",
			"DATA_FUTURA",
			"ATIVO_FPR",
			"TIPO_CENARIO_INDICADOR",
			"CODIGO_INTERNO",
			"VALOR_LIQUIDO_COMPENSADO_DOLAR",
			"VALOR_LIQUIDO_COMPENSADO_REAL",
		]

		df_ = pd.read_fwf(file, colspecs=colspecs, names=column_names, header=None)
		df_["FILE_NAME"] = file_name
		return df_
