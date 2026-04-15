"""B3 Primitive Risk Factors ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import pandas as pd
from requests import Session

from stpstone.ingestion.countries.br.exchange._b3_search_by_trading_session_base import (
	ABCB3SearchByTradingSession,
)


class B3PrimitiveRiskFactors(ABCB3SearchByTradingSession):
	"""B3 Primitive Risk Factors (PRFs)."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Instantiate Constructor method.

		Parameters
		----------
		date_ref : Optional[date]
		    The date reference, by default None.
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
			url="https://www.b3.com.br/pesquisapregao/download?filelist=FP{}.zip",
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
		str_table_name: str = "br_b3_primitive_risk_factors",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
		    The timeout value, by default (12.0, 21.0).
		bool_verify : bool
		    If True, verify the data before inserting it into the database, by default True.
		bool_insert_or_ignore : bool
		    If True, insert the data into the database, by default False.
		str_fmt_dt : str
		    The date format, by default "YYYY-MM-DD".
		str_table_name : str
		    The table name, by default "br_b3_primitive_risk_factors".

		Returns
		-------
		Optional[pd.DataFrame]
		    The transformed DataFrame.
		"""
		return super().run(
			dict_dtypes={
				"TIPO_REGISTRO": str,
				"ID_FPR": str,
				"NOME_FPR": str,
				"FORMATO_VARIACAO": str,
				"ID_GRUPO_FPR": str,
				"ID_CAMARA_INDICADOR": str,
				"ID_INSTRUMENTO_INDICADOR": str,
				"ORIGEM_INSTRUMENTO_INDICADO": str,
				"BASE": str,
				"BASE_INTERPOLACAO": str,
				"CRITERIO_CAPITALIZACAO": str,
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
				"ID_FPR",
				"NOME_FPR",
				"FORMATO_VARIACAO",
				"ID_GRUPO_FPR",
				"ID_CAMARA_INDICADOR",
				"ID_INSTRUMENTO_INDICADOR",
				"ORIGEM_INSTRUMENTO_INDICADO",
				"BASE",
				"BASE_INTERPOLACAO",
				"CRITERIO_CAPITALIZACAO",
			],
		)
		df_["FILE_NAME"] = file_name
		return df_
