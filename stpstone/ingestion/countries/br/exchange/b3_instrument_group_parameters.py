"""B3 Instrument Group Parameters ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import pandas as pd
from requests import Session

from stpstone.ingestion.countries.br.exchange._b3_search_by_trading_session_base import (
	ABCB3SearchByTradingSession,
)


class B3InstrumentGroupParameters(ABCB3SearchByTradingSession):
	"""B3 Instrument Group Parameters."""

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
			url="https://www.b3.com.br/pesquisapregao/download?filelist=PG{}.zip",
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_fmt_dt: str = "DD/MM/YYYY",
		str_table_name: str = "br_b3_instrument_group_parameters",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		    The timeout, by default (12.0, 21.0).
		bool_verify : bool
		    Whether to verify the data, by default True.
		bool_insert_or_ignore : bool
		    Whether to insert or ignore the data, by default False.
		str_fmt_dt : str
		    The format of the date, by default "DD/MM/YYYY".
		str_table_name : str
		    The name of the table, by default "br_b3_instrument_group_parameters".

		Returns
		-------
		Optional[pd.DataFrame]
		    The ingested data.
		"""
		return super().run(
			dict_dtypes={
				"TIPO": str,
				"ID_GRUPO_INSTRUMENTOS": str,
				"MERCADO": str,
				"NOME_CLASSIFICACAO": str,
				"PRAZO_MINIMO_ENCERRAMENTO": str,
				"LIMITE_LIQUIDEZ": str,
				"PRAZO_SUBCARTEIRA": str,
				"PRAZO_LIQUIDACAO": str,
				"PRAZO_LIQUIDACAO_NO_VENCIMENTO": str,
				"DATA_INICIAL_INTERVALO_VENCIMENTOS": str,
				"DATA_FINAL_INTERVALO_VENCIMENTOS": str,
				"INDICADOR_MODELO_GENERICO": str,
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
		    The name of the file.

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
				"TIPO",
				"ID_GRUPO_INSTRUMENTOS",
				"MERCADO",
				"NOME_CLASSIFICACAO",
				"PRAZO_MINIMO_ENCERRAMENTO",
				"LIMITE_LIQUIDEZ",
				"PRAZO_SUBCARTEIRA",
				"PRAZO_LIQUIDACAO",
				"PRAZO_LIQUIDACAO_NO_VENCIMENTO",
				"DATA_INICIAL_INTERVALO_VENCIMENTOS",
				"DATA_FINAL_INTERVALO_VENCIMENTOS",
				"INDICADOR_MODELO_GENERICO",
			],
		)
		df_["FILE_NAME"] = file_name
		return df_
