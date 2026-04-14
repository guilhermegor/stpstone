"""B3 Mapping Standardized Instrument Groups ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import pandas as pd
from requests import Session

from stpstone.ingestion.countries.br.exchange._b3_search_by_trading_session_base import (
	ABCB3SearchByTradingSession,
)


class B3MappingStandardizedInstrumentGroups(ABCB3SearchByTradingSession):
	"""B3 Mapping Standardized Instrument Groups."""

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
			url="https://www.b3.com.br/pesquisapregao/download?filelist=MP{}.zip",
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
		str_table_name: str = "br_b3_mapping_standardized_instrument_groups",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		    The timeout value, by default (12.0, 21.0).
		bool_verify : bool, optional
		    Whether to verify the data, by default True.
		bool_insert_or_ignore : bool, optional
		    Whether to insert or ignore the data, by default False.
		str_fmt_dt : str, optional
		    The date format, by default "DD/MM/YYYY".
		str_table_name : str, optional
		    The table name, by default "br_b3_mapping_standardized_instrument_groups".

		Returns
		-------
		Optional[pd.DataFrame]
		    The DataFrame with the data.
		"""
		return super().run(
			dict_dtypes={
				"TIPO_REGISTRO": str,
				"ID_GRUPO_INSTRUMENTOS": int,
				"ID_FORMULA_RISCO": int,
				"ID_FPR": int,
				"ID_QUALIFICADOR": int,
				"DESCRICAO_QUALIFICADOR": "category",
				"DATA_INICIAL_INTERVALO_VENCIMENTOS": "date",
				"DATA_FINAL_INTERVALO_VENCIMENTOS": "date",
				"INDICADOR_FPR_INDEPENDENTE": int,
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
				"ID_GRUPO_INSTRUMENTOS",
				"ID_FORMULA_RISCO",
				"ID_FPR",
				"ID_QUALIFICADOR",
				"DESCRICAO_QUALIFICADOR",
				"DATA_INICIAL_INTERVALO_VENCIMENTOS",
				"DATA_FINAL_INTERVALO_VENCIMENTOS",
				"INDICADOR_FPR_INDEPENDENTE",
			],
		)
		df_["FILE_NAME"] = file_name
		return df_
