"""B3 Daily Liquidity Limits ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import pandas as pd
from requests import Session

from stpstone.ingestion.countries.br.exchange._b3_search_by_trading_session_base import (
	ABCB3SearchByTradingSession,
)


class B3DailyLiquidityLimits(ABCB3SearchByTradingSession):
	"""B3 Daily Liquidity Limits."""

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
			url="https://www.b3.com.br/pesquisapregao/download?filelist=LD{}.zip",
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
		str_table_name: str = "br_b3_daily_liquidity_limits",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Whether to verify the data, by default True.
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False.
		str_fmt_dt : str
			The format of the date, by default "YYYY-MM-DD".
		str_table_name : str
			The name of the table, by default "br_b3_daily_liquidity_limits".

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		return super().run(
			dict_dtypes={
				"ID_CAMARA": str,
				"ORIGMEM_INSTRUMENTO": str,
				"ID_INSTRUMENTO": str,
				"SIMBOLO_INSTRUMENTO": str,
				"LIMITE_LIQUIDEZ": str,
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
			usecols=[0, 1, 2, 3, 4],
			names=[
				"ID_CAMARA",
				"ORIGMEM_INSTRUMENTO",
				"ID_INSTRUMENTO",
				"SIMBOLO_INSTRUMENTO",
				"LIMITE_LIQUIDEZ",
			],
		)
		df_["FILE_NAME"] = file_name
		return df_
