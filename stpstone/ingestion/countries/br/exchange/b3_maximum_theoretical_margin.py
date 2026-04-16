"""B3 Maximum Theoretical Margin ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import pandas as pd
from requests import Session

from stpstone.ingestion.countries.br.exchange._b3_search_by_trading_session_base import (
	ABCB3SearchByTradingSession,
)


class B3MaximumTheoreticalMargin(ABCB3SearchByTradingSession):
	"""B3 Maximum Theoretical Margin."""

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
			url="https://www.b3.com.br/pesquisapregao/download?filelist=MT{}.zip",
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
		str_table_name: str = "br_b3_maximum_theoretical_margin",
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
			The date format, by default "YYYY-MM-DD".
		str_table_name : str
			The table name, by default "br_b3_maximum_theoretical_margin".

		Returns
		-------
		Optional[pd.DataFrame]
			The DataFrame with the data.
		"""
		return super().run(
			dict_dtypes={
				"INSTRUMENT_MTM": str,
				"HOLDING_DAY": str,
				"MTM_MAX_C_PHI1": str,
				"MTM_MAX_V_PHI1": str,
				"MIN_MARGIN_CREDIT_COLLATERAL_PHI1": str,
				"MIN_MARGIN_CREDIT_COLLATERAL_PHI2": str,
				"TICKER": str,
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
			usecols=[0, 1, 2, 3, 4, 5, 6],
			names=[
				"INSTRUMENT_MTM",
				"HOLDING_DAY",
				"MTM_MAX_C_PHI1",
				"MTM_MAX_V_PHI1",
				"MIN_MARGIN_CREDIT_COLLATERAL_PHI1",
				"MIN_MARGIN_CREDIT_COLLATERAL_PHI2",
				"TICKER",
			],
		)
		df_["FILE_NAME"] = file_name
		return df_
