"""B3 Instruments File BVBG.028.02 Exercise Equities ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import pandas as pd
from requests import Session

from stpstone.ingestion.countries.br.exchange.b3_instruments_file import B3InstrumentsFile


class B3InstrumentsFileExrcEqts(B3InstrumentsFile):
	"""B3 Instruments File BVBG.028.02 InstrumentsFile (ExrcEqts)."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the class.

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
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_b3_instruments_file_exrc_eqts",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		    The timeout, by default (12.0, 21.0).
		bool_verify : bool, optional
		    Whether to verify the SSL certificate, by default True.
		bool_insert_or_ignore : bool, optional
		    Whether to insert or ignore the data, by default False.
		str_table_name : str, optional
		    The table name, by default "br_b3_instruments_file_exrc_eqts".

		Returns
		-------
		Optional[pd.DataFrame]
		    The transformed DataFrame.
		"""
		return super().run(
			timeout=timeout,
			bool_verify=bool_verify,
			bool_insert_or_ignore=bool_insert_or_ignore,
			dict_dtypes={
				"SCTY_CTGY": str,
				"TCKR_SYMB": str,
				"ISIN": str,
				"TRADG_CCY": str,
				"TRADG_START_DT": "date",
				"TRADG_END_DT": "date",
				"DLVRY_TP": str,
				"FILE_NAME": "category",
			},
			str_fmt_dt="YYYY-MM-DD",
			cols_from_case="pascal",
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
		return super().transform_data(
			file=file,
			file_name=file_name,
			tag_parent="ExrcEqtsInf",
			list_tags_children=[
				"SctyCtgy",
				"TckrSymb",
				"ISIN",
				"TradgCcy",
				"TradgStartDt",
				"TradgEndDt",
				"DlvryTp",
			],
			list_tups_attributes=[],
		)
