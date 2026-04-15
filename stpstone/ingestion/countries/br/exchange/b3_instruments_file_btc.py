"""B3 Instruments File BVBG.028.02 BTC ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import pandas as pd
from requests import Session

from stpstone.ingestion.countries.br.exchange.b3_instruments_file import B3InstrumentsFile


class B3InstrumentsFileBTC(B3InstrumentsFile):
	"""B3 Instruments File BVBG.028.02 InstrumentsFile (BTC)."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the class.

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
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_b3_instruments_file_btc",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Whether to verify the SSL certificate, by default True.
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False.
		str_table_name : str
			The name of the table, by default "br_b3_instruments_file_btc".

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
				"FNGB_IND": str,
				"PMT_TP": str,
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
			tag_parent="BTCInf",
			list_tags_children=[
				"SctyCtgy",
				"TckrSymb",
				"FngbINd",
				"PmtTp",
			],
			list_tups_attributes=[],
		)
