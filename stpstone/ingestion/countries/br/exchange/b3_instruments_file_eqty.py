"""B3 Instruments File BVBG.028.02 Equity ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import pandas as pd
from requests import Session

from stpstone.ingestion.countries.br.exchange.b3_instruments_file import B3InstrumentsFile


class B3InstrumentsFileEqty(B3InstrumentsFile):
	"""B3 Instruments File BVBG.028.02 InstrumentsFile (EqtyInf)."""

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
		str_table_name: str = "br_b3_instruments_file_eqty",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Whether to verify the SSL certificate, by default True.
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False.
		str_table_name : str
			The name of the table, by default "br_b3_instruments_file_eqty".

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
				"ISIN": str,
				"DSTRBTN_ID": str,
				"CFICD": str,
				"SPCFCTN_CD": str,
				"CRPN_NM": str,
				"TCKR_SYMB": str,
				"PMT_TP": str,
				"ALLCN_RND_LOT": int,
				"PRIC_FCTR": float,
				"TRADG_START_DT": str,
				"TRADG_END_DT": str,
				"CORP_ACTN_START_DT": str,
				"EXDSTRBTN_NB": int,
				"CTDY_TRTMNT_TP": str,
				"TRADG_CCY": str,
				"MKT_CPTLSTN": str,
				"LAST_PRIC": float,
				"FRST_PRIC": float,
				"DAYS_TO_STTLM": int,
				"RGHTS_ISSE_PRIC": float,
				"ASST_SUB_TP": str,
				"AUCTN_TP": str,
				"MKT_CPTLSTN_CCY": str,
				"LAST_PRIC_CCY": str,
				"FRST_PRIC_CCY": str,
				"RGHTS_ISSE_PRIC_CCY": str,
				"FILE_NAME": "category",
			},
			str_fmt_dt="YYYY-MM-DD",
			cols_from_case="pascal",
			cols_to_case="upper_constant",
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
		return super().transform_data(
			file=file,
			file_name=file_name,
			tag_parent="EqtyInf",
			list_tags_children=[
				"SctyCtgy",
				"ISIN",
				"DstrbtnId",
				"CFICd",
				"SpcfctnCd",
				"CrpnNm",
				"TckrSymb",
				"PmtTp",
				"AllcnRndLot",
				"PricFctr",
				"TradgStartDt",
				"TradgEndDt",
				"CorpActnStartDt",
				"EXDstrbtnNb",
				"CtdyTrtmntTp",
				"TradgCcy",
				"MktCptlstn",
				"LastPric",
				"FrstPric",
				"DaysToSttlm",
				"RghtsIssePric",
				"AsstSubTp",
				"AuctnTp",
			],
			list_tups_attributes=[
				("MktCptlstn", "Ccy"),
				("LastPric", "Ccy"),
				("FrstPric", "Ccy"),
				("RghtsIssePric", "Ccy"),
			],
		)
