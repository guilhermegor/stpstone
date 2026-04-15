"""B3 Price Report BVBG.086.01 ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import pandas as pd
from requests import Session

from stpstone.ingestion.countries.br.exchange._b3_search_by_trading_session_base import (
	ABCB3SearchByTradingSession,
)


class B3PriceReport(ABCB3SearchByTradingSession):
	"""B3 Index Report BVBG.086.01 PriceReport."""

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
			url="https://www.b3.com.br/pesquisapregao/download?filelist=PR{}.zip",
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
		cols_from_case: str = "pascal",
		cols_to_case: str = "upper_constant",
		str_table_name: str = "br_b3_price_report",
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
		str_fmt_dt : str
		    Date format string, by default "YYYY-MM-DD".
		cols_from_case : str
		    The case of the columns, by default "pascal".
		cols_to_case : str
		    The case of the columns, by default "upper_constant".
		str_table_name : str
		    The name of the table, by default "br_b3_price_report".

		Returns
		-------
		Optional[pd.DataFrame]
		    The transformed DataFrame.
		"""
		return super().run(
			dict_dtypes={
				"DT": "date",
				"TCKR_SYMB": str,
				"ID": str,
				"PRTRY": str,
				"MKT_IDR_CD": "category",
				"TRAD_QTY": int,
				"OPN_INTRST": float,
				"FIN_INSTRM_QTY": int,
				"OSCN_PCTG": float,
				"NTL_FIN_VOL": float,
				"INTL_FIN_VOL": float,
				"BEST_BID_PRIC": float,
				"BEST_ASK_PRIC": float,
				"FRST_PRIC": float,
				"MIN_PRIC": float,
				"MAX_PRIC": float,
				"TRAD_AVRG_PRIC": float,
				"LAST_PRIC": float,
				"VARTN_PTS": float,
				"MAX_TRAD_LMT": float,
				"MIN_TRAD_LMT": float,
				"NTL_FIN_VOL_CCY": str,
				"INTL_FIN_VOL_CCY": str,
				"BEST_BID_PRIC_CCY": str,
				"BEST_ASK_PRIC_CCY": str,
				"FRST_PRIC_CCY": str,
				"MIN_PRIC_CCY": str,
				"MAX_PRIC_CCY": str,
				"TRAD_AVRG_PRIC_CCY": str,
				"LAST_PRIC_CCY": str,
				"NTL_RGLR_VOL_CCY": str,
				"INTL_RGLR_VOL_CCY": str,
				"FILE_NAME": "category",
			},
			timeout=timeout,
			bool_verify=bool_verify,
			bool_insert_or_ignore=bool_insert_or_ignore,
			str_fmt_dt=str_fmt_dt,
			cols_from_case=cols_from_case,
			cols_to_case=cols_to_case,
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
		soup_xml = self.cls_xml_handler.memory_parser(file)
		soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag="PricRpt")
		list_ser: list[dict[str, Union[str, int, float]]] = []

		for soup_parent in soup_node:
			dict_ = {}
			for tag in [
				"Dt",
				"TckrSymb",
				"Id",
				"Prtry",
				"MktIdrCd",
				"TradQty",
				"OpnIntrst",
				"FinInstrmQty",
				"OscnPctg",
				"NtlFinVol",
				"IntlFinVol",
				"BestBidPric",
				"BestAskPric",
				"FrstPric",
				"MinPric",
				"MaxPric",
				"TradAvrgPric",
				"LastPric",
				"VartnPts",
				"MaxTradLmt",
				"MinTradLmt",
			]:
				soup_content_tag = soup_parent.find(tag)
				dict_[tag] = soup_content_tag.getText() if soup_content_tag else None
			for tag, attribute in [
				("NtlFinVol", "Ccy"),
				("IntlFinVol", "Ccy"),
				("BestBidPric", "Ccy"),
				("BestAskPric", "Ccy"),
				("FrstPric", "Ccy"),
				("MinPric", "Ccy"),
				("MaxPric", "Ccy"),
				("TradAvrgPric", "Ccy"),
				("LastPric", "Ccy"),
				("NtlRglrVol", "Ccy"),
				("IntlRglrVol", "Ccy"),
			]:
				soup_content_tag = soup_parent.find(tag)
				dict_[tag + attribute] = (
					soup_content_tag.get(attribute) if soup_content_tag else None
				)
			list_ser.append(dict_)

		df_ = pd.DataFrame(list_ser)
		df_["FILE_NAME"] = file_name
		return df_
