"""B3 Fee Daily Unit Cost BVBG.044.01 ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import pandas as pd
from bs4 import Tag
from requests import Session

from stpstone.ingestion.countries.br.exchange._b3_search_by_trading_session_base import (
	ABCB3SearchByTradingSession,
)


class B3FeeDailyUnitCost(ABCB3SearchByTradingSession):
	"""B3 Fee Daily Unit Cost BVBG.044.01 Fee Daily Unit Cost."""

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
			url="https://www.b3.com.br/pesquisapregao/download?filelist=DI{}.zip",
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
		str_table_name: str = "br_b3_fee_daily_unit_cost",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		    The timeout, by default (12.0, 21.0).
		bool_verify : bool, optional
		    Verify the SSL certificate, by default True.
		bool_insert_or_ignore : bool, optional
		    Insert or ignore, by default False.
		str_fmt_dt : str, optional
		    The format of the date, by default "YYYY-MM-DD".
		cols_from_case : str, optional
		    The case of the columns, by default "pascal".
		cols_to_case : str, optional
		    The case of the columns, by default "upper_constant".
		str_table_name : str, optional
		    The name of the table, by default "br_b3_fee_daily_unit_cost".

		Returns
		-------
		Optional[pd.DataFrame]
		    The transformed DataFrame.
		"""
		return super().run(
			dict_dtypes={
				"SGMT": str,
				"MKT": str,
				"ASST": str,
				"XPRTN_CD": str,
				"DAY_TRAD_IND": str,
				"TRAD_TX_TP": str,
				"AMT_XCHG_FEE_UNIT_COST": float,
				"AMT_XCHG_FEE_UNIT_COST_CCY": str,
				"AMT_REGN_FEE_UNIT_COST": float,
				"AMT_REGN_FEE_UNIT_COST_CCY": str,
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
		    The file name.

		Returns
		-------
		pd.DataFrame
		    The transformed DataFrame.
		"""
		soup_xml = self.cls_xml_handler.memory_parser(file)
		soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag="FeeInstrmInf")
		list_ser: list[dict[str, Union[str, int, float]]] = []

		for soup_parent in soup_node:
			list_ser.append(self._instrument_indicator_node(soup_parent))

		df_ = pd.DataFrame(list_ser)
		df_["FILE_NAME"] = file_name
		return df_

	def _instrument_indicator_node(self, soup_parent: Tag) -> dict[str, Union[str, int, float]]:
		"""Get node information from BeautifulSoup XML.

		Parameters
		----------
		soup_parent : Tag
		    Parsed XML document.

		Returns
		-------
		dict[str, Union[str, int, float]]
		    Dictionary containing node information.
		"""
		dict_: dict[str, Union[str, int, float]] = {}

		dict_["Sgmt"] = soup_parent.find("FinInstrmAttrbts").find("Sgmt").text
		dict_["Mkt"] = soup_parent.find("FinInstrmAttrbts").find("Mkt").text
		dict_["Asst"] = soup_parent.find("FinInstrmAttrbts").find("Asst").text
		dict_["XprtnCd"] = soup_parent.find("FinInstrmAttrbts").find("XprtnCd").text
		dict_["DayTradInd"] = soup_parent.find("TradInf").find("DayTradInd").text
		dict_["TradTxTp"] = soup_parent.find("TradInf").find("TradTxTp").text
		dict_["AmtXchgFeeUnitCost"] = soup_parent.find("XchgFeeUnitCost").find("Amt").text
		dict_["AmtXchgFeeUnitCostCcy"] = soup_parent.find("XchgFeeUnitCost").find("Amt").get("Ccy")
		dict_["AmtRegnFeeUnitCost"] = soup_parent.find("RegnFeeUnitCost").find("Amt").text
		dict_["AmtRegnFeeUnitCostCcy"] = soup_parent.find("RegnFeeUnitCost").find("Amt").get("Ccy")

		return dict_
