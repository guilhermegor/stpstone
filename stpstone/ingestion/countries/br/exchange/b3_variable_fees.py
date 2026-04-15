"""B3 Variable Fees BVBG.024.01 ingestion."""

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


class B3VariableFees(ABCB3SearchByTradingSession):
	"""B3 Variable Fees BVBG.024.01 Fee Variables."""

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
			url="https://www.b3.com.br/pesquisapregao/download?filelist=VA{}.zip",
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
		str_table_name: str = "br_b3_variable_fees",
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
		cols_from_case : str
		    The case of the columns, by default "pascal".
		cols_to_case : str
		    The case of the columns, by default "upper_constant".
		str_table_name : str
		    The table name, by default "br_b3_variable_fees".

		Returns
		-------
		Optional[pd.DataFrame]
		    The transformed DataFrame.
		"""
		return super().run(
			dict_dtypes={
				"FRQCY": str,
				"RPT_NB": str,
				"DT": str,
				"FR_DT_TM": str,
				"TO_DT_TM": str,
				"SGMT": str,
				"ASST": str,
				"REF_DT": str,
				"CONVS_IND_VAL": str,
				"ID": str,
				"PRTRY": str,
				"MKT_IDR_CD": str,
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
		soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag="FeeVarblInf")
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

		dict_["Frqcy"] = soup_parent.find("RptParams").find("Frqcy").text
		dict_["RptNb"] = soup_parent.find("RptParams").find("RptNb").text
		dict_["Dt"] = soup_parent.find("RptParams").find("RptDtAndTm").find("Dt").text
		dict_["FrDtTm"] = soup_parent.find("VldtyPrd").find("FrDtTm").text
		dict_["ToDtTm"] = soup_parent.find("VldtyPrd").find("ToDtTm").text
		dict_["Sgmt"] = soup_parent.find("FeeInf").find("FinInstrmAttrbts").find("Sgmt").text
		dict_["Asst"] = soup_parent.find("FeeInf").find("FinInstrmAttrbts").find("Asst").text
		dict_["RefDt"] = soup_parent.find("FeeInf").find("OthrFeeQtnInf").find("RefDt").text
		dict_["ConvsIndVal"] = (
			soup_parent.find("FeeInf").find("OthrFeeQtnInf").find("ConvsIndVal").text
		)
		dict_["Id"] = soup_parent.find("FeeInf").find("ConvsInd").find("OthrId").find("Id").text
		dict_["Prtry"] = (
			soup_parent.find("FeeInf")
			.find("ConvsInd")
			.find("OthrId")
			.find("Tp")
			.find("Prtry")
			.text
		)
		dict_["MktIdrCd"] = (
			soup_parent.find("FeeInf")
			.find("OthrFeeQtnInf")
			.find("PlcOfListg")
			.find("MktIdrCd")
			.text
		)

		return dict_
