"""B3 Instruments File Indicators BVBG.029.02 ingestion."""

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


class B3InstrumentsFileIndicators(ABCB3SearchByTradingSession):
	"""B3 Instruments File Indicators BVBG.029.02 InstrumentsFile Indicator."""

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
			url="https://www.b3.com.br/pesquisapregao/download?filelist=II{}.zip",
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
		str_table_name: str = "br_b3_instruments_file_indicator",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		    The timeout, by default (12.0, 21.0).
		bool_verify : bool
		    Whether to verify the SSL certificate, by default True.
		bool_insert_or_ignore : bool
		    Whether to insert or ignore the data, by default False.
		str_fmt_dt : str
		    The format of the date, by default "YYYY-MM-DD".
		cols_from_case : str
		    The case of the columns, by default "pascal".
		cols_to_case : str
		    The case of the columns, by default "upper_constant".
		str_table_name : str
		    The name of the table, by default "br_b3_instruments_file_indicator".

		Returns
		-------
		Optional[pd.DataFrame]
		    The transformed DataFrame.
		"""
		return super().run(
			dict_dtypes={
				"ACTVTY_IND": str,
				"FRQCY": str,
				"NET_POS_ID": str,
				"DT": "date",
				"ID": str,
				"PRTRY": str,
				"MKT_IDR_CD": str,
				"INSTRM_NM": str,
				"DESC": str,
				"SGMT": str,
				"MKT": str,
				"ASST": str,
				"SCTY_CTGY": str,
				"TP_CD": str,
				"ECNC_IND_DESC": str,
				"BASE_CD": str,
				"VAL_TP_CD": str,
				"NTRY_REF_CD": str,
				"DCML_PRCSN": str,
				"MTRTY": str,
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
		soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag="Inds")
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

		dict_["ActvtyInd"] = soup_parent.find("RptParams").find("ActvtyInd").text
		dict_["Frqcy"] = soup_parent.find("RptParams").find("Frqcy").text
		dict_["NetPosId"] = soup_parent.find("RptParams").find("NetPosId").text
		dict_["Dt"] = soup_parent.find("RptParams").find("RptDtAndTm").find("Dt").text
		dict_["Id"] = soup_parent.find("InstrmId").find("OthrId").find("Id").text
		dict_["Prtry"] = soup_parent.find("InstrmId").find("OthrId").find("Tp").find("Prtry").text
		dict_["MktIdrCd"] = soup_parent.find("InstrmId").find("PlcOfListg").find("MktIdrCd").text
		dict_["InstrmNm"] = soup_parent.find("InstrmInf").find("InstrmNm").text
		dict_["Desc"] = soup_parent.find("InstrmInf").find("Desc").text
		dict_["Sgmt"] = soup_parent.find("InstrmInf").find("Sgmt").text
		dict_["Mkt"] = soup_parent.find("InstrmInf").find("Mkt").text
		dict_["Asst"] = soup_parent.find("InstrmInf").find("Asst").text
		dict_["SctyCtgy"] = soup_parent.find("InstrmInf").find("SctyCtgy").text
		dict_["TpCd"] = soup_parent.find("InstrmDtls").find("EcncIndsInf").find("TpCd").text
		dict_["EcncIndDesc"] = (
			soup_parent.find("InstrmDtls").find("EcncIndsInf").find("EcncIndDesc").text
		)
		dict_["BaseCd"] = soup_parent.find("InstrmDtls").find("EcncIndsInf").find("BaseCd").text
		dict_["ValTpCd"] = soup_parent.find("InstrmDtls").find("EcncIndsInf").find("ValTpCd").text
		dict_["NtryRefCd"] = (
			soup_parent.find("InstrmDtls").find("EcncIndsInf").find("NtryRefCd").text
		)
		dict_["DcmlPrcsn"] = (
			soup_parent.find("InstrmDtls").find("EcncIndsInf").find("DcmlPrcsn").text
		)
		dict_["Mtrty"] = soup_parent.find("InstrmDtls").find("EcncIndsInf").find("Mtrty").text

		return dict_
