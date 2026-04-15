"""B3 Equities Fee Public Information ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

from bs4 import Tag
import pandas as pd
from requests import Session

from stpstone.ingestion.countries.br.exchange._b3_search_by_trading_session_base import (
	ABCB3SearchByTradingSession,
)


class B3EquitiesFeePublicInformation(ABCB3SearchByTradingSession):
	"""B3 Equities Fee Public Information."""

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
			url="https://www.b3.com.br/pesquisapregao/download?filelist=TX{}.zip",
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
		str_table_name: str = "br_b3_instruments_fee_public_information",
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
		cols_from_case : str
			The source case convention for column names, by default "pascal".
		cols_to_case : str
			The target case convention for column names, by default "upper_constant".
		str_table_name : str
			The name of the table, by default "br_b3_instruments_fee_public_information".

		Returns
		-------
		Optional[pd.DataFrame]
			The ingested data.
		"""
		return super().run(
			dict_dtypes={
				"FRQCY": str,
				"RPT_NB": str,
				"DT": str,
				"FR_DT": str,
				"TO_DT": str,
				"FEE_GRP_MKT": str,
				"DAY_TRAD_IND": str,
				"TIER_INITL_VAL": str,
				"TIER_FNL_VAL": str,
				"FEE_TP": str,
				"FEE_COST_VAL": str,
				"FEE_COST_CCY": str,
				"CLNT_CTGY": str,
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
		soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag="EqtsFeeInf")
		list_ser: list[dict[str, Union[str, int, float]]] = []

		for soup_parent in soup_node:
			records = self._instrument_indicator_node(soup_parent)
			list_ser.extend(records)

		df_ = pd.DataFrame(list_ser)
		df_["FILE_NAME"] = file_name
		return df_

	def _instrument_indicator_node(
		self,
		soup_parent: Tag,
	) -> list[dict[str, Union[str, int, float]]]:
		"""Get node information from BeautifulSoup XML.

		Parameters
		----------
		soup_parent : Tag
			Parsed XML document.

		Returns
		-------
		list[dict[str, Union[str, int, float]]]
			List of dictionaries containing node information for each fee record.
		"""
		list_records: list[dict[str, Union[str, int, float]]] = []

		rpt_params = soup_parent.find("RptParams")
		validity_period = soup_parent.find("VldtyPrd")

		frqcy = rpt_params.find("Frqcy").text if rpt_params.find("Frqcy") else None
		rpt_nb = rpt_params.find("RptNb").text if rpt_params.find("RptNb") else None
		dt = (
			rpt_params.find("RptDtAndTm").find("Dt").text
			if rpt_params.find("RptDtAndTm") and rpt_params.find("RptDtAndTm").find("Dt")
			else None
		)
		fr_dt = validity_period.find("FrDt").text if validity_period.find("FrDt") else None
		to_dt = validity_period.find("ToDt").text if validity_period.find("ToDt") else None

		fee_details = soup_parent.find("EqtsFeeDtls")
		if not fee_details:
			return list_records

		for fee_instrm_info in fee_details.find_all("FeeInstrmInf"):
			fee_grp_mkt = (
				fee_instrm_info.find("FeeGrpMkt").text
				if fee_instrm_info.find("FeeGrpMkt")
				else None
			)

			for fee in fee_instrm_info.find_all("Fee"):
				day_trad_ind = fee.find("DayTradInd").text if fee.find("DayTradInd") else None

				for tier_and_cost in fee.find_all("TierAndCost"):
					dict_record: dict[str, Union[str, int, float]] = {}

					dict_record["Frqcy"] = frqcy
					dict_record["RptNb"] = rpt_nb
					dict_record["Dt"] = dt
					dict_record["FrDt"] = fr_dt
					dict_record["ToDt"] = to_dt
					dict_record["FeeGrpMkt"] = fee_grp_mkt
					dict_record["DayTradInd"] = day_trad_ind

					tier_initl_val = tier_and_cost.find("TierInitlVal")
					tier_fnl_val = tier_and_cost.find("TierFnlVal")
					dict_record["TierInitlVal"] = tier_initl_val.text if tier_initl_val else None
					dict_record["TierFnlVal"] = tier_fnl_val.text if tier_fnl_val else None

					fee_tp = tier_and_cost.find("FeeTp")
					dict_record["FeeTp"] = fee_tp.text if fee_tp else None

					cost_inf = tier_and_cost.find("CostInf")
					if cost_inf:
						fee_cost_val = cost_inf.find("FeeCostVal")
						dict_record["FeeCostVal"] = fee_cost_val.text if fee_cost_val else None
						dict_record["FeeCostCcy"] = (
							fee_cost_val.get("Ccy") if fee_cost_val else None
						)
						clnt_ctgy_dtls = cost_inf.find("ClntCtgyDtls")
						if clnt_ctgy_dtls:
							clnt_ctgy = clnt_ctgy_dtls.find("ClntCtgy")
							dict_record["ClntCtgy"] = clnt_ctgy.text if clnt_ctgy else None
						else:
							dict_record["ClntCtgy"] = None

					list_records.append(dict_record)

		return list_records
