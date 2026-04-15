"""ANBIMA Data API funds client: retrieval and transformation of fund data.

References
----------
.. [1] https://developers.anbima.com.br/pt/
.. [2] https://developers.anbima.com.br/pt/swagger-de-fundos-v2-rcvm-175/
"""

from typing import Any, Literal, Optional

import pandas as pd
from requests import exceptions as req_exceptions

from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.lists import ListHandler
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.providers.br.anbimadata_api.anbimadata_api_gen import AnbimaDataGen


class AnbimaDataFunds(AnbimaDataGen):
	"""ANBIMA funds API client — retrieval and transformation of fund data."""

	# --- response structure keys ---
	_KEY_CONTENT = "content"
	_KEY_SUBCLASSES = "subclasses"
	_KEY_SUBCLASS = "subclass"

	# --- column names ---
	_COL_FUND_CODE = "codigo_fundo"
	_COL_TYPE_ID = "tipo_identificador_fundo"
	_COL_FUND_ID = "identificador_fundo"
	_COL_COMP_NAME = "razao_social_fundo"
	_COL_TRADE_NAME = "nome_comercial_fundo"
	_COL_FUND_TYPE = "tipo_fundo"
	_COL_CLASS_CODE = "codigo_classe"
	_COL_CLASS_ID_TYPE = "tipo_identificador_classe"
	_COL_CLASS_ID = "identificador_classe"
	_COL_COMP_CLASS = "razao_social_classe"
	_COL_TRD_CLASS = "nome_comercial_classe"
	_COL_N1_CTG = "nome_nivel1_categoria"
	_COL_SUBCLASSES = "subclasses"
	_COL_FUND_CLOSURE_DT = "data_encerramento_fundo"
	_COL_CLOSURE_DT = "data_encerramento"
	_COL_EFF_DT = "data_vigencia"
	_COL_INCPT_DT = "data_inicio_atividade"
	_COL_UPDATE_TS = "data_atualizacao"
	_COL_SBC_INCPT_DT = "subclass_data_inicio_atividade"
	_COL_SBC_CLOSURE_DT = "subclass_data_encerramento"
	_COL_SBC_EFF_DT = "subclass_data_vigencia"
	_COL_SBC_UPDATE_DT = "subclass_data_atualizacao"
	_COL_NUM_FND = "NUM_FND"
	_COL_NUM_CLASS = "NUM_CLASSES"
	_COL_NUM_PG = "NUM_PG"

	# --- fill-na and format sentinels ---
	_STR_DT_FORMAT = "YYYY-MM-DD"
	_STR_DT_FILL_NA = "2100-01-01"
	_STR_TS_FILL_NA = "2100-01-01T00:00:00.000"
	_STR_FLOAT_FILL_NA = 0.0
	_STR_FILL_NA = "N/A"

	def __init__(
		self,
		str_client_id: str,
		str_client_secret: str,
		str_env: Literal["dev", "prd"] = "dev",
		int_chunk: int = 1000,
	) -> None:
		"""Initialize fund client and cache utility instances.

		Parameters
		----------
		str_client_id : str
			Client ID for API authentication.
		str_client_secret : str
			Client secret for API authentication.
		str_env : Literal["dev", "prd"]
			Target environment, defaults to ``"dev"``.
		int_chunk : int
			Page size for paginated requests, defaults to 1000.
		"""
		super().__init__(str_client_id, str_client_secret, str_env, int_chunk)
		self._cls_dates = DatesBRAnbima()
		self._cls_dicts = HandlingDicts()
		self._cls_str = StrHandler()
		self._cls_list = ListHandler()

	# ------------------------------------------------------------------
	# Funds catalogue
	# ------------------------------------------------------------------

	def funds_raw(self, int_pg: Optional[int] = None) -> list[dict[str, Any]]:
		"""Retrieve one page of the funds catalogue.

		Parameters
		----------
		int_pg : int or None
			Page number.

		Returns
		-------
		list[dict[str, Any]]
			Raw paginated API response.
		"""
		return self.generic_request(
			f"feed/fundos/v2/fundos?size={self.int_chunk}&page={int_pg}", "GET"
		)

	def funds_trt(self, int_pg: int = 0) -> pd.DataFrame:
		"""Fetch and transform all pages of the funds catalogue into a DataFrame.

		Parameters
		----------
		int_pg : int
			Starting page number, defaults to 0.

		Returns
		-------
		pd.DataFrame
			Processed funds catalogue.

		Raises
		------
		ValueError
			If the API returns an unexpected payload structure.
		"""
		list_ser: list[dict[str, Any]] = []
		int_fnd = 0

		while True:
			try:
				json_funds = self.funds_raw(int_pg)
				if not isinstance(json_funds, list) or not json_funds:
					raise ValueError("Invalid funds data format")
			except req_exceptions.HTTPError:
				break

			for dict_cnt in json_funds[self._KEY_CONTENT]:
				dict_aux: dict[str, Any] = {}
				int_fnd += 1

				for key_cnt, data_cnt in dict_cnt.items():
					if isinstance(data_cnt, str):
						dict_aux[key_cnt] = data_cnt.strip()
					elif data_cnt is None:
						dict_aux[key_cnt] = data_cnt
					elif isinstance(data_cnt, list):
						for i_cls, dict_cls in enumerate(data_cnt):
							for key_cls, data_cls in dict_cls.items():
								if key_cls != self._KEY_SUBCLASSES and data_cls is not None:
									dict_aux[key_cls] = data_cls.strip()
								elif key_cls != self._KEY_SUBCLASSES and data_cls is None:
									dict_aux[key_cls] = data_cls
								elif (
									key_cls == self._KEY_SUBCLASSES
									and isinstance(data_cls, list)
								):
									for dict_sbcls in data_cls:
										dict_xpt = dict_aux.copy()
										for key_sbcls, data_sbcls in dict_sbcls.items():
											dict_xpt = self._cls_dicts.merge_n_dicts(
												dict_xpt,
												{f"{self._KEY_SUBCLASS}_{key_sbcls}": data_sbcls},
												{
													self._COL_NUM_FND: int_fnd + 1,
													self._COL_NUM_CLASS: i_cls + 1,
													self._COL_NUM_PG: int_pg,
												},
											)
										list_ser.append(dict_xpt)
								elif key_cls == self._KEY_SUBCLASSES and data_cls is None:
									list_ser.append(
										self._cls_dicts.merge_n_dicts(
											dict_aux,
											{key_cls: data_cls},
											{
												self._COL_NUM_FND: int_fnd + 1,
												self._COL_NUM_CLASS: i_cls + 1,
												self._COL_NUM_PG: int_pg,
											},
										)
									)
								else:
									raise ValueError(
										f"Invalid content in class: page={int_pg}, "
										f"key={key_cls}, data={data_cls}"
									)
					else:
						raise ValueError(f"Invalid content data type: {data_cnt}")

			int_pg += 1

		df_funds = pd.DataFrame(list_ser)
		return self._process_funds_dataframe(df_funds)

	def _process_funds_dataframe(self, df_funds: pd.DataFrame) -> pd.DataFrame:
		"""Clean and type-cast the funds catalogue DataFrame.

		Parameters
		----------
		df_funds : pd.DataFrame
			Raw DataFrame to process.

		Returns
		-------
		pd.DataFrame
			Processed DataFrame with correct column types.

		Raises
		------
		ValueError
			If expected date or timestamp columns are missing.
		"""
		list_date_cols = [
			self._COL_FUND_CLOSURE_DT,
			self._COL_EFF_DT,
			self._COL_INCPT_DT,
			self._COL_CLOSURE_DT,
			self._COL_SBC_INCPT_DT,
			self._COL_SBC_CLOSURE_DT,
			self._COL_SBC_EFF_DT,
		]
		list_ts_cols = [
			self._COL_UPDATE_TS,
			self._COL_SBC_UPDATE_DT,
		]
		list_str_cols = [
			self._COL_FUND_CODE,
			self._COL_TYPE_ID,
			self._COL_FUND_ID,
			self._COL_COMP_NAME,
			self._COL_TRADE_NAME,
			self._COL_FUND_TYPE,
			self._COL_CLASS_CODE,
			self._COL_CLASS_ID_TYPE,
			self._COL_CLASS_ID,
			self._COL_COMP_CLASS,
			self._COL_TRD_CLASS,
			self._COL_N1_CTG,
			self._COL_SUBCLASSES,
		]
		list_all_required = self._cls_list.extend_lists(list_date_cols, list_ts_cols)
		if any(col not in df_funds.columns for col in list_all_required):
			raise ValueError("Missing expected columns in DataFrame")

		for col in list_date_cols:
			df_funds[col] = df_funds[col].fillna(self._STR_DT_FILL_NA)
			df_funds[col] = df_funds[col].apply(
				lambda d: self._cls_dates.str_date_to_datetime(d, self._STR_DT_FORMAT)
			)

		for col in list_ts_cols:
			df_funds[col] = df_funds[col].fillna(self._STR_TS_FILL_NA)
			df_funds[col] = df_funds[col].apply(
				lambda d: self._cls_dates.timestamp_to_date(d, format=self._STR_DT_FORMAT)
			)

		for col in list_str_cols:
			if col not in df_funds.columns:
				continue
			df_funds[col] = df_funds[col].fillna(self._STR_FILL_NA)
			df_funds[col] = df_funds[col].astype(str).str.strip()

		return df_funds.drop_duplicates()

	# ------------------------------------------------------------------
	# Single-fund historical data
	# ------------------------------------------------------------------

	def fund_raw(self, str_code_fnd: str) -> list[dict[str, Any]]:
		"""Retrieve raw historical data for a fund by fund code.

		Parameters
		----------
		str_code_fnd : str
			Fund code identifier.

		Returns
		-------
		list[dict[str, Any]]
			Raw API response.
		"""
		return self.generic_request(
			f"feed/fundos/v2/fundos/{str_code_fnd}/historico", "GET"
		)

	def fund_hist(self, str_code_class: str) -> list[dict[str, Any]]:
		"""Retrieve raw historical data for a fund by class code.

		Parameters
		----------
		str_code_class : str
			Fund class code identifier (differs semantically from fund code).

		Returns
		-------
		list[dict[str, Any]]
			Raw API response.
		"""
		return self.generic_request(
			f"feed/fundos/v2/fundos/{str_code_class}/historico", "GET"
		)

	def fund_trt(self, list_code_fnds: list[str]) -> dict[str, list[pd.DataFrame]]:
		"""Fetch and transform historical data for multiple funds.

		Builds one DataFrame per list-typed response key per fund.

		Parameters
		----------
		list_code_fnds : list[str]
			Fund codes to process.

		Returns
		-------
		dict[str, list[pd.DataFrame]]
			Mapping of fund code → list of processed DataFrames.

		Raises
		------
		ValueError
			If the API returns an unexpected payload structure.
		"""
		dict_dfs: dict[str, list[pd.DataFrame]] = {}

		for str_code_fnd in list_code_fnds:
			dict_dfs[str_code_fnd] = []
			json_fnd_info = self.fund_raw(str_code_fnd)

			if not isinstance(json_fnd_info, dict):
				raise ValueError(
					f"Invalid data type for fund {str_code_fnd}: {type(json_fnd_info)}"
				)

			dict_aux: dict[str, Any] = {}

			for key_cnt, data_cnt in json_fnd_info.items():
				if isinstance(data_cnt, (str, type(None))):
					dict_aux[key_cnt] = data_cnt
					continue

				if not isinstance(data_cnt, list):
					raise ValueError(
						f"Invalid data type for fund {str_code_fnd}: {type(data_cnt)}"
					)

				list_ser: list[dict[str, Any]] = []
				dict_xpt = dict_aux.copy()

				for dict_data in data_cnt:
					has_nested_list = False
					for key_data, data_data in dict_data.items():
						if isinstance(data_data, (str, type(None))):
							dict_xpt[f"{key_cnt}_{key_data}"] = data_data
						elif isinstance(data_data, list):
							has_nested_list = True
							for dict_hist in data_data:
								dict_xpt_2 = dict_xpt.copy()
								for key_hist, data_hist in dict_hist.items():
									dict_xpt_2[f"{key_cnt}_{key_data}_{key_hist}"] = data_hist
								list_ser.append(dict_xpt_2)
					if not has_nested_list:
						list_ser.append(dict_xpt.copy())

				if not list_ser:
					continue

				df_ = pd.DataFrame(list_ser)
				df_ = self._process_fund_dataframe(df_)
				dict_dfs[str_code_fnd].append(df_)

		return dict_dfs

	def _process_fund_dataframe(self, df_: pd.DataFrame) -> pd.DataFrame:
		"""Clean and type-cast a single fund's historical DataFrame.

		Parameters
		----------
		df_ : pd.DataFrame
			Raw DataFrame to process.

		Returns
		-------
		pd.DataFrame
			Processed DataFrame.

		Raises
		------
		ValueError
			If expected columns are missing.
		"""
		list_expected_cols = [
			"classes_historical_data_date",
			"classes_historical_data_timestamp",
			"classes_historical_data_percentual_value",
			"fund_code",
		]
		list_missing = [c for c in list_expected_cols if c not in df_.columns]
		if list_missing:
			raise ValueError(f"Missing expected columns in DataFrame: {list_missing}")

		for col in df_.columns:
			if self._cls_str.match_string_like(col, "*data_*") and len(col) == 10:
				df_[col] = df_[col].fillna(self._STR_DT_FILL_NA)
				df_[col] = df_[col].apply(
					lambda d: self._cls_dates.str_date_to_datetime(d, self._STR_DT_FORMAT)
				)
			elif (
				self._cls_str.match_string_like(col, "*data_*")
				and self._cls_str.match_string_like(col, "*T*")
				and len(col) > 10
			):
				df_[col] = df_[col].fillna(self._STR_TS_FILL_NA)
				df_[col] = df_[col].apply(
					lambda d: self._cls_dates.timestamp_to_date(d, format=self._STR_DT_FORMAT)
				)
			elif self._cls_str.match_string_like(col, "*percentual_*"):
				df_[col] = df_[col].fillna(self._STR_FLOAT_FILL_NA)
				df_[col] = df_[col].astype(float)
			else:
				df_[col] = df_[col].fillna(self._STR_FILL_NA)
				df_[col] = df_[col].astype(str).str.strip()

		return df_

	# ------------------------------------------------------------------
	# Other fund endpoints
	# ------------------------------------------------------------------

	def segment_investor(self, str_code_class: str) -> list[dict[str, Any]]:
		"""Retrieve net asset value by investor segment for a fund class.

		Parameters
		----------
		str_code_class : str
			Fund class code.

		Returns
		-------
		list[dict[str, Any]]
			Raw API response.
		"""
		return self.generic_request(
			f"feed/fundos/v2/fundos/segmento-investidor/{str_code_class}/patrimonio-liquido",
			"GET",
		)

	def time_series_fund(
		self,
		str_date_inf: str,
		str_date_sup: str,
		str_code_class: str,
	) -> list[dict[str, Any]]:
		"""Retrieve time-series (NAV history) for a fund class.

		Parameters
		----------
		str_date_inf : str
			Start date in ``YYYY-MM-DD`` format.
		str_date_sup : str
			End date in ``YYYY-MM-DD`` format.
		str_code_class : str
			Fund class code.

		Returns
		-------
		list[dict[str, Any]]
			Raw API response.
		"""
		dict_payload = {
			"size": self.int_chunk,
			"data-inicio": str_date_inf,
			"data-fim": str_date_sup,
		}
		return self.generic_request(
			f"feed/fundos/v2/fundos/{str_code_class}/serie-historica",
			str_method="GET",
			dict_payload=dict_payload,
		)

	def funds_financials_dt(self, str_date_update: str) -> list[dict[str, Any]]:
		"""Retrieve bulk financials updated on a given date.

		Parameters
		----------
		str_date_update : str
			Reference date in ``YYYY-MM-DD`` format.

		Returns
		-------
		list[dict[str, Any]]
			Raw API response.
		"""
		dict_payload = {"data-atualizacao": str_date_update, "size": self.int_chunk}
		return self.generic_request(
			"feed/fundos/v2/fundos/serie-historica/lote",
			str_method="GET",
			dict_payload=dict_payload,
		)

	def funds_registration_data_dt(self, str_date_update: str) -> list[dict[str, Any]]:
		"""Retrieve bulk registration data updated on a given date.

		Parameters
		----------
		str_date_update : str
			Reference date in ``YYYY-MM-DD`` format.

		Returns
		-------
		list[dict[str, Any]]
			Raw API response.
		"""
		dict_payload = {"data-atualizacao": str_date_update, "size": self.int_chunk}
		return self.generic_request(
			"feed/fundos/v2/fundos/dados-cadastrais/lote",
			str_method="GET",
			dict_payload=dict_payload,
		)

	def institutions(self) -> list[dict[str, Any]]:
		"""Retrieve all registered institutions.

		Returns
		-------
		list[dict[str, Any]]
			Raw API response.
		"""
		return self.generic_request(
			"feed/fundos/v2/fundos/instituicoes",
			str_method="GET",
			dict_payload={"size": self.int_chunk},
		)

	def institution(self, str_ein: str) -> list[dict[str, Any]]:
		"""Retrieve data for a specific institution by EIN (CNPJ).

		Parameters
		----------
		str_ein : str
			Employer Identification Number (CNPJ).

		Returns
		-------
		list[dict[str, Any]]
			Raw API response.
		"""
		return self.generic_request(
			f"feed/fundos/v2/fundos/instituicoes/{str_ein}",
			str_method="GET",
			dict_payload={"size": self.int_chunk},
		)

	def explanatory_notes_fund(self, str_code_class: str) -> list[dict[str, Any]]:
		"""Retrieve explanatory notes for a fund class.

		Parameters
		----------
		str_code_class : str
			Fund class code.

		Returns
		-------
		list[dict[str, Any]]
			Raw API response.
		"""
		return self.generic_request(
			f"feed/fundos/v2/fundos/{str_code_class}/notas-explicativas",
			"GET",
		)
