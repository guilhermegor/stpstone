"""Implementation of B3 ISIN security-detail ingestion instance."""

import base64
from decimal import ROUND_DOWN, Decimal
import json
from logging import Logger
import time
from typing import Optional, TypedDict, Union

import backoff
import pandas as pd
import pycountry
import requests
from requests import Response, Session

from stpstone.ingestion.abc.ingestion_abc import (
	ABCIngestionOperations,
	ContentParser,
	CoreIngestion,
)
from stpstone.transformations.validation.metaclass_type_checker import type_checker
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


BASE_URL = "https://sistemaswebb3-listados.b3.com.br/isinProxy/IsinCall/GetDetail/"

# ISO 10962 category (char 1) — provisional labels, verify vs official standard.
CFI_CATEGORIES: dict[str, str] = {
	"E": "EQUITIES",
	"C": "COLLECTIVE INVESTMENT VEHICLES",
	"D": "DEBT INSTRUMENTS",
	"R": "ENTITLEMENTS (RIGHTS)",
	"O": "OPTIONS",
	"F": "FUTURES",
	"S": "SWAPS",
	"H": "NON-LISTED AND COMPLEX LISTED OPTIONS",
	"I": "SPOT",
	"J": "FORWARDS",
	"K": "STRATEGIES",
	"L": "FINANCING",
	"T": "REFERENTIAL INSTRUMENTS",
	"M": "OTHERS (MISCELLANEOUS)",
}
# ISO 10962 group (char 2) by category — Debt populated; others on demand.
CFI_GROUPS: dict[str, dict[str, str]] = {
	"D": {
		"B": "BONDS",
		"C": "CONVERTIBLE BONDS",
		"W": "BONDS WITH WARRANTS ATTACHED",
		"T": "MEDIUM-TERM NOTES",
		"Y": "MONEY MARKET INSTRUMENTS",
		"G": "MORTGAGE-BACKED SECURITIES",
		"A": "ASSET-BACKED SECURITIES",
		"N": "MUNICIPAL BONDS",
		"S": "STRUCTURED INSTRUMENTS (CAPITAL PROTECTION)",
		"D": "DEPOSITARY RECEIPTS ON DEBT INSTRUMENTS",
		"M": "OTHERS (MISCELLANEOUS)",
	},
}

STR_UNKNOWN = "UNKNOWN"


class ReturnDecodeCFI(TypedDict):
	"""Decoded ISO 10962 CFI fields."""

	CFI_CATEGORY_CODE: str
	CFI_CATEGORY_DESC: str
	CFI_GROUP_CODE: str
	CFI_GROUP_DESC: str
	CFI_ATTRIBUTES: str


@type_checker
def isin_to_token(isin: str) -> str:
	"""Return the base64 token B3 expects for a given ISIN.

	Parameters
	----------
	isin : str
		The ISIN code.

	Returns
	-------
	str
		Base64 encoding of the compact JSON {"isin": isin}.
	"""
	str_payload = json.dumps({"isin": isin}, separators=(",", ":"))
	return base64.b64encode(str_payload.encode("ascii")).decode("ascii")


@type_checker
def is_valid_isin(isin: str) -> bool:
	"""Return True when isin is 12 alphanumeric chars with a valid Luhn digit.

	Implements the ISO 6166 check-digit algorithm: letters map to two digits
	(A=10 ... Z=35), then Luhn mod 10 over the resulting digit string.

	Parameters
	----------
	isin : str
		The ISIN code to validate.

	Returns
	-------
	bool
		True when the ISIN is well-formed and the check digit is valid.
	"""
	if len(isin) != 12 or not isin.isalnum():
		return False
	str_digits = ""
	for str_char in isin.upper():
		str_digits += str(ord(str_char) - 55) if str_char.isalpha() else str_char
	int_total = 0
	for int_idx, str_digit in enumerate(reversed(str_digits)):
		int_n = int(str_digit)
		if int_idx % 2 == 1:
			int_n *= 2
			if int_n > 9:
				int_n -= 9
		int_total += int_n
	return int_total % 10 == 0


@type_checker
def decode_isin_country(isin: str) -> tuple[str, str]:
	"""Decode the ISO 3166-1 alpha-2 country prefix of an ISIN.

	Parameters
	----------
	isin : str
		The ISIN code.

	Returns
	-------
	tuple[str, str]
		The alpha-2 code and the country name ("UNKNOWN" when unassigned).
	"""
	str_code = isin[:2].upper()
	country = pycountry.countries.get(alpha_2=str_code)
	return str_code, (country.name if country is not None else STR_UNKNOWN)


@type_checker
def decode_cfi(cfi: Optional[str]) -> ReturnDecodeCFI:
	"""Decode an ISO 10962 CFI code into category/group labels + raw attributes.

	Degrades gracefully: a missing or malformed cfi yields "UNKNOWN" labels and
	never raises (enrichment must not break ingestion).

	Parameters
	----------
	cfi : Optional[str]
		The 6-char CFI code from the API, or None.

	Returns
	-------
	ReturnDecodeCFI
		The decoded category/group codes and labels plus raw attributes.
	"""
	if not cfi or len(cfi) < 2:
		return {
			"CFI_CATEGORY_CODE": STR_UNKNOWN,
			"CFI_CATEGORY_DESC": STR_UNKNOWN,
			"CFI_GROUP_CODE": STR_UNKNOWN,
			"CFI_GROUP_DESC": STR_UNKNOWN,
			"CFI_ATTRIBUTES": cfi or STR_UNKNOWN,
		}
	str_cat = cfi[0].upper()
	str_grp = cfi[1].upper()
	return {
		"CFI_CATEGORY_CODE": str_cat,
		"CFI_CATEGORY_DESC": CFI_CATEGORIES.get(str_cat, STR_UNKNOWN),
		"CFI_GROUP_CODE": str_grp,
		"CFI_GROUP_DESC": CFI_GROUPS.get(str_cat, {}).get(str_grp, STR_UNKNOWN),
		"CFI_ATTRIBUTES": cfi[2:6],
	}


@type_checker
def _to_decimal(value: Optional[Union[int, float, str]], str_scale: str) -> Optional[Decimal]:
	"""Convert a numeric value to a truncated Decimal at the given scale.

	Parameters
	----------
	value : Optional[Union[int, float, str]]
		The raw value from the API, or None.
	str_scale : str
		The quantize scale as a string, e.g. "0.01".

	Returns
	-------
	Optional[Decimal]
		The truncated Decimal, or None when value is None.
	"""
	if value is None:
		return None
	return Decimal(str(value)).quantize(Decimal(str_scale), rounding=ROUND_DOWN)


class ReturnFlattenRecord(TypedDict):
	"""Flattened ISIN-detail row (one per ISIN)."""

	ISIN: str
	CFI: Optional[str]
	FISN: Optional[str]
	EMISSOR_ID: Optional[int]
	EMISSOR_CODIGO: Optional[str]
	EMISSOR_RAZAO_SOCIAL: Optional[str]
	EMISSOR_NOME_RESUMIDO: Optional[str]
	EMISSOR_CNPJ: Optional[str]
	EMISSOR_SITUACAO_ATIVA: Optional[bool]
	SIGLA_CODIGO: Optional[str]
	SIGLA_DESCRICAO_PT: Optional[str]
	SIGLA_DESCRICAO_EN: Optional[str]
	CATEGORIA_CODIGO: Optional[str]
	CATEGORIA_DESCRICAO_PT: Optional[str]
	CATEGORIA_DESCRICAO_EN: Optional[str]
	ESPECIE_CODIGO: Optional[str]
	ESPECIE_DESCRICAO_PT: Optional[str]
	ESPECIE_DESCRICAO_EN: Optional[str]
	INDEXADOR_CODIGO: Optional[str]
	INDEXADOR_DESCRICAO: Optional[str]
	MOEDA: Optional[str]
	VALOR_NOMINAL: Optional[Decimal]
	SITUACAO_ATIVA: Optional[bool]
	DATA_EMISSAO: Optional[str]
	DATA_VENCIMENTO: Optional[str]
	FORMA: Optional[str]
	DESCRICAO_PT: Optional[str]
	DESCRICAO_EN: Optional[str]
	PATROCINADO: Optional[bool]
	TIPO_EMISSAO: Optional[str]
	GARANTIA: Optional[str]
	TAXA_JUROS: Optional[Decimal]
	FREQUENCIA: Optional[str]
	TIPO_JUROS: Optional[str]
	DATA_PRIMEIRO_PAGAMENTO_JUROS: Optional[str]
	TIPO_VENCIMENTO: Optional[str]
	CODIGO_CETIP: Optional[str]
	ISIN_COUNTRY_CODE: str
	ISIN_COUNTRY_NAME: str
	CFI_CATEGORY_CODE: str
	CFI_CATEGORY_DESC: str
	CFI_GROUP_CODE: str
	CFI_GROUP_DESC: str
	CFI_ATTRIBUTES: str


class B3IsinDetail(ABCIngestionOperations):
	"""B3 ISIN security-detail ingestion concrete class."""

	def __init__(
		self,
		list_isins: Optional[list[str]] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
		int_seconds_between_requests: int = 2,
	) -> None:
		"""Initialize the B3 ISIN detail ingestion class.

		Parameters
		----------
		list_isins : Optional[list[str]]
			The ISIN codes to query, by default None.
		logger : Optional[Logger]
			The logger, by default None.
		cls_db : Optional[Session]
			The database session, by default None.
		int_seconds_between_requests : int
			Pause in seconds between sequential per-ISIN requests, by default 2.

		Returns
		-------
		None
		"""
		super().__init__(cls_db=cls_db)
		CoreIngestion.__init__(self)
		ContentParser.__init__(self)

		self.logger = logger
		self.cls_db = cls_db
		self.cls_dir_files_management = DirFilesManagement()
		self.cls_dates_current = DatesCurrent()
		self.cls_create_log = CreateLog()
		self.cls_dates_br = DatesBRAnbima()
		self.int_seconds_between_requests = int_seconds_between_requests
		self.list_isins = list_isins or []
		self._validate_isins()
		self.list_urls = [BASE_URL + isin_to_token(str_isin) for str_isin in self.list_isins]
		self.date_ref = self.cls_dates_current.curr_date()

	def _validate_isins(self) -> None:
		"""Validate every ISIN's check digit, failing fast on malformed input.

		Returns
		-------
		None

		Raises
		------
		ValueError
			If any ISIN fails the ISO 6166 Luhn check.
		"""
		list_invalid = [str_isin for str_isin in self.list_isins if not is_valid_isin(str_isin)]
		if list_invalid:
			raise ValueError(f"invalid ISIN(s): {list_invalid}")

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_b3_isin_detail",
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
			The name of the table, by default "br_b3_isin_detail".

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame, or None when inserted into the database.
		"""
		list_resp = self.get_response(timeout=timeout, bool_verify=bool_verify)
		list_records = self.parse_raw_file(list_resp)
		df_ = self.transform_data(list_records=list_records)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"ISIN": str,
				"CFI": str,
				"FISN": str,
				"EMISSOR_ID": int,
				"EMISSOR_CODIGO": str,
				"EMISSOR_RAZAO_SOCIAL": str,
				"EMISSOR_NOME_RESUMIDO": str,
				"EMISSOR_CNPJ": str,
				"EMISSOR_SITUACAO_ATIVA": str,
				"SIGLA_CODIGO": str,
				"SIGLA_DESCRICAO_PT": str,
				"SIGLA_DESCRICAO_EN": str,
				"CATEGORIA_CODIGO": str,
				"CATEGORIA_DESCRICAO_PT": str,
				"CATEGORIA_DESCRICAO_EN": str,
				"ESPECIE_CODIGO": str,
				"ESPECIE_DESCRICAO_PT": str,
				"ESPECIE_DESCRICAO_EN": str,
				"INDEXADOR_CODIGO": str,
				"INDEXADOR_DESCRICAO": str,
				"MOEDA": str,
				"VALOR_NOMINAL": object,
				"SITUACAO_ATIVA": str,
				"DATA_EMISSAO": "date",
				"DATA_VENCIMENTO": "date",
				"FORMA": str,
				"DESCRICAO_PT": str,
				"DESCRICAO_EN": str,
				"PATROCINADO": str,
				"TIPO_EMISSAO": str,
				"GARANTIA": str,
				"TAXA_JUROS": object,
				"FREQUENCIA": str,
				"TIPO_JUROS": str,
				"DATA_PRIMEIRO_PAGAMENTO_JUROS": "date",
				"TIPO_VENCIMENTO": str,
				"CODIGO_CETIP": str,
				"ISIN_COUNTRY_CODE": str,
				"ISIN_COUNTRY_NAME": str,
				"CFI_CATEGORY_CODE": str,
				"CFI_CATEGORY_DESC": str,
				"CFI_GROUP_CODE": str,
				"CFI_GROUP_DESC": str,
				"CFI_ATTRIBUTES": str,
			},
			str_fmt_dt="YYYY-MM-DD",
			url=BASE_URL,
		)
		if self.cls_db:
			self.insert_table_db(
				cls_db=self.cls_db,
				str_table_name=str_table_name,
				df_=df_,
				bool_insert_or_ignore=bool_insert_or_ignore,
			)
		else:
			return df_

	def get_response(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
	) -> list[Response]:
		"""Return one response object per queried ISIN.

		Requests are issued sequentially with a fixed pause between them to avoid
		hammering the endpoint; each individual request is retried via backoff in
		`_fetch_one`.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Verify the SSL certificate, by default True.

		Returns
		-------
		list[Response]
			One response object per ISIN URL.
		"""
		list_resp = []
		for int_idx, str_url in enumerate(self.list_urls):
			if int_idx > 0:
				time.sleep(self.int_seconds_between_requests)
			list_resp.append(self._fetch_one(str_url, timeout=timeout, bool_verify=bool_verify))
		return list_resp

	@backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_time=60)
	def _fetch_one(
		self,
		str_url: str,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
	) -> Response:
		"""Fetch a single ISIN detail URL, retrying on HTTP errors.

		Parameters
		----------
		str_url : str
			The fully-built GetDetail URL for one ISIN.
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Verify the SSL certificate, by default True.

		Returns
		-------
		Response
			The response object for the requested URL.
		"""
		dict_headers = {
			"accept": "application/json, text/plain, */*",
			"referer": "https://sistemaswebb3-listados.b3.com.br/",
			"user-agent": (
				"Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 "
				"(KHTML, like Gecko) Chrome/149.0.0.0 Mobile Safari/537.36"
			),
		}
		resp_req = requests.get(str_url, headers=dict_headers, timeout=timeout, verify=bool_verify)
		resp_req.raise_for_status()
		return resp_req

	def parse_raw_file(self, list_resp: list[Response]) -> list[dict]:
		"""Parse each response body into its JSON record.

		Parameters
		----------
		list_resp : list[Response]
			The response objects.

		Returns
		-------
		list[dict]
			One parsed JSON record per response.
		"""
		return [resp_req.json() for resp_req in list_resp]

	def transform_data(self, list_records: list[dict]) -> pd.DataFrame:
		"""Flatten the nested JSON records into a DataFrame.

		Parameters
		----------
		list_records : list[dict]
			The parsed JSON records.

		Returns
		-------
		pd.DataFrame
			One flattened row per record, or empty when there are no records.
		"""
		if not list_records:
			return pd.DataFrame()
		list_flat = [self._flatten_record(record) for record in list_records]
		return pd.DataFrame(list_flat)

	def _flatten_record(self, record: dict) -> ReturnFlattenRecord:
		"""Flatten one nested ISIN-detail record into a flat row.

		Parameters
		----------
		record : dict
			The nested JSON record for a single ISIN.

		Returns
		-------
		ReturnFlattenRecord
			The flattened row with enrichment and Decimal-typed fields.
		"""
		dict_emissor = record.get("emissor") or {}
		dict_sigla = record.get("sigla") or {}
		dict_categoria = dict_sigla.get("categoria") or {}
		dict_especie = record.get("especie") or {}
		dict_indexador = record.get("indexador") or {}
		str_isin = record.get("isin") or ""
		str_country_code, str_country_name = decode_isin_country(str_isin)
		dict_cfi = decode_cfi(record.get("cfi"))
		return {
			"ISIN": str_isin,
			"CFI": record.get("cfi"),
			"FISN": record.get("fisn"),
			"EMISSOR_ID": dict_emissor.get("id"),
			"EMISSOR_CODIGO": dict_emissor.get("codigo"),
			"EMISSOR_RAZAO_SOCIAL": dict_emissor.get("descricaoRazaoSocial"),
			"EMISSOR_NOME_RESUMIDO": dict_emissor.get("nomeResumido"),
			"EMISSOR_CNPJ": dict_emissor.get("cnpj"),
			"EMISSOR_SITUACAO_ATIVA": dict_emissor.get("situacaoAtiva"),
			"SIGLA_CODIGO": dict_sigla.get("codigo"),
			"SIGLA_DESCRICAO_PT": dict_sigla.get("descricaoPt"),
			"SIGLA_DESCRICAO_EN": dict_sigla.get("descricaoEn"),
			"CATEGORIA_CODIGO": dict_categoria.get("codigo"),
			"CATEGORIA_DESCRICAO_PT": dict_categoria.get("descricaoPt"),
			"CATEGORIA_DESCRICAO_EN": dict_categoria.get("descricaoEn"),
			"ESPECIE_CODIGO": dict_especie.get("codigo"),
			"ESPECIE_DESCRICAO_PT": dict_especie.get("descricaoPt"),
			"ESPECIE_DESCRICAO_EN": dict_especie.get("descricaoEn"),
			"INDEXADOR_CODIGO": dict_indexador.get("codigo"),
			"INDEXADOR_DESCRICAO": dict_indexador.get("descricao"),
			"MOEDA": record.get("moeda"),
			"VALOR_NOMINAL": _to_decimal(record.get("valorNominal"), "0.01"),
			"SITUACAO_ATIVA": record.get("situacaoAtiva"),
			"DATA_EMISSAO": record.get("dataEmissao"),
			"DATA_VENCIMENTO": record.get("dataVencimento"),
			"FORMA": record.get("forma"),
			"DESCRICAO_PT": record.get("descricaoPt"),
			"DESCRICAO_EN": record.get("descricaoEn"),
			"PATROCINADO": record.get("patrocinado"),
			"TIPO_EMISSAO": record.get("tipoEmissao"),
			"GARANTIA": record.get("garantia"),
			"TAXA_JUROS": _to_decimal(record.get("taxaJuros"), "0.0001"),
			"FREQUENCIA": record.get("frequencia"),
			"TIPO_JUROS": record.get("tipoJuros"),
			"DATA_PRIMEIRO_PAGAMENTO_JUROS": record.get("dataPrimeiroPagamentoJuros"),
			"TIPO_VENCIMENTO": record.get("tipoVencimento"),
			"CODIGO_CETIP": record.get("codigoCetip"),
			"ISIN_COUNTRY_CODE": str_country_code,
			"ISIN_COUNTRY_NAME": str_country_name,
			"CFI_CATEGORY_CODE": dict_cfi["CFI_CATEGORY_CODE"],
			"CFI_CATEGORY_DESC": dict_cfi["CFI_CATEGORY_DESC"],
			"CFI_GROUP_CODE": dict_cfi["CFI_GROUP_CODE"],
			"CFI_GROUP_DESC": dict_cfi["CFI_GROUP_DESC"],
			"CFI_ATTRIBUTES": dict_cfi["CFI_ATTRIBUTES"],
		}
