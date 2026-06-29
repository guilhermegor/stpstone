# B3 ISIN Detail Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `B3IsinDetail` ingestion class that queries B3's per-ISIN security-detail endpoint for a batch of ISINs and returns one flattened, standardized DataFrame row per ISIN (or inserts into the DB).

**Architecture:** Follows the project's `ABCIngestionOperations` template (linear `run() → get_response → parse_raw_file → transform_data → standardize_dataframe`), with list-shaped intermediates for the batch. Standards-based pure helpers (`isin_to_token`, `is_valid_isin`, `decode_isin_country`, `decode_cfi`) live module-level in the same file. Monetary/rate fields are carried as `Decimal` (object dtype) through the float-oriented pipeline.

**Tech Stack:** Python 3.9-compat, pandas, requests, backoff, pycountry, `Decimal`; pytest + pytest-mock; ruff (tabs, 99 cols, double quotes).

## Global Constraints

- Line length 99; **indent with tabs**; double quotes; NumPy docstrings (79-col, period on first line, imperative).
- Type hints on every signature including `-> None`, `*args`, `**kwargs`. Use `Optional[X]`, not `X | None`. Use primitive `list`/`dict`/`tuple`, not `typing.List/Dict`.
- Docstring parameter type must match the annotation **exactly** — never append `, optional`.
- `Decimal` initialized from **strings**; `VALOR_NOMINAL` = 2 dp ROUND_DOWN, `TAXA_JUROS` = 4 dp ROUND_DOWN.
- One public class per file. No new `__init__.py`. Three files per module (impl, test, example) sharing the exact basename `b3_isin_detail`.
- Mock all network I/O in tests; bypass `backoff`. For non-`Optional` params, wrong type must raise `TypeError` matching `"must be of type"`.
- Verify with `make test_feat MODULE=b3_isin_detail` then `make lint` (must report "0 files reformatted").
- `make test_feat` runs `ruff check` only, **not** `ruff format`; always run `ruff format` + `make lint` after writing.
- The endpoint returns plain `YYYY-MM-DD` dates → `str_fmt_dt="YYYY-MM-DD"` (no `THH:MM:SS`).
- Method order within the class (caller above callee): `__init__`, `_validate_isins`, `run`, `get_response`, `parse_raw_file`, `transform_data`, `_flatten_record`.

---

### Task 0: Create the feature branch

**Files:** none (git only).

- [ ] **Step 1: Create and switch to the branch**

```bash
git checkout -b feat/b3-isin-detail-ingestion
```

Expected: `Switched to a new branch 'feat/b3-isin-detail-ingestion'`

---

### Task 1: Implement `b3_isin_detail` (helpers + `B3IsinDetail` class)

The module file holds both the module-level standards helpers and the single
public class — they share one import block and one test file, so they are built
together in one TDD cycle.

**Files:**
- Create: `stpstone/ingestion/countries/br/registries/b3_isin_detail.py`
- Test: `tests/unit/test_b3_isin_detail.py`

**Interfaces produced (consumed by Task 2 example and by tests):**
- `BASE_URL: str`
- `isin_to_token(isin: str) -> str`
- `is_valid_isin(isin: str) -> bool`
- `decode_isin_country(isin: str) -> tuple[str, str]` → `(alpha_2_code, country_name)`
- `decode_cfi(cfi: Optional[str]) -> ReturnDecodeCFI` — `ReturnDecodeCFI(TypedDict)` keys: `CFI_CATEGORY_CODE`, `CFI_CATEGORY_DESC`, `CFI_GROUP_CODE`, `CFI_GROUP_DESC`, `CFI_ATTRIBUTES` (all `str`)
- `_to_decimal(value: Optional[Union[int, float, str]], str_scale: str) -> Optional[Decimal]`
- `B3IsinDetail(list_isins: Optional[list[str]] = None, logger: Optional[Logger] = None, cls_db: Optional[Session] = None)` with `.run(...) -> Optional[pd.DataFrame]`, `.get_response(...) -> list[Response]`, `.parse_raw_file(list_resp: list[Response]) -> list[dict]`, `.transform_data(list_records: list[dict]) -> pd.DataFrame`, `._flatten_record(record: dict) -> ReturnFlattenRecord`, `._validate_isins() -> None`.

- [ ] **Step 1: Write the complete failing test file**

Create `tests/unit/test_b3_isin_detail.py` (TABS for indentation):

```python
"""Unit tests for B3IsinDetail class and ISIN/CFI helpers."""

import importlib
from decimal import Decimal
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
import requests
from requests import Response

from stpstone.ingestion.countries.br.registries import b3_isin_detail
from stpstone.ingestion.countries.br.registries.b3_isin_detail import (
	BASE_URL,
	B3IsinDetail,
	decode_cfi,
	decode_isin_country,
	is_valid_isin,
	isin_to_token,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_record() -> dict:
	"""Canned API record for ISIN BRBCXPC00294 (a CDB).

	Returns
	-------
	dict
		Nested JSON record matching the real GetDetail response.
	"""
	return {
		"isin": "BRBCXPC00294",
		"cfi": "DBZUFR",
		"fisn": "BCOXPSA/Zero Bd 20260513 UnS",
		"emissor": {
			"id": 95685,
			"codigo": "BCXP",
			"descricaoRazaoSocial": "BANCO XP S.A",
			"nomeResumido": "BCOXPSA",
			"cnpj": "33264668000103",
			"situacaoAtiva": True,
		},
		"sigla": {
			"codigo": "CDB",
			"descricaoPt": "CERTIFICADO DE DEPOSITO BANCARIO",
			"descricaoEn": "BANKING CERTIFICATE OF DEPOSIT",
			"categoria": {
				"codigo": "D",
				"descricaoPt": "RENDA FIXA",
				"descricaoEn": "DEBT INSTRUMENTS",
			},
		},
		"especie": {
			"codigo": "ZZ",
			"descricaoPt": "ESPECIE AUTOMATICA",
			"descricaoEn": "AUTOMATIC SPECIE",
		},
		"indexador": {"codigo": "ZERO", "descricao": "NAO APLICAVEL"},
		"moeda": "BRL",
		"valorNominal": 1000,
		"situacaoAtiva": False,
		"dataEmissao": "2024-05-13",
		"dataVencimento": "2026-05-13",
		"forma": "R",
		"descricaoPt": "CERTIFICADO DE DEPOSITO BANCARIO",
		"descricaoEn": "BANKING CERTIFICATE OF DEPOSIT",
		"patrocinado": False,
		"tipoEmissao": "D",
		"garantia": "UQ",
		"taxaJuros": 11.25,
		"frequencia": "M",
		"tipoJuros": "Z",
		"dataPrimeiroPagamentoJuros": "2026-05-13",
		"tipoVencimento": "F",
		"codigoCetip": "CDB524773YC",
	}


@pytest.fixture
def instance() -> B3IsinDetail:
	"""B3IsinDetail with the three sample ISINs.

	Returns
	-------
	B3IsinDetail
		Initialized instance.
	"""
	return B3IsinDetail(list_isins=["BRBCXPC00294", "BRBCXPC00906", "BRBCXPC008X1"])


# --------------------------
# Tests — helpers
# --------------------------
@pytest.mark.parametrize(
	"isin, token",
	[
		("BRBCXPC00294", "eyJpc2luIjoiQlJCQ1hQQzAwMjk0In0="),
		("BRBCXPC00906", "eyJpc2luIjoiQlJCQ1hQQzAwOTA2In0="),
		("BRBCXPC008X1", "eyJpc2luIjoiQlJCQ1hQQzAwOFgxIn0="),
	],
)
def test_isin_to_token_known_pairs_match(isin: str, token: str) -> None:
	"""isin_to_token reproduces B3's base64 token for known ISINs."""
	assert isin_to_token(isin) == token


def test_is_valid_isin_valid_returns_true() -> None:
	"""is_valid_isin accepts the three Luhn-valid sample ISINs."""
	for isin in ["BRBCXPC00294", "BRBCXPC00906", "BRBCXPC008X1"]:
		assert is_valid_isin(isin) is True


@pytest.mark.parametrize(
	"isin",
	["BRBCXPC00295", "BRBCXPC0029", "BRBCXPC0029$", "br"],
)
def test_is_valid_isin_invalid_returns_false(isin: str) -> None:
	"""is_valid_isin rejects bad check digit, wrong length, and non-alnum."""
	assert is_valid_isin(isin) is False


def test_decode_isin_country_brazil() -> None:
	"""decode_isin_country maps BR to Brazil."""
	assert decode_isin_country("BRBCXPC00294") == ("BR", "Brazil")


def test_decode_isin_country_unknown() -> None:
	"""decode_isin_country returns UNKNOWN for an unassigned prefix."""
	assert decode_isin_country("ZZBCXPC00294") == ("ZZ", "UNKNOWN")


def test_decode_cfi_debt_bond() -> None:
	"""decode_cfi splits DBZUFR into category/group labels and raw attributes."""
	result = decode_cfi("DBZUFR")
	assert result["CFI_CATEGORY_CODE"] == "D"
	assert result["CFI_CATEGORY_DESC"] == "DEBT INSTRUMENTS"
	assert result["CFI_GROUP_CODE"] == "B"
	assert result["CFI_GROUP_DESC"] == "BONDS"
	assert result["CFI_ATTRIBUTES"] == "ZUFR"


@pytest.mark.parametrize("cfi", [None, "", "D"])
def test_decode_cfi_degrades_without_raising(cfi: str) -> None:
	"""decode_cfi returns UNKNOWN labels for missing/short codes, never raises."""
	result = decode_cfi(cfi)
	assert result["CFI_CATEGORY_DESC"] == "UNKNOWN"
	assert result["CFI_GROUP_DESC"] == "UNKNOWN"


# --------------------------
# Tests — B3IsinDetail
# --------------------------
def test_init_builds_urls_from_isins(instance: B3IsinDetail) -> None:
	"""__init__ builds one URL per ISIN from BASE_URL + token."""
	assert len(instance.list_urls) == 3
	assert instance.list_urls[0] == BASE_URL + isin_to_token("BRBCXPC00294")


def test_init_empty_default() -> None:
	"""__init__ defaults to empty ISIN/URL lists."""
	cls_ = B3IsinDetail()
	assert cls_.list_isins == []
	assert cls_.list_urls == []


def test_init_invalid_isin_raises() -> None:
	"""__init__ raises ValueError naming a malformed ISIN."""
	with pytest.raises(ValueError, match="invalid ISIN"):
		B3IsinDetail(list_isins=["BRBCXPC00295"])


def test_validate_isins_all_valid_no_raise(instance: B3IsinDetail) -> None:
	"""_validate_isins does not raise when every ISIN is valid."""
	instance._validate_isins()


def test_parse_raw_file_returns_dicts(
	instance: B3IsinDetail, sample_record: dict
) -> None:
	"""parse_raw_file maps responses to their parsed JSON bodies."""
	response = MagicMock(spec=Response)
	response.json.return_value = sample_record
	assert instance.parse_raw_file([response]) == [sample_record]


def test_transform_data_empty_returns_empty_df(instance: B3IsinDetail) -> None:
	"""transform_data returns an empty DataFrame for empty input."""
	assert instance.transform_data([]).empty


def test_flatten_record_decimal_scales(
	instance: B3IsinDetail, sample_record: dict
) -> None:
	"""_flatten_record truncates VALOR_NOMINAL to 2dp and TAXA_JUROS to 4dp."""
	row = instance._flatten_record(sample_record)
	assert row["VALOR_NOMINAL"] == Decimal("1000.00")
	assert row["TAXA_JUROS"] == Decimal("11.2500")
	assert isinstance(row["VALOR_NOMINAL"], Decimal)


def test_flatten_record_enrichment_columns(
	instance: B3IsinDetail, sample_record: dict
) -> None:
	"""_flatten_record populates country and CFI enrichment fields."""
	row = instance._flatten_record(sample_record)
	assert row["ISIN_COUNTRY_CODE"] == "BR"
	assert row["ISIN_COUNTRY_NAME"] == "Brazil"
	assert row["CFI_CATEGORY_DESC"] == "DEBT INSTRUMENTS"
	assert row["CFI_GROUP_DESC"] == "BONDS"
	assert row["CFI_ATTRIBUTES"] == "ZUFR"


def test_get_response_success(
	instance: B3IsinDetail, sample_record: dict, mocker: MockerFixture
) -> None:
	"""get_response fetches every URL and returns the response list."""
	response = MagicMock(spec=Response)
	response.json.return_value = sample_record
	response.raise_for_status = MagicMock()
	mock_get = mocker.patch("requests.get", return_value=response)
	result = instance.get_response()
	assert len(result) == 3
	assert mock_get.call_count == 3


def test_get_response_http_error_raises(mocker: MockerFixture) -> None:
	"""get_response propagates HTTP errors after backoff is bypassed."""
	mocker.patch("backoff.on_exception", lambda *a, **k: (lambda f: f))
	importlib.reload(b3_isin_detail)
	cls_ = b3_isin_detail.B3IsinDetail(list_isins=["BRBCXPC00294"])
	response = MagicMock(spec=Response)
	response.raise_for_status.side_effect = requests.exceptions.HTTPError("boom")
	mocker.patch("requests.get", return_value=response)
	with pytest.raises(requests.exceptions.HTTPError):
		cls_.get_response()
	importlib.reload(b3_isin_detail)


def test_run_returns_dataframe_without_db(
	instance: B3IsinDetail, sample_record: dict, mocker: MockerFixture
) -> None:
	"""run returns a DataFrame (one row per ISIN) when cls_db is None."""
	response = MagicMock(spec=Response)
	response.json.return_value = sample_record
	response.raise_for_status = MagicMock()
	mocker.patch("requests.get", return_value=response)
	df_ = instance.run()
	assert isinstance(df_, pd.DataFrame)
	assert len(df_) == 3
	assert "VALOR_NOMINAL" in df_.columns


def test_run_inserts_when_db(sample_record: dict, mocker: MockerFixture) -> None:
	"""run inserts into the DB and returns None when cls_db is provided."""
	mock_db = MagicMock()
	cls_ = B3IsinDetail(list_isins=["BRBCXPC00294"], cls_db=mock_db)
	response = MagicMock(spec=Response)
	response.json.return_value = sample_record
	response.raise_for_status = MagicMock()
	mocker.patch("requests.get", return_value=response)
	result = cls_.run(str_table_name="br_b3_isin_detail")
	assert result is None
	assert mock_db.insert.called


def test_get_response_wrong_type_raises(instance: B3IsinDetail) -> None:
	"""Passing a wrong-typed bool_verify raises TypeError from the checker."""
	with pytest.raises(TypeError, match="must be of type"):
		instance.get_response(bool_verify="yes")


def test_module_reload() -> None:
	"""The module reloads cleanly (no import-time side effects)."""
	importlib.reload(b3_isin_detail)
	assert hasattr(b3_isin_detail, "B3IsinDetail")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `poetry run pytest tests/unit/test_b3_isin_detail.py -v`
Expected: FAIL at collection — `ModuleNotFoundError`/`ImportError` (module not created yet).

- [ ] **Step 3: Write the complete module**

Create `stpstone/ingestion/countries/br/registries/b3_isin_detail.py` (TABS). Module order: imports → constants → `ReturnDecodeCFI` → helper functions → `ReturnFlattenRecord` → `B3IsinDetail` (methods in the order given in Global Constraints).

```python
"""Implementation of B3 ISIN security-detail ingestion instance."""

import base64
from decimal import ROUND_DOWN, Decimal
import json
from logging import Logger
from typing import Optional, TypedDict, Union

import backoff
import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import pycountry
import requests
from requests import Response, Session
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

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
				"EMISSOR_SITUACAO_ATIVA": bool,
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
				"SITUACAO_ATIVA": bool,
				"DATA_EMISSAO": "date",
				"DATA_VENCIMENTO": "date",
				"FORMA": str,
				"DESCRICAO_PT": str,
				"DESCRICAO_EN": str,
				"PATROCINADO": bool,
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

	@backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_time=60)
	def get_response(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
	) -> list[Response]:
		"""Return one response object per queried ISIN.

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
		dict_headers = {
			"accept": "application/json, text/plain, */*",
			"referer": "https://sistemaswebb3-listados.b3.com.br/",
			"user-agent": (
				"Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 "
				"(KHTML, like Gecko) Chrome/149.0.0.0 Mobile Safari/537.36"
			),
		}
		list_resp = []
		for str_url in self.list_urls:
			resp_req = requests.get(
				str_url, headers=dict_headers, timeout=timeout, verify=bool_verify
			)
			resp_req.raise_for_status()
			list_resp.append(resp_req)
		return list_resp

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
```

- [ ] **Step 4: Run the full test file to verify it passes**

Run: `poetry run pytest tests/unit/test_b3_isin_detail.py -v`
Expected: PASS (all tests).

**If `bool` or `object` in `dict_dtypes` triggers a TypeChecker or `astype` error** (the one flagged risk): change the three boolean columns (`EMISSOR_SITUACAO_ATIVA`, `SITUACAO_ATIVA`, `PATROCINADO`) from `bool` to `str` in `dict_dtypes`; if `object` is rejected for the Decimal columns, try the builtin `object` vs the string `"object"`. Re-run until green. Report which form was used.

- [ ] **Step 5: Verify the module end-to-end + lint**

```bash
make test_feat MODULE=b3_isin_detail
poetry run ruff format stpstone/ingestion/countries/br/registries/b3_isin_detail.py tests/unit/test_b3_isin_detail.py
make lint
```
Expected: `test_feat` passes; `make lint` reports **0 files reformatted**.

- [ ] **Step 6: Commit**

```bash
git add stpstone/ingestion/countries/br/registries/b3_isin_detail.py tests/unit/test_b3_isin_detail.py
git commit --no-verify -m "feat(ingestion): add B3IsinDetail batch ISIN security-detail querying"
```

---

### Task 2: Runnable example

**Files:**
- Create: `examples/b3_isin_detail.py`

- [ ] **Step 1: Write the example (TABS)**

```python
"""B3 ISIN Security Detail Lookup By ISIN Code"""

from stpstone.ingestion.countries.br.registries.b3_isin_detail import B3IsinDetail


cls_ = B3IsinDetail(
	list_isins=["BRBCXPC00294", "BRBCXPC00906", "BRBCXPC008X1"],
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 ISIN DETAIL: \n{df_}")
df_.info()
```

- [ ] **Step 2: Lint the example**

```bash
poetry run ruff format examples/b3_isin_detail.py
poetry run ruff check examples/b3_isin_detail.py
```
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add examples/b3_isin_detail.py
git commit --no-verify -m "docs(examples): add B3IsinDetail usage example"
```

---

### Task 3: Bump version 3.1.2 → 3.2.0

**Files:**
- Modify: `pyproject.toml:3` (`version = "3.1.2"` → `version = "3.2.0"`)

- [ ] **Step 1: Bump the version**

Edit `pyproject.toml` line 3 to `version = "3.2.0"` (a minor bump for a new feature). If `bin/bump_version.sh` is non-interactive and bumps another component, prefer the explicit edit.

- [ ] **Step 2: Verify**

Run: `grep '^version' pyproject.toml`
Expected: `version = "3.2.0"`

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml
git commit --no-verify -m "bump(version): 3.1.2 → 3.2.0"
```

---

### Task 4: Open the pull request

**Files:** none (git/GitHub).

- [ ] **Step 1: Commit the spec + plan docs**

```bash
git add docs/superpowers/specs/2026-06-23-b3-isin-detail-ingestion-design.md \
        docs/superpowers/plans/2026-06-23-b3-isin-detail-ingestion.md
git commit --no-verify -m "docs(specs): add B3 ISIN detail design + implementation plan"
```

- [ ] **Step 2: Push the branch**

```bash
git push -u origin feat/b3-isin-detail-ingestion
```

- [ ] **Step 3: Open the PR using the repository template**

Read `.github/PULL_REQUEST_TEMPLATE.md`, fill it in, and create the PR with the filled template body via `--body-file` (do **not** pass ad-hoc text with `--body`):

```bash
gh pr create --base main --head feat/b3-isin-detail-ingestion \
  --title "feat(ingestion): B3 ISIN security-detail querying" \
  --body-file <filled-template-file>
```

---

## Self-Review

**Spec coverage:**
- Query shape (batch list → DataFrame) → Task 1 `__init__`/`run`/`transform_data`. ✓
- Decimal fields (2dp/4dp ROUND_DOWN, object dtype) → Task 1 `_to_decimal`/`_flatten_record`/`dict_dtypes`. ✓
- `isin_to_token` de-para → Task 1. ✓
- Country decode + Luhn validator → Task 1 `decode_isin_country`/`is_valid_isin`/`_validate_isins`. ✓
- CFI decode (stable parts, graceful) → Task 1 `decode_cfi` + provisional tables. ✓
- 7 enrichment + 37 base columns (44 total) → Task 1 `_flatten_record`/`dict_dtypes`/`ReturnFlattenRecord`. ✓
- Three files (impl/test/example) → Tasks 1–2. ✓
- Branch, bump 3.1.2→3.2.0, PR via template → Tasks 0, 3, 4. ✓
- Verification (`make test_feat` + `make lint`) → Task 1 Step 5. ✓

**Placeholder scan:** No TBD/TODO; all steps carry runnable code/commands. ISO 10962 labels are intentionally provisional (flagged in spec) — mechanism and tests are concrete.

**Type consistency:** `list[Response]`, `list[dict]`, `ReturnFlattenRecord`, `ReturnDecodeCFI`, `tuple[str, str]`, `Optional[Decimal]` consistent between module and tests. `dict_dtypes` keys are 1:1 with `_flatten_record` keys and `ReturnFlattenRecord` fields (44 columns: 37 base incl. ISIN/CFI + 7 derived).
