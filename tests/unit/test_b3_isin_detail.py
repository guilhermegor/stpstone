"""Unit tests for B3IsinDetail class and ISIN/CFI helpers."""

from decimal import Decimal
import importlib
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
	"""isin_to_token reproduces B3's base64 token for known ISINs.

	Parameters
	----------
	isin : str
		The ISIN code.
	token : str
		The expected base64 token.
	"""
	assert isin_to_token(isin) == token


def test_is_valid_isin_valid_returns_true() -> None:
	"""is_valid_isin accepts the three Luhn-valid sample ISINs.

	Returns
	-------
	None
	"""
	for isin in ["BRBCXPC00294", "BRBCXPC00906", "BRBCXPC008X1"]:
		assert is_valid_isin(isin) is True


@pytest.mark.parametrize(
	"isin",
	["BRBCXPC00295", "BRBCXPC0029", "BRBCXPC0029$", "br"],
)
def test_is_valid_isin_invalid_returns_false(isin: str) -> None:
	"""is_valid_isin rejects bad check digit, wrong length, and non-alnum.

	Parameters
	----------
	isin : str
		The ISIN code to test.

	Returns
	-------
	None
	"""
	assert is_valid_isin(isin) is False


def test_decode_isin_country_brazil() -> None:
	"""decode_isin_country maps BR to Brazil."""
	assert decode_isin_country("BRBCXPC00294") == ("BR", "Brazil")


def test_decode_isin_country_unknown() -> None:
	"""decode_isin_country maps an unassigned prefix to UNKNOWN.

	Returns
	-------
	None
	"""
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
	"""decode_cfi yields UNKNOWN labels for missing/short codes and never raises.

	Parameters
	----------
	cfi : str
		The CFI code to test (None, empty string, or too short).

	Returns
	-------
	None
	"""
	result = decode_cfi(cfi)
	assert result["CFI_CATEGORY_DESC"] == "UNKNOWN"
	assert result["CFI_GROUP_DESC"] == "UNKNOWN"


# --------------------------
# Tests — B3IsinDetail
# --------------------------
def test_init_builds_urls_from_isins(instance: B3IsinDetail) -> None:
	"""__init__ builds one URL per ISIN from BASE_URL + token.

	Parameters
	----------
	instance : B3IsinDetail
		Initialized B3IsinDetail instance.
	"""
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
	"""_validate_isins does not raise when every ISIN is valid.

	Parameters
	----------
	instance : B3IsinDetail
		Initialized B3IsinDetail instance.
	"""
	instance._validate_isins()


def test_parse_raw_file_returns_dicts(instance: B3IsinDetail, sample_record: dict) -> None:
	"""parse_raw_file maps responses to their parsed JSON bodies.

	Parameters
	----------
	instance : B3IsinDetail
		Initialized B3IsinDetail instance.
	sample_record : dict
		Sample JSON record fixture.

	Returns
	-------
	None
	"""
	response = MagicMock(spec=Response)
	response.json.return_value = sample_record
	assert instance.parse_raw_file([response]) == [sample_record]


def test_transform_data_empty_returns_empty_df(instance: B3IsinDetail) -> None:
	"""transform_data yields an empty DataFrame for empty input.

	Parameters
	----------
	instance : B3IsinDetail
		Initialized B3IsinDetail instance.
	"""
	assert instance.transform_data([]).empty


def test_flatten_record_decimal_scales(instance: B3IsinDetail, sample_record: dict) -> None:
	"""_flatten_record truncates VALOR_NOMINAL to 2dp and TAXA_JUROS to 4dp.

	Parameters
	----------
	instance : B3IsinDetail
		Initialized B3IsinDetail instance.
	sample_record : dict
		Sample JSON record fixture.
	"""
	row = instance._flatten_record(sample_record)
	assert row["VALOR_NOMINAL"] == Decimal("1000.00")
	assert row["TAXA_JUROS"] == Decimal("11.2500")
	assert isinstance(row["VALOR_NOMINAL"], Decimal)


def test_flatten_record_enrichment_columns(instance: B3IsinDetail, sample_record: dict) -> None:
	"""_flatten_record populates country and CFI enrichment fields.

	Parameters
	----------
	instance : B3IsinDetail
		Initialized B3IsinDetail instance.
	sample_record : dict
		Sample JSON record fixture.
	"""
	row = instance._flatten_record(sample_record)
	assert row["ISIN_COUNTRY_CODE"] == "BR"
	assert row["ISIN_COUNTRY_NAME"] == "Brazil"
	assert row["CFI_CATEGORY_DESC"] == "DEBT INSTRUMENTS"
	assert row["CFI_GROUP_DESC"] == "BONDS"
	assert row["CFI_ATTRIBUTES"] == "ZUFR"


def test_get_response_success(
	instance: B3IsinDetail, sample_record: dict, mocker: MockerFixture
) -> None:
	"""get_response fetches every URL and gives back the response list.

	Parameters
	----------
	instance : B3IsinDetail
		Initialized B3IsinDetail instance.
	sample_record : dict
		Sample JSON record fixture.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	response = MagicMock(spec=Response)
	response.json.return_value = sample_record
	response.raise_for_status = MagicMock()
	mock_get = mocker.patch("requests.get", return_value=response)
	result = instance.get_response()
	assert len(result) == 3
	assert mock_get.call_count == 3


def test_get_response_http_error_raises(mocker: MockerFixture) -> None:
	"""get_response propagates HTTP errors after backoff is bypassed.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.
	"""
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
	"""Run produces a DataFrame (one row per ISIN) when cls_db is None.

	Parameters
	----------
	instance : B3IsinDetail
		Initialized B3IsinDetail instance.
	sample_record : dict
		Sample JSON record fixture.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	response = MagicMock(spec=Response)
	response.json.return_value = sample_record
	response.raise_for_status = MagicMock()
	mocker.patch("requests.get", return_value=response)
	df_ = instance.run()
	assert isinstance(df_, pd.DataFrame)
	assert len(df_) == 3
	assert "VALOR_NOMINAL" in df_.columns


def test_run_inserts_when_db(sample_record: dict, mocker: MockerFixture) -> None:
	"""Run inserts into the DB and gives back None when cls_db is provided.

	Parameters
	----------
	sample_record : dict
		Sample JSON record fixture.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
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
	"""Passing a wrong-typed bool_verify raises TypeError from the checker.

	Parameters
	----------
	instance : B3IsinDetail
		Initialized B3IsinDetail instance.
	"""
	with pytest.raises(TypeError, match="must be of type"):
		instance.get_response(bool_verify="yes")


def test_run_missing_optional_fields_no_spurious_bool(mocker: MockerFixture) -> None:
	"""Absent boolean fields standardize to the sentinel, never spurious True.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for patching network I/O.
	"""
	record = {
		"isin": "BRBCXPC00294",
		"cfi": "ESVUFR",
		"emissor": {"id": 1, "codigo": "X"},
	}
	cls_ = B3IsinDetail(list_isins=["BRBCXPC00294"])
	response = MagicMock(spec=Response)
	response.json.return_value = record
	response.raise_for_status = MagicMock()
	mocker.patch("requests.get", return_value=response)
	df_ = cls_.run()
	for col_ in ["EMISSOR_SITUACAO_ATIVA", "SITUACAO_ATIVA", "PATROCINADO"]:
		val = df_[col_].iloc[0]
		assert val is not True
		assert isinstance(val, str)


def test_module_reload() -> None:
	"""The module reloads cleanly (no import-time side effects)."""
	importlib.reload(b3_isin_detail)
	assert hasattr(b3_isin_detail, "B3IsinDetail")
