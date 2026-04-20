"""Unit tests for B3BdiStocksTradeByTrade class."""

from datetime import date
from io import BytesIO, StringIO
from logging import Logger
from unittest.mock import MagicMock
from zipfile import BadZipFile, ZipFile

import pandas as pd
import pytest
from pytest_mock import MockerFixture
import requests
from requests import Response

from stpstone.ingestion.countries.br.exchange.b3_bdi_stocks_trade_by_trade import (
	B3BdiStocksTradeByTrade,
)
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Helpers
# --------------------------
_SAMPLE_CSV_HEADER = (
	"DataReferencia;CodigoInstrumento;AcaoAtualizacao;PrecoNegocio;"
	"QuantidadeNegociada;HoraFechamento;CodigoIdentificadorNegocio;"
	"TipoSessaoPregao;DataNegocio;CodigoParticipanteComprador;"
	"CodigoParticipanteVendedor;TipoDoCanal"
)

_SAMPLE_CSV_ROW = "2026-04-17;PETR4;0;38,50;100;100000003;10;1;2026-04-17;308;72;1"

_SAMPLE_CSV_CONTENT = f"{_SAMPLE_CSV_HEADER}\n{_SAMPLE_CSV_ROW}\n"


def _build_zip_bytes(
	csv_content: str,
	txt_filename: str = "trades.txt",
) -> bytes:
	"""Build in-memory ZIP bytes containing a single TXT file.

	Parameters
	----------
	csv_content : str
		The CSV text to embed in the archive.
	txt_filename : str, optional
		The filename for the embedded entry, by default "trades.txt".

	Returns
	-------
	bytes
		Raw ZIP bytes.
	"""
	buf = BytesIO()
	with ZipFile(buf, "w") as zf:
		zf.writestr(txt_filename, csv_content)
	return buf.getvalue()


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_date() -> date:
	"""Fixture providing a sample reference date.

	Returns
	-------
	date
		Sample date object.
	"""
	return date(2026, 4, 17)


@pytest.fixture
def instance(sample_date: date) -> B3BdiStocksTradeByTrade:
	"""Fixture providing a B3BdiStocksTradeByTrade instance.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	B3BdiStocksTradeByTrade
		Initialized instance.
	"""
	return B3BdiStocksTradeByTrade(date_ref=sample_date)


@pytest.fixture
def zip_response() -> Response:
	"""Mock Response whose content is a valid ZIP archive with one data row.

	Returns
	-------
	Response
		Mocked Response object.
	"""
	response = MagicMock(spec=Response)
	response.status_code = 200
	response.content = _build_zip_bytes(_SAMPLE_CSV_CONTENT)
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def empty_zip_response() -> Response:
	"""Mock Response whose content is a ZIP archive with a header-only TXT.

	Returns
	-------
	Response
		Mocked Response object.
	"""
	response = MagicMock(spec=Response)
	response.status_code = 200
	response.content = _build_zip_bytes(_SAMPLE_CSV_HEADER + "\n")
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def sample_stringio() -> StringIO:
	"""Fixture providing a StringIO with one complete trade row.

	Returns
	-------
	StringIO
		In-memory CSV stream ready for transform_data.
	"""
	return StringIO(_SAMPLE_CSV_CONTENT)


@pytest.fixture
def empty_stringio() -> StringIO:
	"""Fixture providing a StringIO with only the CSV header (no data rows).

	Returns
	-------
	StringIO
		In-memory CSV stream with no data rows.
	"""
	return StringIO(_SAMPLE_CSV_HEADER + "\n")


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(sample_date: date) -> None:
	"""Test initialization with all parameters provided.

	Verifies
	--------
	- date_ref and url are set correctly.
	- Standard helpers are initialized.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	inst = B3BdiStocksTradeByTrade(date_ref=sample_date)
	assert inst.date_ref == sample_date
	assert "2026-04-17" in inst.url
	assert "rapinegocios" in inst.url
	assert "type=2" in inst.url
	assert inst.logger is None
	assert isinstance(inst.cls_dir_files_management, DirFilesManagement)
	assert isinstance(inst.cls_dates_br, DatesBRAnbima)
	assert isinstance(inst.cls_create_log, CreateLog)


def test_init_without_date_ref(mocker: MockerFixture) -> None:
	"""Test initialization without date_ref defaults to previous working day.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch.object(DatesBRAnbima, "add_working_days", return_value=date(2026, 4, 16))
	inst = B3BdiStocksTradeByTrade()
	assert inst.date_ref == date(2026, 4, 16)


def test_init_logger_propagated() -> None:
	"""Test that logger is stored on the instance.

	Returns
	-------
	None
	"""
	mock_logger = MagicMock(spec=Logger)
	inst = B3BdiStocksTradeByTrade(date_ref=date(2026, 4, 17), logger=mock_logger)
	assert inst.logger is mock_logger


def test_init_url_contains_date(sample_date: date) -> None:
	"""Test that the URL template is built using the formatted reference date.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	inst = B3BdiStocksTradeByTrade(date_ref=sample_date)
	assert inst.url == "https://drp.b3.com.br/rapinegocios/tickercsv/2026-04-17?type=2"


def test_get_response_success(instance: B3BdiStocksTradeByTrade, mocker: MockerFixture) -> None:
	"""Test get_response issues a GET to the correct URL and returns the response.

	Parameters
	----------
	instance : B3BdiStocksTradeByTrade
		Initialized instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.raise_for_status = MagicMock()
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mock_get = mocker.patch("requests.get", return_value=mock_resp)

	result = instance.get_response()

	mock_get.assert_called_once_with(instance.url, timeout=(12.0, 21.0), verify=True)
	assert result is mock_resp
	mock_resp.raise_for_status.assert_called_once()


def test_get_response_http_error(instance: B3BdiStocksTradeByTrade, mocker: MockerFixture) -> None:
	"""Test get_response raises HTTPError on a bad HTTP status.

	Parameters
	----------
	instance : B3BdiStocksTradeByTrade
		Initialized instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch("requests.get", side_effect=requests.exceptions.HTTPError("500 Server Error"))
	with pytest.raises(requests.exceptions.HTTPError):
		instance.get_response()


def test_get_response_timeout_error(
	instance: B3BdiStocksTradeByTrade, mocker: MockerFixture
) -> None:
	"""Test get_response raises Timeout when the server does not respond.

	Parameters
	----------
	instance : B3BdiStocksTradeByTrade
		Initialized instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch("requests.get", side_effect=requests.exceptions.Timeout("timed out"))
	with pytest.raises(requests.exceptions.Timeout):
		instance.get_response()


def test_get_response_connection_error(
	instance: B3BdiStocksTradeByTrade, mocker: MockerFixture
) -> None:
	"""Test get_response raises ConnectionError when the host is unreachable.

	Parameters
	----------
	instance : B3BdiStocksTradeByTrade
		Initialized instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch(
		"requests.get",
		side_effect=requests.exceptions.ConnectionError("connection refused"),
	)
	with pytest.raises(requests.exceptions.ConnectionError):
		instance.get_response()


@pytest.mark.parametrize(
	"timeout",
	[10, 10.5, (10.0, 20.0), (10, 20)],
)
def test_get_response_timeout_variants(
	instance: B3BdiStocksTradeByTrade,
	mocker: MockerFixture,
	timeout: int | float | tuple,
) -> None:
	"""Test get_response accepts various timeout types.

	Parameters
	----------
	instance : B3BdiStocksTradeByTrade
		Initialized instance.
	mocker : MockerFixture
		Pytest-mock fixture.
	timeout : int | float | tuple
		Various timeout values.

	Returns
	-------
	None
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.raise_for_status = MagicMock()
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mock_get = mocker.patch("requests.get", return_value=mock_resp)

	instance.get_response(timeout=timeout)

	_, kwargs = mock_get.call_args
	assert kwargs["timeout"] == timeout


def test_parse_raw_file_returns_stringio(
	instance: B3BdiStocksTradeByTrade,
	zip_response: Response,
) -> None:
	"""Test parse_raw_file extracts TXT from ZIP and returns a StringIO.

	Parameters
	----------
	instance : B3BdiStocksTradeByTrade
		Initialized instance.
	zip_response : Response
		Mocked Response with valid ZIP content.

	Returns
	-------
	None
	"""
	result = instance.parse_raw_file(zip_response)
	assert isinstance(result, StringIO)
	content = result.read()
	assert "PETR4" in content


def test_parse_raw_file_invalid_zip_raises(
	instance: B3BdiStocksTradeByTrade,
) -> None:
	"""Test parse_raw_file raises an error on invalid ZIP content.

	Parameters
	----------
	instance : B3BdiStocksTradeByTrade
		Initialized instance.

	Returns
	-------
	None
	"""
	bad_response = MagicMock(spec=Response)
	bad_response.content = b"this is not a zip"
	with pytest.raises((BadZipFile, Exception)):
		instance.parse_raw_file(bad_response)


def test_parse_raw_file_no_txt_raises(
	instance: B3BdiStocksTradeByTrade,
) -> None:
	"""Test parse_raw_file raises ValueError when ZIP has no TXT files.

	Parameters
	----------
	instance : B3BdiStocksTradeByTrade
		Initialized instance.

	Returns
	-------
	None
	"""
	buf = BytesIO()
	with ZipFile(buf, "w") as zf:
		zf.writestr("readme.md", "no txt here")
	response = MagicMock(spec=Response)
	response.content = buf.getvalue()
	with pytest.raises(ValueError, match="no .txt files"):
		instance.parse_raw_file(response)


def test_transform_data_normal(
	instance: B3BdiStocksTradeByTrade,
	sample_stringio: StringIO,
) -> None:
	"""Test transform_data produces a DataFrame with the expected UPPER_SNAKE columns.

	Parameters
	----------
	instance : B3BdiStocksTradeByTrade
		Initialized instance.
	sample_stringio : StringIO
		In-memory CSV stream with one data row.

	Returns
	-------
	None
	"""
	df_ = instance.transform_data(file=sample_stringio)
	assert isinstance(df_, pd.DataFrame)
	assert len(df_) == 1
	expected_cols = [
		"DATE_REF",
		"INSTRUMENT_CODE",
		"UPDATE_ACTION",
		"TRADE_PRICE",
		"TRADED_QUANTITY",
		"CLOSING_TIME",
		"TRADE_ID",
		"SESSION_TYPE",
		"TRADE_DATE",
		"BUYER_CODE",
		"SELLER_CODE",
		"CHANNEL_TYPE",
	]
	assert list(df_.columns) == expected_cols
	assert df_["INSTRUMENT_CODE"].iloc[0] == "PETR4"
	assert df_["TRADE_PRICE"].iloc[0] == 38.50
	assert df_["CLOSING_TIME"].iloc[0] == "100000003"
	assert df_["TRADE_ID"].iloc[0] == "10"


def test_transform_data_empty_input(
	instance: B3BdiStocksTradeByTrade,
	empty_stringio: StringIO,
) -> None:
	"""Test transform_data returns an empty DataFrame when no data rows are present.

	Parameters
	----------
	instance : B3BdiStocksTradeByTrade
		Initialized instance.
	empty_stringio : StringIO
		In-memory CSV stream with header only.

	Returns
	-------
	None
	"""
	df_ = instance.transform_data(file=empty_stringio)
	assert isinstance(df_, pd.DataFrame)
	assert df_.empty


def test_transform_data_multiple_rows(instance: B3BdiStocksTradeByTrade) -> None:
	"""Test transform_data handles multiple rows correctly.

	Parameters
	----------
	instance : B3BdiStocksTradeByTrade
		Initialized instance.

	Returns
	-------
	None
	"""
	rows = "\n".join(
		[
			_SAMPLE_CSV_HEADER,
			"2026-04-17;PETR4;0;38,50;100;100000003;10;1;2026-04-17;308;72;1",
			"2026-04-17;VALE3;0;62,20;200;100000005;20;1;2026-04-17;72;308;1",
			"2026-04-17;ITUB4;0;31,10;50;100000007;30;1;2026-04-17;123;456;1",
		]
	)
	df_ = instance.transform_data(file=StringIO(rows))
	assert len(df_) == 3
	assert set(df_["INSTRUMENT_CODE"].tolist()) == {"PETR4", "VALE3", "ITUB4"}


def test_run_without_db(
	instance: B3BdiStocksTradeByTrade,
	zip_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run returns a DataFrame when cls_db is not set.

	Parameters
	----------
	instance : B3BdiStocksTradeByTrade
		Initialized instance.
	zip_response : Response
		Mocked Response with valid ZIP content.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch.object(instance, "get_response", return_value=zip_response)
	mocker.patch.object(instance, "standardize_dataframe", side_effect=lambda df_, **kw: df_)

	result = instance.run()

	assert isinstance(result, pd.DataFrame)
	assert len(result) == 1
	assert "INSTRUMENT_CODE" in result.columns


def test_run_with_db(
	instance: B3BdiStocksTradeByTrade,
	zip_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run inserts into DB and returns None when cls_db is set.

	Parameters
	----------
	instance : B3BdiStocksTradeByTrade
		Initialized instance.
	zip_response : Response
		Mocked Response with valid ZIP content.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	instance.cls_db = MagicMock()
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch.object(instance, "get_response", return_value=zip_response)
	mocker.patch.object(instance, "standardize_dataframe", side_effect=lambda df_, **kw: df_)
	mock_insert = mocker.patch.object(instance, "insert_table_db")

	result = instance.run()

	assert result is None
	mock_insert.assert_called_once()


def test_run_empty_content_returns_none(
	instance: B3BdiStocksTradeByTrade,
	empty_zip_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run returns None when the ZIP yields no data rows.

	Parameters
	----------
	instance : B3BdiStocksTradeByTrade
		Initialized instance.
	empty_zip_response : Response
		Mocked Response with header-only ZIP content.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch.object(instance, "get_response", return_value=empty_zip_response)

	result = instance.run()

	assert result is None


def test_module_reload(sample_date: date) -> None:
	"""Test that the module can be reloaded and the class re-instantiated.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.ingestion.countries.br.exchange.b3_bdi_stocks_trade_by_trade as mod

	importlib.reload(mod)
	inst = mod.B3BdiStocksTradeByTrade(date_ref=sample_date)
	assert inst.date_ref == sample_date
	assert "rapinegocios" in inst.url
