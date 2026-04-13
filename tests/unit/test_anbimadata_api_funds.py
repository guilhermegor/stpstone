"""Unit tests for AnbimaDataFunds — ANBIMA Data API funds client.

Covers funds-catalogue retrieval, single-fund historical data, investor-segment
queries, time-series, bulk financials/registration endpoints, institution lookup,
and explanatory notes — all without hitting real network endpoints.

References
----------
.. [1] https://developers.anbima.com.br/pt/
.. [2] https://developers.anbima.com.br/pt/swagger-de-fundos-v2-rcvm-175/
"""

from typing import Any

import pytest
from pytest_mock import MockerFixture
from requests import exceptions

from stpstone.utils.providers.br.anbimadata_api.anbimadata_api_funds import AnbimaDataFunds


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response_ok(mocker: MockerFixture) -> object:
	"""Fixture providing a mocked successful HTTP response.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks.

	Returns
	-------
	object
		Mocked response with status 200 and ``{"access_token": "test_token"}`` JSON.
	"""
	mock_resp = mocker.Mock()
	mock_resp.status_code = 200
	mock_resp.raise_for_status.return_value = None
	mock_resp.json.return_value = {"access_token": "test_token"}
	return mock_resp


@pytest.fixture
def mock_requests(mocker: MockerFixture) -> object:
	"""Fixture patching ``requests.request`` inside ``anbimadata_api_gen``.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks.

	Returns
	-------
	object
		Patched ``request`` callable.
	"""
	return mocker.patch(
		"stpstone.utils.providers.br.anbimadata_api.anbimadata_api_gen.request"
	)


@pytest.fixture
def anbima_funds(mock_requests: object, mock_response_ok: object) -> AnbimaDataFunds:
	"""Fixture providing an ``AnbimaDataFunds`` instance with mocked HTTP.

	The ``mock_requests`` patch must be active before construction because
	``AnbimaDataFunds.__init__`` calls ``super().__init__`` which calls
	``access_token()``, which issues an HTTP POST for the OAuth token.

	Parameters
	----------
	mock_requests : object
		Patched ``requests.request`` callable — must be active before construction.
	mock_response_ok : object
		Mocked successful HTTP response supplying the OAuth token.

	Returns
	-------
	AnbimaDataFunds
		Initialised ``AnbimaDataFunds`` instance.
	"""
	mock_requests.return_value = mock_response_ok
	return AnbimaDataFunds(
		str_client_id="test_client",
		str_client_secret="test_secret",  # noqa S106
		str_env="dev",
		int_chunk=1000,
	)


@pytest.fixture
def sample_funds_json() -> list[dict[str, Any]]:
	"""Sample paginated funds-catalogue API payload.

	Returns
	-------
	list[dict[str, Any]]
		Minimal valid response structure used by ``funds_raw``.
	"""
	return [
		{
			"content": [
				{
					"fund_code": "123",
					"fund_name": "Test Fund",
					"inception_date": "2023-01-01",
					"update_timestamp": "2023-01-01T12:00:00Z",
					"subclasses": [
						{"subclass_code": "SC1", "subclass_name": "Subclass 1"}
					],
				}
			]
		}
	]


@pytest.fixture
def sample_fund_json() -> dict[str, Any]:
	"""Sample single-fund historical data API payload.

	Returns
	-------
	dict[str, Any]
		Minimal valid response structure used by ``fund_raw``.
	"""
	return {
		"fund_code": "123",
		"fund_name": "Test Fund",
		"classes": [
			{
				"class_code": "C1",
				"historical_data": [
					{
						"classes_historical_data_date": "2023-01-01",
						"classes_historical_data_timestamp": "2023-01-01T12:00:00Z",
						"classes_historical_data_percentual_value": "100.0",
					}
				],
			}
		],
	}


# --------------------------
# Tests
# --------------------------
class TestAnbimaDataFunds:
	"""Test suite for AnbimaDataFunds."""

	def test_funds_raw_success(
		self,
		anbima_funds: AnbimaDataFunds,
		mock_requests: object,
		mock_response_ok: object,
		sample_funds_json: list[dict[str, Any]],
		mocker: MockerFixture,
	) -> None:
		"""``funds_raw`` fetches the correct paginated endpoint and returns raw JSON.

		Parameters
		----------
		anbima_funds : AnbimaDataFunds
			Initialised instance.
		mock_requests : object
			Patched ``requests.request`` callable.
		mock_response_ok : object
			Mocked successful HTTP response.
		sample_funds_json : list[dict[str, Any]]
			Sample paginated catalogue payload.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		mocker.patch.object(
			anbima_funds, "access_token", return_value={"access_token": "test_token"}
		)
		mock_response_ok.json.return_value = sample_funds_json
		mock_requests.return_value = mock_response_ok
		mock_requests.reset_mock()
		result = anbima_funds.funds_raw(int_pg=1)
		assert result == sample_funds_json
		mock_requests.assert_called_once_with(
			method="GET",
			url="https://api-sandbox.anbima.com.br/feed/fundos/v2/fundos?size=1000&page=1",
			headers={
				"accept": "application/json",
				"client_id": "test_client",
				"access_token": "test_token",
			},
			params=None,
			data=None,
			timeout=(200, 200),
		)

	def test_funds_trt_invalid_content_raises(
		self,
		anbima_funds: AnbimaDataFunds,
		mock_requests: object,
		mock_response_ok: object,
		mocker: MockerFixture,
	) -> None:
		"""``funds_trt`` raises when the content contains unexpected types.

		Parameters
		----------
		anbima_funds : AnbimaDataFunds
			Initialised instance.
		mock_requests : object
			Patched ``requests.request`` callable.
		mock_response_ok : object
			Mocked successful HTTP response.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		invalid_json = [{"content": [{"fund_code": 123}]}]
		mock_response_ok.json.return_value = invalid_json
		mock_requests.return_value = mock_response_ok
		mocker.patch.object(anbima_funds, "funds_raw", return_value=invalid_json)
		with pytest.raises(TypeError, match="list indices must be integers or slices"):
			anbima_funds.funds_trt(int_pg=0)

	def test_fund_raw_success(
		self,
		anbima_funds: AnbimaDataFunds,
		mock_requests: object,
		mock_response_ok: object,
		sample_fund_json: dict[str, Any],
		mocker: MockerFixture,
	) -> None:
		"""``fund_raw`` fetches the correct historico endpoint and returns raw JSON.

		Parameters
		----------
		anbima_funds : AnbimaDataFunds
			Initialised instance.
		mock_requests : object
			Patched ``requests.request`` callable.
		mock_response_ok : object
			Mocked successful HTTP response.
		sample_fund_json : dict[str, Any]
			Sample single-fund payload.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		mock_response_ok.json.return_value = sample_fund_json
		mock_requests.return_value = mock_response_ok
		mocker.patch.object(
			anbima_funds, "access_token", return_value={"access_token": "test_token"}
		)
		mock_requests.reset_mock()
		result = anbima_funds.fund_raw(str_code_fnd="123")
		assert result == sample_fund_json
		mock_requests.assert_called_once_with(
			method="GET",
			url="https://api-sandbox.anbima.com.br/feed/fundos/v2/fundos/123/historico",
			headers={
				"accept": "application/json",
				"client_id": "test_client",
				"access_token": "test_token",
			},
			params=None,
			data=None,
			timeout=(200, 200),
		)

	def test_fund_trt_invalid_data_raises(
		self,
		anbima_funds: AnbimaDataFunds,
		mock_requests: object,
		mock_response_ok: object,
		mocker: MockerFixture,
	) -> None:
		"""``fund_trt`` raises ``ValueError`` when the response is not a dict.

		Parameters
		----------
		anbima_funds : AnbimaDataFunds
			Initialised instance.
		mock_requests : object
			Patched ``requests.request`` callable.
		mock_response_ok : object
			Mocked successful HTTP response.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		invalid_json = {"fund_code": "123", "invalid_field": 123}
		mock_response_ok.json.return_value = invalid_json
		mock_requests.return_value = mock_response_ok
		mocker.patch.object(anbima_funds, "fund_raw", return_value=invalid_json)
		with pytest.raises(ValueError, match="Invalid data type for fund"):
			anbima_funds.fund_trt(list_code_fnds=["123"])

	def test_fund_hist_success(
		self,
		anbima_funds: AnbimaDataFunds,
		mock_requests: object,
		mock_response_ok: object,
		mocker: MockerFixture,
	) -> None:
		"""``fund_hist`` fetches the historico endpoint using the class code.

		Parameters
		----------
		anbima_funds : AnbimaDataFunds
			Initialised instance.
		mock_requests : object
			Patched ``requests.request`` callable.
		mock_response_ok : object
			Mocked successful HTTP response.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		mock_response_ok.json.return_value = [{"date": "2023-01-01"}]
		mock_requests.return_value = mock_response_ok
		mocker.patch.object(
			anbima_funds, "access_token", return_value={"access_token": "test_token"}
		)
		mock_requests.reset_mock()
		result = anbima_funds.fund_hist(str_code_class="C1")
		assert result == [{"date": "2023-01-01"}]
		mock_requests.assert_called_once_with(
			method="GET",
			url="https://api-sandbox.anbima.com.br/feed/fundos/v2/fundos/C1/historico",
			headers={
				"accept": "application/json",
				"client_id": "test_client",
				"access_token": "test_token",
			},
			params=None,
			data=None,
			timeout=(200, 200),
		)

	def test_segment_investor_success(
		self,
		anbima_funds: AnbimaDataFunds,
		mock_requests: object,
		mock_response_ok: object,
		mocker: MockerFixture,
	) -> None:
		"""``segment_investor`` fetches the patrimônio-líquido endpoint.

		Parameters
		----------
		anbima_funds : AnbimaDataFunds
			Initialised instance.
		mock_requests : object
			Patched ``requests.request`` callable.
		mock_response_ok : object
			Mocked successful HTTP response.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		mocker.patch.object(
			anbima_funds, "access_token", return_value={"access_token": "test_token"}
		)
		mock_response_ok.json.return_value = [{"segment": "Retail"}]
		mock_requests.return_value = mock_response_ok
		mock_requests.reset_mock()
		result = anbima_funds.segment_investor(str_code_class="C1")
		assert result == [{"segment": "Retail"}]
		mock_requests.assert_called_once_with(
			method="GET",
			url=(
				"https://api-sandbox.anbima.com.br"
				"/feed/fundos/v2/fundos/segmento-investidor/C1/patrimonio-liquido"
			),
			headers={
				"accept": "application/json",
				"client_id": "test_client",
				"access_token": "test_token",
			},
			params=None,
			data=None,
			timeout=(200, 200),
		)

	def test_time_series_fund_success(
		self,
		anbima_funds: AnbimaDataFunds,
		mock_requests: object,
		mock_response_ok: object,
		mocker: MockerFixture,
	) -> None:
		"""``time_series_fund`` passes date range and class code as GET params.

		Parameters
		----------
		anbima_funds : AnbimaDataFunds
			Initialised instance.
		mock_requests : object
			Patched ``requests.request`` callable.
		mock_response_ok : object
			Mocked successful HTTP response.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		mock_response_ok.json.return_value = [{"value": 100.0}]
		mock_requests.return_value = mock_response_ok
		mocker.patch.object(
			anbima_funds, "access_token", return_value={"access_token": "test_token"}
		)
		mock_requests.reset_mock()
		result = anbima_funds.time_series_fund(
			str_date_inf="2023-01-01",
			str_date_sup="2023-12-31",
			str_code_class="C1",
		)
		assert result == [{"value": 100.0}]
		mock_requests.assert_called_once_with(
			method="GET",
			url="https://api-sandbox.anbima.com.br/feed/fundos/v2/fundos/C1/serie-historica",
			headers={
				"accept": "application/json",
				"client_id": "test_client",
				"access_token": "test_token",
			},
			params={
				"size": 1000,
				"data-inicio": "2023-01-01",
				"data-fim": "2023-12-31",
			},
			data=None,
			timeout=(200, 200),
		)

	def test_funds_financials_dt_success(
		self,
		anbima_funds: AnbimaDataFunds,
		mock_requests: object,
		mock_response_ok: object,
		mocker: MockerFixture,
	) -> None:
		"""``funds_financials_dt`` sends the update date as a GET parameter.

		Parameters
		----------
		anbima_funds : AnbimaDataFunds
			Initialised instance.
		mock_requests : object
			Patched ``requests.request`` callable.
		mock_response_ok : object
			Mocked successful HTTP response.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		mocker.patch.object(
			anbima_funds, "access_token", return_value={"access_token": "test_token"}
		)
		mock_response_ok.json.return_value = [{"financials": "data"}]
		mock_requests.return_value = mock_response_ok
		mock_requests.reset_mock()
		result = anbima_funds.funds_financials_dt(str_date_update="2023-01-01")
		assert result == [{"financials": "data"}]
		mock_requests.assert_called_once_with(
			method="GET",
			url=(
				"https://api-sandbox.anbima.com.br"
				"/feed/fundos/v2/fundos/serie-historica/lote"
			),
			headers={
				"accept": "application/json",
				"client_id": "test_client",
				"access_token": "test_token",
			},
			params={"data-atualizacao": "2023-01-01", "size": 1000},
			data=None,
			timeout=(200, 200),
		)

	def test_funds_registration_data_dt_success(
		self,
		anbima_funds: AnbimaDataFunds,
		mock_requests: object,
		mock_response_ok: object,
		mocker: MockerFixture,
	) -> None:
		"""``funds_registration_data_dt`` sends the update date as a GET parameter.

		Parameters
		----------
		anbima_funds : AnbimaDataFunds
			Initialised instance.
		mock_requests : object
			Patched ``requests.request`` callable.
		mock_response_ok : object
			Mocked successful HTTP response.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		mocker.patch.object(
			anbima_funds, "access_token", return_value={"access_token": "test_token"}
		)
		mock_response_ok.json.return_value = [{"registration": "data"}]
		mock_requests.return_value = mock_response_ok
		mock_requests.reset_mock()
		result = anbima_funds.funds_registration_data_dt(str_date_update="2023-01-01")
		assert result == [{"registration": "data"}]
		mock_requests.assert_called_once_with(
			method="GET",
			url=(
				"https://api-sandbox.anbima.com.br"
				"/feed/fundos/v2/fundos/dados-cadastrais/lote"
			),
			headers={
				"accept": "application/json",
				"client_id": "test_client",
				"access_token": "test_token",
			},
			params={"data-atualizacao": "2023-01-01", "size": 1000},
			data=None,
			timeout=(200, 200),
		)

	def test_institutions_success(
		self,
		anbima_funds: AnbimaDataFunds,
		mock_requests: object,
		mock_response_ok: object,
		mocker: MockerFixture,
	) -> None:
		"""``institutions`` fetches the instituicoes endpoint with page-size param.

		Parameters
		----------
		anbima_funds : AnbimaDataFunds
			Initialised instance.
		mock_requests : object
			Patched ``requests.request`` callable.
		mock_response_ok : object
			Mocked successful HTTP response.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		mocker.patch.object(
			anbima_funds, "access_token", return_value={"access_token": "test_token"}
		)
		mock_response_ok.json.return_value = [{"name": "Institution"}]
		mock_requests.return_value = mock_response_ok
		mock_requests.reset_mock()
		result = anbima_funds.institutions()
		assert result == [{"name": "Institution"}]
		mock_requests.assert_called_once_with(
			method="GET",
			url=(
				"https://api-sandbox.anbima.com.br"
				"/feed/fundos/v2/fundos/instituicoes"
			),
			headers={
				"accept": "application/json",
				"client_id": "test_client",
				"access_token": "test_token",
			},
			params={"size": 1000},
			data=None,
			timeout=(200, 200),
		)

	def test_institution_by_ein_success(
		self,
		anbima_funds: AnbimaDataFunds,
		mock_requests: object,
		mock_response_ok: object,
		mocker: MockerFixture,
	) -> None:
		"""``institution`` fetches the EIN-scoped instituicoes endpoint.

		Parameters
		----------
		anbima_funds : AnbimaDataFunds
			Initialised instance.
		mock_requests : object
			Patched ``requests.request`` callable.
		mock_response_ok : object
			Mocked successful HTTP response.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		mocker.patch.object(
			anbima_funds, "access_token", return_value={"access_token": "test_token"}
		)
		mock_response_ok.json.return_value = [{"ein": "123456"}]
		mock_requests.return_value = mock_response_ok
		mock_requests.reset_mock()
		result = anbima_funds.institution(str_ein="123456")
		assert result == [{"ein": "123456"}]
		mock_requests.assert_called_once_with(
			method="GET",
			url=(
				"https://api-sandbox.anbima.com.br"
				"/feed/fundos/v2/fundos/instituicoes/123456"
			),
			headers={
				"accept": "application/json",
				"client_id": "test_client",
				"access_token": "test_token",
			},
			params={"size": 1000},
			data=None,
			timeout=(200, 200),
		)

	def test_explanatory_notes_fund_success(
		self,
		anbima_funds: AnbimaDataFunds,
		mock_requests: object,
		mock_response_ok: object,
		mocker: MockerFixture,
	) -> None:
		"""``explanatory_notes_fund`` fetches the notas-explicativas endpoint.

		Parameters
		----------
		anbima_funds : AnbimaDataFunds
			Initialised instance.
		mock_requests : object
			Patched ``requests.request`` callable.
		mock_response_ok : object
			Mocked successful HTTP response.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		mocker.patch.object(
			anbima_funds, "access_token", return_value={"access_token": "test_token"}
		)
		mock_response_ok.json.return_value = [{"note": "Note"}]
		mock_requests.return_value = mock_response_ok
		mock_requests.reset_mock()
		result = anbima_funds.explanatory_notes_fund(str_code_class="C1")
		assert result == [{"note": "Note"}]
		mock_requests.assert_called_once_with(
			method="GET",
			url=(
				"https://api-sandbox.anbima.com.br"
				"/feed/fundos/v2/fundos/C1/notas-explicativas"
			),
			headers={
				"accept": "application/json",
				"client_id": "test_client",
				"access_token": "test_token",
			},
			params=None,
			data=None,
			timeout=(200, 200),
		)

	def test_generic_request_http_failure_raises(
		self,
		anbima_funds: AnbimaDataFunds,
		mock_requests: object,
		mocker: MockerFixture,
	) -> None:
		"""An HTTP error in any endpoint call is converted to ``ValueError``.

		Parameters
		----------
		anbima_funds : AnbimaDataFunds
			Initialised instance.
		mock_requests : object
			Patched ``requests.request`` callable.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		failing_resp = mocker.Mock()
		failing_resp.raise_for_status.side_effect = exceptions.HTTPError("Error")
		mock_requests.return_value = failing_resp
		with pytest.raises(ValueError, match="API request failed: Error"):
			anbima_funds.segment_investor(str_code_class="C1")
