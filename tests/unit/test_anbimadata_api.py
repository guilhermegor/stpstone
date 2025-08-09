"""Unit tests for ANBIMA Data API client.

Tests the ANBIMA Data API client functionality, including initialization,
data retrieval, and processing, without requiring real API credentials.
Covers normal operations, edge cases, error conditions, and type validation.

References
----------
.. [1] https://developers.anbima.com.br/pt/
.. [2] https://developers.anbima.com.br/pt/swagger-de-fundos-v2-rcvm-175/#/Notas%20explicativas/buscarNotasExplicativas
"""

from datetime import datetime
import importlib
import sys
from typing import Any

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import exceptions

from stpstone._config.global_slots import YAML_ANBIMA_DATA_API
from stpstone.utils.parsers.json import JsonFiles
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.providers.br.anbimadata_api import AnbimaDataFunds, AnbimaDataGen


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response_ok(mocker: MockerFixture) -> object:
    """Mock requests response with successful status.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    object
        Mocked response object with status code 200
    """
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"access_token": "test_token"}
    return mock_response


@pytest.fixture
def mock_requests(mocker: MockerFixture) -> object:
    """Mock requests.request function.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    object
        Mocked requests.request function
    """
    return mocker.patch("stpstone.utils.providers.br.anbimadata_api.request")


@pytest.fixture
def anbima_gen(mock_requests: object, mock_response_ok: object) -> AnbimaDataGen:
    """Fixture providing AnbimaDataGen instance.

    Parameters
    ----------
    mock_requests : object
        Mocked requests.request function
    mock_response_ok : object
        Mocked successful response object

    Returns
    -------
    AnbimaDataGen
        Initialized AnbimaDataGen instance
    """
    mock_requests.return_value = mock_response_ok
    return AnbimaDataGen(
        str_client_id="test_client",
        str_client_secret="test_secret", # noqa S106: possible hardcoded password
        str_env="dev",
        int_chunk=1000,
    )


@pytest.fixture
def anbima_funds(anbima_gen: AnbimaDataGen) -> AnbimaDataFunds:
    """Fixture providing AnbimaDataFunds instance.

    Parameters
    ----------
    anbima_gen : AnbimaDataGen
        Initialized AnbimaDataGen instance

    Returns
    -------
    AnbimaDataFunds
        Initialized AnbimaDataFunds instance
    """
    return AnbimaDataFunds(
        str_client_id="test_client",
        str_client_secret="test_secret", # noqa S106: possible hardcoded password
        str_env="dev",
        int_chunk=1000,
    )


@pytest.fixture
def sample_funds_json() -> list[dict[str, str]]:
    """Fixture providing sample funds JSON data.

    Returns
    -------
    list[dict[str, str]]
        Sample JSON data for funds
    """
    return [
        {
            YAML_ANBIMA_DATA_API["key_content"]: [
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
    """Fixture providing sample fund JSON data.

    Returns
    -------
    dict[str, Any]
        Sample JSON data for a single fund
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
# Tests for AnbimaDataGen
# --------------------------
class TestAnbimaDataGen:
    """Test cases for AnbimaDataGen class."""

    def test_init_valid_inputs(self, mocker: MockerFixture) -> None:
        """Test initialization with valid inputs.

        Verifies
        --------
        - Instance is created with valid credentials and environment
        - Attributes are set correctly
        - Token is retrieved

        Parameters
        ----------
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mocker.patch(
            "stpstone.utils.providers.br.anbimadata_api.AnbimaDataGen.access_token",
            return_value={"access_token": "test_token"}
        )
        gen = AnbimaDataGen(
            str_client_id="client",
            str_client_secret="secret", # noqa S106: possible hardcoded password
            str_env="dev",
            int_chunk=500,
        )
        assert gen.str_client_id == "client"
        assert gen.str_client_secret == "secret" # noqa S106: possible hardcoded password
        assert gen.str_host == "https://api-sandbox.anbima.com.br/"
        assert gen.int_chunk == 500
        assert gen.str_token == "test_token" # noqa S106: possible hardcoded password
    
    @pytest.mark.parametrize(
        "client_id,client_secret,env,chunk,expected_error,match",
        [
            (123, "secret", "dev", 1000, TypeError, "must be of type"),
            ("client", 123, "dev", 1000, TypeError, "must be of type"),
            ("client", "secret", "invalid", 1000, TypeError, "must be one of"),
            ("client", "secret", "dev", -1, ValueError, "Failed to retrieve access token"),
            ("", "secret", "dev", 1000, ValueError, "cannot be empty"),
            ("client", "", "dev", 1000, ValueError, "cannot be empty"),
        ],
    )
    def test_init_invalid_inputs(
        self,
        client_id: str,
        client_secret: str,
        env: str,
        chunk: int,
        expected_error: type,
        match: str,
        mocker: MockerFixture,
    ) -> None:
        """Test initialization with invalid inputs.

        Verifies
        --------
        - Appropriate errors are raised for invalid inputs
        - Error messages match expected patterns

        Parameters
        ----------
        client_id : str
            Client ID to test
        client_secret : str
            Client secret to test
        env : str
            Environment to test
        chunk : int
            Chunk size to test
        expected_error : type
            Expected exception type
        match : str
            Expected error message pattern
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mocker.patch(
            "stpstone.utils.providers.br.anbimadata_api.AnbimaDataGen.access_token",
            side_effect=ValueError("Failed to retrieve access token")
        )
        with pytest.raises(expected_error, match=match):
            AnbimaDataGen(
                str_client_id=client_id,
                str_client_secret=client_secret,
                str_env=env,
                int_chunk=chunk,
            )

    def test_access_token_success(
        self, anbima_gen: AnbimaDataGen, mock_requests: object, mock_response_ok: object
    ) -> None:
        """Test successful access token retrieval.

        Verifies
        --------
        - Token is retrieved successfully
        - Request is made with correct parameters

        Parameters
        ----------
        anbima_gen : AnbimaDataGen
            Initialized AnbimaDataGen instance
        mock_requests : object
            Mocked requests.request function
        mock_response_ok : object
            Mocked successful response object

        Returns
        -------
        None
        """
        mock_requests.return_value = mock_response_ok
        result = anbima_gen.access_token()
        assert result == {"access_token": "test_token"}
        mock_requests.assert_called_once_with(
            method="POST",
            url="https://api.anbima.com.br/oauth/access-token",
            headers={
                "Content-Type": "application/json",
                "Authorization": StrHandler().base64_encode("test_client", "test_secret"),
            },
            data=JsonFiles().dict_to_json({"grant_type": "client_credentials"}),
            timeout=(200, 200),
        )

    def test_access_token_failure(
        self, 
        anbima_gen: AnbimaDataGen, 
        mock_requests: object, 
        mocker: MockerFixture
    ) -> None:
        """Test access token retrieval failure.

        Verifies
        --------
        - ValueError is raised on request failure
        - Error message is correct

        Parameters
        ----------
        anbima_gen : AnbimaDataGen
            Initialized AnbimaDataGen instance
        mock_requests : object
            Mocked requests.request function
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mock_response = mocker.Mock()
        mock_response.raise_for_status.side_effect = exceptions.HTTPError("HTTP error occurred")
        mock_response.status_code = 500
        mock_requests.return_value = mock_response
        with pytest.raises(ValueError, match="Failed to retrieve access token"):
            anbima_gen.access_token()

    def test_generic_request_success(
        self, 
        anbima_gen: AnbimaDataGen, 
        mock_requests: object, 
        mock_response_ok: object, 
        mocker: MockerFixture
    ) -> None:
        """Test successful generic API request.

        Verifies
        --------
        - Request is made successfully
        - Correct response is returned

        Parameters
        ----------
        anbima_gen : AnbimaDataGen
            Initialized AnbimaDataGen instance
        mock_requests : object
            Mocked requests.request function
        mock_response_ok : object
            Mocked successful response object
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mocker.patch.object(anbima_gen, "access_token", 
                            return_value={"access_token": "test_token"})
        mock_response_ok.json.return_value = [{"key": "value"}]
        mock_requests.return_value = mock_response_ok
        mock_requests.reset_mock()  # reset to clear any calls from initialization
        result = anbima_gen.generic_request(str_app="test_endpoint", str_method="GET")
        assert result == [{"key": "value"}]
        mock_requests.assert_called_once_with(
            method="GET",
            url="https://api-sandbox.anbima.com.br/test_endpoint",
            headers={
                "accept": "application/json",
                "client_id": "test_client",
                "access_token": "test_token",
            },
            params=None,
            data=None,
            timeout=(200, 200),
        )

    def test_generic_request_invalid_method(
        self, anbima_gen: AnbimaDataGen
    ) -> None:
        """Test generic request with invalid method.

        Verifies
        --------
        - TypeError is raised for invalid HTTP method
        - Error message is correct

        Parameters
        ----------
        anbima_gen : AnbimaDataGen
            Initialized AnbimaDataGen instance

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="str_method must be one of"):
            anbima_gen.generic_request(str_app="test_endpoint", str_method="PUT")

    def test_generic_request_failure(
        self, anbima_gen: AnbimaDataGen, mock_requests: object, mocker: MockerFixture
    ) -> None:
        """Test generic request failure.

        Verifies
        --------
        - ValueError is raised on request failure
        - Error message includes underlying error

        Parameters
        ----------
        anbima_gen : AnbimaDataGen
            Initialized AnbimaDataGen instance
        mock_requests : object
            Mocked requests.request function
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mock_response = mocker.Mock()
        mock_response.raise_for_status.side_effect = exceptions.HTTPError("Error")
        mock_requests.return_value = mock_response
        with pytest.raises(ValueError, match="API request failed: Error"):
            anbima_gen.generic_request(str_app="test_endpoint", str_method="GET")


# --------------------------
# Tests for AnbimaDataFunds
# --------------------------
class TestAnbimaDataFunds:
    """Test cases for AnbimaDataFunds class."""

    def test_funds_raw_success(
        self, 
        anbima_funds: AnbimaDataFunds, 
        mock_requests: object, 
        mock_response_ok: object, 
        sample_funds_json: list[dict[str, str]], 
        mocker: MockerFixture
    ) -> None:
        """Test successful funds_raw method.

        Verifies
        --------
        - Funds data is retrieved successfully
        - Correct endpoint and parameters are used

        Parameters
        ----------
        anbima_funds : AnbimaDataFunds
            Initialized AnbimaDataFunds instance
        mock_requests : object
            Mocked requests.request function
        mock_response_ok : object
            Mocked successful response object
        sample_funds_json : list[dict[str, str]]
            Sample funds data
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mocker.patch.object(anbima_funds, "access_token", 
                            return_value={"access_token": "test_token"})
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

    def test_funds_trt_success(
        self,
        anbima_funds: AnbimaDataFunds,
        mock_requests: object,
        mock_response_ok: object,
        sample_funds_json: list[dict[str, str]],
        mocker: MockerFixture,
    ) -> None:
        """Test successful funds_trt method.

        Verifies
        --------
        - Funds data is processed into a DataFrame
        - DataFrame has correct columns and types
        - Pagination is handled correctly

        Parameters
        ----------
        anbima_funds : AnbimaDataFunds
            Initialized AnbimaDataFunds instance
        mock_requests : object
            Mocked requests.request function
        mock_response_ok : object
            Mocked successful response object
        sample_funds_json : list[dict[str, str]]
            Sample funds JSON data
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mocker.patch.object(anbima_funds, "access_token", 
                            return_value={"access_token": "test_token"})
        mock_response_ok.json.return_value = sample_funds_json
        mock_requests.return_value = mock_response_ok
        mocker.patch.object(anbima_funds, "funds_raw", side_effect=[sample_funds_json, []])
        mock_requests.reset_mock()
        df_ = anbima_funds.funds_trt(int_pg=0)
        assert isinstance(df_, pd.DataFrame)
        assert YAML_ANBIMA_DATA_API["col_fund_code"] in df_.columns
        assert df_[YAML_ANBIMA_DATA_API["col_fund_code"]].dtype == "object"
        assert isinstance(df_[YAML_ANBIMA_DATA_API["col_incpt_dt"]].iloc[0], datetime)
    
    def test_funds_trt_invalid_content(
        self,
        anbima_funds: AnbimaDataFunds,
        mock_requests: object,
        mock_response_ok: object,
        mocker: MockerFixture,
    ) -> None:
        """Test funds_trt with invalid content.

        Verifies
        --------
        - ValueError is raised for invalid content
        - Error message is correct

        Parameters
        ----------
        anbima_funds : AnbimaDataFunds
            Initialized AnbimaDataFunds instance
        mock_requests : object
            Mocked requests.request function
        mock_response_ok : object
            Mocked successful response object
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        invalid_json = [
            {
                YAML_ANBIMA_DATA_API["key_content"]: [
                    {"fund_code": 123}  # invalid type
                ]
            }
        ]
        mock_response_ok.json.return_value = invalid_json
        mock_requests.return_value = mock_response_ok
        mocker.patch.object(anbima_funds, "funds_raw", return_value=invalid_json)
        with pytest.raises(TypeError, match="list indices must be integers or slices"):
            anbima_funds.funds_trt(int_pg=0)

    def test_process_funds_dataframe(
        self, anbima_funds: AnbimaDataFunds
    ) -> None:
        """Test _process_funds_dataframe method.

        Verifies
        --------
        - DataFrame is processed correctly
        - Dates are converted to datetime
        - Strings are handled properly
        - Duplicates are removed

        Parameters
        ----------
        anbima_funds : AnbimaDataFunds
            Initialized AnbimaDataFunds instance

        Returns
        -------
        None
        """
        df_ = pd.DataFrame({
            YAML_ANBIMA_DATA_API["col_fund_code"]: ["123", "123"],
            YAML_ANBIMA_DATA_API["col_incpt_dt"]: ["2023-01-01", "2023-01-01"],
            YAML_ANBIMA_DATA_API["col_update_ts"]: ["2023-01-01T12:00:00Z", None],
            YAML_ANBIMA_DATA_API["col_fund_closure_dt"]: [None, None],
            YAML_ANBIMA_DATA_API["col_eff_dt"]: [None, None],
            YAML_ANBIMA_DATA_API["col_closure_dt"]: [None, None],
            YAML_ANBIMA_DATA_API["col_sbc_incpt_dt"]: [None, None],
            YAML_ANBIMA_DATA_API["col_sbc_closure_dt"]: [None, None],
            YAML_ANBIMA_DATA_API["col_sbc_eff_dt"]: [None, None],
            YAML_ANBIMA_DATA_API["col_sbc_update_dt"]: [None, None],
            YAML_ANBIMA_DATA_API["col_type_id"]: [None, None],
            YAML_ANBIMA_DATA_API["col_fund_id"]: [None, None],
            YAML_ANBIMA_DATA_API["col_comp_name"]: [None, None],
            YAML_ANBIMA_DATA_API["col_trade_name"]: [None, None],
            YAML_ANBIMA_DATA_API["col_fund_type"]: [None, None],
            YAML_ANBIMA_DATA_API["col_class_code"]: [None, None],
            YAML_ANBIMA_DATA_API["col_class_id_type"]: [None, None],
            YAML_ANBIMA_DATA_API["col_class_id"]: [None, None],
            YAML_ANBIMA_DATA_API["col_comp_class"]: [None, None],
            YAML_ANBIMA_DATA_API["col_trd_class"]: [None, None],
            YAML_ANBIMA_DATA_API["col_n1_ctg"]: [None, None],
            YAML_ANBIMA_DATA_API["col_subclasses"]: [None, None],
        })
        result = anbima_funds._process_funds_dataframe(df_)
        assert len(result) == 1
        assert isinstance(result[YAML_ANBIMA_DATA_API["col_incpt_dt"]].iloc[0], datetime)
        assert result[YAML_ANBIMA_DATA_API["col_update_ts"]].iloc[0] == datetime(2023, 1, 1)

    def test_fund_raw_success(
        self, 
        anbima_funds: AnbimaDataFunds, 
        mock_requests: object, 
        mock_response_ok: object, 
        sample_fund_json: dict[str, Any], 
        mocker: MockerFixture
    ) -> None:
        """Test successful fund_raw method.

        Verifies
        --------
        - Fund data is retrieved successfully
        - Correct endpoint is used

        Parameters
        ----------
        anbima_funds : AnbimaDataFunds
            Initialized AnbimaDataFunds instance
        mock_requests : object
            Mocked requests.request function
        mock_response_ok : object
            Mocked successful response object
        sample_fund_json : dict[str, Any]
            Sample fund data
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mock_response_ok.json.return_value = sample_fund_json
        mock_requests.return_value = mock_response_ok
        mocker.patch.object(anbima_funds, "access_token", 
                            return_value={"access_token": "test_token"})
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

    def test_fund_trt_success(
        self,
        anbima_funds: AnbimaDataFunds,
        mock_requests: object,
        mock_response_ok: object,
        sample_fund_json: dict[str, Any],
        mocker: MockerFixture,
    ) -> None:
        """Test successful fund_trt method.

        Verifies
        --------
        - Fund data is processed into DataFrames
        - Correct structure is returned

        Parameters
        ----------
        anbima_funds : AnbimaDataFunds
            Initialized AnbimaDataFunds instance
        mock_requests : object
            Mocked requests.request function
        mock_response_ok : object
            Mocked successful response object
        sample_fund_json : dict[str, Any]
            Sample fund JSON data
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mock_response_ok.json.return_value = sample_fund_json
        mock_requests.return_value = mock_response_ok
        mocker.patch.object(anbima_funds, "access_token", 
                            return_value={"access_token": "test_token"})
        mocker.patch.object(anbima_funds, "fund_raw", return_value=sample_fund_json)
        result = anbima_funds.fund_trt(list_code_fnds=["123"])
        assert "123" in result
        assert isinstance(result["123"], list)
        assert isinstance(result["123"][0], pd.DataFrame)
        assert all(col in result["123"][0].columns for col in [
            "classes_historical_data_date",
            "classes_historical_data_timestamp",
            "classes_historical_data_percentual_value",
            "fund_code"
        ])

    def test_fund_trt_invalid_data(
        self,
        anbima_funds: AnbimaDataFunds,
        mock_requests: object,
        mock_response_ok: object,
        mocker: MockerFixture,
    ) -> None:
        """Test fund_trt with invalid data type.

        Verifies
        --------
        - ValueError is raised for invalid data type
        - Error message is correct

        Parameters
        ----------
        anbima_funds : AnbimaDataFunds
            Initialized AnbimaDataFunds instance
        mock_requests : object
            Mocked requests.request function
        mock_response_ok : object
            Mocked successful response object
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        invalid_json = {"fund_code": "123", "invalid_field": 123}
        mock_response_ok.json.return_value = invalid_json
        mock_requests.return_value = mock_response_ok
        mocker.patch.object(anbima_funds, "fund_raw", return_value=invalid_json)
        with pytest.raises(ValueError, match="Missing expected columns in DataFrame"):
            anbima_funds.fund_trt(list_code_fnds=["123"])

    def test_process_fund_dataframe(
        self, anbima_funds: AnbimaDataFunds
    ) -> None:
        """Test _process_fund_dataframe method.

        Verifies
        --------
        - DataFrame is processed correctly
        - Dates, timestamps, and percentages are handled properly

        Parameters
        ----------
        anbima_funds : AnbimaDataFunds
            Initialized AnbimaDataFunds instance

        Returns
        -------
        None
        """
        df_ = pd.DataFrame({
            "classes_historical_data_date": ["2023-01-01", None],
            "classes_historical_data_timestamp": ["2023-01-01T12:00:00Z", None],
            "classes_historical_data_percentual_value": ["10.5", None],
            "fund_code": ["123", "123"],
        })
        result = anbima_funds._process_fund_dataframe(df_)
        assert isinstance(result["classes_historical_data_date"].iloc[0], datetime)
        assert result["classes_historical_data_percentual_value"].iloc[0] == 10.5
        assert result["fund_code"].dtype == "object"

    def test_fund_hist_success(
        self, 
        anbima_funds: AnbimaDataFunds, 
        mock_requests: object, 
        mock_response_ok: object, 
        mocker: MockerFixture
    ) -> None:
        """Test successful fund_hist method.

        Verifies
        --------
        - Historical data is retrieved successfully
        - Correct endpoint is used

        Parameters
        ----------
        anbima_funds : AnbimaDataFunds
            Initialized AnbimaDataFunds instance
        mock_requests : object
            Mocked requests.request function
        mock_response_ok : object
            Mocked successful response object
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mock_response_ok.json.return_value = [{"date": "2023-01-01"}]
        mock_requests.return_value = mock_response_ok
        mocker.patch.object(anbima_funds, "access_token", 
                            return_value={"access_token": "test_token"})
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
        mocker: MockerFixture
    ) -> None:
        """Test successful segment_investor method.

        Verifies
        --------
        - Investor segment data is retrieved successfully
        - Correct endpoint is used

        Parameters
        ----------
        anbima_funds : AnbimaDataFunds
            Initialized AnbimaDataFunds instance
        mock_requests : object
            Mocked requests.request function
        mock_response_ok : object
            Mocked successful response object
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mocker.patch.object(anbima_funds, "access_token", return_value={"access_token": 
                                                                        "test_token"})
        mock_response_ok.json.return_value = [{"segment": "Retail"}]
        mock_requests.return_value = mock_response_ok
        mock_requests.reset_mock()
        result = anbima_funds.segment_investor(str_code_class="C1")
        assert result == [{"segment": "Retail"}]
        mock_requests.assert_called_once_with(
            method="GET",
            url="https://api-sandbox.anbima.com.br/feed/fundos/v2/fundos/segmento-investidor/C1/patrimonio-liquido",
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
        mocker: MockerFixture
    ) -> None:
        """Test successful time_series_fund method.

        Verifies
        --------
        - Time series data is retrieved successfully
        - Correct payload is sent

        Parameters
        ----------
        anbima_funds : AnbimaDataFunds
            Initialized AnbimaDataFunds instance
        mock_requests : object
            Mocked requests.request function
        mock_response_ok : object
            Mocked successful response object
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mock_response_ok.json.return_value = [{"value": 100.0}]
        mock_requests.return_value = mock_response_ok
        mocker.patch.object(anbima_funds, "access_token", 
                            return_value={"access_token": "test_token"})
        mock_requests.reset_mock()
        result = anbima_funds.time_series_fund(str_date_inf="2023-01-01", 
                                               str_date_sup="2023-12-31", str_code_class="C1")
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
        mocker: MockerFixture
    ) -> None:
        """Test successful funds_financials_dt method.

        Verifies
        --------
        - Financials data is retrieved successfully
        - Correct payload is sent

        Parameters
        ----------
        anbima_funds : AnbimaDataFunds
            Initialized AnbimaDataFunds instance
        mock_requests : object
            Mocked requests.request function
        mock_response_ok : object
            Mocked successful response object
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mocker.patch.object(anbima_funds, "access_token", 
                            return_value={"access_token": "test_token"})
        mock_response_ok.json.return_value = [{"financials": "data"}]
        mock_requests.return_value = mock_response_ok
        mock_requests.reset_mock()
        result = anbima_funds.funds_financials_dt(str_date_update="2023-01-01")
        assert result == [{"financials": "data"}]
        mock_requests.assert_called_once_with(
            method="GET",
            url="https://api-sandbox.anbima.com.br/feed/fundos/v2/fundos/serie-historica/lote",
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
        mocker: MockerFixture
    ) -> None:
        """Test successful funds_registration_data_dt method.

        Verifies
        --------
        - Registration data is retrieved successfully
        - Correct payload is sent

        Parameters
        ----------
        anbima_funds : AnbimaDataFunds
            Initialized AnbimaDataFunds instance
        mock_requests : object
            Mocked requests.request function
        mock_response_ok : object
            Mocked successful response object
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mocker.patch.object(anbima_funds, "access_token", 
                            return_value={"access_token": "test_token"})
        mock_response_ok.json.return_value = [{"registration": "data"}]
        mock_requests.return_value = mock_response_ok
        mock_requests.reset_mock()
        result = anbima_funds.funds_registration_data_dt(str_date_update="2023-01-01")
        assert result == [{"registration": "data"}]
        mock_requests.assert_called_once_with(
            method="GET",
            url="https://api-sandbox.anbima.com.br/feed/fundos/v2/fundos/dados-cadastrais/lote",
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
        mocker: MockerFixture
    ) -> None:
        """Test successful institutions method.

        Verifies
        --------
        - Institutions data is retrieved successfully
        - Correct endpoint is used

        Parameters
        ----------
        anbima_funds : AnbimaDataFunds
            Initialized AnbimaDataFunds instance
        mock_requests : object
            Mocked requests.request function
        mock_response_ok : object
            Mocked successful response object
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mocker.patch.object(anbima_funds, "access_token", 
                            return_value={"access_token": "test_token"})
        mock_response_ok.json.return_value = [{"name": "Institution"}]
        mock_requests.return_value = mock_response_ok
        mock_requests.reset_mock()
        result = anbima_funds.institutions()
        assert result == [{"name": "Institution"}]
        mock_requests.assert_called_once_with(
            method="GET",
            url="https://api-sandbox.anbima.com.br/feed/fundos/v2/fundos/instituicoes",
            headers={
                "accept": "application/json",
                "client_id": "test_client",
                "access_token": "test_token",
            },
            params={"size": 1000},
            data=None,
            timeout=(200, 200),
        )

    def test_institution_success(
        self,
        anbima_funds: AnbimaDataFunds,
        mock_requests: object,
        mock_response_ok: object,
        mocker: MockerFixture
    ) -> None:
        """Test successful institution method.

        Verifies
        --------
        - Specific institution data is retrieved successfully
        - Correct endpoint is used

        Parameters
        ----------
        anbima_funds : AnbimaDataFunds
            Initialized AnbimaDataFunds instance
        mock_requests : object
            Mocked requests.request function
        mock_response_ok : object
            Mocked successful response object
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mocker.patch.object(anbima_funds, "access_token", 
                            return_value={"access_token": "test_token"})
        mock_response_ok.json.return_value = [{"ein": "123456"}]
        mock_requests.return_value = mock_response_ok
        mock_requests.reset_mock()
        result = anbima_funds.institution(str_ein="123456")
        assert result == [{"ein": "123456"}]
        mock_requests.assert_called_once_with(
            method="GET",
            url="https://api-sandbox.anbima.com.br/feed/fundos/v2/fundos/instituicoes/123456",
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
        mocker: MockerFixture
    ) -> None:
        """Test successful explanatory_notes_fund method.

        Verifies
        --------
        - Explanatory notes are retrieved successfully
        - Correct endpoint is used

        Parameters
        ----------
        anbima_funds : AnbimaDataFunds
            Initialized AnbimaDataFunds instance
        mock_requests : object
            Mocked requests.request function
        mock_response_ok : object
            Mocked successful response object
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mocker.patch.object(anbima_funds, "access_token", 
                            return_value={"access_token": "test_token"})
        mock_response_ok.json.return_value = [{"note": "Note"}]
        mock_requests.return_value = mock_response_ok
        mock_requests.reset_mock()
        result = anbima_funds.explanatory_notes_fund(str_code_class="C1")
        assert result == [{"note": "Note"}]
        mock_requests.assert_called_once_with(
            method="GET",
            url="https://api-sandbox.anbima.com.br/feed/fundos/v2/fundos/C1/notas-explicativas",
            headers={
                "accept": "application/json",
                "client_id": "test_client",
                "access_token": "test_token",
            },
            params=None,
            data=None,
            timeout=(200, 200),
        )

    def test_module_reload(self, anbima_funds: AnbimaDataFunds) -> None:
        """Test module reload behavior.

        Verifies
        --------
        - Module can be reloaded without errors
        - Instance attributes are preserved

        Parameters
        ----------
        anbima_funds : AnbimaDataFunds
            Initialized AnbimaDataFunds instance

        Returns
        -------
        None
        """
        original_token = anbima_funds.str_token
        importlib.reload(sys.modules["stpstone.utils.providers.br.anbimadata_api"])
        assert anbima_funds.str_token == original_token