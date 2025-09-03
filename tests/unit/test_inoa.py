"""Unit tests for AlphaTools API client.

Tests the functionality of the AlphaTools class including:
- Initialization and parameter validation
- API request handling with retry logic
- Funds data retrieval
- Quotes data retrieval
- Error handling and edge cases
"""

from datetime import datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from requests import Response, exceptions

from stpstone.utils.providers.br.inoa import AlphaTools


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def valid_alpha_tools_params() -> dict[str, Any]:
    """Fixture providing valid initialization parameters for AlphaTools.

    Returns
    -------
    dict[str, Any]
        Dictionary containing valid initialization parameters
    """
    return {
        "str_user": "test_user",
        "str_passw": "test_pass",
        "str_host": "https://test.host/",
        "str_instance": "test_instance",
        "date_start": datetime(2023, 1, 1),
        "date_end": datetime(2023, 1, 31),
        "str_fmt_date_output": "YYYY-MM-DD",
    }


@pytest.fixture
def mock_dates_br() -> MagicMock:
    """Fixture providing mocked DatesBRAnbima class.

    Returns
    -------
    MagicMock
        Mocked DatesBRAnbima class
    """
    with patch("stpstone.utils.calendars.calendar_abc.ABCCalendarOperations") as mock:
        mock_instance = mock.return_value
        mock_instance.str_date_to_datetime.return_value = datetime(2023, 1, 1)
        yield mock_instance


@pytest.fixture
def mock_response() -> MagicMock:
    """Fixture providing mocked API response.

    Returns
    -------
    MagicMock
        Mocked Response object
    """
    response = MagicMock(spec=Response)
    response.raise_for_status.return_value = None
    return response


@pytest.fixture
def alpha_tools(
    valid_alpha_tools_params: dict[str, Any]
) -> Any: # noqa ANN401: typing.Any is not allowed
    """Fixture providing initialized AlphaTools instance.

    Parameters
    ----------
    valid_alpha_tools_params : dict[str, Any]
        Valid initialization parameters

    Returns
    -------
    Any
        Initialized AlphaTools instance
    """
    return AlphaTools(**valid_alpha_tools_params)


# --------------------------
# Test Classes
# --------------------------
class TestInit:
    """Tests for AlphaTools initialization and validation."""

    def test_init_with_valid_params(
        self, valid_alpha_tools_params: dict[str, Any]
    ) -> None:
        """Test initialization with valid parameters.

        Verifies
        --------
        - Instance is created without errors
        - Attributes are correctly set

        Parameters
        ----------
        valid_alpha_tools_params : dict[str, Any]
            Valid initialization parameters

        Returns
        -------
        None
        """
        alpha = AlphaTools(**valid_alpha_tools_params)
        assert alpha.str_user == "test_user"
        assert alpha.str_passw == "test_pass"
        assert alpha.str_host == "https://test.host/"
        assert alpha.str_instance == "test_instance"
        assert alpha.date_start == datetime(2023, 1, 1)
        assert alpha.date_end == datetime(2023, 1, 31)
        assert alpha.str_fmt_date_output == "YYYY-MM-DD"

    @pytest.mark.parametrize(
        "param_name,param_value,error_msg,error_raise",
        [
            ("str_user", "", "cannot be empty", ValueError),
            ("str_passw", 123, "must be of type", TypeError),
            ("str_host", None, "must be of type", TypeError),
            ("str_instance", "", "cannot be empty", ValueError),
        ],
    )
    def test_init_invalid_string_params(
        self,
        valid_alpha_tools_params: dict[str, Any],
        param_name: str,
        param_value: Any, # noqa ANN401: typing.Any is not allowed
        error_msg: str,
        error_raise: Any, # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test initialization with invalid string parameters.

        Verifies
        --------
        - ValueError is raised with correct message

        Parameters
        ----------
        valid_alpha_tools_params : dict[str, Any]
            Base valid parameters
        param_name : str
            Parameter name to modify
        param_value : Any
            Invalid value to test
        error_msg : str
            Expected error message
        error_raise : Any
            Expected error type

        Returns
        -------
        None
        """
        params = valid_alpha_tools_params.copy()
        params[param_name] = param_value
        with pytest.raises(error_raise, match=error_msg):
            AlphaTools(**params)

    def test_init_invalid_dates(self, valid_alpha_tools_params: dict[str, Any]) -> None:
        """Test initialization with invalid date range.

        Parameters
        ----------
        valid_alpha_tools_params : dict[str, Any]
            Base valid parameters

        Verifies
        --------
        - ValueError is raised when start date > end date

        Parameters
        ----------
        valid_alpha_tools_params : dict[str, Any]
            Base valid parameters

        Returns
        -------
        None
        """
        params = valid_alpha_tools_params.copy()
        params["date_start"], params["date_end"] = params["date_end"], params["date_start"]
        with pytest.raises(ValueError, match="Start date cannot be after end date"):
            AlphaTools(**params)

    def test_init_non_datetime(self, valid_alpha_tools_params: dict[str, Any]) -> None:
        """Test initialization with non-datetime dates.

        Parameters
        ----------
        valid_alpha_tools_params : dict[str, Any]
            Base valid parameters

        Verifies
        --------
        - ValueError is raised when dates aren't datetime objects

        Parameters
        ----------
        valid_alpha_tools_params : dict[str, Any]
            Base valid parameters

        Returns
        -------
        None
        """
        params = valid_alpha_tools_params.copy()
        params["date_start"] = "2023-01-01"
        with pytest.raises(TypeError, match="must be of type"):
            AlphaTools(**params)


class TestGenericReq:
    """Tests for generic_req method."""

    def test_successful_request(
        self, 
        alpha_tools: Any, # noqa ANN401: typing.Any is not allowed
        mock_response: MagicMock
    ) -> None:
        """Test successful API request.

        Parameters
        ----------
        alpha_tools : Any
            AlphaTools instance
        mock_response : MagicMock
            Mocked response object

        Verifies
        --------
        - Request is made with correct parameters
        - Response is returned as expected

        Parameters
        ----------
        alpha_tools : Any
            AlphaTools instance
        mock_response : MagicMock
            Mocked response object

        Returns
        -------
        None
        """
        mock_response.json.return_value = [{"test": "data"}]
        with patch("stpstone.utils.providers.br.inoa.request", return_value=mock_response) \
            as mock_request:
            result = alpha_tools.generic_req("POST", "test_endpoint", {"param": "value"})
            assert result == [{"test": "data"}]
            mock_request.assert_called_once()

    def test_request_retry_on_failure(
        self, 
        alpha_tools: Any, # noqa ANN401: typing.Any is not allowed
        mock_response: MagicMock
    ) -> None:
        """Test request retry on failure.

        Parameters
        ----------
        alpha_tools : Any
            AlphaTools instance
        mock_response : MagicMock
            Mocked response object

        Verifies
        --------
        - Request is retried on RequestException

        Parameters
        ----------
        alpha_tools : Any
            AlphaTools instance
        mock_response : MagicMock
            Mocked response object

        Returns
        -------
        None
        """
        mock_response.raise_for_status.side_effect = exceptions.RequestException
        with patch("requests.request", return_value=mock_response), pytest.raises(
            ValueError, match="API request failed"
        ):
            alpha_tools.generic_req("POST", "test_endpoint", {"param": "value"})

    def test_request_auth(
        self, 
        alpha_tools: Any, # noqa ANN401: typing.Any is not allowed
        mock_response: MagicMock
    ) -> None:
        """Test request authentication.

        Parameters
        ----------
        alpha_tools : Any
            AlphaTools instance
        mock_response : MagicMock
            Mocked response object

        Verifies
        --------
        - Authentication is passed correctly

        Parameters
        ----------
        alpha_tools : Any
            AlphaTools instance
        mock_response : MagicMock
            Mocked response object

        Returns
        -------
        None
        """
        mock_response.json.return_value = [{"test": "data"}]
        with patch("stpstone.utils.providers.br.inoa.request", return_value=mock_response) \
            as mock_request:
            alpha_tools.generic_req("POST", "test_endpoint", {"param": "value"})
            mock_request.assert_called_with(
                "POST",
                url="https://test.host/test_endpoint",
                json={"param": "value"},
                auth=("test_user", "test_pass"),
                timeout=(200, 200),  # Add timeout to match the actual call
            )


class TestFunds:
    """Tests for funds property."""

    def test_successful_funds_retrieval(
        self, 
        alpha_tools: Any, # noqa ANN401: typing.Any is not allowed
        mock_response: MagicMock
    ) -> None:
        """Test successful funds data retrieval.

        Parameters
        ----------
        alpha_tools : Any
            AlphaTools instance
        mock_response : MagicMock
            Mocked response object

        Verifies
        --------
        - DataFrame is returned with correct structure
        - Columns are properly formatted

        Parameters
        ----------
        alpha_tools : Any
            AlphaTools instance
        mock_response : MagicMock
            Mocked response object

        Returns
        -------
        None
        """
        mock_response.json.return_value = {
            "1": {"id": 1, "name": "Fund A", "legal_id": "A123"},
            "2": {"id": 2, "name": "Fund B", "legal_id": "B456"},
        }
        with patch("stpstone.utils.providers.br.inoa.request", return_value=mock_response) \
            as mock_request:
            df_ = alpha_tools.funds
            assert isinstance(df_, pd.DataFrame)
            assert set(df_.columns) == {"ID", "NAME", "LEGAL_ID", "ORIGIN"}
            assert len(df_) == 2
            assert df_["ORIGIN"].iloc[0] == "test_instance"
            mock_request.assert_called_once()

    def test_funds_retrieval_failure(
        self, 
        alpha_tools: Any, # noqa ANN401: typing.Any is not allowed
        mock_response: MagicMock
    ) -> None:
        """Test funds retrieval failure.

        Parameters
        ----------
        alpha_tools : Any
            AlphaTools instance
        mock_response : MagicMock
            Mocked response object

        Verifies
        --------
        - ValueError is raised when parsing fails

        Parameters
        ----------
        alpha_tools : Any
            AlphaTools instance
        mock_response : MagicMock
            Mocked response object

        Returns
        -------
        None
        """
        mock_response.json.side_effect = ValueError("Invalid JSON")
        with patch("requests.request", return_value=mock_response), pytest.raises(
            ValueError, match="Failed to process funds data"
        ):
            _ = alpha_tools.funds


class TestQuotes:
    """Tests for quotes method."""

    def test_successful_quotes_retrieval(
        self, 
        alpha_tools: Any, # noqa ANN401: typing.Any is not allowed
        mock_response: MagicMock, 
        mock_dates_br: MagicMock
    ) -> None:
        """Test successful quotes data retrieval.

        Parameters
        ----------
        alpha_tools : Any
            AlphaTools instance
        mock_response : MagicMock
            Mocked response object
        mock_dates_br : MagicMock
            Mocked DatesBRAnbima instance

        Verifies
        --------
        - DataFrame is returned with correct structure
        - Dates are properly converted
        - Columns are properly formatted

        Parameters
        ----------
        alpha_tools : Any
            AlphaTools instance
        mock_response : MagicMock
            Mocked response object
        mock_dates_br : MagicMock
            Mocked DatesBRAnbima instance

        Returns
        -------
        None
        """
        mock_response.json.return_value = {
            "items": [
                {"fund_id": 1, "date": "2023-01-01", "status_display": "Active"},
                {"fund_id": 2, "date": "2023-01-02", "status_display": "Inactive"},
            ]
        }
        with patch("stpstone.utils.providers.br.inoa.request", return_value=mock_response) \
            as mock_request:
            df_ = alpha_tools.quotes([1, 2])
            assert isinstance(df_, pd.DataFrame)
            assert set(df_.columns) == {"FUND_ID", "DATE", "STATUS_DISPLAY"}
            assert len(df_) == 2
            mock_request.assert_called_once()

    def test_empty_fund_ids(
        self, 
        alpha_tools: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test quotes with empty fund ID list.

        Parameters
        ----------
        alpha_tools : Any
            AlphaTools instance

        Verifies
        --------
        - ValueError is raised when fund ID list is empty
        """
        with pytest.raises(ValueError, match="Fund ID list cannot be empty"):
            alpha_tools.quotes([])

    def test_quotes_retrieval_failure(
        self, 
        alpha_tools: Any, # noqa ANN401: typing.Any is not allowed
        mock_response: MagicMock
    ) -> None:
        """Test quotes retrieval failure.

        Parameters
        ----------
        alpha_tools : Any
            AlphaTools instance
        mock_response : MagicMock
            Mocked response object

        Verifies
        --------
        - ValueError is raised when parsing fails

        Parameters
        ----------
        alpha_tools : Any
            AlphaTools instance
        mock_response : MagicMock
            Mocked response object

        Returns
        -------
        None
        """
        mock_response.json.side_effect = ValueError("Invalid JSON")
        with patch("requests.request", return_value=mock_response), pytest.raises(
            ValueError, match="Failed to process quotes data"
        ):
            alpha_tools.quotes([1, 2])

    def test_missing_items_key(
        self, 
        alpha_tools: Any, # noqa ANN401: typing.Any is not allowed
        mock_response: MagicMock
    ) -> None:
        """Test quotes response with missing items key.

        Parameters
        ----------
        alpha_tools : Any
            AlphaTools instance
        mock_response : MagicMock
            Mocked response object

        Verifies
        --------
        - ValueError is raised when items key is missing

        Parameters
        ----------
        alpha_tools : Any
            AlphaTools instance
        mock_response : MagicMock
            Mocked response object

        Returns
        -------
        None
        """
        mock_response.json.return_value = {"data": []}
        with patch("requests.request", return_value=mock_response), pytest.raises(
            ValueError, match="Failed to process quotes data"
        ):
            alpha_tools.quotes([1, 2])