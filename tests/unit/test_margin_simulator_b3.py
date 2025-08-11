"""Unit tests for MarginSimulatorB3 class.

Tests the B3 Margin Simulator API interaction including:
- Input validation for portfolio payloads
- API request construction and execution
- Error handling and response processing
"""

from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from requests.exceptions import RequestException

from stpstone.utils.parsers.json import JsonFiles
from stpstone.utils.providers.br.margin_simulator_b3 import MarginSimulatorB3


class TestMarginSimulatorB3:
    """Test cases for MarginSimulatorB3 class."""

    @pytest.fixture
    def valid_payload(self) -> list[dict]:
        """Fixture providing valid portfolio payload.

        Returns
        -------
        list[dict]
            Valid portfolio positions data
        """
        return [
            {
                "Security": {"symbol": "ABCBF160"},
                "SecurityGroup": {"positionTypeCode": 0},
                "Position": {
                    "longQuantity": 100,
                    "shortQuantity": 0,
                    "longPrice": 0,
                    "shortPrice": 0
                }
            }
        ]

    @pytest.fixture
    def invalid_payload_missing_key(self) -> list[dict]:
        """Fixture providing invalid payload missing required keys.

        Returns
        -------
        list[dict]
            Invalid portfolio positions data
        """
        return [{"Security": {"symbol": "ABCBF160"}}]

    @pytest.fixture
    def mock_json_parser(self) -> MagicMock:
        """Fixture providing mock for JsonFiles parser.

        Returns
        -------
        MagicMock
            Mocked JsonFiles instance
        """
        mock = MagicMock(spec=JsonFiles)
        mock.dict_to_json.return_value = '{"mock": "data"}'
        return mock

    def test_init_with_valid_payload(self, valid_payload: list[dict]) -> None:
        """Test initialization with valid portfolio payload.

        Verifies
        --------
        - The class can be initialized with valid payload
        - The payload is correctly stored in the instance
        - No exceptions are raised

        Parameters
        ----------
        valid_payload : list[dict]
            Valid portfolio data from fixture
        """
        simulator = MarginSimulatorB3(valid_payload, "test_token")
        assert simulator.dict_payload == valid_payload
        assert simulator.token == "test_token" # noqa S105: possible hardcoded password
        assert simulator.host == "https://simulador.b3.com.br/api/cors-app"

    def test_init_with_empty_payload(self) -> None:
        """Test initialization with empty payload raises ValueError.

        Verifies
        --------
        - Empty payload raises ValueError with appropriate message
        """
        with pytest.raises(ValueError, match="Portfolio payload cannot be empty"):
            MarginSimulatorB3([], "test_token")

    def test_init_with_invalid_payload(self, invalid_payload_missing_key: list[dict]) -> None:
        """Test initialization with invalid payload raises ValueError.

        Verifies
        --------
        - Payload missing required keys raises ValueError
        - Error message indicates missing required fields

        Parameters
        ----------
        invalid_payload_missing_key : list[dict]
            Invalid portfolio data from fixture
        """
        with pytest.raises(ValueError, match="must contain Security, SecurityGroup and Position"):
            MarginSimulatorB3(invalid_payload_missing_key, "test_token")

    @patch("stpstone.utils.providers.br.margin_simulator_b3.request")
    def test_total_deficit_surplus_default_timeout(
        self,
        mock_request: MagicMock,
        valid_payload: list[dict],
        mock_json_parser: MagicMock
    ) -> None:
        """Test default timeout is used when not specified.
        
        Verifies
        --------
        - Default timeout is used when not specified
        - Request is made with the correct parameters
        - Response is processed correctly
        
        Parameters
        ----------
        mock_request : MagicMock
            Mocked requests.request function
        valid_payload : list[dict]
            Valid portfolio data from fixture
        mock_json_parser : MagicMock
            Mocked JsonFiles instance

        Returns
        -------
        None
        """
        mock_response = MagicMock()
        mock_response.json.return_value = [{"result": "success"}]
        mock_request.return_value = mock_response

        with patch.object(JsonFiles, "dict_to_json", mock_json_parser.dict_to_json):
            simulator = MarginSimulatorB3(valid_payload, "test_token")
            result = simulator.total_deficit_surplus()

        mock_request.assert_called_once_with(
            method="POST",
            url="https://simulador.b3.com.br/api/cors-app/web/V1.0/RiskCalculation",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json"
            },
            data='{"mock": "data"}',
            verify=False,
            timeout=(200, 200)
        )
        assert result == [{"result": "success"}]

    @patch("stpstone.utils.providers.br.margin_simulator_b3.request")
    def test_total_deficit_surplus_custom_timeout(
        self,
        mock_request: MagicMock,
        valid_payload: list[dict],
        mock_json_parser: MagicMock
    ) -> None:
        """Test custom timeout can be specified.
        
        Verifies
        --------
        - Custom timeout is used when specified
        - Request is made with the correct parameters
        - Response is processed correctly
        
        Parameters
        ----------
        mock_request : MagicMock
            Mocked requests.request function
        valid_payload : list[dict]
            Valid portfolio data from fixture
        mock_json_parser : MagicMock
            Mocked JsonFiles instance

        Returns
        -------
        None
        """
        mock_response = MagicMock()
        mock_response.json.return_value = [{"result": "success"}]
        mock_request.return_value = mock_response

        with patch.object(JsonFiles, "dict_to_json", mock_json_parser.dict_to_json):
            simulator = MarginSimulatorB3(valid_payload, "test_token")
            result = simulator.total_deficit_surplus(timeout=10)

        mock_request.assert_called_once_with(
            method="POST",
            url="https://simulador.b3.com.br/api/cors-app/web/V1.0/RiskCalculation",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json"
            },
            data='{"mock": "data"}',
            verify=False,
            timeout=10)
        assert result == [{"result": "success"}]

    @patch("stpstone.utils.providers.br.margin_simulator_b3.request")
    def test_total_deficit_surplus_custom_liquidity(
        self,
        mock_request: MagicMock,
        valid_payload: list[dict],
        mock_json_parser: MagicMock
    ) -> None:
        """Test custom liquidity resource value handling.

        Verifies
        --------
        - Custom liquidity value is properly included in payload
        - Default value can be overridden

        Parameters
        ----------
        mock_request : MagicMock
            Mocked requests.request function
        valid_payload : list[dict]
            Valid portfolio data from fixture
        mock_json_parser : MagicMock
            Mocked JsonFiles instance
        """
        mock_response = MagicMock()
        mock_response.json.return_value = [{"result": "success"}]
        mock_request.return_value = mock_response

        with patch.object(JsonFiles, "dict_to_json", mock_json_parser.dict_to_json):
            simulator = MarginSimulatorB3(valid_payload, "test_token")
            simulator.total_deficit_surplus(value_liquidity_resource=np.int64(5_000_000_000))

        call_args = mock_json_parser.dict_to_json.call_args[0][0]
        assert call_args["LiquidityResource"]["value"] == 5_000_000_000

    @patch("stpstone.utils.providers.br.margin_simulator_b3.request")
    def test_total_deficit_surplus_request_failure(
        self,
        mock_request: MagicMock,
        valid_payload: list[dict]
    ) -> None:
        """Test handling of failed API request.

        Verifies
        --------
        - Request exceptions are properly caught and wrapped
        - Error message includes original exception details

        Parameters
        ----------
        mock_request : MagicMock
            Mocked requests.request function
        valid_payload : list[dict]
            Valid portfolio data from fixture
        """
        mock_request.side_effect = RequestException("Connection failed")

        simulator = MarginSimulatorB3(valid_payload, "test_token")
        with pytest.raises(ValueError, match="API request failed: Connection failed"):
            simulator.total_deficit_surplus()

    @patch("stpstone.utils.providers.br.margin_simulator_b3.request")
    def test_total_deficit_surplus_invalid_response(
        self,
        mock_request: MagicMock,
        valid_payload: list[dict]
    ) -> None:
        """Test handling of invalid API response.

        Verifies
        --------
        - Non-2xx responses raise ValueError
        - Error message includes status code

        Parameters
        ----------
        mock_request : MagicMock
            Mocked requests.request function
        valid_payload : list[dict]
            Valid portfolio data from fixture
        """
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = RequestException("400 Bad Request")
        mock_request.return_value = mock_response

        simulator = MarginSimulatorB3(valid_payload, "test_token")
        with pytest.raises(ValueError, match="API request failed: 400 Bad Request"):
            simulator.total_deficit_surplus()

    @patch("stpstone.utils.providers.br.margin_simulator_b3.JsonFiles")
    def test_json_parsing_error(
        self, 
        mock_json_files: MagicMock,
        valid_payload: list[dict]
    ) -> None:
        """Test handling of JSON parsing errors.

        Verifies
        --------
        - JSON parsing exceptions are properly caught and wrapped
        - Error message includes original exception details

        Parameters
        ----------
        valid_payload : list[dict]
            Valid portfolio data from fixture
        mock_json_files : MagicMock
            Mocked JsonFiles class
        """
        mock_instance = MagicMock()
        mock_instance.dict_to_json.side_effect = ValueError("Invalid JSON")
        mock_json_files.return_value = mock_instance

        simulator = MarginSimulatorB3(valid_payload, "test_token")
        with pytest.raises(ValueError, match="Invalid JSON"):
            simulator.total_deficit_surplus()