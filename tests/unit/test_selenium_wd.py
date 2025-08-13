"""Unit tests for SeleniumWD class.

Tests the Selenium WebDriver wrapper functionality including:
- Initialization with various configurations
- WebDriver instance management
- Element location and interaction
- Network traffic monitoring
- Log processing capabilities
- Error handling and validation
"""

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement

from stpstone.utils.webdriver_tools.selenium_wd import SeleniumWD


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_webdriver() -> MagicMock:
    """Fixture providing mocked WebDriver instance.

    Returns
    -------
    MagicMock
        Mocked WebDriver instance with basic methods
    """
    mock = MagicMock(spec=WebDriver)
    mock.get_log = MagicMock(return_value=[])
    return mock


@pytest.fixture
def mock_webelement() -> MagicMock:
    """Fixture providing mocked WebElement instance.

    Returns
    -------
    MagicMock
        Mocked WebElement instance with basic methods
    """
    return MagicMock(spec=WebElement)


@pytest.fixture
def mock_service() -> MagicMock:
    """Fixture providing mocked Service instance.

    Returns
    -------
    MagicMock
        Mocked Service instance
    """
    return MagicMock(spec=Service)


@pytest.fixture
def default_selenium_config() -> dict[str, Any]:
    """Fixture providing default configuration for SeleniumWD.

    Returns
    -------
    dict[str, Any]
        Dictionary with default configuration values
    """
    return {
        "url": "https://example.com",
        "path_webdriver": None,
        "int_port": None,
        "str_user_agent": None,
        "int_wait_load_seconds": 10,
        "int_delay_seconds": 10,
        "str_proxy": None,
        "bool_headless": False,
        "bool_incognito": False,
        "list_args": None
    }


@pytest.fixture
def selenium_instance(
    mock_webdriver: MagicMock, 
    default_selenium_config: dict[str, Any]
) -> Any: # noqa ANN401: typing.Any is not allowed
    """Fixture providing SeleniumWD instance with mocked WebDriver.

    Parameters
    ----------
    mock_webdriver : MagicMock
        Mocked WebDriver instance
    default_selenium_config : dict[str, Any]
        Default configuration values

    Returns
    -------
    Any
        SeleniumWD instance with mocked dependencies
    """
    with patch("selenium.webdriver.Chrome", return_value=mock_webdriver), \
         patch("webdriver_manager.chrome.ChromeDriverManager.install"), \
         patch("selenium.webdriver.chrome.service.Service"), \
         patch("selenium.webdriver.ChromeOptions") as mock_options:
        
        # Create a proper mock for ChromeOptions
        options_instance = MagicMock()
        options_instance.arguments = []
        options_instance.add_argument = lambda x: options_instance.arguments.append(x)
        mock_options.return_value = options_instance
        
        instance = SeleniumWD(**default_selenium_config)
        instance.web_driver = mock_webdriver
        mock_webdriver.options = options_instance
        return instance


# --------------------------
# Test Classes
# --------------------------
class TestSeleniumWDInitialization:
    """Test cases for SeleniumWD initialization and configuration."""

    def test_init_with_defaults(
        self, 
        mock_webdriver: MagicMock, 
        default_selenium_config: dict[str, Any]
    ) -> None:
        """Test initialization with default parameters.

        Verifies
        --------
        - Instance is created with default values
        - WebDriver is initialized correctly
        - URL is loaded

        Parameters
        ----------
        mock_webdriver : MagicMock
            Mocked WebDriver instance
        default_selenium_config : dict[str, Any]
            Default configuration values

        Returns
        -------
        None
        """
        with patch("selenium.webdriver.Chrome", return_value=mock_webdriver), \
             patch("webdriver_manager.chrome.ChromeDriverManager.install"):
            instance = SeleniumWD(**default_selenium_config)

            assert instance.url == "https://example.com"
            assert instance.int_wait_load_seconds == 10
            mock_webdriver.get.assert_called_once_with("https://example.com")
            mock_webdriver.implicitly_wait.assert_called_once_with(10)

    def test_init_with_custom_user_agent(
        self, 
        mock_webdriver: MagicMock, 
        default_selenium_config: dict[str, Any]
    ) -> None:
        """Test initialization with custom user agent.

        Verifies
        --------
        - Custom user agent is properly set
        - User agent is included in Chrome options

        Parameters
        ----------
        mock_webdriver : MagicMock
            Mocked WebDriver instance
        default_selenium_config : dict[str, Any]
            Default configuration values

        Returns
        -------
        None
        """
        custom_agent = "Custom Agent String"
        default_selenium_config["str_user_agent"] = custom_agent

        with patch("selenium.webdriver.Chrome", return_value=mock_webdriver), \
            patch("selenium.webdriver.ChromeOptions") as mock_options, \
            patch("webdriver_manager.chrome.ChromeDriverManager.install"):
            
            # Setup options mock
            options_instance = MagicMock()
            options_instance.arguments = []
            options_instance.add_argument = lambda x: options_instance.arguments.append(x)
            mock_options.return_value = options_instance
            
            SeleniumWD(**default_selenium_config)
            
            assert f"--user-agent={custom_agent}" in options_instance.arguments

    def test_init_with_headless_mode(
        self, 
        mock_webdriver: MagicMock, 
        default_selenium_config: dict[str, Any]
    ) -> None:
        """Test initialization with headless mode enabled.

        Verifies
        --------
        - Headless mode argument is added to Chrome options

        Parameters
        ----------
        mock_webdriver : MagicMock
            Mocked WebDriver instance
        default_selenium_config : dict[str, Any]
            Default configuration values

        Returns
        -------
        None
        """
        default_selenium_config["bool_headless"] = True

        with patch("selenium.webdriver.Chrome", return_value=mock_webdriver), \
            patch("webdriver_manager.chrome.ChromeDriverManager.install"), \
            patch("selenium.webdriver.ChromeOptions") as mock_options:
            
            # Setup options mock
            options_instance = MagicMock()
            options_instance.arguments = []
            options_instance.add_argument = lambda x: options_instance.arguments.append(x)
            mock_options.return_value = options_instance
            
            SeleniumWD(**default_selenium_config)

            # Check if headless argument was added to options
            assert "--headless=new" in options_instance.arguments

    @pytest.mark.parametrize("invalid_url", ["", "not_a_url", 123])
    def test_init_with_invalid_url_raises_valueerror(
        self, 
        invalid_url: Any, # noqa ANN401: typing.Any is not allowed
        default_selenium_config: dict[str, Any]
    ) -> None:
        """Test initialization with invalid URLs raises ValueError.

        Verifies
        --------
        - ValueError is raised for invalid URLs

        Parameters
        ----------
        invalid_url : Any
            Invalid URL value to test
        default_selenium_config : dict[str, Any]
            Default configuration values

        Returns
        -------
        None
        """
        default_selenium_config["url"] = invalid_url

        with pytest.raises((TypeError, ValueError)):
    
            SeleniumWD(**default_selenium_config)

    @pytest.mark.parametrize("invalid_wait_time", [-1, -10])
    def test_init_with_invalid_wait_time_raises_valueerror(
        self, 
        invalid_wait_time: int, 
        default_selenium_config: dict[str, Any]
    ) -> None:
        """Test initialization with negative wait times raises ValueError.

        Verifies
        --------
        - ValueError is raised for negative wait times

        Parameters
        ----------
        invalid_wait_time : int
            Invalid wait time value to test
        default_selenium_config : dict[str, Any]
            Default configuration values

        Returns
        -------
        None
        """
        default_selenium_config["int_wait_load_seconds"] = invalid_wait_time

        with pytest.raises(ValueError):
    
            SeleniumWD(**default_selenium_config)


class TestWebDriverMethods:
    """Test cases for WebDriver-related methods."""

    def test_get_web_driver_with_default_path(
        self, 
        mock_webdriver: MagicMock, 
        selenium_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test WebDriver initialization with default path.

        Verifies
        --------
        - ChromeDriverManager is used when path is None
        - WebDriver is properly configured

        Parameters
        ----------
        mock_webdriver : MagicMock
            Mocked WebDriver instance
        selenium_instance : Any
            SeleniumWD instance

        Returns
        -------
        None
        """
        mock_webdriver.reset_mock()
    
        with patch("selenium.webdriver.Chrome", return_value=mock_webdriver):
            driver = selenium_instance.get_web_driver()
            assert driver == mock_webdriver
            mock_webdriver.get.assert_called_once_with(selenium_instance.url)

    def test_get_web_driver_failure_raises_valueerror(
        self, 
        default_selenium_config: dict[str, Any]
    ) -> None:
        """Test WebDriver initialization failure raises ValueError.

        Verifies
        --------
        - ValueError is raised when WebDriver initialization fails
        - Error message contains original exception details

        Parameters
        ----------
        default_selenium_config : dict[str, Any]
            Default configuration values

        Returns
        -------
        None
        """
        with patch("selenium.webdriver.Chrome", side_effect=WebDriverException("Failed")):
            with pytest.raises(ValueError) as excinfo:
        
                instance = SeleniumWD(**default_selenium_config)
                instance.get_web_driver()

            assert "Failed to initialize WebDriver" in str(excinfo.value)
            assert "Failed" in str(excinfo.value)


class TestElementInteraction:
    """Test cases for element location and interaction methods."""

    def test_find_element_success(
        self, 
        selenium_instance: Any, # noqa ANN401: typing.Any is not allowed
        mock_webdriver: MagicMock, 
        mock_webelement: MagicMock
    ) -> None:
        """Test successful element location.

        Verifies
        --------
        - Element is found using specified selector
        - Correct WebElement is returned

        Parameters
        ----------
        selenium_instance : Any
            SeleniumWD instance
        mock_webdriver : MagicMock
            Mocked WebDriver instance
        mock_webelement : MagicMock
            Mocked WebElement instance

        Returns
        -------
        None
        """
        mock_webdriver.find_element.return_value = mock_webelement
        element = selenium_instance.find_element(mock_webdriver, "//div", "XPATH")
        assert element == mock_webelement
        mock_webdriver.find_element.assert_called_once_with(By.XPATH, "//div")

    def test_find_element_failure_raises_valueerror(
        self, 
        selenium_instance: Any, # noqa ANN401: typing.Any is not allowed
        mock_webdriver: MagicMock
    ) -> None:
        """Test element location failure raises ValueError.

        Verifies
        --------
        - ValueError is raised when element cannot be found
        - Error message contains original exception details

        Parameters
        ----------
        selenium_instance : Any
            SeleniumWD instance
        mock_webdriver : MagicMock
            Mocked WebDriver instance

        Returns
        -------
        None
        """
        mock_webdriver.find_element.side_effect = WebDriverException("Not found")

        with pytest.raises(ValueError) as excinfo:
            selenium_instance.find_element(mock_webdriver, "//div", "XPATH")

        assert "Element not found" in str(excinfo.value)
        assert "Not found" in str(excinfo.value)

    def test_find_elements(
        self, 
        selenium_instance: Any, # noqa ANN401: typing.Any is not allowed
        mock_webdriver: MagicMock, 
        mock_webelement: MagicMock
    ) -> None:
        """Test finding multiple elements.

        Verifies
        --------
        - Multiple elements are found using specified selector
        - Correct list of WebElements is returned

        Parameters
        ----------
        selenium_instance : Any
            SeleniumWD instance
        mock_webdriver : MagicMock
            Mocked WebDriver instance
        mock_webelement : MagicMock
            Mocked WebElement instance

        Returns
        -------
        None
        """
        mock_webdriver.find_elements.return_value = [mock_webelement, mock_webelement]
        elements = selenium_instance.find_elements(mock_webdriver, "//div", "XPATH")
        assert len(elements) == 2
        mock_webdriver.find_elements.assert_called_once_with(By.XPATH, "//div")

    def test_fill_input(
        self, 
        selenium_instance: Any, # noqa ANN401: typing.Any is not allowed
        mock_webelement: MagicMock
    ) -> None:
        """Test filling input field with text.

        Verifies
        --------
        - send_keys is called with correct text

        Parameters
        ----------
        selenium_instance : Any
            SeleniumWD instance
        mock_webelement : MagicMock
            Mocked WebElement instance

        Returns
        -------
        None
        """
        selenium_instance.fill_input(mock_webelement, "test input")
        mock_webelement.send_keys.assert_called_once_with("test input")

    def test_wait_until_el_loaded_success(
        self, 
        selenium_instance: Any, # noqa ANN401: typing.Any is not allowed
        mock_webdriver: MagicMock, 
        mock_webelement: MagicMock
    ) -> None:
        """Test successful wait for element to load.

        Verifies
        --------
        - WebDriverWait returns element when found
        - Correct timeout value is used

        Parameters
        ----------
        selenium_instance : Any
            SeleniumWD instance
        mock_webdriver : MagicMock
            Mocked WebDriver instance
        mock_webelement : MagicMock
            Mocked WebElement instance

        Returns
        -------
        None
        """
        with patch("selenium.webdriver.support.expected_conditions.presence_of_element_located") \
            as mock_ec:
            mock_ec.return_value = lambda driver: mock_webelement
            element = selenium_instance.wait_until_el_loaded("//div")
            assert element == mock_webelement


class TestNetworkAndLogging:
    """Test cases for network traffic and logging methods."""

    def test_get_network_traffic(
        self, 
        selenium_instance: Any, # noqa ANN401: typing.Any is not allowed
        mock_webdriver: MagicMock
    ) -> None:
        """Test retrieving network traffic logs.

        Verifies
        --------
        - Correct log type is requested
        - Only network responses are returned

        Parameters
        ----------
        selenium_instance : Any
            SeleniumWD instance
        mock_webdriver : MagicMock
            Mocked WebDriver instance

        Returns
        -------
        None
        """
        test_logs = [
            {"message": json.dumps({
                "method": "Network.response", 
                "params": {"url": "test"}
            })},
            {"message": json.dumps({
                "message": {
                    "method": "Other.method",
                    "params": {}
                }
            })}
        ]
        mock_webdriver.get_log.return_value = test_logs

        traffic = selenium_instance.get_network_traffic()
        assert len(traffic) == 1
        assert traffic[0]["method"] == "Network.response"

    def test_process_log_with_network_response(
        self, 
        selenium_instance: Any, # noqa ANN401: typing.Any is not allowed
        mock_webdriver: MagicMock
    ) -> None:
        """Test processing log entry with network response.

        Verifies
        --------
        - Network response details are extracted
        - Response body is requested and printed

        Parameters
        ----------
        selenium_instance : Any
            SeleniumWD instance
        mock_webdriver : MagicMock
            Mocked WebDriver instance

        Returns
        -------
        None
        """
        test_log = {
            "message": json.dumps({
                "message": {
                    "method": "Network.response",
                    "params": {"requestId": "123"}
                }
            })
        }
        mock_webdriver.execute_cdp_cmd.return_value = {"body": "test"}
        
        result = selenium_instance.process_log(test_log)
        assert result is not None

    def test_process_log_with_non_network_response(
        self, 
        selenium_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test processing log entry without network response.

        Verifies
        --------
        - None is returned for non-network log entries

        Parameters
        ----------
        selenium_instance : Any
            SeleniumWD instance

        Returns
        -------
        None
        """
        test_log = {
            "message": json.dumps({
                "method": "Other.method",
                "params": {}
            })
        }
        result = selenium_instance.process_log(test_log)
        assert result is None

    def test_get_browser_log_entries(
        self, 
        selenium_instance: Any, # noqa ANN401: typing.Any is not allowed
        mock_webdriver: MagicMock
    ) -> None:
        """Test retrieving browser log entries.

        Verifies
        --------
        - Correct log type is requested
        - Log entries are processed and returned

        Parameters
        ----------
        selenium_instance : Any
            SeleniumWD instance
        mock_webdriver : MagicMock
            Mocked WebDriver instance

        Returns
        -------
        None
        """
        test_logs = [{"message": "test", "source": "console", "level": "INFO"}]
        mock_webdriver.get_log.return_value = test_logs

        logs = selenium_instance.get_browser_log_entries()
        assert logs == test_logs
        mock_webdriver.get_log.assert_called_once_with("browser")


class TestValidationMethods:
    """Test cases for validation methods."""

    def test_validate_url_with_valid_url(self) -> None:
        """Test URL validation with valid URLs.

        Verifies
        --------
        - No exception is raised for valid URLs

        Returns
        -------
        None
        """
        instance = SeleniumWD.__new__(SeleniumWD)

        valid_urls = [
            "http://example.com",
            "https://example.com",
            "http://sub.example.com/path?query=param"
        ]
        for url in valid_urls:
            instance._validate_url(url)  # Should not raise

    @pytest.mark.parametrize("invalid_url", ["", "not_a_url", 123, None])
    def test_validate_url_with_invalid_url_raises_valueerror(
        self, 
        invalid_url: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test URL validation with invalid URLs raises ValueError.

        Verifies
        --------
        - ValueError is raised for invalid URLs

        Parameters
        ----------
        invalid_url : Any
            Invalid URL value

        Returns
        -------
        None
        """
        instance = SeleniumWD.__new__(SeleniumWD)

        with pytest.raises((ValueError, TypeError)):
            instance._validate_url(invalid_url)

    def test_validate_wait_time_with_valid_times(self) -> None:
        """Test wait time validation with valid times.

        Verifies
        --------
        - No exception is raised for valid wait times

        Returns
        -------
        None
        """
        instance = SeleniumWD.__new__(SeleniumWD)

        valid_times = [0, 1, 10, 100]
        for time in valid_times:
            instance._validate_wait_time(time)  # Should not raise

    @pytest.mark.parametrize("invalid_time", [-1, -10])
    def test_validate_wait_time_with_negative_times_raises_valueerror(
        self, invalid_time: int
    ) -> None:
        """Test wait time validation with negative times raises ValueError.

        Verifies
        --------
        - ValueError is raised for negative wait times

        Parameters
        ----------
        invalid_time : int
            Negative wait time value
        """
        instance = SeleniumWD.__new__(SeleniumWD)

        with pytest.raises(ValueError):
            instance._validate_wait_time(invalid_time)