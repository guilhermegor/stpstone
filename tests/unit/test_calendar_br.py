"""Unit tests for Brazilian holiday calendar implementations.

Tests the ANBIMA and FEBRABAN holiday calendar classes with various scenarios
including normal operations, edge cases, error conditions, and type validation.
"""

from datetime import date
from io import BytesIO
from typing import Any
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
import requests

from stpstone.utils.calendars.calendar_br import DatesBRAnbima, DatesBRFebraban


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def anbima_instance() -> DatesBRAnbima:
    """Fixture providing DatesBRAnbima instance.

    Returns
    -------
    DatesBRAnbima
        Instance of ANBIMA calendar class
    """
    return DatesBRAnbima()


@pytest.fixture
def febraban_instance() -> DatesBRFebraban:
    """Fixture providing DatesBRFebraban instance.

    Returns
    -------
    DatesBRFebraban
        Instance of FEBRABAN calendar class
    """
    return DatesBRFebraban()


@pytest.fixture
def sample_anbima_dataframe() -> pd.DataFrame:
    """Fixture providing sample ANBIMA holiday data.

    Returns
    -------
    pd.DataFrame
        Sample DataFrame with holiday data
    """
    return pd.DataFrame({
        "DATE": ["01/01/2023", "25/12/2023"],
        "WEEKDAY": ["Sunday", "Monday"],
        "NAME": ["New Year", "Christmas"]
    })


@pytest.fixture
def sample_febraban_data() -> list[dict]:
    """Fixture providing sample FEBRABAN holiday data.

    Returns
    -------
    list[dict]
        Sample list of holiday dictionaries
    """
    return [
        {"diaMes": "1 de janeiro", "diaSemana": "Domingo", "nomeFeriado": "Ano Novo"},
        {"diaMes": "25 de dezembro", "diaSemana": "Segunda", "nomeFeriado": "Natal"}
    ]


@pytest.fixture
def mock_response_content() -> bytes:
    """Fixture providing mock Excel content.

    Returns
    -------
    bytes
        Mock Excel file content
    """
    return b"mock excel content"


# --------------------------
# Tests for DatesBRAnbima
# --------------------------
class TestDatesBRAnbima:
    """Test cases for DatesBRAnbima class.

    Tests the ANBIMA holiday calendar functionality including data fetching,
    transformation, and validation methods.
    """

    def test_init(self, anbima_instance: DatesBRAnbima) -> None:
        """Test initialization of DatesBRAnbima.

        Verifies
        --------
        - Instance is created successfully
        - Instance is of correct type

        Parameters
        ----------
        anbima_instance : DatesBRAnbima
            ANBIMA calendar instance from fixture

        Returns
        -------
        None
        """
        assert isinstance(anbima_instance, DatesBRAnbima)

    @patch("requests.get")
    def test_get_holidays_raw_success(
        self, mock_get: Mock, anbima_instance: DatesBRAnbima, mock_response_content: bytes
    ) -> None:
        """Test successful raw holiday data fetching.

        Verifies
        --------
        - HTTP request is made with correct headers
        - Response content is validated
        - DataFrame is created with correct structure

        Parameters
        ----------
        mock_get : Mock
            Mocked requests.get function
        anbima_instance : DatesBRAnbima
            ANBIMA calendar instance
        mock_response_content : bytes
            Mock response content

        Returns
        -------
        None
        """
        mock_response = Mock()
        mock_response.content = mock_response_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        with patch("pandas.read_excel") as mock_read_excel:
            mock_read_excel.return_value = pd.DataFrame({
                "DATE": ["01/01/2023"], "WEEKDAY": ["Sunday"], "NAME": ["New Year"]
            })
            result = anbima_instance.get_holidays_raw()

        mock_get.assert_called_once_with(
            "https://www.anbima.com.br/feriados/arqs/feriados_nacionais.xls",
            headers={
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "en-US,en;q=0.9,pt;q=0.8,es;q=0.7",
                "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            }
        )
        assert isinstance(result, pd.DataFrame)

    @patch("requests.get")
    def test_get_holidays_raw_http_error(
        self, mock_get: Mock, anbima_instance: DatesBRAnbima
    ) -> None:
        """Test HTTP error during holiday data fetching.

        Verifies
        --------
        - HTTP errors are properly raised

        Parameters
        ----------
        mock_get : Mock
            Mocked requests.get function
        anbima_instance : DatesBRAnbima
            ANBIMA calendar instance

        Returns
        -------
        None
        """
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("HTTP Error")
        mock_get.return_value = mock_response

        with pytest.raises(requests.HTTPError):
            anbima_instance.get_holidays_raw()

    def test_validate_dataframe_none(self, anbima_instance: DatesBRAnbima) -> None:
        """Test DataFrame validation with None input.

        Verifies
        --------
        - None DataFrame raises ValueError

        Parameters
        ----------
        anbima_instance : DatesBRAnbima
            ANBIMA calendar instance

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="cannot be None"):
            anbima_instance._validate_dataframe(None, "test_df")

    def test_validate_dataframe_not_dataframe(self, anbima_instance: DatesBRAnbima) -> None:
        """Test DataFrame validation with non-DataFrame input.

        Verifies
        --------
        - Non-DataFrame input raises ValueError

        Parameters
        ----------
        anbima_instance : DatesBRAnbima
            ANBIMA calendar instance

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="must be a pandas DataFrame"):
            anbima_instance._validate_dataframe("not a dataframe", "test_df")

    def test_validate_dataframe_empty(self, anbima_instance: DatesBRAnbima) -> None:
        """Test DataFrame validation with empty DataFrame.

        Verifies
        --------
        - Empty DataFrame raises ValueError

        Parameters
        ----------
        anbima_instance : DatesBRAnbima
            ANBIMA calendar instance

        Returns
        -------
        None
        """
        empty_df = pd.DataFrame()
        with pytest.raises(ValueError, match="cannot be empty"):
            anbima_instance._validate_dataframe(empty_df, "test_df")

    def test_validate_response_content_none(self, anbima_instance: DatesBRAnbima) -> None:
        """Test response content validation with None.

        Verifies
        --------
        - None content raises ValueError

        Parameters
        ----------
        anbima_instance : DatesBRAnbima
            ANBIMA calendar instance

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="cannot be None"):
            anbima_instance._validate_response_content(None)

    def test_validate_response_content_empty(self, anbima_instance: DatesBRAnbima) -> None:
        """Test response content validation with empty content.

        Verifies
        --------
        - Empty content raises ValueError

        Parameters
        ----------
        anbima_instance : DatesBRAnbima
            ANBIMA calendar instance

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="cannot be empty"):
            anbima_instance._validate_response_content(b"")

    def test_remove_footer_no_footer(
        self, anbima_instance: DatesBRAnbima, sample_anbima_dataframe: pd.DataFrame
    ) -> None:
        """Test footer removal when no footer exists.

        Verifies
        --------
        - DataFrame remains unchanged when no footer is found
        - Validation passes

        Parameters
        ----------
        anbima_instance : DatesBRAnbima
            ANBIMA calendar instance
        sample_anbima_dataframe : pd.DataFrame
            Sample holiday DataFrame

        Returns
        -------
        None
        """
        original_len = len(sample_anbima_dataframe)
        result = anbima_instance._remove_footer(sample_anbima_dataframe)
        assert len(result) == original_len

    def test_remove_footer_with_footer(
        self, anbima_instance: DatesBRAnbima, sample_anbima_dataframe: pd.DataFrame
    ) -> None:
        """Test footer removal when footer exists.

        Verifies
        --------
        - Footer rows are removed from DataFrame
        - Validation passes

        Parameters
        ----------
        anbima_instance : DatesBRAnbima
            ANBIMA calendar instance
        sample_anbima_dataframe : pd.DataFrame
            Sample holiday DataFrame

        Returns
        -------
        None
        """
        df_with_footer = sample_anbima_dataframe.copy()
        footer_row = pd.Series({
            "DATE": "fonte: anbima", "WEEKDAY": "fonte: anbima", "NAME": "fonte: anbima"
        })
        df_with_footer = pd.concat([df_with_footer, pd.DataFrame([footer_row])], ignore_index=True)

        result = anbima_instance._remove_footer(df_with_footer)
        assert len(result) == len(sample_anbima_dataframe)
        assert "fonte: anbima" not in result.values

    @patch.object(DatesBRAnbima, "get_holidays_raw")
    @patch.object(DatesBRAnbima, "transform_holidays")
    def test_holidays_method(
        self,
        mock_transform: Mock,
        mock_get_raw: Mock,
        anbima_instance: DatesBRAnbima,
        sample_anbima_dataframe: pd.DataFrame
    ) -> None:
        """Test holidays method integration.

        Verifies
        --------
        - Method calls required internal methods
        - Returns correct format

        Parameters
        ----------
        mock_transform : Mock
            Mocked transform_holidays method
        mock_get_raw : Mock
            Mocked get_holidays_raw method
        anbima_instance : DatesBRAnbima
            ANBIMA calendar instance
        sample_anbima_dataframe : pd.DataFrame
            Sample holiday DataFrame

        Returns
        -------
        None
        """
        mock_get_raw.return_value = sample_anbima_dataframe
        mock_transform.return_value = sample_anbima_dataframe

        result = anbima_instance.holidays()

        mock_get_raw.assert_called_once()
        mock_transform.assert_called_once_with(sample_anbima_dataframe)
        assert isinstance(result, list)
        assert all(isinstance(item, tuple) and len(item) == 2 for item in result)


# --------------------------
# Tests for DatesBRFebraban
# --------------------------
class TestDatesBRFebraban:
    """Test cases for DatesBRFebraban class.

    Tests the FEBRABAN holiday calendar functionality including data fetching,
    date parsing, and validation methods.
    """

    def test_init(self, febraban_instance: DatesBRFebraban) -> None:
        """Test initialization of DatesBRFebraban.

        Verifies
        --------
        - Instance is created successfully
        - String handler is initialized

        Parameters
        ----------
        febraban_instance : DatesBRFebraban
            FEBRABAN calendar instance

        Returns
        -------
        None
        """
        assert isinstance(febraban_instance, DatesBRFebraban)
        assert hasattr(febraban_instance, "cls_str_handler")

    def test_validate_year_valid(self, febraban_instance: DatesBRFebraban) -> None:
        """Test year validation with valid years.

        Verifies
        --------
        - Valid years pass validation

        Parameters
        ----------
        febraban_instance : DatesBRFebraban
            FEBRABAN calendar instance

        Returns
        -------
        None
        """
        valid_years = [1900, 2000, 2100]
        for year in valid_years:
            febraban_instance._validate_year(year)

    def test_validate_year_invalid_type(self, febraban_instance: DatesBRFebraban) -> None:
        """Test year validation with invalid types.

        Verifies
        --------
        - Non-integer years raise ValueError

        Parameters
        ----------
        febraban_instance : DatesBRFebraban
            FEBRABAN calendar instance

        Returns
        -------
        None
        """
        invalid_years = ["2023", 2023.5, None]
        for year in invalid_years:
            with pytest.raises(ValueError, match="must be an integer"):
                febraban_instance._validate_year(year)

    def test_validate_year_out_of_range(self, febraban_instance: DatesBRFebraban) -> None:
        """Test year validation with out-of-range years.

        Verifies
        --------
        - Years outside valid range raise ValueError

        Parameters
        ----------
        febraban_instance : DatesBRFebraban
            FEBRABAN calendar instance

        Returns
        -------
        None
        """
        invalid_years = [1899, 2101]
        for year in invalid_years:
            with pytest.raises(ValueError, match="must be between 1900 and 2100"):
                febraban_instance._validate_year(year)

    def test_validate_year_range_valid(self, febraban_instance: DatesBRFebraban) -> None:
        """Test year range validation with valid ranges.

        Verifies
        --------
        - Valid year ranges pass validation

        Parameters
        ----------
        febraban_instance : DatesBRFebraban
            FEBRABAN calendar instance

        Returns
        -------
        None
        """
        valid_ranges = [(2000, 2020), (2010, 2010)]
        for start, end in valid_ranges:
            febraban_instance._validate_year_range(start, end)

    def test_validate_year_range_invalid(self, febraban_instance: DatesBRFebraban) -> None:
        """Test year range validation with invalid ranges.

        Verifies
        --------
        - Invalid year ranges raise ValueError

        Parameters
        ----------
        febraban_instance : DatesBRFebraban
            FEBRABAN calendar instance

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="cannot be after end year"):
            febraban_instance._validate_year_range(2020, 2010)

    def test_validate_date_string_valid(self, febraban_instance: DatesBRFebraban) -> None:
        """Test date string validation with valid strings.

        Verifies
        --------
        - Valid date strings pass validation

        Parameters
        ----------
        febraban_instance : DatesBRFebraban
            FEBRABAN calendar instance

        Returns
        -------
        None
        """
        valid_dates = ["1 de janeiro", "25 de dezembro"]
        for date_str in valid_dates:
            febraban_instance._validate_date_string(date_str)

    def test_validate_date_string_invalid(self, febraban_instance: DatesBRFebraban) -> None:
        """Test date string validation with invalid strings.

        Verifies
        --------
        - Invalid date strings raise ValueError

        Parameters
        ----------
        febraban_instance : DatesBRFebraban
            FEBRABAN calendar instance

        Returns
        -------
        None
        """
        invalid_dates = ["", None, "1-janeiro", "1"]
        for date_str in invalid_dates:
            with pytest.raises(ValueError):
                febraban_instance._validate_date_string(date_str)

    def test_validate_json_response_valid(
        self, febraban_instance: DatesBRFebraban, sample_febraban_data: list[dict]
    ) -> None:
        """Test JSON response validation with valid data.

        Verifies
        --------
        - Valid JSON responses pass validation

        Parameters
        ----------
        febraban_instance : DatesBRFebraban
            FEBRABAN calendar instance
        sample_febraban_data : list[dict]
            Sample holiday data

        Returns
        -------
        None
        """
        febraban_instance._validate_json_response(sample_febraban_data, 2023)

    def test_validate_json_response_invalid(self, febraban_instance: DatesBRFebraban) -> None:
        """Test JSON response validation with invalid data.

        Verifies
        --------
        - Invalid JSON responses raise ValueError

        Parameters
        ----------
        febraban_instance : DatesBRFebraban
            FEBRABAN calendar instance

        Returns
        -------
        None
        """
        invalid_responses = [None, "not a list", []]
        for response in invalid_responses:
            with pytest.raises(ValueError):
                febraban_instance._validate_json_response(response, 2023)

    def test_parse_brazillian_date_valid(self, febraban_instance: DatesBRFebraban) -> None:
        """Test Brazilian date parsing with valid dates.

        Verifies
        --------
        - Valid date strings are parsed correctly
        - Returns proper date objects

        Parameters
        ----------
        febraban_instance : DatesBRFebraban
            FEBRABAN calendar instance

        Returns
        -------
        None
        """
        test_cases = [
            ("1 de janeiro", 2023, date(2023, 1, 1)),
            ("25 de dezembro", 2023, date(2023, 12, 25)),
        ]

        for date_str, year, expected in test_cases:
            result = febraban_instance._parse_brazillian_date(date_str, year)
            assert result == expected

    def test_parse_brazillian_date_invalid(self, febraban_instance: DatesBRFebraban) -> None:
        """Test Brazilian date parsing with invalid dates.

        Verifies
        --------
        - Invalid date strings raise ValueError

        Parameters
        ----------
        febraban_instance : DatesBRFebraban
            FEBRABAN calendar instance

        Returns
        -------
        None
        """
        invalid_dates = ["32 de janeiro", "1 de invalidmonth", "de janeiro"]
        for date_str in invalid_dates:
            with pytest.raises(ValueError, match="Invalid date format"):
                febraban_instance._parse_brazillian_date(date_str, 2023)

    @patch("requests.get")
    def test_get_holidays_raw_success(
        self, mock_get: Mock, febraban_instance: DatesBRFebraban, sample_febraban_data: list[dict]
    ) -> None:
        """Test successful raw holiday data fetching.

        Verifies
        --------
        - HTTP request is made with correct parameters
        - JSON response is validated
        - Returns correct data

        Parameters
        ----------
        mock_get : Mock
            Mocked requests.get function
        febraban_instance : DatesBRFebraban
            FEBRABAN calendar instance
        sample_febraban_data : list[dict]
            Sample holiday data

        Returns
        -------
        None
        """
        mock_response = Mock()
        mock_response.json.return_value = sample_febraban_data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = febraban_instance.get_holidays_raw(2023)

        mock_get.assert_called_once()
        assert result == sample_febraban_data

    def test_get_holidays_years_range(self, febraban_instance: DatesBRFebraban) -> None:
        """Test year range calculation for holidays.

        Verifies
        --------
        - Year range is calculated correctly
        - Multiple years are processed

        Parameters
        ----------
        febraban_instance : DatesBRFebraban
            FEBRABAN calendar instance

        Returns
        -------
        None
        """
        with patch.object(febraban_instance, "get_holidays_raw") as mock_get_raw:
            mock_get_raw.return_value = [{"diaMes": "1 de janeiro", "nomeFeriado": "Ano Novo"}]
            result = febraban_instance.get_holidays_years()

        assert isinstance(result, pd.DataFrame)
        assert mock_get_raw.call_count > 1

    @patch.object(DatesBRFebraban, "get_holidays_years")
    @patch.object(DatesBRFebraban, "transform_holidays")
    def test_holidays_method(
        self,
        mock_transform: Mock,
        mock_get_years: Mock,
        febraban_instance: DatesBRFebraban,
        sample_anbima_dataframe: pd.DataFrame
    ) -> None:
        """Test holidays method integration.

        Verifies
        --------
        - Method calls required internal methods
        - Returns correct format

        Parameters
        ----------
        mock_transform : Mock
            Mocked transform_holidays method
        mock_get_years : Mock
            Mocked get_holidays_years method
        febraban_instance : DatesBRFebraban
            FEBRABAN calendar instance
        sample_anbima_dataframe : pd.DataFrame
            Sample holiday DataFrame

        Returns
        -------
        None
        """
        mock_get_years.return_value = sample_anbima_dataframe
        mock_transform.return_value = sample_anbima_dataframe

        result = febraban_instance.holidays()

        mock_get_years.assert_called_once()
        mock_transform.assert_called_once_with(sample_anbima_dataframe)
        assert isinstance(result, list)
        assert all(isinstance(item, tuple) and len(item) == 2 for item in result)


# --------------------------
# Error Handling Tests
# --------------------------
def test_anbima_holidays_error_handling(anbima_instance: DatesBRAnbima) -> None:
    """Test error handling in ANBIMA holidays method.

    Verifies
    --------
    - Exceptions from internal methods are properly handled

    Parameters
    ----------
    anbima_instance : DatesBRAnbima
        ANBIMA calendar instance

    Returns
    -------
    None
    """
    with patch.object(anbima_instance, "get_holidays_raw") as mock_get:
        mock_get.side_effect = ValueError("Test error")
        with pytest.raises(ValueError, match="Test error"):
            anbima_instance.holidays()

def test_febraban_holidays_error_handling(febraban_instance: DatesBRFebraban) -> None:
    """Test error handling in FEBRABAN holidays method.

    Verifies
    --------
    - Exceptions from internal methods are properly handled

    Parameters
    ----------
    febraban_instance : DatesBRFebraban
        FEBRABAN calendar instance

    Returns
    -------
    None
    """
    with patch.object(febraban_instance, "get_holidays_years") as mock_get:
        mock_get.side_effect = ValueError("Test error")
        with pytest.raises(ValueError, match="Test error"):
            febraban_instance.holidays()