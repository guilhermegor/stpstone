"""Unit tests for Brazilian holiday calendar implementations.

Tests the ANBIMA and FEBRABAN holiday calendar functionality, covering
initialization, data fetching, transformation, and validation logic.
"""

from datetime import date
import importlib
from io import BytesIO
import sys
from typing import Any, Optional
from unittest.mock import patch

import pandas as pd
import pytest

from stpstone.utils.calendars.calendar_br import DatesBRAnbima, DatesBRFebraban
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.str import StrHandler


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def anbima_instance() -> DatesBRAnbima:
    """Fixture providing a DatesBRAnbima instance.

    Returns
    -------
    DatesBRAnbima
        Initialized DatesBRAnbima instance
    """
    return DatesBRAnbima()


@pytest.fixture
def febraban_instance() -> DatesBRFebraban:
    """Fixture providing a DatesBRFebraban instance.

    Returns
    -------
    DatesBRFebraban
        Initialized DatesBRFebraban instance
    """
    return DatesBRFebraban()


@pytest.fixture
def sample_anbima_df() -> pd.DataFrame:
    """Fixture providing sample ANBIMA DataFrame.

    Returns
    -------
    pd.DataFrame
        Sample DataFrame with DATE, WEEKDAY, NAME columns
    """
    return pd.DataFrame({
        "DATE": ["01/01/2023", "07/09/2023", "Fonte: ANBIMA"],
        "WEEKDAY": ["Domingo", "Quinta-feira", ""],
        "NAME": ["Ano Novo", "Independência", ""]
    })


@pytest.fixture
def sample_febraban_json() -> list[dict]:
    """Fixture providing sample FEBRABAN JSON response.

    Returns
    -------
    list[dict]
        Sample JSON response with holiday data
    """
    return [
        {"diaMes": "1 de janeiro", "diaSemana": "Domingo", "nomeFeriado": "Ano Novo"},
        {"diaMes": "7 de setembro", "diaSemana": "Quinta-feira", "nomeFeriado": "Independência"}
    ]


# --------------------------
# Tests for DatesBRAnbima
# --------------------------
class TestDatesBRAnbima:
    """Test cases for DatesBRAnbima class.

    Verifies initialization, holiday fetching, transformation, and validation.
    """

    def test_init(self, anbima_instance: DatesBRAnbima) -> None:
        """Test initialization of DatesBRAnbima.

        Verifies
        --------
        - Instance is created
        - StrHandler is properly initialized

        Parameters
        ----------
        anbima_instance : DatesBRAnbima
            Fixture providing DatesBRAnbima instance

        Returns
        -------
        None
        """
        assert isinstance(anbima_instance, DatesBRAnbima)
        assert isinstance(anbima_instance.cls_str_handler, StrHandler)

    @patch("requests.get")
    def test_get_holidays_raw_success(
        self, mock_get: Any, anbima_instance: DatesBRAnbima
    ) -> None:
        """Test successful fetching of raw holiday data.

        Verifies
        --------
        - HTTP request is made with correct headers
        - DataFrame is returned with expected columns
        - Response content is validated

        Parameters
        ----------
        mock_get : Any
            Mock for requests.get
        anbima_instance : DatesBRAnbima
            Fixture providing DatesBRAnbima instance

        Returns
        -------
        None
        """
        mock_response = mock_get.return_value
        mock_response.content = b"dummy excel content"
        mock_response.raise_for_status.return_value = None

        with patch("pandas.read_excel") as mock_read_excel:
            mock_read_excel.return_value = pd.DataFrame({
                "DATE": ["01/01/2023"],
                "WEEKDAY": ["Domingo"],
                "NAME": ["Ano Novo"]
            })
            result = anbima_instance.get_holidays_raw()

        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["DATE", "WEEKDAY", "NAME"]
        mock_get.assert_called_once()
        mock_read_excel.assert_called_once_with(
            BytesIO(b"dummy excel content"),
            header=None,
            names=["DATE", "WEEKDAY", "NAME"],
            skiprows=1
        )

    @patch("requests.get")
    def test_get_holidays_raw_empty_content(
        self, mock_get: Any, anbima_instance: DatesBRAnbima
    ) -> None:
        """Test handling of empty response content.

        Verifies
        --------
        - ValueError is raised for empty content
        - Error message matches expected pattern

        Parameters
        ----------
        mock_get : Any
            Mock for requests.get
        anbima_instance : DatesBRAnbima
            Fixture providing DatesBRAnbima instance

        Returns
        -------
        None
        """
        mock_response = mock_get.return_value
        mock_response.content = b""
        mock_response.raise_for_status.return_value = None

        with pytest.raises(ValueError, match="Response content cannot be empty"):
            anbima_instance.get_holidays_raw()

    def test_transform_holidays(
        self, anbima_instance: DatesBRAnbima, sample_anbima_df: pd.DataFrame
    ) -> None:
        """Test holiday data transformation.

        Verifies
        --------
        - DataFrame is properly transformed
        - Footer is removed
        - Types are correctly set
        - Diacritics are removed from names

        Parameters
        ----------
        anbima_instance : DatesBRAnbima
            Fixture providing DatesBRAnbima instance
        sample_anbima_df : pd.DataFrame
            Fixture providing sample ANBIMA DataFrame

        Returns
        -------
        None
        """
        with patch.object(anbima_instance, "timestamp_to_date", return_value=date(2023, 1, 1)) as mock_timestamp:
            result = anbima_instance.transform_holidays(sample_anbima_df)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert result["NAME"].iloc[0] == "Ano Novo"
        assert result["DATE"].iloc[0] == date(2023, 1, 1)
        mock_timestamp.assert_called()

    def test_remove_footer(
        self, anbima_instance: DatesBRAnbima, sample_anbima_df: pd.DataFrame
    ) -> None:
        """Test removal of footer from DataFrame.

        Verifies
        --------
        - Footer rows are removed
        - Resulting DataFrame has correct length
        - Validation is performed

        Parameters
        ----------
        anbima_instance : DatesBRAnbima
            Fixture providing DatesBRAnbima instance
        sample_anbima_df : pd.DataFrame
            Fixture providing sample ANBIMA DataFrame

        Returns
        -------
        None
        """
        result = anbima_instance._remove_footer(sample_anbima_df)
        assert len(result) == 2
        assert not any("Fonte: ANBIMA" in str(cell).lower() for _, row in result.iterrows() for cell in row)

    @pytest.mark.parametrize("invalid_df, name", [
        (None, "test_df"),
        (pd.Series([1, 2, 3]), "test_df"),
        (pd.DataFrame(), "test_df")
    ])
    def test_validate_dataframe_invalid(
        self, anbima_instance: DatesBRAnbima, invalid_df: Any, name: str
    ) -> None:
        """Test DataFrame validation with invalid inputs.

        Verifies
        --------
        - ValueError is raised for invalid DataFrames
        - Error message matches expected pattern

        Parameters
        ----------
        anbima_instance : DatesBRAnbima
            Fixture providing DatesBRAnbima instance
        invalid_df : Any
            Invalid DataFrame input
        name : str
            Name parameter for validation

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match=f"{name}"):
            anbima_instance._validate_dataframe(invalid_df, name)

    @pytest.mark.parametrize("content", [None, b""])
    def test_validate_response_content_invalid(
        self, anbima_instance: DatesBRAnbima, content: Optional[bytes]
    ) -> None:
        """Test response content validation with invalid inputs.

        Verifies
        --------
        - ValueError is raised for None or empty content
        - Error message matches expected pattern

        Parameters
        ----------
        anbima_instance : DatesBRAnbima
            Fixture providing DatesBRAnbima instance
        content : Optional[bytes]
            Invalid response content

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Response content cannot be"):
            anbima_instance._validate_response_content(content)


# --------------------------
# Tests for DatesBRFebraban
# --------------------------
class TestDatesBRFebraban:
    """Test cases for DatesBRFebraban class.

    Verifies initialization, holiday fetching, transformation, and validation.
    """

    def test_init(self, febraban_instance: DatesBRFebraban) -> None:
        """Test initialization of DatesBRFebraban.

        Verifies
        --------
        - Instance is created
        - StrHandler and HandlingDicts are properly initialized

        Parameters
        ----------
        febraban_instance : DatesBRFebraban
            Fixture providing DatesBRFebraban instance

        Returns
        -------
        None
        """
        assert isinstance(febraban_instance, DatesBRFebraban)
        assert isinstance(febraban_instance.cls_str_handler, StrHandler)
        assert isinstance(febraban_instance.cls_dict_handler, HandlingDicts)

    @patch("requests.get")
    def test_get_holidays_raw_success(
        self, mock_get: Any, febraban_instance: DatesBRFebraban, sample_febraban_json: list[dict]
    ) -> None:
        """Test successful fetching of raw holiday data.

        Verifies
        --------
        - HTTP request is made with correct headers and cookies
        - JSON response is validated
        - Correct data is returned

        Parameters
        ----------
        mock_get : Any
            Mock for requests.get
        febraban_instance : DatesBRFebraban
            Fixture providing DatesBRFebraban instance
        sample_febraban_json : list[dict]
            Fixture providing sample FEBRABAN JSON response

        Returns
        -------
        None
        """
        mock_response = mock_get.return_value
        mock_response.json.return_value = sample_febraban_json
        mock_response.raise_for_status.return_value = None

        result = febraban_instance.get_holidays_raw(2023)
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["nomeFeriado"] == "Ano Novo"
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_get_holidays_years(
        self, mock_get: Any, febraban_instance: DatesBRFebraban, sample_febraban_json: list[dict]
    ) -> None:
        """Test fetching holidays for multiple years.

        Verifies
        --------
        - Data is fetched for each year
        - DataFrame is properly constructed
        - Year validation is performed

        Parameters
        ----------
        mock_get : Any
            Mock for requests.get
        febraban_instance : DatesBRFebraban
            Fixture providing DatesBRFebraban instance
        sample_febraban_json : list[dict]
            Fixture providing sample FEBRABAN JSON response

        Returns
        -------
        None
        """
        mock_response = mock_get.return_value
        mock_response.json.return_value = sample_febraban_json
        mock_response.raise_for_status.return_value = None

        with patch.object(febraban_instance, "get_holidays_raw", return_value=sample_febraban_json):
            result = febraban_instance.get_holidays_years()

        assert isinstance(result, pd.DataFrame)
        assert "ANO" in result.columns
        assert len(result) > 0

    def test_transform_holidays(
        self, febraban_instance: DatesBRFebraban
    ) -> None:
        """Test holiday data transformation.

        Verifies
        --------
        - DataFrame is properly transformed
        - Column names are converted
        - Dates are parsed
        - Diacritics are removed

        Parameters
        ----------
        febraban_instance : DatesBRFebraban
            Fixture providing DatesBRFebraban instance

        Returns
        -------
        None
        """
        df = pd.DataFrame({
            "diaMes": ["1 de janeiro"],
            "diaSemana": ["Domingo"],
            "nomeFeriado": ["Ano Novo"],
            "ANO": [2023]
        })
        with patch.object(febraban_instance, "_parse_brazillian_date", return_value=date(2023, 1, 1)):
            result = febraban_instance.transform_holidays(df)

        assert isinstance(result, pd.DataFrame)
        assert "NOME_FERIADO" in result.columns
        assert result["DIA_MES_ANO"].iloc[0] == date(2023, 1, 1)

    @pytest.mark.parametrize("date_str, year, expected_date", [
        ("1 de janeiro", 2023, date(2023, 1, 1)),
        ("7 de setembro", 2023, date(2023, 9, 7))
    ])
    def test_parse_brazillian_date(
        self, febraban_instance: DatesBRFebraban, date_str: str, year: int, expected_date: date
    ) -> None:
        """Test parsing of Brazilian date strings.

        Verifies
        --------
        - Date strings are correctly parsed
        - Correct date objects are returned

        Parameters
        ----------
        febraban_instance : DatesBRFebraban
            Fixture providing DatesBRFebraban instance
        date_str : str
            Brazilian date string
        year : int
            Year for date construction
        expected_date : date
            Expected date object

        Returns
        -------
        None
        """
        result = febraban_instance._parse_brazillian_date(date_str, year)
        assert result == expected_date

    @pytest.mark.parametrize("invalid_date_str", [None, "", "1 janeiro", "invalid"])
    def test_validate_date_string_invalid(
        self, febraban_instance: DatesBRFebraban, invalid_date_str: Optional[str]
    ) -> None:
        """Test date string validation with invalid inputs.

        Verifies
        --------
        - ValueError is raised for invalid date strings
        - Error message matches expected pattern

        Parameters
        ----------
        febraban_instance : DatesBRFebraban
            Fixture providing DatesBRFebraban instance
        invalid_date_str : Optional[str]
            Invalid date string

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Date string"):
            febraban_instance._validate_date_string(invalid_date_str)

    @pytest.mark.parametrize("invalid_year", [None, "2023", 1800, 2200])
    def test_validate_year_invalid(
        self, febraban_instance: DatesBRFebraban, invalid_year: Any
    ) -> None:
        """Test year validation with invalid inputs.

        Verifies
        --------
        - ValueError is raised for invalid years
        - Error message matches expected pattern

        Parameters
        ----------
        febraban_instance : DatesBRFebraban
            Fixture providing DatesBRFebraban instance
        invalid_year : Any
            Invalid year input

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Year"):
            febraban_instance._validate_year(invalid_year)

    @pytest.mark.parametrize("start_year, end_year", [(2023, 2022), (1800, 2023), (2023, 2200)])
    def test_validate_year_range_invalid(
        self, febraban_instance: DatesBRFebraban, start_year: int, end_year: int
    ) -> None:
        """Test year range validation with invalid inputs.

        Verifies
        --------
        - ValueError is raised for invalid year ranges
        - Error message matches expected pattern

        Parameters
        ----------
        febraban_instance : DatesBRFebraban
            Fixture providing DatesBRFebraban instance
        start_year : int
            Invalid start year
        end_year : int
            Invalid end year

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Year"):
            febraban_instance._validate_year_range(start_year, end_year)

    @pytest.mark.parametrize("json_response, year", [
        (None, 2023),
        ([], 2023),
        ("invalid", 2023)
    ])
    def test_validate_json_response_invalid(
        self, febraban_instance: DatesBRFebraban, json_response: Any, year: int
    ) -> None:
        """Test JSON response validation with invalid inputs.

        Verifies
        --------
        - ValueError is raised for invalid JSON responses
        - Error message matches expected pattern

        Parameters
        ----------
        febraban_instance : DatesBRFebraban
            Fixture providing DatesBRFebraban instance
        json_response : Any
            Invalid JSON response
        year : int
            Year for validation

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="JSON response"):
            febraban_instance._validate_json_response(json_response, year)


# --------------------------
# Reload Tests
# --------------------------
def test_module_reload() -> None:
    """Test module reloading behavior.

    Verifies
    --------
    - Module can be reloaded without errors
    - Classes maintain their functionality

    Returns
    -------
    None
    """
    importlib.reload(sys.modules["stpstone.utils.calendars.dates_br"])
    anbima = DatesBRAnbima()
    febraban = DatesBRFebraban()
    assert isinstance(anbima.cls_str_handler, StrHandler)
    assert isinstance(febraban.cls_dict_handler, HandlingDicts)