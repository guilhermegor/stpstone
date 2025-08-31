"""Unit tests for Brazilian holiday calendar implementations.

Tests the ANBIMA and FEBRABAN holiday calendar classes, covering
initialization, data fetching, transformation, and validation logic.
"""

from datetime import date, timedelta
from typing import Any
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from stpstone.utils.calendars.calendar_br import DatesBRAnbima, DatesBRFebraban
from stpstone.utils.parsers.str import StrHandler


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def anbima_instance() -> DatesBRAnbima:
    """Fixture providing a DatesBRAnbima instance with caching disabled.

    Returns
    -------
    DatesBRAnbima
        Initialized ANBIMA calendar instance
    """
    return DatesBRAnbima(bool_reuse_cache=False, bool_persist_cache=False)


@pytest.fixture
def febraban_instance() -> DatesBRFebraban:
    """Fixture providing a DatesBRFebraban instance with default years.

    Returns
    -------
    DatesBRFebraban
        Initialized FEBRABAN calendar instance
    """
    return DatesBRFebraban()


@pytest.fixture
def sample_anbima_df() -> pd.DataFrame:
    """Fixture providing sample ANBIMA DataFrame.

    Returns
    -------
    pd.DataFrame
        Sample DataFrame with holiday data
    """
    return pd.DataFrame({
        "DATE": ["2023-01-01", "2023-04-21", "Fonte: ANBIMA"],
        "WEEKDAY": ["Domingo", "Sexta-feira", ""],
        "NAME": ["Ano Novo", "Tiradentes", ""]
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
        {"diaMes": "21 de abril", "diaSemana": "Sexta-feira", "nomeFeriado": "Tiradentes"}
    ]


@pytest.fixture
def sample_febraban_df() -> pd.DataFrame:
    """Fixture providing sample FEBRABAN DataFrame.

    Returns
    -------
    pd.DataFrame
        Sample DataFrame with holiday data
    """
    return pd.DataFrame({
        "diaMes": ["1 de janeiro", "21 de abril"],
        "diaSemana": ["Domingo", "Sexta-feira"],
        "nomeFeriado": ["Ano Novo", "Tiradentes"],
        "ANO": [2023, 2023]
    })


# --------------------------
# Tests for DatesBRAnbima
# --------------------------
def test_anbima_init(anbima_instance: DatesBRAnbima) -> None:
    """Test initialization of DatesBRAnbima.

    Parameters
    ----------
    anbima_instance : DatesBRAnbima
        ANBIMA calendar instance

    Verifies
    --------
    - Instance is created successfully
    - cls_str_handler is properly initialized

    Returns
    -------
    None
    """
    assert isinstance(anbima_instance, DatesBRAnbima)
    assert isinstance(anbima_instance.cls_str_handler, StrHandler)


@patch("requests.get")
def test_anbima_get_holidays_raw_success(
    mock_get: Mock, anbima_instance: DatesBRAnbima, sample_anbima_df: pd.DataFrame
) -> None:
    """Test successful fetching of raw ANBIMA holiday data.

    Verifies
    --------
    - HTTP request is made with correct headers
    - Response content is processed into DataFrame
    - Correct column names are set

    Parameters
    ----------
    mock_get : Mock
        Mocked requests.get function
    anbima_instance : DatesBRAnbima
        ANBIMA calendar instance
    sample_anbima_df : pd.DataFrame
        Sample ANBIMA DataFrame

    Returns
    -------
    None
    """
    mock_response = Mock()
    mock_response.content = b"dummy_excel_data"
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    with patch("pandas.read_excel", return_value=sample_anbima_df):
        df_ = anbima_instance.get_holidays_raw_cached()
        assert isinstance(df_, pd.DataFrame)
        assert list(df_.columns) == ["DATE", "WEEKDAY", "NAME"]
        mock_get.assert_called_once()
        assert mock_get.call_args[1]["headers"]["accept"] == (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,"
            "application/signed-exchange;v=b3;q=0.7"
        )


@patch("requests.get")
def test_get_holidays_raw_empty_content(
    mock_get: Mock, anbima_instance: DatesBRAnbima
) -> None:
    """Test handling of empty response content in get_holidays_raw.

    Verifies
    --------
    - ValueError is raised for empty content
    - Error message contains expected text

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
    mock_response.content = b""
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    with pytest.raises(ValueError, match="Response content cannot be empty"):
        anbima_instance.get_holidays_raw_cached()


def test_anbima_transform_holidays_valid(
    anbima_instance: DatesBRAnbima, sample_anbima_df: pd.DataFrame
) -> None:
    """Test transformation of valid ANBIMA holiday data.

    Verifies
    --------
    - DataFrame is properly transformed
    - Footer is removed
    - Column types are correct
    - Dates are converted properly

    Parameters
    ----------
    anbima_instance : DatesBRAnbima
        ANBIMA calendar instance
    sample_anbima_df : pd.DataFrame
        Sample ANBIMA DataFrame

    Returns
    -------
    None
    """
    with patch.object(anbima_instance, "timestamp_to_date", return_value=date(2023, 1, 1)):
        df_ = anbima_instance.transform_holidays(sample_anbima_df)
        assert isinstance(df_, pd.DataFrame)
        assert len(df_) == 2
        assert df_["DATE"].iloc[0] == date(2023, 1, 1)
        assert all(df_[col].dtype == "object" for col in ["WEEKDAY", "NAME"])


def test_transform_holidays_empty_df(anbima_instance: DatesBRAnbima) -> None:
    """Test transformation with empty DataFrame.

    Verifies
    --------
    - ValueError is raised for empty DataFrame
    - Error message contains expected text

    Parameters
    ----------
    anbima_instance : DatesBRAnbima
        ANBIMA calendar instance

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="df_holidays_raw cannot be empty"):
        anbima_instance.transform_holidays(pd.DataFrame())


@pytest.mark.parametrize("invalid_df", [None, "not_a_dataframe"])
def test_validate_dataframe_invalid(
    anbima_instance: DatesBRAnbima, 
    invalid_df: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test validation of invalid DataFrame inputs.

    Verifies
    --------
    - TypeError is raised for None or non-DataFrame inputs
    - Error message contains expected text

    Parameters
    ----------
    anbima_instance : DatesBRAnbima
        ANBIMA calendar instance
    invalid_df : Any
        Invalid DataFrame input (None or non-DataFrame)

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="df_ must be of type DataFrame, got (NoneType|str)"):
        anbima_instance._validate_dataframe(invalid_df, "test_df")


def test_remove_footer_valid(
    anbima_instance: DatesBRAnbima, sample_anbima_df: pd.DataFrame
) -> None:
    """Test footer removal from ANBIMA DataFrame.

    Verifies
    --------
    - Footer rows are correctly removed
    - Remaining data is intact

    Parameters
    ----------
    anbima_instance : DatesBRAnbima
        ANBIMA calendar instance
    sample_anbima_df : pd.DataFrame
        Sample ANBIMA DataFrame

    Returns
    -------
    None
    """
    df_ = anbima_instance._remove_footer(sample_anbima_df)
    assert len(df_) == 2
    assert "Fonte: ANBIMA" not in df_["DATE"].to_numpy()


def test_anbima_holidays_integration(
    anbima_instance: DatesBRAnbima, 
    sample_anbima_df: pd.DataFrame
) -> None:
    """Test full holidays workflow for ANBIMA.

    Verifies
    --------
    - holidays() returns list of tuples
    - Each tuple contains string name and date object
    - List is not empty

    Parameters
    ----------
    anbima_instance : DatesBRAnbima
        ANBIMA calendar instance
    sample_anbima_df : pd.DataFrame
        Sample ANBIMA DataFrame

    Returns
    -------
    None
    """
    with patch.object(anbima_instance, "get_holidays_raw", return_value=sample_anbima_df), \
        patch.object(anbima_instance, "timestamp_to_date", return_value=date(2023, 1, 1)):
            holidays = anbima_instance.holidays()
            assert isinstance(holidays, list)
            assert all(isinstance(h, tuple) for h in holidays)
            assert all(isinstance(h[0], str) and isinstance(h[1], date) for h in holidays)
            assert len(holidays) == 2


# --------------------------
# Tests for DatesBRFebraban
# --------------------------
def test_febraban_init(febraban_instance: DatesBRFebraban) -> None:
    """Test initialization of DatesBRFebraban.

    Verifies
    --------
    - Instance is created with correct year range
    - cls_str_handler and cls_dict_handler are initialized

    Parameters
    ----------
    febraban_instance : DatesBRFebraban
        FEBRABAN calendar instance

    Returns
    -------
    None
    """
    assert isinstance(febraban_instance, DatesBRFebraban)
    assert isinstance(febraban_instance.cls_str_handler, StrHandler)


@patch("requests.get")
def test_febraban_get_holidays_raw_success(
    mock_get: Mock, febraban_instance: DatesBRFebraban, sample_febraban_json: list[dict]
) -> None:
    """Test successful fetching of raw FEBRABAN holiday data.

    Verifies
    --------
    - HTTP request is made with correct headers and cookies
    - Response JSON is processed into a DataFrame
    - DataFrame contains expected columns and data

    Parameters
    ----------
    mock_get : Mock
        Mocked requests.get function
    febraban_instance : DatesBRFebraban
        FEBRABAN calendar instance
    sample_febraban_json : list[dict]
        Sample FEBRABAN JSON response

    Returns
    -------
    None
    """
    mock_response = Mock()
    mock_response.json.return_value = sample_febraban_json
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = febraban_instance.get_holidays_raw(2023)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == ["diaMes", "diaSemana", "nomeFeriado"]
    assert result["diaMes"].iloc[0] == "1 de janeiro"
    assert result["nomeFeriado"].iloc[0] == "Ano Novo"
    mock_get.assert_called_once()
    assert mock_get.call_args[1]["headers"]["Accept"] == (
        "application/json, text/javascript, */*; q=0.01"
    )


def test_febraban_transform_holidays_valid(
    febraban_instance: DatesBRFebraban, sample_febraban_df: pd.DataFrame
) -> None:
    """Test transformation of valid FEBRABAN holiday data.

    Verifies
    --------
    - DataFrame is properly transformed
    - Column names are converted to upper constant case
    - Dates are parsed correctly

    Parameters
    ----------
    febraban_instance : DatesBRFebraban
        FEBRABAN calendar instance
    sample_febraban_df : pd.DataFrame
        Sample FEBRABAN DataFrame

    Returns
    -------
    None
    """
    with patch.object(febraban_instance, "_parse_brazillian_date", return_value=date(2023, 1, 1)):
        df_ = febraban_instance.transform_holidays(sample_febraban_df)
        assert isinstance(df_, pd.DataFrame)
        assert len(df_) == 2
        assert list(df_.columns) == ["DIA_MES", "DIA_SEMANA", "NOME_FERIADO", "ANO", "DIA_MES_ANO"]
        assert df_["DIA_MES_ANO"].iloc[0] == date(2023, 1, 1)


@pytest.mark.parametrize("invalid_year", [1899, 2101, "2023", None])
def test_validate_year_invalid(
    febraban_instance: DatesBRFebraban, 
    invalid_year: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test validation of invalid year inputs.

    Verifies
    --------
    - ValueError is raised for years outside 1900-2100
    - TypeError is raised for non-integer inputs

    Parameters
    ----------
    febraban_instance : DatesBRFebraban
        FEBRABAN calendar instance
    invalid_year : Any
        Invalid year value

    Returns
    -------
    None
    """
    with pytest.raises((ValueError, TypeError), match="Year must be|year must be of type int"):
        febraban_instance._validate_year(invalid_year)


@pytest.mark.parametrize(
    "start_year, end_year",
    [(2024, 2023), (2101, 2023), ("2023", 2023)]
)
def test_validate_year_range_invalid(
    febraban_instance: DatesBRFebraban, 
    start_year: Any, # noqa ANN401: typing.Any is not allowed
    end_year: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test validation of invalid year ranges.

    Verifies
    --------
    - ValueError is raised for invalid ranges
    - TypeError is raised for non-integer inputs

    Parameters
    ----------
    febraban_instance : DatesBRFebraban
        FEBRABAN calendar instance
    start_year : Any
        Start year of range
    end_year : Any
        End year of range

    Returns
    -------
    None
    """
    with pytest.raises(
        (ValueError, TypeError), 
        match="Year must be between|must be of type|Start year .* cannot be after"
    ):
        febraban_instance._validate_year_range(start_year, end_year)


@pytest.mark.parametrize("invalid_date", [None, "", "1 janeiro", 123])
def test_validate_date_string_invalid(
    febraban_instance: DatesBRFebraban, 
    invalid_date: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test validation of invalid date string formats.

    Verifies
    --------
    - ValueError is raised for empty or malformed date strings
    - TypeError is raised for non-string inputs

    Parameters
    ----------
    febraban_instance : DatesBRFebraban
        FEBRABAN calendar instance
    invalid_date : Any
        Invalid date string

    Returns
    -------
    None
    """
    with pytest.raises((ValueError, TypeError), match="Date string|date_str must be of type str"):
        febraban_instance._validate_date_string(invalid_date)


def test_parse_brazillian_date_valid(febraban_instance: DatesBRFebraban) -> None:
    """Test parsing of valid Brazilian date string.

    Verifies
    --------
    - Date string is correctly parsed into date object
    - Month mapping works correctly

    Parameters
    ----------
    febraban_instance : DatesBRFebraban
        FEBRABAN calendar instance

    Returns
    -------
    None
    """
    result = febraban_instance._parse_brazillian_date("1 de janeiro", 2023)
    assert isinstance(result, date)
    assert result == date(2023, 1, 1)


@pytest.mark.parametrize("invalid_date", ["32 de janeiro", "1 de invalid", "abc"])
def test_parse_brazillian_date_invalid(
    febraban_instance: DatesBRFebraban, invalid_date: str
) -> None:
    """Test parsing of invalid Brazilian date strings.

    Verifies
    --------
    - ValueError is raised for invalid date formats
    - Error message contains expected text

    Parameters
    ----------
    febraban_instance : DatesBRFebraban
        FEBRABAN calendar instance
    invalid_date : str
        Invalid date string

    Returns
    -------
    None
    """
    with pytest.raises(
        ValueError, 
        match="Invalid date format|Date string must contain ' de ' separator"
    ):
        febraban_instance._parse_brazillian_date(invalid_date, 2023)


def test_get_holidays_years_valid(
    febraban_instance: DatesBRFebraban, sample_febraban_json: list[dict]
) -> None:
    """Test fetching holiday data for multiple years.

    Verifies
    --------
    - Data is fetched for each year in range
    - DataFrame is correctly constructed
    - Year column is added

    Parameters
    ----------
    febraban_instance : DatesBRFebraban
        FEBRABAN calendar instance
    sample_febraban_json : list[dict]
        Sample FEBRABAN JSON response

    Returns
    -------
    None
    """
    # Convert sample_febraban_json to DataFrame for mocking
    mock_df = pd.DataFrame(sample_febraban_json)
    
    # Define the year range for the test (default values from get_holidays_years)
    int_year_start = (date.today() - timedelta(days=22)).year - 1
    int_year_end = (date.today() - timedelta(days=22)).year
    
    # Set instance attributes to match the test's year range
    febraban_instance.int_year_start = int_year_start
    febraban_instance.int_year_end = int_year_end
    
    with patch.object(febraban_instance, "get_holidays_raw", return_value=mock_df):
        df_ = febraban_instance.get_holidays_years()  # Call without arguments
        assert isinstance(df_, pd.DataFrame)
        assert "ANO" in df_.columns
        # Expect 2 holidays per year (from sample_febraban_json) times the number of years
        assert len(df_) == 2 * (int_year_end - int_year_start + 1)
        assert df_["diaMes"].iloc[0] == "1 de janeiro"
        assert df_["nomeFeriado"].iloc[0] == "Ano Novo"


def test_febraban_holidays_integration(
    febraban_instance: DatesBRFebraban, 
    sample_febraban_df: pd.DataFrame
) -> None:
    """Test full holidays workflow for FEBRABAN.

    Verifies
    --------
    - holidays() returns list of tuples
    - Each tuple contains string name and date object
    - List is not empty

    Parameters
    ----------
    febraban_instance : DatesBRFebraban
        FEBRABAN calendar instance
    sample_febraban_df : pd.DataFrame
        Sample FEBRABAN DataFrame

    Returns
    -------
    None
    """
    with patch.object(febraban_instance, "get_holidays_years", return_value=sample_febraban_df), \
        patch.object(febraban_instance, "_parse_brazillian_date", return_value=date(2023, 1, 1)):
            holidays = febraban_instance.holidays()
            assert isinstance(holidays, list)
            assert all(isinstance(h, tuple) for h in holidays)
            assert all(isinstance(h[0], str) and isinstance(h[1], date) for h in holidays)
            assert len(holidays) == 2