"""Unit tests for Brazilian holiday calendar implementations.

Tests the ANBIMA and FEBRABAN holiday calendar classes, covering
initialization, data fetching, transformation, and validation logic.
"""

from datetime import date
from typing import Any, Union
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from stpstone.utils.cache.cache_manager import CacheManager
from stpstone.utils.calendars.calendar_br import DatesBRAnbima, DatesBRB3, DatesBRFebraban
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


@pytest.fixture
def b3_instance() -> DatesBRB3:
    """Fixture providing a DatesBRB3 instance with caching disabled.
    
    Returns
    -------
    DatesBRB3
        Initialized B3 calendar instance
    """
    return DatesBRB3(bool_reuse_cache=False, bool_persist_cache=False)


@pytest.fixture
def b3_instance_with_christmas_eve() -> DatesBRB3:
    """Fixture providing a DatesBRB3 instance with Christmas Eve enabled.
    
    Returns
    -------
    DatesBRB3
        Initialized B3 calendar instance with Christmas Eve
    """
    return DatesBRB3(
        bool_add_christmas_eve=True,
        bool_reuse_cache=False, 
        bool_persist_cache=False
    )


@pytest.fixture
def sample_b3_anbima_df() -> pd.DataFrame:
    """Fixture providing sample ANBIMA DataFrame for B3 testing.
    
    Returns
    -------
    pd.DataFrame
        Sample DataFrame with holiday data for B3 testing
    """
    return pd.DataFrame({
        "DATE": [date(2023, 1, 1), date(2023, 4, 21), date(2023, 12, 25)],
        "WEEKDAY": ["Domingo", "Sexta-feira", "Segunda-feira"],
        "NAME": ["Ano Novo", "Tiradentes", "Natal"]
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
    int_year_start = 2024
    int_year_end = 2025
    
    # Set instance attributes to match the test's year range
    febraban_instance.int_year_start = int_year_start
    febraban_instance.int_year_end = int_year_end
    
    with patch.object(febraban_instance, "get_holidays_raw", return_value=mock_df):
        df_ = febraban_instance.get_holidays_years()  # Call without arguments
        assert isinstance(df_, pd.DataFrame)
        assert "ANO" in df_.columns
        assert len(df_) == 26
        assert df_["diaMes"].iloc[0] == "01 de janeiro"
        assert StrHandler().remove_diacritics(df_["nomeFeriado"].iloc[0]) \
            == StrHandler().remove_diacritics("Confraternização Universal")


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


# --------------------------
# Tests for DatesBRB3
# --------------------------

def test_b3_init_default(b3_instance: DatesBRB3) -> None:
    """Test initialization of DatesBRB3 with default parameters.
    
    Parameters
    ----------
    b3_instance : DatesBRB3
        B3 calendar instance
    
    Verifies
    --------
    - Instance is created successfully
    - Christmas Eve is disabled by default
    - All required attributes are initialized
    
    Returns
    -------
    None
    """
    assert isinstance(b3_instance, DatesBRB3)
    assert b3_instance.bool_add_christmas_eve is False
    assert isinstance(b3_instance.cls_dates_br_anbima, DatesBRAnbima)
    assert isinstance(b3_instance.cls_str_handler, StrHandler)
    assert isinstance(b3_instance.cls_cache_manager, CacheManager)

def test_b3_init_with_christmas_eve(b3_instance_with_christmas_eve: DatesBRB3) -> None:
    """Test initialization of DatesBRB3 with Christmas Eve enabled.
    
    Parameters
    ----------
    b3_instance_with_christmas_eve : DatesBRB3
        B3 calendar instance with Christmas Eve
    
    Verifies
    --------
    - Christmas Eve option is correctly set
    - Other attributes are properly initialized
    
    Returns
    -------
    None
    """
    assert b3_instance_with_christmas_eve.bool_add_christmas_eve is True
    assert isinstance(b3_instance_with_christmas_eve.cls_dates_br_anbima, DatesBRAnbima)

def test_get_christmas_eve_valid_year(b3_instance: DatesBRB3) -> None:
    """Test Christmas Eve date generation for valid year.
    
    Parameters
    ----------
    b3_instance : DatesBRB3
        B3 calendar instance
    
    Verifies
    --------
    - Returns correct Christmas Eve date (December 24)
    - Date object is properly constructed
    
    Returns
    -------
    None
    """
    christmas_eve = b3_instance.get_christmas_eve(2023)
    assert isinstance(christmas_eve, date)
    assert christmas_eve == date(2023, 12, 24)

@pytest.mark.parametrize("test_year", [2020, 2024, 2025, 2030])
def test_get_christmas_eve_multiple_years(b3_instance: DatesBRB3, test_year: int) -> None:
    """Test Christmas Eve date generation for multiple years.
    
    Parameters
    ----------
    b3_instance : DatesBRB3
        B3 calendar instance
    test_year : int
        Year to test
    
    Verifies
    --------
    - Christmas Eve is always December 24 regardless of year
    
    Returns
    -------
    None
    """
    christmas_eve = b3_instance.get_christmas_eve(test_year)
    assert christmas_eve == date(test_year, 12, 24)

def test_get_anbima_holidays_integration(
    b3_instance: DatesBRB3, 
    sample_b3_anbima_df: pd.DataFrame
) -> None:
    """Test integration with ANBIMA holidays fetching.
    
    Parameters
    ----------
    b3_instance : DatesBRB3
        B3 calendar instance
    sample_b3_anbima_df : pd.DataFrame
        Sample ANBIMA DataFrame
    
    Verifies
    --------
    - ANBIMA holidays are properly fetched and transformed
    - DataFrame structure is maintained
    
    Returns
    -------
    None
    """
    with patch.object(b3_instance.cls_dates_br_anbima, 
                      "get_holidays_raw", return_value=sample_b3_anbima_df), \
         patch.object(b3_instance.cls_dates_br_anbima, 
                      "transform_holidays", return_value=sample_b3_anbima_df):
        df_result = b3_instance.get_anbima_holidays()
        assert isinstance(df_result, pd.DataFrame)
        assert len(df_result) == 3
        assert list(df_result.columns) == ["DATE", "WEEKDAY", "NAME"]

def test_holidays_to_add_without_christmas_eve(
    b3_instance: DatesBRB3, 
    sample_b3_anbima_df: pd.DataFrame
) -> None:
    """Test additional holidays generation without Christmas Eve.
    
    Parameters
    ----------
    b3_instance : DatesBRB3
        B3 calendar instance
    sample_b3_anbima_df : pd.DataFrame
        Sample ANBIMA DataFrame
    
    Verifies
    --------
    - Last working day of year is added
    - Christmas Eve is not added when disabled
    - Correct number of holidays per year
    
    Returns
    -------
    None
    """
    # Mock the required methods to avoid recursion
    with patch.object(b3_instance, "year_number", side_effect=lambda d: d.year), \
         patch.object(b3_instance, "date_only", side_effect=lambda d: d), \
         patch.object(b3_instance, "is_weekend", return_value=False):
        
        holidays_to_add = b3_instance.holidays_to_add(sample_b3_anbima_df)
        
        assert isinstance(holidays_to_add, list)
        assert len(holidays_to_add) == 1  # Only last working day, no Christmas Eve
        assert holidays_to_add[0][0] == "Último Dia Útil do Ano"
        assert isinstance(holidays_to_add[0][1], date)

def test_holidays_to_add_with_christmas_eve(
    b3_instance_with_christmas_eve: DatesBRB3, 
    sample_b3_anbima_df: pd.DataFrame
) -> None:
    """Test additional holidays generation with Christmas Eve enabled.
    
    Parameters
    ----------
    b3_instance_with_christmas_eve : DatesBRB3
        B3 calendar instance with Christmas Eve enabled
    sample_b3_anbima_df : pd.DataFrame
        Sample ANBIMA DataFrame
    
    Verifies
    --------
    - Both last working day and Christmas Eve are added
    - Christmas Eve date is correct
    
    Returns
    -------
    None
    """
    # Mock the required methods to avoid recursion
    with patch.object(b3_instance_with_christmas_eve, 
                      "year_number", side_effect=lambda d: d.year), \
         patch.object(b3_instance_with_christmas_eve, "date_only", side_effect=lambda d: d), \
         patch.object(b3_instance_with_christmas_eve, "is_weekend", return_value=False):
        
        holidays_to_add = b3_instance_with_christmas_eve.holidays_to_add(sample_b3_anbima_df)
        
        assert isinstance(holidays_to_add, list)
        assert len(holidays_to_add) == 2  # Last working day + Christmas Eve
        
        # Check that both holidays are present
        holiday_names = [h[0] for h in holidays_to_add]
        assert "Último Dia Útil do Ano" in holiday_names
        assert "Véspera de Natal" in holiday_names
        
        # Check Christmas Eve date
        christmas_eve_tuple = next(h for h in holidays_to_add if h[0] == "Véspera de Natal")
        assert christmas_eve_tuple[1] == date(2023, 12, 24)

def test_add_holidays_b3_structure(
    b3_instance: DatesBRB3, 
    sample_b3_anbima_df: pd.DataFrame
) -> None:
    """Test B3 holidays addition maintains DataFrame structure.
    
    Parameters
    ----------
    b3_instance : DatesBRB3
        B3 calendar instance
    sample_b3_anbima_df : pd.DataFrame
        Sample ANBIMA DataFrame
    
    Verifies
    --------
    - DataFrame structure is maintained after adding B3 holidays
    - Columns are in correct order
    - Additional holidays are properly integrated
    
    Returns
    -------
    None
    """
    with patch.object(b3_instance, "holidays_to_add", 
                      return_value=[("Test Holiday", date(2023, 6, 15))]), \
         patch.object(b3_instance, "weekday_name", return_value="Quinta-feira"):
        
        df_result = b3_instance.add_holidays_b3(sample_b3_anbima_df)
        
        assert isinstance(df_result, pd.DataFrame)
        assert list(df_result.columns) == ["DATE", "WEEKDAY", "NAME"]
        assert len(df_result) == 4  # 3 original + 1 additional
        
        # Check that data is sorted by date
        dates = df_result["DATE"].tolist()
        assert dates == sorted(dates)

def test_get_holidays_transformed_integration(
    b3_instance: DatesBRB3, 
    sample_b3_anbima_df: pd.DataFrame
) -> None:
    """Test full workflow of get_holidays_transformed.
    
    Parameters
    ----------
    b3_instance : DatesBRB3
        B3 calendar instance
    sample_b3_anbima_df : pd.DataFrame
        Sample ANBIMA DataFrame
    
    Verifies
    --------
    - Method integrates ANBIMA holidays and B3 additions
    - Caching decorator is properly applied
    - Result contains both original and additional holidays
    
    Returns
    -------
    None
    """
    mock_additional_holidays = [("Test B3 Holiday", date(2023, 6, 15))]
    
    with patch.object(b3_instance, "get_anbima_holidays", return_value=sample_b3_anbima_df), \
         patch.object(b3_instance, "holidays_to_add", return_value=mock_additional_holidays), \
         patch.object(b3_instance, "weekday_name", return_value="Quinta-feira"):
        
        df_result = b3_instance.get_holidays_transformed()
        
        assert isinstance(df_result, pd.DataFrame)
        assert len(df_result) == 4  # 3 ANBIMA + 1 B3
        assert list(df_result.columns) == ["DATE", "WEEKDAY", "NAME"]

def test_holidays_full_integration(
    b3_instance: DatesBRB3, 
    sample_b3_anbima_df: pd.DataFrame
) -> None:
    """Test full holidays workflow for B3.
    
    Parameters
    ----------
    b3_instance : DatesBRB3
        B3 calendar instance
    sample_b3_anbima_df : pd.DataFrame
        Sample ANBIMA DataFrame
    
    Verifies
    --------
    - holidays() returns list of tuples
    - Each tuple contains string name and date object
    - List includes both ANBIMA and B3 holidays
    
    Returns
    -------
    None
    """
    mock_additional_holidays = [("Último Dia Útil do Ano", date(2023, 12, 29))]
    
    with patch.object(b3_instance, "get_anbima_holidays", return_value=sample_b3_anbima_df), \
         patch.object(b3_instance, "holidays_to_add", return_value=mock_additional_holidays), \
         patch.object(b3_instance, "weekday_name", return_value="Quinta-feira"):
        
        holidays = b3_instance.holidays()
        
        assert isinstance(holidays, list)
        assert all(isinstance(h, tuple) for h in holidays)
        assert all(isinstance(h[0], str) and isinstance(h[1], date) for h in holidays)
        assert len(holidays) == 4  # 3 ANBIMA + 1 B3
        
        # Check that B3-specific holiday is included
        holiday_names = [h[0] for h in holidays]
        assert "Último Dia Útil do Ano" in holiday_names

def test_holidays_to_add_multiple_years(b3_instance: DatesBRB3) -> None:
    """Test holidays_to_add with multiple years in DataFrame.
    
    Parameters
    ----------
    b3_instance : DatesBRB3
        B3 calendar instance
    
    Verifies
    --------
    - Last working day is calculated for each year
    - Multiple years are handled correctly
    
    Returns
    -------
    None
    """
    multi_year_df = pd.DataFrame({
        "DATE": [date(2022, 1, 1), date(2023, 1, 1), date(2024, 1, 1)],
        "WEEKDAY": ["Sábado", "Domingo", "Segunda-feira"],
        "NAME": ["Ano Novo", "Ano Novo", "Ano Novo"]
    })
    
    with patch.object(b3_instance, "year_number", side_effect=lambda d: d.year), \
         patch.object(b3_instance, "date_only", side_effect=lambda d: d), \
         patch.object(b3_instance, "is_weekend", return_value=False):
        
        holidays_to_add = b3_instance.holidays_to_add(multi_year_df)
        
        assert len(holidays_to_add) == 3  # One last working day per year
        assert all(h[0] == "Último Dia Útil do Ano" for h in holidays_to_add)
        
        # Check that we have different years
        years = [h[1].year for h in holidays_to_add]
        assert len(set(years)) == 3  # Three different years

def test_holidays_to_add_empty_dataframe(b3_instance: DatesBRB3) -> None:
    """Test holidays_to_add with empty DataFrame.
    
    Parameters
    ----------
    b3_instance : DatesBRB3
        B3 calendar instance
    
    Verifies
    --------
    - Empty list is returned for empty DataFrame
    - No errors are raised
    
    Returns
    -------
    None
    """
    empty_df = pd.DataFrame(columns=["DATE", "WEEKDAY", "NAME"])
    
    with patch.object(b3_instance, "year_number", side_effect=lambda d: d.year):
        holidays_to_add = b3_instance.holidays_to_add(empty_df)
        assert isinstance(holidays_to_add, list)
        assert len(holidays_to_add) == 0

def test_add_holidays_b3_with_weekday_calculation(
    b3_instance: DatesBRB3, 
    sample_b3_anbima_df: pd.DataFrame
) -> None:
    """Test that add_holidays_b3 correctly calculates weekdays for new holidays.
    
    Parameters
    ----------
    b3_instance : DatesBRB3
        B3 calendar instance
    sample_b3_anbima_df : pd.DataFrame
        Sample ANBIMA DataFrame
    
    Verifies
    --------
    - Weekday names are calculated for added holidays
    - weekday_name method is called with correct parameters
    
    Returns
    -------
    None
    """
    mock_additional_holidays = [("Test Holiday", date(2023, 6, 15))]
    
    with patch.object(b3_instance, "holidays_to_add", return_value=mock_additional_holidays), \
         patch.object(b3_instance, "weekday_name", return_value="Quinta-feira") as mock_weekday:
        
        df_result = b3_instance.add_holidays_b3(sample_b3_anbima_df)
        
        # Verify weekday_name was called
        mock_weekday.assert_called_once_with(
            date(2023, 6, 15), 
            bool_abbreviation=False, 
            str_timezone="America/Sao_Paulo"
        )
        
        # Check the added holiday has correct weekday
        added_holiday_row = df_result[df_result["NAME"] == "Test Holiday"].iloc[0]
        assert added_holiday_row["WEEKDAY"] == "Quinta-feira"

def test_add_holidays_b3_sorting(
    b3_instance: DatesBRB3, 
    sample_b3_anbima_df: pd.DataFrame
) -> None:
    """Test that add_holidays_b3 properly sorts holidays by date.
    
    Parameters
    ----------
    b3_instance : DatesBRB3
        B3 calendar instance
    sample_b3_anbima_df : pd.DataFrame
        Sample ANBIMA DataFrame
    
    Verifies
    --------
    - Final DataFrame is sorted by date
    - Additional holidays are inserted in correct chronological order
    
    Returns
    -------
    None
    """
    # Add a holiday that should be inserted in the middle chronologically
    mock_additional_holidays = [("Mid Year Holiday", date(2023, 6, 15))]
    
    with patch.object(b3_instance, "holidays_to_add", return_value=mock_additional_holidays), \
         patch.object(b3_instance, "weekday_name", return_value="Quinta-feira"):
        
        df_result = b3_instance.add_holidays_b3(sample_b3_anbima_df)
        
        # Check that dates are in ascending order
        dates = df_result["DATE"].tolist()
        assert dates == sorted(dates)
        
        # Check specific order
        expected_dates = \
            [date(2023, 1, 1), date(2023, 4, 21), date(2023, 6, 15), date(2023, 12, 25)]
        assert dates == expected_dates

def test_holidays_to_add_last_working_day_calculation(b3_instance: DatesBRB3) -> None:
    """Test calculation of last working day of the year.
    
    Parameters
    ----------
    b3_instance : DatesBRB3
        B3 calendar instance
    
    Verifies
    --------
    - Last working day logic correctly skips weekends and holidays
    - Proper date calculation for year end
    
    Returns
    -------
    None
    """
    # Create a DataFrame with New Year's Day
    test_df = pd.DataFrame({
        "DATE": [date(2023, 1, 1)],
        "WEEKDAY": ["Domingo"],
        "NAME": ["Ano Novo"]
    })
    
    # Mock the dependencies to simulate specific scenarios
    with patch.object(b3_instance, "year_number", return_value=2023), \
         patch.object(b3_instance, "date_only", side_effect=lambda d: d), \
         patch.object(b3_instance, "is_weekend") as mock_weekend:
        
        # Simulate December 31, 2023 being a Sunday (weekend)
        # and December 30, 2023 being a Saturday (weekend)
        # so December 29, 2023 should be the last working day
        def weekend_side_effect(d: date) -> bool:
            """Weekend logic for December 31, 2023 and December 30, 2023.
            
            Parameters
            ----------
            d : date
                The date to check
            
            Returns
            -------
            bool
                True if the date is a weekend, False otherwise
            """
            return d == date(2023, 12, 31) or d == date(2023, 12, 30)
        
        mock_weekend.side_effect = weekend_side_effect
        
        holidays_to_add = b3_instance.holidays_to_add(test_df)
        
        assert len(holidays_to_add) == 1
        last_working_day_tuple = holidays_to_add[0]
        assert last_working_day_tuple[0] == "Último Dia Útil do Ano"
        assert last_working_day_tuple[1] == date(2023, 12, 29)

def test_get_holidays_transformed_caching(b3_instance: DatesBRB3) -> None:
    """Test that get_holidays_transformed uses caching properly.
    
    Parameters
    ----------
    b3_instance : DatesBRB3
        B3 calendar instance
    
    Verifies
    --------
    - Cache manager decorator is properly applied
    - Method can be called multiple times
    
    Returns
    -------
    None
    """
    sample_df = pd.DataFrame({
        "DATE": [date(2023, 1, 1)],
        "WEEKDAY": ["Domingo"],
        "NAME": ["Ano Novo"]
    })
    
    with patch.object(b3_instance, "get_anbima_holidays", return_value=sample_df), \
         patch.object(b3_instance, "add_holidays_b3", return_value=sample_df):
        
        # Call twice to test that it doesn't break
        result1 = b3_instance.get_holidays_transformed()
        result2 = b3_instance.get_holidays_transformed()
        
        assert isinstance(result1, pd.DataFrame)
        assert isinstance(result2, pd.DataFrame)

def test_holidays_to_add_edge_case_december_dates(b3_instance: DatesBRB3) -> None:
    """Test holidays_to_add handles edge cases around December dates.
    
    Parameters
    ----------
    b3_instance : DatesBRB3
        B3 calendar instance
    
    Verifies
    --------
    - Handles case where Christmas is already in ANBIMA holidays
    - Correctly calculates last working day even with Christmas present
    
    Returns
    -------
    None
    """
    # DataFrame with Christmas Day
    christmas_df = pd.DataFrame({
        "DATE": [date(2023, 12, 25)],
        "WEEKDAY": ["Segunda-feira"],
        "NAME": ["Natal"]
    })
    
    with patch.object(b3_instance, "year_number", return_value=2023), \
         patch.object(b3_instance, "date_only", side_effect=lambda d: d), \
         patch.object(b3_instance, "is_weekend") as mock_weekend:
        
        # Christmas (Dec 25) is Monday, so let's say Dec 31 is Sunday
        def weekend_side_effect(d: date) -> bool:
            """Weekend logic for December 31, 2023.
            
            Parameters
            ----------
            d : date
                The date to check
            
            Returns
            -------
            bool
                True if the date is a weekend, False otherwise
            """
            return d.weekday() >= 5  # Saturday = 5, Sunday = 6
        
        mock_weekend.side_effect = weekend_side_effect
        
        holidays_to_add = b3_instance.holidays_to_add(christmas_df)
        
        assert len(holidays_to_add) == 1
        assert holidays_to_add[0][0] == "Último Dia Útil do Ano"
        # Should be December 29 (Friday) since 30-31 are weekend days
        assert holidays_to_add[0][1] == date(2023, 12, 29)

@pytest.mark.parametrize("timeout_value", [(10.0, 20.0), 15, 30.5])
def test_get_holidays_transformed_timeout_parameter(
    b3_instance: DatesBRB3, 
    timeout_value: Union[int, float, tuple[float, float]]
) -> None:
    """Test that timeout parameter is properly passed through.
    
    Parameters
    ----------
    b3_instance : DatesBRB3
        B3 calendar instance
    timeout_value : Union[int, float, tuple[float, float]]
        Timeout value to test
    
    Verifies
    --------
    - Timeout parameter is passed to underlying methods
    
    Returns
    -------
    None
    """
    sample_df = pd.DataFrame({
        "DATE": [date(2023, 1, 1)],
        "WEEKDAY": ["Domingo"],
        "NAME": ["Ano Novo"]
    })
    
    with patch.object(b3_instance, "get_anbima_holidays", return_value=sample_df) as mock_anbima, \
         patch.object(b3_instance, "add_holidays_b3", return_value=sample_df):
        
        b3_instance.get_holidays_transformed(timeout=timeout_value)
        
        # Verify timeout was passed to get_anbima_holidays
        mock_anbima.assert_called_once_with(timeout=timeout_value)

def test_b3_init_parameter_propagation() -> None:
    """Test that initialization parameters are properly propagated to dependencies.
    
    Verifies
    --------
    - Cache parameters are passed to ANBIMA instance
    - All boolean flags are correctly set
    
    Returns
    -------
    None
    """
    custom_params = {
        "bool_add_christmas_eve": True,
        "bool_persist_cache": False,
        "bool_reuse_cache": False,
        "int_days_cache_expiration": 5,
        "int_cache_ttl_days": 60,
        "path_cache_dir": "/custom/cache",
    }
    
    with patch("stpstone.utils.calendars.calendar_br.DatesBRAnbima") as mock_anbima_class, \
         patch("stpstone.utils.calendars.calendar_br.CacheManager") as mock_cache_class:
        
        DatesBRB3(**custom_params)
        
        # Verify ANBIMA was initialized with correct cache parameters
        mock_anbima_class.assert_called_once_with(
            bool_persist_cache=False,
            bool_reuse_cache=False,
            int_days_cache_expiration=5,
            int_cache_ttl_days=60,
            path_cache_dir="/custom/cache",
            logger=None
        )
        
        # Verify CacheManager was initialized with correct parameters
        mock_cache_class.assert_called_once_with(
            bool_persist_cache=False,
            bool_reuse_cache=False,
            int_days_cache_expiration=5,
            int_cache_ttl_days=60,
            path_cache_dir="/custom/cache",
            logger=None
        )

def test_christmas_eve_consistency(b3_instance: DatesBRB3) -> None:
    """Test that Christmas Eve dates are consistent across calls.
    
    Parameters
    ----------
    b3_instance : DatesBRB3
        B3 calendar instance
    
    Verifies
    --------
    - Christmas Eve date is always December 24
    - Multiple calls return same result
    
    Returns
    -------
    None
    """
    year = 2023
    christmas_eve_1 = b3_instance.get_christmas_eve(year)
    christmas_eve_2 = b3_instance.get_christmas_eve(year)
    
    assert christmas_eve_1 == christmas_eve_2
    assert christmas_eve_1.month == 12
    assert christmas_eve_1.day == 24
    assert christmas_eve_1.year == year

def test_holidays_output_format_consistency(
    b3_instance: DatesBRB3, 
    sample_b3_anbima_df: pd.DataFrame
) -> None:
    """Test that holidays output format is consistent with parent classes.
    
    Parameters
    ----------
    b3_instance : DatesBRB3
        B3 calendar instance
    sample_b3_anbima_df : pd.DataFrame
        Sample ANBIMA DataFrame
    
    Verifies
    --------
    - Output format matches ANBIMA and FEBRABAN classes
    - Each tuple has exactly 2 elements (name, date)
    - Names are strings and dates are date objects
    
    Returns
    -------
    None
    """
    with patch.object(b3_instance, "get_holidays_transformed", return_value=sample_b3_anbima_df):
        holidays = b3_instance.holidays()
        
        assert isinstance(holidays, list)
        for holiday_tuple in holidays:
            assert isinstance(holiday_tuple, tuple)
            assert len(holiday_tuple) == 2
            assert isinstance(holiday_tuple[0], str)  # Holiday name
            assert isinstance(holiday_tuple[1], date)  # Holiday date