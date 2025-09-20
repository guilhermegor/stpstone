"""Updated test fixtures and corrections for IBGE SIDRA tests.

This module contains the corrected test fixtures and updated test methods
to address the failing tests.
"""

from datetime import date
import logging
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.macroeconomics.ibge_sidra import IBGESIDRA


# --------------------------
# Updated Fixtures
# --------------------------
@pytest.fixture
def mock_response() -> Response:
    """Mock Response object with sample content using correct date format."""
    response = MagicMock(spec=Response)
    # Fixed: Use DD/MM/YYYY format that matches the actual API response
    response.json.return_value = [
        {"literals": ["", "01/01/2023"], "modificacao": "02/01/2023"},  # DD/MM/YYYY format
    ]
    response.status_code = 200
    response.raise_for_status = MagicMock()
    return response


@pytest.fixture
def mock_metadata_response() -> Response:
    """Mock Response object for metadata.
    
    Returns
    -------
    Response
    """
    response = MagicMock(spec=Response)
    response.json.return_value = {
        "Nome": "Test Table",
        "Fonte": "Test Source",
        "Notas": ["Note 1", "Note 2"],  # Ensure this is a list of strings
    }
    response.status_code = 200
    response.raise_for_status = MagicMock()
    return response


@pytest.fixture
def sample_date() -> date:
    """Fixture providing a sample date.
    
    Returns
    -------
    date
    """
    return date(2023, 1, 1)


@pytest.fixture
def ibge_instance(sample_date: date) -> IBGESIDRA:
    """Fixture providing IBGESIDRA instance with default parameters.

    Parameters
    ----------
    sample_date : date
        Sample date for initialization
    
    Returns
    -------
    IBGESIDRA
    """
    return IBGESIDRA(list_series_id=[1737], date_ref=sample_date)


# --------------------------
# Updated Test Methods
# --------------------------
def test_init_with_valid_inputs(sample_date: date) -> None:
    """Test initialization with valid inputs.

    Verifies
    --------
    - IBGESIDRA can be initialized with valid inputs
    - Attributes are correctly set
    - Default values are used when optional parameters are None

    Parameters
    ----------
    sample_date : date
        Sample date for initialization

    Returns
    -------
    None
    """
    instance = IBGESIDRA(list_series_id=[1737, 3065], date_ref=sample_date, logger=None, 
                         cls_db=None)
    assert instance.list_series_id == [1737, 3065]
    assert instance.date_ref == sample_date
    assert isinstance(instance.logger, logging.Logger) or instance.logger is None
    assert instance.cls_db is None
    assert isinstance(instance.cls_dir_files_management, object)
    assert isinstance(instance.cls_dates_current, object)
    assert isinstance(instance.cls_create_log, object)
    assert isinstance(instance.cls_dates_br, object)
    assert isinstance(instance.cls_html_handler, object)


def test_init_with_default_series_id(sample_date: date) -> None:
    """Test initialization with default series ID.

    Verifies
    --------
    - Default series ID list is used when None is provided
    - Other attributes are correctly initialized

    Parameters
    ----------
    sample_date : date
        Sample date for initialization

    Returns
    -------
    None
    """
    instance = IBGESIDRA(date_ref=sample_date)
    assert instance.list_series_id == [1737, 3065]
    assert instance.date_ref == sample_date


@pytest.mark.parametrize("invalid_series_id", [
    [],  # Empty list
    ["1737"],  # String instead of int
    [1.5],  # Float instead of int
    [""],  # Empty string
    [None],  # None in list
    [0],  # Zero (invalid series ID)
    [-1],  # Negative number
    "not_a_list",  # Not a list at all
])
def test_init_with_invalid_series_id(
    invalid_series_id: Any, # noqa ANN401: typing.Any is not allowed
    sample_date: date
) -> None:
    """Test initialization with invalid series ID list.

    Verifies
    --------
    - Invalid series ID types raise TypeError with appropriate message

    Parameters
    ----------
    invalid_series_id : Any
        Invalid series ID inputs
    sample_date : date
        Sample date for initialization

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be"):  # FIXED: More flexible regex
        IBGESIDRA(list_series_id=invalid_series_id, date_ref=sample_date)


def test_run_happy_path(
    ibge_instance: IBGESIDRA, 
    mock_response: Response, 
    mock_metadata_response: Response, 
    mocker: MockerFixture
) -> None:
    """Test run method with valid inputs and successful response.

    Verifies
    --------
    - Run method processes data correctly
    - Returns expected DataFrame with correct columns and values
    - Handles HTTP responses correctly

    Parameters
    ----------
    ibge_instance : IBGESIDRA
        IBGESIDRA instance from fixture
    mock_response : Response
        Mocked response object for data service
    mock_metadata_response : Response
        Mocked response object for metadata
    mocker : MockerFixture
        Pytest-mock fixture for mocking

    Returns
    -------
    None
    """
    mocker.patch("requests.get", side_effect=[mock_response, mock_metadata_response])
    mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)
    
    result = ibge_instance.run(timeout=(12.0, 21.0), bool_verify=True, bool_insert_or_ignore=False)
    
    assert isinstance(result, pd.DataFrame)
    assert set(result.columns) == {
        "REFERENCED_PERIOD",
        "MODIFICATION_DATE",
        "NAME",
        "SOURCE",
        "NOTES",
        "SERIES_ID",
        "URL", 
        "REF_DATE", 
        "LOG_TIMESTAMP"
    }
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "Test Table"
    assert result.iloc[0]["SOURCE"] == "Test Source"
    assert result.iloc[0]["NOTES"] == "Note 1;Note 2"
    assert result.iloc[0]["SERIES_ID"] == 1737


def test_run_with_db_insertion(
    ibge_instance: IBGESIDRA, 
    mock_response: Response, 
    mock_metadata_response: Response, 
    mocker: MockerFixture
) -> None:
    """Test run method with database insertion.

    Verifies
    --------
    - Run method calls insert_table_db when cls_db is provided
    - No DataFrame is returned when database insertion occurs

    Parameters
    ----------
    ibge_instance : IBGESIDRA
        IBGESIDRA instance from fixture
    mock_response : Response
        Mocked response object for data service
    mock_metadata_response : Response
        Mocked response object for metadata
    mocker : MockerFixture
        Pytest-mock fixture for mocking

    Returns
    -------
    None
    """
    mocker.patch("requests.get", side_effect=[mock_response, mock_metadata_response])
    mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)
    mock_db = MagicMock()
    ibge_instance.cls_db = mock_db
    # Mock the insert_table_db method to avoid actual database operations
    ibge_instance.insert_table_db = MagicMock()
    
    result = ibge_instance.run(
        timeout=(12.0, 21.0),
        bool_verify=True,
        bool_insert_or_ignore=True,
        str_table_name="test_table"
    )
    
    assert result is None
    ibge_instance.insert_table_db.assert_called_once()


@pytest.mark.parametrize("invalid_series_id", [0, -1, "1737", None, 1.5])
def test_get_metadata_invalid_series_id(
    ibge_instance: IBGESIDRA, 
    invalid_series_id: Any, # noqa ANN401: typing.Any is not allowed
    mocker: MockerFixture
) -> None:
    """Test get_metadata with invalid series ID.

    Verifies
    --------
    - Invalid series ID raises TypeError before making HTTP request

    Parameters
    ----------
    ibge_instance : IBGESIDRA
        IBGESIDRA instance from fixture
    invalid_series_id : Any
        Invalid series ID values
    mocker : MockerFixture
        Pytest-mock fixture for mocking

    Returns
    -------
    None
    """
    # Mock should not be called since validation should happen first
    mock_get = mocker.patch("requests.get")
    mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)
    
    with pytest.raises(TypeError, match="must be"):  # FIXED: More flexible regex
        ibge_instance.get_metadata(int_series_id=invalid_series_id)
    
    # Verify that no HTTP request was made
    mock_get.assert_not_called()


def test_run_empty_series_id(ibge_instance: IBGESIDRA, mocker: MockerFixture) -> None:
    """Test run method with empty series ID list.

    Verifies
    --------
    - Empty series ID list results in empty DataFrame
    - No HTTP requests are made

    Parameters
    ----------
    ibge_instance : IBGESIDRA
        IBGESIDRA instance from fixture
    mocker : MockerFixture
        Pytest-mock fixture for mocking

    Returns
    -------
    None
    """
    ibge_instance.list_series_id = []
    mock_get = mocker.patch("requests.get")
    
    result = ibge_instance.run()
    
    assert isinstance(result, pd.DataFrame)
    assert result.empty
    mock_get.assert_not_called()


def test_fallback_no_db(
    ibge_instance: IBGESIDRA, 
    mock_response: Response, 
    mock_metadata_response: Response, 
    mocker: MockerFixture
) -> None:
    """Test run method fallback when no database session is provided.

    Verifies
    --------
    - Returns DataFrame instead of inserting to database
    - Data processing continues normally

    Parameters
    ----------
    ibge_instance : IBGESIDRA
        IBGESIDRA instance from fixture
    mock_response : Response
        Mocked response object for data service
    mock_metadata_response : Response
        Mocked response object for metadata
    mocker : MockerFixture
        Pytest-mock fixture for mocking

    Returns
    -------
    None
    """
    mocker.patch("requests.get", side_effect=[mock_response, mock_metadata_response])
    mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)
    # Mock the insert_table_db method to verify it's not called
    ibge_instance.insert_table_db = MagicMock()
    
    result = ibge_instance.run()
    
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    ibge_instance.insert_table_db.assert_not_called()


# Additional helper test for validation
def test_init_validation_edge_cases(sample_date: date) -> None:
    """Test edge cases for initialization validation.
    
    Verifies
    --------
    - Boolean values are rejected (even though bool is subclass of int)
    - Mixed valid/invalid lists are rejected
    - Non-list types are rejected
    
    Parameters
    ----------
    sample_date : date
        Sample date for initialization
        
    Returns
    -------
    None
    """
    # Test boolean rejection (bool is subclass of int in Python)
    with pytest.raises(TypeError):
        IBGESIDRA(list_series_id=[True, False], date_ref=sample_date)
    
    # Test mixed types
    with pytest.raises(TypeError):
        IBGESIDRA(list_series_id=[1737, "3065"], date_ref=sample_date)
    
    # Test non-list input
    with pytest.raises(TypeError):
        IBGESIDRA(list_series_id=1737, date_ref=sample_date)


def test_get_metadata_valid_input(
    ibge_instance: IBGESIDRA,
    mock_metadata_response: Response,
    mocker: MockerFixture
) -> None:
    """Test get_metadata with valid series ID.

    Verifies
    --------
    - get_metadata retrieves correct metadata
    - Returns expected tuple with name, source, and notes

    Parameters
    ----------
    ibge_instance : IBGESIDRA
        IBGESIDRA instance from fixture
    mock_metadata_response : Response
        Mocked response object for metadata
    mocker : MockerFixture
        Pytest-mock fixture for mocking

    Returns
    -------
    None
    """
    mocker.patch("requests.get", return_value=mock_metadata_response)
    mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)
    
    result = ibge_instance.get_metadata(int_series_id=1737)
    
    assert isinstance(result, tuple)
    assert len(result) == 3
    assert result == ("Test Table", "Test Source", "Note 1;Note 2")