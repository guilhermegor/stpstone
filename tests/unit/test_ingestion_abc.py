"""Unit tests for ingestion operations in the stpstone package.

Tests the functionality of ABCIngestion, CoreIngestion, and ABCIngestionOperations
classes, covering initialization, DataFrame standardization, database insertion,
and abstract method implementations.
"""

from datetime import date
from typing import Optional, Union
from unittest.mock import Mock

import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import pytest
from pytest_mock import MockerFixture
from requests import Response
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.abc.ingestion_abc import (
    ABCIngestion,
    ABCIngestionOperations,
    CoreIngestion,
)
from stpstone.transformations.standardization.standardizer_df import DFStandardization
from stpstone.utils.loggs.db_logs import DBLogs


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response() -> Mock:
    """Fixture providing a mock response object.

    Returns
    -------
    Mock
        Mock object simulating a response
    """
    return Mock()


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Fixture providing a sample DataFrame for testing.

    Returns
    -------
    pd.DataFrame
        DataFrame with sample data
    """
    return pd.DataFrame({
        "col1": ["A", "B", "C"],
        "col2": [1, 2, 3],
        "col3": [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)]
    })


@pytest.fixture
def core_ingestion() -> CoreIngestion:
    """Fixture providing a CoreIngestion instance with default parameters.

    Returns
    -------
    CoreIngestion
        Initialized CoreIngestion instance
    """
    return CoreIngestion(
        url="https://example.com",
        date_ref=date(2023, 1, 1),
        dict_dtypes={"col1": str, "col2": int, "col3": date}
    )


@pytest.fixture
def mock_db_session(mocker: MockerFixture) -> Mock:
    """Fixture providing a mock database session.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    Mock
        Mock object simulating a database session
    """
    mock_session = Mock()
    mock_session.insert = Mock()
    return mock_session


# --------------------------
# Tests for ABCIngestion
# --------------------------
def test_abc_ingestion_cannot_instantiate_without_implementing_methods() -> None:
    """Test that ABCIngestion cannot be instantiated without implementing abstract methods.

    Verifies
    --------
    - Cannot create instance without implementing get_response
    - Cannot create instance without implementing transform_response

    Returns
    -------
    None
    """
    # Test missing get_response implementation
    class TestIngestionMissingGetResponse(ABCIngestion):
        """Missing get_response implementation."""

        def transform_response(
            self, 
            resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
        ) -> pd.DataFrame:
            """Missing transform_response implementation.
            
            Parameters
            ----------
            resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
                The response object.
            
            Returns
            -------
            pd.DataFrame
                The transformed DataFrame.
            """
            return pd.DataFrame()

    with pytest.raises(TypeError, match="Can't instantiate abstract class"):
        TestIngestionMissingGetResponse()

    # Test missing transform_response implementation  
    class TestIngestionMissingTransformResponse(ABCIngestion):
        """Missing transform_response implementation."""

        def get_response(self) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
            """Missing get_response implementation.
            
            Returns
            -------
            Union[Response, PlaywrightPage, SeleniumWebDriver]
                The response object.
            """
            return Mock()

    with pytest.raises(TypeError, match="Can't instantiate abstract class"):
        TestIngestionMissingTransformResponse()


def test_abc_ingestion_can_instantiate_with_all_methods() -> None:
    """Test that ABCIngestion can be instantiated when all methods are implemented.

    Returns
    -------
    None
    """
    class TestIngestion(ABCIngestion):
        """Test implementation of ABCIngestion."""

        def get_response(self) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
            """Test get_response implementation.
            
            Returns
            -------
            Union[Response, PlaywrightPage, SeleniumWebDriver]
                The response object.
            """
            return Mock()
        
        def transform_response(
            self, 
            resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
        ) -> pd.DataFrame:
            """Test transform_response implementation.
            
            Parameters
            ----------
            resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
                The response object.
            
            Returns
            -------
            pd.DataFrame
                The transformed DataFrame.
            """
            return pd.DataFrame()

    # Should not raise any exception
    ingestion = TestIngestion()
    assert isinstance(ingestion, ABCIngestion)


# --------------------------
# Tests for CoreIngestion
# --------------------------
def test_core_ingestion_init_valid_inputs(core_ingestion: CoreIngestion) -> None:
    """Test initialization of CoreIngestion with valid inputs.

    Verifies
    --------
    - Instance is created with correct attribute values
    - DFStandardization and DBLogs are properly initialized
    - Type hints are respected

    Parameters
    ----------
    core_ingestion : CoreIngestion
        CoreIngestion instance from fixture

    Returns
    -------
    None
    """
    assert core_ingestion.url == "https://example.com"
    assert core_ingestion.date_ref == date(2023, 1, 1)
    assert core_ingestion.bool_format_log_as_str is True
    assert isinstance(core_ingestion.cls_df_standardization, DFStandardization)
    assert isinstance(core_ingestion.cls_db_logs, DBLogs)
    assert core_ingestion.cls_df_standardization.dict_dtypes == {
        "col1": str,
        "col2": int,
        "col3": date
    }


@pytest.mark.parametrize(
    "url", [None, 123, [], {}]
)
def test_core_ingestion_invalid_url(url: Optional[Union[str, int, list, dict]]) -> None:
    """Test initialization with invalid URL raises TypeError.

    Parameters
    ----------
    url : Optional[Union[str, int, list, dict]]
        Invalid URL values to test

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="url must be of type"):
        CoreIngestion(
            url=url,
            date_ref=date(2023, 1, 1),
            dict_dtypes={"col1": str}
        )


def test_core_ingestion_empty_url_allowed() -> None:
    """Test that empty strings and whitespace-only URLs are allowed.

    Returns
    -------
    None
    """
    # Empty string should be allowed
    ingestion = CoreIngestion(
        url="",
        date_ref=date(2023, 1, 1),
        dict_dtypes={"col1": str}
    )
    assert ingestion.url == ""

    # Whitespace-only string should be allowed
    ingestion = CoreIngestion(
        url="  ",
        date_ref=date(2023, 1, 1),
        dict_dtypes={"col1": str}
    )
    assert ingestion.url == "  "


@pytest.mark.parametrize(
    "date_ref", [None, "2023-01-01", 123, [], {}]
)
def test_core_ingestion_invalid_date_ref(date_ref: Optional[Union[str, int, list, dict]]) -> None:
    """Test initialization with invalid date_ref raises TypeError.

    Parameters
    ----------
    date_ref : Optional[Union[str, int, list, dict]]
        Invalid date_ref values to test

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="date_ref must be of type"):
        CoreIngestion(
            url="https://example.com",
            date_ref=date_ref,
            dict_dtypes={"col1": str}
        )


@pytest.mark.parametrize(
    "dict_dtypes", [None, [], "dict", 123]
)
def test_core_ingestion_invalid_dict_dtypes(
    dict_dtypes: Optional[Union[list, str, int, dict]]
) -> None:
    """Test initialization with invalid dict_dtypes raises TypeError.

    Parameters
    ----------
    dict_dtypes : Optional[Union[list, str, int, dict]]
        Invalid dict_dtypes values to test

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="dict_dtypes must be of type"):
        CoreIngestion(
            url="https://example.com",
            date_ref=date(2023, 1, 1),
            dict_dtypes=dict_dtypes
        )


def test_core_ingestion_empty_dict_dtypes_raises_value_error() -> None:
    """Test initialization with empty dict_dtypes raises ValueError.

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="dict_dtypes cannot be empty"):
        CoreIngestion(
            url="https://example.com",
            date_ref=date(2023, 1, 1),
            dict_dtypes={}
        )


def test_standardize_dataframe(
    core_ingestion: CoreIngestion,
    sample_dataframe: pd.DataFrame,
    mocker: MockerFixture
) -> None:
    """Test standardize_dataframe method.

    Verifies
    --------
    - Pipeline is called with correct DataFrame
    - Audit log is called with correct parameters
    - Returns standardized DataFrame

    Parameters
    ----------
    core_ingestion : CoreIngestion
        CoreIngestion instance from fixture
    sample_dataframe : pd.DataFrame
        Sample DataFrame from fixture
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_pipeline = mocker.patch.object(
        core_ingestion.cls_df_standardization,
        "pipeline",
        return_value=sample_dataframe
    )
    mock_audit_log = mocker.patch.object(
        core_ingestion.cls_db_logs,
        "audit_log",
        return_value=sample_dataframe
    )

    result = core_ingestion.standardize_dataframe(sample_dataframe)

    mock_pipeline.assert_called_once_with(sample_dataframe)
    mock_audit_log.assert_called_once_with(
        sample_dataframe,
        core_ingestion.url,
        core_ingestion.date_ref,
        core_ingestion.bool_format_log_as_str
    )
    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(result, sample_dataframe)


def test_standardize_dataframe_invalid_input(
    core_ingestion: CoreIngestion
) -> None:
    """Test standardize_dataframe with invalid input raises TypeError.

    Parameters
    ----------
    core_ingestion : CoreIngestion
        CoreIngestion instance from fixture

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="df_ must be of type"):
        core_ingestion.standardize_dataframe([1, 2, 3])


def test_insert_table_db(
    core_ingestion: CoreIngestion,
    sample_dataframe: pd.DataFrame,
    mock_db_session: Mock,
) -> None:
    """Test insert_table_db method.

    Verifies
    --------
    - DataFrame is converted to records correctly
    - Database session insert is called with correct parameters
    - No return value (None)

    Parameters
    ----------
    core_ingestion : CoreIngestion
        CoreIngestion instance from fixture
    sample_dataframe : pd.DataFrame
        Sample DataFrame from fixture
    mock_db_session : Mock
        Mock database session from fixture

    Returns
    -------
    None
    """
    result = core_ingestion.insert_table_db(
        mock_db_session,
        "test_table",
        sample_dataframe,
        bool_insert_or_ignore=True
    )

    expected_records = sample_dataframe.to_dict(orient="records")
    mock_db_session.insert.assert_called_once_with(
        expected_records,
        str_table_name="test_table",
        bool_insert_or_ignore=True
    )
    assert result is None


@pytest.mark.parametrize(
    "cls_db", [None, "not_a_session", 123, []]
)
def test_insert_table_db_invalid_session(
    core_ingestion: CoreIngestion,
    sample_dataframe: pd.DataFrame,
    cls_db: Optional[Union[str, int, list]]
) -> None:
    """Test insert_table_db with invalid database session raises TypeError.

    Parameters
    ----------
    core_ingestion : CoreIngestion
        CoreIngestion instance from fixture
    sample_dataframe : pd.DataFrame
        Sample DataFrame from fixture
    cls_db : Optional[Union[str, int, list]]
        Invalid database session values to test

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="cls_db must be of type"):
        core_ingestion.insert_table_db(cls_db, "test_table", sample_dataframe)


@pytest.mark.parametrize(
    "str_table_name", [None, 123, []]
)
def test_insert_table_db_invalid_table_name(
    core_ingestion: CoreIngestion,
    sample_dataframe: pd.DataFrame,
    mock_db_session: Mock,
    str_table_name: Optional[Union[str, int, list]]
) -> None:
    """Test insert_table_db with invalid table name raises TypeError.

    Parameters
    ----------
    core_ingestion : CoreIngestion
        CoreIngestion instance from fixture
    sample_dataframe : pd.DataFrame
        Sample DataFrame from fixture
    mock_db_session : Mock
        Mock database session from fixture
    str_table_name : Optional[Union[str, int, list]]
        Invalid table name values to test

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="str_table_name must be of type"):
        core_ingestion.insert_table_db(mock_db_session, str_table_name, sample_dataframe)


def test_insert_table_db_empty_string_table_name_allowed(
    core_ingestion: CoreIngestion,
    sample_dataframe: pd.DataFrame,
    mock_db_session: Mock,
) -> None:
    """Test insert_table_db allows empty string and whitespace table names.

    Parameters
    ----------
    core_ingestion : CoreIngestion
        CoreIngestion instance from fixture
    sample_dataframe : pd.DataFrame
        Sample DataFrame from fixture
    mock_db_session : Mock
        Mock database session from fixture

    Returns
    -------
    None
    """
    # Empty string should be allowed
    core_ingestion.insert_table_db(mock_db_session, "", sample_dataframe)
    
    # Whitespace-only string should be allowed
    core_ingestion.insert_table_db(mock_db_session, "  ", sample_dataframe)


def test_insert_table_db_invalid_dataframe(
    core_ingestion: CoreIngestion,
    mock_db_session: Mock
) -> None:
    """Test insert_table_db with invalid DataFrame raises TypeError.

    Parameters
    ----------
    core_ingestion : CoreIngestion
        CoreIngestion instance from fixture
    mock_db_session : Mock
        Mock database session from fixture

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="df_ must be of type"):
        core_ingestion.insert_table_db(mock_db_session, "test_table", [1, 2, 3])


# --------------------------
# Tests for ABCIngestionOperations
# --------------------------
def test_abc_ingestion_operations_inherits_correctly() -> None:
    """Test ABCIngestionOperations inherits from both parent classes.

    Verifies
    --------
    - ABCIngestionOperations inherits from ABCIngestion and CoreIngestion
    - Instance can be created with valid parameters

    Returns
    -------
    None
    """
    class TestIngestionOperations(ABCIngestionOperations):
        """Test class for ABCIngestionOperations."""

        def get_response(self) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
            """Test get_response implementation.
            
            Returns
            -------
            Union[Response, PlaywrightPage, SeleniumWebDriver]
                The response object.
            """
            return Mock()
        
        def transform_response(
            self, 
            resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
        ) -> pd.DataFrame:
            """Test transform_response implementation.
            
            Parameters
            ----------
            resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
                The response object.
            
            Returns
            -------
            pd.DataFrame
                The transformed DataFrame.
            """
            return pd.DataFrame()

    ingestion = TestIngestionOperations(
        url="https://example.com",
        date_ref=date(2023, 1, 1),
        dict_dtypes={"col1": str}
    )
    assert isinstance(ingestion, ABCIngestion)
    assert isinstance(ingestion, CoreIngestion)
    assert isinstance(ingestion, TestIngestionOperations)


# --------------------------
# Edge Cases and Error Conditions
# --------------------------
def test_core_ingestion_empty_dataframe(
    core_ingestion: CoreIngestion,
    mocker: MockerFixture
) -> None:
    """Test standardize_dataframe with empty DataFrame.

    Verifies
    --------
    - Empty DataFrame is processed correctly
    - Pipeline and audit log are called

    Parameters
    ----------
    core_ingestion : CoreIngestion
        CoreIngestion instance from fixture
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    empty_df = pd.DataFrame()
    mock_pipeline = mocker.patch.object(
        core_ingestion.cls_df_standardization,
        "pipeline",
        return_value=empty_df
    )
    mock_audit_log = mocker.patch.object(
        core_ingestion.cls_db_logs,
        "audit_log",
        return_value=empty_df
    )

    result = core_ingestion.standardize_dataframe(empty_df)
    mock_pipeline.assert_called_once_with(empty_df)
    mock_audit_log.assert_called_once()
    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_insert_table_db_empty_dataframe(
    core_ingestion: CoreIngestion,
    mock_db_session: Mock,
) -> None:
    """Test insert_table_db with empty DataFrame.

    Verifies
    --------
    - Empty DataFrame results in empty records list
    - Database insert is called with empty list

    Parameters
    ----------
    core_ingestion : CoreIngestion
        CoreIngestion instance from fixture
    mock_db_session : Mock
        Mock database session from fixture

    Returns
    -------
    None
    """
    empty_df = pd.DataFrame()
    core_ingestion.insert_table_db(mock_db_session, "test_table", empty_df)
    mock_db_session.insert.assert_called_once_with(
        [],
        str_table_name="test_table",
        bool_insert_or_ignore=False
    )


# --------------------------
# Reload Logic
# --------------------------
def test_module_reload(mocker: MockerFixture) -> None:
    """Test module reload preserves functionality.

    Verifies
    --------
    - Module can be reloaded without breaking functionality
    - CoreIngestion instance can still be created post-reload

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    import importlib

    import stpstone

    mocker.patch.object(DFStandardization, "__init__", return_value=None)
    mocker.patch.object(DBLogs, "__init__", return_value=None)

    importlib.reload(stpstone)
    ingestion = CoreIngestion(
        url="https://example.com",
        date_ref=date(2023, 1, 1),
        dict_dtypes={"col1": str}
    )
    assert isinstance(ingestion, CoreIngestion)


# --------------------------
# Type Validation
# --------------------------
def test_core_ingestion_type_checker() -> None:
    """Test TypeChecker metaclass enforces type validation.

    Verifies
    --------
    - TypeChecker raises TypeError for invalid types
    - Valid types pass without errors

    Returns
    -------
    None
    """
    # Valid initialization
    CoreIngestion(
        url="https://example.com",
        date_ref=date(2023, 1, 1),
        dict_dtypes={"col1": str}
    )

    # Invalid types should raise TypeError
    with pytest.raises(TypeError, match="url must be of type"):
        CoreIngestion(
            url=123,
            date_ref=date(2023, 1, 1),
            dict_dtypes={"col1": str}
        )


def test_optional_parameters_type_validation() -> None:
    """Test optional parameters accept None or correct types.

    Verifies
    --------
    - Optional parameters accept None
    - Optional parameters accept correct types
    - Invalid types raise TypeError

    Returns
    -------
    None
    """
    # Valid optional parameters
    ingestion = CoreIngestion(
        url="https://example.com",
        date_ref=date(2023, 1, 1),
        dict_dtypes={"col1": str},
        cols_from_case=None,
        cols_to_case=None,
        list_cols_drop_dupl=None,
        dict_fillna_strt=None,
        str_dt_fillna=None,
        logger=None
    )
    assert ingestion.cls_df_standardization.cols_from_case is None
    assert ingestion.cls_df_standardization.cols_to_case is None
    # Note: list_cols_drop_dupl defaults to [] when None is passed
    assert ingestion.cls_df_standardization.list_cols_drop_dupl == []
    # Note: dict_fillna_strt defaults to {} when None is passed
    assert ingestion.cls_df_standardization.dict_fillna_strt == {}
    # Note: str_dt_fillna defaults to '2099-12-31' when None is passed
    assert ingestion.cls_df_standardization.str_dt_fillna == '2099-12-31'
    assert ingestion.cls_df_standardization.logger is None

    # Test with valid non-None values
    ingestion_with_values = CoreIngestion(
        url="https://example.com",
        date_ref=date(2023, 1, 1),
        dict_dtypes={"col1": str},
        list_cols_drop_dupl=["col1"],
        str_data_fillna="-88888",
        bool_format_log_as_str=False
    )
    assert ingestion_with_values.cls_df_standardization.list_cols_drop_dupl == ["col1"]
    assert ingestion_with_values.cls_df_standardization.str_data_fillna == "-88888"
    assert ingestion_with_values.bool_format_log_as_str is False


# --------------------------
# Fallback Logic
# --------------------------
def test_standardize_dataframe_fallback(
    core_ingestion: CoreIngestion,
    sample_dataframe: pd.DataFrame,
    mocker: MockerFixture
) -> None:
    """Test standardize_dataframe handles pipeline errors gracefully.

    Verifies
    --------
    - If pipeline raises an error, audit_log is not called
    - Error is propagated correctly

    Parameters
    ----------
    core_ingestion : CoreIngestion
        CoreIngestion instance from fixture
    sample_dataframe : pd.DataFrame
        Sample DataFrame from fixture
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mocker.patch.object(
        core_ingestion.cls_df_standardization,
        "pipeline",
        side_effect=ValueError("Pipeline error")
    )
    mock_audit_log = mocker.patch.object(core_ingestion.cls_db_logs, "audit_log")

    with pytest.raises(ValueError, match="Pipeline error"):
        core_ingestion.standardize_dataframe(sample_dataframe)
    mock_audit_log.assert_not_called()