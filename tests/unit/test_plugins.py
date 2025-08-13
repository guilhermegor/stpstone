"""Unit tests for AirflowPlugins class.

Tests the working day validation functionality with various input scenarios including:
- Normal operation with valid inputs
- Missing required keys in context
- Invalid date string formats
- Working day vs non-working day scenarios
"""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.orchestrators.airflow.plugins import AirflowPlugins


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def airflow_plugins() -> AirflowPlugins:
    """Fixture providing an instance of AirflowPlugins.

    Returns
    -------
    AirflowPlugins
        Instance of the class to be tested
    """
    return AirflowPlugins()


@pytest.fixture
def valid_context() -> dict[str, Any]:
    """Fixture providing a valid Airflow context dictionary.

    Returns
    -------
    dict[str, Any]
        Dictionary containing:
        - 'ds': Valid date string
        - 'ti': Mocked task instance
    """
    return {
        'ds': '2023-01-02',  # Monday (working day)
        'ti': MagicMock()
    }


# --------------------------
# Tests
# --------------------------
class TestValidateDs:
    """Test cases for _validate_ds method."""

    def test_valid_ds_string(self, airflow_plugins: AirflowPlugins) -> None:
        """Test validation passes with valid date string.

        Verifies
        --------
        - Method accepts valid non-empty string
        - No exceptions are raised

        Parameters
        ----------
        airflow_plugins : AirflowPlugins
            Instance of the class being tested
        """
        airflow_plugins._validate_ds("2023-01-01")

    def test_empty_ds_string(self, airflow_plugins: AirflowPlugins) -> None:
        """Test raises ValueError with empty date string.

        Verifies
        --------
        - Empty string raises ValueError
        - Error message is appropriate

        Parameters
        ----------
        airflow_plugins : AirflowPlugins
            Instance of the class being tested
        """
        with pytest.raises(ValueError) as excinfo:
            airflow_plugins._validate_ds("")
        assert "Date string (ds) cannot be empty" in str(excinfo.value)

    def test_none_ds_value(self, airflow_plugins: AirflowPlugins) -> None:
        """Test raises ValueError with None input.

        Verifies
        --------
        - None value raises ValueError
        - Error message is appropriate

        Parameters
        ----------
        airflow_plugins : AirflowPlugins
            Instance of the class being tested
        """
        with pytest.raises(TypeError, match="must be of type"):
            airflow_plugins._validate_ds(None)


class TestValidateWorkingDay:
    """Test cases for validate_working_day method."""

    def test_valid_working_day(
        self, 
        airflow_plugins: AirflowPlugins,
        valid_context: dict[str, Any]
    ) -> None:
        """Test successful execution with valid working day.

        Verifies
        --------
        - Method executes without exceptions
        - XCom value is pushed with correct key
        - DatesBR.is_working_day is called once

        Parameters
        ----------
        airflow_plugins : AirflowPlugins
            Instance of the class being tested
        valid_context : dict[str, Any]
            Valid Airflow context dictionary
        """
        with patch.object(DatesBR, 'is_working_day', return_value=True) as mock_method:
            airflow_plugins.validate_working_day(**valid_context)
            valid_context['ti'].xcom_push.assert_called_once_with(
                key='bool_continue', 
                value=True
            )
            mock_method.assert_called_once_with(valid_context['ds'])

    def test_missing_ds_key(
        self, 
        airflow_plugins: AirflowPlugins,
        valid_context: dict[str, Any]
    ) -> None:
        """Test raises KeyError when 'ds' key is missing.

        Verifies
        --------
        - Missing 'ds' key raises KeyError
        - Error message is appropriate

        Parameters
        ----------
        airflow_plugins : AirflowPlugins
            Instance of the class being tested
        valid_context : dict[str, Any]
            Valid Airflow context dictionary
        """
        invalid_context = valid_context.copy()
        del invalid_context['ds']
        with pytest.raises(KeyError) as excinfo:
            airflow_plugins.validate_working_day(**invalid_context)
        assert "Missing required key in context: 'ds'" in str(excinfo.value)

    def test_missing_ti_key(
        self, 
        airflow_plugins: AirflowPlugins,
        valid_context: dict[str, Any]
    ) -> None:
        """Test raises KeyError when 'ti' key is missing.

        Verifies
        --------
        - Missing 'ti' key raises KeyError
        - Error message is appropriate

        Parameters
        ----------
        airflow_plugins : AirflowPlugins
            Instance of the class being tested
        valid_context : dict[str, Any]
            Valid Airflow context dictionary
        """
        invalid_context = valid_context.copy()
        del invalid_context['ti']
        with pytest.raises(KeyError) as excinfo:
            airflow_plugins.validate_working_day(**invalid_context)
        assert "Missing required key in context: 'ti'" in str(excinfo.value)

    def test_invalid_date_format(
        self, 
        airflow_plugins: AirflowPlugins,
        valid_context: dict[str, Any]
    ) -> None:
        """Test raises ValueError with invalid date format.

        Verifies
        --------
        - Invalid date string raises ValueError
        - Error message is appropriate

        Parameters
        ----------
        airflow_plugins : AirflowPlugins
            Instance of the class being tested
        valid_context : dict[str, Any]
            Valid Airflow context dictionary
        """
        invalid_context = valid_context.copy()
        invalid_context['ds'] = 'invalid-date'
        with pytest.raises(ValueError) as excinfo:
            airflow_plugins.validate_working_day(**invalid_context)
        assert "Date validation failed" in str(excinfo.value)

    @pytest.mark.parametrize("date_str,expected_result", [
        ("2023-01-02", True),   # Monday
        ("2023-01-07", False),  # Saturday
        ("2023-01-08", False),  # Sunday
    ])
    def test_working_day_variations(
        self,
        date_str: str,
        expected_result: bool,
        airflow_plugins: AirflowPlugins,
        valid_context: dict[str, Any]
    ) -> None:
        """Test different date scenarios.

        Verifies
        --------
        - Correct working day detection for different dates
        - Proper XCom value pushing

        Parameters
        ----------
        date_str : str
            Date string to test
        expected_result : bool
            Expected working day result
        airflow_plugins : AirflowPlugins
            Instance of the class being tested
        valid_context : dict[str, Any]
            Valid Airflow context dictionary
        """
        test_context = valid_context.copy()
        test_context['ds'] = date_str
        with patch.object(DatesBR, 'is_working_day', return_value=expected_result):
            airflow_plugins.validate_working_day(**test_context)
            test_context['ti'].xcom_push.assert_called_once_with(
                key='bool_continue',
                value=expected_result
            )