"""Unit tests for WorldGovBondsSovereignCds class."""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from requests import Response, Session

from stpstone.ingestion.countries.ww.macroeconomics.world_gov_bonds_sovereign_cds import (
    WorldGovBondsSovereignCds,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_date() -> date:
    """Provide a fixed date for testing.

    Returns
    -------
    date
        Fixed reference date.
    """
    return date(2025, 1, 1)


@pytest.fixture
def instance(sample_date: date) -> WorldGovBondsSovereignCds:
    """Provide a WorldGovBondsSovereignCds instance for testing.

    Parameters
    ----------
    sample_date : date
        Fixed reference date.

    Returns
    -------
    WorldGovBondsSovereignCds
        Initialized instance.
    """
    return WorldGovBondsSovereignCds(date_ref=sample_date)


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_date(sample_date: date) -> None:
    """Test initialization sets expected attributes.

    Parameters
    ----------
    sample_date : date
        Fixed reference date.

    Returns
    -------
    None
    """
    obj = WorldGovBondsSovereignCds(date_ref=sample_date)
    assert obj.date_ref == sample_date
    assert isinstance(obj.cls_dates_current, DatesCurrent)
    assert isinstance(obj.cls_dates_br, DatesBRAnbima)
    assert isinstance(obj.cls_create_log, CreateLog)
    assert isinstance(obj.cls_dir_files_management, DirFilesManagement)
    assert "sovereign-cds" in obj.url
    assert obj._TRIM_LAST is True


def test_init_with_default_date() -> None:
    """Test initialization without date_ref uses the previous working day.

    Returns
    -------
    None
    """
    with patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 1)):
        obj = WorldGovBondsSovereignCds()
        assert obj.date_ref == date(2025, 1, 1)


def test_get_response_returns_none(instance: WorldGovBondsSovereignCds) -> None:
    """Test get_response returns None as Playwright handles navigation internally.

    Parameters
    ----------
    instance : WorldGovBondsSovereignCds
        Ingestion instance.

    Returns
    -------
    None
    """
    result = instance.get_response()
    assert result is None


def test_parse_raw_file_passthrough(instance: WorldGovBondsSovereignCds) -> None:
    """Test parse_raw_file returns the input unchanged.

    Parameters
    ----------
    instance : WorldGovBondsSovereignCds
        Ingestion instance.

    Returns
    -------
    None
    """
    mock_resp = MagicMock(spec=Response)
    result = instance.parse_raw_file(mock_resp)
    assert result is mock_resp


def test_run_without_db(instance: WorldGovBondsSovereignCds) -> None:
    """Test run returns a DataFrame when no cls_db is provided.

    Parameters
    ----------
    instance : WorldGovBondsSovereignCds
        Ingestion instance.

    Returns
    -------
    None
    """
    sample_df = pd.DataFrame({"COUNTRY": ["Germany"]})
    with patch.object(
        instance, "transform_data", return_value=sample_df
    ), patch.object(instance, "standardize_dataframe", return_value=sample_df):
        result = instance.run()
    assert isinstance(result, pd.DataFrame)


def test_run_with_db(instance: WorldGovBondsSovereignCds) -> None:
    """Test run calls insert_table_db and returns None when cls_db is set.

    Parameters
    ----------
    instance : WorldGovBondsSovereignCds
        Ingestion instance.

    Returns
    -------
    None
    """
    instance.cls_db = MagicMock(spec=Session)
    sample_df = pd.DataFrame({"COUNTRY": ["Germany"]})
    with patch.object(
        instance, "transform_data", return_value=sample_df
    ), patch.object(
        instance, "standardize_dataframe", return_value=sample_df
    ), patch.object(instance, "insert_table_db") as mock_insert:
        result = instance.run()
    assert result is None
    mock_insert.assert_called_once()


def test_reload_module() -> None:
    """Test that the module can be reloaded without errors.

    Returns
    -------
    None
    """
    import importlib

    import stpstone.ingestion.countries.ww.macroeconomics.world_gov_bonds_sovereign_cds as mod

    importlib.reload(mod)
    obj = mod.WorldGovBondsSovereignCds(date_ref=date(2025, 1, 1))
    assert obj.date_ref == date(2025, 1, 1)
