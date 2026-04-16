"""Unit tests for RatingsCorpSPGlobal orchestrator class."""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from stpstone.ingestion.countries.ww.registries.ratings_corp_spglobal import RatingsCorpSPGlobal
from stpstone.utils.loggs.create_logs import CreateLog


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
def instance(sample_date: date) -> RatingsCorpSPGlobal:
	"""Provide a RatingsCorpSPGlobal instance for testing.

	Parameters
	----------
	sample_date : date
		Fixed reference date.

	Returns
	-------
	RatingsCorpSPGlobal
		Initialized orchestrator instance.
	"""
	return RatingsCorpSPGlobal(date_ref=sample_date)


@pytest.fixture
def sample_ratings_df() -> pd.DataFrame:
	"""Provide a sample ratings DataFrame.

	Returns
	-------
	pd.DataFrame
		DataFrame with one rating action row.
	"""
	return pd.DataFrame(
		{
			"RATING_ACTION_DATE": ["2025-01-01"],
			"ACTION_TYPE_CODE": ["DN"],
			"ENTITY_ID": [123],
			"SOURCE_PROVIDED_NAME": ["Test Corp"],
		}
	)


# --------------------------
# Tests
# --------------------------
def test_init_sets_attributes(sample_date: date) -> None:
	"""Test initialization sets date_ref, logger, cls_db, and cls_create_log.

	Parameters
	----------
	sample_date : date
		Fixed reference date.

	Returns
	-------
	None
	"""
	obj = RatingsCorpSPGlobal(date_ref=sample_date)
	assert obj.date_ref == sample_date
	assert obj.logger is None
	assert obj.cls_db is None
	assert isinstance(obj.cls_create_log, CreateLog)


def test_init_defaults() -> None:
	"""Test initialization with no arguments sets all optional attributes to None.

	Returns
	-------
	None
	"""
	obj = RatingsCorpSPGlobal()
	assert obj.date_ref is None
	assert obj.logger is None
	assert obj.cls_db is None


def test_get_corp_ratings_paginates_until_empty(
	instance: RatingsCorpSPGlobal,
	sample_ratings_df: pd.DataFrame,
) -> None:
	"""Test get_corp_ratings fetches pages until an empty DataFrame is returned.

	Parameters
	----------
	instance : RatingsCorpSPGlobal
		Orchestrator instance.
	sample_ratings_df : pd.DataFrame
		Sample DataFrame for page 1.

	Returns
	-------
	None
	"""
	mock_page_cls = MagicMock()
	mock_page_cls.get_bearer = "Bearer test-token"
	mock_page_instance_1 = MagicMock()
	mock_page_instance_1.run.return_value = sample_ratings_df
	mock_page_instance_2 = MagicMock()
	mock_page_instance_2.run.return_value = pd.DataFrame()

	call_count = 0

	def make_page(*args: object, **kwargs: object) -> object:
		"""Return mock page instance based on call order.

		Parameters
		----------
		*args : object
			Positional arguments forwarded from the patch.
		**kwargs : object
			Keyword arguments forwarded from the patch.

		Returns
		-------
		object
			Mock page instance.
		"""
		nonlocal call_count
		call_count += 1
		if kwargs.get("bearer") == "" or args[0:1] == ("",):
			return mock_page_cls
		if kwargs.get("pg_number", 1) == 1:
			return mock_page_instance_1
		return mock_page_instance_2

	with (
		patch(
			"stpstone.ingestion.countries.ww.registries.ratings_corp_spglobal.RatingsCorpSPGlobalOnePage",
			side_effect=make_page,
		),
		patch("stpstone.ingestion.countries.ww.registries.ratings_corp_spglobal.sleep"),
	):
		df_ = instance.get_corp_ratings
	assert isinstance(df_, pd.DataFrame)


def test_get_corp_ratings_handles_exception(instance: RatingsCorpSPGlobal) -> None:
	"""Test get_corp_ratings stops and returns partial data on exception.

	Parameters
	----------
	instance : RatingsCorpSPGlobal
		Orchestrator instance.

	Returns
	-------
	None
	"""
	mock_bearer_instance = MagicMock()
	mock_bearer_instance.get_bearer = "Bearer test"
	error_instance = MagicMock()
	error_instance.side_effect = ValueError("network error")

	def make_page(*args: object, **kwargs: object) -> object:
		"""Return mock page or error instance based on bearer argument.

		Parameters
		----------
		*args : object
			Positional arguments forwarded from the patch.
		**kwargs : object
			Keyword arguments forwarded from the patch.

		Returns
		-------
		object
			Mock page or error instance.
		"""
		if kwargs.get("bearer") == "" or (args and args[0] == ""):
			return mock_bearer_instance
		return error_instance

	with (
		patch(
			"stpstone.ingestion.countries.ww.registries.ratings_corp_spglobal.RatingsCorpSPGlobalOnePage",
			side_effect=make_page,
		),
		patch("stpstone.ingestion.countries.ww.registries.ratings_corp_spglobal.sleep"),
	):
		df_ = instance.get_corp_ratings
	assert isinstance(df_, pd.DataFrame)


def test_update_db_calls_run_for_each_page(
	instance: RatingsCorpSPGlobal,
	sample_ratings_df: pd.DataFrame,
) -> None:
	"""Test update_db calls run on page instances until exception stops iteration.

	Parameters
	----------
	instance : RatingsCorpSPGlobal
		Orchestrator instance.
	sample_ratings_df : pd.DataFrame
		Sample DataFrame returned by page 1.

	Returns
	-------
	None
	"""
	instance.cls_db = MagicMock()
	mock_bearer_instance = MagicMock()
	mock_bearer_instance.get_bearer = "Bearer test"
	mock_page_instance = MagicMock()
	mock_page_instance.run.return_value = None

	call_count = 0
	error_instance = MagicMock()
	error_instance.side_effect = ValueError("stop")

	def make_page(*args: object, **kwargs: object) -> object:
		"""Return mock page or error instance based on call count.

		Parameters
		----------
		*args : object
			Positional arguments forwarded from the patch.
		**kwargs : object
			Keyword arguments forwarded from the patch.

		Returns
		-------
		object
			Mock page or error instance.
		"""
		nonlocal call_count
		if kwargs.get("bearer") == "" or (args and args[0] == ""):
			return mock_bearer_instance
		call_count += 1
		if call_count > 1:
			return error_instance
		return mock_page_instance

	with (
		patch(
			"stpstone.ingestion.countries.ww.registries.ratings_corp_spglobal.RatingsCorpSPGlobalOnePage",
			side_effect=make_page,
		),
		patch("stpstone.ingestion.countries.ww.registries.ratings_corp_spglobal.sleep"),
	):
		instance.update_db()
	mock_page_instance.run.assert_called_once()


def test_reload_module() -> None:
	"""Test that the module can be reloaded without errors.

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.ingestion.countries.ww.registries.ratings_corp_spglobal as mod

	importlib.reload(mod)
	obj = mod.RatingsCorpSPGlobal(date_ref=date(2025, 1, 1))
	assert obj.date_ref == date(2025, 1, 1)
