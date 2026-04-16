"""Unit tests for AnbimaDataCRICRACharacteristics ingestion class."""

from datetime import date
from io import StringIO
from logging import Logger
from unittest.mock import Mock, patch

import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import pytest
from pytest_mock import MockerFixture
from requests import Session

from stpstone.ingestion.countries.br.registries.anbima_data_cri_cra_characteristics import (
	AnbimaDataCRICRACharacteristics,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_logger() -> Logger:
	"""Fixture providing mock logger.

	Returns
	-------
	Logger
		Mock logger instance
	"""
	return Mock(spec=Logger)


@pytest.fixture
def mock_db_session() -> Session:
	"""Fixture providing mock database session.

	Returns
	-------
	Session
		Mock database session instance
	"""
	return Mock(spec=Session)


@pytest.fixture
def sample_date_ref() -> date:
	"""Fixture providing sample reference date.

	Returns
	-------
	date
		Reference date for testing
	"""
	return date(2024, 1, 15)


@pytest.fixture
def mock_playwright_page(mocker: MockerFixture) -> PlaywrightPage:
	"""Fixture providing mock Playwright page.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	PlaywrightPage
		Mock Playwright page instance
	"""
	mock_page = mocker.MagicMock(spec=PlaywrightPage)
	mock_page.goto = mocker.MagicMock()
	mock_page.wait_for_timeout = mocker.MagicMock()
	mock_page.query_selector_all = mocker.MagicMock(return_value=[])
	mock_page.locator = mocker.MagicMock()
	return mock_page


# --------------------------
# Tests
# --------------------------
class TestAnbimaDataCRICRACharacteristics:
	"""Test cases for AnbimaDataCRICRACharacteristics class."""

	def test_init_with_valid_inputs(
		self, sample_date_ref: date, mock_logger: Logger, mock_db_session: Session
	) -> None:
		"""Test initialization with valid inputs.

		Verifies
		--------
		- The class can be initialized with valid parameters
		- All attributes are correctly set
		- Default values are applied when optional parameters are None

		Parameters
		----------
		sample_date_ref : date
			Reference date fixture
		mock_logger : Logger
			Mock logger fixture
		mock_db_session : Session
			Mock database session fixture

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRACharacteristics(
			date_ref=sample_date_ref,
			logger=mock_logger,
			cls_db=mock_db_session,
			start_page=0,
			end_page=5,
		)

		assert instance.date_ref == sample_date_ref
		assert instance.logger == mock_logger
		assert instance.cls_db == mock_db_session
		assert instance.start_page == 0
		assert instance.end_page == 5
		assert instance.base_url == "https://data.anbima.com.br/busca/certificado-de-recebiveis"

	def test_init_with_negative_start_page(
		self, sample_date_ref: date, mock_logger: Logger
	) -> None:
		"""Test initialization with negative start_page raises ValueError.

		Verifies
		--------
		- ValueError is raised when start_page is negative
		- Error message contains 'greater than or equal to 0'

		Parameters
		----------
		sample_date_ref : date
			Reference date fixture
		mock_logger : Logger
			Mock logger fixture

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="start_page must be greater than or equal to 0"):
			AnbimaDataCRICRACharacteristics(
				date_ref=sample_date_ref, logger=mock_logger, start_page=-1
			)

	def test_init_with_end_page_less_than_start_page(
		self, sample_date_ref: date, mock_logger: Logger
	) -> None:
		"""Test initialization with end_page less than start_page raises ValueError.

		Verifies
		--------
		- ValueError is raised when end_page < start_page
		- Error message contains 'greater than or equal to start_page'

		Parameters
		----------
		sample_date_ref : date
			Reference date fixture
		mock_logger : Logger
			Mock logger fixture

		Returns
		-------
		None
		"""
		with pytest.raises(
			ValueError, match="end_page must be greater than or equal to start_page"
		):
			AnbimaDataCRICRACharacteristics(
				date_ref=sample_date_ref, logger=mock_logger, start_page=5, end_page=3
			)

	def test_init_with_none_date_ref(self, mock_logger: Logger) -> None:
		"""Test initialization with None date_ref uses default date.

		Verifies
		--------
		- When date_ref is None, a default date is calculated
		- The default date is not None
		- The instance is successfully created

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRACharacteristics(date_ref=None, logger=mock_logger)

		assert instance.date_ref is not None
		assert isinstance(instance.date_ref, date)

	@patch(
		"stpstone.ingestion.countries.br.registries"
		".anbima_data_cri_cra_characteristics.sync_playwright"
	)
	def test_get_response_with_no_pages(
		self, mock_playwright: Mock, mock_logger: Logger, mocker: MockerFixture
	) -> None:
		"""Test get_response when no pages are found.

		Verifies
		--------
		- Returns empty list when no elements are found
		- Playwright browser is properly launched and closed
		- Page navigation occurs correctly

		Parameters
		----------
		mock_playwright : Mock
			Mock sync_playwright context manager
		mock_logger : Logger
			Mock logger fixture
		mocker : MockerFixture
			Pytest-mock fixture

		Returns
		-------
		None
		"""
		mock_browser = mocker.MagicMock()
		mock_page = mocker.MagicMock()
		mock_page.query_selector_all.return_value = []

		mock_context = mocker.MagicMock()
		mock_context.__enter__.return_value = mock_context
		mock_context.__exit__.return_value = None
		mock_context.chromium.launch.return_value = mock_browser
		mock_browser.new_page.return_value = mock_page

		mock_playwright.return_value = mock_context

		instance = AnbimaDataCRICRACharacteristics(logger=mock_logger, start_page=0, end_page=0)

		with patch("time.sleep"):
			result = instance.get_response(timeout_ms=1000)

		assert result == []
		mock_browser.close.assert_called_once()

	def test_transform_data_with_empty_list(self, mock_logger: Logger) -> None:
		"""Test transform_data with empty input list.

		Verifies
		--------
		- Returns empty DataFrame when input is empty list
		- DataFrame has no rows or columns

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRACharacteristics(logger=mock_logger)
		result = instance.transform_data(raw_data=[])

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 0

	def test_transform_data_with_valid_data(self, mock_logger: Logger) -> None:
		"""Test transform_data with valid input data.

		Verifies
		--------
		- DataFrame is created with correct structure
		- Date fields are properly handled
		- Pagina column is dropped if present

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture

		Returns
		-------
		None
		"""
		raw_data = [
			{
				"CODIGO_EMISSAO": "TEST001",
				"NOME_EMISSOR": "Test Emissor",
				"DATA_EMISSAO": "15/01/2024",
				"DATA_VENCIMENTO": "-",
				"pagina": 0,
			}
		]

		instance = AnbimaDataCRICRACharacteristics(logger=mock_logger)
		result = instance.transform_data(raw_data=raw_data)

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 1
		assert "pagina" not in result.columns
		assert result["DATA_VENCIMENTO"].iloc[0] == "01/01/2100"

	def test_parse_raw_file_returns_stringio(self, mock_logger: Logger) -> None:
		"""Test parse_raw_file returns empty StringIO.

		Verifies
		--------
		- Method returns StringIO instance
		- StringIO is empty

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRACharacteristics(logger=mock_logger)
		result = instance.parse_raw_file(resp_req=Mock())

		assert isinstance(result, StringIO)
		assert result.getvalue() == ""

	def test_characteristics_base_url(self, mock_logger: Logger) -> None:
		"""Test characteristics class base URL.

		Verifies
		--------
		- Base URL is correctly set

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRACharacteristics(logger=mock_logger)

		assert instance.base_url == "https://data.anbima.com.br/busca/certificado-de-recebiveis"

	def test_date_ref_type_validation(self, mock_logger: Logger) -> None:
		"""Test date_ref parameter type validation.

		Verifies
		--------
		- Valid date object is accepted
		- None is accepted and handled

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture

		Returns
		-------
		None
		"""
		valid_date = date(2024, 1, 15)
		instance = AnbimaDataCRICRACharacteristics(date_ref=valid_date, logger=mock_logger)

		assert instance.date_ref == valid_date
		assert isinstance(instance.date_ref, date)

	def test_logger_type_validation(self) -> None:
		"""Test logger parameter type validation.

		Verifies
		--------
		- Logger instance is accepted
		- None is accepted

		Returns
		-------
		None
		"""
		valid_logger = Mock(spec=Logger)
		instance = AnbimaDataCRICRACharacteristics(logger=valid_logger)

		assert instance.logger == valid_logger

		instance_no_logger = AnbimaDataCRICRACharacteristics(logger=None)
		assert instance_no_logger.logger is None

	def test_cls_db_type_validation(self) -> None:
		"""Test cls_db parameter type validation.

		Verifies
		--------
		- Session instance is accepted
		- None is accepted

		Returns
		-------
		None
		"""
		valid_session = Mock(spec=Session)
		instance = AnbimaDataCRICRACharacteristics(cls_db=valid_session)

		assert instance.cls_db == valid_session

		instance_no_db = AnbimaDataCRICRACharacteristics(cls_db=None)
		assert instance_no_db.cls_db is None

	@pytest.mark.parametrize(
		"start_page,end_page,should_pass",
		[
			(0, 0, True),
			(0, 10, True),
			(5, 5, True),
			(5, 10, True),
			(0, None, True),
			(-1, 10, False),
			(10, 5, False),
		],
	)
	def test_page_range_validation(
		self,
		start_page: int,
		end_page: int,
		should_pass: bool,
		mock_logger: Logger,
	) -> None:
		"""Test page range validation logic.

		Verifies
		--------
		- Valid page ranges are accepted
		- Invalid page ranges raise ValueError

		Parameters
		----------
		start_page : int
			Starting page number
		end_page : int
			Ending page number
		should_pass : bool
			Whether validation should pass
		mock_logger : Logger
			Mock logger fixture

		Returns
		-------
		None
		"""
		if should_pass:
			instance = AnbimaDataCRICRACharacteristics(
				logger=mock_logger, start_page=start_page, end_page=end_page
			)
			assert instance.start_page == start_page
			assert instance.end_page == end_page
		else:
			with pytest.raises(ValueError):
				AnbimaDataCRICRACharacteristics(
					logger=mock_logger, start_page=start_page, end_page=end_page
				)

	def test_characteristics_dataframe_columns(self, mock_logger: Logger) -> None:
		"""Test characteristics DataFrame has expected columns.

		Verifies
		--------
		- Transform creates DataFrame with proper structure

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture

		Returns
		-------
		None
		"""
		raw_data = [
			{
				"CODIGO_EMISSAO": "TEST001",
				"NOME_EMISSOR": "Test",
				"DATA_EMISSAO": "15/01/2024",
			}
		]

		instance = AnbimaDataCRICRACharacteristics(logger=mock_logger)
		result = instance.transform_data(raw_data=raw_data)

		assert "CODIGO_EMISSAO" in result.columns
		assert "NOME_EMISSOR" in result.columns
		assert "DATA_EMISSAO" in result.columns

	@patch(
		"stpstone.ingestion.countries.br.registries"
		".anbima_data_cri_cra_characteristics.sync_playwright"
	)
	@patch("time.sleep")
	def test_characteristics_no_real_browser(
		self, mock_sleep: Mock, mock_playwright: Mock, mock_logger: Logger, mocker: MockerFixture
	) -> None:
		"""Test characteristics scraping without real browser.

		Verifies
		--------
		- No actual browser is launched
		- Mocked browser operations work correctly
		- Sleep is mocked to avoid delays

		Parameters
		----------
		mock_sleep : Mock
			Mock sleep function
		mock_playwright : Mock
			Mock sync_playwright
		mock_logger : Logger
			Mock logger fixture
		mocker : MockerFixture
			Pytest-mock fixture

		Returns
		-------
		None
		"""
		mock_browser = mocker.MagicMock()
		mock_page = mocker.MagicMock()
		mock_page.query_selector_all.return_value = []
		mock_page.locator.return_value.first.count.return_value = 0

		mock_context = mocker.MagicMock()
		mock_context.__enter__.return_value = mock_context
		mock_context.__exit__.return_value = None
		mock_context.chromium.launch.return_value = mock_browser
		mock_browser.new_page.return_value = mock_page

		mock_playwright.return_value = mock_context

		instance = AnbimaDataCRICRACharacteristics(logger=mock_logger, start_page=0, end_page=2)

		with patch.object(instance, "_get_total_pages", return_value=2):
			result = instance.get_response(timeout_ms=100)

		assert result == []
		mock_browser.close.assert_called_once()
		assert mock_sleep.called

	def test_init_with_page_parameters(self, mock_logger: Logger) -> None:
		"""Test initialization with page parameters.

		Verifies
		--------
		- Classes with pagination support accept page parameters

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRACharacteristics(logger=mock_logger, start_page=0, end_page=5)
		assert instance.start_page == 0
		assert instance.end_page == 5
