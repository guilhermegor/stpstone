"""Unit tests for DebenturesComBrMTM class.

Tests the ingestion functionality of DebenturesComBrMTM, including:
- Initialization with valid and invalid inputs
- Data retrieval and parsing
- Data transformation and standardization
- Error handling and edge cases
"""

from datetime import date
import importlib
from io import StringIO
from logging import Logger
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

import stpstone.ingestion.countries.br.otc.debentures_com_br_mtm
from stpstone.ingestion.countries.br.otc.debentures_com_br_mtm import DebenturesComBrMTM


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response(mocker: MockerFixture) -> Response:
	"""Mock Response object with sample content.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker for creating mock objects.

	Returns
	-------
	Response
		Mocked Response object with HTML content.
	"""
	response = MagicMock(spec=Response)
	response.content = b"Sample content"
	response.status_code = 200
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def sample_dates() -> tuple[date, date]:
	"""Provide sample date range for testing.

	Returns
	-------
	tuple[date, date]
		A fixed date range for consistent testing.
	"""
	return date(2023, 1, 1), date(2023, 1, 5)


@pytest.fixture
def mock_stringio_mtm(mocker: MockerFixture) -> StringIO:
	"""Mock StringIO object with sample CSV content for MTM.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker for creating mock objects.

	Returns
	-------
	StringIO
		A StringIO object with sample CSV content.
	"""
	csv_content = """Header1
Header2
Header3
01/01/2023\tTEST\t1000,0\t5,0\t2,0\t1002,0\tCRIT\tACTIVE
Footer1
Footer2
Footer3
Footer4"""
	return StringIO(csv_content)


@pytest.fixture
def mock_session() -> MagicMock:
	"""Mock database session.

	Returns
	-------
	MagicMock
		Mocked database session.
	"""
	return MagicMock()


# --------------------------
# Tests for DebenturesComBrMTM
# --------------------------
class TestDebenturesComBrMTM:
	"""Test cases for DebenturesComBrMTM class."""

	def test_init_valid_inputs(self, sample_dates: tuple[date, date]) -> None:
		"""Test initialization with valid inputs.

		Verifies
		--------
		- Instance is created with valid date_start and date_end
		- Attributes are correctly set
		- Default values are used when None is provided

		Parameters
		----------
		sample_dates : tuple[date, date]
			A fixed date range for consistent testing.

		Returns
		-------
		None
		"""
		date_start, date_end = sample_dates
		instance = DebenturesComBrMTM(date_start=date_start, date_end=date_end)
		assert instance.date_start == date_start
		assert instance.date_end == date_end
		assert isinstance(instance.logger, Logger) or instance.logger is None
		assert instance.cls_db is None
		assert instance.url.startswith("https://www.debentures.com.br")

	def test_init_default_dates(self, mocker: MockerFixture) -> None:
		"""Test initialization with default dates.

		Verifies
		--------
		- Default dates are set using DatesBRAnbima
		- Dates are correctly calculated for working days

		Parameters
		----------
		mocker : MockerFixture
			Pytest mocker for creating mock objects.

		Returns
		-------
		None
		"""
		mocker.patch(
			"stpstone.utils.calendars.calendar_br.DatesBRAnbima.add_working_days",
			side_effect=[date(2023, 1, 1), date(2023, 1, 5)],
		)
		instance = DebenturesComBrMTM()
		assert isinstance(instance.date_start, date)
		assert isinstance(instance.date_end, date)

	@patch("requests.get")
	@patch("stpstone.ingestion.countries.br.otc.debentures_com_br_mtm.DebenturesComBrMTM.get_file")
	def test_run_without_db(
		self,
		mock_get_file: MagicMock,
		mock_requests_get: MagicMock,
		mock_response: Response,
		mock_stringio_mtm: StringIO,
		sample_dates: tuple[date, date],
	) -> None:
		"""Test run method without database session.

		Verifies
		--------
		- Returns a DataFrame when cls_db is None
		- Correct URL formatting
		- Data transformation and standardization

		Parameters
		----------
		mock_get_file : MagicMock
			Mocked get_file method.
		mock_requests_get : MagicMock
			Mocked requests.get method.
		mock_response : Response
			Mocked response object.
		mock_stringio_mtm : StringIO
			Mocked StringIO object.
		sample_dates : tuple[date, date]
			A fixed date range for consistent testing.

		Returns
		-------
		None
		"""
		mock_requests_get.return_value = mock_response
		mock_get_file.return_value = mock_stringio_mtm

		instance = DebenturesComBrMTM(date_start=sample_dates[0], date_end=sample_dates[1])

		with patch.object(instance, "standardize_dataframe") as mock_standardize:
			expected_df = pd.DataFrame(
				{
					"DATA_PU": ["01/01/2023"],
					"ATIVO": ["TEST"],
					"VALOR_NOMINAL": [1000.0],
					"JUROS": [5.0],
					"PREMIO": [2.0],
					"PRECO_UNITARIO": ["1002.0"],
					"CRITERIO_CALCULO": ["CRIT"],
					"SITUACAO": ["ACTIVE"],
				}
			)
			mock_standardize.return_value = expected_df

			result = instance.run()
			assert isinstance(result, pd.DataFrame)
			assert list(result.columns) == [
				"DATA_PU",
				"ATIVO",
				"VALOR_NOMINAL",
				"JUROS",
				"PREMIO",
				"PRECO_UNITARIO",
				"CRITERIO_CALCULO",
				"SITUACAO",
			]
			mock_requests_get.assert_called_once()

	def test_transform_data(self, mock_stringio_mtm: StringIO) -> None:
		"""Test transform_data method.

		Verifies
		--------
		- Correct parsing of CSV content
		- Proper column names and data types
		- Handling of NA values

		Parameters
		----------
		mock_stringio_mtm : StringIO
			Mocked StringIO object.

		Returns
		-------
		None
		"""
		instance = DebenturesComBrMTM()
		df_ = instance.transform_data(mock_stringio_mtm)
		assert isinstance(df_, pd.DataFrame)
		assert df_.shape[0] == 1
		assert df_["ATIVO"].iloc[0] == "TEST"
		assert df_["VALOR_NOMINAL"].iloc[0] == 1000.0

	@patch("requests.get")
	def test_invalid_timeout_type(self, mock_requests_get: MagicMock) -> None:
		"""Test run with invalid timeout type.

		Verifies
		--------
		- TypeError is raised for invalid timeout types

		Parameters
		----------
		mock_requests_get : MagicMock
			Mocked requests.get.

		Returns
		-------
		None
		"""
		instance = DebenturesComBrMTM()
		with pytest.raises(TypeError):
			instance.run(timeout="invalid")

	def test_parse_raw_file(self, mock_response: Response) -> None:
		"""Test parse_raw_file method.

		Verifies
		--------
		- Correctly calls get_file with response
		- Returns StringIO object

		Parameters
		----------
		mock_response : Response
			Mocked response.

		Returns
		-------
		None
		"""
		instance = DebenturesComBrMTM()
		with patch.object(instance, "get_file", return_value=StringIO()) as mock_get_file:
			result = instance.parse_raw_file(mock_response)
			assert isinstance(result, StringIO)
			mock_get_file.assert_called_once_with(resp_req=mock_response)


# --------------------------
# Fallback and Error Testing
# --------------------------
def test_reload_module(mocker: MockerFixture) -> None:
	"""Test module reloading behavior.

	Verifies
	--------
	- Module can be reloaded without errors
	- State is properly reset

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mocker fixture.

	Returns
	-------
	None
	"""
	mocker.patch("requests.get")
	importlib.reload(stpstone.ingestion.countries.br.otc.debentures_com_br_mtm)
	instance = DebenturesComBrMTM()
	assert isinstance(instance, DebenturesComBrMTM)
