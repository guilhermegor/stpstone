"""Unit tests for DebenturesComBrInfos class.

Tests the ingestion functionality of DebenturesComBrInfos, including:
- Initialization with valid inputs
- Data retrieval and parsing
- Data transformation with numerous columns
- Error handling and edge cases
"""

from datetime import date
from io import StringIO
from logging import Logger
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.otc.debentures_com_br_infos import DebenturesComBrInfos


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
def mock_stringio_infos(mocker: MockerFixture) -> StringIO:
	"""Mock StringIO object with sample CSV content for Infos (85 columns).

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker for creating mock objects.

	Returns
	-------
	StringIO
		A StringIO object with sample CSV content.
	"""
	header_lines = ["Header1", "Header2", "Header3", "Header4", "Header5"]
	test_data = ["TEST"] * 85
	csv_content = "\n".join(header_lines) + "\n" + "\t".join(test_data)
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
# Tests for DebenturesComBrInfos
# --------------------------
class TestDebenturesComBrInfos:
	"""Test cases for DebenturesComBrInfos class."""

	def test_init_valid_inputs(self, sample_dates: tuple[date, date]) -> None:
		"""Test initialization with valid inputs.

		Verifies
		--------
		- Instance is created with valid date_start and date_end
		- Attributes are correctly set

		Parameters
		----------
		sample_dates : tuple[date, date]
			A fixed date range for consistent testing.

		Returns
		-------
		None
		"""
		date_start, date_end = sample_dates
		instance = DebenturesComBrInfos(date_start=date_start, date_end=date_end)
		assert instance.date_start == date_start
		assert instance.date_end == date_end
		assert isinstance(instance.logger, Logger) or instance.logger is None
		assert instance.cls_db is None

	@patch("requests.get")
	@patch(
		"stpstone.ingestion.countries.br.otc.debentures_com_br_infos.DebenturesComBrInfos.get_file"
	)
	def test_run_with_db(
		self,
		mock_get_file: MagicMock,
		mock_requests_get: MagicMock,
		mock_response: Response,
		mock_stringio_infos: StringIO,
		mock_session: MagicMock,
		sample_dates: tuple[date, date],
	) -> None:
		"""Test run method with database session.

		Verifies
		--------
		- Inserts data into database when cls_db is provided
		- Correct data transformation and standardization

		Parameters
		----------
		mock_get_file : MagicMock
			Mocked get_file method.
		mock_requests_get : MagicMock
			Mocked requests.get.
		mock_response : Response
			Mocked response object.
		mock_stringio_infos : StringIO
			Mocked StringIO object.
		mock_session : MagicMock
			Mocked database session.
		sample_dates : tuple[date, date]
			A fixed date range for consistent testing.

		Returns
		-------
		None
		"""
		mock_requests_get.return_value = mock_response
		mock_get_file.return_value = mock_stringio_infos
		instance = DebenturesComBrInfos(
			date_start=sample_dates[0], date_end=sample_dates[1], cls_db=mock_session
		)

		with (
			patch.object(instance, "insert_table_db") as mock_insert,
			patch.object(instance, "standardize_dataframe") as mock_standardize,
		):
			expected_df = pd.DataFrame(
				{col: ["TEST"] for col in instance.transform_data(mock_stringio_infos).columns}
			)
			mock_standardize.return_value = expected_df

			result = instance.run()
			assert result is None
			mock_insert.assert_called_once()

	def test_transform_data_infos(self, mock_stringio_infos: StringIO) -> None:
		"""Test transform_data method for Infos class.

		Verifies
		--------
		- Correct parsing of complex CSV content
		- Proper handling of numerous columns
		- Correct data types

		Parameters
		----------
		mock_stringio_infos : StringIO
			Mocked StringIO object.

		Returns
		-------
		None
		"""
		instance = DebenturesComBrInfos()
		df_ = instance.transform_data(mock_stringio_infos)
		assert isinstance(df_, pd.DataFrame)
		assert len(df_.columns) == 85
		assert df_["CODIGO_DO_ATIVO"].iloc[0] == "TEST"
