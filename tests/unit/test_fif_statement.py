"""Unit tests for FIFStatement ingestion module."""

from datetime import date
from io import StringIO
from logging import Logger
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests
from requests import Response, Session

from stpstone.ingestion.countries.br.registries.fif_statement import FIFStatement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_logger() -> MagicMock:
    """Provide a mock logger.

    Returns
    -------
    MagicMock
        Mock logger instance.
    """
    return MagicMock(spec=Logger)


@pytest.fixture
def mock_db_session() -> MagicMock:
    """Provide a mock database session.

    Returns
    -------
    MagicMock
        Mock database session.
    """
    return MagicMock(spec=Session)


@pytest.fixture
def sample_date() -> date:
    """Provide a sample date for testing.

    Returns
    -------
    date
        Sample date (2023-12-01).
    """
    return date(2023, 12, 1)


@pytest.fixture
def sample_csv_content() -> str:
    """Provide sample CSV content.

    Returns
    -------
    str
        Sample CSV data.
    """
    return "CNPJ_FUNDO_CLASSE;DENOM_SOCIAL;DT_COMPTC\n12345678000195;Test Fund;2023-12-01"


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Provide sample FIF statement DataFrame.

    Returns
    -------
    pd.DataFrame
        Sample DataFrame.
    """
    return pd.DataFrame({
        "CNPJ_FUNDO_CLASSE": ["12345678000195"],
        "DENOM_SOCIAL": ["Test Fund"],
    })


# --------------------------
# Tests
# --------------------------
class TestFIFStatement:
    """Test cases for FIFStatement class."""

    def test_init_url_uses_year_only(
        self, mock_logger: MagicMock, sample_date: date
    ) -> None:
        """Test URL construction uses year-only format.

        Parameters
        ----------
        mock_logger : MagicMock
            Mock logger instance.
        sample_date : date
            Sample date for testing.

        Returns
        -------
        None
        """
        instance = FIFStatement(date_ref=sample_date, logger=mock_logger)

        expected_url = (
            "https://dados.cvm.gov.br/dados/FI/DOC/EXTRATO/DADOS/extrato_fi_2023.csv"
        )
        assert instance.url == expected_url

    def test_init_with_custom_date(
        self, mock_logger: MagicMock, sample_date: date
    ) -> None:
        """Test initialization uses provided date_ref.

        Parameters
        ----------
        mock_logger : MagicMock
            Mock logger instance.
        sample_date : date
            Sample date for testing.

        Returns
        -------
        None
        """
        instance = FIFStatement(date_ref=sample_date, logger=mock_logger)

        assert instance.date_ref == sample_date

    def test_get_response_success(
        self, mock_logger: MagicMock, sample_date: date
    ) -> None:
        """Test successful HTTP response retrieval.

        Parameters
        ----------
        mock_logger : MagicMock
            Mock logger instance.
        sample_date : date
            Sample date for testing.

        Returns
        -------
        None
        """
        with patch(
            "stpstone.ingestion.countries.br.registries.fif_statement.requests.get"
        ) as mock_get:
            mock_response = MagicMock(spec=Response)
            mock_response.content = b"CSV;DATA\n1;2"
            mock_response.raise_for_status = MagicMock()
            mock_get.return_value = mock_response

            instance = FIFStatement(date_ref=sample_date, logger=mock_logger)
            result = instance.get_response()

            assert result == mock_response
            mock_response.raise_for_status.assert_called_once()

    def test_get_response_http_error(
        self, mock_logger: MagicMock, sample_date: date
    ) -> None:
        """Test HTTP error propagates from get_response.

        Parameters
        ----------
        mock_logger : MagicMock
            Mock logger instance.
        sample_date : date
            Sample date for testing.

        Returns
        -------
        None
        """
        with patch(
            "stpstone.ingestion.countries.br.registries.fif_statement.requests.get"
        ) as mock_get:
            mock_response = MagicMock(spec=Response)
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
                "404 Not Found"
            )
            mock_get.return_value = mock_response

            instance = FIFStatement(date_ref=sample_date, logger=mock_logger)

            with pytest.raises(requests.exceptions.HTTPError):
                instance.get_response()

    def test_parse_raw_file_success(
        self, mock_logger: MagicMock, sample_date: date
    ) -> None:
        """Test successful parsing of CSV response.

        Parameters
        ----------
        mock_logger : MagicMock
            Mock logger instance.
        sample_date : date
            Sample date for testing.

        Returns
        -------
        None
        """
        mock_response = MagicMock(spec=Response)
        mock_response.content = b"CNPJ;DENOM\n12345678000195;Fund"

        instance = FIFStatement(date_ref=sample_date, logger=mock_logger)
        result_content, result_filename = instance.parse_raw_file(mock_response)

        assert isinstance(result_content, StringIO)
        assert result_filename == "extrato_fi_2023.csv"

    def test_parse_raw_file_empty_content(
        self, mock_logger: MagicMock, sample_date: date
    ) -> None:
        """Test ValueError raised on empty response content.

        Parameters
        ----------
        mock_logger : MagicMock
            Mock logger instance.
        sample_date : date
            Sample date for testing.

        Returns
        -------
        None
        """
        mock_response = MagicMock(spec=Response)
        mock_response.content = b""

        instance = FIFStatement(date_ref=sample_date, logger=mock_logger)

        with pytest.raises(ValueError, match="Response content is empty"):
            instance.parse_raw_file(mock_response)

    def test_transform_data_adds_file_name(
        self,
        mock_logger: MagicMock,
        sample_date: date,
        sample_csv_content: str,
    ) -> None:
        """Test transform_data adds FILE_NAME column with year.

        Parameters
        ----------
        mock_logger : MagicMock
            Mock logger instance.
        sample_date : date
            Sample date for testing.
        sample_csv_content : str
            Sample CSV content.

        Returns
        -------
        None
        """
        instance = FIFStatement(date_ref=sample_date, logger=mock_logger)
        result_df = instance.transform_data(StringIO(sample_csv_content))

        assert isinstance(result_df, pd.DataFrame)
        assert "FILE_NAME" in result_df.columns
        assert result_df["FILE_NAME"].iloc[0] == "extrato_fi_2023.csv"

    def test_run_without_db(
        self,
        mock_logger: MagicMock,
        sample_date: date,
        sample_dataframe: pd.DataFrame,
    ) -> None:
        """Test run method without database session returns DataFrame.

        Parameters
        ----------
        mock_logger : MagicMock
            Mock logger instance.
        sample_date : date
            Sample date for testing.
        sample_dataframe : pd.DataFrame
            Sample DataFrame.

        Returns
        -------
        None
        """
        with patch.object(FIFStatement, "get_response") as mock_get_response, \
                patch.object(FIFStatement, "parse_raw_file") as mock_parse, \
                patch.object(FIFStatement, "transform_data") as mock_transform, \
                patch.object(FIFStatement, "standardize_dataframe") as mock_standardize:

            mock_get_response.return_value = MagicMock()
            mock_parse.return_value = (StringIO(), "test.csv")
            mock_transform.return_value = sample_dataframe
            mock_standardize.return_value = sample_dataframe

            instance = FIFStatement(date_ref=sample_date, logger=mock_logger)
            result = instance.run()

            assert isinstance(result, pd.DataFrame)

    def test_run_with_db(
        self,
        mock_logger: MagicMock,
        mock_db_session: MagicMock,
        sample_date: date,
        sample_dataframe: pd.DataFrame,
    ) -> None:
        """Test run method with database session returns None.

        Parameters
        ----------
        mock_logger : MagicMock
            Mock logger instance.
        mock_db_session : MagicMock
            Mock database session.
        sample_date : date
            Sample date for testing.
        sample_dataframe : pd.DataFrame
            Sample DataFrame.

        Returns
        -------
        None
        """
        with patch.object(FIFStatement, "get_response") as mock_get_response, \
                patch.object(FIFStatement, "parse_raw_file") as mock_parse, \
                patch.object(FIFStatement, "transform_data") as mock_transform, \
                patch.object(FIFStatement, "standardize_dataframe") as mock_standardize, \
                patch.object(FIFStatement, "insert_table_db") as mock_insert_db:

            mock_get_response.return_value = MagicMock()
            mock_parse.return_value = (StringIO(), "test.csv")
            mock_transform.return_value = sample_dataframe
            mock_standardize.return_value = sample_dataframe

            instance = FIFStatement(
                date_ref=sample_date, logger=mock_logger, cls_db=mock_db_session
            )
            result = instance.run()

            mock_insert_db.assert_called_once_with(
                cls_db=mock_db_session,
                str_table_name="br_cvm_fif_statement",
                df_=sample_dataframe,
                bool_insert_or_ignore=False,
            )
            assert result is None
