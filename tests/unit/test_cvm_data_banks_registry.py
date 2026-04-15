"""Unit tests for CVMDataBanksRegistry ingestion module."""

from datetime import date
from io import BytesIO, StringIO
from logging import Logger
from unittest.mock import MagicMock, patch
import zipfile

import pandas as pd
import pytest
import requests
from requests import Response, Session

from stpstone.ingestion.countries.br.registries.cvm_data_banks_registry import (
    CVMDataBanksRegistry,
)


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
def mock_zip_response() -> MagicMock:
    """Provide a mock ZIP HTTP response.

    Returns
    -------
    MagicMock
        Mock response with ZIP content.
    """
    response = MagicMock(spec=Response)

    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zip_file:
        zip_file.writestr("cad_intermed.csv", "CNPJ;DENOM_SOCIAL;SIT\n12345678000195;Bank;ATIVO")

    response.content = zip_buffer.getvalue()
    response.status_code = 200
    response.raise_for_status = MagicMock()
    return response


@pytest.fixture
def sample_csv_content() -> str:
    """Provide sample CSV content.

    Returns
    -------
    str
        Sample CSV data.
    """
    return "CNPJ;DENOM_SOCIAL;SIT\n12345678000195;Bank A;EM FUNCIONAMENTO NORMAL"


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Provide sample banks registry DataFrame.

    Returns
    -------
    pd.DataFrame
        Sample DataFrame.
    """
    return pd.DataFrame({
        "CNPJ": ["12345678000195"],
        "DENOM_SOCIAL": ["Bank A"],
        "SIT": ["EM FUNCIONAMENTO NORMAL"],
    })


# --------------------------
# Tests
# --------------------------
class TestCVMDataBanksRegistry:
    """Test cases for CVMDataBanksRegistry class."""

    def test_init_static_url(
        self, mock_logger: MagicMock, sample_date: date
    ) -> None:
        """Test URL is the static cad_intermed.zip endpoint.

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
        instance = CVMDataBanksRegistry(date_ref=sample_date, logger=mock_logger)

        assert (
            instance.url
            == "https://dados.cvm.gov.br/dados/INTERMED/CAD/DADOS/cad_intermed.zip"
        )

    def test_get_response_success(
        self, mock_logger: MagicMock, sample_date: date, mock_zip_response: MagicMock
    ) -> None:
        """Test successful HTTP response retrieval.

        Parameters
        ----------
        mock_logger : MagicMock
            Mock logger instance.
        sample_date : date
            Sample date for testing.
        mock_zip_response : MagicMock
            Mock ZIP response.

        Returns
        -------
        None
        """
        with patch(
            "stpstone.ingestion.countries.br.registries.cvm_data_banks_registry.requests.get"
        ) as mock_get:
            mock_get.return_value = mock_zip_response

            instance = CVMDataBanksRegistry(date_ref=sample_date, logger=mock_logger)
            result = instance.get_response()

            assert result == mock_zip_response
            mock_zip_response.raise_for_status.assert_called_once()

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
            "stpstone.ingestion.countries.br.registries.cvm_data_banks_registry.requests.get"
        ) as mock_get:
            mock_response = MagicMock(spec=Response)
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
                "404 Not Found"
            )
            mock_get.return_value = mock_response

            instance = CVMDataBanksRegistry(date_ref=sample_date, logger=mock_logger)

            with pytest.raises(requests.exceptions.HTTPError):
                instance.get_response()

    def test_parse_raw_file_success(
        self,
        mock_logger: MagicMock,
        sample_date: date,
        mock_zip_response: MagicMock,
    ) -> None:
        """Test successful parsing extracts cad_intermed.csv.

        Parameters
        ----------
        mock_logger : MagicMock
            Mock logger instance.
        sample_date : date
            Sample date for testing.
        mock_zip_response : MagicMock
            Mock ZIP response.

        Returns
        -------
        None
        """
        with patch(
            "stpstone.ingestion.countries.br.registries.cvm_data_banks_registry.DirFilesManagement"
        ) as mock_dir_files:
            mock_dir_files_instance = MagicMock()
            mock_dir_files_instance.recursive_unzip_in_memory.return_value = [
                (
                    BytesIO(b"CNPJ;DENOM\n12345678000195;Bank"),
                    "cad_intermed.csv",
                )
            ]
            mock_dir_files.return_value = mock_dir_files_instance

            instance = CVMDataBanksRegistry(date_ref=sample_date, logger=mock_logger)
            result_content, result_filename = instance.parse_raw_file(mock_zip_response)

            assert isinstance(result_content, StringIO)
            assert result_filename == "cad_intermed.csv"

    def test_parse_raw_file_no_csv_raises(
        self,
        mock_logger: MagicMock,
        sample_date: date,
        mock_zip_response: MagicMock,
    ) -> None:
        """Test ValueError raised when no CSV file found in ZIP.

        Parameters
        ----------
        mock_logger : MagicMock
            Mock logger instance.
        sample_date : date
            Sample date for testing.
        mock_zip_response : MagicMock
            Mock ZIP response.

        Returns
        -------
        None
        """
        with patch(
            "stpstone.ingestion.countries.br.registries.cvm_data_banks_registry.DirFilesManagement"
        ) as mock_dir_files:
            mock_dir_files_instance = MagicMock()
            mock_dir_files_instance.recursive_unzip_in_memory.return_value = [
                (BytesIO(b"binary data"), "file.bin")
            ]
            mock_dir_files.return_value = mock_dir_files_instance

            instance = CVMDataBanksRegistry(date_ref=sample_date, logger=mock_logger)

            with pytest.raises(ValueError, match="not found"):
                instance.parse_raw_file(mock_zip_response)

    def test_transform_data_adds_file_name(
        self,
        mock_logger: MagicMock,
        sample_date: date,
        sample_csv_content: str,
    ) -> None:
        """Test transform_data adds FILE_NAME column.

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
        instance = CVMDataBanksRegistry(date_ref=sample_date, logger=mock_logger)
        result_df = instance.transform_data(StringIO(sample_csv_content))

        assert isinstance(result_df, pd.DataFrame)
        assert "FILE_NAME" in result_df.columns
        assert result_df["FILE_NAME"].iloc[0] == "cad_intermed.csv"

    def test_transform_data_replaces_empty_strings(
        self, mock_logger: MagicMock, sample_date: date
    ) -> None:
        """Test transform_data replaces empty strings with pd.NA.

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
        csv_with_empty = "CNPJ;DENOM_SOCIAL;SIT\n12345678000195;;ATIVO"
        instance = CVMDataBanksRegistry(date_ref=sample_date, logger=mock_logger)
        result_df = instance.transform_data(StringIO(csv_with_empty))

        assert pd.isna(result_df["DENOM_SOCIAL"].iloc[0])

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
        with patch.object(CVMDataBanksRegistry, "get_response") as mock_get_response, \
                patch.object(CVMDataBanksRegistry, "parse_raw_file") as mock_parse, \
                patch.object(CVMDataBanksRegistry, "transform_data") as mock_transform, \
                patch.object(
                    CVMDataBanksRegistry, "standardize_dataframe"
                ) as mock_standardize:

            mock_get_response.return_value = MagicMock()
            mock_parse.return_value = (StringIO(), "cad_intermed.csv")
            mock_transform.return_value = sample_dataframe
            mock_standardize.return_value = sample_dataframe

            instance = CVMDataBanksRegistry(date_ref=sample_date, logger=mock_logger)
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
        with patch.object(CVMDataBanksRegistry, "get_response") as mock_get_response, \
                patch.object(CVMDataBanksRegistry, "parse_raw_file") as mock_parse, \
                patch.object(CVMDataBanksRegistry, "transform_data") as mock_transform, \
                patch.object(
                    CVMDataBanksRegistry, "standardize_dataframe"
                ) as mock_standardize, \
                patch.object(CVMDataBanksRegistry, "insert_table_db") as mock_insert_db:

            mock_get_response.return_value = MagicMock()
            mock_parse.return_value = (StringIO(), "cad_intermed.csv")
            mock_transform.return_value = sample_dataframe
            mock_standardize.return_value = sample_dataframe

            instance = CVMDataBanksRegistry(
                date_ref=sample_date, logger=mock_logger, cls_db=mock_db_session
            )
            result = instance.run()

            mock_insert_db.assert_called_once_with(
                cls_db=mock_db_session,
                str_table_name="br_cvm_financial_intermediaries",
                df_=sample_dataframe,
                bool_insert_or_ignore=False,
            )
            assert result is None
