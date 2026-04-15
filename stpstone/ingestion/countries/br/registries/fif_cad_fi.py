"""Implementation of CVM FIF Registration Data (CAD/FI) ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import backoff
import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import requests
from requests import Response, Session
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.abc.ingestion_abc import (
    ABCIngestionOperations,
    ContentParser,
    CoreIngestion,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


class FIFCADFI(ABCIngestionOperations):
    """CVM FIF Registration Data (CAD/FI) - concrete implementation.

    This class handles the ingestion of fund registration data from the
    Brazilian Securities and Exchange Commission (CVM). The data includes
    comprehensive registration information for all investment funds in Brazil,
    containing fund characteristics, administrative details, service providers,
    and operational status.
    """

    def __init__(
        self,
        date_ref: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the FIF CAD/FI ingestion class.

        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference for data retrieval. If None, defaults to
            current date, by default None.
        logger : Optional[Logger], optional
            Logger instance for tracking operations, by default None.
        cls_db : Optional[Session], optional
            Database session for data persistence, by default None.

        Returns
        -------
        None
        """
        super().__init__(cls_db=cls_db)
        CoreIngestion.__init__(self)
        ContentParser.__init__(self)

        self.logger = logger
        self.cls_db = cls_db
        self.cls_dir_files_management = DirFilesManagement()
        self.cls_dates_current = DatesCurrent()
        self.cls_create_log = CreateLog()
        self.cls_dates_br = DatesBRAnbima()
        self.date_ref = date_ref or self.cls_dates_current.curr_date()

        self.url = "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"

    def run(
        self,
        timeout: Optional[
            Union[int, float, tuple[float, float], tuple[int, int]]
        ] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "br_cvm_fif_registration",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process for fund registration data.

        If a database session is provided, data is inserted into the database.
        Otherwise, the transformed DataFrame is returned for further processing.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            Request timeout in seconds. Can be a single value or tuple of
            (connect, read) timeouts, by default (12.0, 21.0).
        bool_verify : bool, optional
            Whether to verify SSL certificates, by default True.
        bool_insert_or_ignore : bool, optional
            If True, uses INSERT OR IGNORE for database operations,
            by default False.
        str_table_name : str, optional
            Target database table name, by default "br_cvm_fif_registration".

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame if no database session is provided,
            otherwise None.
        """
        resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
        file, _ = self.parse_raw_file(resp_req)
        df_ = self.transform_data(file=file)
        df_ = self.standardize_dataframe(
            df_=df_,
            date_ref=self.date_ref,
            dict_dtypes={
                "TP_FUNDO": str,
                "CNPJ_FUNDO": str,
                "DENOM_SOCIAL": str,
                "DT_REG": "date",
                "DT_CONST": "date",
                "CD_CVM": str,
                "DT_CANCEL": "date",
                "SIT": str,
                "DT_INI_SIT": "date",
                "DT_INI_ATIV": "date",
                "DT_INI_EXERC": "date",
                "DT_FIM_EXERC": "date",
                "CLASSE": str,  # codespell:ignore
                "DT_INI_CLASSE": "date",
                "RENTAB_FUNDO": str,
                "CONDOM": str,
                "FUNDO_COTAS": str,
                "FUNDO_EXCLUSIVO": str,
                "TRIB_LPRAZO": str,
                "PUBLICO_ALVO": str,
                "ENTID_INVEST": str,
                "TAXA_PERFM": str,
                "INF_TAXA_PERFM": str,
                "TAXA_ADM": str,
                "INF_TAXA_ADM": str,
                "VL_PATRIM_LIQ": float,
                "DT_PATRIM_LIQ": "date",
                "DIRETOR": str,
                "CNPJ_ADMIN": str,
                "ADMIN": str,
                "PF_PJ_GESTOR": str,
                "CPF_CNPJ_GESTOR": str,
                "GESTOR": str,
                "CNPJ_AUDITOR": str,
                "AUDITOR": str,
                "CNPJ_CUSTODIANTE": str,
                "CUSTODIANTE": str,
                "CNPJ_CONTROLADOR": str,
                "CONTROLADOR": str,
                "INVEST_CEMPR_EXTER": str,
                "CLASSE_ANBIMA": str,
                "FILE_NAME": "category",
            },
            str_fmt_dt="YYYY-MM-DD",
            url=self.url,
        )
        if self.cls_db:
            self.insert_table_db(
                cls_db=self.cls_db,
                str_table_name=str_table_name,
                df_=df_,
                bool_insert_or_ignore=bool_insert_or_ignore,
            )
        else:
            return df_

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.HTTPError,
        max_time=60,
    )
    def get_response(
        self,
        timeout: Optional[
            Union[int, float, tuple[float, float], tuple[int, int]]
        ] = (12.0, 21.0),
        bool_verify: bool = True,
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Fetch CSV file containing fund registration data from CVM website.

        Performs HTTP GET request with exponential backoff retry logic for
        handling transient network errors.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            Request timeout in seconds, by default (12.0, 21.0).
        bool_verify : bool, optional
            Whether to verify SSL certificates, by default True.

        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            The HTTP response object containing CSV data.
        """
        self.cls_create_log.log_message(
            self.logger,
            f"Fetching fund registration data from URL: {self.url}",
            "info",
        )

        resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
        resp_req.raise_for_status()

        self.cls_create_log.log_message(
            self.logger,
            f"Successfully fetched {len(resp_req.content)} bytes from CVM",
            "info",
        )

        return resp_req

    def parse_raw_file(
        self,
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
    ) -> tuple[StringIO, str]:
        """Parse raw CSV content from HTTP response.

        Handles encoding detection and conversion, trying UTF-8, Latin-1,
        and CP1252 encodings in sequence to properly decode Brazilian
        Portuguese text.

        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The HTTP response object containing CSV data.

        Returns
        -------
        tuple[StringIO, str]
            A tuple containing:
            - StringIO object with decoded CSV content
            - Filename string for tracking purposes

        Raises
        ------
        ValueError
            If response content is empty or cannot be decoded.
        """
        self.cls_create_log.log_message(
            self.logger,
            "Parsing CSV content from response",
            "info",
        )

        if not resp_req.content:
            raise ValueError("Response content is empty")

        content_bytes = resp_req.content
        csv_content = None

        for encoding in ("utf-8", "latin-1", "cp1252"):
            try:
                csv_content = StringIO(content_bytes.decode(encoding))
                self.cls_create_log.log_message(
                    self.logger,
                    f"Successfully decoded CSV with {encoding} encoding",
                    "info",
                )
                break
            except UnicodeDecodeError:
                continue

        if csv_content is None:
            self.cls_create_log.log_message(
                self.logger,
                "Using UTF-8 with error replacement as fallback",
                "warning",
            )
            csv_content = StringIO(content_bytes.decode("utf-8", errors="replace"))

        file_name = "cad_fi.csv"

        self.cls_create_log.log_message(
            self.logger,
            f"Successfully parsed CSV content: {file_name}",
            "info",
        )

        return csv_content, file_name

    def transform_data(
        self,
        file: StringIO,
    ) -> pd.DataFrame:
        """Transform CSV content into structured DataFrame.

        Reads semicolon-separated CSV data and adds metadata column for
        tracking data source. Handles potential data quality issues and
        empty values in the registration data.

        Parameters
        ----------
        file : StringIO
            StringIO object containing CSV content.

        Returns
        -------
        pd.DataFrame
            Transformed DataFrame with CVM fund registration data and metadata.
        """
        self.cls_create_log.log_message(
            self.logger,
            "Transforming registration CSV data into DataFrame",
            "info",
        )

        try:
            df_ = pd.read_csv(file, sep=";", dtype=str)
            df_ = df_.replace("", pd.NA)

        except pd.errors.ParserError as e:
            self.cls_create_log.log_message(
                self.logger,
                f"Parser error encountered: {str(e)}. Trying with error handling.",
                "warning",
            )
            file.seek(0)
            try:
                df_ = pd.read_csv(file, sep=";", on_bad_lines="skip", dtype=str)
                df_ = df_.replace("", pd.NA)
                self.cls_create_log.log_message(
                    self.logger,
                    "Successfully loaded CSV with error handling (skipping bad lines)",
                    "info",
                )
            except Exception as exc:
                self.cls_create_log.log_message(
                    self.logger,
                    f"Failed to load CSV even with error handling: {str(exc)}",
                    "error",
                )
                raise

        self.cls_create_log.log_message(
            self.logger,
            f"Successfully loaded {len(df_)} rows and {len(df_.columns)} columns",
            "info",
        )

        df_["FILE_NAME"] = "cad_fi.csv"
        active_funds = df_[df_["SIT"] != "CANCELADA"].shape[0] if "SIT" in df_.columns else 0
        cancelled_funds = df_[df_["SIT"] == "CANCELADA"].shape[0] if "SIT" in df_.columns else 0

        self.cls_create_log.log_message(
            self.logger,
            f"Registration data summary: {active_funds} active funds, {cancelled_funds} "
            "cancelled funds",
            "info",
        )

        return df_
