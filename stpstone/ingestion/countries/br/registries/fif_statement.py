"""Implementation of CVM FIF Statement (Extrato) ingestion."""

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


class FIFStatement(ABCIngestionOperations):
    """CVM FIF Statement (Extrato) Data - concrete implementation.

    This class handles the ingestion of statement data for investment funds
    from the Brazilian Securities and Exchange Commission (CVM). The data
    includes comprehensive fund information such as fund characteristics,
    investment policies, fees, operational details, and portfolio composition
    limits.
    """

    def __init__(
        self,
        date_ref: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the FIF Statement ingestion class.

        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference for data retrieval. If None, defaults to 22
            working days before current date, by default None.
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
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(
                self.cls_dates_current.curr_date(), -22
            )

        str_year = self.date_ref.strftime("%Y")
        self.url = (
            "https://dados.cvm.gov.br/dados/FI/DOC/EXTRATO/DADOS/"
            f"extrato_fi_{str_year}.csv"
        )

    def run(
        self,
        timeout: Optional[
            Union[int, float, tuple[float, float], tuple[int, int]]
        ] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "br_cvm_fif_statement",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process for statement data.

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
            Target database table name, by default "br_cvm_fif_statement".

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
                "TP_FUNDO_CLASSE": str,
                "CNPJ_FUNDO_CLASSE": str,
                "DENOM_SOCIAL": str,
                "DT_COMPTC": "date",
                "CONDOM": str,
                "NEGOC_MERC": str,
                "MERCADO": str,
                "TP_PRAZO": str,
                "PRAZO": str,
                "PUBLICO_ALVO": str,
                "REG_ANBIMA": str,
                "CLASSE_ANBIMA": str,
                "DISTRIB": str,
                "POLIT_INVEST": str,
                "APLIC_MAX_FUNDO_LIGADO": float,
                "RESULT_CART_INCORP_PL": str,
                "FUNDO_COTAS": str,
                "FUNDO_ESPELHO": str,
                "APLIC_MIN": float,
                "ATUALIZ_DIARIA_COTA": str,
                "PRAZO_ATUALIZ_COTA": str,
                "COTA_EMISSAO": str,
                "COTA_PL": str,
                "QT_DIA_CONVERSAO_COTA": int,
                "QT_DIA_PAGTO_COTA": int,
                "QT_DIA_RESGATE_COTAS": int,
                "QT_DIA_PAGTO_RESGATE": int,
                "TP_DIA_PAGTO_RESGATE": str,
                "TAXA_SAIDA_PAGTO_RESGATE": str,
                "TAXA_ADM": float,
                "TAXA_CUSTODIA_MAX": float,
                "EXISTE_TAXA_PERFM": str,
                "TAXA_PERFM": float,
                "PARAM_TAXA_PERFM": str,
                "PR_INDICE_REFER_TAXA_PERFM": float,
                "VL_CUPOM": float,
                "CALC_TAXA_PERFM": str,
                "INF_TAXA_PERFM": str,
                "EXISTE_TAXA_INGRESSO": str,
                "TAXA_INGRESSO_REAL": float,
                "TAXA_INGRESSO_PR": float,
                "EXISTE_TAXA_SAIDA": str,
                "TAXA_SAIDA_REAL": float,
                "TAXA_SAIDA_PR": float,
                "OPER_DERIV": str,
                "FINALIDADE_OPER_DERIV": str,
                "OPER_VL_SUPERIOR_PL": str,
                "FATOR_OPER_VL_SUPERIOR_PL": float,
                "CONTRAP_LIGADO": str,
                "INVEST_EXTERIOR": str,
                "APLIC_MAX_ATIVO_EXTERIOR": float,
                "ATIVO_CRED_PRIV": str,
                "APLIC_MAX_ATIVO_CRED_PRIV": float,
                "PR_INSTITUICAO_FINANC_MIN": float,
                "PR_INSTITUICAO_FINANC_MAX": float,
                "PR_CIA_MIN": float,
                "PR_CIA_MAX": float,
                "PR_FI_MIN": float,
                "PR_FI_MAX": float,
                "PR_UNIAO_MIN": float,
                "PR_UNIAO_MAX": float,
                "PR_ADMIN_GESTOR_MIN": float,
                "PR_ADMIN_GESTOR_MAX": float,
                "PR_EMISSOR_OUTRO_MIN": float,
                "PR_EMISSOR_OUTRO_MAX": float,
                "PR_COTA_FI_MIN": float,
                "PR_COTA_FI_MAX": float,
                "PR_COTA_FIC_MIN": float,
                "PR_COTA_FIC_MAX": float,
                "PR_COTA_FI_QUALIF_MIN": float,
                "PR_COTA_FI_QUALIF_MAX": float,
                "PR_COTA_FIC_QUALIF_MIN": float,
                "PR_COTA_FIC_QUALIF_MAX": float,
                "PR_COTA_FI_PROF_MIN": float,
                "PR_COTA_FI_PROF_MAX": float,
                "PR_COTA_FIC_PROF_MIN": float,
                "PR_COTA_FIC_PROF_MAX": float,
                "PR_COTA_FII_MIN": float,
                "PR_COTA_FII_MAX": float,
                "PR_COTA_FIDC_MIN": float,
                "PR_COTA_FIDC_MAX": float,
                "PR_COTA_FICFIDC_MIN": float,
                "PR_COTA_FICFIDC_MAX": float,
                "PR_COTA_FIDC_NP_MIN": float,
                "PR_COTA_FIDC_NP_MAX": float,
                "PR_COTA_FICFIDC_NP_MIN": float,
                "PR_COTA_FICFIDC_NP_MAX": float,
                "PR_COTA_ETF_MIN": float,
                "PR_COTA_ETF_MAX": float,
                "PR_CRI_MIN": float,
                "PR_CRI_MAX": float,
                "PR_TITPUB_MIN": float,
                "PR_TITPUB_MAX": float,
                "PR_OURO_MIN": float,
                "PR_OURO_MAX": float,
                "PR_TIT_INSTITUICAO_FINANC_BACEN_MIN": float,
                "PR_TIT_INSTITUICAO_FINANC_BACEN_MAX": float,
                "PR_VLMOB_MIN": float,
                "PR_VLMOB_MAX": float,
                "PR_ACAO_MIN": float,
                "PR_ACAO_MAX": float,
                "PR_DEBENTURE_MIN": float,
                "PR_DEBENTURE_MAX": float,
                "PR_NP_MIN": float,
                "PR_NP_MAX": float,
                "PR_COMPROM_MIN": float,
                "PR_COMPROM_MAX": float,
                "PR_DERIV_MIN": float,
                "PR_DERIV_MAX": float,
                "PR_ATIVO_OUTRO_MIN": float,
                "PR_ATIVO_OUTRO_MAX": float,
                "PR_COTA_FMIEE_MIN": float,
                "PR_COTA_FMIEE_MAX": float,
                "PR_COTA_FIP_MIN": float,
                "PR_COTA_FIP_MAX": float,
                "PR_COTA_FICFIP_MIN": float,
                "PR_COTA_FICFIP_MAX": float,
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
        """Fetch CSV file from CVM website.

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
            f"Fetching statement data from URL: {self.url}",
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

        file_year_str = self.date_ref.strftime("%Y")
        file_name = f"extrato_fi_{file_year_str}.csv"

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
        tracking data source.

        Parameters
        ----------
        file : StringIO
            StringIO object containing CSV content.

        Returns
        -------
        pd.DataFrame
            Transformed DataFrame with CVM statement data and metadata.
        """
        self.cls_create_log.log_message(
            self.logger,
            "Transforming CSV data into DataFrame",
            "info",
        )

        df_ = pd.read_csv(file, sep=";")

        self.cls_create_log.log_message(
            self.logger,
            f"Successfully loaded {len(df_)} rows and {len(df_.columns)} columns",
            "info",
        )

        file_year_str = self.date_ref.strftime("%Y")
        df_["FILE_NAME"] = f"extrato_fi_{file_year_str}.csv"

        return df_
