"""Implementation of CVM FIF Monthly Profile ingestion."""

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


class FIFMonthlyProfile(ABCIngestionOperations):
    """CVM FIF Monthly Profile Data - concrete implementation.

    This class handles the ingestion of monthly profile data for investment
    funds from the Brazilian Securities and Exchange Commission (CVM). The data
    includes detailed information about fund composition, investor profiles,
    risk factors, and performance metrics.
    """

    def __init__(
        self,
        date_ref: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the FIF Monthly Profile ingestion class.

        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference for data retrieval. If None, defaults to the
            previous working day, by default None.
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

        str_date_fmt = self.date_ref.strftime("%Y%m")
        self.url = (
            "https://dados.cvm.gov.br/dados/FI/DOC/PERFIL_MENSAL/DADOS/"
            f"perfil_mensal_fi_{str_date_fmt}.csv"
        )

    def run(
        self,
        timeout: Optional[
            Union[int, float, tuple[float, float], tuple[int, int]]
        ] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "br_cvm_fif_monthly_profile",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process for monthly profile data.

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
            Target database table name, by default "br_cvm_fif_monthly_profile".

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
                "VERSAO": int,
                "NR_COTST_PF_PB": int,
                "NR_COTST_PF_VAREJO": int,
                "NR_COTST_PJ_NAO_FINANC_PB": int,
                "NR_COTST_PJ_NAO_FINANC_VAREJO": int,
                "NR_COTST_BANCO": int,
                "NR_COTST_CORRETORA_DISTRIB": int,
                "NR_COTST_PJ_FINANC": int,
                "NR_COTST_INVNR": int,
                "NR_COTST_EAPC": int,
                "NR_COTST_EFPC": int,
                "NR_COTST_RPPS": int,
                "NR_COTST_SEGUR": int,
                "NR_COTST_CAPITALIZ": int,
                "NR_COTST_FI_CLUBE": int,
                "NR_COTST_DISTRIB": int,
                "NR_COTST_OUTRO": int,
                "PR_PL_COTST_PF_PB": float,
                "PR_PL_COTST_PF_VAREJO": float,
                "PR_PL_COTST_PJ_NAO_FINANC_PB": float,
                "PR_PL_COTST_PJ_NAO_FINANC_VAREJO": float,
                "PR_PL_COTST_BANCO": float,
                "PR_PL_COTST_CORRETORA_DISTRIB": float,
                "PR_PL_COTST_PJ_FINANC": float,
                "PR_PL_COTST_INVNR": float,
                "PR_PL_COTST_EAPC": float,
                "PR_PL_COTST_EFPC": float,
                "PR_PL_COTST_RPPS": float,
                "PR_PL_COTST_SEGUR": float,
                "PR_PL_COTST_CAPITALIZ": float,
                "PR_PL_COTST_FI_CLUBE": float,
                "PR_PL_COTST_DISTRIB": float,
                "PR_PL_COTST_OUTRO": float,
                "VOTO_ADMIN_ASSEMB": str,
                "JUSTIF_VOTO_ADMIN_ASSEMB": str,
                "PR_VAR_CARTEIRA": float,
                "MOD_VAR": str,
                "PRAZO_CARTEIRA_TITULO": float,
                "DELIB_ASSEMB": str,
                "VL_CONTRATO_COMPRA_DOLAR": float,
                "VL_CONTRATO_VENDA_DOLAR": float,
                "PR_VARIACAO_DIARIA_COTA": float,
                "FPR": str,
                "CENARIO_FPR_IBOVESPA": str,
                "CENARIO_FPR_JUROS": str,
                "CENARIO_FPR_CUPOM": str,
                "CENARIO_FPR_DOLAR": str,
                "CENARIO_FPR_OUTRO": str,
                "PR_VARIACAO_DIARIA_COTA_ESTRESSE": float,
                "PR_VARIACAO_DIARIA_PL_TAXA_ANUAL": float,
                "PR_VARIACAO_DIARIA_PL_TAXA_CAMBIO": float,
                "PR_VARIACAO_DIARIA_PL_IBOVESPA": float,
                "FATOR_RISCO_OUTRO": str,
                "PR_VARIACAO_DIARIA_OUTRO": float,
                "PR_COLATERAL_DERIV": float,
                "FATOR_RISCO_NOCIONAL": str,
                "VL_FATOR_RISCO_NOCIONAL_LONG_IBOVESPA": float,
                "VL_FATOR_RISCO_NOCIONAL_LONG_JUROS": float,
                "VL_FATOR_RISCO_NOCIONAL_LONG_CUPOM": float,
                "VL_FATOR_RISCO_NOCIONAL_LONG_DOLAR": float,
                "VL_FATOR_RISCO_NOCIONAL_LONG_OUTRO": float,
                "VL_FATOR_RISCO_NOCIONAL_SHORT_IBOVESPA": float,
                "VL_FATOR_RISCO_NOCIONAL_SHORT_JUROS": float,
                "VL_FATOR_RISCO_NOCIONAL_SHORT_CUPOM": float,
                "VL_FATOR_RISCO_NOCIONAL_SHORT_DOLAR": float,
                "VL_FATOR_RISCO_NOCIONAL_SHORT_OUTRO": float,
                "PF_PJ_COMITENTE_1": str,
                "CPF_CNPJ_COMITENTE_1": str,
                "COMITENTE_LIGADO_1": str,
                "PR_COMITENTE_1": float,
                "PF_PJ_COMITENTE_2": str,
                "CPF_CNPJ_COMITENTE_2": str,
                "COMITENTE_LIGADO_2": str,
                "PR_COMITENTE_2": float,
                "PF_PJ_COMITENTE_3": str,
                "CPF_CNPJ_COMITENTE_3": str,
                "COMITENTE_LIGADO_3": str,
                "PR_COMITENTE_3": float,
                "PR_ATIVO_EMISSOR_LIGADO": float,
                "PF_PJ_EMISSOR_1": str,
                "CPF_CNPJ_EMISSOR_1": str,
                "EMISSOR_LIGADO_1": str,
                "PR_EMISSOR_1": float,
                "PF_PJ_EMISSOR_2": str,
                "CPF_CNPJ_EMISSOR_2": str,
                "EMISSOR_LIGADO_2": str,
                "PR_EMISSOR_2": float,
                "PF_PJ_EMISSOR_3": str,
                "CPF_CNPJ_EMISSOR_3": str,
                "EMISSOR_LIGADO_3": str,
                "PR_EMISSOR_3": float,
                "PR_ATIVO_CRED_PRIV": float,
                "VEDAC_TAXA_PERFM": str,
                "DT_COTA_TAXA_PERFM": str,
                "VL_COTA_TAXA_PERFM": float,
                "VL_DIREITO_DISTRIB": float,
                "NR_COTST_ENTID_PREVID_COMPL": int,
                "PR_COTST_ENTID_PREVID_COMPL": float,
                "PR_PATRIM_LIQ_MAIOR_COTST": float,
                "NR_DIA_CINQU_PERC": int,
                "NR_DIA_CEM_PERC": int,
                "ST_LIQDEZ": str,
                "PR_PATRIM_LIQ_CONVTD_CAIXA": float,
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
            f"Fetching monthly profile data from URL: {self.url}",
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

        file_date_str = self.date_ref.strftime("%Y%m")
        file_name = f"perfil_mensal_fi_{file_date_str}.csv"

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
            Transformed DataFrame with CVM monthly profile data and metadata.
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

        file_date_str = self.date_ref.strftime("%Y%m")
        df_["FILE_NAME"] = f"perfil_mensal_fi_{file_date_str}.csv"

        return df_
