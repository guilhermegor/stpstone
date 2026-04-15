"""Implementation of CVM FIF Fact Sheet (Lamina) ingestion."""

from datetime import date
from io import BytesIO, StringIO
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


class FIFFactSheet(ABCIngestionOperations):
    """CVM FIF Fact Sheet (Lâmina) Data - concrete implementation.

    This class handles the ingestion of fact sheet data for investment funds
    from the Brazilian Securities and Exchange Commission (CVM). The data
    includes comprehensive fund information such as investment objectives,
    risk profiles, fees, performance metrics, and operational details.
    """

    def __init__(
        self,
        date_ref: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the FIF Fact Sheet ingestion class.

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

        str_yearmonth = self.date_ref.strftime("%Y%m")
        self.url = (
            "https://dados.cvm.gov.br/dados/FI/DOC/LAMINA/DADOS/"
            f"lamina_fi_{str_yearmonth}.zip"
        )

    def run(
        self,
        timeout: Optional[
            Union[int, float, tuple[float, float], tuple[int, int]]
        ] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "br_cvm_fif_fact_sheet",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process for fact sheet data.

        If a database session is provided, data is inserted into the database.
        Otherwise, the consolidated DataFrame is returned for further processing.

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
            Target database table name, by default "br_cvm_fif_fact_sheet".

        Returns
        -------
        Optional[pd.DataFrame]
            The consolidated DataFrame if no database session is provided,
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
                "ID_SUBCLASSE": str,
                "DENOM_SOCIAL": str,
                "DT_COMPTC": "date",
                "NM_FANTASIA": str,
                "ENDER_ELETRONICO": str,
                "PUBLICO_ALVO": str,
                "RESTR_INVEST": str,
                "OBJETIVO": str,
                "POLIT_INVEST": str,
                "PR_PL_ATIVO_EXTERIOR": float,
                "PR_PL_ATIVO_CRED_PRIV": float,
                "PR_PL_ALAVANC": float,
                "PR_ATIVO_EMISSOR": float,
                "DERIV_PROTECAO_CARTEIRA": str,
                "RISCO_PERDA": str,
                "RISCO_PERDA_NEGATIVO": str,
                "PR_PL_APLIC_MAX_FUNDO_UNICO": float,
                "INVEST_INICIAL_MIN": float,
                "INVEST_ADIC": float,
                "RESGATE_MIN": float,
                "HORA_APLIC_RESGATE": str,
                "VL_MIN_PERMAN": float,
                "QT_DIA_CAREN": int,
                "CONDIC_CAREN": str,
                "CONVERSAO_COTA_COMPRA": str,
                "QT_DIA_CONVERSAO_COTA_COMPRA": int,
                "CONVERSAO_COTA_CANC": str,
                "QT_DIA_CONVERSAO_COTA_RESGATE": int,
                "TP_DIA_PAGTO_RESGATE": str,
                "QT_DIA_PAGTO_RESGATE": int,
                "TP_TAXA_ADM": str,
                "TAXA_ADM": float,
                "TAXA_ADM_MIN": float,
                "TAXA_ADM_MAX": float,
                "TAXA_ADM_OBS": str,
                "TAXA_ENTR": float,
                "CONDIC_ENTR": str,
                "QT_DIA_SAIDA": int,
                "TAXA_SAIDA": float,
                "CONDIC_SAIDA": str,
                "TAXA_PERFM": str,
                "PR_PL_DESPESA": float,
                "DT_INI_DESPESA": str,
                "DT_FIM_DESPESA": str,
                "ENDER_ELETRONICO_DESPESA": str,
                "VL_PATRIM_LIQ": float,
                "CLASSE_RISCO_ADMIN": int,
                "PR_RENTAB_FUNDO_5ANO": float,
                "INDICE_REFER": str,
                "PR_VARIACAO_INDICE_REFER_5ANO": float,
                "QT_ANO_PERDA": int,
                "DT_INI_ATIV_5ANO": str,
                "ANO_SEM_RENTAB": str,
                "CALC_RENTAB_FUNDO_GATILHO": str,
                "PR_VARIACAO_PERFM": float,
                "CALC_RENTAB_FUNDO": str,
                "RENTAB_GATILHO": str,
                "DS_RENTAB_GATILHO": str,
                "ANO_EXEMPLO": int,
                "ANO_ANTER_EXEMPLO": int,
                "VL_RESGATE_EXEMPLO": float,
                "VL_IMPOSTO_EXEMPLO": float,
                "VL_TAXA_ENTR_EXEMPLO": float,
                "VL_TAXA_SAIDA_EXEMPLO": float,
                "VL_AJUSTE_PERFM_EXEMPLO": float,
                "VL_DESPESA_EXEMPLO": float,
                "VL_DESPESA_3ANO": float,
                "VL_DESPESA_5ANO": float,
                "VL_RETORNO_3ANO": float,
                "VL_RETORNO_5ANO": float,
                "REMUN_DISTRIB": str,
                "DISTRIB_GESTOR_UNICO": str,
                "CONFLITO_VENDA": str,
                "TEL_SAC": str,
                "ENDER_ELETRONICO_RECLAMACAO": str,
                "INF_SAC": str,
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
        """Fetch ZIP file containing fact sheet data from CVM website.

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
            The HTTP response object containing ZIP data.
        """
        self.cls_create_log.log_message(
            self.logger,
            f"Fetching fact sheet data from URL: {self.url}",
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
        """Parse raw ZIP file and extract only the main lamina_fi_YYYYMM.csv file.

        Extracts only the main fact sheet CSV file (lamina_fi_YYYYMM.csv) from
        the ZIP archive and converts it to StringIO object with proper
        encoding handling for Brazilian Portuguese text.

        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The HTTP response object containing ZIP data.

        Returns
        -------
        tuple[StringIO, str]
            A tuple containing:
            - StringIO object with decoded CSV content
            - Filename string for tracking purposes

        Raises
        ------
        ValueError
            If no files found in ZIP or main CSV file not present.
        """
        self.cls_create_log.log_message(
            self.logger,
            "Extracting main lamina_fi_YYYYMM.csv file from ZIP archive in memory",
            "info",
        )

        files_list = self.cls_dir_files_management.recursive_unzip_in_memory(
            BytesIO(resp_req.content)
        )

        if not files_list:
            raise ValueError("No files found in the downloaded ZIP content")

        main_csv_file = None
        main_filename = None

        str_yearmonth = self.date_ref.strftime("%Y%m")
        expected_main_filename = f"lamina_fi_{str_yearmonth}.csv"

        for file_content, filename in files_list:
            if filename == expected_main_filename:
                main_csv_file = file_content
                main_filename = filename
                self.cls_create_log.log_message(
                    self.logger,
                    f"Found main fact sheet file: {filename}",
                    "info",
                )
                break

        if main_csv_file is None:
            for file_content, filename in files_list:
                if (
                    filename.lower().endswith(".csv")
                    and filename.lower().startswith("lamina_fi_")
                    and "carteira" not in filename.lower()
                    and "rentab" not in filename.lower()
                ):
                    main_csv_file = file_content
                    main_filename = filename
                    self.cls_create_log.log_message(
                        self.logger,
                        f"Using alternative main fact sheet file: {filename}",
                        "info",
                    )
                    break

        if main_csv_file is None:
            raise ValueError(
                f"Main fact sheet file {expected_main_filename} not found in the downloaded ZIP"
            )

        csv_string_io = self._decode_csv_content(main_csv_file, main_filename)

        self.cls_create_log.log_message(
            self.logger,
            f"Successfully extracted and decoded main CSV file: {main_filename}",
            "info",
        )

        return csv_string_io, main_filename

    def _decode_csv_content(
        self,
        file_content: Union[BytesIO, str, StringIO],
        filename: str,
    ) -> StringIO:
        """Decode CSV content with proper encoding handling.

        Tries multiple encodings (UTF-8, Latin-1, CP1252) to properly decode
        Brazilian Portuguese text content.

        Parameters
        ----------
        file_content : Union[BytesIO, str, StringIO]
            The file content in various possible formats.
        filename : str
            The filename for logging purposes.

        Returns
        -------
        StringIO
            StringIO object with decoded content.
        """
        if isinstance(file_content, StringIO):
            return file_content

        if isinstance(file_content, str):
            return StringIO(file_content)

        if isinstance(file_content, BytesIO):
            for encoding in ("utf-8", "latin-1", "cp1252"):
                try:
                    content_str = file_content.getvalue().decode(encoding)
                    self.cls_create_log.log_message(
                        self.logger,
                        f"Successfully decoded {filename} with {encoding} encoding",
                        "info",
                    )
                    return StringIO(content_str)
                except UnicodeDecodeError:
                    continue

            self.cls_create_log.log_message(
                self.logger,
                f"Using UTF-8 with error replacement for {filename}",
                "warning",
            )
            return StringIO(file_content.getvalue().decode("utf-8", errors="replace"))

        return StringIO(str(file_content))

    def transform_data(
        self,
        file: StringIO,
    ) -> pd.DataFrame:
        """Transform CSV content into structured DataFrame.

        Reads semicolon-separated CSV data with robust error handling for
        malformed lines and adds metadata column for tracking data source.

        Parameters
        ----------
        file : StringIO
            StringIO object containing CSV content.

        Returns
        -------
        pd.DataFrame
            Transformed DataFrame with CVM fact sheet data and metadata.
        """
        self.cls_create_log.log_message(
            self.logger,
            "Transforming CSV data into DataFrame with robust error handling",
            "info",
        )

        try:
            df_ = pd.read_csv(file, sep=";")
        except pd.errors.ParserError as e:
            self.cls_create_log.log_message(
                self.logger,
                f"Parser error encountered: {str(e)}. Trying with error handling.",
                "warning",
            )
            file.seek(0)
            try:
                df_ = pd.read_csv(file, sep=";", on_bad_lines="skip")
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

        file_yearmonth_str = self.date_ref.strftime("%Y%m")
        df_["FILE_NAME"] = f"lamina_fi_{file_yearmonth_str}.csv"

        return df_
