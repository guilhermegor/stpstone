"""Implementation of CVM Securities Distribution Offers ingestion."""

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


class CVMDataDistributionOffers(ABCIngestionOperations):
    """CVM Securities Distribution Offers Data - concrete implementation.

    This class handles the ingestion of securities distribution offers data
    from the Brazilian Securities and Exchange Commission (CVM). The data includes
    comprehensive information about public offerings of securities in Brazil,
    containing details about issuers, offer characteristics, pricing, distribution,
    and investor participation across different categories.
    """

    def __init__(
        self,
        date_ref: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the Distribution Offers ingestion class.

        Parameters
        ----------
        date_ref : Optional[date]
            The date of reference for data retrieval. If None, defaults to
            current date, by default None.
        logger : Optional[Logger]
            Logger instance for tracking operations, by default None.
        cls_db : Optional[Session]
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

        self.url = "https://dados.cvm.gov.br/dados/OFERTA/DISTRIB/DADOS/oferta_distribuicao.zip"

    def run(
        self,
        timeout: Optional[
            Union[int, float, tuple[float, float], tuple[int, int]]
        ] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "br_cvm_distribution_offers",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process for distribution offers data.

        If a database session is provided, data is inserted into the database.
        Otherwise, the transformed DataFrame is returned for further processing.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            Request timeout in seconds. Can be a single value or tuple of
            (connect, read) timeouts, by default (12.0, 21.0).
        bool_verify : bool
            Whether to verify SSL certificates, by default True.
        bool_insert_or_ignore : bool
            If True, uses INSERT OR IGNORE for database operations,
            by default False.
        str_table_name : str
            Target database table name, by default "br_cvm_distribution_offers".

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
                "NUMERO_PROCESSO": str,
                "NUMERO_REGISTRO_OFERTA": str,
                "TIPO_OFERTA": str,
                "TIPO_COMPONENTE_OFERTA_MISTA": str,
                "TIPO_ATIVO": str,
                "CNPJ_EMISSOR": str,
                "NOME_EMISSOR": str,
                "CNPJ_LIDER": str,
                "NOME_LIDER": str,
                "NOME_VENDEDOR": str,
                "CNPJ_OFERTANTE": str,
                "NOME_OFERTANTE": str,
                "RITO_OFERTA": str,
                "MODALIDADE_OFERTA": str,
                "MODALIDADE_REGISTRO": str,
                "MODALIDADE_DISPENSA_REGISTRO": str,
                "DATA_ABERTURA_PROCESSO": "date",
                "DATA_PROTOCOLO": "date",
                "DATA_DISPENSA_OFERTA": "date",
                "DATA_REGISTRO_OFERTA": "date",
                "DATA_INICIO_OFERTA": "date",
                "DATA_ENCERRAMENTO_OFERTA": "date",
                "EMISSAO": str,
                "CLASSE_ATIVO": str,
                "SERIE": str,  # codespell:ignore
                "ESPECIE_ATIVO": str,
                "FORMA_ATIVO": str,
                "DATA_EMISSAO": "date",
                "DATA_VENCIMENTO": "date",
                "QUANTIDADE_SEM_LOTE_SUPLEMENTAR": float,
                "QUANTIDADE_NO_LOTE_SUPLEMENTAR": float,
                "QUANTIDADE_TOTAL": float,
                "PRECO_UNITARIO": float,
                "VALOR_TOTAL": float,
                "OFERTA_INICIAL": str,
                "OFERTA_INCENTIVO_FISCAL": str,
                "OFERTA_REGIME_FIDUCIARIO": str,
                "ATUALIZACAO_MONETARIA": str,
                "JUROS": str,
                "PROJETO_AUDIOVISUAL": str,
                "TIPO_SOCIETARIO_EMISSOR": str,
                "TIPO_FUNDO_INVESTIMENTO": str,
                "ULTIMO_COMUNICADO": str,
                "DATA_COMUNICADO": "date",
                "NR_PESSOA_FISICA": int,
                "QTD_PESSOA_FISICA": float,
                "NR_CLUBE_INVESTIMENTO": int,
                "QTD_CLUBE_INVESTIMENTO": float,
                "NR_FUNDOS_INVESTIMENTO": int,
                "QTD_FUNDOS_INVESTIMENTO": float,
                "NR_ENTIDADE_PREVIDENCIA_PRIVADA": int,
                "QTD_ENTIDADE_PREVIDENCIA_PRIVADA": float,
                "NR_COMPANHIA_SEGURADORA": int,
                "QTD_COMPANHIA_SEGURADORA": float,
                "NR_INVESTIDOR_ESTRANGEIRO": int,
                "QTD_INVESTIDOR_ESTRANGEIRO": float,
                "NR_INSTIT_INTERMED_PARTIC_CONSORCIO_DISTRIB": int,
                "QTD_INSTIT_INTERMED_PARTIC_CONSORCIO_DISTRIB": float,
                "NR_INSTIT_FINANC_EMISSORA_PARTIC_CONSORCIO": int,
                "QTD_INSTIT_FINANC_EMISSORA_PARTIC_CONSORCIO": float,
                "NR_DEMAIS_INSTIT_FINANC": int,
                "QTD_DEMAIS_INSTIT_FINANC": float,
                "NR_DEMAIS_PESSOA_JURIDICA_EMISSORA_PARTIC_CONSORCIO": int,
                "QTD_DEMAIS_PESSOA_JURIDICA_EMISSORA_PARTIC_CONSORCIO": float,
                "NR_DEMAIS_PESSOA_JURIDICA": int,
                "QTD_DEMAIS_PESSOA_JURIDICA": float,
                "NR_SOC_ADM_EMP_PROP_DEMAIS_PESS_JURID_EMISS_PARTIC_CONSORCIO": int,
                "QDT_SOC_ADM_EMP_PROP_DEMAIS_PESS_JURID_EMISS_PARTIC_CONSORCIO": float,
                "NR_OUTROS": int,
                "QTD_OUTROS": float,
                "QTD_CLI_PESSOA_FISICA": float,
                "QTD_CLI_PESSOA_JURIDICA": float,
                "QTD_CLI_PESSOA_JURIDICA_LIGADA_ADM": float,
                "QTD_CLI_DEMAIS_PESSOA_JURIDICA": float,
                "QTD_CLI_INVESTIDOR_ESTRANGEIRO": float,
                "QTD_CLI_SOC_ADM_EMP_PROP_DEMAIS_PESS_JURID_EMISS_PARTIC_CONSORCIO": float,
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
        """Fetch ZIP file containing distribution offers data from CVM website.

        Performs HTTP GET request with exponential backoff retry logic for
        handling transient network errors.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            Request timeout in seconds, by default (12.0, 21.0).
        bool_verify : bool
            Whether to verify SSL certificates, by default True.

        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            The HTTP response object containing ZIP data.
        """
        self.cls_create_log.log_message(
            self.logger,
            f"Fetching distribution offers data from URL: {self.url}",
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
        """Parse raw ZIP file and extract the oferta_distribuicao.csv file.

        Extracts the distribution offers CSV file (oferta_distribuicao.csv) from
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
            If no files found in ZIP or the required CSV file not present.
        """
        self.cls_create_log.log_message(
            self.logger,
            "Extracting oferta_distribuicao.csv file from ZIP archive in memory",
            "info",
        )

        files_list = self.cls_dir_files_management.recursive_unzip_in_memory(
            BytesIO(resp_req.content)
        )

        if not files_list:
            raise ValueError("No files found in the downloaded ZIP content")

        offers_csv_file = None
        offers_filename = None

        for file_content, filename in files_list:
            if filename == "oferta_distribuicao.csv":
                offers_csv_file = file_content
                offers_filename = filename
                self.cls_create_log.log_message(
                    self.logger,
                    f"Found distribution offers file: {filename}",
                    "info",
                )
                break

        if offers_csv_file is None:
            for file_content, filename in files_list:
                if filename.lower().endswith(".csv"):
                    offers_csv_file = file_content
                    offers_filename = filename
                    self.cls_create_log.log_message(
                        self.logger,
                        f"Using alternative distribution offers file: {filename}",
                        "info",
                    )
                    break

        if offers_csv_file is None:
            raise ValueError(
                "Distribution offers file oferta_distribuicao.csv not found in "
                "the downloaded ZIP"
            )

        csv_string_io = self._decode_csv_content(offers_csv_file, offers_filename)

        self.cls_create_log.log_message(
            self.logger,
            f"Successfully extracted and decoded distribution offers CSV file: {offers_filename}",
            "info",
        )

        return csv_string_io, offers_filename

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

        Reads semicolon-separated CSV data and adds metadata column for
        tracking data source. Handles potential data quality issues and
        empty values in the distribution offers data. Converts all column
        names to uppercase for consistency.

        Parameters
        ----------
        file : StringIO
            StringIO object containing CSV content.

        Returns
        -------
        pd.DataFrame
            Transformed DataFrame with CVM distribution offers data and metadata.
        """
        self.cls_create_log.log_message(
            self.logger,
            "Transforming distribution offers CSV data into DataFrame",
            "info",
        )

        try:
            df_ = pd.read_csv(file, sep=";", dtype=str)
            df_ = df_.replace("", pd.NA)
            df_.columns = [col.upper() for col in df_.columns]

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
                df_.columns = [col.upper() for col in df_.columns]
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

        df_["FILE_NAME"] = "oferta_distribuicao.csv"

        if "TIPO_OFERTA" in df_.columns:
            offer_types = df_["TIPO_OFERTA"].value_counts()
            self.cls_create_log.log_message(
                self.logger,
                f"Offer type distribution: {offer_types.to_dict()}",
                "info",
            )

        if "TIPO_ATIVO" in df_.columns:
            asset_types = df_["TIPO_ATIVO"].value_counts()
            self.cls_create_log.log_message(
                self.logger,
                f"Asset type distribution: {asset_types.to_dict()}",
                "info",
            )

        if "MODALIDADE_OFERTA" in df_.columns:
            modality_counts = df_["MODALIDADE_OFERTA"].value_counts()
            self.cls_create_log.log_message(
                self.logger,
                f"Offer modality distribution: {modality_counts.to_dict()}",
                "info",
            )

        if "VALOR_TOTAL" in df_.columns:
            total_offer_value = df_["VALOR_TOTAL"].astype(float).sum()
            avg_offer_value = df_["VALOR_TOTAL"].astype(float).mean()
            self.cls_create_log.log_message(
                self.logger,
                f"Financial summary: Total offer value = {total_offer_value:,.2f}, "
                f"Average offer value = {avg_offer_value:,.2f}",
                "info",
            )

        return df_
