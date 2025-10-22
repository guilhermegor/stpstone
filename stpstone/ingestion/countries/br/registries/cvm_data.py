"""Enhanced implementation of CVM Data ingestion.

CVM stands for "Comissão de Valores Mobiliários" (Securities and Exchange Commission of Brazil).
"""

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


class FIFDailyInfos(ABCIngestionOperations):
    """Liquid funds daily infos from CVM Data - concrete implementation."""
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the ingestion class.
        
        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference, by default None.
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
            The database session, by default None.
        
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
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -4)
        
        str_date_fmt = self.date_ref.strftime("%Y%m")
        self.url = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_" \
            + f"{str_date_fmt}.zip"
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_cvm_data"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "br_cvm_data"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
        file, _ =  self.parse_raw_file(resp_req)
        df_ = self.transform_data(file=file)
        df_ = self.standardize_dataframe(
            df_=df_, 
            date_ref=self.date_ref,
            dict_dtypes={
                "TP_FUNDO_CLASSE": str,
                "CNPJ_FUNDO_CLASSE": str,
                "ID_SUBCLASSE": str,
                "DT_COMPTC": "date",
                "VL_TOTAL": float,
                "VL_QUOTA": float,
                "VL_PATRIM_LIQ": float,
                "CAPTC_DIA": float,
                "RESG_DIA": float,
                "NR_COTST": int,
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
                bool_insert_or_ignore=bool_insert_or_ignore
            )
        else:
            return df_

    @backoff.on_exception(
        backoff.expo, 
        requests.exceptions.HTTPError, 
        max_time=60
    )
    def get_response(
        self, 
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0), 
        bool_verify: bool = True
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Return a response object with the ZIP file content.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Verify the SSL certificate, by default True
        
        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        """
        self.cls_create_log.log_message(
            self.logger,
            f"Fetching data from URL: {self.url}",
            "info"
        )
        
        resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
        resp_req.raise_for_status()
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully fetched {len(resp_req.content)} bytes from CVM",
            "info"
        )
        
        return resp_req
    
    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> tuple[StringIO, str]:
        """Parse the raw ZIP file content in memory and extract CSV.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        tuple[StringIO, str]
            The parsed CSV content as StringIO and the filename.

        Raises
        ------
        ValueError
            If no CSV files found in the ZIP archive.
        """
        self.cls_create_log.log_message(
            self.logger,
            "Extracting CSV from ZIP file in memory",
            "info"
        )
        
        files_list = self.cls_dir_files_management.recursive_unzip_in_memory(
            BytesIO(resp_req.content)
        )
        
        if not files_list:
            raise ValueError("No files found in the downloaded ZIP content")
        
        csv_file_content = None
        csv_filename = None
        
        for file_content, filename in files_list:
            if filename.lower().endswith('.csv'):
                csv_file_content = file_content
                csv_filename = filename
                break
        
        if csv_file_content is None:
            raise ValueError("No CSV file found in the downloaded ZIP")
        
        self.cls_create_log.log_message(
            self.logger,
            f"Found CSV file: {csv_filename}",
            "info"
        )
        
        if isinstance(csv_file_content, BytesIO):
            try:
                content_str = csv_file_content.getvalue().decode("utf-8")
                csv_file_content = StringIO(content_str)
            except UnicodeDecodeError:
                try:
                    content_str = csv_file_content.getvalue().decode("latin-1")
                    csv_file_content = StringIO(content_str)
                except UnicodeDecodeError:
                    try:
                        content_str = csv_file_content.getvalue().decode("cp1252")
                        csv_file_content = StringIO(content_str)
                    except UnicodeDecodeError:
                        content_str = csv_file_content.getvalue().decode("utf-8", errors="replace")
                        csv_file_content = StringIO(content_str)
        
        elif isinstance(csv_file_content, str):
            csv_file_content = StringIO(csv_file_content)
        
        elif not isinstance(csv_file_content, StringIO):
            content_str = str(csv_file_content)
            csv_file_content = StringIO(content_str)
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully parsed CSV content from {csv_filename}",
            "info"
        )
        
        return csv_file_content, csv_filename
    
    def transform_data(
        self, 
        file: StringIO
    ) -> pd.DataFrame:
        """Transform the CSV content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The parsed CSV content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame with CVM fund data.
        """
        self.cls_create_log.log_message(
            self.logger,
            "Transforming CSV data into DataFrame",
            "info"
        )
        
        df_ = pd.read_csv(file, sep=";")
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully loaded {len(df_)} rows and {len(df_.columns)} columns",
            "info"
        )
        
        file_date_str = self.date_ref.strftime("%Y%m")
        df_["FILE_NAME"] = f"inf_diario_fi_{file_date_str}.csv"
        
        return df_


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
		str_table_name: str = "br_cvm_fif_monthly_profile"
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
		file, _ =  self.parse_raw_file(resp_req)
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
				bool_insert_or_ignore=bool_insert_or_ignore
			)
		else:
			return df_

	@backoff.on_exception(
		backoff.expo, 
		requests.exceptions.HTTPError, 
		max_time=60
	)
	def get_response(
		self, 
		timeout: Optional[
			Union[int, float, tuple[float, float], tuple[int, int]]
		] = (12.0, 21.0), 
		bool_verify: bool = True
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
			
		Raises
		------
		HTTPError
			If the HTTP request fails after all retry attempts.
		"""
		self.cls_create_log.log_message(
			self.logger,
			f"Fetching monthly profile data from URL: {self.url}",
			"info"
		)
		
		resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
		resp_req.raise_for_status()
		
		self.cls_create_log.log_message(
			self.logger,
			f"Successfully fetched {len(resp_req.content)} bytes from CVM",
			"info"
		)
		
		return resp_req
	
	def parse_raw_file(
		self, 
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
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
			"info"
		)
		
		if not resp_req.content:
			raise ValueError("Response content is empty")
		
		content_bytes = resp_req.content
		csv_content = None
          
		encodings = ["utf-8", "latin-1", "cp1252"]
		for encoding in encodings:
			try:
				content_str = content_bytes.decode(encoding)
				csv_content = StringIO(content_str)
				self.cls_create_log.log_message(
					self.logger,
					f"Successfully decoded CSV with {encoding} encoding",
					"info"
				)
				break
			except UnicodeDecodeError:
				continue
               
		if csv_content is None:
			self.cls_create_log.log_message(
				self.logger,
				"Using UTF-8 with error replacement as fallback",
				"warning"
			)
			content_str = content_bytes.decode("utf-8", errors="replace")
			csv_content = StringIO(content_str)
        
		file_date_str = self.date_ref.strftime("%Y%m")
		file_name = f"perfil_mensal_fi_{file_date_str}.csv"
		
		self.cls_create_log.log_message(
			self.logger,
			f"Successfully parsed CSV content: {file_name}",
			"info"
		)
		
		return csv_content, file_name
	
	def transform_data(
		self, 
		file: StringIO
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
			
		Raises
		------
		pd.errors.EmptyDataError
			If the CSV file is empty.
		pd.errors.ParserError
			If the CSV format is invalid.
		"""
		self.cls_create_log.log_message(
			self.logger,
			"Transforming CSV data into DataFrame",
			"info"
		)
        
		df_ = pd.read_csv(file, sep=";")
		
		self.cls_create_log.log_message(
			self.logger,
			f"Successfully loaded {len(df_)} rows and {len(df_.columns)} columns",
			"info"
		)
            
		file_date_str = self.date_ref.strftime("%Y%m")
		df_["FILE_NAME"] = f"perfil_mensal_fi_{file_date_str}.csv"
		
		return df_


class FIFCDA(ABCIngestionOperations):
    """CVM FIF CDA (Composição e Diversificação das Aplicações) Data.
    
    This class handles the ingestion of CDA data for investment funds from the 
    Brazilian Securities and Exchange Commission (CVM). The data includes 
    detailed composition and diversification of fund investments across 
    different asset types. The source ZIP file contains multiple CSV files 
    that are consolidated into a single DataFrame.
    """
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the FIF CDA ingestion class.
        
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
        
        str_date_fmt = self.date_ref.strftime("%Y%m")
        self.url = (
            "https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/"
            f"cda_fi_{str_date_fmt}.zip"
        )
    
    def run(
        self,
        timeout: Optional[
            Union[int, float, tuple[float, float], tuple[int, int]]
        ] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_cvm_fif_cda"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process for CDA data.
        
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
            Target database table name, by default "br_cvm_fif_cda".

        Returns
        -------
        Optional[pd.DataFrame]
            The consolidated DataFrame if no database session is provided, 
            otherwise None.
        """
        resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
        files_list = self.parse_raw_file(resp_req)
        df_ = self.transform_data(files_list=files_list)
        df_ = self.standardize_dataframe(
            df_=df_, 
            date_ref=self.date_ref,
            dict_dtypes={
                "TP_FUNDO_CLASSE": str,
                "CNPJ_FUNDO_CLASSE": str,
                "DENOM_SOCIAL": str,
                "DT_COMPTC": "date",
                "ID_DOC": str,
                "TP_APLIC": str,
                "TP_ATIVO": str,
                "EMISSOR_LIGADO": str,
                "TP_NEGOC": str,
                "QT_VENDA_NEGOC": float,
                "VL_VENDA_NEGOC": float,
                "QT_AQUIS_NEGOC": float,
                "VL_AQUIS_NEGOC": float,
                "QT_POS_FINAL": float,
                "VL_MERC_POS_FINAL": float,
                "VL_CUSTO_POS_FINAL": float,
                "DT_CONFID_APLIC": str,
                "TP_TITPUB": str,
                "CD_ISIN": str,
                "CD_SELIC": str,
                "DT_EMISSAO": str,
                "DT_VENC": str,
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
                bool_insert_or_ignore=bool_insert_or_ignore
            )
        else:
            return df_

    @backoff.on_exception(
        backoff.expo, 
        requests.exceptions.HTTPError, 
        max_time=60
    )
    def get_response(
        self, 
        timeout: Optional[
            Union[int, float, tuple[float, float], tuple[int, int]]
        ] = (12.0, 21.0), 
        bool_verify: bool = True
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Fetch ZIP file containing CDA data from CVM website.
        
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
            
        Raises
        ------
        HTTPError
            If the HTTP request fails after all retry attempts.
        """
        self.cls_create_log.log_message(
            self.logger,
            f"Fetching CDA data from URL: {self.url}",
            "info"
        )
        
        resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
        resp_req.raise_for_status()
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully fetched {len(resp_req.content)} bytes from CVM",
            "info"
        )
        
        return resp_req
    
    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> list[tuple[StringIO, str]]:
        """Parse raw ZIP file and extract all CSV files.
        
        Extracts all CSV files from the ZIP archive and converts them to 
        StringIO objects with proper encoding handling for Brazilian 
        Portuguese text.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The HTTP response object containing ZIP data.
        
        Returns
        -------
        list[tuple[StringIO, str]]
            List of tuples, each containing:
            - StringIO object with decoded CSV content
            - Filename string for tracking purposes

        Raises
        ------
        ValueError
            If no files found in ZIP or no CSV files present.
        """
        self.cls_create_log.log_message(
            self.logger,
            "Extracting CSV files from ZIP archive in memory",
            "info"
        )
        
        files_list = self.cls_dir_files_management.recursive_unzip_in_memory(
            BytesIO(resp_req.content)
        )
        
        if not files_list:
            raise ValueError("No files found in the downloaded ZIP content")
        
        csv_files = []
        for file_content, filename in files_list:
            if not filename.lower().endswith('.csv'):
                self.cls_create_log.log_message(
                    self.logger,
                    f"Skipping non-CSV file: {filename}",
                    "debug"
                )
                continue
            
            self.cls_create_log.log_message(
                self.logger,
                f"Processing CSV file: {filename}",
                "info"
            )
            
            csv_string_io = self._decode_csv_content(file_content, filename)
            csv_files.append((csv_string_io, filename))
        
        if not csv_files:
            raise ValueError("No CSV files found in the downloaded ZIP")
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully extracted {len(csv_files)} CSV files from ZIP",
            "info"
        )
        
        return csv_files
    
    def _decode_csv_content(
        self, 
        file_content: Union[BytesIO, str, StringIO], 
        filename: str
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
            encodings = ["utf-8", "latin-1", "cp1252"]
            for encoding in encodings:
                try:
                    content_str = file_content.getvalue().decode(encoding)
                    self.cls_create_log.log_message(
                        self.logger,
                        f"Successfully decoded {filename} with {encoding} encoding",
                        "debug"
                    )
                    return StringIO(content_str)
                except UnicodeDecodeError:
                    continue
                
            self.cls_create_log.log_message(
                self.logger,
                f"Using UTF-8 with error replacement for {filename}",
                "warning"
            )
            content_str = file_content.getvalue().decode("utf-8", errors="replace")
            return StringIO(content_str)
        
        return StringIO(str(file_content))
    
    def transform_data(
        self, 
        files_list: list[tuple[StringIO, str]]
    ) -> pd.DataFrame:
        """Transform multiple CSV files into consolidated DataFrame.
        
        Reads all CSV files, adds file metadata, and consolidates them into 
        a single DataFrame. Handles files with different column structures by 
        using union of all columns and filling missing values.
        
        Parameters
        ----------
        files_list : list[tuple[StringIO, str]]
            List of tuples containing StringIO objects and filenames.
        
        Returns
        -------
        pd.DataFrame
            Consolidated DataFrame with CVM CDA data from all CSV files.
            
        Raises
        ------
        pd.errors.EmptyDataError
            If any CSV file is empty.
        pd.errors.ParserError
            If any CSV format is invalid.
        ValueError
            If no valid data could be loaded from any file.
        """
        self.cls_create_log.log_message(
            self.logger,
            f"Consolidating {len(files_list)} CSV files into single DataFrame",
            "info"
        )
        
        dataframes = []
        total_rows = 0
        
        for file_io, filename in files_list:
            try:
                df_temp = pd.read_csv(file_io, sep=";")
                
                df_temp["FILE_NAME"] = filename
                
                dataframes.append(df_temp)
                total_rows += len(df_temp)
                
                self.cls_create_log.log_message(
                    self.logger,
                    f"Loaded {len(df_temp)} rows from {filename}",
                    "info"
                )
            except pd.errors.EmptyDataError:
                self.cls_create_log.log_message(
                    self.logger,
                    f"Skipping empty file: {filename}",
                    "warning"
                )
            except pd.errors.ParserError as e:
                self.cls_create_log.log_message(
                    self.logger,
                    f"Error parsing {filename}: {str(e)}",
                    "error"
                )
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger,
                    f"Unexpected error processing {filename}: {str(e)}",
                    "error"
                )
        
        if not dataframes:
            raise ValueError("No valid data could be loaded from any CSV file")
        self.cls_create_log.log_message(
            self.logger,
            "Concatenating all dataframes",
            "info"
        )
        df_consolidated = pd.concat(dataframes, ignore_index=True, sort=False)
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully consolidated {total_rows} total rows into "
            f"DataFrame with {len(df_consolidated.columns)} columns",
            "info"
        )
        
        return df_consolidated
    

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
        str_table_name: str = "br_cvm_fif_statement"
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
        file, _ =  self.parse_raw_file(resp_req)
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
                bool_insert_or_ignore=bool_insert_or_ignore
            )
        else:
            return df_

    @backoff.on_exception(
        backoff.expo, 
        requests.exceptions.HTTPError, 
        max_time=60
    )
    def get_response(
        self, 
        timeout: Optional[
            Union[int, float, tuple[float, float], tuple[int, int]]
        ] = (12.0, 21.0), 
        bool_verify: bool = True
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
            
        Raises
        ------
        HTTPError
            If the HTTP request fails after all retry attempts.
        """
        self.cls_create_log.log_message(
            self.logger,
            f"Fetching statement data from URL: {self.url}",
            "info"
        )
        
        resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
        resp_req.raise_for_status()
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully fetched {len(resp_req.content)} bytes from CVM",
            "info"
        )
        
        return resp_req
    
    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
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
            "info"
        )
        
        if not resp_req.content:
            raise ValueError("Response content is empty")
        
        content_bytes = resp_req.content
        csv_content = None
          
        encodings = ["utf-8", "latin-1", "cp1252"]
        for encoding in encodings:
            try:
                content_str = content_bytes.decode(encoding)
                csv_content = StringIO(content_str)
                self.cls_create_log.log_message(
                    self.logger,
                    f"Successfully decoded CSV with {encoding} encoding",
                    "info"
                )
                break
            except UnicodeDecodeError:
                continue
               
        if csv_content is None:
            self.cls_create_log.log_message(
                self.logger,
                "Using UTF-8 with error replacement as fallback",
                "warning"
            )
            content_str = content_bytes.decode("utf-8", errors="replace")
            csv_content = StringIO(content_str)
        
        file_year_str = self.date_ref.strftime("%Y")
        file_name = f"extrato_fi_{file_year_str}.csv"
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully parsed CSV content: {file_name}",
            "info"
        )
        
        return csv_content, file_name
    
    def transform_data(
        self, 
        file: StringIO
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
            
        Raises
        ------
        pd.errors.EmptyDataError
            If the CSV file is empty.
        pd.errors.ParserError
            If the CSV format is invalid.
        """
        self.cls_create_log.log_message(
            self.logger,
            "Transforming CSV data into DataFrame",
            "info"
        )
        
        df_ = pd.read_csv(file, sep=";")
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully loaded {len(df_)} rows and {len(df_.columns)} columns",
            "info"
        )
            
        file_year_str = self.date_ref.strftime("%Y")
        df_["FILE_NAME"] = f"extrato_fi_{file_year_str}.csv"
        
        return df_
    

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
        str_table_name: str = "br_cvm_fif_fact_sheet"
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
        file, _ =  self.parse_raw_file(resp_req)
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
                bool_insert_or_ignore=bool_insert_or_ignore
            )
        else:
            return df_

    @backoff.on_exception(
        backoff.expo, 
        requests.exceptions.HTTPError, 
        max_time=60
    )
    def get_response(
        self, 
        timeout: Optional[
            Union[int, float, tuple[float, float], tuple[int, int]]
        ] = (12.0, 21.0), 
        bool_verify: bool = True
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
            
        Raises
        ------
        HTTPError
            If the HTTP request fails after all retry attempts.
        """
        self.cls_create_log.log_message(
            self.logger,
            f"Fetching fact sheet data from URL: {self.url}",
            "info"
        )
        
        resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
        resp_req.raise_for_status()
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully fetched {len(resp_req.content)} bytes from CVM",
            "info"
        )
        
        return resp_req
    
    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
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
            "info"
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
                    "info"
                )
                break
        
        if main_csv_file is None:
            for file_content, filename in files_list:
                if (filename.lower().endswith('.csv') and 
                    filename.lower().startswith('lamina_fi_') and
                    'carteira' not in filename.lower() and
                    'rentab' not in filename.lower()):
                    main_csv_file = file_content
                    main_filename = filename
                    self.cls_create_log.log_message(
                        self.logger,
                        f"Using alternative main fact sheet file: {filename}",
                        "info"
                    )
                    break
        
        if main_csv_file is None:
            raise ValueError(
                f"Main fact sheet file {expected_main_filename} not found in the downloaded ZIP")
        
        csv_string_io = self._decode_csv_content(main_csv_file, main_filename)
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully extracted and decoded main CSV file: {main_filename}",
            "info"
        )
        
        return csv_string_io, main_filename
    
    def _decode_csv_content(
        self, 
        file_content: Union[BytesIO, str, StringIO], 
        filename: str
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
            encodings = ["utf-8", "latin-1", "cp1252"]
            for encoding in encodings:
                try:
                    content_str = file_content.getvalue().decode(encoding)
                    self.cls_create_log.log_message(
                        self.logger,
                        f"Successfully decoded {filename} with {encoding} encoding",
                        "debug"
                    )
                    return StringIO(content_str)
                except UnicodeDecodeError:
                    continue
                
            self.cls_create_log.log_message(
                self.logger,
                f"Using UTF-8 with error replacement for {filename}",
                "warning"
            )
            content_str = file_content.getvalue().decode("utf-8", errors="replace")
            return StringIO(content_str)
        
        return StringIO(str(file_content))
    
    def transform_data(
        self, 
        file: StringIO
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
            
        Raises
        ------
        pd.errors.EmptyDataError
            If the CSV file is empty.
        pd.errors.ParserError
            If the CSV format is invalid.
        """
        self.cls_create_log.log_message(
            self.logger,
            "Transforming CSV data into DataFrame with robust error handling",
            "info"
        )
        
        try:
            df_ = pd.read_csv(file, sep=";")
        except pd.errors.ParserError as e:
            self.cls_create_log.log_message(
                self.logger,
                f"Parser error encountered: {str(e)}. Trying with error handling.",
                "warning"
            )
            
            file.seek(0)
            try:
                df_ = pd.read_csv(file, sep=";", on_bad_lines='skip')
                self.cls_create_log.log_message(
                    self.logger,
                    "Successfully loaded CSV with error handling (skipping bad lines)",
                    "info"
                )
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger,
                    f"Failed to load CSV even with error handling: {str(e)}",
                    "error"
                )
                raise
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully loaded {len(df_)} rows and {len(df_.columns)} columns",
            "info"
        )
        
        file_yearmonth_str = self.date_ref.strftime("%Y%m")
        df_["FILE_NAME"] = f"lamina_fi_{file_yearmonth_str}.csv"
        
        return df_


class FIFPortfolio(ABCIngestionOperations):
    """CVM FIF Portfolio Composition Data - concrete implementation.
    
    This class handles the ingestion of portfolio composition data for investment funds 
    from the Brazilian Securities and Exchange Commission (CVM). The data includes 
    detailed breakdown of fund investments across different asset types, showing the 
    percentage allocation of each asset class in the fund's portfolio.
    """
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the FIF Portfolio ingestion class.
        
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
        str_table_name: str = "br_cvm_fif_portfolio"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process for portfolio composition data.
        
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
            Target database table name, by default "br_cvm_fif_portfolio".

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame if no database session is provided, 
            otherwise None.
        """
        resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
        file, _ =  self.parse_raw_file(resp_req)
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
                "TP_ATIVO": str,
                "PR_PL_ATIVO": float,
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
                bool_insert_or_ignore=bool_insert_or_ignore
            )
        else:
            return df_

    @backoff.on_exception(
        backoff.expo, 
        requests.exceptions.HTTPError, 
        max_time=60
    )
    def get_response(
        self, 
        timeout: Optional[
            Union[int, float, tuple[float, float], tuple[int, int]]
        ] = (12.0, 21.0), 
        bool_verify: bool = True
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Fetch ZIP file containing portfolio data from CVM website.
        
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
            
        Raises
        ------
        HTTPError
            If the HTTP request fails after all retry attempts.
        """
        self.cls_create_log.log_message(
            self.logger,
            f"Fetching portfolio data from URL: {self.url}",
            "info"
        )
        
        resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
        resp_req.raise_for_status()
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully fetched {len(resp_req.content)} bytes from CVM",
            "info"
        )
        
        return resp_req
    
    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> tuple[StringIO, str]:
        """Parse raw ZIP file and extract only the lamina_fi_carteira_*.csv file.
        
        Extracts only the portfolio composition CSV file (lamina_fi_carteira_*.csv) from 
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
            If no files found in ZIP or portfolio CSV file not present.
        """
        self.cls_create_log.log_message(
            self.logger,
            "Extracting lamina_fi_carteira_*.csv file from ZIP archive in memory",
            "info"
        )
        
        files_list = self.cls_dir_files_management.recursive_unzip_in_memory(
            BytesIO(resp_req.content)
        )
        
        if not files_list:
            raise ValueError("No files found in the downloaded ZIP content")
        
        portfolio_csv_file = None
        portfolio_filename = None
        
        str_yearmonth = self.date_ref.strftime("%Y%m")
        expected_portfolio_filename = f"lamina_fi_carteira_{str_yearmonth}.csv"
        
        for file_content, filename in files_list:
            if filename == expected_portfolio_filename:
                portfolio_csv_file = file_content
                portfolio_filename = filename
                self.cls_create_log.log_message(
                    self.logger,
                    f"Found portfolio composition file: {filename}",
                    "info"
                )
                break
        
        if portfolio_csv_file is None:
            for file_content, filename in files_list:
                if (filename.lower().endswith('.csv') and 
                    'carteira' in filename.lower()):
                    portfolio_csv_file = file_content
                    portfolio_filename = filename
                    self.cls_create_log.log_message(
                        self.logger,
                        f"Using alternative portfolio composition file: {filename}",
                        "info"
                    )
                    break
        
        if portfolio_csv_file is None:
            raise ValueError(f"Portfolio composition file {expected_portfolio_filename} "
                             "not found in the downloaded ZIP")
        
        csv_string_io = self._decode_csv_content(portfolio_csv_file, portfolio_filename)
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully extracted and decoded portfolio CSV file: {portfolio_filename}",
            "info"
        )
        
        return csv_string_io, portfolio_filename
    
    def _decode_csv_content(
        self, 
        file_content: Union[BytesIO, str, StringIO], 
        filename: str
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
            encodings = ["utf-8", "latin-1", "cp1252"]
            for encoding in encodings:
                try:
                    content_str = file_content.getvalue().decode(encoding)
                    self.cls_create_log.log_message(
                        self.logger,
                        f"Successfully decoded {filename} with {encoding} encoding",
                        "debug"
                    )
                    return StringIO(content_str)
                except UnicodeDecodeError:
                    continue
                
            self.cls_create_log.log_message(
                self.logger,
                f"Using UTF-8 with error replacement for {filename}",
                "warning"
            )
            content_str = file_content.getvalue().decode("utf-8", errors="replace")
            return StringIO(content_str)
        
        return StringIO(str(file_content))
    
    def transform_data(
        self, 
        file: StringIO
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
            Transformed DataFrame with CVM portfolio composition data and metadata.
            
        Raises
        ------
        pd.errors.EmptyDataError
            If the CSV file is empty.
        pd.errors.ParserError
            If the CSV format is invalid.
        """
        self.cls_create_log.log_message(
            self.logger,
            "Transforming portfolio CSV data into DataFrame",
            "info"
        )
        
        try:
            df_ = pd.read_csv(file, sep=";")
        except pd.errors.ParserError as e:
            self.cls_create_log.log_message(
                self.logger,
                f"Parser error encountered: {str(e)}. Trying with error handling.",
                "warning"
            )
            file.seek(0)
            try:
                df_ = pd.read_csv(file, sep=";", on_bad_lines='skip')
                self.cls_create_log.log_message(
                    self.logger,
                    "Successfully loaded CSV with error handling (skipping bad lines)",
                    "info"
                )
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger,
                    f"Failed to load CSV even with error handling: {str(e)}",
                    "error"
                )
                raise
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully loaded {len(df_)} rows and {len(df_.columns)} columns",
            "info"
        )
        
        file_yearmonth_str = self.date_ref.strftime("%Y%m")
        df_["FILE_NAME"] = f"lamina_fi_carteira_{file_yearmonth_str}.csv"
        
        return df_
    

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
        str_table_name: str = "br_cvm_fif_registration"
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
        file, _ =  self.parse_raw_file(resp_req)
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
                "CLASSE": str,
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
                bool_insert_or_ignore=bool_insert_or_ignore
            )
        else:
            return df_

    @backoff.on_exception(
        backoff.expo, 
        requests.exceptions.HTTPError, 
        max_time=60
    )
    def get_response(
        self, 
        timeout: Optional[
            Union[int, float, tuple[float, float], tuple[int, int]]
        ] = (12.0, 21.0), 
        bool_verify: bool = True
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
            
        Raises
        ------
        HTTPError
            If the HTTP request fails after all retry attempts.
        """
        self.cls_create_log.log_message(
            self.logger,
            f"Fetching fund registration data from URL: {self.url}",
            "info"
        )
        
        resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
        resp_req.raise_for_status()
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully fetched {len(resp_req.content)} bytes from CVM",
            "info"
        )
        
        return resp_req
    
    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
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
            "info"
        )
        
        if not resp_req.content:
            raise ValueError("Response content is empty")
        
        content_bytes = resp_req.content
        csv_content = None
          
        encodings = ["utf-8", "latin-1", "cp1252"]
        for encoding in encodings:
            try:
                content_str = content_bytes.decode(encoding)
                csv_content = StringIO(content_str)
                self.cls_create_log.log_message(
                    self.logger,
                    f"Successfully decoded CSV with {encoding} encoding",
                    "info"
                )
                break
            except UnicodeDecodeError:
                continue
               
        if csv_content is None:
            self.cls_create_log.log_message(
                self.logger,
                "Using UTF-8 with error replacement as fallback",
                "warning"
            )
            content_str = content_bytes.decode("utf-8", errors="replace")
            csv_content = StringIO(content_str)
        
        file_name = "cad_fi.csv"
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully parsed CSV content: {file_name}",
            "info"
        )
        
        return csv_content, file_name
    
    def transform_data(
        self, 
        file: StringIO
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
            
        Raises
        ------
        pd.errors.EmptyDataError
            If the CSV file is empty.
        pd.errors.ParserError
            If the CSV format is invalid.
        """
        self.cls_create_log.log_message(
            self.logger,
            "Transforming registration CSV data into DataFrame",
            "info"
        )
        
        try:
            df_ = pd.read_csv(file, sep=";", dtype=str)
            df_ = df_.replace('', pd.NA)
            
        except pd.errors.ParserError as e:
            self.cls_create_log.log_message(
                self.logger,
                f"Parser error encountered: {str(e)}. Trying with error handling.",
                "warning"
            )
            
            file.seek(0)
            try:
                df_ = pd.read_csv(file, sep=";", on_bad_lines='skip', dtype=str)
                df_ = df_.replace('', pd.NA)
                self.cls_create_log.log_message(
                    self.logger,
                    "Successfully loaded CSV with error handling (skipping bad lines)",
                    "info"
                )
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger,
                    f"Failed to load CSV even with error handling: {str(e)}",
                    "error"
                )
                raise
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully loaded {len(df_)} rows and {len(df_.columns)} columns",
            "info"
        )
        
        df_["FILE_NAME"] = "cad_fi.csv"
        active_funds = df_[df_["SIT"] != "CANCELADA"].shape[0] if "SIT" in df_.columns else 0
        cancelled_funds = df_[df_["SIT"] == "CANCELADA"].shape[0] if "SIT" in df_.columns else 0
        
        self.cls_create_log.log_message(
            self.logger,
            f"Registration data summary: {active_funds} active funds, {cancelled_funds} "
            "cancelled funds",
            "info"
        )
        
        return df_


class CVMDataBanksRegistry(ABCIngestionOperations):
    """CVM Financial Intermediaries Registry Data - concrete implementation.
    
    This class handles the ingestion of financial intermediaries registration data 
    from the Brazilian Securities and Exchange Commission (CVM). The data includes 
    comprehensive information about banks, investment banks, and other financial 
    institutions registered with CVM, containing institutional details, contact 
    information, operational status, and financial data.
    """
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the Financial Intermediaries Registry ingestion class.
        
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
        
        self.url = "https://dados.cvm.gov.br/dados/INTERMED/CAD/DADOS/cad_intermed.zip"
    
    def run(
        self,
        timeout: Optional[
            Union[int, float, tuple[float, float], tuple[int, int]]
        ] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_cvm_financial_intermediaries"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process for financial intermediaries data.
        
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
            Target database table name, by default "br_cvm_financial_intermediaries".

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame if no database session is provided, 
            otherwise None.
        """
        resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
        file, file_name = self.parse_raw_file(resp_req)
        df_ = self.transform_data(file=file)
        df_ = self.standardize_dataframe(
            df_=df_, 
            date_ref=self.date_ref,
            dict_dtypes={
                "TP_PARTIC": str,
                "CNPJ": str,
                "DENOM_SOCIAL": str,
                "DENOM_COMERC": str,
                "DT_REG": "date",
                "DT_CANCEL": "date",
                "MOTIVO_CANCEL": str,
                "SIT": str,
                "DT_INI_SIT": "date",
                "CD_CVM": str,
                "SETOR_ATIV": str,
                "CONTROLE_ACIONARIO": str,
                "VL_PATRIM_LIQ": float,
                "DT_PATRIM_LIQ": "date",
                "TP_ENDER": str,
                "LOGRADOURO": str,
                "COMPL": str,
                "BAIRRO": str,
                "MUN": str,
                "UF": str,
                "PAIS": str,
                "CEP": str,
                "DDD_TEL": str,
                "TEL": str,
                "DDD_FAX": str,
                "FAX": str,
                "EMAIL": str,
                "SITE_WEB": str,
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
                bool_insert_or_ignore=bool_insert_or_ignore
            )
        else:
            return df_

    @backoff.on_exception(
        backoff.expo, 
        requests.exceptions.HTTPError, 
        max_time=60
    )
    def get_response(
        self, 
        timeout: Optional[
            Union[int, float, tuple[float, float], tuple[int, int]]
        ] = (12.0, 21.0), 
        bool_verify: bool = True
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Fetch ZIP file containing financial intermediaries data from CVM website.
        
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
            
        Raises
        ------
        HTTPError
            If the HTTP request fails after all retry attempts.
        """
        self.cls_create_log.log_message(
            self.logger,
            f"Fetching financial intermediaries data from URL: {self.url}",
            "info"
        )
        
        resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
        resp_req.raise_for_status()
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully fetched {len(resp_req.content)} bytes from CVM",
            "info"
        )
        
        return resp_req
    
    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> tuple[StringIO, str]:
        """Parse raw ZIP file and extract the cad_intermed.csv file.
        
        Extracts the financial intermediaries CSV file (cad_intermed.csv) from 
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
            "Extracting cad_intermed.csv file from ZIP archive in memory",
            "info"
        )
        
        files_list = self.cls_dir_files_management.recursive_unzip_in_memory(
            BytesIO(resp_req.content)
        )
        
        if not files_list:
            raise ValueError("No files found in the downloaded ZIP content")
        
        intermed_csv_file = None
        intermed_filename = None
        
        for file_content, filename in files_list:
            if filename == "cad_intermed.csv":
                intermed_csv_file = file_content
                intermed_filename = filename
                self.cls_create_log.log_message(
                    self.logger,
                    f"Found financial intermediaries file: {filename}",
                    "info"
                )
                break
        
        if intermed_csv_file is None:
            for file_content, filename in files_list:
                if filename.lower().endswith('.csv'):
                    intermed_csv_file = file_content
                    intermed_filename = filename
                    self.cls_create_log.log_message(
                        self.logger,
                        f"Using alternative financial intermediaries file: {filename}",
                        "info"
                    )
                    break
        
        if intermed_csv_file is None:
            raise ValueError("Financial intermediaries file cad_intermed.csv not found in "
                             "the downloaded ZIP")
        
        # Decode the CSV content
        csv_string_io = self._decode_csv_content(intermed_csv_file, intermed_filename)
        
        self.cls_create_log.log_message(
            self.logger,
            "Successfully extracted and decoded financial intermediaries "
            f"CSV file: {intermed_filename}",
            "info"
        )
        
        return csv_string_io, intermed_filename
    
    def _decode_csv_content(
        self, 
        file_content: Union[BytesIO, str, StringIO], 
        filename: str
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
            encodings = ["utf-8", "latin-1", "cp1252"]
            for encoding in encodings:
                try:
                    content_str = file_content.getvalue().decode(encoding)
                    self.cls_create_log.log_message(
                        self.logger,
                        f"Successfully decoded {filename} with {encoding} encoding",
                        "debug"
                    )
                    return StringIO(content_str)
                except UnicodeDecodeError:
                    continue
                
            self.cls_create_log.log_message(
                self.logger,
                f"Using UTF-8 with error replacement for {filename}",
                "warning"
            )
            content_str = file_content.getvalue().decode("utf-8", errors="replace")
            return StringIO(content_str)
        
        return StringIO(str(file_content))
    
    def transform_data(
        self, 
        file: StringIO
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
            Transformed DataFrame with CVM financial intermediaries data and metadata.
            
        Raises
        ------
        pd.errors.EmptyDataError
            If the CSV file is empty.
        pd.errors.ParserError
            If the CSV format is invalid.
        """
        self.cls_create_log.log_message(
            self.logger,
            "Transforming financial intermediaries CSV data into DataFrame",
            "info"
        )
        
        try:
            df_ = pd.read_csv(file, sep=";", dtype=str)
            df_ = df_.replace('', pd.NA)
            
        except pd.errors.ParserError as e:
            self.cls_create_log.log_message(
                self.logger,
                f"Parser error encountered: {str(e)}. Trying with error handling.",
                "warning"
            )
            file.seek(0)
            
            try:
                df_ = pd.read_csv(file, sep=";", on_bad_lines='skip', dtype=str)
                df_ = df_.replace('', pd.NA)
                self.cls_create_log.log_message(
                    self.logger,
                    "Successfully loaded CSV with error handling (skipping bad lines)",
                    "info"
                )
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger,
                    f"Failed to load CSV even with error handling: {str(e)}",
                    "error"
                )
                raise
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully loaded {len(df_)} rows and {len(df_.columns)} columns",
            "info"
        )
        df_["FILE_NAME"] = "cad_intermed.csv"
        
        if "TP_PARTIC" in df_.columns:
            participant_types = df_["TP_PARTIC"].value_counts()
            self.cls_create_log.log_message(
                self.logger,
                f"Participant type distribution: {participant_types.to_dict()}",
                "info"
            )
        
        if "SIT" in df_.columns:
            status_counts = df_["SIT"].value_counts()
            self.cls_create_log.log_message(
                self.logger,
                f"Status distribution: {status_counts.to_dict()}",
                "info"
            )
        
        active_institutions = df_[df_["SIT"] == "EM FUNCIONAMENTO NORMAL"].shape[0] \
            if "SIT" in df_.columns else 0
        cancelled_institutions = df_[df_["SIT"] == "CANCELADA"].shape[0] \
            if "SIT" in df_.columns else 0
        
        self.cls_create_log.log_message(
            self.logger,
            f"Financial intermediaries summary: {active_institutions} active, "
            f"{cancelled_institutions} cancelled",
            "info"
        )
        
        return df_


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
        
        self.url = "https://dados.cvm.gov.br/dados/OFERTA/DISTRIB/DADOS/oferta_distribuicao.zip"
    
    def run(
        self,
        timeout: Optional[
            Union[int, float, tuple[float, float], tuple[int, int]]
        ] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_cvm_distribution_offers"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process for distribution offers data.
        
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
            Target database table name, by default "br_cvm_distribution_offers".

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame if no database session is provided, 
            otherwise None.
        """
        resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
        file, file_name = self.parse_raw_file(resp_req)
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
                "SERIE": str,
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
                bool_insert_or_ignore=bool_insert_or_ignore
            )
        else:
            return df_

    @backoff.on_exception(
        backoff.expo, 
        requests.exceptions.HTTPError, 
        max_time=60
    )
    def get_response(
        self, 
        timeout: Optional[
            Union[int, float, tuple[float, float], tuple[int, int]]
        ] = (12.0, 21.0), 
        bool_verify: bool = True
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Fetch ZIP file containing distribution offers data from CVM website.
        
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
            
        Raises
        ------
        HTTPError
            If the HTTP request fails after all retry attempts.
        """
        self.cls_create_log.log_message(
            self.logger,
            f"Fetching distribution offers data from URL: {self.url}",
            "info"
        )
        
        resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
        resp_req.raise_for_status()
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully fetched {len(resp_req.content)} bytes from CVM",
            "info"
        )
        
        return resp_req
    
    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
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
            "info"
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
                    "info"
                )
                break
        
        if offers_csv_file is None:
            for file_content, filename in files_list:
                if filename.lower().endswith('.csv'):
                    offers_csv_file = file_content
                    offers_filename = filename
                    self.cls_create_log.log_message(
                        self.logger,
                        f"Using alternative distribution offers file: {filename}",
                        "info"
                    )
                    break
        
        if offers_csv_file is None:
            raise ValueError("Distribution offers file oferta_distribuicao.csv not found in "
                             "the downloaded ZIP")
        
        csv_string_io = self._decode_csv_content(offers_csv_file, offers_filename)
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully extracted and decoded distribution offers CSV file: {offers_filename}",
            "info"
        )
        
        return csv_string_io, offers_filename
    
    def _decode_csv_content(
        self, 
        file_content: Union[BytesIO, str, StringIO], 
        filename: str
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
            encodings = ["utf-8", "latin-1", "cp1252"]
            for encoding in encodings:
                try:
                    content_str = file_content.getvalue().decode(encoding)
                    self.cls_create_log.log_message(
                        self.logger,
                        f"Successfully decoded {filename} with {encoding} encoding",
                        "debug"
                    )
                    return StringIO(content_str)
                except UnicodeDecodeError:
                    continue
                
            self.cls_create_log.log_message(
                self.logger,
                f"Using UTF-8 with error replacement for {filename}",
                "warning"
            )
            content_str = file_content.getvalue().decode("utf-8", errors="replace")
            return StringIO(content_str)
        
        return StringIO(str(file_content))
    
    def transform_data(
        self, 
        file: StringIO
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
            
        Raises
        ------
        pd.errors.EmptyDataError
            If the CSV file is empty.
        pd.errors.ParserError
            If the CSV format is invalid.
        """
        self.cls_create_log.log_message(
            self.logger,
            "Transforming distribution offers CSV data into DataFrame",
            "info"
        )
        
        try:
            df_ = pd.read_csv(file, sep=";", dtype=str)
            df_ = df_.replace('', pd.NA)
            df_.columns = [col.upper() for col in df_.columns]
            
        except pd.errors.ParserError as e:
            self.cls_create_log.log_message(
                self.logger,
                f"Parser error encountered: {str(e)}. Trying with error handling.",
                "warning"
            )
            file.seek(0)
            try:
                df_ = pd.read_csv(file, sep=";", on_bad_lines='skip', dtype=str)
                df_ = df_.replace('', pd.NA)
                df_.columns = [col.upper() for col in df_.columns]
                self.cls_create_log.log_message(
                    self.logger,
                    "Successfully loaded CSV with error handling (skipping bad lines)",
                    "info"
                )
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger,
                    f"Failed to load CSV even with error handling: {str(e)}",
                    "error"
                )
                raise
        
        self.cls_create_log.log_message(
            self.logger,
            f"Successfully loaded {len(df_)} rows and {len(df_.columns)} columns",
            "info"
        )
        
        df_["FILE_NAME"] = "oferta_distribuicao.csv"
        
        if "TIPO_OFERTA" in df_.columns:
            offer_types = df_["TIPO_OFERTA"].value_counts()
            self.cls_create_log.log_message(
                self.logger,
                f"Offer type distribution: {offer_types.to_dict()}",
                "info"
            )
        
        if "TIPO_ATIVO" in df_.columns:
            asset_types = df_["TIPO_ATIVO"].value_counts()
            self.cls_create_log.log_message(
                self.logger,
                f"Asset type distribution: {asset_types.to_dict()}",
                "info"
            )
        
        if "MODALIDADE_OFERTA" in df_.columns:
            modality_counts = df_["MODALIDADE_OFERTA"].value_counts()
            self.cls_create_log.log_message(
                self.logger,
                f"Offer modality distribution: {modality_counts.to_dict()}",
                "info"
            )
            
        if "VALOR_TOTAL" in df_.columns:
            total_offer_value = df_["VALOR_TOTAL"].astype(float).sum()
            avg_offer_value = df_["VALOR_TOTAL"].astype(float).mean()
            self.cls_create_log.log_message(
                self.logger,
                f"Financial summary: Total offer value = {total_offer_value:,.2f}, "
                "Average offer value = {avg_offer_value:,.2f}",
                "info"
            )
        
        return df_