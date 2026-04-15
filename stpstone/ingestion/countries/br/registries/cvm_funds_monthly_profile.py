"""CVM Funds Monthly Profile ingestion module."""

from datetime import date, timedelta
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


class CvmFundsMonthlyProfile(ABCIngestionOperations):
	"""CVM Funds Monthly Profile ingestion class.

	Fetches the monthly profile report (perfil_mensal_fi) from the CVM open
	data portal. Each file covers one reference month and contains fund-level
	risk, liquidity, quota-holder, and derivative exposure attributes.
	"""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the CvmFundsMonthlyProfile ingestion class.

		Parameters
		----------
		date_ref : Optional[date]
			The date of reference for the monthly report. When None, defaults
			to the first day of the previous calendar month.
		logger : Optional[Logger]
			The logger, by default None.
		cls_db : Optional[Session]
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

		if date_ref is None:
			curr = self.cls_dates_current.curr_date()
			prev_month_last = curr.replace(day=1) - timedelta(days=1)
			date_ref = prev_month_last.replace(day=1)

		self.date_ref = date_ref
		self.date_ref_yyyymm = self.date_ref.strftime("%Y%m")
		self.url = (
			"https://dados.cvm.gov.br/dados/FI/DOC/PERFIL_MENSAL/DADOS/"
			f"perfil_mensal_fi_{self.date_ref_yyyymm}.csv"
		)

	def run(
		self,
		bool_verify: bool = True,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_cvm_funds_monthly_profile",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the
		database. Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		bool_verify : bool
			Whether to verify the SSL certificate, by default True.
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0).
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False.
		str_table_name : str
			The name of the table, by default 'br_cvm_funds_monthly_profile'.

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
		file = self.parse_raw_file(resp_req=resp_req)
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
				"DT_COTA_TAXA_PERFM": "date",
				"VL_COTA_TAXA_PERFM": float,
				"VL_DIREITO_DISTRIB": float,
				"NR_COTST_ENTID_PREVID_COMPL": int,
				"PR_COTST_ENTID_PREVID_COMPL": float,
				"PR_PATRIM_LIQ_MAIOR_COTST": float,
				"NR_DIA_CINQU_PERC": int,
				"NR_DIA_CEM_PERC": int,
				"ST_LIQDEZ": str,
				"PR_PATRIM_LIQ_CONVTD_CAIXA": float,
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

	@backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_time=60)
	def get_response(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Return a response object for the CVM monthly profile CSV.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Verify the SSL certificate, by default True.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
			A response object.
		"""
		resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
		resp_req.raise_for_status()
		return resp_req

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
	) -> StringIO:
		"""Parse the raw response into a StringIO buffer.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The response object.

		Returns
		-------
		StringIO
			The parsed content.
		"""
		return self.get_file(resp_req=resp_req)

	def transform_data(self, file: StringIO) -> pd.DataFrame:
		"""Transform the raw CSV buffer into a DataFrame.

		Parameters
		----------
		file : StringIO
			The parsed content.

		Returns
		-------
		pd.DataFrame
			The transformed DataFrame.
		"""
		try:
			return pd.read_csv(file, sep=";", encoding="latin-1")
		except pd.errors.EmptyDataError:
			return pd.DataFrame()
