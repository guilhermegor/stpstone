"""Mais Retorno Fund Properties Detail Page."""

from datetime import date
from logging import Logger
from typing import Optional, Union

import pandas as pd
from requests import Session

from stpstone.ingestion.abc.ingestion_abc import (
	ABCIngestionOperations,
	ContentParser,
	CoreIngestion,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.colors import ColorIdentifier
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


class MaisRetornoFundProperties(ABCIngestionOperations):
	"""Mais Retorno individual fund detail properties."""

	_BASE_URL = "https://maisretorno.com/fundo/{}"
	_XPATH_H1_FUND_NICKNAME = (
		"//h1[@class=\"MuiTypography-root MuiTypography-h1 css-mqbfcn\"]"
	)
	_XPATH_P_FUND_FULL_NAME = (
		"//p[@class=\"MuiTypography-root MuiTypography-body1 css-fuk7hj\" and @id=\"fund-name\"]"
	)
	_XPATH_SPAN_STATUS = "//li[@data-testid=\"status\"]/span"
	_XPATH_SVG_POOL_OPEN = "//*[@data-testid=\"open-condominium\"]//*[name()=\"svg\"]"
	_XPATH_SVG_QUALIFIED_INVESTOR = (
		"//li[@data-testid=\"qualified-investor\"]//*[name()=\"svg\"]"
	)
	_XPATH_SVG_EXCLUSIVE_FUND = "//li[@data-testid=\"exclusive\"]//*[name()=\"svg\"]"
	_XPATH_SVG_LONG_TERM_TAXATION = (
		"//li[@data-testid=\"long_term_tributation\"]//*[name()=\"svg\"]"
	)
	_XPATH_SVG_PENSION_FUND = "//li[@data-testid=\"pension_fund\"]//*[name()=\"svg\"]"
	_XPATH_P_CNPJ_FUND = "//p[@data-testid=\"fund-cnpj\"]"
	_XPATH_P_BENCHMARK = "//p[@data-testid=\"fund-benhcmark\"]"
	_XPATH_P_FUND_INITIAL_DATE = "//p[@data-testid=\"fund-initial-date\"]"
	_XPATH_P_FUND_TYPE = "//p[@data-testid=\"fund-type\"]"
	_XPATH_A_FUND_ADMINISTRATOR = (
		"//li[@class=\"MuiGrid-root MuiGrid-item MuiGrid-grid-xs-12 MuiGrid-grid-sm-6"
		" MuiGrid-grid-md-4 css-1twzmnh\"]"
		"/*[self::a or self::p][contains(@class, \"MuiTypography-root MuiTypography-body1\")]"
	)
	_XPATH_A_FUND_CLASS = (
		"//li[@class=\"MuiGrid-root MuiGrid-item MuiGrid-grid-xs-12 MuiGrid-grid-sm-6"
		" MuiGrid-grid-md-2 css-wubtha\"]"
		"/p[@class=\"MuiTypography-root MuiTypography-body1 css-fuk7hj\""
		" and text()=\"Classe\"]"
		"/following-sibling::p[@class=\"MuiTypography-root MuiTypography-body1 css-fuk7hj\"]"
		" | //li[@class=\"MuiGrid-root MuiGrid-item MuiGrid-grid-xs-12 MuiGrid-grid-sm-6"
		" MuiGrid-grid-md-2 css-wubtha\"]/p[text()=\"Classe\"]"
		"/following-sibling::*[self::a["
		"@class=\"MuiTypography-root MuiTypography-body1 css-13xqfhh\"]"
		" or self::p[class=\"MuiTypography-root MuiTypography-body1 css-fuk7hj\"]]"
	)
	_XPATH_A_FUND_SUBCLASS = (
		"//li[@class=\"MuiGrid-root MuiGrid-item MuiGrid-grid-xs-12 MuiGrid-grid-sm-6"
		" MuiGrid-grid-md-2 css-wubtha\"]"
		"/p[@class=\"MuiTypography-root MuiTypography-body1 css-fuk7hj\""
		" and text()=\"Subclasse\"]"
		"/following-sibling::*[self::p["
		"@class=\"MuiTypography-root MuiTypography-body1 css-fuk7hj\"]"
		" or self::a[@class=\"MuiTypography-root MuiTypography-body1 css-13xqfhh\"]]"
	)
	_XPATH_A_FUND_MANAGER = "//div[@class=\"MuiStack-root css-yhwlx1\"]/a"
	_XPATH_P_RENTABILITY_LTM = (
		"//div[@class=\"MuiBox-root css-139zab0\"]"
		"/h3[@class=\"MuiTypography-root MuiTypography-body1 css-1wethdf\""
		" and contains(text(), \"Rentabilidade\")]"
		"/following-sibling::p[contains(@class, \"MuiTypography-root MuiTypography-h4\")]"
	)
	_XPATH_P_AUM = (
		"//div[@class=\"MuiGrid-root MuiGrid-item MuiGrid-grid-xs-6 MuiGrid-grid-sm-6"
		" MuiGrid-grid-lg-2 css-1s8n1jb\"]"
		"//h3[@class=\"MuiTypography-root MuiTypography-body1 css-1wethdf\""
		" and contains(text(), \"Patrim\u00f4nio L\u00edquido\")]"
		"/following-sibling::p[@class=\"MuiTypography-root MuiTypography-h4 css-wu84th\"]"
	)
	_XPATH_P_AVERAGE_AUM_LTM = (
		"//div[@class=\"MuiGrid-root MuiGrid-item MuiGrid-grid-xs-6 MuiGrid-grid-sm-6"
		" MuiGrid-grid-lg-2 css-1s8n1jb\"]"
		"//h3[@class=\"MuiTypography-root MuiTypography-body1 css-1wethdf\""
		" and contains(text(), \"PL M\u00e9dio 12M\")]"
		"/following-sibling::p[@class=\"MuiTypography-root MuiTypography-h4 css-wu84th\"]"
	)
	_XPATH_P_VOLATILITY_LTM = (
		"//div[@class=\"MuiGrid-root MuiGrid-item MuiGrid-grid-xs-6 MuiGrid-grid-sm-6"
		" MuiGrid-grid-lg-2 css-1s8n1jb\"]"
		"//h3[@class=\"MuiTypography-root MuiTypography-body1 css-1wethdf\""
		" and contains(text(), \"Volatilidade 12M\")]"
		"/following-sibling::p[@class=\"MuiTypography-root MuiTypography-h4 css-wu84th\"]"
	)
	_XPATH_P_SHARPE_LTM = (
		"//div[@class=\"MuiGrid-root MuiGrid-item MuiGrid-grid-xs-6 MuiGrid-grid-sm-6"
		" MuiGrid-grid-lg-2 css-1s8n1jb\"]"
		"//h3[@class=\"MuiTypography-root MuiTypography-body1 css-1wethdf\""
		" and contains(text(), \"\u00cdndice de Sharpe 12M\")]"
		"/following-sibling::p[contains(@class, \"MuiTypography-root MuiTypography-h4\")]"
	)
	_XPATH_P_QTY_UNITHOLDERS = (
		"//div[@class=\"MuiGrid-root MuiGrid-item MuiGrid-grid-xs-6 MuiGrid-grid-sm-6"
		" MuiGrid-grid-lg-2 css-1s8n1jb\"]"
		"//h3[@class=\"MuiTypography-root MuiTypography-body1 css-1wethdf\""
		" and contains(text(), \"Cotistas\")]"
		"/following-sibling::p[@class=\"MuiTypography-root MuiTypography-h4 css-wu84th\"]"
	)
	_XPATH_H3_MANAGER_FULL_NAME = (
		"//h3[@class=\"MuiTypography-root MuiTypography-h5 css-1cqpmia\"]"
	)
	_XPATH_P_FUND_MANAGER_DIRECTOR = (
		"(//div[@class=\"MuiGrid-root MuiGrid-container MuiGrid-spacing-xs-2 css-b965al\"]"
		"//p[@class=\"MuiTypography-root MuiTypography-body2 css-oc8vpl\"])[1]"
	)
	_XPATH_P_FUND_MANAGER_EMAIL = (
		"(//div[@class=\"MuiGrid-root MuiGrid-container MuiGrid-spacing-xs-2 css-b965al\"]"
		"//p[@class=\"MuiTypography-root MuiTypography-body2 css-oc8vpl\"])[2]"
	)
	_XPATH_P_FUND_MANAGER_SITE = (
		"(//div[@class=\"MuiGrid-root MuiGrid-container MuiGrid-spacing-xs-2 css-b965al\"]"
		"//p[@class=\"MuiTypography-root MuiTypography-body2 css-oc8vpl\"])[3]"
	)
	_XPATH_P_FUND_MANAGER_TELEPHONE = (
		"(//div[@class=\"MuiGrid-root MuiGrid-container MuiGrid-spacing-xs-2 css-b965al\"]"
		"//p[@class=\"MuiTypography-root MuiTypography-body2 css-oc8vpl\"])[4]"
	)
	_XPATH_P_MINIMUM_INVESTMENT_AMOUNT = (
		"(//div[@class=\"MuiGrid-root MuiGrid-item MuiGrid-grid-xs-6 MuiGrid-grid-sm-4"
		" MuiGrid-grid-lg-2 css-d5w72u\"]"
		"//p[@class=\"MuiTypography-root MuiTypography-body2 css-11lk3u8\"])[1]"
	)
	_XPATH_P_MINIMUM_BALANCE_REQUIRED = (
		"(//div[@class=\"MuiGrid-root MuiGrid-item MuiGrid-grid-xs-6 MuiGrid-grid-sm-4"
		" MuiGrid-grid-lg-2 css-d5w72u\"]"
		"//p[@class=\"MuiTypography-root MuiTypography-body2 css-11lk3u8\"])[2]"
	)
	_XPATH_P_ADMINISTRATION_FEE = (
		"(//div[@class=\"MuiGrid-root MuiGrid-item MuiGrid-grid-xs-6 MuiGrid-grid-sm-4"
		" MuiGrid-grid-lg-2 css-d5w72u\"]"
		"//p[@class=\"MuiTypography-root MuiTypography-body2 css-11lk3u8\"])[3]"
	)
	_XPATH_P_ADMINISTRATION_FEE_MAX = (
		"(//div[@class=\"MuiGrid-root MuiGrid-item MuiGrid-grid-xs-6 MuiGrid-grid-sm-4"
		" MuiGrid-grid-lg-2 css-d5w72u\"]"
		"//p[@class=\"MuiTypography-root MuiTypography-body2 css-11lk3u8\"])[4]"
	)
	_XPATH_P_PERFORMANCE_FEE = (
		"(//div[@class=\"MuiGrid-root MuiGrid-item MuiGrid-grid-xs-6 MuiGrid-grid-sm-4"
		" MuiGrid-grid-lg-2 css-d5w72u\"]"
		"//p[@class=\"MuiTypography-root MuiTypography-body2 css-11lk3u8\"])[5]"
	)
	_XPATH_P_FUND_QUOTATION_PERIOD = (
		"//div[@class=\"MuiGrid-root MuiGrid-item MuiGrid-grid-xs-12 MuiGrid-grid-sm-4"
		" MuiGrid-grid-lg-2 css-1e35mkm\"]"
		"//p[@class=\"MuiTypography-root MuiTypography-body2 css-oc8vpl\""
		" and contains(text(), \"Prazo de cotiza\")]"
		"/following-sibling::p[@class=\"MuiTypography-root MuiTypography-body2 css-11lk3u8\"]"
	)
	_XPATH_P_FUND_SETTLEMENT_PERIOD = (
		"//div[@class=\"MuiGrid-root MuiGrid-item MuiGrid-grid-xs-12 MuiGrid-grid-sm-4"
		" MuiGrid-grid-lg-2 css-1e35mkm\"]"
		"//p[@class=\"MuiTypography-root MuiTypography-body2 css-oc8vpl\""
		" and contains(text(), \"Liqui\")]"
		"/following-sibling::p[@class=\"MuiTypography-root MuiTypography-body2 css-11lk3u8\"]"
	)

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
		list_slugs: Optional[list] = None,
		bool_headless: bool = True,
		int_wait_load_seconds: int = 60,
	) -> None:
		"""Initialize the Mais Retorno Fund Properties ingestion class.

		Parameters
		----------
		date_ref : Optional[date], optional
			The date of reference, by default None.
		logger : Optional[Logger], optional
			The logger, by default None.
		cls_db : Optional[Session], optional
			The database session, by default None.
		list_slugs : Optional[list], optional
			List of fund slug identifiers to fetch, by default None.
		bool_headless : bool, optional
			Whether to run the browser in headless mode, by default True.
		int_wait_load_seconds : int, optional
			Seconds to wait for page elements to load, by default 60.

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
			self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
		self.list_slugs = list_slugs or ["aasl-fia"]
		self.bool_headless = bool_headless
		self.int_wait_load_seconds = int_wait_load_seconds
		self.url = self._BASE_URL

	def run(
		self,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_mais_retorno_funds_properties",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		bool_insert_or_ignore : bool, optional
			Whether to insert or ignore the data, by default False.
		str_table_name : str, optional
			The name of the table, by default 'br_mais_retorno_funds_properties'.

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		scraper_playwright = self.parse_raw_file()
		df_ = self.transform_data(scraper_playwright=scraper_playwright)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"NICKNAME": str,
				"FUND_NAME": str,
				"STATUS": str,
				"BL_POOL_OPEN": bool,
				"BL_QUALIFIED_INVESTOR": bool,
				"BL_EXCLUSIVE_FUND": bool,
				"BL_LONG_TERM_TAXATION": bool,
				"BL_PENSION_FUND": bool,
				"CNPJ": str,
				"BENCHMARK": str,
				"FUND_INITIAL_DATE": "date",
				"FUND_TYPE": str,
				"ADMINISTRATOR": str,
				"CLASS": str,
				"SUBCLASS": str,
				"MANAGER": str,
				"RENTABILITY_LTM": float,
				"AUM": str,
				"AVERAGE_AUM_LTM": str,
				"VOLATILITY_LTM": float,
				"SHARPE_LTM": float,
				"QTY_UNITHOLDERS": str,
				"MANAGER_FULL_NAME": str,
				"MANAGER_DIRECTOR": str,
				"MANAGER_EMAIL": str,
				"MANAGER_TELEPHONE": str,
				"MINIMUM_INVESTMENT_AMOUNT": float,
				"MINIMUM_BALANCE_REQUIRED": float,
				"ADMINISTRATION_FEE": float,
				"ADMINISTRATION_FEE_MAX": float,
				"PERFORMANCE_FEE": float,
				"FUND_QUOTATION_PERIOD": str,
				"FUND_SETTLEMENT_PERIOD": str,
			},
			str_fmt_dt="DD/MM/YYYY",
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

	def get_response(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
	) -> None:
		"""Return None — Playwright does not use HTTP responses.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
			The timeout, by default (12.0, 21.0).
		bool_verify : bool, optional
			Verify the SSL certificate, by default True.

		Returns
		-------
		None
		"""
		pass

	def parse_raw_file(self) -> PlaywrightScraper:
		"""Return a PlaywrightScraper instance for browser automation.

		Returns
		-------
		PlaywrightScraper
			Configured PlaywrightScraper instance.
		"""
		return PlaywrightScraper(
			bool_headless=self.bool_headless,
			int_default_timeout=self.int_wait_load_seconds * 1_000,
			logger=self.logger,
		)

	def transform_data(self, scraper_playwright: PlaywrightScraper) -> pd.DataFrame:
		"""Scrape fund properties from each slug's detail page.

		Parameters
		----------
		scraper_playwright : PlaywrightScraper
			Configured Playwright scraper instance.

		Returns
		-------
		pd.DataFrame
			DataFrame with fund properties for all slugs.
		"""
		list_ser = []
		with scraper_playwright.launch():
			for slug in self.list_slugs:
				page_url = self._BASE_URL.format(slug)
				if not scraper_playwright.navigate(page_url):
					self.cls_create_log.log_message(
						self.logger,
						f"Failed to navigate to fund page '{slug}': {page_url}",
						"warning",
					)
					continue
				list_ser.extend(self._extract_fund_properties(scraper_playwright))
		return pd.DataFrame(list_ser)

	def _extract_fund_properties(self, scraper: PlaywrightScraper) -> list:
		"""Extract all properties from the currently loaded fund detail page.

		Parameters
		----------
		scraper : PlaywrightScraper
			Active PlaywrightScraper instance positioned on the fund detail page.

		Returns
		-------
		list
			Single-element list with a dict of all fund property fields.
		"""
		h1_fund_nickname = scraper.get_element(
			self._XPATH_H1_FUND_NICKNAME, selector_type="xpath"
		)
		p_fund_full_name = scraper.get_element(
			self._XPATH_P_FUND_FULL_NAME, selector_type="xpath"
		)
		span_status = scraper.get_element(
			self._XPATH_SPAN_STATUS, selector_type="xpath"
		)
		hex_pool_open = scraper.get_element_attrb(
			self._XPATH_SVG_POOL_OPEN, str_attribute="color", selector_type="xpath"
		)
		hex_qualified_investor = scraper.get_element_attrb(
			self._XPATH_SVG_QUALIFIED_INVESTOR,
			str_attribute="color",
			selector_type="xpath",
		)
		hex_exclusive_fund = scraper.get_element_attrb(
			self._XPATH_SVG_EXCLUSIVE_FUND, str_attribute="color", selector_type="xpath"
		)
		hex_long_term_taxation = scraper.get_element_attrb(
			self._XPATH_SVG_LONG_TERM_TAXATION,
			str_attribute="color",
			selector_type="xpath",
		)
		hex_pension_fund = scraper.get_element_attrb(
			self._XPATH_SVG_PENSION_FUND, str_attribute="color", selector_type="xpath"
		)
		p_cnpj_fund = scraper.get_element(
			self._XPATH_P_CNPJ_FUND, selector_type="xpath"
		)
		p_benchmark = scraper.get_element(
			self._XPATH_P_BENCHMARK, selector_type="xpath"
		)
		p_fund_initial_date = scraper.get_element(
			self._XPATH_P_FUND_INITIAL_DATE, selector_type="xpath"
		)
		p_fund_type = scraper.get_element(
			self._XPATH_P_FUND_TYPE, selector_type="xpath"
		)
		a_fund_administrator = scraper.get_element(
			self._XPATH_A_FUND_ADMINISTRATOR, selector_type="xpath"
		)
		a_fund_class = scraper.get_element(
			self._XPATH_A_FUND_CLASS, selector_type="xpath"
		)
		a_fund_subclass = scraper.get_element(
			self._XPATH_A_FUND_SUBCLASS, selector_type="xpath"
		)
		a_fund_manager = scraper.get_element(
			self._XPATH_A_FUND_MANAGER, selector_type="xpath"
		)
		p_rentability_ltm = scraper.get_element(
			self._XPATH_P_RENTABILITY_LTM, selector_type="xpath"
		)
		p_aum = scraper.get_element(self._XPATH_P_AUM, selector_type="xpath")
		p_average_aum_ltm = scraper.get_element(
			self._XPATH_P_AVERAGE_AUM_LTM, selector_type="xpath"
		)
		p_volatility_ltm = scraper.get_element(
			self._XPATH_P_VOLATILITY_LTM, selector_type="xpath"
		)
		p_sharpe_ltm = scraper.get_element(
			self._XPATH_P_SHARPE_LTM, selector_type="xpath"
		)
		p_qty_unitholders = scraper.get_element(
			self._XPATH_P_QTY_UNITHOLDERS, selector_type="xpath"
		)
		h3_manager_full_name = scraper.get_element(
			self._XPATH_H3_MANAGER_FULL_NAME, selector_type="xpath"
		)
		p_fund_manager_director = scraper.get_element(
			self._XPATH_P_FUND_MANAGER_DIRECTOR, selector_type="xpath"
		)
		p_fund_manager_email = scraper.get_element(
			self._XPATH_P_FUND_MANAGER_EMAIL, selector_type="xpath"
		)
		p_fund_manager_site = scraper.get_element(
			self._XPATH_P_FUND_MANAGER_SITE, selector_type="xpath"
		)
		p_fund_manager_telephone = scraper.get_element(
			self._XPATH_P_FUND_MANAGER_TELEPHONE, selector_type="xpath"
		)
		p_minimum_investment_amount = scraper.get_element(
			self._XPATH_P_MINIMUM_INVESTMENT_AMOUNT, selector_type="xpath"
		)
		p_minimum_balance_required = scraper.get_element(
			self._XPATH_P_MINIMUM_BALANCE_REQUIRED, selector_type="xpath"
		)
		p_administration_fee = scraper.get_element(
			self._XPATH_P_ADMINISTRATION_FEE, selector_type="xpath"
		)
		p_administration_fee_max = scraper.get_element(
			self._XPATH_P_ADMINISTRATION_FEE_MAX, selector_type="xpath"
		)
		p_performance_fee = scraper.get_element(
			self._XPATH_P_PERFORMANCE_FEE, selector_type="xpath"
		)
		p_fund_quotation_period = scraper.get_element(
			self._XPATH_P_FUND_QUOTATION_PERIOD, selector_type="xpath"
		)
		p_fund_settlement_period = scraper.get_element(
			self._XPATH_P_FUND_SETTLEMENT_PERIOD, selector_type="xpath"
		)

		rentability_ltm_text = (
			p_rentability_ltm.get("text", None) if p_rentability_ltm else None
		)
		volatility_ltm_text = (
			p_volatility_ltm.get("text", None) if p_volatility_ltm else None
		)
		sharpe_ltm_text = p_sharpe_ltm.get("text", None) if p_sharpe_ltm else None
		min_inv_text = (
			p_minimum_investment_amount.get("text", None)
			if p_minimum_investment_amount
			else None
		)
		min_bal_text = (
			p_minimum_balance_required.get("text", None)
			if p_minimum_balance_required
			else None
		)
		admin_fee_text = (
			p_administration_fee.get("text", None) if p_administration_fee else None
		)
		admin_fee_max_text = (
			p_administration_fee_max.get("text", None)
			if p_administration_fee_max
			else None
		)
		perf_fee_text = (
			p_performance_fee.get("text", None) if p_performance_fee else None
		)
		fund_initial_date_text = (
			p_fund_initial_date.get("text", None) if p_fund_initial_date else None
		)

		return [
			{
				"NICKNAME": h1_fund_nickname.get("text", None)
				if h1_fund_nickname
				else None,
				"FUND_NAME": p_fund_full_name.get("text", None)
				if p_fund_full_name
				else None,
				"STATUS": span_status.get("text", None) if span_status else None,
				"BL_POOL_OPEN": ColorIdentifier(hex_pool_open).bool_green()
				if hex_pool_open is not None
				else False,
				"BL_QUALIFIED_INVESTOR": ColorIdentifier(hex_qualified_investor).bool_green()
				if hex_qualified_investor is not None
				else False,
				"BL_EXCLUSIVE_FUND": ColorIdentifier(hex_exclusive_fund).bool_green()
				if hex_exclusive_fund is not None
				else False,
				"BL_LONG_TERM_TAXATION": ColorIdentifier(hex_long_term_taxation).bool_green()
				if hex_long_term_taxation is not None
				else False,
				"BL_PENSION_FUND": ColorIdentifier(hex_pension_fund).bool_green()
				if hex_pension_fund is not None
				else False,
				"CNPJ": p_cnpj_fund.get("text", None) if p_cnpj_fund else None,
				"BENCHMARK": p_benchmark.get("text", None) if p_benchmark else None,
				"FUND_INITIAL_DATE": DatesBRAnbima().str_date_to_datetime(
					fund_initial_date_text, "DD/MM/YYYY"
				),
				"FUND_TYPE": p_fund_type.get("text", None) if p_fund_type else None,
				"ADMINISTRATOR": a_fund_administrator.get("text", None)
				if a_fund_administrator
				else None,
				"CLASS": a_fund_class.get("text", None) if a_fund_class else None,
				"SUBCLASS": a_fund_subclass.get("text", None) if a_fund_subclass else None,
				"MANAGER": a_fund_manager.get("text", None) if a_fund_manager else None,
				"RENTABILITY_LTM": float(
					rentability_ltm_text.replace("%", "").replace(",", ".")
				)
				/ 100.0
				if rentability_ltm_text is not None and rentability_ltm_text != "-"
				else None,
				"AUM": p_aum.get("text", None) if p_aum else None,
				"AVERAGE_AUM_LTM": p_average_aum_ltm.get("text", None)
				if p_average_aum_ltm
				else None,
				"VOLATILITY_LTM": float(
					volatility_ltm_text.replace("%", "").replace(",", ".")
				)
				/ 100.0
				if volatility_ltm_text is not None and volatility_ltm_text != "-"
				else None,
				"SHARPE_LTM": float(sharpe_ltm_text.replace(",", "."))
				if sharpe_ltm_text is not None and sharpe_ltm_text != "-"
				else None,
				"QTY_UNITHOLDERS": p_qty_unitholders.get("text", None)
				if p_qty_unitholders
				else None,
				"MANAGER_FULL_NAME": h3_manager_full_name.get("text", None)
				if h3_manager_full_name
				else None,
				"MANAGER_DIRECTOR": p_fund_manager_director.get("text", None)
				if p_fund_manager_director
				else None,
				"MANAGER_EMAIL": p_fund_manager_email.get("text", None)
				if p_fund_manager_email
				else None,
				"MANAGER_SITE": p_fund_manager_site.get("text", None)
				if p_fund_manager_site
				else None,
				"MANAGER_TELEPHONE": p_fund_manager_telephone.get("text", None)
				if p_fund_manager_telephone
				else None,
				"MINIMUM_INVESTMENT_AMOUNT": float(
					min_inv_text.replace(".", "").replace("R$ ", "")
				)
				if min_inv_text is not None
				and min_inv_text != "-"
				and len(min_inv_text) > 1
				else None,
				"MINIMUM_BALANCE_REQUIRED": float(
					min_bal_text.replace(".", "").replace("R$ ", "")
				)
				if min_bal_text is not None
				and min_bal_text != "-"
				and len(min_bal_text) > 1
				else None,
				"ADMINISTRATION_FEE": float(
					admin_fee_text.replace("%", "").replace(",", ".")
				)
				/ 100.0
				if admin_fee_text is not None and admin_fee_text != "-"
				else None,
				"ADMINISTRATION_FEE_MAX": float(
					admin_fee_max_text.replace("%", "").replace(",", ".")
				)
				/ 100.0
				if admin_fee_max_text is not None and admin_fee_text != "-"
				else None,
				"PERFORMANCE_FEE": float(perf_fee_text.replace("%", "").replace(",", "."))
				/ 100.0
				if perf_fee_text is not None and perf_fee_text != "-"
				else None,
				"FUND_QUOTATION_PERIOD": p_fund_quotation_period.get("text", None)
				if p_fund_quotation_period is not None
				and p_fund_quotation_period != "-"
				else None,
				"FUND_SETTLEMENT_PERIOD": p_fund_settlement_period.get("text", None)
				if p_fund_settlement_period is not None
				and p_fund_settlement_period != "-"
				else None,
			}
		]
