"""Implementation of ingestion instance."""

from datetime import date
from logging import Logger
from typing import Optional, Union

import backoff
from lxml.html import HtmlElement
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
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.str import StrHandler


class B3OptionsSettlementCalendar(ABCIngestionOperations):
	"""Ingestion concrete class."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the ingestion class.

		Parameters
		----------
		date_ref : Optional[date]
		    The date of reference, by default None.
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
		self.cls_html_handler = HtmlHandler()
		self.cls_handling_dicts = HandlingDicts()
		self.cls_handling_str = StrHandler()
		self.date_ref = date_ref or self.cls_dates_br.add_working_days(
			self.cls_dates_current.curr_date(), -1
		)
		self.url = "https://www.b3.com.br/pt_br/solucoes/plataformas/puma-trading-system/para-participantes-e-traders/calendario-de-negociacao/vencimentos/calendario-de-vencimentos-de-opcoes-sobre-acoes-e-indices/"

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_b3_options_settlement_calendar",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		    The timeout, by default (12.0, 21.0)
		bool_verify : bool
		    Whether to verify the SSL certificate, by default True
		bool_insert_or_ignore : bool
		    Whether to insert or ignore the data, by default False
		str_table_name : str
		    The name of the table, by default "br_b3_options_settlement_calendar"

		Returns
		-------
		Optional[pd.DataFrame]
		    The transformed DataFrame.
		"""
		resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
		html_root = self.parse_raw_file(resp_req)
		df_ = self.transform_data(html_root=html_root)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"DIA": str,
				"DETALHE": str,
				"ANO_CALENDARIO": str,
				"MES": str,
				"DATA_VENCIMENTO": "date",
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
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Return a list of response objects.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		    The timeout, by default (12.0, 21.0)
		bool_verify : bool
		    Verify the SSL certificate, by default True

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
		    A list of response objects.
		"""
		resp_req = requests.get(
			self.url,
			timeout=timeout,
			verify=bool_verify,
			headers={
				"accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",  # noqa E501: line too long
				"accept-language": "en-US,en;q=0.9,pt;q=0.8,es;q=0.7",
				"cache-control": "max-age=0",
				"if-modified-since": "Mon, 07 Apr 2025 18:37:09 GMT",
				"priority": "u=0, i",
				"sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
				"sec-ch-ua-mobile": "?0",
				"sec-ch-ua-platform": '"Linux"',
				"sec-fetch-dest": "document",
				"sec-fetch-mode": "navigate",
				"sec-fetch-site": "none",
				"sec-fetch-user": "?1",
				"upgrade-insecure-requests": "1",
				"user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",  # noqa E501: line too long
				"Cookie": "_ga=GA1.1.246440908.1756239909; lumUserLocale=pt_BR; dtCookie=v_4_srv_28_sn_27042D340E548082776D7334977260FE_perc_100000_ol_0_mul_1_app-3Afd69ce40c52bd20e_0; lumClientId=8AA8D0CC9851DC3001992B49146429DD; F051234a800=!dvR2HtrLPkg28TnjrAp71Xe4MLiPRlXdXn9epWh+3CV2kvmGd0C19+zc+64n2hJ/PjEhorX2N8GqpaM=; OptanonAlertBoxClosed=2025-09-08T21:47:45.364Z; TS01f22489=011d592ce16c3d84b5ded4d1f6b0dec3bb45f17e995a7830b3781317e15dd664125dcf21d367cdc00ace7bb52856dd0c5c7d25bb98; TS0171d45d=011d592ce15eb77f771ffd730e246529692c3c73a4506b2be356131d8e55070e445caff78176b7d28a0f890bd08c656a61236d6786; cf_clearance=1Kg49wgSisbUN_cBq5bMVVZfSuirx92iRt7FGkY8poA-1757413266-1.2.1.1-R7vW0TrBjIzfWvMZt9VkFPlx9k5S_LG.vf.FbrRPbiUu2cv2ELotyBxhYuD6amD.ygVQmfz4edFE4y3Da6ORAgXcM261tA1vLgI0cdElyHZHI.4uPaeizKM10iGoeo7KuSf2YjnIDkAAo8jGn6Svs7Yn7OCzfzXlpdM_RsjoF2KVBgkUB5AfryLTKlbs_F4.dC6Z7UXV_0HFqROu5EEaPEX_gfSBHRvdQUHSnhXxOZo; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Sep+09+2025+07%3A21%3A07+GMT-0300+(Hor%C3%A1rio+Padr%C3%A3o+de+Bras%C3%ADlia)&version=6.21.0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0003%3A0%2CC0001%3A1%2CC0004%3A0%2CC0002%3A0&AwaitingReconsent=false&geolocation=%3B; _clck=e74245%5E2%5Efz6%5E0%5E2064; _ga_SS7FXRTPP3=GS2.1.s1757413267$o3$g0$t1757413273$j54$l0$h0; JSESSIONID=67F1F6E2AF706A34B5565201DEBAB5FC.lumcor00201b; lumUserSessionId=viEpwd-YjGtJsXvOPe81o69Q-oMUTd95; lumUserName=Guest; lumIsLoggedUser=false; _clsk=lpepvk%5E1757415714095%5E3%5E1%5Es.clarity.ms%2Fcollect",  # noqa E501: line too long
			},
		)
		resp_req.raise_for_status()
		return resp_req

	def parse_raw_file(
		self, resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
	) -> HtmlElement:
		"""Parse the raw file content.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
		    The response object.

		Returns
		-------
		HtmlElement
		    The parsed content.
		"""
		return self.cls_html_handler.lxml_parser(resp_req=resp_req)

	def transform_data(self, html_root: HtmlElement) -> pd.DataFrame:
		"""Transform a list of response objects into a DataFrame.

		Parameters
		----------
		html_root : HtmlElement
		    The parsed content.

		Returns
		-------
		pd.DataFrame
		    The transformed DataFrame.
		"""
		str_xpath_calendar_year: str = '//*[@id="conteudo-principal"]/div[4]/div/div/h2'
		str_xpath_td: str = '//*[contains(@id, "panel")]/table/tbody/tr/td'

		list_td = [
			el.text
			for el in self.cls_html_handler.lxml_xpath(
				html_content=html_root, str_xpath=str_xpath_td
			)
		]
		str_calendar_year = self.cls_html_handler.lxml_xpath(
			html_content=html_root, str_xpath=str_xpath_calendar_year
		)[0].text

		list_td_months = self._months_options_settlement()

		list_ser = self.cls_handling_dicts.pair_headers_with_data(
			list_headers=["DIA", "DETALHE"], list_data=list_td
		)
		list_ser = self.cls_handling_dicts.add_key_value_to_dicts(
			list_ser=list_ser, key="ANO_CALENDARIO", value=str_calendar_year
		)

		df_ = pd.DataFrame(list_ser)
		df_["MES"] = list_td_months
		df_["DATA_VENCIMENTO"] = [
			self.cls_dates_br.build_date(
				int(row["ANO_CALENDARIO"].split(" ")[-1]), row["MES"], int(row["DIA"])
			)
			for _, row in df_.iterrows()
		]
		df_ = self._fill_missing_values_td(df_)

		return df_

	def _months_options_settlement(self) -> list[int]:
		"""Return the list of months for options settlement.

		Returns
		-------
		list[int]
		    The list of months.

		Notes
		-----
		[1] The number of month name appearance is the number of dates with options settlements
		[2] Two indicate maturity dates for options with the reference price in IBOV and stocks
		[3] Three indicate maturity dates for options with the reference price in IBOV, stocks and
		IBRX50
		"""
		list_months_raw = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
		list_months_trt: list[str] = []
		for i in range(len(list_months_raw)):
			list_months_trt.extend([list_months_raw[i]] * 2)
			if i % 2 == 1:
				list_months_trt.append(list_months_raw[i])

		return list_months_trt

	def _fill_missing_values_td(self, df_: pd.DataFrame) -> list[str]:
		"""Fill missing values in a list of strings.

		Parameters
		----------
		df_ : pd.DataFrame
		    The DataFrame to fill missing values in.

		Returns
		-------
		list[str]
		    The list of strings with missing values filled.
		"""
		for index, row in df_.iterrows():
			if row["DETALHE"] is None:
				if int(row["MES"]) % 2 == 1:
					df_.loc[index, "DETALHE"] = df_.loc[index - 3, "DETALHE"]
				else:
					index_ = index - 4 if index > 4 else index - 2
					df_.loc[index, "DETALHE"] = df_.loc[index_, "DETALHE"]
			if index + 1 < df_.shape[0] and index > 0:
				if (
					df_.loc[index, "DETALHE"] == df_.loc[index + 1, "DETALHE"]
					and df_.loc[index, "MES"] == df_.loc[index + 1, "MES"]
					and int(row["MES"]) % 2 == 0
				):
					df_.loc[index, "DETALHE"] = df_.loc[index - 2, "DETALHE"]
				elif (
					df_.loc[index, "DETALHE"] == df_.loc[index - 1, "DETALHE"]
					and df_.loc[index, "MES"] == df_.loc[index - 1, "MES"]
					and int(row["MES"]) % 2 == 1
				):
					df_.loc[index, "DETALHE"] = df_.loc[index - 2, "DETALHE"]  # noqa SIM114: combine `if` branches using logical `or` operator

		return df_
