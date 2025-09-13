"""Implementation of ingestion instance."""

from datetime import date
from io import StringIO
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


class B3TradingHoursCore(ABCIngestionOperations):
    """B3 Trading Hours."""
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        url: str = "FILL_ME",
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
        self.cls_html_handler = HtmlHandler()
        self.cls_dicts_handler = HandlingDicts()
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.url = url

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
        """Return a list of response objects.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Verify the SSL certificate, by default True
        
        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            A list of response objects.
        """
        resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
        resp_req.raise_for_status()
        return resp_req
    
    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
    ) -> StringIO:
        """Parse the raw file content.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
            The parsed content.
        """
        return self.cls_html_handler.lxml_parser(resp_req=resp_req)
    
    def transform_data(
        self, 
        html_root: HtmlElement, 
        list_th: list[str], 
        xpath_td: str, 
        na_values: str = "-",
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        html_root : HtmlElement
            The root element of the HTML document.
        list_th : list[str]
            The list of table headers.
        xpath_td : str
            The XPath expression for the table data.
        na_values : str, optional
            The value to use for missing data, by default "-"
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        list_td = self.cls_html_handler.lxml_xpath(
            html_content=html_root, 
            str_xpath=xpath_td,
        )
        list_td = [el.text for el in list_td]
        list_ser = self.cls_dicts_handler.pair_headers_with_data(
            list_headers=list_th, 
            list_data=list_td,
        )
        df_ = pd.DataFrame(list_ser)
        df_ = df_.replace(to_replace=na_values, value=None)
        return df_
    
    def run(
        self,
        dict_dtypes: dict[str, Union[str, int, float]],
        str_fmt_dt: str = "YYYY-MM-DD",
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "<COUNTRY>_<SOURCE>_<TABLE_NAME>"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        dict_dtypes : dict[str, Union[str, int, float]]
            The data types of the columns
        str_fmt_dt : str, optional
            The date format string, by default "YYYY-MM-DD"
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "<COUNTRY>_<SOURCE>_<TABLE_NAME>"

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
            dict_dtypes=dict_dtypes, 
            str_fmt_dt=str_fmt_dt,
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
        

class B3TradingHoursStocks(B3TradingHoursCore):
    """B3 Trading Hours for Stocks Market."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        url: str = "https://www.b3.com.br/pt_br/solucoes/plataformas/puma-trading-system/para-participantes-e-traders/horario-de-negociacao/acoes/",
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
        super().__init__(date_ref=date_ref, logger=logger, cls_db=cls_db, url=url)

    def transform_data(
        self, 
        html_root: HtmlElement, 
        list_th: list[str] = [
            "MERCADO",
            "CANCELAMENTO_DE_OFERTAS_INICIO",
            "CANCELAMENTO_DE_OFERTAS_FIM",
            "PRE_ABERTURA_INICIO",
            "PRE_ABERTURA_FIM",
            "NEGOCIACAO_INICIO",
            "NEGOCIACAO_FIM",
            "CALL_DE_FECHAMENTO_INICIO",
            "CALL_DE_FECHAMENTO_FIM",
            "AFTER_MARKET_CANCELAMENTO_DE_OFERTAS_INICIO",
            "AFTER_MARKET_CANCELAMENTO_DE_OFERTAS_FIM",
            "AFTER_MARKET_NEGOCIACAO_INICIO",
            "AFTER_MARKET_NEGOCIACAO_FIM",
            "AFTER_MARKET_CANCELAMENTO_DE_OFERTAS_FECHAMENTO_INICIO",
            "AFTER_MARKET_CANCELAMENTO_DE_OFERTAS_FECHAMENTO_FIM", 
        ], 
        xpath_td: str = '//*[@id="conteudo-principal"]/div[4]/div/div/table[1]/' \
            + 'tbody/tr[position() > 1]/td', 
        na_values: str = "-"
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        html_root : HtmlElement
            The root element of the HTML document.
        list_th : list[str], optional
            The list of table headers.
        xpath_td : str, optional
            The XPath expression for the table data.
        na_values : str, optional
            The value to use for missing data, by default "-"
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return super().transform_data(
            html_root=html_root, 
            list_th=list_th, 
            xpath_td=xpath_td,
            na_values=na_values,
        )
    
    def run(
        self, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "MERCADO": str,
            "CANCELAMENTO_DE_OFERTAS_INICIO": str,
            "CANCELAMENTO_DE_OFERTAS_FIM": str,
            "PRE_ABERTURA_INICIO": str,
            "PRE_ABERTURA_FIM": str,
            "NEGOCIACAO_INICIO": str,
            "NEGOCIACAO_FIM": str,
            "CALL_DE_FECHAMENTO_INICIO": str,
            "CALL_DE_FECHAMENTO_FIM": str,
            "AFTER_MARKET_CANCELAMENTO_DE_OFERTAS_INICIO": str,
            "AFTER_MARKET_CANCELAMENTO_DE_OFERTAS_FIM": str,
            "AFTER_MARKET_NEGOCIACAO_INICIO": str,
            "AFTER_MARKET_NEGOCIACAO_FIM": str,
            "AFTER_MARKET_CANCELAMENTO_DE_OFERTAS_FECHAMENTO_INICIO": str,
            "AFTER_MARKET_CANCELAMENTO_DE_OFERTAS_FECHAMENTO_FIM": str,
        },
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "br_b3_trading_hours_stocks"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        dict_dtypes : dict[str, Union[str, int, float]], optional
            The data types of the columns.
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "br_b3_trading_hours_stocks"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        return super().run(
            dict_dtypes=dict_dtypes,
            timeout=timeout,
            bool_verify=bool_verify,
            bool_insert_or_ignore=bool_insert_or_ignore,
            str_table_name=str_table_name,
        )
    

class B3TradingHoursOptionsExercise(B3TradingHoursCore):
    """B3 Trading Hours for Options Exercise."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        url: str = "https://www.b3.com.br/pt_br/solucoes/plataformas/puma-trading-system/para-participantes-e-traders/horario-de-negociacao/acoes/",
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
        super().__init__(date_ref=date_ref, logger=logger, cls_db=cls_db, url=url)

    def transform_data(
        self, 
        html_root: HtmlElement, 
        list_th: list[str] = [
            "MERCADO",
            "EXERCICIO_MANUAL_DE_POSICAO_TITULAR_ANTES_DO_VENCIMENTO_INICIO",
            "EXERCICIO_MANUAL_DE_POSICAO_TITULAR_ANTES_DO_VENCIMENTO_FIM",
            "EXERCICIO_MANUAL_DE_POSICAO_TITULAR_NO_VENCIMENTO_INICIO",
            "EXERCICIO_MANUAL_DE_POSICAO_TITULAR_NO_VENCIMENTO_FIM",
            "ARQUIVO_DE_POSICOES_MAIS_IMBARQ_NO_VENCIMENTO",
            "CONTRARY_EXERCISE_NO_VENCIMENTO_INICIO",
            "CONTRARY_EXERCISE_NO_VENCIMENTO_FIM",
            "EXERCICIO_AUTOMATICO_DE_POSICAO_TITULAR_NO_VENCIMENTO_INICIO",
        ], 
        xpath_td: str = '//*[@id="conteudo-principal"]/div[4]/div/div/table[2]/' \
            + 'tbody/tr[position() > 1]/td', 
        na_values: str = "-"
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        html_root : HtmlElement
            The root element of the HTML document.
        list_th : list[str], optional
            The list of table headers.
        xpath_td : str, optional
            The XPath expression for the table data.
        na_values : str, optional
            The value to use for missing data, by default "-"
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return super().transform_data(
            html_root=html_root, 
            list_th=list_th, 
            xpath_td=xpath_td,
            na_values=na_values,
        )
    
    def run(
        self, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "MERCADO": str,
            "EXERCICIO_MANUAL_DE_POSICAO_TITULAR_ANTES_DO_VENCIMENTO_INICIO": str,
            "EXERCICIO_MANUAL_DE_POSICAO_TITULAR_ANTES_DO_VENCIMENTO_FIM": str,
            "EXERCICIO_MANUAL_DE_POSICAO_TITULAR_NO_VENCIMENTO_INICIO": str,
            "EXERCICIO_MANUAL_DE_POSICAO_TITULAR_NO_VENCIMENTO_FIM": str,
            "ARQUIVO_DE_POSICOES_MAIS_IMBARQ_NO_VENCIMENTO": str,
            "CONTRARY_EXERCISE_NO_VENCIMENTO_INICIO": str,
            "CONTRARY_EXERCISE_NO_VENCIMENTO_FIM": str,
            "EXERCICIO_AUTOMATICO_DE_POSICAO_TITULAR_NO_VENCIMENTO_INICIO": str,
        },
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "br_b3_trading_hours_options_exercise",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        dict_dtypes : dict[str, Union[str, int, float]], optional
            The data types of the columns.
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "br_b3_trading_hours_options_exercise"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        return super().run(
            dict_dtypes=dict_dtypes,
            timeout=timeout,
            bool_verify=bool_verify,
            bool_insert_or_ignore=bool_insert_or_ignore,
            str_table_name=str_table_name,
        )
    

class B3TradingHoursPMIFutures(B3TradingHoursCore):
    """B3 Trading Hours for Options Exercise."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        url: str = "https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-members-and-traders/trading-hours/derivatives/indices/",
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
        super().__init__(date_ref=date_ref, logger=logger, cls_db=cls_db, url=url)

    def transform_data(
        self, 
        html_root: HtmlElement, 
        list_th: list[str] = [
            "CONTRACT",
            "TICKER",
            "REGULAR_HOURS_OPENING",
            "REGULAR_HOURS_CLOSING",
            "ORDER_CANCELLATION_OPENING",
            "ORDER_CANCELLATION_CLOSING",
            "ELECTRONIC_CALL_OPENING",
            "ORDER_CANCELLATION_EOD_OPENING",
            "ORDER_CANCELLATION_EOD_CLOSING",
            "EXTENDED_HOURS_T_0_OPENING",
            "EXTENDED_HOURS_T_0_CLOSING",
            "AFTER_HOURS_T_1_OPENING",
            "AFTER_HOURS_T_1_CLOSING",
        ], 
        xpath_td: str = '//*[@id="conteudo-principal"]/div[4]/div/div/table[1]/tbody/tr/td', 
        na_values: str = "-"
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        html_root : HtmlElement
            The root element of the HTML document.
        list_th : list[str], optional
            The list of table headers.
        xpath_td : str, optional
            The XPath expression for the table data.
        na_values : str, optional
            The value to use for missing data, by default "-"
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return super().transform_data(
            html_root=html_root, 
            list_th=list_th, 
            xpath_td=xpath_td,
            na_values=na_values,
        )
    
    def run(
        self, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "CONTRACT": str,
            "TICKER": str,
            "REGULAR_HOURS_OPENING": str,
            "REGULAR_HOURS_CLOSING": str,
            "ORDER_CANCELLATION_OPENING": str,
            "ORDER_CANCELLATION_CLOSING": str,
            "ELECTRONIC_CALL_OPENING": str,
            "ORDER_CANCELLATION_EOD_OPENING": str,
            "ORDER_CANCELLATION_EOD_CLOSING": str,
            "EXTENDED_HOURS_T_0_OPENING": str,
            "EXTENDED_HOURS_T_0_CLOSING": str,
            "AFTER_HOURS_T_1_OPENING": str,
            "AFTER_HOURS_T_1_CLOSING": str,
        },
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "br_b3_trading_hours_pmi_future",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        dict_dtypes : dict[str, Union[str, int, float]], optional
            The data types of the columns.
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "br_b3_trading_hours_pmi_future"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        return super().run(
            dict_dtypes=dict_dtypes,
            timeout=timeout,
            bool_verify=bool_verify,
            bool_insert_or_ignore=bool_insert_or_ignore,
            str_table_name=str_table_name,
        )


class B3TradingHoursStockIndexFutures(B3TradingHoursCore):
    """B3 Trading Hours for Options Exercise."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        url: str = "https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-members-and-traders/trading-hours/derivatives/indices/",
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
        super().__init__(date_ref=date_ref, logger=logger, cls_db=cls_db, url=url)

    def transform_data(
        self, 
        html_root: HtmlElement, 
        list_th: list[str] = [
            "CONTRACT",
            "TICKER",
            "REGULAR_HOURS_OPENING",
            "REGULAR_HOURS_CLOSING",
            "ORDER_CANCELLATION_OPENING",
            "ORDER_CANCELLATION_CLOSING",
            "ELECTRONIC_CALL_OPENING",
            "ORDER_CANCELLATION_EOD_OPENING",
            "ORDER_CANCELLATION_EOD_CLOSING",
            "EXTENDED_HOURS_T_0_OPENING",
            "EXTENDED_HOURS_T_0_CLOSING",
            "AFTER_HOURS_T_1_OPENING",
            "AFTER_HOURS_T_1_CLOSING",
        ], 
        xpath_td: str = '//*[@id="conteudo-principal"]/div[4]/div/div/table[2]/tbody/tr/td', 
        na_values: str = "-"
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        html_root : HtmlElement
            The root element of the HTML document.
        list_th : list[str], optional
            The list of table headers.
        xpath_td : str, optional
            The XPath expression for the table data.
        na_values : str, optional
            The value to use for missing data, by default "-"
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return super().transform_data(
            html_root=html_root, 
            list_th=list_th, 
            xpath_td=xpath_td,
            na_values=na_values,
        )
    
    def run(
        self, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "CONTRACT": str,
            "TICKER": str,
            "REGULAR_HOURS_OPENING": str,
            "REGULAR_HOURS_CLOSING": str,
            "ORDER_CANCELLATION_OPENING": str,
            "ORDER_CANCELLATION_CLOSING": str,
            "ELECTRONIC_CALL_OPENING": str,
            "ORDER_CANCELLATION_EOD_OPENING": str,
            "ORDER_CANCELLATION_EOD_CLOSING": str,
            "EXTENDED_HOURS_T_0_OPENING": str,
            "EXTENDED_HOURS_T_0_CLOSING": str,
            "AFTER_HOURS_T_1_OPENING": str,
            "AFTER_HOURS_T_1_CLOSING": str,
        },
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "br_b3_trading_hours_stock_index_futures",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        dict_dtypes : dict[str, Union[str, int, float]], optional
            The data types of the columns.
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "br_b3_trading_hours_stock_index_futures"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        return super().run(
            dict_dtypes=dict_dtypes,
            timeout=timeout,
            bool_verify=bool_verify,
            bool_insert_or_ignore=bool_insert_or_ignore,
            str_table_name=str_table_name,
        )
    

class B3TradingHoursRealDenominatedInterestRates(B3TradingHoursCore):
    """B3 Trading Hours for Real Denominated Interest Rates.
    
    Real denominated interest rates, options contracts and structured transactions.
    """

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        url: str = "https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-members-and-traders/trading-hours/derivatives/interest-rates/",
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
        super().__init__(date_ref=date_ref, logger=logger, cls_db=cls_db, url=url)

    def transform_data(
        self, 
        html_root: HtmlElement, 
        list_th: list[str] = [
            "CONTRACT",
            "TICKER",
            "REGULAR_HOURS_OPENING",
            "REGULAR_HOURS_CLOSING",
            "ORDER_CANCELLATION_OPENING",
            "ORDER_CANCELLATION_CLOSING",
            "ELECTRONIC_CALL_OPENING",
            "ORDER_CANCELLATION_EOD_OPENING",
            "ORDER_CANCELLATION_EOD_CLOSING",
            "EXTENDED_HOURS_T_0_OPENING",
            "EXTENDED_HOURS_T_0_CLOSING",
            "AFTER_HOURS_T_1_OPENING",
            "AFTER_HOURS_T_1_CLOSING",
        ], 
        xpath_td: str = '//*[@id="conteudo-principal"]/div[4]/div/div/table[1]/tbody/tr/td', 
        na_values: str = "-"
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        html_root : HtmlElement
            The root element of the HTML document.
        list_th : list[str], optional
            The list of table headers.
        xpath_td : str, optional
            The XPath expression for the table data.
        na_values : str, optional
            The value to use for missing data, by default "-"
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return super().transform_data(
            html_root=html_root, 
            list_th=list_th, 
            xpath_td=xpath_td,
            na_values=na_values,
        )
    
    def run(
        self, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "CONTRACT": str,
            "TICKER": str,
            "REGULAR_HOURS_OPENING": str,
            "REGULAR_HOURS_CLOSING": str,
            "ORDER_CANCELLATION_OPENING": str,
            "ORDER_CANCELLATION_CLOSING": str,
            "ELECTRONIC_CALL_OPENING": str,
            "ORDER_CANCELLATION_EOD_OPENING": str,
            "ORDER_CANCELLATION_EOD_CLOSING": str,
            "EXTENDED_HOURS_T_0_OPENING": str,
            "EXTENDED_HOURS_T_0_CLOSING": str,
            "AFTER_HOURS_T_1_OPENING": str,
            "AFTER_HOURS_T_1_CLOSING": str,
        },
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "br_b3_trading_hours_real_denominated_interest_rates",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        dict_dtypes : dict[str, Union[str, int, float]], optional
            The data types of the columns.
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "br_b3_trading_hours_real_denominated_interest_rates"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        return super().run(
            dict_dtypes=dict_dtypes,
            timeout=timeout,
            bool_verify=bool_verify,
            bool_insert_or_ignore=bool_insert_or_ignore,
            str_table_name=str_table_name,
        )
    

class B3TradingHoursUSDollarDenominatedInterestRatesFutures(B3TradingHoursCore):
    """B3 Trading Hours for US Dollar Denominated Interest Rates Futures.
    
    Dollar Denominated Interest Rates Futures, Options Contracts and Structures Transactions.
    """

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        url: str = "https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-members-and-traders/trading-hours/derivatives/interest-rates/",
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
        super().__init__(date_ref=date_ref, logger=logger, cls_db=cls_db, url=url)

    def transform_data(
        self, 
        html_root: HtmlElement, 
        list_th: list[str] = [
            "CONTRACT",
            "TICKER",
            "REGULAR_HOURS_OPENING",
            "REGULAR_HOURS_CLOSING",
            "ORDER_CANCELLATION_OPENING",
            "ORDER_CANCELLATION_CLOSING",
            "ELECTRONIC_CALL_OPENING",
            "ORDER_CANCELLATION_EOD_OPENING",
            "ORDER_CANCELLATION_EOD_CLOSING",
            "EXTENDED_HOURS_T_0_OPENING",
            "EXTENDED_HOURS_T_0_CLOSING",
            "AFTER_HOURS_T_1_OPENING",
            "AFTER_HOURS_T_1_CLOSING",
        ], 
        xpath_td: str = '//*[@id="conteudo-principal"]/div[4]/div/div/table[2]/tbody/tr/td', 
        na_values: str = "-"
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        html_root : HtmlElement
            The root element of the HTML document.
        list_th : list[str], optional
            The list of table headers.
        xpath_td : str, optional
            The XPath expression for the table data.
        na_values : str, optional
            The value to use for missing data, by default "-"
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return super().transform_data(
            html_root=html_root, 
            list_th=list_th, 
            xpath_td=xpath_td,
            na_values=na_values,
        )
    
    def run(
        self, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "CONTRACT": str,
            "TICKER": str,
            "REGULAR_HOURS_OPENING": str,
            "REGULAR_HOURS_CLOSING": str,
            "ORDER_CANCELLATION_OPENING": str,
            "ORDER_CANCELLATION_CLOSING": str,
            "ELECTRONIC_CALL_OPENING": str,
            "ORDER_CANCELLATION_EOD_OPENING": str,
            "ORDER_CANCELLATION_EOD_CLOSING": str,
            "EXTENDED_HOURS_T_0_OPENING": str,
            "EXTENDED_HOURS_T_0_CLOSING": str,
            "AFTER_HOURS_T_1_OPENING": str,
            "AFTER_HOURS_T_1_CLOSING": str,
        },
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "br_b3_trading_hours_usdollar_denominated_interest_rates_futures",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        dict_dtypes : dict[str, Union[str, int, float]], optional
            The data types of the columns.
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default 
            "br_b3_trading_hours_usdollar_denominated_interest_rates_futures"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        return super().run(
            dict_dtypes=dict_dtypes,
            timeout=timeout,
            bool_verify=bool_verify,
            bool_insert_or_ignore=bool_insert_or_ignore,
            str_table_name=str_table_name,
        )
    

class B3TradingHoursCommoditiesFutures(B3TradingHoursCore):
    """B3 Trading Hours for Commodities Futures.
    
    Commodities Futures, Options Contracts and Structured Transactions.
    """

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        url: str = "https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-members-and-traders/trading-hours/derivatives/commodities/",
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
        super().__init__(date_ref=date_ref, logger=logger, cls_db=cls_db, url=url)

    def transform_data(
        self, 
        html_root: HtmlElement, 
        list_th: list[str] = [
            "CONTRACT",
            "TICKER",
            "REGULAR_HOURS_OPENING",
            "REGULAR_HOURS_CLOSING",
            "AFTER_HOURS_T_1_OPENING",
            "AFTER_HOURS_T_1_CLOSING",
        ], 
        xpath_td: str = '//*[@id="conteudo-principal"]/div[4]/div/div/div[1]/table/tbody/tr/td', 
        na_values: str = "-"
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        html_root : HtmlElement
            The root element of the HTML document.
        list_th : list[str], optional
            The list of table headers.
        xpath_td : str, optional
            The XPath expression for the table data.
        na_values : str, optional
            The value to use for missing data, by default "-"
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return super().transform_data(
            html_root=html_root, 
            list_th=list_th, 
            xpath_td=xpath_td,
            na_values=na_values,
        )
    
    def run(
        self, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "CONTRACT": str,
            "TICKER": str,
            "REGULAR_HOURS_OPENING": str,
            "REGULAR_HOURS_CLOSING": str,
            "AFTER_HOURS_T_1_OPENING": str,
            "AFTER_HOURS_T_1_CLOSING": str
        },
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "br_b3_trading_hours_commodities_futures",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        dict_dtypes : dict[str, Union[str, int, float]], optional
            The data types of the columns.
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "br_b3_trading_hours_commodities_futures"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        return super().run(
            dict_dtypes=dict_dtypes,
            timeout=timeout,
            bool_verify=bool_verify,
            bool_insert_or_ignore=bool_insert_or_ignore,
            str_table_name=str_table_name,
        )