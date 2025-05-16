import pandas as pd
from datetime import datetime
from typing import Optional, List
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from urllib.parse import urlparse
from stpstone._config.global_slots import YAML_BR_PTAX_BCB
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.parsers.html import HtmlHandler


class PTAXBCB(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        dt_start:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 2),
        dt_end:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None,
        token:Optional[str]=None,
        list_slugs:Optional[List[str]]=None,
        bl_debug:bool=False
    ) -> None:
        super().__init__(
            dict_metadata=YAML_BR_PTAX_BCB,
            session=session,
            cls_db=cls_db,
            logger=logger,
            token=token
        )
        self.session = session
        self.dt_start = dt_start
        self.dt_end = dt_end
        self.cls_db = cls_db
        self.logger = logger
        self.token = token
        self.bl_debug = bl_debug
        self.dt_sup_yyyymmdd = self.dt_end.strftime('%Y%m%d')
        self.dt_inf_ddmmyyyy = self.dt_start.strftime('%d/%m/%Y')
        self.dt_sup_ddmmyyyy = self.dt_end.strftime('%d/%m/%Y')
        self.df_ids = self.source('ids', bl_fetch=True)
        self.list_slugs = list_slugs if list_slugs is not None else \
            list(self.df_ids['CURRENCY_ID'].unique())

    def req_trt_injection(self, resp_req:Response) -> Optional[pd.DataFrame]:
        tup_parsed_url = urlparse(resp_req.url)
        if (f'{tup_parsed_url.scheme}://{tup_parsed_url.hostname}/'
             == YAML_BR_PTAX_BCB['ids']['host']) \
            and ('&' not in tup_parsed_url.query):
            root = HtmlHandler().lxml_parser(resp_req)
            selectors_currency = HtmlHandler().lxml_xpath(
                root, YAML_BR_PTAX_BCB['ids']['xpaths']['currency_options']
            )
            list_ser = [
                {
                    'CURRENCY_ID': selector.get('value'),
                    'CURRENCY_NAME': selector.text
                } for selector in selectors_currency
            ]
            return pd.DataFrame(list_ser)
        else:
            return None

    @property
    def composition_currency(self) -> pd.DataFrame:
        """
        Composition of available currency rates and close currency rates.
        - fetches the available currency rates from PTAX BCB;
        - left merge with the ids dataframe to add the CURRENCY_ID;
        - fetches the close currency rates, and performs left-join to add CURRENCY_NAME,
            COUNTRY_CODE and COUNTRY_NAME

        Args:
            None

        Returns:
            pd.DataFrame

        Metadata: https://www.bcb.gov.br/estabilidadefinanceira/historicocotacoes
        """
        df_available_currencies = super().source(
            'available_currencies', bl_fetch=True, bl_debug=self.bl_debug)
        df_available_currencies = df_available_currencies.merge(
            self.df_ids,
            how='left',
            left_on='NAME',
            right_on='CURRENCY_NAME',
            suffixes=('_', '')
        )
        df_close_currency_rates = super().source(
            'close_currency_rates', bl_fetch=True, bl_debug=self.bl_debug)
        df_close_currency_rates = df_close_currency_rates.merge(
            df_available_currencies,
            how='left',
            left_on='CURRENCY_SYMBOL',
            right_on='SYMBOL',
            suffixes=('_', '')
        )
        return df_close_currency_rates
