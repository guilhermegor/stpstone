import pandas as pd
from datetime import datetime
from typing import Optional, List, Any, Tuple
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from stpstone._config.global_slots import YAML_B3_TRADING_HOURS_B3
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.loggs.create_logs import CreateLog


class TradingHoursB3(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        dt_ref:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None,
        token:Optional[str]=None,
        list_slugs:Optional[List[str]]=None
    ) -> None:
        super().__init__(
            dict_metadata=YAML_B3_TRADING_HOURS_B3,
            session=session,
            dt_ref=dt_ref,
            cls_db=cls_db,
            logger=logger,
            token=token,
            list_slugs=list_slugs
        )
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.token = token,
        self.list_slugs = list_slugs

    def td_th_parser(self, resp_req:Response, list_th:List[Any]) \
        -> Tuple[List[Any], int, Optional[int]]:
        list_headers = list_th.copy()
        int_init_td = 0
        int_end_td = None
        if StrHandler().match_string_like(resp_req.url, '*#source=stocks*') == True:
            list_headers = [
                list_th[0],
                list_th[1] + ' Início',
                list_th[1] + ' Fim',
                list_th[2] + ' Início',
                list_th[2] + ' Fim',
                list_th[3] + ' Início',
                list_th[3] + ' Fim',
                list_th[4] + ' Início',
                list_th[4] + ' Fim',
                list_th[5] + ' ' + list_th[6] + ' Início',
                list_th[5] + ' ' + list_th[6] + ' Fim',
                list_th[5] + ' ' + list_th[7] + ' Início',
                list_th[5] + ' ' + list_th[7] + ' Fim',
                list_th[5] + ' ' + list_th[8] + ' Fechamento Início',
                list_th[5] + ' ' + list_th[8] + ' Fechamento Fim',
            ]
            int_init_td = 0
            int_end_td = 195
        elif StrHandler().match_string_like(resp_req.url, '*#source=stock_options*') == True:
            list_headers = [
                list_th[9],
                list_th[13] + ' Antes do Vencimento - Início',
                list_th[13] + ' Antes do Vencimento - Fim',
                list_th[13] + ' No Vencimento - Início',
                list_th[13] + ' No Vencimento - Fim',
                list_th[15] + ' No Vencimento',
                list_th[16] + ' No Vencimento - Início',
                list_th[16] + ' No Vencimento - Fim',
                list_th[17] + ' No Vencimento - Início',
            ]
            int_init_td = 195
            int_end_td = None
        elif StrHandler().match_string_like(resp_req.url, '*#source=pmi_future*') == True:
            list_headers = [
                list_th[0],
                list_th[1],
                list_th[2] + ' - Opening',
                list_th[2] + ' - Closing',
                list_th[3] + ' - Opening',
                list_th[3] + ' - Closing',
                list_th[4] + ' - Opening',
                list_th[5] + ' EOD - Opening',
                list_th[5] + ' EOD - Closing',
                list_th[6] + ' - Opening',
                list_th[6] + ' - Closing',
                list_th[7] + ' - Opening',
                list_th[7] + ' - Closing',
            ]
            int_init_td = 0
            int_end_td = 52
        elif StrHandler().match_string_like(resp_req.url, '*#source=stock_index_futures*') == True:
            list_headers = [
                list_th[0],
                list_th[1],
                list_th[2] + ' - Opening',
                list_th[2] + ' - Closing',
                list_th[3] + ' - Opening',
                list_th[3] + ' - Closing',
                list_th[4] + ' - Opening',
                list_th[5] + ' EOD - Opening',
                list_th[5] + ' EOD - Closing',
                list_th[6] + ' - Opening',
                list_th[6] + ' - Closing',
                list_th[7] + ' - Opening',
                list_th[7] + ' - Closing',
            ]
            int_init_td = 52
            int_end_td = None
        elif StrHandler().match_string_like(resp_req.url, '*#source=interest_rates*') == True:
            list_headers = [
                list_th[0],
                list_th[1],
                list_th[2] + ' - Opening',
                list_th[2] + ' - Closing',
                list_th[3] + ' - Opening',
                list_th[3] + ' - Closing',
                list_th[4] + ' - Opening',
                list_th[5] + ' EOD - Opening',
                list_th[5] + ' EOD - Closing',
                list_th[6] + ' - Opening',
                list_th[6] + ' - Closing',
                list_th[7] + ' - Opening',
                list_th[7] + ' - Closing',
            ]
            int_init_td = 0
            int_end_td = 130
        elif StrHandler().match_string_like(resp_req.url, '*#source=usd_interest_rates*') == True:
            list_headers = [
                list_th[0],
                list_th[1],
                list_th[2] + ' - Opening',
                list_th[2] + ' - Closing',
                list_th[3] + ' - Opening',
                list_th[3] + ' - Closing',
                list_th[4] + ' - Opening',
                list_th[5] + ' EOD - Opening',
                list_th[5] + ' EOD - Closing',
                list_th[6] + ' - Opening',
                list_th[6] + ' - Closing',
                list_th[7] + ' - Opening',
                list_th[7] + ' - Closing',
            ]
            int_init_td = 130
            int_end_td = None
        elif StrHandler().match_string_like(resp_req.url, '*#source=commodities*') == True:
            list_headers = [
                list_th[0],
                list_th[1],
                list_th[2] + ' - Opening',
                list_th[2] + ' - Closing',
                list_th[3] + ' - Opening',
                list_th[3] + ' - Closing'
            ]
            int_init_td = 0
            int_end_td = None
        elif StrHandler().match_string_like(resp_req.url, '*#source=crypto*') == True:
            list_headers = [
                list_th[0],
                list_th[1],
                list_th[2] + ' - Opening',
                list_th[2] + ' - Closing',
                list_th[3] + ' - Opening',
                list_th[3] + ' - Closing',
                list_th[4] + ' - Opening',
                list_th[5] + ' EOD - Opening',
                list_th[5] + ' EOD - Closing',
                list_th[6] + ' - Opening',
                list_th[6] + ' - Closing'
            ]
            int_init_td = 0
            int_end_td = None
        elif StrHandler().match_string_like(resp_req.url, '*#source=foreign_exchange_and_dollar_spot*') == True:
            list_headers = [
                list_th[0],
                list_th[1],
                list_th[2] + ' - Opening',
                list_th[2] + ' - Closing',
                list_th[3] + ' - Opening',
                list_th[3] + ' - Closing',
                list_th[4] + ' - Opening',
                list_th[5] + ' EOD - Opening',
                list_th[5] + ' EOD - Closing',
                list_th[6] + ' - Opening',
                list_th[6] + ' - Closing',
                list_th[7] + ' - Opening',
                list_th[7] + ' - Closing',
            ]
            int_init_td = 0
            int_end_td = None
        elif StrHandler().match_string_like(resp_req.url, '*#source=otc*') == True:
            list_headers = [
                list_th[0],
                list_th[1] + ' - Início',
                list_th[1] + ' - Fim',
                list_th[2] + ' - Início',
                list_th[2] + ' - Fim',
                list_th[3] + ' - Início',
                list_th[3] + ' - Fim',
                list_th[10] + ' - Antes do Vencimento - Exercício de Posição Titular - Início',
                list_th[10] + ' - Antes do Vencimento - Exercício de Posição Titular - Fim',
                list_th[10] + ' - No Vencimento - Encerramento de Posição - Início',
                list_th[10] + ' - No Vencimento - Encerramento de Posição - Fim',
                list_th[10] + ' - No Vencimento - Exercício de Posição Titular - Início',
                list_th[10] + ' - No Vencimento - Exercício de Posição Titular - Fim',
                list_th[4] + ' - Opening',
                list_th[4] + ' - Closing',
                list_th[6] + ' EOD - Opening',
                list_th[6] + ' EOD - Closing',
                list_th[5] + ' - Opening',
                list_th[5] + ' - Closing',
            ]
            int_init_td = 0
            int_end_td = None
        elif StrHandler().match_string_like(resp_req.url, '*#source=opf_before_exc_date*') == True:
            list_headers = [
                list_th[0],
                list_th[1],
                list_th[2],
                list_th[3]
            ]
            int_init_td = 0
            int_end_td = 32
        elif StrHandler().match_string_like(resp_req.url, '*#source=opf_after_exc_date*') == True:
            list_headers = [
                list_th[4],
                list_th[5],
                list_th[6],
                list_th[7] + ' Without Exercise Risk',
                list_th[7] + ' Holder'
            ]
            int_init_td = 32
            int_end_td = None
        else:
            if self.logger is not None:
                CreateLog().warning(
                    self.logger,
                    'No source found in url, for HTML webscraping, please revisit the code'
                    + f' if it is an unexpected behaviour - URL: {resp_req.url}'
                )
            else:
                print('No source found in url, for HTML webscraping, please revisit the code'
                    + f' if it is an unexpected behaviour - URL: {resp_req.url}')
        return list_headers, int_init_td, int_end_td

    def req_trt_injection(self, resp_req:Response) -> Optional[pd.DataFrame]:
        bl_debug = True if StrHandler().match_string_like(
            resp_req.url, '*&bl_debug=True*') == True else False
        root = HtmlHandler().lxml_parser(resp_req)
        # export html tree to data folder, if is user's will
        if bl_debug == True:
            path_project = DirFilesManagement().find_project_root(marker='pyproject.toml')
            HtmlHandler().html_tree(root, file_path=rf'{path_project}/data/test.html')
        list_th = [
            x.text.strip() for x in HtmlHandler().lxml_xpath(
                root, YAML_B3_TRADING_HOURS_B3['stocks']['xpaths']['list_th']
            )
        ]
        list_td = [
            '' if x.text is None else x.text.replace('\xa0', '').strip()
            for x in HtmlHandler().lxml_xpath(
                root, YAML_B3_TRADING_HOURS_B3['stocks']['xpaths']['list_td']
            )
        ]
        if bl_debug == True:
            print(f'LIST TH RAW: {list_th}')
            print(f'LIST TD RAW: {list_td}')
        # deal with data/headers specificity for the project
        list_headers, int_init_td, int_end_td = self.td_th_parser(resp_req, list_th)
        if bl_debug == True:
            print(f'LIST TH TRT: {list_headers}')
            print(f'LEN LIST HEADERS: {len(list_headers)}')
            print(f'LIST TD TRT: {list_td[int_init_td:int_end_td]}')
            print(f'LEN LIST TD: {len(list_td[int_init_td:int_end_td])}')
        list_ser = HandlingDicts().pair_headers_with_data(
            list_headers,
            list_td[int_init_td:int_end_td]
        )
        return pd.DataFrame(list_ser)
