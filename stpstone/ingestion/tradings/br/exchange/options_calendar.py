### OPTIONS CALENDAR INGESTION REQUEST ###

# pypi.org libs
import pandas as pd
from datetime import datetime
from typing import Optional, List
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
# project modules
from stpstone._config.global_slots import YAML_B3_OPTIONS_CALENDAR
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.session import ReqSession
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.parsers.html import HtmlHndler
from stpstone.utils.parsers.dicts import HandlingDicts


class OptionsCalendarB3(ABCRequests):

    def __init__(
        self,
        session:Optional[ReqSession]=None,
        dt_ref:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None, 
        token:Optional[str]=None, 
        list_slugs:Optional[List[str]]=None
    ) -> None:
        super().__init__(
            dict_metadata=YAML_B3_OPTIONS_CALENDAR,
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
    
    def req_trt_injection(self, req_resp:Response) -> Optional[pd.DataFrame]:
        list_ser = list()
        i = 1
        root = HtmlHndler().lxml_parser(req_resp)
        HtmlHndler().html_tree(root, r"C:\Users\guiro\Downloads\html-tree-options-settlement-calendar_20250225_0936.txt")
        while i <= 20:
            try:
                list_th = [
                    x.text for x in HtmlHndler().lxml_xpath(
                        root, YAML_B3_OPTIONS_CALENDAR['settlement_dates']['xpaths']['list_th'].format(i)
                    )
                ]
                list_td = [
                    x.text for x in HtmlHndler().lxml_xpath(
                        root, YAML_B3_OPTIONS_CALENDAR['settlement_dates']['xpaths']['list_td'].format(i)
                    )
                ]
                dict_ = HandlingDicts().merge_n_dicts(
                    dict(zip(list_th, list_td)),
                    {
                    'Mês Referência': HtmlHndler().lxml_xpath(
                        root, YAML_B3_OPTIONS_CALENDAR['settlement_dates']['xpaths']['mes_ref'].format(i)
                    )[0].text
                }
                )
                list_ser.append(dict_)
                if (list_th is None) or (list_td is None):
                    break
                i += 1
            except IndexError:
                break
        return pd.DataFrame(list_ser)
