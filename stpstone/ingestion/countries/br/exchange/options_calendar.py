from datetime import datetime
from logging import Logger
from time import sleep
from typing import List, Optional

import pandas as pd
from requests import Response
from sqlalchemy.orm import Session

from stpstone._config.global_slots import YAML_B3_OPTIONS_CALENDAR
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.html import HtmlHandler


class OptionsCalendarB3(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        date_ref:datetime=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None,
        token:Optional[str]=None,
        list_slugs:Optional[List[str]]=None
    ) -> None:
        super().__init__(
            dict_metadata=YAML_B3_OPTIONS_CALENDAR,
            session=session,
            date_ref=date_ref,
            cls_db=cls_db,
            logger=logger,
            token=token,
            list_slugs=list_slugs
        )
        self.session = session
        self.date_ref = date_ref
        self.cls_db = cls_db
        self.logger = logger
        self.token = token,
        self.list_slugs = list_slugs

    def req_trt_injection(self, resp_req:Response) -> Optional[pd.DataFrame]:
        list_ser = list()
        i = 1
        root = HtmlHandler().lxml_parser(resp_req)
        while i <= 20:
            try:
                list_th = [
                    x.text for x in HtmlHandler().lxml_xpath(
                        root, YAML_B3_OPTIONS_CALENDAR['settlement_dates']['xpaths']['list_th'].format(i)
                    )
                ]
                list_td = [
                    x.text for x in HtmlHandler().lxml_xpath(
                        root, YAML_B3_OPTIONS_CALENDAR['settlement_dates']['xpaths']['list_td'].format(i)
                    )
                ]
                dict_ = HandlingDicts().merge_n_dicts(
                    dict(zip(list_th, list_td)),
                    {
                        'Mês Referência': HtmlHandler().lxml_xpath(
                            root, YAML_B3_OPTIONS_CALENDAR['settlement_dates']['xpaths'][
                                'mes_ref'].format(i)
                        )[0].text,
                        'Ano Referência': DatesBRAnbima().year_number(DatesBRAnbima().curr_date())
                    }
                )
                list_ser.append(dict_)
                if (list_th is None) or (list_td is None):
                    break
                i += 1
            except IndexError:
                break
        return pd.DataFrame(list_ser)
