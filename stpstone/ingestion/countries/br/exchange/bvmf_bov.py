import pandas as pd
from datetime import datetime
from typing import Optional, List
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from stpstone._config.global_slots import YAML_B3_BVMF_BOV
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.numbers import NumHandler


class BVMFBOV(ABCRequests):

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
            dict_metadata=YAML_B3_BVMF_BOV,
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
        self.dt_ref_mm_yyyy = dt_ref.strftime('%m/%Y')

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        # setting variables
        list_th = list()
        list_td = list()
        bs_html = HtmlHandler().bs_parser(resp_req)
        # table
        bs_table = bs_html.find_all('table')[11]
        # looping within rows
        for i_tr, tr in enumerate(bs_table.find_all('tr')):
            #   checking if is a header how, otherwise it is a data row
            if i_tr < 2:
                #   getting headers
                list_th.extend([
                    StrHandler().remove_diacritics(el.get_text())
                        .replace('\xa0', '')
                        .replace('Totais dos pregoes  Ref: ', '')
                        .replace('(R$)', 'BRL')
                        .replace(' - ', ' ')
                        .replace(' ', '_')
                        .strip()
                        .upper()
                    for el in tr.find_all('td')
                    if len(
                        StrHandler().remove_diacritics(el.get_text())\
                        .replace('\xa0', '')
                    ) > 0
                ])
            else:
                #   getting data
                list_td.extend([
                    # data numeric
                    float(td.get_text()
                        .strip()
                        .replace('.', '')
                        .replace(",", "."))
                    if NumHandler().is_numeric(
                        StrHandler().remove_diacritics(td.get_text())
                            .strip()
                            .replace('.', '')
                            .replace(",", ".")
                    )
                    # data not numeric
                    else
                        StrHandler().remove_diacritics(td.get_text())
                            .strip()
                            .replace(' de ', ' ')
                            .replace(' do ', ' ')
                            .replace(' a ', ' ')
                            .replace(' e ', ' ')
                            .replace(' - ', ' ')
                            .replace('-', ' ')
                            .replace(' / ', ' ')
                            .replace(' ', '_')
                            .replace('.', '')
                            .replace(",", ".")
                            .replace('(', '')
                            .replace(')', '')
                            .replace('/', '')
                            .upper()
                    for td in tr.find_all('td')
                ])
        # dealing with header raw data
        list_th = [
            list_th[2],
            list_th[3],
            list_th[4],
            list_th[3] + '_' + '12M',
            list_th[4] + '_' + '12M',
        ]
        # pair headers and data within a list
        list_ser = HandlingDicts().pair_headers_with_data(
            list_th,
            list_td
        )
        # turning into dataframe
        df_ = pd.DataFrame(list_ser)
        df_['PERIODO_REF'] = self.dt_ref_mm_yyyy
        return df_
