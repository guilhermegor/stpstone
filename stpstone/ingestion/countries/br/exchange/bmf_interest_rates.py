import pandas as pd
from datetime import datetime
from typing import Optional, List, Any, Tuple, Dict, Union
from numbers import Number
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from lxml.html import HtmlElement
from stpstone._config.global_slots import YAML_BMF_INTEREST_RATES
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.loggs.create_logs import CreateLog


class BMFInterestRates(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        dt_ref: datetime = DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db: Optional[Session] = None,
        logger: Optional[Logger] = None,
        token: Optional[str] = None,
        list_slugs: Optional[List[str]] = None
    ) -> None:
        super().__init__(
            dict_metadata=YAML_BMF_INTEREST_RATES,
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
        self.token = token
        self.list_slugs = list_slugs
        self.dt_ref_ddmmyyyy = dt_ref.strftime('%d/%m/%Y')
        self.dt_ref_yyyymmdd = dt_ref.strftime('%Y%m%d')

    def td_th_parser(self, root: HtmlElement, int_iter: int) -> List[Dict[str, Union[str, Number]]]:
        list_th = list()
        for x in HtmlHandler().lxml_xpath(
            root, YAML_BMF_INTEREST_RATES['rates']['xpaths']['list_th'].format(int_iter)
        ):
            str_raw = StrHandler().remove_diacritics(x.text.strip().lower())
            if str_raw == "dias":
                list_th.append("DIAS_CORRIDOS")
            elif str_raw == "di x pre":
                list_th.append("DI_PRE_252")
                list_th.append("DI_PRE_360")
            elif str_raw == "selic x pre":
                list_th.append("SELIC_PRE_252")
            elif str_raw == "di x tr":
                list_th.append("DI_TR_252")
                list_th.append("DI_TR_360")
            elif str_raw == "dolar x pre":
                list_th.append("DOLAR_PRE_252")
                list_th.append("DOLAR_PRE_360")
            elif str_raw == "real x euro":
                list_th.append("REAL_EURO")
            elif str_raw == "di x euro":
                list_th.append("DI_EURO_360")
            elif str_raw == "tbf x pre":
                list_th.append("TBF_PRE_252")
                list_th.append("TBF_PRE_360")
            elif str_raw == "tr x pre":
                list_th.append("TR_PRE_252")
                list_th.append("TR_PRE_360")
            elif str_raw == "di x dolar":
                list_th.append("DI_DOLAR_360")
            elif str_raw == "cupom cambial oc1":
                list_th.append("CUPOM_CAMBIAL_OC1_360")
            elif str_raw == "cupom limpo":
                list_th.append("CUPOM_LIMPO_360")
            elif str_raw == "real x dolar":
                list_th.append("REAL_DOLAR")
            elif str_raw == "ibrx-50":
                list_th.append("IBRX_50")
            elif str_raw == "ibovespa":
                list_th.append("IBOVESPA")
            elif str_raw == "di x igp-m":
                list_th.append("DI_IGP_M_252")
            elif str_raw == "di x ipca":
                list_th.append("DI_IPCA_252")
            elif str_raw == "ajuste pre":
                list_th.append("AJUSTE_PRE_252")
                list_th.append("AJUSTE_PRE_360")
            elif str_raw == "ajuste cupom":
                list_th.append("AJUSTE_CUPOM_360")
            elif str_raw == "real x iene":
                list_th.append("REAL_IENE")
            elif str_raw == "spread libor euro x dolar":
                list_th.append("SPREAD_LIBOR_EURO_DOLAR")
            elif str_raw == "libor":
                list_th.append("LIBOR_360")
            else:
                raise Exception(f"Table Header not Found: {str_raw}")
        list_td = [
            float(x.text.strip().replace(",", "."))
            for x in HtmlHandler().lxml_xpath(
                root, YAML_BMF_INTEREST_RATES['rates']['xpaths']['list_td'].format(int_iter)
            )
        ]
        list_ser = HandlingDicts().pair_headers_with_data(list_th, list_td)
        return list_ser

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        list_dfs = list()
        bl_debug = True if StrHandler().match_string_like(
            resp_req.url, '*bl_debug=True*') == True else False
        root = HtmlHandler().lxml_parser(resp_req)
        # export html tree to data folder, if is user's will
        if bl_debug == True:
            path_project = DirFilesManagement().find_project_root(marker='pyproject.toml')
            HtmlHandler().html_tree(root, file_path=rf'{path_project}/data/test.html')
        for i in range(1, 5):
            list_ser = self.td_th_parser(root, i)
            list_dfs.append(pd.DataFrame(list_ser))
        df_ = list_dfs[0]
        for i in range(1, len(list_dfs)):
            df_ = df_.merge(list_dfs[i], how='left', on='DIAS_CORRIDOS', suffixes=('', f'_{i}'))
        return df_
