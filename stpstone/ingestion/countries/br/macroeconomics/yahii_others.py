import pandas as pd
from datetime import datetime
from typing import Optional, List, Any, Dict
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from stpstone._config.global_slots import YAML_YAHII_OHTERS
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.parsers.numbers import NumHandler
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.loggs.create_logs import CreateLog


class YahiiOthersBR(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        int_delay_seconds: int = 20,
        dt_ref: datetime = DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db: Optional[Session] = None,
        logger: Optional[Logger] = None,
        token: Optional[str] = None,
        list_slugs: Optional[List[str]] = None
    ) -> None:
        super().__init__(
            dict_metadata=YAML_YAHII_OHTERS,
            session=session,
            dt_ref=dt_ref,
            cls_db=cls_db,
            logger=logger,
            token=token,
            list_slugs=list_slugs, 
            int_delay_seconds=int_delay_seconds,
        )
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.token = token
        self.list_slugs = list_slugs
        self.int_delay_seconds = int_delay_seconds
        self.year_yy = self.dt_ref.strftime("%y")

    def td_th_parser(self, list_td: List[Any], list_headers: List[str], source: str) \
        -> pd.DataFrame:
        if source == "min_wage":
            for i, td in enumerate(list_td):
                if "DECRETO" not in td and "$" in td:
                    list_td[i] = float(td.replace("NCz$ ", "").replace("NCr$ ", "")\
                        .replace("CR$", "").replace("R$", "").replace("Cr$", "").replace("NCz$", "")\
                        .replace("$000", "").replace(".", "")\
                        .replace(",", ".").replace("Cz$ ", "").strip())
                elif "DECRETO" not in td and len(td) >= 8 and "." in td:
                    list_td[i] = td.replace(" (Abono)", "").replace(" (URV) – (Real)", "")\
                        .replace(" (Cruzeiro Real)", "").replace(" (Cruzeiro)", "")\
                        .replace(" (Cruzado)", "").replace(" (Cruzeiro Novo)", "")\
                        .replace(" (Cruzado Novo)", "").replace(" (Réis)", "")
            list_ser = HandlingDicts().pair_headers_with_data(
                list_headers,
                list_td
            )
            df_ = pd.DataFrame(list_ser)
            df_ = df_[df_["DATA"] != "REVOGADA"]
            df_["DATA"] = [DatesBR().str_date_to_datetime(d, "DD.MM.YY") for d in df_["DATA"]]
            df_ = df_[(df_["DATA"] >= DatesBR().build_date(2000, 1, 1)) & (df_["DATA"] <= self.dt_ref)]
        elif source == "inss_contribution":
            list_td = [
                x.replace("\r\n                 ", "").strip().replace("%", "")\
                    .replace(",01,01", "").replace("Até  4.190,84 ", "De 4.190,84 ").strip()\
                for x in list_td
            ]
            list_td = [
                float(x.replace(",", ".")) / 100.0 if NumHandler().is_numeric(x.replace(",", ".")) \
                else x for x in list_td
            ]
            list_ser = HandlingDicts().pair_headers_with_data(
                list_headers,
                list_td
            )
            df_ = pd.DataFrame(list_ser)
            df_["SALARIO_INF"] = [
                StrHandler().get_between(StrHandler().remove_diacritics(x.lower()), "de", "ate")\
                    .strip() if "de " in x.lower() and "acima de " not in x.lower() else \
                StrHandler().get_after(StrHandler().remove_diacritics(x.lower().strip()), "acima de ")\
                    .strip() if "acima de " in StrHandler().remove_diacritics(x.lower().strip()) \
                    else 0.0 for x in df_["SALARIO_CONTRIBUICAO"]
            ]
            df_["SALARIO_SUP"] = [
                StrHandler().get_after(StrHandler().remove_diacritics(x.lower()), "ate ")\
                    .strip() if "ate " in StrHandler().remove_diacritics(x.lower()) else 0.0 \
                for x in df_["SALARIO_CONTRIBUICAO"]
            ]
            df_["SALARIO_INF"] = pd.to_numeric(
                df_["SALARIO_INF"].str.replace(".", "").str.replace(",", "."), 
                errors="coerce"
            )
            df_["SALARIO_SUP"] = pd.to_numeric(
                df_["SALARIO_SUP"].str.replace(".", "").str.replace(",", "."), 
                errors="coerce"
            )
            df_["SALARIO_INF"] = df_["SALARIO_INF"].fillna(0.0)
            df_["SALARIO_SUP"] = df_["SALARIO_SUP"].fillna(1_000_000_000)
        elif source in ["daily_usdbrl", "daily_eurbrl"]:
            list_td = [x.replace(",", ".") for x in list_td \
                       if (StrHandler().match_string_like(x, "*/*/*") and len(x) == 10) \
                        or "," in x or "." in x \
                        or (StrHandler().match_string_like(x, "*/*") and len(x) == 9 \
                            and StrHandler().has_no_letters(x))]
            list_ser = HandlingDicts().pair_headers_with_data(
                list_headers,
                list_td
            )
            df_ = pd.DataFrame(list_ser)
        return df_

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        root = HtmlHandler().lxml_parser(resp_req)
        source = self.get_query_params(resp_req.url, "source").lower()
        list_td = [
            x.text.strip() for x in HtmlHandler().lxml_xpath(
                root, YAML_YAHII_OHTERS[source]["xpaths"]["list_td"]
            )
            if x.text is not None
        ]
        if source == "min_wage":
            list_headers = ["DISPOSITIVO_LEGAL", "DATA", "VALOR"]
        elif source == "inss_contribution":
            list_headers = ["SALARIO_CONTRIBUICAO", "ALIQUOTA_RECOLHIMENTO_INSS"]
        elif source in ["daily_usdbrl", "daily_eurbrl"]:
            list_headers = ["DATA", "COMPRA", "VENDA"]
        else:
            raise ValueError(f"Source {source} not implemented.")
        return self.td_th_parser(list_td, list_headers, source)