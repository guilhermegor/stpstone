import pandas as pd
from typing import Dict, Optional, Any, List, Union, Tuple
from lxml import html
from logging import Logger
from stpstone.ingestion.abc.anbima_data_ws import AnbimaDataDecrypt, AnbimaDataFetcher, AnbimaDataTrt
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.cals.handling_dates import DatesBR


class FundsDecrypt(AnbimaDataDecrypt):

    def __init__(self, dict_metadata: Dict[str, str], cls_db: Optional[Any] = None,
                 bl_schema: bool = True, str_tbl_name: Optional[str] = None,
                 str_schema_name: Optional[str] = None,
                 bl_insert_or_ignore: Optional[bool] = False,
                 logger: Optional[Logger] = None) -> None:
        self.dict_metadata  = dict_metadata
        self.cls_db = cls_db
        self.bl_schema = bl_schema
        self.str_tbl_name = str_tbl_name
        self.str_schema_name = str_schema_name
        self.bl_insert_or_ignore = bl_insert_or_ignore
        self.logger = logger


class FundsFetcher(AnbimaDataFetcher):

    def __init__(self, resource: str, dict_metadata: Dict[str, Any], list_slugs: List[str],
                 str_bucket_name: str, session: ProxyScrapeAll, client_s3: Any,
                 logger: Optional[Logger] = None) -> None:
        self.resource = resource
        self.dict_metadata = dict_metadata
        self.list_slugs = list_slugs
        self.str_bucket_name = str_bucket_name
        self.session = session
        self.client_s3 = client_s3
        self.logger = logger


class FundTrt(AnbimaDataTrt):

    def __init__(self, url: str, html_content: html.HtmlElement, xpath_script: str,
                 dict_re_patters: Dict[str, str]) -> None:
        self.url = url
        self.html_content = html_content
        self.xpath_script = xpath_script
        self.dict_re_patterns = dict_re_patters

    def _get_object(self, dict_re_matches: Dict[str, str], str_property: str,
                    dict_replacers: Optional[Dict[str, str]] = None) -> Any:
        if len(dict_re_matches[str_property]) == 0:
            return "N/A"
        if dict_replacers is not None:
            return StrHandler().replace_all(dict_re_matches[str_property][0], dict_replacers)
        else:
            return dict_re_matches[str_property][0]

    def _num_shareholders_safe_len(self, list_num_shareholders:Union[List[str], List[None]]) -> int:
        if \
            (len(list_num_shareholders) > 0) \
            and (list_num_shareholders is not None) \
            and (list_num_shareholders[0] is not None):
            return len(list_num_shareholders[0])
        else:
            return 1_000_000

    def parse_info(self, dict_replacers: Dict[str, Dict[str, str]]) -> List[Dict[str, str]]:
        dict_ = dict()
        dict_re_matches = self.get_re_matches
        if dict_re_matches is None: return None
        for key in dict_re_matches.keys():
            if dict_replacers is not None:
                dict_repl = dict_replacers.get(key, None)
            else:
                dict_repl = None
            dict_[key] = self._get_object(dict_re_matches, key, dict_repl)
        return dict_


class FundsConsolidated:

    def __init__(
        self,
        client_s3: Any,
        bucket_name: str,
        xpath_script: str,
        dict_re_patterns: Optional[Dict[str, str]] = None,
        dict_replacers: Optional[Dict[str, str]] = None
    ) -> None:
        self.client_s3 = client_s3
        self.bucket_name = bucket_name
        self.xpath_script = xpath_script
        self.dict_re_patterns = dict_re_patterns
        self.dict_replacers = dict_replacers
        self.fstr_url = "https://data.anbima.com.br/fundos/{}/indicadores"

    @property
    def funds_infos_ts(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        list_ser = list()
        list_objects_s3 = self.client_s3.list_objects(
            bucket_name=self.bucket_name, bl_include_version=False)
        for i, obj_name in enumerate(list_objects_s3):
            html_bytes = self.client_s3.get_object_as_bytes(
                bucket_name=self.bucket_name,
                object_name=obj_name
            )
            if html_bytes is None: continue
            html_str = html_bytes.decode('utf-8')
            html_content = html.fromstring(html_str)
            cls_fund_trt = FundTrt(
                url=self.fstr_url.format(obj_name.replace(".html", "")),
                html_content=html_content,
                xpath_script=self.xpath_script,
                dict_re_patters=self.dict_re_patterns
            )
            dict_ = cls_fund_trt.parse_info(self.dict_replacers)
            list_ser.append(dict_)
        df_ = pd.DataFrame(list_ser)
        return df_
