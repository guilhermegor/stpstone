from typing import Dict, Optional, Any, List, Union
from lxml import html
from stpstone.ingestion.abc.anbima_data_ws import AnbimaDataDecrypt, AnbimaDataFetcher, AnbimaDataTrt
from stpstone.utils.connections.netops.session import ReqSession
from stpstone.utils.parsers.object import HandlingObjects
from stpstone.utils.parsers.str import StrHandler


class FundsDecrypt(AnbimaDataDecrypt):

    def __init__(self, dict_metadata: Dict[str, str], cls_db: Optional[Any] = None,
                 bl_schema: bool = True, str_tbl_name: Optional[str] = None,
                 str_schema_name: Optional[str] = None,
                 bl_insert_or_ignore: Optional[bool] = False) -> None:
        self.dict_metadata  = dict_metadata
        self.cls_db = cls_db
        self.bl_schema = bl_schema
        self.str_tbl_name = str_tbl_name
        self.str_schema_name = str_schema_name
        self.bl_insert_or_ignore = bl_insert_or_ignore


class FundsFetcher(AnbimaDataFetcher):

    def __init__(self, resource: str, dict_metadata: Dict[str, Any], list_slugs: List[str],
                 str_bucket_name: str, session: ReqSession, client_nosql: Any) -> None:
        self.resource = resource
        self.dict_metadata = dict_metadata
        self.list_slugs = list_slugs
        self.str_bucket_name = str_bucket_name
        self.session = session
        self.client_nosql = client_nosql


class FundTrt(AnbimaDataTrt):

    def __init__(self, url: str, html_content: html.HtmlElement, xpath_script: str,
                 dict_re_patters: Dict[str, str]) -> None:
        self.url = url
        self.html_content = html_content
        self.xpath_script = xpath_script
        self.dict_re_patterns = dict_re_patters

    def _get_object(self, dict_re_matches, str_property: str,
                    dict_replacers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        try:
            dict_ = HandlingObjects().literal_eval_data(dict_re_matches[str_property][0])
        except (SyntaxError, IndexError):
            dict_ = HandlingObjects().literal_eval_data(
                StrHandler().replace_all(dict_re_matches[str_property][0], dict_replacers)
            )
        return dict_

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
            dict_[key] = self._get_object(dict_re_matches, key, dict_replacers.get(key))
        return dict_
