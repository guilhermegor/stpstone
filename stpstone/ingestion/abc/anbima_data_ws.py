import backoff
import re
from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Any
from requests import request, Response
from requests.exceptions import ReadTimeout, ConnectTimeout, ChunkedEncodingError
from lxml import html
from stpstone.utils.connections.netops.session import ReqSession
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.parsers.html import HtmlHndler


class AnbimaDataUtils:

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

    def get_property(self, property_: str, resource: Optional[str] = None) -> str:
        if resource is not None:
            return self.dict_metadata[resource].get(property_, None) \
                if self.dict_metadata[resource].get(property_, None) is not None \
                else self.dict_metadata["credentials"].get(property_, None)
        else:
            return self.dict_metadata["credentials"].get(property_, None)

    def insert_db(self, list_ser) -> None:
        if self.bl_schema == False:
            str_table_name = f"{self.str_schema_name}_{self.str_tbl_name}"
        self.cls_db.insert(
            list_ser,
            str_table_name=str_table_name,
            bl_insert_or_ignore=self.bl_insert_or_ignore,
        )


class AnbimaDataDecrypt(AnbimaDataUtils):

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

    def urls_builder(self, fstr_url: str, int_lower_bound: int, int_upper_bound: int, int_step: int,
                     str_prefix: str, int_length: int) -> str:
        list_ser = list()
        for i in range(int_lower_bound, int_upper_bound, int_step):
            id_ = StrHandler().fill_zeros(str_prefix, i, int_length)
            url = fstr_url.format(id_)
            req_resp = request("GET", headers=self.get_property("headers"),
                               cookies=self.get_property("cookies"))
            list_ser.append({
                "COD_ANBIMA": id_,
                "URL": url,
                "STATUS_CODE": req_resp.status_code
            })
        return list_ser


class AnbimaDataFetcher(AnbimaDataUtils):

    def __init__(self, resource: str, dict_metadata: Dict[str, Any], list_slugs: List[str],
                 str_bucket_name: str, session: ReqSession, client_nosql: Any) -> None:
        self.resource = resource
        self.dict_metadata = dict_metadata
        self.list_slugs = list_slugs
        self.str_bucket_name = str_bucket_name
        self.session = session
        self.client_nosql = client_nosql

    @backoff.on_exception(
        backoff.expo,
        (ReadTimeout, ConnectTimeout, ChunkedEncodingError),
        max_tries=20,
        base=2,
        factor=2,
        max_value=1200
    )
    def req_wo_session(self, url: str) -> Response:
        return request(
            "GET",
            url,
            verify=self.get_property("verify", self.resource),
            headers=self.get_property("headers", self.resource),
            params=self.get_property("params", self.resource),
            cookies=self.get_property("cookies", self.resource),
        )

    def get_data(self, slug: str) -> Optional[html.HtmlElement]:
        app_ = StrHandler().fill_placeholders(self.dict_metadata["app"], {"slug": slug})
        url = self.dict_metadata["host"] + app_
        if self.session is None:
            req_resp = self.req_wo_session(url)
        else:
            req_resp = self.session.get(
                url,
                verify=self.get_property("verify", self.resource),
                headers=self.get_property("headers", self.resource),
                params=self.get_property("params", self.resource),
                cookies=self.get_property("cookies", self.resource),
            )
        if req_resp.status_code == 200:
            return None
        return HtmlHndler().lxml_parser(req_resp)

    @property
    def filtered_slugs(self) -> List[str]:
        list_slugs_stored = self.client_nosql.list_objects(self.str_bucket_name)
        return [x for x in self.list_slugs if x not in list_slugs_stored]

    @property
    def store_s3_data(self) -> Optional[bool]:
        for slug in self.filtered_slugs:
            html_content = self.get_data(slug)
            if html_content is not None:
                html_str = html.tostring(html_content, encoding="unicode", pretty_print=True)
                html_bytes = html_str.encode("utf-8")
                blame_s3 = self.client_nosql.put_object_from_bytes(
                    bucket_name="html",
                    object_name=f"{slug}.html",
                    data=html_bytes,
                    content_type="text/html"
                )
                return blame_s3
            else:
                return None


class AnbimaDataTrt(ABC):

    def __init__(self, url: str, html_content: html.HtmlElement, xpath_script: str,
                 dict_re_patters: Dict[str, str]) -> None:
        self.url = url
        self.html_content = html_content
        self.xpath_script = xpath_script
        self.dict_re_patterns = dict_re_patters

    @property
    def get_re_matches(self) -> Dict[str, str]:
        dict_matches = dict()
        for el_ in HtmlHndler().lxml_parser(self.html_content, self.xpath_script):
            for key, re_pattern in self.dict_re_patterns.items():
                if key not in dict_matches: dict_matches[key] = list()
                regex_match = re.search(re_pattern, el_)
                if regex_match is not None:
                    for i_match in range(0, len(regex_match.groups()) + 1):
                        regex_group = regex_match.group(i_match).replace('\n', ' ')
                        regex_group = re.sub(r'\s+', ' ', regex_group).strip()
                        regex_group = regex_group.replace('\\', '').replace(r'\"', '"')
                        if regex_group not in dict_matches[key]: dict_matches[key].append(regex_group)
            dict_matches["url"] = self.url
        return dict_matches

    @abstractmethod
    def parse_info(self):
        pass
