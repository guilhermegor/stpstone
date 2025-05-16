import backoff
import re
from logging import Logger
from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Any
from requests import request, Response
from requests.exceptions import ReadTimeout, ConnectTimeout, ChunkedEncodingError, ConnectionError
from lxml import html
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.loggs.create_logs import CreateLog


class AnbimaDataUtils:

    def __init__(self, dict_metadata: Dict[str, str], cls_db: Optional[Any] = None,
                 bl_schema: bool = True, str_tbl_name: Optional[str] = None,
                 str_schema_name: Optional[str] = None, bl_insert_or_ignore: Optional[bool] = False,
                 logger: Optional[Logger] = None) -> None:
        self.dict_metadata  = dict_metadata
        self.cls_db = cls_db
        self.bl_schema = bl_schema
        self.str_tbl_name = str_tbl_name
        self.str_schema_name = str_schema_name
        self.bl_insert_or_ignore = bl_insert_or_ignore
        self.logger = logger

    def _log_info(self, message: str):
        if self.logger is not None:
            CreateLog().info(self.logger, message)
        else:
            print(f"INFO: {message}")

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

    def urls_funds_builder(
        self,
        int_lower_bound: int = 0,
        int_upper_bound: int = 1_000_000,
        int_step: int = 1,
        str_prefix: str = "C",
        int_length: int = 11,
        list_ids_ignore: Optional[List[str]] = None,
        fstr_url: str = "https://data.anbima.com.br/fundos/{}/indicadores"
    ) -> List[Dict[str, str]]:
        list_ser = list()
        for i in range(int_lower_bound, int_upper_bound, int_step):
            id_ = StrHandler().fill_zeros(str_prefix, i, int_length)
            if id_ in list_ids_ignore: continue
            url = fstr_url.format(id_)
            resp_req = request("GET", url, headers=self.get_property("headers"),
                               cookies=self.get_property("cookies"))
            list_ser.append({
                "COD_ANBIMA": id_,
                "URL": url,
                "STATUS_CODE": resp_req.status_code
            })
        if self.cls_db is not None:
            self.insert_db(list_ser)
        return list_ser


class AnbimaDataFetcher(AnbimaDataUtils):

    def __init__(self, resource: str, dict_metadata: Dict[str, Any], list_slugs: List[str],
                 str_bucket_name: str, session: YieldFreeProxy, client_s3: Any,
                 logger: Optional[Logger] = None) -> None:
        self.resource = resource
        self.dict_metadata = dict_metadata
        self.list_slugs = list_slugs
        self.str_bucket_name = str_bucket_name
        self.session = session
        self.client_s3 = client_s3
        self.logger = logger

    @backoff.on_exception(
        backoff.expo,
        (ReadTimeout, ConnectTimeout, ChunkedEncodingError, ConnectionError),
        max_tries=20,
        base=2,
        factor=2,
        max_value=1200
    )
    def _req_wo_session(self, url: str) -> Response:
        return request(
            "GET",
            url,
            verify=self.get_property("verify", self.resource),
            headers=self.get_property("headers", self.resource),
            params=self.get_property("params", self.resource),
            cookies=self.get_property("cookies", self.resource),
        )

    def _get_data(self, slug: str) -> Optional[html.HtmlElement]:
        app_ = StrHandler().fill_placeholders(
            self.dict_metadata[self.resource]["app"], {"slug": slug})
        url = self.dict_metadata["credentials"]["host"] + app_
        if self.session is None:
            resp_req = self._req_wo_session(url)
        else:
            resp_req = self.session.get(
                url,
                verify=self.get_property("verify", self.resource),
                headers=self.get_property("headers", self.resource),
                params=self.get_property("params", self.resource),
                cookies=self.get_property("cookies", self.resource),
            )
        if resp_req.status_code == 200:
            return None
        return HtmlHandler().lxml_parser(resp_req)

    @property
    def filtered_slugs(self) -> List[str]:
        list_slugs_stored = [str(x).replace(".html", "") for x in
                             self.client_s3.list_objects(self.str_bucket_name)]
        return [x for x in self.list_slugs if x not in list_slugs_stored]

    @property
    def store_s3_data(self) -> None:
        for slug in self.filtered_slugs:
            html_content = self._get_data(slug)
            if html_content is not None:
                html_str = html.tostring(html_content, encoding="unicode", pretty_print=True)
                html_bytes = html_str.encode("utf-8")
                blame_s3 = self.client_s3.put_object_from_bytes(
                    bucket_name=self.str_bucket_name,
                    object_name=f"{slug}.html",
                    data=html_bytes,
                    content_type="text/html"
                )
                self._log_info(f"Status Put Object - Bucket {self.str_bucket_name}: {blame_s3}")
            else:
                self._log_info(f"Empty content, process continued without put action - ID: {slug}")


class AnbimaDataTrt(ABC):

    def __init__(self, url: str, html_content: html.HtmlElement, xpath_script: str,
                 dict_re_patters: Dict[str, str]) -> None:
        self.url = url
        self.html_content = html_content
        self.xpath_script = xpath_script
        self.dict_re_patterns = dict_re_patters

    @property
    def get_re_matches(self) -> Dict[str, str]:
        dict_re_matches = dict()
        for el_ in HtmlHandler().lxml_xpath(self.html_content, self.xpath_script):
            for key, re_pattern in self.dict_re_patterns.items():
                if key not in dict_re_matches: dict_re_matches[key] = list()
                regex_match = re.search(re_pattern, el_)
                if regex_match is not None:
                    for i_match in range(0, len(regex_match.groups()) + 1):
                        regex_group = regex_match.group(i_match).replace('\n', ' ')
                        regex_group = re.sub(r'\s+', ' ', regex_group).strip()
                        regex_group = regex_group.replace('\\', '').replace(r'\"', '"')
                        if regex_group not in dict_re_matches[key]: \
                            dict_re_matches[key].append(regex_group)
            dict_re_matches["url"] = self.url
        return dict_re_matches

    @abstractmethod
    def parse_info(self):
        pass
