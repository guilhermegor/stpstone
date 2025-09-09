"""Implementation of ingestion instance."""

from abc import abstractmethod
from datetime import date
from io import BytesIO, StringIO
from logging import Logger
from typing import Optional, Union

import backoff
from bs4 import BeautifulSoup
import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import requests
from requests import Response, Session
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.abc.ingestion_abc import (
    ABCIngestionOperations,
    ContentParser,
    CoreIngestion,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.str import TypeCaseFrom, TypeCaseTo
from stpstone.utils.parsers.xml import XMLFiles


class ABCB3SearchByTradingSession(ABCIngestionOperations):
    """Ingestion concrete class."""
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        url: str = "FILL_ME"
    ) -> None:
        """Initialize the ingestion class.
        
        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference, by default None.
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
            The database session, by default None.
        
        Returns
        -------
        None
        """
        super().__init__(cls_db=cls_db)
        CoreIngestion.__init__(self)
        ContentParser.__init__(self)

        self.logger = logger
        self.cls_db = cls_db
        self.cls_dir_files_management = DirFilesManagement()
        self.cls_dates_current = DatesCurrent()
        self.cls_create_log = CreateLog()
        self.cls_dates_br = DatesBRAnbima()
        self.cls_xml_handler = XMLFiles()
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.url = url.format(self.date_ref.strftime("%y%m%d"))

    @backoff.on_exception(
        backoff.expo, 
        requests.exceptions.HTTPError, 
        max_time=60
    )
    def get_response(
        self, 
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0), 
        bool_verify: bool = True
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Return a list of response objects.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Verify the SSL certificate, by default True
        
        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            A list of response objects.
        """
        resp_req = requests.get(
            self.url, 
            timeout=timeout, 
            verify=bool_verify,
        )
        resp_req.raise_for_status()
        return resp_req
    
    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> StringIO:
        """Parse the raw file content.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
            The parsed content.
        """
        return self.cls_dir_files_management.recursive_unzip_in_memory(
            BytesIO(resp_req.content))[0]
    
    @abstractmethod
    def transform_data(
        self, 
        file: StringIO
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The parsed content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.

        Raises
        ------
        NotImplementedError
            If the method is not implemented in a subclass.
        """
        return pd.read_csv(file, sep=";")
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "COL_1": str,
            "COL_2": float, 
            "COL_3": int, 
            "COL_4": "date"
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        cols_from_case: Optional[TypeCaseFrom] = None,
        cols_to_case: Optional[TypeCaseTo] = None,
        str_table_name: str = "<COUNTRY>_<SOURCE>_<TABLE_NAME>"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "<COUNTRY>_<SOURCE>_<TABLE_NAME>"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
        file = self.parse_raw_file(resp_req)
        df_ = self.transform_data(file=file)
        df_ = self.standardize_dataframe(
            df_=df_, 
            date_ref=self.date_ref,
            dict_dtypes=dict_dtypes, 
            str_fmt_dt=str_fmt_dt,
            url=self.url,
            cols_from_case=cols_from_case, 
            cols_to_case=cols_to_case,
        )
        if self.cls_db:
            self.insert_table_db(
                cls_db=self.cls_db, 
                str_table_name=str_table_name, 
                df_=df_, 
                bool_insert_or_ignore=bool_insert_or_ignore
            )
        else:
            return df_


class B3StandardizedInstrumentGroups(ABCB3SearchByTradingSession):
    """B3 Standardized Instrument Groups."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=AI{}.zip"
        )

    def transform_data(self, file: StringIO) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return pd.read_csv(file, sep=";", skiprows=1, names=[
            "TIPO", 
            "ID_GRUPO_INSTRUMENTOS", 
            "ID_CAMARA", 
            "ID_INSTRUMENTO", 
            "ORIGEM_INSTRUMENTO",
        ])
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "TIPO": str,
            "ID_GRUPO_INSTRUMENTOS": str, 
            "ID_CAMARA": str, 
            "ID_INSTRUMENTO": str, 
            "ORIGEM_INSTRUMENTO": str
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_standardized_instrument_groups"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           str_table_name=str_table_name)


class B3IndexReport(ABCB3SearchByTradingSession):
    """B3 Index Report BVBG.087.01 IndexReport."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=IR{}.zip"
        )

    def transform_data(self, file: StringIO) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        soup_xml = self.cls_xml_handler.memory_parser(file)
        soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag="IndxInf")
        list_ser: list[dict[str, Union[str, int, float]]] = []

        for soup_parent in soup_node:
            dict_ = {}
            for tag in [
                "TckrSymb",
                "Id",
                "Prtry",
                "MktIdrCd",
                "OpngPric",
                "MinPric",
                "MaxPric",
                "TradAvrgPric",
                "PrvsDayClsgPric",
                "ClsgPric",
                "IndxVal",
                "OscnVal",
                "AsstDesc",
                "SttlmVal",
                "RsngShrsNb",
                "FlngShrsNb",
                "StblShrsNb",
            ]:
                soup_content_tag = soup_parent.find(tag)
                dict_[tag] = soup_content_tag.getText() if soup_content_tag else None
            for tag, attribute in [
                ("SttlmVal", "Ccy"),
            ]:
                soup_content_tag = soup_parent.find(tag)
                dict_[tag + attribute] = soup_content_tag.get(attribute) if soup_content_tag else None
            list_ser.append(dict_)

        return pd.DataFrame(list_ser)
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "TCKR_SYMB": str,
            "ID": str, 
            "PRTRY": str, 
            "MKT_IDR_CD": str, 
            "OPNG_PRIC": float, 
            "MIN_PRIC": float,
            "MAX_PRIC": float,
            "TRAD_AVRG_PRIC": float,
            "PRVS_DAY_CLSG_PRIC": float,
            "CLSG_PRIC": float,
            "INDX_VAL": float,
            "OSCN_VAL": float,
            "ASST_DESC": str,
            "STTLM_VAL": str,
            "STTLM_VAL_CCY": str,
            "RSNG_SHRS_NB": int,
            "FLNG_SHRS_NB": int,
            "STBL_SHRS_NB": int,
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        cols_from_case: str = "pascal",
        cols_to_case: str = "upper_constant",
        str_table_name: str = "br_b3_standardized_instrument_groups"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           cols_from_case=cols_from_case, cols_to_case=cols_to_case,
                           str_table_name=str_table_name)


class B3PriceReport(ABCB3SearchByTradingSession):
    """B3 Index Report BVBG.086.01 PriceReport."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=PR{}.zip"
        )

    def transform_data(self, file: StringIO) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        soup_xml = self.cls_xml_handler.memory_parser(file)
        soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag="PricRpt")
        list_ser: list[dict[str, Union[str, int, float]]] = []

        for soup_parent in soup_node:
            dict_ = {}
            for tag in [
                "Dt",
                "TckrSymb",
                "Id",
                "Prtry",
                "MktIdrCd",
                "TradQty",
                "OpnIntrst",
                "FinInstrmQty",
                "OscnPctg",
                "NtlFinVol",
                "IntlFinVol",
                "BestBidPric",
                "BestAskPric",
                "FrstPric",
                "MinPric",
                "MaxPric",
                "TradAvrgPric",
                "LastPric",
                "VartnPts",
                "MaxTradLmt",
                "MinTradLmt",
            ]:
                soup_content_tag = soup_parent.find(tag)
                dict_[tag] = soup_content_tag.getText() if soup_content_tag else None
            for tag, attribute in [
                ("NtlFinVol", "Ccy"),
                ("IntlFinVol", "Ccy"),
                ("BestBidPric", "Ccy"),
                ("BestAskPric", "Ccy"),
                ("FrstPric", "Ccy"),
                ("MinPric", "Ccy"),
                ("MaxPric", "Ccy"),
                ("TradAvrgPric", "Ccy"),
                ("LastPric", "Ccy"),
                ("NtlRglrVol", "Ccy"),
                ("IntlRglrVol", "Ccy"),
            ]:
                soup_content_tag = soup_parent.find(tag)
                dict_[tag + attribute] = soup_content_tag.get(attribute) if soup_content_tag else None
            list_ser.append(dict_)

        return pd.DataFrame(list_ser)
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "DT": "date",
            "TCKR_SYMB": str,
            "ID": str, 
            "PRTRY": str, 
            "MKT_IDR_CD": "category",
            "TRAD_QTY": int, 
            "OPN_INTRST": float, 
            "FIN_INSTRM_QTY": int, 
            "OSCN_PCTG": float, 
            "NTL_FIN_VOL": float, 
            "INTL_FIN_VOL": float, 
            "BEST_BID_PRIC": float, 
            "BEST_ASK_PRIC": float, 
            "FRST_PRIC": float, 
            "MIN_PRIC": float, 
            "MAX_PRIC": float, 
            "TRAD_AVRG_PRIC": float, 
            "LAST_PRIC": float, 
            "VARTN_PTS": float, 
            "MAX_TRAD_LMT": float, 
            "MIN_TRAD_LMT": float, 
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        cols_from_case: str = "pascal",
        cols_to_case: str = "upper_constant",
        str_table_name: str = "br_b3_standardized_instrument_groups"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           cols_from_case=cols_from_case, cols_to_case=cols_to_case,
                           str_table_name=str_table_name)


class B3InstrumentsFile(ABCB3SearchByTradingSession):
    """B3 Instruments File BVBG.028.02 InstrumentsFile."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=PR{}.zip"
        )

    def transform_data(
        self, 
        file: StringIO, 
        tag_parent: str, 
        list_tags_children: list[str],
        list_tups_attributes: list[tuple[str, str]],
    ) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        soup_xml = self.cls_xml_handler.memory_parser(file)
        list_ser = self.get_node_info(
            soup_xml=soup_xml, 
            tag_parent=tag_parent, 
            list_tags_children=list_tags_children, 
            list_tups_attributes=list_tups_attributes
        )
        return pd.DataFrame(list_ser)
    
    def get_node_info(
        self, 
        soup_xml: BeautifulSoup, 
        tag_parent: str, 
        list_tags_children: list[str], 
        list_tups_attributes: list[tuple[str, str]]
    ) -> str:
        """Get node information from BeautifulSoup XML.
        
        Parameters
        ----------
        soup_xml : BeautifulSoup
            Parsed XML document
        tag_parent : str
            Parent tag name
        list_tags_children : list[str]
            List of child tags
        list_tups_attributes : list[tuple[str, str]]
            List of tuples containing tag name and attribute name
        
        Returns
        -------
        list[dict[str, Union[str, int, float]]]
            List of dictionaries containing node information
        """
        soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag=tag_parent)
        list_ser: list[dict[str, Union[str, int, float]]] = []

        for soup_parent in soup_node:
            dict_ = {}
            for tag in list_tags_children:
                soup_content_tag = soup_parent.find(tag)
                dict_[tag] = soup_content_tag.getText() if soup_content_tag else None
            for tag, attribute in list_tups_attributes:
                soup_content_tag = soup_parent.find(tag)
                dict_[tag + attribute] = soup_content_tag.get(attribute) if soup_content_tag \
                    else None
            list_ser.append(dict_)

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {},
        str_fmt_dt: str = "YYYY-MM-DD",
        cols_from_case: str = "pascal",
        cols_to_case: str = "upper_constant",
        str_table_name: str = "<COUNTRY>_<SOURCE>_<TABLE_NAME>"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           cols_from_case=cols_from_case, cols_to_case=cols_to_case,
                           str_table_name=str_table_name)


class B3InstrumentsFileEqty(B3InstrumentsFile):
    """B3 Instruments File BVBG.028.02 InstrumentsFile (EqtyInf)."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=PR{}.zip"
        )

    def transform_data(self, file: StringIO) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return super().transform_data(
            file=file, 
            tag_parent="EqtyInf", 
            list_tags_children=[
                "SctyCtgy", 
                "ISIN", 
                "DstrbtnId", 
                "CFICd",
                "SpcfctnCd", 
                "CrpnNm", 
                "TckrSymb", 
                "PmtTp", 
                "AllcnRndLot", 
                "PricFctr", 
                "TradgStartDt", 
                "TradgEndDt", 
                "CorpActnStartDt", 
                "EXDstrbtnNb",
                "CtdyTrtmntTp", 
                "TradgCcy", 
                "MktCptlstn", 
                "LastPric", 
                "FirstPric", 
                "DaysToSttlm", 
                "RghtsIssePric", 
                "AsstSubTp", 
                "AuctnTp"
            ], 
            list_tups_attributes=[
                ("MktCptlstn", "Ccy"),
                ("LastPric", "Ccy"),
                ("FirstPric", "Ccy"),
                ("RghtsIssePric", "Ccy"),
            ]
        )

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "SCTY_CTGY": str,
            "ISIN": str,
            "DSTRBTN_ID": str,
            "CFI_CD": str,
            "SPCFCTN_CD": str,
            "CRPN_NM": str,
            "TCKR_SYMB": str,
            "PMT_TP": str,
            "ALLCN_RND_LOT": int,
            "PRIC_FCTR": float,
            "TRADG_START_DT": str,
            "TRADG_END_DT": str,
            "CORP_ACTN_START_DT": str,
            "EXDSTRBTN_NB": int,
            "CTDY_TRTMNT_TP": str,
            "TRADG_CCY": str,
            "MKT_CPTLSTN": str,
            "LAST_PRIC": float,
            "FIRST_PRIC": float,
            "DAYS_TO_STTLM": int,
            "RGHTS_ISSE_PRIC": float,
            "ASST_SUB_TP": str,
            "AUCTN_TP": str, 
            "MKT_CPTLSTN_CCY": str,
            "LAST_PRIC_CCY": str,
            "FIRST_PRIC_CCY": str,
            "RGHTS_ISSE_PRIC_CCY": str,
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        cols_from_case: str = "pascal",
        cols_to_case: str = "upper_constant",
        str_table_name: str = "br_b3_instruments_file_eqty"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           cols_from_case=cols_from_case, cols_to_case=cols_to_case,
                           str_table_name=str_table_name)
    

class B3InstrumentsFileOptnOnEqts(B3InstrumentsFile):
    """B3 Instruments File BVBG.028.02 InstrumentsFile (OptnOnEqts)."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=PR{}.zip"
        )

    def transform_data(self, file: StringIO) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return super().transform_data(
            file=file, 
            tag_parent="OptnOnEqtsInf", 
            list_tags_children=[
                "SctyCtgy", 
                "ISIN", 
                "DstrbtnId", 
                "CFICd",
                "TckrSymb",
                "ExrcPric",
                "OptnStyle",
                "XprtnDt",
                "OptnTp",
                "SrsTp",
                "PrtcnFlg",
                "PrmUpfrntInd",
                "TradgStartDt",
                "TradgEndDt",
                "PmtTp",
                "AllcnRndLot",
                "PricFctr", 
                "TradgCcy", 
                "DaysToSttlm",
                "DlvryTp",
                "AutomtcExrcInd",
            ], 
            list_tups_attributes=[
                ("ExrcPric", "Ccy"),
            ]
        )

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "SCTY_CTGY": str,
            "ISIN": str,
            "DSTRBTN_ID": str,
            "CFI_CD": str,
            "TCKR_SYMB": str,
            "EXRC_PRIC": float,
            "OPTN_STYLE": str,
            "XPRTN_DT": "date",
            "OPTN_TP": str,
            "SRS_TP": str,
            "PRTCN_FLG": str,
            "PRM_UPFRNT_IND": str,
            "TRADG_START_DT": "date",
            "TRADG_END_DT": "date",
            "PMT_TP": str,
            "ALLCN_RND_LOT": int,
            "PRIC_FCTR": int,
            "TRADG_CCY": str,
            "DAYS_TO_STTLM": int,
            "DLVRY_TP": str,
            "AUTOMTC_EXRC_IND": str,
            "EXRC_PRIC_CCY": str,
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        cols_from_case: str = "pascal",
        cols_to_case: str = "upper_constant",
        str_table_name: str = "br_b3_instruments_file_optn_on_eqts"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           cols_from_case=cols_from_case, cols_to_case=cols_to_case,
                           str_table_name=str_table_name)