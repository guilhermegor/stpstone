### CVM WEB SERVICE - BRAZILLIAN SEC

import backoff
import pandas as pd
import multiprocessing as mp
from requests import request
from requests.exceptions import ReadTimeout
from getpass import getuser
from time import sleep
from lxml import html
from typing import Tuple, Optional
from random import shuffle
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.lists import ListHandler
from stpstone.utils.pipelines.mp_helper import mp_worker, mp_run_parallel
from stpstone.transformations.validation.br_docs import DocumentsNumbersBR


class CVMWeb_WS_Funds:

    def __init__(
        self,
        str_cookie:Optional[str]=None,
        str_id:Optional[str]=None,
        str_passw:Optional[str]=None,
        webdriver_path:Optional[str]=None,
        port:Optional[str]=None,
        time_wait_page_load:Optional[int]=10,
        time_wait_el:Optional[int]=10,
        bl_open_minimized:Optional[bool]=True,
        bl_parallel:bool=False,
        bl_insert_or_ignore:bool=True,
        cls_db:type=None,
        int_sleep:object=1,
        int_ncpus:int=mp.cpu_count() - 2 if mp.cpu_count() > 2 else 1,
        key_fund_code:str='fund_code',
        key_fund_daily_infos_url:str='url',
        key_fund_ein:str='fund_ein',
        key_fund_name:str='fund_name',
        key_ref_date:str='db_ref_date',
        key_total_portfolio:str='total_portfolio',
        key_aum:str='aum',
        key_quote:str='quote',
        key_fund_raising:str='fund_raising',
        key_redemptions:str='redemptions',
        key_provisioned_redemptions:str='provisioned_redemptions',
        key_liquid_assets:str='liquid_assets',
        key_num_shareholders:str='num_shareholders',
        key_fund_ein_unm='fund_ein_unmasked',
        fstr_greatest_shareholders:str='greatest_shareholder_{}',
        key_greatest_shareholders_like:str='greatest_shareholders_*',
        str_host_ex_fund:str=r'https://cvmweb.cvm.gov.br/SWB/Sistemas/SCW/CReservd/',
        str_host_post_fund:str=r'https://cvmweb.cvm.gov.br/SWB/Sistemas/ADM/CReservada/InfDiario/'
    ):
        """
        DOCSTRING: CVM WEB SERVICE - BRAZILLIAN SEC INPUTS
        INPUTS: COOKIE CAN BE CATCH TRACKING NETWORK WHEN LOGGING WITH THE GOV.BR ACCOUNT IN
            https://cvmweb.cvm.gov.br/swb/default.asp?sg_sistema=scw (SEARCH FOR ouvidor=0)
        OUTPUTS: -
        """
        self.str_id = str_id
        self.str_passw = str_passw
        self.webdriver_path = webdriver_path
        self.port = port
        self.time_wait_page_load = time_wait_page_load
        self.time_wait_el = time_wait_el
        self.bl_open_minimized = bl_open_minimized
        self.str_cookie = str_cookie if str_cookie is not None else self.cookie_govbr(
            str_id,
            str_passw,
            webdriver_path,
            port,
            time_wait_page_load,
            time_wait_el,
            bl_open_minimized
        )
        self.bl_parallel = bl_parallel
        self.bl_insert_or_ignore = bl_insert_or_ignore
        self.cls_db = cls_db
        self.int_sleep = int_sleep
        self.int_ncpus = int_ncpus
        self.dict_cookie = {'Cookie': self.str_cookie}
        self.key_fund_code = key_fund_code
        self.key_fund_daily_infos_url = key_fund_daily_infos_url
        self.key_fund_ein = key_fund_ein
        self.key_fund_name = key_fund_name
        self.key_ref_date = key_ref_date
        self.key_total_portfolio = key_total_portfolio
        self.key_aum = key_aum
        self.key_quote = key_quote
        self.key_fund_raising = key_fund_raising
        self.key_redemptions = key_redemptions
        self.key_provisioned_redemptions = key_provisioned_redemptions
        self.key_liquid_assets = key_liquid_assets
        self.key_num_shareholders = key_num_shareholders
        self.key_fund_ein_unm = key_fund_ein_unm
        self.fstr_greatest_shareholders = fstr_greatest_shareholders
        self.key_greatest_shareholders_like = key_greatest_shareholders_like
        self.str_host_ex_fund = str_host_ex_fund
        self.str_host_post_fund = str_host_post_fund

    def cookie_govbr(self,
        str_id:str,
        str_passw:str,
        webdriver_path:str,
        port:str,
        time_wait_page_load:int=30,
        time_wait_el:int=30,
        bl_open_minimized:bool=False,
        url:str='https://cvmweb.cvm.gov.br/swb/default.asp?sg_sistema=scw',
        str_frame_name:str='Main',
        xpath_click_login:str='//*[@id="linkGovBr"]/img',
        xpath_fill_tin_br:str='//*[@id="accountId"]',
        xpath_fill_passw:str='//*[@id="password"]',
    ) -> str:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        cls_selenium = SeleniumWD()
        # create driver element, wait until page is loaded and the login button is available
        driver = cls_selenium.selenium_web_driver(url, webdriver_path, port, time_wait_page_load,
            bl_open_minimized)
        cls_selenium.selenium_element_is_clickable(driver, xpath_click_login,
            frame_name=str_frame_name)
        # click login button
        cls_selenium.selenium_find_element(driver, xpath_click_login).click()
        # fill TIN br - tax payer id number
        cls_selenium.selenium_wait_until_element_loaded(driver, xpath_fill_tin_br, time_wait_el)
        cls_selenium.selenium_find_element(driver, xpath_fill_tin_br).send_keys(str_id)
        # fill password
        cls_selenium.selenium_wait_until_element_loaded(driver, xpath_fill_passw, time_wait_el)
        cls_selenium.selenium_find_element(driver, xpath_fill_passw).send_keys(str_passw)
        # get cookie
        list_events = cls_selenium.get_network_traffic(driver)
        print(list_events)
        raise Exception('BREAK')

    def generic_req(self, str_method:str, url:str, str_header_ref:str, dict_data:dict={},
        bl_allow_redirects:bool=True, tup_timeout:Tuple[float, float]=(12.0, 21.0)) -> html.HtmlElement:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        dict_headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9,pt;q=0.8,es;q=0.7',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Cookie': f'{self.str_cookie}',
            'Referer': 'https://cvmweb.cvm.gov.br/SWB/Sistemas/SCW/CReservd/{}'.format(str_header_ref),
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
        resp_req = request(
            method=str_method,
            url=url,
            headers=dict_headers,
            data=dict_data,
            cookies=self.dict_cookie,
            allow_redirects=bl_allow_redirects,
            timeout=tup_timeout
        )
        resp_req.raise_for_status()
        html_content = HtmlHandler().lxml_parser(page=resp_req.text)
        return html_content

    @property
    @backoff.on_exception(
        backoff.constant,
        ReadTimeout,
        interval=10,
        max_tries=20,
    )
    def available_funds(
        self,
        str_table_nane:str='RAW_CVMWEB_COD_FUNDS',
        str_header_ref:str='CReservd.asp',
        str_app:str='SelecPartic.aspx?CD_TP_INFORM=15',
        str_xpath_funds:str='//select[@id="PK_PARTIC"]/option',
        str_xpath_value:str='./@value',
        str_method:str='GET',
        bl_allow_redirects:bool=True
    ) -> pd.DataFrame:
        """
        DOCSTRING: AVAILABLE FUND CODES AND CNPJ
        INPUTS: -
        OUTPUTS: DATAFRAME
        """
        # setting variables
        list_ser = list()
        # request html
        html_content = self.generic_req(str_method, self.str_host_ex_fund + str_app,
                                        str_header_ref, bl_allow_redirects=bl_allow_redirects)
        # print(f'HTML CONTENT: \n{html_content}')
        # print('OPTION FUND: {}'.format(HtmlHandler().html_lxml_xpath(html_content, '//select[@id="PK_PARTIC"]/option[3]/text()')))
        # looping within available funds and filling serialized list
        for el_option_fund in HtmlHandler().html_lxml_xpath(html_content, str_xpath_funds):
            #   get text and split into fund's cnpj and name
            try:
                str_option_fund_raw = HtmlHandler().html_lxml_xpath(el_option_fund, './text()')
                if len(str_option_fund_raw) == 0:
                    continue
                else:
                    str_option_fund_raw = str_option_fund_raw[0]
                str_option_fund_trt = StrHandler().remove_diacritics(str_option_fund_raw)
                str_option_fund_trt = StrHandler().replace_all(
                    str_option_fund_trt,
                    {
                        '- (CLASSE DE COTAS) -': '- (CLASSE DE COTAS)',
                        '- CREDITO PRIVADO': 'CREDITO PRIVADO',
                        '- RENDA FIXA -': 'RENDA FIXA',
                        'FUNDO DE INVESTIMENTO - RENDA FIXA': 'FUNDO DE INVESTIMENTO RENDA FIXA',
                        '- FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO': \
                            'FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO',
                        '- FGTS ': 'FGTS ',
                        '- CRED PRIV': 'CRED PRIV',
                        '- INVESTIMENTO NO EXTERIOR': 'INVESTIMENTO NO EXTERIOR',
                        '- FUNDO DE INVESTIMENTO EM ACOES': 'FUNDO DE INVESTIMENTO EM ACOES',
                        ' - IE': 'IE',
                        '- RESPONSABILIDADE LIMITADA': 'RESPONSABILIDADE LIMITADA',
                    }
                )
                str_fund_ein, str_fund_name =  str_option_fund_trt.split(' - ')
            except ValueError:
                raise Exception(
                    '** ERROR: FUND CNPJ AND NAME NOT FOUND \n\t'
                    + '- COMMON ERROR IS AN UNEXPECTED "-" CHARACTER, PROVIDED THE CODE'
                    + ' EXPECTS ONLY ONE SEPARATOR FOR FUND CNPJ AND NAME \n\t'
                    + '- raw DROP DOWN OPTION FUND: {} \n\t'.format(str_option_fund_raw)
                    + '- REPLACED OPTION FUND: {} \n\t'.format(str_option_fund_trt)
                    + '- URL CVMWEB: {}'.format(self.str_host_ex_fund + str_app)
                )
            #   backup in db, if is user's will
            list_ser.append({
                self.key_fund_code: HtmlHandler().html_lxml_xpath(
                    el_option_fund, str_xpath_value)[0],
                self.key_fund_ein: str_fund_ein,
                self.key_fund_name: str_fund_name,
            })
        # creating pandas dataframe
        df_funds = pd.DataFrame(list_ser)
        # changing columns types
        df_funds = df_funds.astype({
            self.key_fund_code: str,
            self.key_fund_ein: str,
            self.key_fund_name: str
        })
        # unmasking cnpj
        df_funds[self.key_fund_ein_unm] = DocumentsNumbersBR(
            df_funds[self.key_fund_ein].tolist()).unmask_docs
        # loading in db, if is user's will
        if self.cls_db is not None:
            self.cls_db.insert(
                df_funds.to_dict(orient='records'),
                str_table_nane,
                bl_insert_or_ignore=self.bl_insert_or_ignore
            )
        # returning dataframe
        return df_funds

    def fund_daily_report_gen(
        self,
        str_fund_code:str,
        dict_data:dict={},
        str_header_ref:str='SelecPartic.aspx?CD_TP_INFORM=15',
        str_app:str='ConsInfDiario.aspx?PK_PARTIC={}&PK_SUBCLASSE=-1',
        str_method:str='GET',
        bl_allow_redirects:bool=False,
    ) -> Tuple[html.HtmlElement, str]:
        """
        DOCSTRING: GENERAL FUND DAILY REPORT WITH THE MOST RECENT DATE
        INPUTS: FUND CODE
        OUTPUTS: HTML
        """
        # print(f'URL: {self.str_host_post_fund + str_app.format(str_fund_code)}')
        return \
            self.generic_req(
                str_method,
                self.str_host_post_fund + str_app.format(str_fund_code),
                str_header_ref,
                dict_data,
                bl_allow_redirects=bl_allow_redirects,
            ), \
            self.str_host_post_fund + str_app.format(str_fund_code)

    def available_dates_report_fund(
        self,
        html_content:html.HtmlElement,
        str_fund_code:str,
        url_fund_daily_infos:str,
        str_table_nane:str='RAW_CVMWEB_DATAS_FUNDOS',
        str_dt_fmt:str='DD/MM/YYYY',
        xpath_avl_dates:str='//*[@id="ddCOMPTC"]/option/text()'
    ):
        """
        DOCSTRING: AVAILABLE DATES FOR FUND DAY REPORT
        INPUTS: FUND CODE
        OUTPUTS: LIST
        """
        list_dts = [
            el_
            for el_ in HtmlHandler().html_lxml_xpath(html_content, xpath_avl_dates)
            if el_ != ''
        ]
        # print(f'**\nFUND_CODE: {str_fund_code} / LIST_DATAS_CVM: {list_dts}')
        list_avl_dts_fund = [
            {
                self.key_fund_code: str_fund_code,
                self.key_ref_date: DatesBR().str_date_to_datetime(d, str_dt_fmt),
                self.key_fund_daily_infos_url: url_fund_daily_infos,
            } for d in list_dts
        ]
        if (self.cls_db is not None) \
            and (len(list_avl_dts_fund) > 0):
            self.cls_db.insert(
                list_avl_dts_fund,
                str_table_nane,
                bl_insert_or_ignore=self.bl_insert_or_ignore
            )
        return list_dts

    def fund_daily_reports_raw(
        self,
        html_content_gen:str,
        str_fund_code:str,
        list_str_dts:list,
        el_eventtarget='__EVENTTARGET',
        el_eventargument='__EVENTARGUMENT',
        el_lastfocus='__LASTFOCUS',
        el_viewstate='__VIEWSTATE',
        el_eventvalidation='__EVENTVALIDATION',
        el_viewstategen='__VIEWSTATEGENERATOR',
        str_xpath_event_el_like:str='//*[@id="{}"]/@value',
        str_event_target:str='ddCOMPTC',
        str_header_ref:str='SelecPartic.aspx?CD_TP_INFORM=15',
        str_app:str='ConsInfDiario.aspx?PK_PARTIC={}&PK_SUBCLASSE=-1',
        str_method_fund_report_dt:str='POST',
        bl_allow_redirects:bool=False
    ):
        """
        DOCSTRING: SIMULATION OF JAVASCRIPT CODE EXECUTION WHEN THE REFERENCE DATE IS CHANGED IN THE
            DROP DOWN MENU, IN ORDER TO RETRIEVE THE FUND DAILY REPORTS FOR DATES OF INTEREST
        INPUTS:
        OUTPUTS:
        """
        # setting variables
        dict_html_contents_dts = dict()
        # looping within list of dates
        # print(f'LIST_STR_DTS:{list_str_dts}')
        for str_dt in list_str_dts:
            #   build data dictionary
            dict_data = {
                el_eventtarget: str_event_target,
                el_eventargument: '',
                el_lastfocus: '',
                el_viewstate: HtmlHandler().html_lxml_xpath(html_content_gen,
                    str_xpath_event_el_like.format(el_viewstate)),
                el_eventvalidation: HtmlHandler().html_lxml_xpath(html_content_gen,
                    str_xpath_event_el_like.format(el_eventvalidation)),
                el_viewstategen: HtmlHandler().html_lxml_xpath(html_content_gen, str_xpath_event_el_like.format(
                    el_viewstategen)),
                str_event_target: str_dt
            }
            # print(f'PARAMS FORM INFOS DIARIAS CVM: {dict_data}')
            # print(f'DICT_PARAMS_DAILY_INFOS: {dict_data}')
            #   request html for a specific date of fund report and serialize it
            # html_content = self.generic_req(
            #     str_method_fund_report_dt,
            #     self.str_host_post_fund + str_app.format(str_fund_code),
            #     str_header_ref,
            #     dict_data,
            #     bl_allow_redirects=bl_allow_redirects,
            # )
            resp_req = request(
                method=str_method_fund_report_dt,
                url=self.str_host_post_fund + str_app.format(str_fund_code),
                allow_redirects=False,
                data=dict_data,
                cookies=self.dict_cookie
            )
            resp_req.raise_for_status()
            html_content = HtmlHandler().lxml_parser(page=resp_req.text)
            # print(f'\nLIST_SER2_DAILY_INFOS: {list_ser_2}')
            dict_html_contents_dts[str_dt] = html_content
        # return html contents
        return dict_html_contents_dts

    def fund_daily_report_trt(
        self,
        url_daily_report_fund:str,
        html_content_gen:str,
        str_fund_code:str,
        list_str_dts:list,
        dict_xpaths:dict={
            'total_portfolio': '//*[@id="lblTotalCarteira"]/text()',
            'aum': '//*[@id="lblValorPL"]/text()',
            'quote': '///*[@id="lbVlrCota"]/text()',
            'fund_raising': '//*[@id="lblVlrCaptacoes"]/text()',
            'redemptions': '//*[@id="lblVlrResgates"]/text()',
            'provisioned_redemptions': '//*[@id="lblVlrTotalSaidas"]/text()',
            'liquid_assets': '//*[@id="lblValorTotalAtivos"]/text()',
            'num_shareholders': '//*[@id="lblNumCotistas"]/text()'
        },
        str_xpath_greatest_shareholders:str='//*[starts-with(@id, "lblCNPJPartic")]/text()',
        str_xpath_dropdown_box:str='//*[@id="ddCOMPTC"]/option[1]/@value',
        str_fmt_date:str='DD/MM/YYYY'
    ) -> list:
        # setting variables
        list_ser = list()
        # in case of no available dates, return an empty list
        # print('DROPDOWN_EL: {}'.format(
        #     len(HtmlHandler().html_lxml_xpath(html_content_gen,
        #         str_xpath_dropdown_box)) == 0
        # ))
        # print(f'URL: {url_daily_report_fund}')
        # raise Exception('BREAK')
        if (len(list_str_dts) == 0) \
            or (len(HtmlHandler().html_lxml_xpath(html_content_gen,
                str_xpath_dropdown_box)) == 0):
            return []
        # daily reports for a given set of fund and dates
        dict_html_contents_dts = self.fund_daily_reports_raw(
            html_content_gen,
            str_fund_code,
            list_str_dts
        )
        # print(f'HTML INFOS DIARIAS - DTS CONSULTAR: {dict_html_contents_dts}')
        # looping within available funds and filling serialized list
        for str_dt, html_content_dt in dict_html_contents_dts.items():
            #   greatest shareholders and their holdings in the current fund
            dict_g_shareholders = {
                self.fstr_greatest_shareholders.format(i): el_ein[0] if el_ein is not None else ''
                for i, el_ein in enumerate(
                    HtmlHandler().html_lxml_xpath(html_content_dt, str_xpath_greatest_shareholders)
                )
            }
            #   daily infos
            try:
                dict_daily_infos = {
                    self.__dict__[f'key_{k}']: str(
                        HtmlHandler().html_lxml_xpath(html_content_dt, v)[0]).replace(",", ".")
                    for k, v in dict_xpaths.items()
                }
            except IndexError:
                raise Exception(f'ERROR HTML XML XPATH - URL: {url_daily_report_fund}')
            dict_daily_infos[self.key_fund_code] = str_fund_code
            dict_daily_infos[self.key_fund_daily_infos_url] = url_daily_report_fund
            dict_daily_infos[self.key_ref_date] = DatesBR().str_date_to_datetime(str_dt, str_fmt_date)
            #   appending to exporting list
            dict_ = HandlingDicts().merge_n_dicts(dict_daily_infos, dict_g_shareholders)
            # print(f'DICT_DAILY_INFOS: {dict_}')
            list_ser.append(dict_)
        # retuning dictionary
        return list_ser

    def block_fund_fetch(
        self,
        str_fund_code:str,
        dict_dts_funds:dict,
        str_code_version:str='dev',
        str_table_nane:str='RAW_CVMWEB_INFOS_DIARIAS'
    ) -> list:
        """
        DOCSTRING: BLOCK FUND DAILY REPORTS FOR DATES OF INTEREST TO FETCH - PARALLELIZED
        INPUTS: FUND CODE, DATES OF INTEREST, DIRECTORY TO BACKUP, NAME OF BACKUP FILE, CODE VERSION
        OUTPUTS: LIST OF DICTIONARIES
        """
        # generic request for the given fund code
        html_content_gen, url_fund_daily_infos = self.fund_daily_report_gen(str_fund_code)
        # dates of daily report available
        list_str_dts_reports = self.available_dates_report_fund(html_content_gen,
            str_fund_code, url_fund_daily_infos)
        # filtering dates of interest with daily reports available
        list_ftd_dts = [
            d
            for d in dict_dts_funds[str_fund_code]
            if d in list_str_dts_reports
        ]
        # print(f'\n** FUND_CODE: {str_fund_code} / LIST_FLTD_DTS: {list_ftd_dts}')
        # retrieving available daily reports for the given funds, bounded by dates of interest
        list_ser = self.fund_daily_report_trt(
            url_fund_daily_infos, html_content_gen, str_fund_code, list_ftd_dts)
        # print(f'LIST_SER_CVMWEB_DAILY_INFOS: \n{list_ser}')
        # backup in db, if is user's will
        if (self.cls_db is not None) \
            and (len(list_ser) > 0):
            self.cls_db.insert(
                list_ser,
                str_table_nane,
                bl_insert_or_ignore=self.bl_insert_or_ignore
            )
        # wait for the next iteration, if is user's will
        if self.int_sleep is not None:
            sleep(self.int_sleep)
        # returning list of dictionaries
        return list_ser

    def funds_daily_reports_trt(
        self,
        dict_dts_funds:dict,
        str_code_version:str='dev',
        str_dt_fmt_1:str='YYYY-MM-DD',
        str_strftime_format:str='%d/%m/%Y',
    ) -> pd.DataFrame:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # setting variables
        list_funds = list()
        # check date format
        for str_fund_code, list_dts in dict_dts_funds.items():
            for i, str_dt in enumerate(list_dts):
                if DatesBR().check_date_datetime_format(str_dt) == True:
                    list_dts[i] = str_dt.strftime(str_dt_fmt_1)
                elif (StrHandler().match_string_like(str_dt, '*-*') == True) \
                    and (isinstance(str_dt, str) == True):
                    list_dts[i] = DatesBR().str_date_to_datetime(
                        str_dt, format=str_dt_fmt_1).strftime(str_strftime_format)
            dict_dts_funds[str_fund_code] = list_dts
        #   parallelized fetch, if is user's will
        if self.bl_parallel == True:
            #   preparing task arguments
            list_task_args = [
                (
                    (str_fund_code, dict_dts_funds, str_code_version),
                    {}
                )
                for str_fund_code in list(dict_dts_funds.keys())
            ]
            #   executing tasks in parallel
            list_funds = mp_run_parallel(
                self.block_fund_fetch(
                    str_fund_code,
                    dict_dts_funds,
                    str_code_version
                ),
                list_task_args,
                int_ncpus=self.int_ncpus
            )
            #   flattening list
            list_funds = ListHandler().flatten_list(list_funds)
        else:
            # randomize fund codes
            list_fnds_cds = list(dict_dts_funds.keys())
            shuffle(list_fnds_cds)
            # loop within funds
            for str_fund_code in list_fnds_cds:
                list_funds.extend(
                    self.block_fund_fetch(
                        str_fund_code,
                        dict_dts_funds,
                        str_code_version
                    )
                )
        # appending to pandas dataframe
        df_funds_daily_reports = pd.DataFrame(list_funds)
        # print(df_funds_daily_reports)
        # changing data types
        df_funds_daily_reports = df_funds_daily_reports.astype({
            self.key_fund_code: str,
            self.key_total_portfolio: float,
            self.key_aum: float,
            self.key_quote: float,
            self.key_fund_raising: float,
            self.key_redemptions: float,
            self.key_provisioned_redemptions: float,
            self.key_liquid_assets: float,
            self.key_num_shareholders: int,
            self.key_fund_daily_infos_url: str
        })
        cols_greatest_shareholders = [
            c
            for c in df_funds_daily_reports.columns
            if StrHandler().match_string_like(c, self.key_greatest_shareholders_like) == True
        ]
        df_funds_daily_reports[cols_greatest_shareholders] = df_funds_daily_reports[
            cols_greatest_shareholders
        ].astype(str)
        # remove duplicated data
        df_funds_daily_reports.drop_duplicates(inplace=True)
        # retuning dataframe
        return df_funds_daily_reports
