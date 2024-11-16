### CVM WEB SERVICE - BRAZILLIAN SEC

import pandas as pd
import multiprocessing as mp
from requests import request
from getpass import getuser
from time import sleep
from stpstone.handling_data.html_parser import HtmlHndler
from stpstone.handling_data.dicts import HandlingDicts
from stpstone.cals.handling_dates import DatesBR
from stpstone.handling_data.str import StrHandler
from stpstone.loggs.create_logs import CreateLog
from stpstone.directories_files_manag.managing_ff import DirFilesManagement
from stpstone.handling_data.lists import HandlingLists
from stpstone.multithreading.mp_helper import mp_worker, mp_run_parallel


class CVMWeb_WS_Funds:

    def __init__(
        self,
        str_cookie:str,
        logger:object=None,
        bl_parallel:bool=False,
        int_sleep:object=1,
        int_ncpus:int=mp.cpu_count() - 2 if mp.cpu_count() > 2 else 1,
        bl_debug_mode:bool=False,
        key_fund_code:str='fund_code',
        key_fund_ein:str='fund_ein',
        key_fund_name:str='fund_name',
        key_ref_date:str='ref_date',
        key_total_portfolio:str='total_portfolio',
        key_aum:str='aum',
        key_quote:str='quote',
        key_fund_raising:str='fund_raising',
        key_redemptions:str='redemptions',
        key_provisioned_redemptions:str='provisioned_redemptions',
        key_liquid_assets:str='liquid_assets',
        key_num_shareholders:str='num_shareholders',
        fstr_greatest_shareholders:str='greatest_shareholder_{}',
        key_greatest_shareholders_like:str='greatest_shareholders_*',
        str_host_ex_fund:str=r'https://cvmweb.cvm.gov.br/SWB/Sistemas/SCW/CReservd/',
        str_host_post_fund:str=r'https://cvmweb.cvm.gov.br/SWB/Sistemas/ADM/CReservada/InfDiario/'
    ):
        '''
        DOCSTRING: CVM WEB SERVICE - BRAZILLIAN SEC INPUTS
        INPUTS: COOKIE CAN BE CATCH TRACKING NETWORK WHEN LOGGING WITH THE GOV.BR ACCOUNT IN 
            https://cvmweb.cvm.gov.br/swb/default.asp?sg_sistema=scw (SEARCH FOR ouvidor=0)
        OUTPUTS: - 
        '''
        self.str_cookie=str_cookie
        self.logger=logger
        self.bl_parallel=bl_parallel
        self.int_sleep=int_sleep
        self.int_ncpus=int_ncpus
        self.bl_debug_mode=bl_debug_mode
        self.dict_cookie={'Cookie': self.str_cookie}
        self.key_fund_code=key_fund_code
        self.key_fund_ein=key_fund_ein
        self.key_fund_name=key_fund_name
        self.key_ref_date=key_ref_date
        self.key_total_portfolio=key_total_portfolio
        self.key_aum=key_aum
        self.key_quote=key_quote
        self.key_fund_raising=key_fund_raising
        self.key_redemptions=key_redemptions
        self.key_provisioned_redemptions=key_provisioned_redemptions
        self.key_liquid_assets=key_liquid_assets
        self.key_num_shareholders=key_num_shareholders
        self.fstr_greatest_shareholders=fstr_greatest_shareholders
        self.key_greatest_shareholders_like=key_greatest_shareholders_like
        self.str_host_ex_fund=str_host_ex_fund
        self.str_host_post_fund=str_host_post_fund
    def generic_req(self, str_method:str, url:str, str_header_ref:str, dict_data:dict={}):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
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
        if self.bl_debug_mode == True:
            print('\nMETHOD: {} / URL: {} / HEADERS: {} / DATA: {} / COOKIE: {}'.format(
                str_method, url, dict_headers, dict_data, self.str_cookie))
        resp_req = request(
            method=str_method, 
            url=url, 
            headers=dict_headers, 
            data=dict_data, 
            cookies=self.dict_cookie
        )
        resp_req.raise_for_status()
        html_content = HtmlHndler().html_lxml_parser(page=resp_req.text)
        return html_content
    
    @property
    def available_funds(
        self, 
        str_header_ref:str='CReservd.asp',
        str_app:str='SelecPartic.aspx?CD_TP_INFORM=15',
        str_xpath_funds:str='//*[@id="PK_PARTIC"]/option',
        str_xpath_value:str='./@value',
        str_method:str='GET',
        list_ser=list()
    ):
        '''
        DOCSTRING: AVAILABLE FUND CODES AND EIN
        INPUTS: -
        OUTPUTS: DATAFRAME
        '''
        # request html
        html_content = self.generic_req(str_method, self.str_host_ex_fund + str_app, 
                                        str_header_ref)
        # looping within available funds and filling serialized list
        for el_option_fund in HtmlHndler().html_lxml_xpath(html_content, str_xpath_funds):
            #   get text and split into fund's ein and name
            try:
                str_option_fund_raw = HtmlHndler().html_lxml_xpath(el_option_fund, './text()')[0]
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
                if self.bl_debug_mode == True:
                    print('OPTION FUND: {}'.format(str_option_fund_trt))
                str_fund_ein, str_fund_name =  str_option_fund_trt.split(' - ')
            except IndexError:
                if self.bl_debug_mode == True:
                    print('ERROR: FUND EIN AND NAME NOT FOUND')
                continue
            except ValueError:
                if self.logger is not None:
                    CreateLog().errors(
                        self.logger, 
                        '** ERROR: FUND EIN AND NAME NOT FOUND \n\t' 
                        + '- COMMON ERROR IS AN UNEXPECTED "-" CHARACTER, PROVIDED THE CODE'
                        + ' EXPECTS ONLY ONE SEPARATOR FOR FUND EIN AND NAME \n\t'
                        + '- RAW DROP DOWN OPTION FUND: {} \n\t'.format(str_option_fund_raw)
                        + '- REPLACED OPTION FUND: {} \n\t'.format(str_option_fund_trt)
                        + '- URL CVMWEB: {}'.format(self.str_host_ex_fund + str_app)
                    )
                raise Exception(
                    '** ERROR: FUND EIN AND NAME NOT FOUND \n\t' 
                    + '- COMMON ERROR IS AN UNEXPECTED "-" CHARACTER, PROVIDED THE CODE'
                    + ' EXPECTS ONLY ONE SEPARATOR FOR FUND EIN AND NAME \n\t'
                    + '- RAW DROP DOWN OPTION FUND: {} \n\t'.format(str_option_fund_raw)
                    + '- REPLACED OPTION FUND: {} \n\t'.format(str_option_fund_trt)
                    + '- URL CVMWEB: {}'.format(self.str_host_ex_fund + str_app)
                )
            if self.bl_debug_mode == True:
                print(f'FUND EIN: {str_fund_ein} / FUND NAME: {str_fund_name}')
            #   appending to serialized list
            list_ser.append({
                self.key_fund_code: HtmlHndler().html_lxml_xpath(el_option_fund, str_xpath_value)[0],
                self.key_fund_ein: str_fund_ein,
                self.key_fund_name: str_fund_name
            })
        # creating pandas dataframe
        df_funds = pd.DataFrame(list_ser)
        # changing columns types
        df_funds = df_funds.astype({
            self.key_fund_code: str,
            self.key_fund_ein: str,
            self.key_fund_name: str
        })
        # returning dataframe
        return df_funds

    def fund_daily_report_gen(
        self,
        str_fund_code:str,
        dict_data:dict={},
        str_header_ref:str='SelecPartic.aspx?CD_TP_INFORM=15',
        str_app:str='ConsInfDiario.aspx?PK_PARTIC={}',
        str_method:str='GET'
    ):
        '''
        DOCSTRING: GENERAL FUND DAY REPORT WITH THE MOST RECENT DATE
        INPUTS: FUND CODE
        OUTPUTS: HTML
        '''
        # return html content
        return self.generic_req(
            str_method, 
            self.str_host_post_fund + str_app.format(str_fund_code), 
            str_header_ref, dict_data
        )

    def available_dates_report_fund(
        self,
        html_content,
        xpath_avl_dates:str='//*[@id="ddCOMPTC"]/option/text()'
    ):
        '''
        DOCSTRING: AVAILABLE DATES FOR FUND DAY REPORT
        INPUTS: FUND CODE
        OUTPUTS: LIST
        '''
        return [
            el_ 
            for el_ in HtmlHndler().html_lxml_xpath(html_content, xpath_avl_dates) 
            if el_ != ''
        ]

    def fund_daily_reports_raw(
        self, 
        html_content_gen:str,
        str_fund_code:str,
        list_str_dts:list,
        list_event_inputs_hidden:list=[
            '__EVENTTARGET', 
            '__EVENTARGUMENT', 
            '__LASTFOCUS', 
            '__VIEWSTATE', 
            '__VIEWSTATEGENERATOR', 
            '__EVENTVALIDATION'
        ],
        str_xpath_event_el_like:str='//*[@id="__{}"]/@value',
        str_event_target:str='ddCOMPTC',
        str_header_ref:str='SelecPartic.aspx?CD_TP_INFORM=15',
        str_app:str='ConsInfDiario.aspx?PK_PARTIC={}',
        str_method_fund_report_dt:str='POST',
        dict_html_contents_dts:dict=dict()
    ):
        '''
        DOCSTRING: SIMULATION OF JAVASCRIPT CODE EXECUTION WHEN THE REFERENCE DATE IS CHANGED IN THE 
            DROP DOWN MENU, IN ORDER TO RETRIEVE THE FUND DAILY REPORTS FOR DATES OF INTEREST
        INPUTS: 
        OUTPUTS:
        '''
        # looping within list of dates
        for str_dt in list_str_dts:
            #   build data dictionary
            dict_data = {
                k: HtmlHndler().html_lxml_xpath(html_content_gen, str_xpath_event_el_like.format(k)) 
                for k in list_event_inputs_hidden
            }
            dict_data[str_event_target] = str_dt
            #   request html for a specific date of fund report and serialize it
            dict_html_contents_dts[str_dt] = self.generic_req(
                str_method_fund_report_dt, 
                self.str_host_post_fund + str_app.format(str_fund_code), 
                str_header_ref, 
                dict_data
            )
        # return html contents
        return dict_html_contents_dts

    def fund_daily_report_trt(
        self, 
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
        list_ser=list()
    ):
        # daily reports for a given set of fund and dates
        dict_html_contents_dts = self.fund_daily_reports_raw(
            html_content_gen,
            str_fund_code,
            list_str_dts
        )
        # looping within available funds and filling serialized list
        for str_dt, html_content_dt in dict_html_contents_dts.items():
            #   greatest shareholders and their holdings in the current fund
            dict_g_shareholders = {
                self.fstr_greatest_shareholders.format(i): el_ein[0] if el_ein is not None else ''
                for i, el_ein in enumerate(
                    HtmlHndler().html_lxml_xpath(html_content_dt, str_xpath_greatest_shareholders)
                )
            }
            #   daily infos
            dict_daily_infos = {
                self.__dict__[f'key_{k}']: str(
                    HtmlHndler().html_lxml_xpath(html_content_dt, v)[0]).replace(',', '.') 
                for k, v in dict_xpaths.items()
            }
            dict_daily_infos[self.key_fund_code] = str_fund_code
            dict_daily_infos[self.key_ref_date] = str_dt
            #   appending to exporting list
            list_ser.append(
                HandlingDicts().merge_n_dicts(dict_daily_infos, dict_g_shareholders)
            )
        # retuning dictionary
        return list_ser

    def block_fund_fetch(
        self,
        str_fund_code:str,
        dict_dts_funds:dict,
        fstr_dir_parent:object=None,
        fstr_name_bkp:object=None,
        str_code_version:str='dev'
    ):
        '''
        DOCSTRING: BLOCK FUND DAILY REPORTS FOR DATES OF INTEREST TO FETCH - PARALLELIZED
        INPUTS: FUND CODE, DATES OF INTEREST, DIRECTORY TO BACKUP, NAME OF BACKUP FILE, CODE VERSION
        OUTPUTS: LIST OF DICTIONARIES
        '''
        # generic request for the given fund code
        html_content_gen = self.fund_daily_report_gen(str_fund_code)
        if self.logger is not None:
            CreateLog().infos(
                self.logger, 
                'General request for fund - OK (this step is necessary in order to return '
                + 'the available dates for daily reports)'
            )
        # dates of daily report available
        list_str_dts_reports = self.available_dates_report_fund(html_content_gen)
        if self.logger is not None:
            CreateLog().infos(
                self.logger, 
                f'Available dates for fund - {list_str_dts_reports} - OK'
            )
        # filtering dates of interest with daily reports available
        list_str_dts_filtd = [
            d 
            for d in dict_dts_funds[str_fund_code] 
            if d in list_str_dts_reports
        ]
        if self.logger is not None:
            CreateLog().infos(
                self.logger, 
                f'Dates of interest for fund bounded by availables: {list_str_dts_filtd} - OK'
            )
        # retrieving available daily reports for the given funds, bounded by dates of interest
        list_ser = self.fund_daily_report_trt(
            html_content_gen, str_fund_code, list_str_dts_filtd)
        if self.logger is not None:
            CreateLog().infos(self.logger, 'Daily reports for fund code - OK')
        # backup in hard drive, if is user's will
        if \
            (fstr_dir_parent is not None) \
            and (fstr_name_bkp is not None):
            #   directory parent and complete path
            dir_parent = fstr_dir_parent.format(DatesBR().curr_date)
            name_bkp = fstr_name_bkp.format(
                str_code_version.lower(), 
                getuser(), 
                DatesBR().curr_date.strftime('%Y%m%d'), 
                DatesBR().curr_time.strftime('%H%M%S')
            )
            complete_path_bkp = dir_parent + name_bkp
            #   checking wether directory exists, if not create it and backup the file
            _ = DirFilesManagement().mk_new_directory(dir_parent)
            pd.DataFrame(list_ser).to_csv(
                complete_path_bkp,
                index=False
            )
            CreateLog().infos(self.logger, 'Backup for fund - OK')
        # wait for the next iteration, if is user's will
        if self.int_sleep is not None:
            sleep(self.int_sleep)
        # returning list of dictionaries
        return list_ser

    def funds_daily_reports_trt(
        self,
        list_str_funds_codes:list,
        dict_dts_funds:dict,
        fstr_dir_parent:object=None,
        fstr_name_bkp:object=None,
        str_code_version:str='dev',
        str_dt_fmt_1:str='YYYY-MM-DD',
        str_strftime_format:str='%d/%m/%Y',
        list_funds:list=list()
    ):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # check date format
        for str_fund_code, list_dts in dict_dts_funds.items():
            for i, str_dt in enumerate(list_dts):
                if DatesBR().check_date_datetime_format(str_dt) == True:
                    list_dts[i] = str_dt.strftime(str_dt_fmt_1)
                elif \
                    (StrHandler().match_string_like(str_dt, '*-*') == True) \
                    and (isinstance(str_dt, str) == True):
                    list_dts[i] = DatesBR().str_date_to_datetime(
                        str_dt, format=str_dt_fmt_1).strftime(str_strftime_format)
            dict_dts_funds[str_fund_code] = list_dts
        if self.logger is not None:
            CreateLog().infos(
                self.logger, 
                f'Dates of interest for every fund: {dict_dts_funds}'
            )
        #   parallelized fetch, if is user's will
        if self.bl_parallel == True:
            #   preparing task arguments
            list_task_args = [
                (
                    (str_fund_code, dict_dts_funds, fstr_dir_parent, fstr_name_bkp, str_code_version), 
                    {}
                )
                for str_fund_code in list_str_funds_codes
            ]
            #   executing tasks in parallel
            list_funds = mp_run_parallel(
                self.block_fund_fetch, 
                list_task_args, 
                int_ncpus=self.int_ncpus
            )
            if self.bl_debug_mode == True:
                print(f'LIST BEFORE FLATTENING: {list_funds}')
            #   flattening list
            list_funds = HandlingLists().flatten_list(list_funds)
        else:
            for str_fund_code in list_str_funds_codes:
                list_funds.extend(
                    self.block_fund_fetch(
                        str_fund_code,
                        dict_dts_funds,
                        fstr_dir_parent,
                        fstr_name_bkp,
                        str_code_version
                    )
                )            
        # appending to pandas dataframe
        df_funds_daily_reports = pd.DataFrame(list_funds)
        if self.bl_debug_mode == True:
            print('DF BEFORE CHANGING DATA TYPES: \n{}'.format(df_funds_daily_reports))
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
            self.key_num_shareholders: int
        })
        cols_greatest_shareholders = [
            c 
            for c in df_funds_daily_reports.columns 
            if StrHandler().match_string_like(c, self.key_greatest_shareholders_like) == True
        ]
        df_funds_daily_reports[cols_greatest_shareholders] = df_funds_daily_reports[
            cols_greatest_shareholders
        ].astype(str)
        # retuning dataframe
        return df_funds_daily_reports