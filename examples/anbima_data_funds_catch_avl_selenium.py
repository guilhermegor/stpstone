import sys
import pandas as pd
from keyring import get_password
from random import choice, randint
from time import sleep
from getpass import getuser
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from urllib3.exceptions import ReadTimeoutError
from stpstone.utils.webdriver_tools.selenium_wd import SeleniumWD
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy
from stpstone.utils.cals.handling_dates import DatesBR


# setting variables
url_anbima_data_funds = 'https://data.anbima.com.br/busca/fundos?size=100&page={}'
xpath_fund_infos = '//*[@id="__next"]/main/div/div/div/div/ul/li[{}]/article/a'
xpath_ein_number = './div/div[1]/div/p[4]'
int_max_retries = 100
list_ser = list()

# getting proxy
cls_session = YieldFreeProxy(
    bl_new_proxy=True,
    float_min_ratio_times_alive_dead=0.01,
    float_max_timeout=1000
)
# list_ser_proxies = cls_session.get_proxies
list_ser_proxies = None
if list_ser_proxies is not None:
    dict_proxy = list_ser_proxies.pop(list_ser_proxies.index(choice(list_ser_proxies)))
else:
    dict_proxy = None
if dict_proxy is not None:
    str_proxy = f'{dict_proxy["ip"]}:{dict_proxy["port"]}'
else:
    str_proxy = None

# looping within anbima data pages
for i in range(450, 506):
    url = url_anbima_data_funds.format(i)
    print('{} - #{}/{} PG ANBIMA DATA FUNDS: {}'.format(DatesBR().current_timestamp_string, i, 505, url))
    print('PROXY: {}'.format(str_proxy))
    cls_selenium = SeleniumWD(
        url=url,
        path_webdriver=get_password('CHROMEDRIVER', 'PATH'),
        int_port=get_password('CHROMEDRIVER', 'PORT'),
        int_wait_load=120,
        str_proxy=str_proxy
    )
    web_driver = cls_selenium.web_driver
    bl_retry = True
    i_loop = int_max_retries
    #   loop within cards - funds available
    for i_fund_info in range(1, 101):
        while \
            (bl_retry == True) \
            and (i_loop > 0):
            try:
                a_fund_infos = cls_selenium.find_element(
                    web_driver,
                    xpath_fund_infos.format(i_fund_info)
                )
                str_anbima_fund_info = a_fund_infos.get_attribute('href')
                bl_retry = False
            except (NoSuchElementException, WebDriverException, ReadTimeoutError):
                sleep(randint(10, 60))
                if list_ser_proxies is not None:
                    dict_proxy = list_ser_proxies.pop(list_ser_proxies.index(choice(list_ser_proxies)))
                else:
                    dict_proxy = None
                if dict_proxy is not None:
                    str_proxy = f'{dict_proxy["ip"]}:{dict_proxy["port"]}'
                else:
                    str_proxy = None
                print('PROXY: {}'.format(str_proxy))
                cls_selenium = SeleniumWD(
                    url=url,
                    path_webdriver=get_password('CHROMEDRIVER', 'PATH'),
                    int_port=get_password('CHROMEDRIVER', 'PORT'),
                    int_wait_load=120,
                    str_proxy=str_proxy
                )
                web_driver = cls_selenium.web_driver
            i_loop -= 1
        if bl_retry == True:
            raise Exception('MAX RETRIES EXCEEDED')
        p_ein_number = cls_selenium.find_element(a_fund_infos, xpath_ein_number)
        str_ein_number = p_ein_number.text
        list_ser.append({
            'EIN_NUMBER': str_ein_number,
            'URL_ANBIMA_FUND': str_anbima_fund_info
        })
        bl_retry = True
    df_ = pd.DataFrame(list_ser)
    df_.to_csv(
        "data/anbima-avl-funds-pg-{}_{}_{}_{}.csv".format(
            i,
            getuser(),
            DatesBR().curr_date.strftime('%Y%m%d'),
            DatesBR().curr_time.strftime('%H%M%S')
        ),
        index=False
    )
