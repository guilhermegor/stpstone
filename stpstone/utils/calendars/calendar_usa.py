import pandas as pd

from stpstone.utils.calendars.calendar_abc import ABCCalendarOperations
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


class DatesUSANasdaq(ABCCalendarOperations):

    def get_holidays_raw(self) -> pd.DataFrame:
        url = "https://nasdaqtrader.com/trader.aspx?id=Calendar"
        scraper = PlaywrightScraper(
            bool_headless=False, 
            bool_incognito=True
        )
        with scraper.launch():
            if scraper.navigate(url):
                list_td = scraper.get_list_data(
                    table_selector='//*[@id="leftWideCOL"]/div/table/tbody[2]/tr/td',
                    selector_type="xpath"
                )
            