"""Proxy management class for Spys.one proxy service.

This module provides a class for fetching and managing proxy information from
Spys.one's proxy list service, supporting country-specific proxy filtering.
"""

from logging import Logger
import re
from typing import Optional

from selenium.webdriver.remote.webelement import WebElement

from stpstone.utils.connections.netops.proxies.proxies_abc import (
    ABCSession,
    ReturnAvailableProxies,
)
from stpstone.utils.geography.geo_ww import WWGeography, WWTimezones
from stpstone.utils.parsers.numbers import NumHandler
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.webdriver_tools.selenium_wd import SeleniumWD


class SpysOneCountry(ABCSession):
    """Class for fetching proxies from Spys.one service."""

    def __init__(
        self,
        bool_new_proxy: bool = True,
        dict_proxies: Optional[dict[str, str]] = None,
        int_retries: int = 10,
        int_backoff_factor: int = 1,
        bool_alive: bool = True,
        list_anonymity_value: Optional[list[str]] = None,
        list_protocol: str = "http",
        str_continent_code: Optional[str] = None,
        str_country_code: Optional[str] = None,
        bool_ssl: Optional[bool] = None,
        float_min_ratio_times_alive_dead: Optional[float] = 0.02,
        float_max_timeout: Optional[float] = 600,
        bool_use_timer: bool = False,
        list_status_forcelist: Optional[list[int]] = None,
        logger: Optional[Logger] = None,
        int_wait_load_seconds: int = 10,
    ) -> None:
        """Initialize SpysOneCountry instance.

        Parameters
        ----------
        bool_new_proxy : bool
            Flag to use new proxy (default: True)
        dict_proxies : Optional[dict[str, str]]
            Existing proxies dictionary (default: None)
        int_retries : int
            Number of retries for requests (default: 10)
        int_backoff_factor : int
            Backoff factor for retries (default: 1)
        bool_alive : bool
            Flag to check proxy liveness (default: True)
        list_anonymity_value : list[str]
            Allowed anonymity levels (default: ["anonymous", "elite"])
        list_protocol : str
            Proxy protocol (default: "http")
        str_continent_code : Optional[str]
            Continent code filter (default: None)
        str_country_code : Optional[str]
            Country code filter (default: None)
        bool_ssl : Optional[bool]
            SSL requirement flag (default: None)
        float_min_ratio_times_alive_dead : Optional[float]
            Minimum alive/dead ratio (default: 0.02)
        float_max_timeout : Optional[float]
            Maximum timeout threshold (default: 600)
        bool_use_timer : bool
            Flag to use timer (default: False)
        list_status_forcelist : list[int]
            HTTP status codes to force retry (default: [429, 500, 502, 503, 504])
        logger : Optional[Logger]
            Logger instance (default: None)
        int_wait_load_seconds : int
            Seconds to wait for page load (default: 10)
        """
        super().__init__(
            bool_new_proxy=bool_new_proxy,
            dict_proxies=dict_proxies,
            int_retries=int_retries,
            int_backoff_factor=int_backoff_factor,
            bool_alive=bool_alive,
            list_anonymity_value=list_anonymity_value,
            list_protocol=list_protocol,
            str_continent_code=str_continent_code,
            str_country_code=str_country_code,
            bool_ssl=bool_ssl,
            float_min_ratio_times_alive_dead=float_min_ratio_times_alive_dead,
            float_max_timeout=float_max_timeout,
            bool_use_timer=bool_use_timer,
            list_status_forcelist=list_status_forcelist,
            logger=logger
        )
        self.int_wait_load_seconds = int_wait_load_seconds
        self.fstr_url = "https://spys.one/free-proxy-list/{}/"
        self.xpath_tr = '//tr[contains(@class, "spy1x")]'
        self.xpath_dd_anonimity = '//select[@name="xf1"]'
        self.xpath_dd_show = '//select[@name="xpp"]'
        self.xpath_ssl = '//select[@name="xf2"]'
        self.xpath_type = '//select[@name="xf5"]'
        self.ww_timezones = WWTimezones()
        self.ww_geography = WWGeography()
        self.cls_num_handler = NumHandler()
        self.str_handler = StrHandler()

    def _validate_country_code(self) -> None:
        """Validate that country code is set and valid.

        Raises
        ------
        ValueError
            If country code is not set or invalid
        """
        if not self.str_country_code:
            raise ValueError("Country code must be set")
        if len(self.str_country_code) != 2:
            raise ValueError("Country code must be 2 characters")

    def _available_proxies(self) -> list[ReturnAvailableProxies]:
        """Fetch and parse available proxies from Spys.one.

        Returns
        -------
        list[ReturnAvailableProxies]
            List of proxy dictionaries with detailed information

        Raises
        ------
        ValueError
            If country code is invalid or proxy parsing fails
        """
        self._validate_country_code()
        proxies = []
        cls_selenium_wd = SeleniumWD(
            url=self.fstr_url.format(self.str_country_code.upper()),
            bool_headless=True,
            bool_incognito=True,
            int_wait_load_seconds=self.int_wait_load_seconds
        )

        try:
            driver = cls_selenium_wd.get_web_driver()
            cls_selenium_wd.wait_until_el_loaded(self.xpath_tr)

            # configure proxy filters
            for xpath, option_xpath in [
                (self.xpath_dd_anonimity, './option[@value="1"]'),
                (self.xpath_dd_show, './/option[last()]'),
                (self.xpath_ssl, './/option[@value="1"]'),
                (self.xpath_type, './/option[@value="1"]'),
            ]:
                element = cls_selenium_wd.find_element(driver, xpath)
                option = cls_selenium_wd.find_element(element, option_xpath)
                option.click()
                cls_selenium_wd.wait(10)

            # parse proxy rows
            rows = cls_selenium_wd.find_elements(driver, self.xpath_tr)
            for row in rows:
                try:
                    proxy_data = self._parse_proxy_row(row, cls_selenium_wd)
                    proxies.append(proxy_data)
                except Exception as err:
                    if self.logger:
                        self.logger.warning(f"Skipping proxy row due to error: {err}")
                    continue

        finally:
            driver.quit()

        return proxies

    def _parse_proxy_row(
        self, 
        row: WebElement, 
        cls_selenium_wd: SeleniumWD
    ) -> ReturnAvailableProxies:
        """Parse a single proxy row from the table.

        Parameters
        ----------
        row : WebElement
            The table row element to parse
        cls_selenium_wd : SeleniumWD
            Selenium webdriver helper instance

        Returns
        -------
        ReturnAvailableProxies
            Dictionary with parsed proxy information

        Raises
        ------
        ValueError
            If required proxy data cannot be parsed
        """
        ip, port, protocol = self._parse_basic_proxy_info(row, cls_selenium_wd)

        anonymity = self._parse_anonymity(row, cls_selenium_wd)

        timeout, uptime, ts_check_date = \
            self._parse_performance_metrics(row, cls_selenium_wd)

        timezones, continent, continent_code, country_name, country_code, city, region = \
            self._parse_geographic_info(row, cls_selenium_wd)

        organization = cls_selenium_wd.find_element(row, './td[5]').text

        return {
            "protocol": protocol,
            "bool_alive": True,
            "status": "success",
            "alive_since": ts_check_date,
            "anonymity": anonymity,
            "average_timeout": timeout,
            "first_seen": ts_check_date,
            "ip_data": "",
            "ip_name": "",
            "timezone": timezones,
            "continent": continent,
            "continent_code": continent_code,
            "country": country_name,
            "country_code": country_code,
            "city": city,
            "district": region,
            "region_name": region,
            "zip": "",
            "bool_hosting": False,
            "isp": "",
            "latitude": 0.0,
            "longitude": 0.0,
            "organization": organization,
            "proxy": f"{ip}:{port}",
            "ip": ip,
            "port": port,
            "bool_ssl": True,
            "timeout": timeout,
            "times_alive": 0,
            "times_dead": 0,
            "ratio_times_alive_dead": 1.0,
            "uptime": uptime
        }

    def _parse_location(self, location_str: str) -> tuple[str, str, str]:
        """Parse location strings in format "BR São João da Ponte (Minas Gerais)".

        Parameters
        ----------
        location_str : str
            The location string to parse

        Returns
        -------
        tuple[str, str, str]
            Tuple containing (country_code, city, region)

        Raises
        ------
        ValueError
            If location string cannot be parsed
        """
        if len(location_str) == 2:
            return location_str, "N/A", "N/A"
        
        if not self.str_handler.match_string_like(location_str, "*(*"):
            pattern = r'(?i)^([a-z]{2})\s*([^*]+?)$'
            match = re.match(pattern, location_str.strip())
            if match:
                return match.group(1), match.group(2), "N/A"
            raise ValueError(f"Could not parse location string: {location_str}")
        
        pattern = r'(?i)^([a-z]{2})\s*([^*]+?)\(([^*]+?)\)'
        match = re.match(pattern, location_str.strip())
        if match:
            return match.group(1), match.group(2), match.group(3)
        
        raise ValueError(f"Could not parse location string: {location_str}")

    def _parse_basic_proxy_info(
        self, 
        row: WebElement, 
        cls_selenium_wd: SeleniumWD
    ) -> tuple[str, str, str]:
        """Extract basic proxy information (IP, port, protocol) from the row.

        Parameters
        ----------
        row : WebElement
            The table row element to parse
        cls_selenium_wd : SeleniumWD
            Selenium webdriver helper instance

        Returns
        -------
        tuple[str, str, str]
            Tuple containing (ip, port, protocol)

        Raises
        ------
        ValueError
            If IP:Port format is invalid
        """
        ip_port = cls_selenium_wd.find_element(row, './td[1]').text.split(":")
        if len(ip_port) != 2 or not self.cls_num_handler.is_numeric(ip_port[1]):
            raise ValueError(f"Invalid IP: Port format- {ip_port[1]}")
        
        ip, port = ip_port
        protocol = cls_selenium_wd.find_element(row, './td[2]').text.lower().split(" ")[0]
        return ip, port, protocol

    def _parse_anonymity(
        self, 
        row: WebElement, 
        cls_selenium_wd: SeleniumWD
    ) -> str:
        """Parse anonymity level from the row.

        Parameters
        ----------
        row : WebElement
            The table row element to parse
        cls_selenium_wd : SeleniumWD
            Selenium webdriver helper instance

        Returns
        -------
        str
            Anonymity level ("elite", "anonymous", or "transparent")
        """
        anonymity_text = cls_selenium_wd.find_element(row, './td[3]').text.lower()
        return "elite" if anonymity_text == "hia" else (
            "anonymous" if anonymity_text == "anm" else "transparent"
        )

    def _parse_performance_metrics(
        self, 
        row: WebElement, 
        cls_selenium_wd: SeleniumWD
    ) -> tuple[float, str, float]:
        """Parse performance metrics (timeout, uptime, ts_check_date) from the row.

        Parameters
        ----------
        row : WebElement
            The table row element to parse
        cls_selenium_wd : SeleniumWD
            Selenium webdriver helper instance

        Returns
        -------
        tuple[float, str, int]
            Tuple containing (timeout, uptime, ts_check_date)
        """
        latency_text = cls_selenium_wd.find_element(row, './td[6]').text
        try:
            latency = float(latency_text.replace(".", ""))
            timeout = 1.0 / latency
        except (ValueError, ZeroDivisionError):
            latency = 1.0
            timeout = 1.0

        uptime = cls_selenium_wd.find_element(row, './td[8]').text
        check_date = cls_selenium_wd.find_element(row, './td[9]').text
        ts_check_date = self.composed_time_ago_to_ts_unix(check_date)
        return timeout, uptime, ts_check_date

    def _parse_geographic_info(
        self, 
        row: WebElement, 
        cls_selenium_wd: SeleniumWD
    ) -> tuple[str, str, str, str, str, str, str]:
        """Parse geographic information (country_code, city, region, etc.) from the row.

        Parameters
        ----------
        row : WebElement
            The table row element to parse
        cls_selenium_wd : SeleniumWD
            Selenium webdriver helper instance

        Returns
        -------
        tuple[str, str, str, str, str, str, str]
            Tuple containing (timezones, continent, continent_code, country_name, country_code, 
            city, region)
        """
        location_text = cls_selenium_wd.find_element(row, './td[4]').text
        country_code, city, region = self._parse_location(location_text)
        timezones = ", ".join(self.ww_timezones.get_timezones_by_country_code(country_code))
        continent = self.ww_geography.get_continent_by_country_code(country_code)
        continent_code = self.ww_geography.get_continent_code_by_country_code(country_code)
        country_details = self.ww_geography.get_country_details(country_code) or {}
        country_name = country_details.get("name", "")
        return timezones, continent, continent_code, country_name, country_code, city, region