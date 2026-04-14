"""Anbima IPCA Forecasts for the Last Twelve Months ingestion module."""

from collections import Counter
from logging import Logger
from math import nan
import re
from typing import Optional, Union

import backoff
import pandas as pd
import requests
from requests import Session

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.br.macroeconomics._anbima_ipca_core import AnbimaIPCACore
from stpstone.transformations.validation.metaclass_type_checker import type_checker
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


class AnbimaIPCAForecastsLTM(AnbimaIPCACore, ABCIngestionOperations):
    """Anbima IPCA Forecasts for the Last Twelve Months."""

    def __init__(
        self,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        url: str = "https://www.anbima.com.br/pt_br/informar/estatisticas/precos-e-indices/projecao-de-inflacao-gp-m.htm",
    ) -> None:
        """Initialize the Anbima IPCA Forecasts for the Last Twelve Months.

        Parameters
        ----------
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
            The database session, by default None.
        url : str, optional
            The URL, by default the ANBIMA IPCA projection page.

        Returns
        -------
        None
        """
        super().__init__(logger=logger, cls_db=cls_db, url=url)

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "br_anbima_ipca_forecasts_ltm",
    ) -> Optional[pd.DataFrame]:
        """Run the Anbima IPCA Forecasts for the Last Twelve Months.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0).
        bool_verify : bool, optional
            Verify the SSL certificate, by default True.
        bool_insert_or_ignore : bool, optional
            Insert or ignore the data, by default False.
        str_table_name : str, optional
            The table name, by default "br_anbima_ipca_forecasts_ltm".

        Returns
        -------
        Optional[pd.DataFrame]
            The DataFrame.
        """
        return super().run(
            dict_dtypes={
                "MES_COLETA": str,
                "DATA": "date",
                "PROJECAO_PCT": float,
                "DATA_VALIDADE": "date",
                "IPCA_EFETIVO_PCT": float,
            },
            timeout=timeout,
            bool_verify=bool_verify,
            bool_insert_or_ignore=bool_insert_or_ignore,
            str_table_name=str_table_name,
        )

    @backoff.on_exception(
        backoff.expo,
        (RuntimeError, requests.exceptions.HTTPError),
        max_time=60,
    )
    def transform_data(self, scraper_playwright: PlaywrightScraper) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.

        Parameters
        ----------
        scraper_playwright : PlaywrightScraper
            The PlaywrightScraper.

        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.

        Raises
        ------
        RuntimeError
            If the navigation to the URL fails.
        """
        xpath_current_month_forecasts = """
        //*[@id="profile"]/div/div[5]/table/tbody/tr[position() > 2]/td
        """
        list_th: list[str] = [
            "MES_COLETA", "DATA", "PROJECAO_PCT", "DATA_VALIDADE", "IPCA_EFETIVO_PCT"
        ]

        with scraper_playwright.launch():
            if not scraper_playwright.navigate(self.url):
                raise RuntimeError(f"Failed to navigate to URL: {self.url}")

            scraper_playwright.trigger_strategies(
                json_steps=[
                    {
                        "type": "aria",
                        "selector": "aria/IPCA",
                        "description": "ARIA selector for IPCA",
                    },
                    {
                        "type": "css",
                        "selector": "body > div.container li:nth-of-type(2) > a",
                        "description": "CSS selector from DevTools",
                    },
                    {
                        "type": "xpath",
                        "selector": "//html/body/div[3]/div/main/ul/li[2]/a",
                        "description": "Exact XPath from recording",
                    },
                    {
                        "type": "css",
                        "selector": "body > div.container li:nth-of-type(2) > a",
                        "description": "Pierce selector fallback",
                    },
                    {
                        "type": "css",
                        "selector": "main ul li:nth-child(2) a",
                        "description": "Flexible CSS selector",
                    },
                    {
                        "type": "xpath",
                        "selector": "//main//ul/li[2]/a",
                        "description": "Flexible XPath",
                    },
                    {
                        "type": "text",
                        "selector": "text=IPCA",
                        "description": "Text-based selector",
                    },
                    {
                        "type": "xpath",
                        "selector": "//a[contains(text(), 'IPCA')]",
                        "description": "XPath with text content",
                    },
                ],
                target_content_selectors=[
                    "table",
                    "[id='profile'] table",
                    ".table",
                    "//table[contains(., 'DATA') or contains(., 'MES')]",
                ],
            )

            list_td = scraper_playwright.get_elements(
                selector=xpath_current_month_forecasts,
                timeout=10_000,
            )
            list_td = [
                nan if len(str(dict_["text"])) == 1
                else str(dict_["text"]).replace(",", ".")
                for dict_ in list_td
            ]

        list_ser = self._restructure_table_with_missing_columns(
            flat_list=list_td,
            expected_columns=list_th,
            missing_value=None,
        )

        df_ = pd.DataFrame(list_ser)
        df_["IPCA_EFETIVO_PCT"] = df_.groupby("MES_COLETA")["IPCA_EFETIVO_PCT"].ffill()
        return df_

    def _restructure_table_with_missing_columns(
        self,
        flat_list: list[str],
        expected_columns: list[str],
        month_pattern: str = r"^\w+\s+de\s+\d{4}$",
        date_pattern: str = r"^\d{2}/\d{2}/\d{2}$",
        numeric_pattern: str = r"^-?\d+\.?\d*$",
        missing_value: Optional[str] = None,
    ) -> list[dict[str, str]]:
        """Restructure flat table data handling rows with missing columns.

        Parameters
        ----------
        flat_list : list[str]
            Flat list of table data with irregular structure.
        expected_columns : list[str]
            Expected column names in order.
        month_pattern : str, optional
            Regex pattern to identify month/year values.
        date_pattern : str, optional
            Regex pattern to identify date values.
        numeric_pattern : str, optional
            Regex pattern to identify numeric values.
        missing_value : Optional[str], optional
            Value to use for missing columns, by default None.

        Returns
        -------
        list[dict[str, str]]
            List of dictionaries representing table rows.
        """
        if not flat_list or not expected_columns:
            return []

        month_regex = re.compile(month_pattern, re.IGNORECASE)
        date_regex = re.compile(date_pattern)
        numeric_regex = re.compile(numeric_pattern)

        @type_checker
        def identify_value_type(value: str) -> str:
            """Identify the type of value based on regex patterns.

            Parameters
            ----------
            value : str
                Value to identify.

            Returns
            -------
            str
                Type of value.
            """
            if month_regex.match(value):
                return "month"
            elif date_regex.match(value):
                return "date"
            elif numeric_regex.match(value):
                return "numeric"
            else:
                return "unknown"

        @type_checker
        def analyze_sequence(
            values: list[str], start_idx: int, max_look_ahead: int = 6
        ) -> dict:
            """Analyze a sequence of values to determine the best row structure.

            Parameters
            ----------
            values : list[str]
                List of values to analyze.
            start_idx : int
                Starting index of the sequence.
            max_look_ahead : int, optional
                Maximum number of values to look ahead, by default 6.

            Returns
            -------
            dict
                Dictionary containing information about the sequence.
            """
            sequence_info = []
            for i in range(min(max_look_ahead, len(values) - start_idx)):
                if start_idx + i < len(values):
                    val = values[start_idx + i]
                    val_type = identify_value_type(val)
                    sequence_info.append({
                        "value": val,
                        "type": val_type,
                        "position": start_idx + i,
                    })
            return sequence_info

        result_rows = []
        i = 0
        current_month = missing_value

        self.cls_create_log.log_message(
            self.logger,
            f"Processing {len(flat_list)} elements",
            "info",
        )

        while i < len(flat_list):
            sequence = analyze_sequence(flat_list, i)

            if not sequence:
                break

            self.cls_create_log.log_message(
                self.logger,
                f"\nPosition {i}: Analyzing sequence:",
                "info",
            )
            for seq in sequence:
                self.cls_create_log.log_message(
                    self.logger,
                    f"  {seq['position']}: '{seq['value']}' -> {seq['type']}",
                    "info",
                )

            if len(sequence) >= 5:  # noqa SIM102: use a single `if` statement instead of nested `if` statements
                if (
                    sequence[0]["type"] == "month"
                    and sequence[1]["type"] == "date"
                    and sequence[2]["type"] == "numeric"
                    and sequence[3]["type"] == "date"
                    and sequence[4]["type"] == "numeric"
                ):
                    row = {
                        expected_columns[0]: sequence[0]["value"],
                        expected_columns[1]: sequence[1]["value"],
                        expected_columns[2]: sequence[2]["value"],
                        expected_columns[3]: sequence[3]["value"],
                        expected_columns[4]: sequence[4]["value"],
                    }
                    current_month = sequence[0]["value"]
                    i += 5
                    self.cls_create_log.log_message(
                        self.logger,
                        f"  -> Complete row: {row}",
                        "info",
                    )
                    result_rows.append(row)
                    continue

            if len(sequence) >= 4:  # noqa SIM102: use a single `if` statement instead of nested `if` statements
                if (
                    sequence[0]["type"] == "month"
                    and sequence[1]["type"] == "date"
                    and sequence[2]["type"] == "numeric"
                    and sequence[3]["type"] == "date"
                ):
                    row = {
                        expected_columns[0]: sequence[0]["value"],
                        expected_columns[1]: sequence[1]["value"],
                        expected_columns[2]: sequence[2]["value"],
                        expected_columns[3]: sequence[3]["value"],
                        expected_columns[4]: missing_value,
                    }
                    current_month = sequence[0]["value"]
                    i += 4
                    self.cls_create_log.log_message(
                        self.logger,
                        f"  -> Missing last column: {row}",
                        "info",
                    )
                    result_rows.append(row)
                    continue

                if (
                    sequence[0]["type"] == "date"
                    and sequence[1]["type"] == "numeric"
                    and sequence[2]["type"] == "date"
                    and sequence[3]["type"] == "numeric"
                ):
                    row = {
                        expected_columns[0]: current_month,
                        expected_columns[1]: sequence[0]["value"],
                        expected_columns[2]: sequence[1]["value"],
                        expected_columns[3]: sequence[2]["value"],
                        expected_columns[4]: sequence[3]["value"],
                    }
                    i += 4
                    self.cls_create_log.log_message(
                        self.logger,
                        f"  -> Missing first column: {row}",
                        "info",
                    )
                    result_rows.append(row)
                    continue

            if len(sequence) >= 3:  # noqa SIM102: use a single `if` statement instead of nested `if` statements
                if (
                    sequence[0]["type"] == "date"
                    and sequence[1]["type"] == "numeric"
                    and sequence[2]["type"] == "date"
                ):
                    row = {
                        expected_columns[0]: current_month,
                        expected_columns[1]: sequence[0]["value"],
                        expected_columns[2]: sequence[1]["value"],
                        expected_columns[3]: sequence[2]["value"],
                        expected_columns[4]: missing_value,
                    }
                    i += 3
                    self.cls_create_log.log_message(
                        self.logger,
                        f"  -> Missing first and last: {row}",
                        "info",
                    )
                    result_rows.append(row)
                    continue

            self.cls_create_log.log_message(
                self.logger,
                f"  -> No pattern matched, skipping element '{flat_list[i]}'",
                "warning",
            )
            i += 1

        return result_rows

    def _debug_flat_list_structure(self, flat_list: list[str]) -> None:
        """Debug the structure of a flat list by logging type analysis.

        Parameters
        ----------
        flat_list : list[str]
            Flat list of table data.

        Returns
        -------
        None
        """
        month_pattern = r"^\w+\s+de\s+\d{4}$"
        date_pattern = r"^\d{2}/\d{2}/\d{2}$"
        numeric_pattern = r"^-?\d+\.?\d*$"

        month_regex = re.compile(month_pattern, re.IGNORECASE)
        date_regex = re.compile(date_pattern)
        numeric_regex = re.compile(numeric_pattern)

        self.cls_create_log.log_message(
            self.logger,
            "=== FLAT LIST STRUCTURE ANALYSIS ===",
            "debug",
        )
        for i, item in enumerate(flat_list):
            item_type = "unknown"
            if month_regex.match(item):
                item_type = "month"
            elif date_regex.match(item):
                item_type = "date"
            elif numeric_regex.match(item):
                item_type = "numeric"

            self.cls_create_log.log_message(
                self.logger,
                f"{i:3d}: '{item}' -> {item_type}",
                "debug",
            )

        month_positions = [i for i, item in enumerate(flat_list) if month_regex.match(item)]
        self.cls_create_log.log_message(
            self.logger,
            f"\nMonth positions: {month_positions}",
            "debug",
        )

        if len(month_positions) > 1:
            distances = [
                month_positions[i] - month_positions[i - 1]
                for i in range(1, len(month_positions))
            ]
            self.cls_create_log.log_message(
                self.logger,
                f"Distances between months: {distances}",
                "debug",
            )

            distance_freq = Counter(distances)
            self.cls_create_log.log_message(
                self.logger,
                f"Distance frequencies: {dict(distance_freq)}",
                "debug",
            )
