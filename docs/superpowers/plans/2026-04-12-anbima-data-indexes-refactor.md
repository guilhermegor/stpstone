# Anbima Data Indexes Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace 4 near-identical ingestion classes with a private base class driven by class-level config constants, eliminating ~500 lines of duplication.

**Architecture:** Introduce `_AnbimaDataIndexBase(ABCIngestionOperations)` holding all shared logic. Each public class declares 4 class-level constants (`_INDEX_FILE`, `_TABLE_NAME`, `_COLUMNS`, `_DTYPES`) and inherits everything else. Public API is unchanged.

**Tech Stack:** Python 3.9+, pandas, requests, backoff, pytest, SQLAlchemy (Session)

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `stpstone/ingestion/countries/br/exchange/anbima_data_indexes.py` | Modify | Refactored module: base class + 4 slim concrete classes |
| `tests/unit/test_anbima_data_indexes.py` | Modify | Remove `skiprows=0` from `pd.read_excel` assertions; add base-class coverage |
| `examples/anbima_data_indexes.py` | Create | Runnable usage example (required by CLAUDE.md) |

---

### Task 1: Rewrite the module

**Files:**
- Modify: `stpstone/ingestion/countries/br/exchange/anbima_data_indexes.py`

- [ ] **Step 1: Replace the file content**

Write the following complete file (708 lines → ~170 lines):

```python
"""Anbima Data Indexes."""

from datetime import date
from logging import Logger
from typing import Optional, Union

import backoff
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


_BASE_URL = (
    "https://s3-data-prd-use1-precos.s3.us-east-1.amazonaws.com/"
    "arquivos/indices-historico/"
)


class _AnbimaDataIndexBase(ABCIngestionOperations):
    """Shared base for Anbima historical index ingestion classes."""

    _INDEX_FILE: str
    _TABLE_NAME: str
    _COLUMNS: list[str]
    _DTYPES: dict[str, object]

    def __init__(
        self,
        date_ref: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
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
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.url = _BASE_URL + self._INDEX_FILE

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.

        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0).
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default True.
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False.
        str_table_name : str, optional
            The name of the table; falls back to the class default when empty,
            by default "".

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
        str_url = self.parse_raw_file(resp_req)
        df_ = self.transform_data(str_url=str_url)
        df_ = self.standardize_dataframe(
            df_=df_,
            date_ref=self.date_ref,
            dict_dtypes=self._DTYPES,
            str_fmt_dt="YYYY-MM-DD",
            url=self.url,
        )
        _table_name = str_table_name or self._TABLE_NAME
        if self.cls_db:
            self.insert_table_db(
                cls_db=self.cls_db,
                str_table_name=_table_name,
                df_=df_,
                bool_insert_or_ignore=bool_insert_or_ignore,
            )
        else:
            return df_

    @backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_time=60)
    def get_response(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Return a list of response objects.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0).
        bool_verify : bool, optional
            Verify the SSL certificate, by default True.

        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            A list of response objects.
        """
        resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
        resp_req.raise_for_status()
        return resp_req

    def parse_raw_file(
        self,
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
    ) -> str:
        """Parse the raw file content.

        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.

        Returns
        -------
        str
            The parsed content.
        """
        return resp_req.url

    def transform_data(self, str_url: str) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.

        Parameters
        ----------
        str_url : str
            The url with the excel content.

        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return pd.read_excel(str_url, engine="openpyxl", names=self._COLUMNS)


class AnbimaDataIMAGeral(_AnbimaDataIndexBase):
    """Anbima Data IMAGeral."""

    _INDEX_FILE = "IMAGERAL-HISTORICO.xls"
    _TABLE_NAME = "br_anbima_data_indexes_ima_geral"
    _COLUMNS = [
        "INDICE",
        "DATA_REFERENCIA",
        "NUMERO_INDICE",
        "VARIACAO_DIARIA",
        "VARIACAO_MES",
        "VARIACAO_ANUAL",
        "VARIACAO_12_MESES",
        "VARIACAO_24_MESES",
        "DURATION_DU",
        "PMR",
    ]
    _DTYPES: dict[str, object] = {
        "INDICE": "category",
        "DATA_REFERENCIA": "date",
        "NUMERO_INDICE": float,
        "VARIACAO_DIARIA": float,
        "VARIACAO_MES": float,
        "VARIACAO_ANUAL": float,
        "VARIACAO_12_MESES": float,
        "VARIACAO_24_MESES": float,
        "DURATION_DU": float,
        "PMR": float,
    }


class AnbimaDataIDAGeral(_AnbimaDataIndexBase):
    """Anbima Data IDA Geral."""

    _INDEX_FILE = "IDAGERAL-HISTORICO.xls"
    _TABLE_NAME = "br_anbima_data_indexes_ida_geral"
    _COLUMNS = [
        "INDICE",
        "DATA_REFERENCIA",
        "NUMERO_INDICE",
        "VARIACAO_DIARIA",
        "VARIACAO_MES",
        "VARIACAO_ANUAL",
        "VARIACAO_12_MESES",
        "VARIACAO_24_MESES",
        "DURATION_DU",
    ]
    _DTYPES: dict[str, object] = {
        "INDICE": "category",
        "DATA_REFERENCIA": "date",
        "NUMERO_INDICE": float,
        "VARIACAO_DIARIA": float,
        "VARIACAO_MES": float,
        "VARIACAO_ANUAL": float,
        "VARIACAO_12_MESES": float,
        "VARIACAO_24_MESES": float,
        "DURATION_DU": float,
    }


class AnbimaDataIDALIQGeral(_AnbimaDataIndexBase):
    """Anbima Data IDALIQ Geral."""

    _INDEX_FILE = "IDALIQGERAL-HISTORICO.xls"
    _TABLE_NAME = "br_anbima_data_indexes_ida_liq_geral"
    _COLUMNS = [
        "INDICE",
        "DATA_REFERENCIA",
        "NUMERO_INDICE",
        "VARIACAO_DIARIA",
        "VARIACAO_MES",
        "VARIACAO_ANUAL",
        "VARIACAO_12_MESES",
        "VARIACAO_24_MESES",
        "DURATION_DU",
    ]
    _DTYPES: dict[str, object] = {
        "INDICE": "category",
        "DATA_REFERENCIA": "date",
        "NUMERO_INDICE": float,
        "VARIACAO_DIARIA": float,
        "VARIACAO_MES": float,
        "VARIACAO_ANUAL": float,
        "VARIACAO_12_MESES": float,
        "VARIACAO_24_MESES": float,
        "DURATION_DU": float,
    }


class AnbimaDataIDKAPre1A(_AnbimaDataIndexBase):
    """Anbima Data IDKA Pré 1 Ano Geral."""

    _INDEX_FILE = "IDKAPRE1A-HISTORICO.xls"
    _TABLE_NAME = "br_anbima_data_indexes_idka_pre_1a"
    _COLUMNS = [
        "INDICE",
        "DATA_REFERENCIA",
        "NUMERO_INDICE",
        "VARIACAO_DIARIA",
        "VARIACAO_MES",
        "VARIACAO_ANUAL",
        "VARIACAO_12_MESES",
    ]
    _DTYPES: dict[str, object] = {
        "INDICE": "category",
        "DATA_REFERENCIA": "date",
        "NUMERO_INDICE": float,
        "VARIACAO_DIARIA": float,
        "VARIACAO_MES": float,
        "VARIACAO_ANUAL": float,
        "VARIACAO_12_MESES": float,
    }
```

---

### Task 2: Update tests

**Files:**
- Modify: `tests/unit/test_anbima_data_indexes.py`

Two changes needed:
1. Remove `skiprows=0` from every `pd.read_excel.assert_called_once_with(...)` call (3 occurrences: IMAGeral, IDAGeral, IDALIQGeral, IDKAPre1A tests).
2. Add a test that verifies `str_table_name` fallback resolves to the class constant.

- [ ] **Step 1: Remove `skiprows=0` from all `transform_data` test assertions**

In `test_transform_data` for `TestAnbimaDataIMAGeral` (line ~352), change:

```python
        pd.read_excel.assert_called_once_with(
            "https://example.com/test.xls",
            skiprows=0,
            engine="openpyxl",
            names=[
                "INDICE",
                "DATA_REFERENCIA",
                "NUMERO_INDICE",
                "VARIACAO_DIARIA",
                "VARIACAO_MES",
                "VARIACAO_ANUAL",
                "VARIACAO_12_MESES",
                "VARIACAO_24_MESES",
                "DURATION_DU",
                "PMR"
            ]
        )
```

to:

```python
        pd.read_excel.assert_called_once_with(
            "https://example.com/test.xls",
            engine="openpyxl",
            names=[
                "INDICE",
                "DATA_REFERENCIA",
                "NUMERO_INDICE",
                "VARIACAO_DIARIA",
                "VARIACAO_MES",
                "VARIACAO_ANUAL",
                "VARIACAO_12_MESES",
                "VARIACAO_24_MESES",
                "DURATION_DU",
                "PMR",
            ]
        )
```

Apply the same removal of `skiprows=0` in `TestAnbimaDataIDAGeral.test_transform_data`, `TestAnbimaDataIDALIQGeral.test_transform_data`, and `TestAnbimaDataIDKAPre1A.test_transform_data`.

- [ ] **Step 2: Add `str_table_name` fallback test to `TestAnbimaDataIMAGeral`**

After `test_run_with_db`, add:

```python
    def test_run_with_db_default_table_name(
        self,
        instance: AnbimaDataIMAGeral,
        mock_requests_get: MagicMock,
        mock_response: Response,
        sample_dataframe: pd.DataFrame,
        mocker: MockerFixture,
    ) -> None:
        """Test that run resolves the class-default table name when none is passed.

        Verifies
        --------
        - insert_table_db receives the class-level _TABLE_NAME when str_table_name is ""

        Parameters
        ----------
        instance : AnbimaDataIMAGeral
            Initialized instance
        mock_requests_get : MagicMock
            Mocked requests.get
        mock_response : Response
            Mocked response object
        sample_dataframe : pd.DataFrame
            Sample DataFrame for testing
        mocker : MockerFixture
            Pytest-mock fixture for creating mocks

        Returns
        -------
        None
        """
        mock_requests_get.return_value = mock_response
        mocker.patch.object(instance, "transform_data", return_value=sample_dataframe)
        mocker.patch.object(instance, "standardize_dataframe", return_value=sample_dataframe)
        mocker.patch.object(instance, "insert_table_db")
        instance.run(str_table_name="")
        instance.insert_table_db.assert_called_once_with(
            cls_db=instance.cls_db,
            str_table_name="br_anbima_data_indexes_ima_geral",
            df_=sample_dataframe,
            bool_insert_or_ignore=False,
        )
```

- [ ] **Step 3: Run tests to verify all pass**

```bash
cd /home/guilhermegor/github/stpstone
poetry run pytest tests/unit/test_anbima_data_indexes.py -v
```

Expected: all tests pass (green).

---

### Task 3: Create examples file

**Files:**
- Create: `examples/anbima_data_indexes.py`

- [ ] **Step 1: Write the examples file**

```python
"""Anbima Data Indexes historical series from ANBIMA S3."""

from stpstone.ingestion.countries.br.exchange.anbima_data_indexes import AnbimaDataIMAGeral


cls_ = AnbimaDataIMAGeral(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF IMA Geral: \n{df_}")
df_.info()
```

---

### Task 4: Final verification

**Files:** no changes

- [ ] **Step 1: Run full feature check**

```bash
cd /home/guilhermegor/github/stpstone
make test_feat MODULE=anbima_data_indexes
```

Expected: codespell, ruff lint, ruff format, and pytest all pass with no errors.

- [ ] **Step 2: Commit**

```bash
git add stpstone/ingestion/countries/br/exchange/anbima_data_indexes.py \
        tests/unit/test_anbima_data_indexes.py \
        examples/anbima_data_indexes.py \
        docs/superpowers/specs/2026-04-12-anbima-data-indexes-refactor-design.md \
        docs/superpowers/plans/2026-04-12-anbima-data-indexes-refactor.md
git commit -m "refactor(ingestion): consolidate AnbimaDataIndex classes into shared base"
```
