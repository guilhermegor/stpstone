# Design: B3 BDI Fixed Income Available Quantities Ingestion

**Date:** 2026-04-18
**Branch:** feat/bdi-b3

---

## Overview

Ingest the B3 BDI `Register` table, which reports the daily count of registered fixed-income
instruments and their aggregate financial volume, broken down by instrument type (CDB, LCI, DI, etc.).

---

## File Placement

| Purpose | Path |
|---|---|
| Implementation | `stpstone/ingestion/countries/br/registries/b3_bdi_fixed_income_available_quantities.py` |
| Unit tests | `tests/unit/test_b3_bdi_fixed_income_available_quantities.py` |
| Example | `examples/b3_bdi_fixed_income_available_quantities.py` |

---

## API

- **Endpoint:** `https://arquivos.b3.com.br/bdi/table/Register/{date}/{date}/{page}/{page_size}`
- **Method:** POST with empty JSON body `{}`
- **Pagination:** 1-based page index; loop terminates when `table.values` is empty
- **Default page size:** 1 000 records

---

## Class

**Name:** `B3BdiFixedIncomeAvailableQuantities`
**Base:** `ABCIngestionOperations` + `CoreIngestion` + `ContentParser`

### `__init__` parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date_ref` | `Optional[date]` | `None` | Reference date; falls back to previous BR working day |
| `logger` | `Optional[Logger]` | `None` | Logger instance |
| `cls_db` | `Optional[Session]` | `None` | SQLAlchemy session for DB writes |
| `int_page_size` | `int` | `1_000` | Records per page |
| `int_page_min` | `int` | `1` | First page (1-based) |
| `int_page_max` | `Optional[int]` | `None` | Last page; `None` = fetch until empty |

---

## Column / dtype Mapping

| API name | UPPER_SNAKE_CASE | dtype |
|---|---|---|
| `RptDt` | `RPT_DT` | `"date"` |
| `TckrSymb` | `TCKR_SYMB` | `str` |
| `QuantityInstrument` | `QUANTITY_INSTRUMENT` | `int` |
| `RegisterVolume` | `REGISTER_VOLUME` | `float` |
| *(injected)* | `URL` | `str` |

Column names are derived by `StrHandler.convert_case(col["name"], "pascal", "upper_constant")`,
identical to all other BDI siblings.

---

## Method Order (top-down call order)

1. `__init__`
2. `run` — pagination loop → concat → standardize → DB insert or return
3. `_log_page_progress` — called from `run`
4. `get_response` — POST request with backoff
5. `parse_raw_file` — extracts `resp.json()["table"]`
6. `transform_data` — builds DataFrame from columns + values

---

## Error Handling

`get_response` decorated with `@backoff.on_exception`:
- Retries on: `HTTPError`, `Timeout`, `ConnectionError`
- `max_tries=5`, `factor=2`, `max_time=120`

`run` returns `None` (not an error) when no pages are returned.

---

## DB table name

`br_b3_bdi_fixed_income_available_quantities`

---

## Testing

Cover:
- `__init__`: default date fallback, explicit `date_ref`
- `run`: single-page success, multi-page success, empty first page → returns `None`, `cls_db` path (insert called, return `None`), no-`cls_db` path (DataFrame returned)
- `get_response`: success (200), HTTP error triggers retry, timeout triggers retry
- `parse_raw_file`: valid response, wrong type raises `TypeError`
- `transform_data`: normal input, empty `values` returns empty DataFrame
- Module reload (import smoke test)

All network calls mocked via `unittest.mock.patch("requests.post")`. Backoff bypassed with `mock.patch("backoff.on_exception", lambda *a, **kw: lambda f: f)`.
