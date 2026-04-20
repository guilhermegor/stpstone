# Design: B3 BDI OTC Derivatives Ingestion

**Date:** 2026-04-20
**Branch:** feat/bdi-b3
**Scope:** Three new ingestion modules for B3 BDI OTC derivatives data

---

## 1. Overview

Three new ingestion classes consuming the B3 BDI JSON API at
`https://arquivos.b3.com.br/bdi/table/{TableName}/{date}/{date}/{page}/{size}`.
All three use `POST json={}` and follow the existing `ABCIngestionOperations` pattern
already established in `stpstone/ingestion/countries/br/otc/`.

---

## 2. Modules

### 2.1 `b3_bdi_derivatives_inventory_ccp.py`

- **Location:** `stpstone/ingestion/countries/br/otc/`
- **Class:** `B3BdiDerivativesInventoryCcp`
- **API table name:** `OTCInventoryCCP`
- **URL template:** `https://arquivos.b3.com.br/bdi/table/OTCInventoryCCP/{date}/{date}/{page}/{size}`
- **Default pagination:** `int_page_max=1` (dataset fits on one page)
- **DB table:** `br_b3_bdi_derivatives_inventory_ccp`
- **Output columns:**

| Column | API name | dtype |
|--------|----------|-------|
| `TCKR_SYMB` | `TckrSymb` | `str` |
| `TRAD_QTY` | `TradQty` | `int` |
| `FINANCIAL_VOL` | `FinancialVol` | `float` |

### 2.2 `b3_bdi_derivatives_inventory_wccp.py`

- **Location:** `stpstone/ingestion/countries/br/otc/`
- **Class:** `B3BdiDerivativesInventoryWccp`
- **API table name:** `OTCInventoryWCCP`
- **URL template:** `https://arquivos.b3.com.br/bdi/table/OTCInventoryWCCP/{date}/{date}/{page}/{size}`
- **Default pagination:** `int_page_max=1`
- **DB table:** `br_b3_bdi_derivatives_inventory_wccp`
- **Output columns:** identical to CCP variant

### 2.3 `b3_bdi_derivatives_options_flex.py`

- **Location:** `stpstone/ingestion/countries/br/otc/`
- **Class:** `B3BdiDerivativesOptionsFlex`
- **API table name:** `FlexibleOptions`
- **URL template:** `https://arquivos.b3.com.br/bdi/table/FlexibleOptions/{date}/{date}/{page}/{size}`
- **Default pagination:** `int_page_max=None` (paginate until empty)
- **DB table:** `br_b3_bdi_derivatives_options_flex`
- **Output columns:**

| Column | API name | dtype |
|--------|----------|-------|
| `TCKR_SYMB` | `TckrSymb` | `str` |
| `OPTION_TYPE` | `OptionType` | `str` |
| `OPTION_NEG` | `OptionNeg` | `str` |
| `DUE_DATE` | `DueDate` | `str` (datetime string `YYYY-MM-DDTHH:MM:SS`) |
| `NMBR_BUSINESSES` | `NmbrBusinesses` | `int` |
| `NATIONAL_VOL` | `NationalVol` | `float` |
| `AVRG_PREMIUM` | `AvrgPremium` | `float` |
| `AVRG_PRICE` | `AvrgPrice` | `float` |

---

## 3. Shared Implementation Pattern

All three classes follow the same structure:

```
__init__ → run → get_response → parse_raw_file → transform_data
```

### `__init__`

Parameters: `date_ref`, `logger`, `cls_db`, `int_page_size=1_000`, `int_page_min=1`, `int_page_max`.
Builds `url_tpl` from `date_ref` (defaults to previous BR working day).

### `run`

- Paginates from `int_page_min` until page returns empty DataFrame or `int_page_max` reached.
- Appends `URL` column per page.
- Concatenates pages, runs `standardize_dataframe`, inserts to DB or returns DataFrame.

### `get_response`

`requests.post(url, json={})` with backoff on `HTTPError`, `Timeout`, `ConnectionError`
(`max_tries=5`, `factor=2`, `max_time=120`).

### `parse_raw_file`

Returns `resp_req.json()["table"]`.

### `transform_data`

1. Guard: return empty `DataFrame` if `values` is empty.
2. Convert `columns[].name` from PascalCase → UPPER_SNAKE_CASE via `StrHandler`.
3. Build DataFrame from `values`, rename integer columns to names.
4. Select only the named columns (drops trailing null element from each row).

---

## 4. Deliverables Per Module

Each module requires exactly three files:

| File | Path |
|------|------|
| Implementation | `stpstone/ingestion/countries/br/otc/<module>.py` |
| Unit tests | `tests/unit/test_<module>.py` |
| Example | `examples/<module>.py` |

---

## 5. Test Coverage Requirements

Each test file must cover:

- `test_init_with_valid_inputs` — attributes set, URL template correct
- `test_init_default_page_size` — default `int_page_size=1_000`
- `test_init_without_date_ref` — falls back to previous working day
- `test_init_logger_propagated`
- `test_get_response_success` — correct POST call
- `test_get_response_http_error`
- `test_get_response_timeout_error`
- `test_get_response_connection_error`
- `test_get_response_timeout_variants` — parametrized
- `test_parse_raw_file_returns_table`
- `test_parse_raw_file_missing_table_key`
- `test_transform_data_normal`
- `test_transform_data_multiple_rows`
- `test_transform_data_empty_values`
- `test_run_without_db_paginates`
- `test_run_with_db`
- `test_run_no_data_returns_none`
- `test_module_reload`

---

## 6. Verification

After implementation:

```bash
make test_feat MODULE=b3_bdi_derivatives_inventory_ccp
make test_feat MODULE=b3_bdi_derivatives_inventory_wccp
make test_feat MODULE=b3_bdi_derivatives_options_flex
make lint
```

`make lint` must report **"0 files reformatted"**.
