# Design: B3 BDI Fixed Income Puma Trades Ingestion

**Date:** 2026-04-18
**Branch:** feat/bdi-b3

---

## Summary

New ingestion module for the B3 BDI `DebenturesBusiness` table — fixed income securities (debentures and similar instruments) traded on the Puma electronic platform. Follows the established BDI paginated-POST pattern used by all other BDI fixed income modules.

---

## Files

| File | Path |
|------|------|
| Implementation | `stpstone/ingestion/countries/br/exchange/b3_bdi_fixed_income_puma_trades.py` |
| Unit tests | `tests/unit/test_b3_bdi_fixed_income_puma_trades.py` |
| Example runner | `examples/b3_bdi_fixed_income_puma_trades.py` |

---

## API

- **URL template:** `https://arquivos.b3.com.br/bdi/table/DebenturesBusiness/{date}/{date}/{page}/{page_size}`
- **Method:** `POST` with empty JSON body `{}`
- **Pagination:** 1-based page index; stop when `values` is empty or `int_page_max` reached
- **Date format:** `YYYY-MM-DD`; defaults to previous working day via `DatesBRAnbima`

---

## Class

```
B3BdiFixedIncomePumaTrades(ABCIngestionOperations)
```

### Constructor parameters

| Parameter | Type | Default | Notes |
|-----------|------|---------|-------|
| `date_ref` | `Optional[date]` | `None` | Defaults to previous BR working day |
| `logger` | `Optional[Logger]` | `None` | |
| `cls_db` | `Optional[Session]` | `None` | |
| `int_page_size` | `int` | `1_000` | Records per request |
| `int_page_min` | `int` | `1` | First page (1-based) |
| `int_page_max` | `Optional[int]` | `None` | Last page inclusive; `None` = auto-stop |

### Method order (caller above callee)

1. `__init__`
2. `run`
3. `_log_page_progress`
4. `get_response` (backoff: HTTPError, Timeout, ConnectionError; max_tries=5, factor=2, max_time=120)
5. `parse_raw_file`
6. `transform_data`

---

## Column / dtype map

| API column | DataFrame column | dtype |
|------------|-----------------|-------|
| `RptDt` | `RPT_DT` | `"date"` |
| `TckrSymb` | `TCKR_SYMB` | `str` |
| `Company` | `COMPANY` | `str` |
| `Opening` | `OPENING` | `float` |
| `Minimum` | `MINIMUM` | `float` |
| `Maximum` | `MAXIMUM` | `float` |
| `Average` | `AVERAGE` | `float` |
| `Final` | `FINAL` | `float` |
| `ClosRefPric` | `CLOS_REF_PRIC` | `float` |
| `PurchaseOffer` | `PURCHASE_OFFER` | `float` |
| `SaleOffer` | `SALE_OFFER` | `float` |
| `Quant` | `QUANT` | `int` |
| `Neg` | `NEG` | `int` |
| `Volume` | `VOLUME` | `float` |
| _(injected)_ | `URL` | `str` |

Column names derived via `StrHandler.convert_case(name, "pascal", "upper_constant")`.

---

## DB table name

`br_b3_bdi_fixed_income_puma_trades`

---

## Test coverage

- `__init__`: default date fallback, explicit date, pagination params
- `run`: no-db returns DataFrame; with db calls `insert_table_db`; empty page stops loop
- `get_response`: success (200), HTTPError triggers backoff, Timeout triggers backoff
- `parse_raw_file`: valid response extracts `["table"]`
- `transform_data`: normal multi-row input; empty `values` returns empty DataFrame
- Module reload (import smoke test)

All network calls mocked; `backoff.on_exception` patched to bypass delays.
