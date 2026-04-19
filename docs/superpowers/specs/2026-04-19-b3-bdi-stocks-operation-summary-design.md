# B3 BDI Stocks Operation Summary — Design Spec

**Date:** 2026-04-19
**Status:** approved

---

## Goal

Add an ingestion module for the B3 BDI `StocksOperationSummary` table, which breaks down equity market activity by transaction type (standard lots, fractional, etc.) for a given reference date.

---

## Endpoint

```
POST https://arquivos.b3.com.br/bdi/table/StocksOperationSummary/{date_start}/{date_end}/{page}/{page_size}
Body: {}
```

- Same request shape as the existing `DailyAverageStocks` BDI endpoint.
- Response envelope: `{"table": {"columns": [...], "values": [...]}}`

---

## Schema

| API column (PascalCase) | Output column (UPPER_SNAKE) | dtype  | Description                    |
|-------------------------|-----------------------------|--------|--------------------------------|
| `TckrSymb`              | `TCKR_SYMB`                 | `str`  | Transaction type label         |
| `NmbrTradesDay`         | `NMBR_TRADES_DAY`           | `int`  | Number of trades               |
| `ThousandTitles`        | `THOUSAND_TITLES`           | `float`| Volume in thousands of shares  |
| `Part1`                 | `PART1`                     | `float`| Participation % by volume      |
| `ValueInThousand`       | `VALUE_IN_THOUSAND`         | `float`| Financial value (BRL K)        |
| `Part2`                 | `PART2`                     | `float`| Participation % by value       |
| `OrderCol`              | `ORDER_COL`                 | `int`  | Hidden sort key                |

---

## Architecture

- **Class:** `B3BdiStocksOperationSummary` — extends `ABCIngestionOperations` following the standard scaffold.
- **Pagination:** `int_page_max=1` default; the table is a small fixed set of transaction-type rows (≤10), same as `DailyAverageStocks`.
- **date_ref:** defaults to previous Brazilian working day via `DatesBRAnbima.add_working_days`.
- **Backoff:** exponential retry on `HTTPError`, `Timeout`, `ConnectionError` (max 5 tries, 120 s cap).
- **`run()`** loop: fetch pages → `parse_raw_file` → `transform_data` → `standardize_dataframe` → optional DB insert.

---

## Files

| File | Path |
|------|------|
| Implementation | `stpstone/ingestion/countries/br/exchange/b3_bdi_stocks_operation_summary.py` |
| Unit tests | `tests/unit/test_b3_bdi_stocks_operation_summary.py` |
| Example | `examples/b3_bdi_stocks_operation_summary.py` |

DB table name: `br_b3_bdi_stocks_operation_summary`

---

## Testing plan

- `test_init_*` — date_ref default, custom page_size, logger propagation, URL template correctness.
- `test_get_response_*` — success (POST to correct URL), HTTP error, timeout variants.
- `test_parse_raw_file_*` — extracts `["table"]` from JSON envelope.
- `test_transform_data_*` — normal row, all transaction-type rows, empty values list.
- `test_run_*` — no DB (returns DataFrame), with DB (returns None, calls insert), all-empty first page (returns None).
