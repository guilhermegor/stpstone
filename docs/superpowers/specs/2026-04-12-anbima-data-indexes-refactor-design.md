# Refactor: anbima_data_indexes.py — Private Base Class

**Date:** 2026-04-12  
**File:** `stpstone/ingestion/countries/br/exchange/anbima_data_indexes.py`

---

## Problem

The file contains 4 classes (`AnbimaDataIMAGeral`, `AnbimaDataIDAGeral`, `AnbimaDataIDALIQGeral`, `AnbimaDataIDKAPre1A`) that share identical implementations of `__init__`, `get_response`, and `parse_raw_file`. The only differences per class are:

| Class | URL suffix | Table name | Columns | Dtypes |
|---|---|---|---|---|
| `AnbimaDataIMAGeral` | `IMAGERAL-HISTORICO.xls` | `br_anbima_data_indexes_ima_geral` | 10 (incl. PMR) | 10 entries |
| `AnbimaDataIDAGeral` | `IDAGERAL-HISTORICO.xls` | `br_anbima_data_indexes_ida_geral` | 9 (no PMR) | 9 entries |
| `AnbimaDataIDALIQGeral` | `IDALIQGERAL-HISTORICO.xls` | `br_anbima_data_indexes_ida_liq_geral` | 9 (no PMR) | 9 entries |
| `AnbimaDataIDKAPre1A` | `IDKAPRE1A-HISTORICO.xls` | `br_anbima_data_indexes_idka_pre_1a` | 7 | 7 entries |

Result: ~500 lines of duplication in a 708-line file.

---

## Design

### Approach: Private base class with class-level config constants

Introduce `_AnbimaDataIndexBase(ABCIngestionOperations)` that:
- Holds all shared logic (`__init__`, `run`, `get_response`, `parse_raw_file`, `transform_data`)
- Declares abstract class-level attributes: `_INDEX_FILE`, `_TABLE_NAME`, `_COLUMNS`, `_DTYPES`
- Resolves `self.url = BASE_URL + self._INDEX_FILE` in `__init__`
- In `run`, uses `str_table_name or self._TABLE_NAME` so callers can still override

Each concrete class becomes 5 lines of constant definitions only.

### Base URL

```
https://s3-data-prd-use1-precos.s3.us-east-1.amazonaws.com/arquivos/indices-historico/
```

### `run` signature change

`str_table_name` default changes from a hardcoded string to `""` in the base signature.  
At runtime: `_table_name = str_table_name or self._TABLE_NAME`. Public behaviour is identical.

### Naming

- `transform_data` parameter stays `str_url: str` (not `file: StringIO`) — this source passes the S3 URL directly to `pd.read_excel`, not a byte stream.

---

## Files Changed

| File | Change |
|---|---|
| `stpstone/ingestion/countries/br/exchange/anbima_data_indexes.py` | Refactor: add `_AnbimaDataIndexBase`, slim each class to constants |
| `tests/unit/test_anbima_data_indexes.py` | Update to reflect base class; add `test_module_reload` imports |
| `examples/anbima_data_indexes.py` | **Create** (missing, required by CLAUDE.md) |

---

## Constraints

- Public API unchanged: same 4 class names, same `__init__` signature, same `run` signature
- Tests remain green; `test_run_with_db` still checks the resolved table name
- No YAML config (per project memory: inline all config)
- No `skiprows=0` — it's the pandas default; remove it to reduce noise
