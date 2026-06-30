# Apply QUOTE_NONE to every CVM `;`-delimited CSV reader (full coverage)

**Repo:** stpstone. **Type:** robustness fix + consistency. **Origin:** the same class of bug
fixed in 3.2.1 for `cvm_funds_monthly_profile` — generalise it to the sibling CVM readers that
share the identical `pd.read_csv(file, sep=";")` pattern.

---

## Background — what 3.2.1 already fixed (and why)

CVM open-data CSVs are `;`-delimited and **do not use `"` as a field wrapper** — a `"` in a
free-text field (e.g. an assembly-deliberation note) is literal data, and administrators
sometimes submit an **unescaped/unbalanced `"`**. pandas' default C engine treats `"` as a
quote char, so an unclosed quote swallows the following `;`-separated values into one field,
**shifting subsequent columns left**. Downstream that corrupts data or crashes a dtype cast
(the 3.2.1 case: a risk-factor name `IPCA` shifted into the numeric `PRAZO_CARTEIRA_TITULO`,
crashing `change_dtypes`).

**Proven on the official `perfil_mensal_fi_202605.csv`** (byte-identical to the live
`dados.cvm.gov.br` source, md5 `8b720861a07b09c1d6aa0301e6ad9b2d`): 4 administrator rows have an
unescaped `"`. Reading with `quoting=csv.QUOTE_NONE` makes `"` literal and parses cleanly —
**(25127, 110)**, 0 non-numeric `PRAZO_CARTEIRA_TITULO`, and it even **recovers ~266 rows** the
runaway quote was otherwise swallowing. Lossless: every data line splits to exactly 107 fields,
so no field legitimately contains `;` and there are no real quoted/multiline fields.

The fix shipped in **3.2.1** for one reader only:
`stpstone/ingestion/countries/br/registries/cvm_funds_monthly_profile.py` (`transform_data`,
`pd.read_csv(file, sep=";", encoding="latin-1", quoting=csv.QUOTE_NONE)`).

## Why generalise

Every other CVM `;`-delimited reader has the **identical bug surface**. Worse, several use an
`on_bad_lines="skip"` fallback that **silently drops** the malformed rows — data loss with no
error. `QUOTE_NONE` is strictly better: it *keeps* those rows, correctly parsed. Applying it
uniformly removes the whole bug class and lets the `on_bad_lines="skip"` fallbacks go away.

---

## Scope — the 9 CVM registry readers to fix

(`pd.read_csv(file, sep=";")` over CVM open data; line numbers as of 3.2.1 `main`.)

| File (`stpstone/ingestion/countries/br/registries/`) | read_csv line(s) | notes |
|---|---|---|
| `cvm_fif_daily_infos.py` | 269 | plain `sep=";"` |
| `cvm_fif_cda.py` | 350 | reads inside a per-file loop |
| `cvm_fif_portfolio.py` | 341, 350 | line 350 is an `on_bad_lines="skip"` fallback |
| `cvm_fif_fact_sheet.py` | 416, 425 | 425 is `on_bad_lines="skip"` fallback |
| `cvm_fif_cad_fi.py` | 306, 317 | `dtype=str`; 317 is `on_bad_lines="skip"` fallback |
| `cvm_data_banks_registry.py` | 356, 367 | `dtype=str`; 367 fallback |
| `cvm_fif_statement.py` | 384 | plain `sep=";"` |
| `cvm_fif_monthly_profile.py` | 376 | plain `sep=";"` |
| `cvm_data_distribution_offers.py` | 404, 416 | `dtype=str`; 416 fallback |

Already done (reference implementation): `cvm_funds_monthly_profile.py:302`.

### Verify-first (do NOT blindly change)

- `stpstone/ingestion/countries/br/exchange/_b3_search_by_trading_session_base.py:432` — also
  `pd.read_csv(file, sep=";")`, but it is a **B3** trading-session feed, not CVM open data.
  Confirm B3 doesn't legitimately quote fields before applying `QUOTE_NONE`; if its format
  genuinely uses quoting, leave it out.

### Out of scope

The `anbima_*` and `b3_*` **exchange** readers use different formats (`decimal=","`, `sep=","`,
custom `skiprows`/`header`) — not the CVM `;` open-data shape. Do not touch them here.

---

## Implementation — prefer a shared helper (DRY), so the convention can't drift

There is currently **no shared CSV-read helper** — each `transform_data` calls `pd.read_csv`
directly, and there is no CVM/registry base class hosting one. Add a small util and route every
CVM reader through it.

**1. New helper** — `stpstone/utils/parsers/cvm_csv.py` (or the project's existing parsers
location; mirror where `reading_yaml` etc. live):

```python
import csv
from io import StringIO
import pandas as pd


def read_cvm_csv(file: StringIO, **kwargs) -> pd.DataFrame:
    """Read a CVM `;`-delimited open-data CSV.

    CVM open data never wraps fields in double quotes, and administrators sometimes submit an
    unescaped `"` in a free-text field. QUOTE_NONE keeps `"` as a literal character so a stray
    quote cannot swallow the following `;`-separated values and shift columns. Caller kwargs
    (encoding, dtype, ...) override the defaults.
    """
    return pd.read_csv(file, sep=";", quoting=csv.QUOTE_NONE, **kwargs)
```

**2. Each reader** — replace its `pd.read_csv(file, sep=";", ...)` with `read_cvm_csv(file, ...)`,
dropping the now-redundant `sep=";"` and keeping any `encoding`/`dtype`. **Delete the
`on_bad_lines="skip"` fallback branches** — with `QUOTE_NONE` they are no longer needed, and
keeping them would re-introduce silent row loss. (If a reader genuinely needs a fallback for a
*different* failure mode, keep it but route it through `read_cvm_csv` too.)

**Minimal alternative** (if a shared util is undesirable): add `quoting=csv.QUOTE_NONE` (and
`import csv`) to each call inline, exactly as 3.2.1 did for `cvm_funds_monthly_profile`. The
helper is preferred — one authoritative definition, no per-file drift.

Also refactor `cvm_funds_monthly_profile.py` to call the new helper, so the 3.2.1 inline fix
isn't an outlier.

---

## Acceptance criteria

- Each touched CVM reader, given a file with an unescaped `"` in a text field, **does not crash
  and loses no rows** (field count stays the header's width; the malformed row is parsed, not
  skipped).
- No `on_bad_lines="skip"` remains on a CVM `;`-reader (or, if one remains, it is justified for
  a non-quote failure mode and documented).
- No regression on clean files (existing fixtures parse identically).
- `cvm_funds_monthly_profile` still reads `perfil_mensal_fi_202605` to (25127, 110), 0
  non-numeric `PRAZO_CARTEIRA_TITULO`.

## Tests

Per affected reader, a tiny fixture: header + 1 clean row + 1 row with an unescaped `"` in a
free-text column. Assert: read succeeds; field/column count == header width; the malformed row
is present (not dropped); a numeric column after the quote parses numerically. A shared
parametrised test over `read_cvm_csv` covers the helper once; per-reader smoke tests confirm
each is wired to it.

Reusable real-data fixture for the monthly-profile reader (already proven):
```bash
head -1 perfil_mensal_fi_202605.csv > fixture.csv
grep -aF "44.674.282/0001-88" perfil_mensal_fi_202605.csv | head -1 >> fixture.csv
```

## Release

Bump stpstone (3.2.1 → 3.2.2 / 3.3.0 per your semver) and publish. Downstream
(`perfil_mensal_cvm`) already pins `>=3.2.1`; bump to the new version when it consumes any of
the other readers — only `cvm_funds_monthly_profile` is on the current critical path.

## One-shot verification snippet

```python
import csv, pandas as pd
for f in [ "perfil_mensal_fi_202605.csv" ]:  # add other CVM dumps as available
    df = pd.read_csv(f, sep=";", encoding="latin-1", quoting=csv.QUOTE_NONE)
    n = open(f, encoding="latin-1").readline().count(";") + 1
    print(f, df.shape, "cols==header:", df.shape[1] == n)
```
