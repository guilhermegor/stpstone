# B3 ISIN Detail Ingestion — Design

**Date:** 2026-06-23
**Status:** Approved (pending user review)
**Author:** Claude (brainstorming session)

## Problem

B3 exposes a per-ISIN security-detail endpoint:

```
https://sistemaswebb3-listados.b3.com.br/isinProxy/IsinCall/GetDetail/<token>
```

The `<token>` is **not a hash** — it is the URL-safe-irrelevant standard
base64 encoding of the compact JSON `{"isin":"<ISIN>"}` (no spaces). It is
fully reversible and the API accepts it with or without `=` padding:

| ISIN | token |
|------|-------|
| `BRBCXPC00294` | `eyJpc2luIjoiQlJCQ1hQQzAwMjk0In0=` |
| `BRBCXPC00906` | `eyJpc2luIjoiQlJCQ1hQQzAwOTA2In0=` |
| `BRBCXPC008X1` | `eyJpc2luIjoiQlJCQ1hQQzAwOFgxIn0=` |

The endpoint returns a single **nested JSON object** per ISIN describing the
security (issuer, security-type acronym, category, species, indexer, currency,
face value, issue/maturity dates, interest rate, etc.).

We need an ingestion class, following the project's `ABCIngestionOperations`
template, that accepts a **list of ISINs** and returns one flattened DataFrame
row per ISIN (or inserts into the DB when a session is provided).

## Reconnaissance findings

- A plain `requests.get` with `accept`/`referer`/`user-agent` headers returns
  **HTTP 200** from this environment — **no Cloudflare `cf_clearance` cookie is
  required**. The project's standard `get_response` (requests + backoff) is
  sufficient; no Playwright/Selenium fallback is needed.
- `DFStandardization.limit_columns_to_dtypes` **drops every column not present
  in `dict_dtypes`** — so the dtype dict doubles as the column allowlist.
- pandas has **no native Decimal dtype**. `change_dtypes` calls
  `df.astype(dict_dtypes)`. To carry `Decimal` instances through the pipeline we
  list the two numeric columns with dtype `object` (so `astype` leaves them
  untouched), having already converted them to `Decimal` in `transform_data`.
  The string-cleaning steps (`replace_num_delimiters`, `clean_encoding_issues`,
  `strip_data`, `strip_hidden_characters`) only act on `str` cells, so `Decimal`
  objects pass through untouched.

## Decisions (confirmed with user)

1. **Query shape:** batch — constructor takes `list_isins: List[str]`; `run()`
   fetches each and returns one DataFrame with one row per ISIN.
2. **Numeric type:** `Decimal` (diverging from the float-only pipeline default)
   for the two monetary/rate fields.
3. **`VALOR_NOMINAL`** (`valorNominal`): `Decimal`, **2 dp, ROUND_DOWN**
   (truncation) — `Decimal(str(v)).quantize(Decimal("0.01"), ROUND_DOWN)`.
4. **`TAXA_JUROS`** (`taxaJuros`): `Decimal`, **4 dp, ROUND_DOWN** (truncation),
   stored as the percent number B3 returns (e.g. `11.2500`).
5. **ISIN enrichment (standards-based only):** add a deterministic **country
   decode** (ISO 3166-1) and a **Luhn check-digit validator** (ISO 6166).
6. **CFI enrichment:** decode the response's `cfi` field per **ISO 10962** —
   category (char 1) and group (char 2) to human labels; attributes (chars 3–6)
   kept raw. No guessing.
7. **NOT decoding the NSIN (ISIN chars 3–11):** the issuer/instrument-type
   substrings (`BCXP`→Banco XP, `…CDB…`) are B3-proprietary and unverifiable;
   the API already returns `emissor.*` and `sigla.*` authoritatively. Out of
   scope (see "Out of scope").

## Files (three, created together — per ingestion CLAUDE.md)

| File | Path |
|------|------|
| Implementation | `stpstone/ingestion/countries/br/registries/b3_isin_detail.py` |
| Unit tests | `tests/unit/test_b3_isin_detail.py` |
| Runnable example | `examples/b3_isin_detail.py` |

- **Country/domain:** `br/registries` — it is a securities-registry lookup.
- **Class name:** `B3IsinDetail` (named after the file).
- **Table name:** `br_b3_isin_detail`.
- No new `__init__.py` files.

## Public de-para helper

A module-level function in the same file (not a second public class — allowed
by the one-class-per-file rule, which forbids only a second *class*):

```python
BASE_URL = "https://sistemaswebb3-listados.b3.com.br/isinProxy/IsinCall/GetDetail/"

def isin_to_token(isin: str) -> str:
    """Return the base64 token B3 expects for a given ISIN."""
    payload = json.dumps({"isin": isin}, separators=(",", ":"))
    return base64.b64encode(payload.encode("ascii")).decode("ascii")
```

This is the reusable ISIN→token de-para the user asked for. The class builds its
URLs from it (`BASE_URL + isin_to_token(isin)`), keeping a single authoritative
definition (DRY). The inverse (`token_to_isin`) is **out of scope** (YAGNI —
the ingestion only needs ISIN→token).

## ISIN & CFI enrichment (standards-based, module-level helpers)

All decoders are **pure module-level functions** in the same file (no extra
class — only the one public class is allowed; functions are fine). They are kept
in-file for now (the only consumer is this feature); extracting them to a shared
`utils/parsers` codec is a later move if a second consumer appears (YAGNI). They
operate on **international standards only** (ISO 6166 / ISO 3166-1 / ISO 10962),
never on B3's proprietary NSIN layout.

### ISIN structure (ISO 6166) — what is decodable

```
BR  BCXPC0029  4
└┬┘ └───┬───┘  └┬┘
 │      │       └─ check digit (Luhn mod-10 over chars 1–11)  → VALIDATED
 │      └───────── NSIN, 9 chars, B3-proprietary               → NOT decoded
 └──────────────── ISO 3166-1 alpha-2 country code             → DECODED
```

All three sample ISINs (`BRBCXPC00294`, `BRBCXPC00906`, `BRBCXPC008X1`) were
verified Luhn-valid; tampering the check digit (`…00295`) fails — confirming the
algorithm.

### Country decode (reuses existing `pycountry` dependency)

`pycountry` (`>=24.6.1`) is already in `pyproject.toml` — no hardcoded ISO 3166
table.

```python
def decode_isin_country(isin: str) -> tuple[str, str]:
    """Return (alpha_2_code, country_name) for an ISIN's first two chars."""
    code = isin[:2].upper()
    country = pycountry.countries.get(alpha_2=code)
    return code, (country.name if country is not None else "UNKNOWN")
```

### ISIN validator (fail-fast, ISO 6166 Luhn)

```python
def is_valid_isin(isin: str) -> bool:
    """Return True when isin is 12 alphanumeric chars with a valid Luhn digit."""
    # format check (12 alnum) + letter→digit expansion (A=10…Z=35) + Luhn mod 10
```

The class validates in `__init__` and raises `ValueError` naming any invalid
ISINs — a malformed ISIN would otherwise query a meaningless token.

### CFI decode (ISO 10962) — stable parts only

```python
CFI_CATEGORIES: dict[str, str] = {   # char 1 — closed, stable set
    "E": "EQUITIES", "C": "COLLECTIVE INVESTMENT VEHICLES",
    "D": "DEBT INSTRUMENTS", "R": "ENTITLEMENTS (RIGHTS)",
    "O": "OPTIONS", "F": "FUTURES", "S": "SWAPS",
    "H": "NON-LISTED AND COMPLEX LISTED OPTIONS", "I": "SPOT",
    "J": "FORWARDS", "K": "STRATEGIES", "L": "FINANCING",
    "T": "REFERENTIAL INSTRUMENTS", "M": "OTHERS (MISCELLANEOUS)",
}
CFI_GROUPS: dict[str, dict[str, str]] = {   # char 2, by category — Debt populated
    "D": {
        "B": "BONDS", "C": "CONVERTIBLE BONDS",
        "W": "BONDS WITH WARRANTS ATTACHED", "T": "MEDIUM-TERM NOTES",
        "Y": "MONEY MARKET INSTRUMENTS", "G": "MORTGAGE-BACKED SECURITIES",
        "A": "ASSET-BACKED SECURITIES", "N": "MUNICIPAL BONDS",
        "S": "STRUCTURED INSTRUMENTS (CAPITAL PROTECTION)",
        "D": "DEPOSITARY RECEIPTS ON DEBT INSTRUMENTS",
        "M": "OTHERS (MISCELLANEOUS)",
    },
    # other categories added on demand; unknown → "UNKNOWN"
}

def decode_cfi(cfi: Optional[str]) -> ReturnDecodeCFI:
    """Decode an ISO 10962 CFI code into category/group labels + raw attributes.

    Degrades gracefully: a missing or malformed cfi yields "UNKNOWN" labels and
    never raises (enrichment must not break ingestion).
    """
```

For `DBZUFR`: `CFI_CATEGORY_CODE="D"`, `CFI_CATEGORY_DESC="DEBT INSTRUMENTS"`,
`CFI_GROUP_CODE="B"`, `CFI_GROUP_DESC="BONDS"`, `CFI_ATTRIBUTES="ZUFR"`.

> **Verification caveat (carried into the plan):** the `CFI_CATEGORIES` /
> `CFI_GROUPS` label values above are my best-known ISO 10962:2021 mappings and
> **must be cross-checked against the official standard during implementation**
> before merge. The *mechanism* (decode chars 1–2, keep 3–6 raw, fall back to
> `UNKNOWN`) is final; the *label strings* are provisional. Attribute chars
> (3–6) are intentionally **not** decoded — they are group- and version-specific
> and cannot be mapped reliably from memory.

## Class shape

Follows the `ABCIngestionOperations` template; the linear `run()` flow is
unchanged, only the intermediate types become list-shaped.

```python
class B3IsinDetail(ABCIngestionOperations):

    def __init__(
        self,
        list_isins: Optional[List[str]] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        ...
        self.list_isins = list_isins or []
        self._validate_isins()  # fail-fast on malformed ISINs (ISO 6166 Luhn)
        self.list_urls = [BASE_URL + isin_to_token(i) for i in self.list_isins]
        # date_ref kept for standardize_dataframe audit log only
        self.date_ref = self.cls_dates_current.curr_date()
```

**Note on `date_ref`:** unlike the template, this source is not date-keyed.
`date_ref` is retained solely so `standardize_dataframe`'s `audit_log` can stamp
the ingestion date. The constructor does **not** accept `date_ref`; it defaults
to the current date internally.

### Method flow (top-down call order)

1. `run(timeout, bool_verify=True, bool_insert_or_ignore=False, str_table_name="br_b3_isin_detail")`
   - `list_resp = self.get_response(...)`
   - `list_records = self.parse_raw_file(list_resp)`
   - `df_ = self.transform_data(list_records)`
   - `df_ = self.standardize_dataframe(df_=df_, date_ref=self.date_ref, dict_dtypes=..., str_fmt_dt="YYYY-MM-DD", url=BASE_URL)`
   - insert into DB if `cls_db`, else return `df_`
   - **Guard:** raise `ValueError` if `self.list_isins` is empty.
   - **Guard (`_validate_isins`)** already ran in `__init__`.
2. `_validate_isins() -> None`
   raises `ValueError("invalid ISIN(s): …")` for any entry failing
   `is_valid_isin`. Placed immediately after `__init__` (its only caller).
3. `get_response(timeout, bool_verify) -> List[Response]`
   `@backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_time=60)` —
   loops over `self.list_urls`, `requests.get` each with the required headers
   (`accept`, `referer`, `user-agent`), `raise_for_status()`, returns the list.
4. `parse_raw_file(list_resp: List[Response]) -> List[dict]`
   returns `[r.json() for r in list_resp]`.
5. `transform_data(list_records: List[dict]) -> pd.DataFrame`
   flattens each nested record via `self._flatten_record`, builds the DataFrame.
6. `_flatten_record(record: dict) -> ReturnFlattenRecord`
   maps nested JSON → flat UPPER_SNAKE_CASE dict using `.get()` chains (missing
   sub-objects tolerated for non-CDB instrument types); converts `VALOR_NOMINAL`
   and `TAXA_JUROS` to quantized `Decimal`; calls `decode_isin_country` and
   `decode_cfi` to populate the enrichment columns. Returns a `TypedDict`
   (`ReturnFlattenRecord`).

### Column mapping (`dict_dtypes`)

| JSON path | Column | dtype |
|-----------|--------|-------|
| `isin` | `ISIN` | `str` |
| `cfi` | `CFI` | `str` |
| `fisn` | `FISN` | `str` |
| `emissor.id` | `EMISSOR_ID` | `int` |
| `emissor.codigo` | `EMISSOR_CODIGO` | `str` |
| `emissor.descricaoRazaoSocial` | `EMISSOR_RAZAO_SOCIAL` | `str` |
| `emissor.nomeResumido` | `EMISSOR_NOME_RESUMIDO` | `str` |
| `emissor.cnpj` | `EMISSOR_CNPJ` | `str` |
| `emissor.situacaoAtiva` | `EMISSOR_SITUACAO_ATIVA` | `str` (see note) |
| `sigla.codigo` | `SIGLA_CODIGO` | `str` |
| `sigla.descricaoPt` | `SIGLA_DESCRICAO_PT` | `str` |
| `sigla.descricaoEn` | `SIGLA_DESCRICAO_EN` | `str` |
| `sigla.categoria.codigo` | `CATEGORIA_CODIGO` | `str` |
| `sigla.categoria.descricaoPt` | `CATEGORIA_DESCRICAO_PT` | `str` |
| `sigla.categoria.descricaoEn` | `CATEGORIA_DESCRICAO_EN` | `str` |
| `especie.codigo` | `ESPECIE_CODIGO` | `str` |
| `especie.descricaoPt` | `ESPECIE_DESCRICAO_PT` | `str` |
| `especie.descricaoEn` | `ESPECIE_DESCRICAO_EN` | `str` |
| `indexador.codigo` | `INDEXADOR_CODIGO` | `str` |
| `indexador.descricao` | `INDEXADOR_DESCRICAO` | `str` |
| `moeda` | `MOEDA` | `str` |
| `valorNominal` | `VALOR_NOMINAL` | `object` (Decimal, 2dp) |
| `situacaoAtiva` | `SITUACAO_ATIVA` | `str` (see note) |
| `dataEmissao` | `DATA_EMISSAO` | `date` |
| `dataVencimento` | `DATA_VENCIMENTO` | `date` |
| `forma` | `FORMA` | `str` |
| `descricaoPt` | `DESCRICAO_PT` | `str` |
| `descricaoEn` | `DESCRICAO_EN` | `str` |
| `patrocinado` | `PATROCINADO` | `str` (see note) |
| `tipoEmissao` | `TIPO_EMISSAO` | `str` |
| `garantia` | `GARANTIA` | `str` |
| `taxaJuros` | `TAXA_JUROS` | `object` (Decimal, 4dp) |
| `frequencia` | `FREQUENCIA` | `str` |
| `tipoJuros` | `TIPO_JUROS` | `str` |
| `dataPrimeiroPagamentoJuros` | `DATA_PRIMEIRO_PAGAMENTO_JUROS` | `date` |
| `tipoVencimento` | `TIPO_VENCIMENTO` | `str` |
| `codigoCetip` | `CODIGO_CETIP` | `str` |
| *derived from `isin[:2]`* | `ISIN_COUNTRY_CODE` | `str` |
| *derived via `pycountry`* | `ISIN_COUNTRY_NAME` | `str` |
| *derived from `cfi[0]`* | `CFI_CATEGORY_CODE` | `str` |
| *derived from `cfi[0]`* | `CFI_CATEGORY_DESC` | `str` |
| *derived from `cfi[1]`* | `CFI_GROUP_CODE` | `str` |
| *derived from `cfi[1]`* | `CFI_GROUP_DESC` | `str` |
| *derived from `cfi[2:6]`* | `CFI_ATTRIBUTES` | `str` |

The 7 derived columns are computed in `_flatten_record`, then listed in
`dict_dtypes` like any other column (so they survive `limit_columns_to_dtypes`).

**Note — boolean fields typed as `str`, not `bool`:** `DFStandardization.pipeline`
runs `filler` (fills missing non-date values with the string `"-99999"`) *before*
`change_dtypes`. A `bool` column with a missing value would become `"-99999"` then
`astype(bool)` → `True`, silently flipping an absent/false flag to True (the B3
endpoint omits these fields for many instrument types). `str` is the pipeline's
supported sentinel-friendly representation: present → `"True"`/`"False"`, missing →
`"-99999"`. `_flatten_record` still yields native Python `bool`/`None`
(`ReturnFlattenRecord` keeps `Optional[bool]`); only the post-standardization cast is
`str`. Covered by `test_run_missing_optional_fields_no_spurious_bool`.

Date columns use `str_fmt_dt="YYYY-MM-DD"` (the API returns plain `YYYY-MM-DD`
dates, **not** datetimes — so the BDI `…THH:MM:SS` caveat does not apply).

## Error handling

- Empty `list_isins` → `ValueError` raised in `run()` (fail fast).
- HTTP errors → `raise_for_status()` + `backoff` retry (same as template).
- Missing nested keys (other instrument types) → `.get()` chains yield `None`,
  which `standardize_dataframe.filler` replaces with the standard `-99999`
  sentinel for non-date columns / `2099-12-31` for dates.

## Testing (`tests/unit/test_b3_isin_detail.py`)

Mock all network I/O (`requests.get`) and bypass `backoff`. Cover:

- `isin_to_token` — known ISIN→token pairs (the 3 from the table), padding
  behaviour, type validation.
- `is_valid_isin` — the 3 valid sample ISINs return `True`; a tampered check
  digit (`BRBCXPC00295`), wrong length, and non-alnum input return `False`.
- `decode_isin_country` — `BR`→`Brazil`; unknown alpha-2 → `UNKNOWN`.
- `decode_cfi` — `DBZUFR`→ category `D`/`DEBT INSTRUMENTS`, group
  `B`/`BONDS`, attributes `ZUFR`; `None`/short/unknown codes → `UNKNOWN`
  labels and **no exception raised**.
- `_validate_isins` — a list with one malformed ISIN raises `ValueError`
  naming it; an all-valid list does not raise.
- `__init__` — URL list built from `list_isins`; empty default; malformed
  ISIN raises at construction.
- `run` — with `cls_db` (asserts `insert_table_db` called) and without (returns
  DataFrame); empty `list_isins` raises `ValueError`.
- `get_response` — success (multiple ISINs), HTTP error, timeout variants.
- `parse_raw_file` — valid list of responses → list of dicts; invalid type.
- `transform_data` — full CDB record (all fields), record with missing nested
  objects, empty input; assert the 7 enrichment columns are present and correct.
- `_flatten_record` — Decimal quantization for `VALOR_NOMINAL` (2dp) and
  `TAXA_JUROS` (4dp); assert resulting values are `Decimal` with correct scale;
  assert enrichment columns (`ISIN_COUNTRY_*`, `CFI_*`) populated from the
  record's `isin`/`cfi`.
- Type validation: wrong-type args to non-Optional params raise `TypeError`
  matching `"must be of type"`.
- Module reload.

Fixture: a canned JSON record matching the real `BRBCXPC00294` response.

## Example (`examples/b3_isin_detail.py`)

```python
"""B3 ISIN Security Detail Lookup By ISIN Code"""

from stpstone.ingestion.countries.br.registries.b3_isin_detail import B3IsinDetail


cls_ = B3IsinDetail(
    list_isins=["BRBCXPC00294", "BRBCXPC00906", "BRBCXPC008X1"],
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 ISIN DETAIL: \n{df_}")
df_.info()
```

## Out of scope

- `token_to_isin` inverse helper (YAGNI).
- Cloudflare cookie/Playwright fallback (not needed — plain GET returns 200).
- Pagination / bulk discovery of ISINs (caller supplies the list).
- **Decoding the NSIN (ISIN chars 3–11)** into issuer/instrument-type — B3's
  scheme is proprietary and not authoritatively known to us; the `emissor.*`
  and `sigla.*` fields the API returns already provide this, more reliably.
- **Decoding CFI attribute chars (3–6)** — group/version-specific, not mappable
  from memory; kept raw as `CFI_ATTRIBUTES`.
- Extracting the ISIN/CFI codec into a shared `utils` module — deferred until a
  second consumer exists (YAGNI).

## Delivery steps (after spec approval → plan → implementation)

1. New branch `feat/b3-isin-detail-ingestion`.
2. Implement the three files.
3. `make test_feat MODULE=b3_isin_detail` + `make lint` (must pass, 0 reformats).
4. Bump version `3.1.2 → 3.2.0` (`make bump_version` / `bin/bump_version.sh`).
5. User commits everything; open PR using `.github/PULL_REQUEST_TEMPLATE.md`.
