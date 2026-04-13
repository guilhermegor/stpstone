# Refactor: Flat Utils Modules → Namespace Packages

**Date:** 2026-04-13
**Branch:** bugfix/ingestion
**Scope:** 6 flat `.py` files under `stpstone/utils/`
**Execution:** Ordered incremental via `py-dev` agent, one module at a time

---

## 1. Problem Statement

Six utility modules exist as flat `.py` files, violating the mandatory package structure
defined in `stpstone/utils/CLAUDE.md`. They also:

- Define TypedDicts inline alongside implementation classes (no `_dto.py` separation)
- Have no Protocols / port contracts (`_ports.py` absent)
- Have no `isinstance` port-contract guard in `__init__`
- `line_b3.py` has a 3-level inheritance chain (`Resources → Operations → ConnectionApi`),
  violating the "max 2 levels / composition over inheritance" rule
- `margin_simulator_b3.py` has hardcoded browser session cookies inside methods
- `margin_simulator_b3.py` has a duplicate `SecurityGroup` TypedDict definition

---

## 2. Target Modules

| Source file | Area | Classes |
|---|---|---|
| `utils/providers/br/inoa.py` | br providers | `AlphaTools` |
| `utils/providers/br/line_b3.py` | br providers | `ConnectionApi`, `Operations`, `Resources`, `AccountsData`, `DocumentsData`, `Professional`, `ProfilesData` |
| `utils/providers/br/margin_simulator_b3.py` | br providers | `MarginSimulatorB3` |
| `utils/providers/ww/reuters.py` | ww providers | `Reuters` |
| `utils/trading_platforms/mt5.py` | trading platforms | `MT5` |
| `utils/llms/gpt.py` | LLMs | `GPT` |

---

## 3. Package Layout (Target State)

Each flat file becomes a namespace package (no `__init__.py`). Full required artifact set
per `stpstone/utils/CLAUDE.md`:

```
utils/providers/br/inoa/
├── _dto.py                 ← ReturnGenericReq TypedDict
├── _ports.py               ← IAlphaToolsClient Protocol (@runtime_checkable)
└── inoa.py                 ← AlphaTools adapter (metaclass=TypeChecker)

utils/providers/br/line_b3/
├── _dto.py                 ← (no TypedDicts currently; present as empty stub)
├── _ports.py               ← IConnectionApi, IOperations, IResources,
│                              IAccountsData, IDocumentsData, IProfessional,
│                              IProfilesData Protocols
├── connection_api.py       ← ConnectionApi
├── operations.py           ← Operations
├── resources.py            ← Resources
├── accounts_data.py        ← AccountsData
├── documents_data.py       ← DocumentsData
├── professional.py         ← Professional
└── profiles_data.py        ← ProfilesData

utils/providers/br/margin_simulator_b3/
├── _dto.py                 ← all TypedDicts (SecurityGroup deduplicated)
├── _ports.py               ← IMarginSimulatorB3 Protocol
└── margin_simulator_b3.py  ← MarginSimulatorB3 adapter

utils/providers/ww/reuters/
├── _dto.py                 ← ReturnToken TypedDict
├── _ports.py               ← IReuters Protocol
└── reuters.py              ← Reuters adapter

utils/trading_platforms/mt5/
├── _ports.py               ← IMT5 Protocol  (no TypedDicts)
└── mt5.py                  ← MT5 adapter

utils/llms/gpt/
├── _dto.py                 ← ReturnRunPrompt TypedDict
├── _ports.py               ← IGPT Protocol
└── gpt.py                  ← GPT adapter
```

Examples and tests (per module):
```
examples/<feature>.py               ← new; minimal runnable usage with placeholder creds
tests/unit/test_<feature>.py        ← existing; import paths updated only
```

---

## 4. Composition Design for `line_b3`

The existing 3-level inheritance chain is replaced with constructor injection.

### Dependency graph

```
ConnectionApi          — no injected dep; owns auth, token refresh, app_request
Operations             — injects IConnectionApi
Resources              — injects IOperations
                         (IOperations exposes both app_request and exchange_limits,
                          so Resources needs only one dependency)
AccountsData           — injects IConnectionApi
DocumentsData          — injects IConnectionApi
Professional           — injects IConnectionApi
ProfilesData           — injects IConnectionApi
```

### Caller usage

```python
conn = ConnectionApi(client_id=..., client_secret=..., broker_code=..., category_code=...)
ops  = Operations(conn=conn)
res  = Resources(ops=ops)
accs = AccountsData(conn=conn)
```

### Port contract rule

Every adapter `__init__` must end with:
```python
if not isinstance(self, I<ClassName>):
    raise NotImplementedError(
        f"{type(self).__name__} does not satisfy I<ClassName> — ..."
    )
```
Both the class docstring and `__init__` docstring must document `NotImplementedError`
in their `Raises` sections (required by `test_feature.sh` AST checker).

---

## 5. `margin_simulator_b3` Cookie Handling

The hardcoded cookie string is extracted to a class-level constant `_DEFAULT_COOKIES`
and made injectable via the constructor:

```python
class MarginSimulatorB3(metaclass=TypeChecker):
    _DEFAULT_COOKIES: str = "<existing hardcoded cookie value>"

    def __init__(
        self,
        dict_payload: ResultReferenceData,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        dict_cookies: Optional[str] = None,
    ) -> None:
        self._cookies = dict_cookies or self._DEFAULT_COOKIES
```

Both `_get_reference_data` and `risk_calculation` use `self._cookies` instead of the
inline literal. Callers that pass nothing get the existing behaviour unchanged.

The duplicate `SecurityGroup` definition is resolved: the more complete version
(with `securityTypeCode` and `symbolList` fields) is kept and moved to `_dto.py`.

---

## 6. `Reuters` Stateless Class

`Reuters` currently has no `__init__`. A minimal constructor is added solely to host
the port-contract guard:

```python
def __init__(self) -> None:
    if not isinstance(self, IReuters):
        raise NotImplementedError(...)
```

---

## 7. Import Path Changes

All existing `tests/unit/test_<feature>.py` files are updated:

| Before | After |
|---|---|
| `from stpstone.utils.providers.br.inoa import AlphaTools` | `from stpstone.utils.providers.br.inoa.inoa import AlphaTools` |
| `from stpstone.utils.providers.br.line_b3 import ConnectionApi` | `from stpstone.utils.providers.br.line_b3.connection_api import ConnectionApi` |
| `from stpstone.utils.providers.br.margin_simulator_b3 import MarginSimulatorB3, ResultReferenceData` | `from stpstone.utils.providers.br.margin_simulator_b3.margin_simulator_b3 import MarginSimulatorB3` and `from stpstone.utils.providers.br.margin_simulator_b3._dto import ResultReferenceData` |
| `from stpstone.utils.providers.ww.reuters import Reuters` | `from stpstone.utils.providers.ww.reuters.reuters import Reuters` |
| `from stpstone.utils.llms.gpt import GPT` | `from stpstone.utils.llms.gpt.gpt import GPT` |
| MT5 import (to be confirmed from test file) | `from stpstone.utils.trading_platforms.mt5.mt5 import MT5` |

---

## 8. Execution Order

Each module is handled by a dedicated `py-dev` agent invocation. Order (simplest → hardest):

1. `gpt` — 1 class, 1 TypedDict, straightforward
2. `reuters` — 1 stateless class, 1 TypedDict, add minimal `__init__`
3. `inoa` — 1 class, 1 TypedDict, backoff decorator to preserve
4. `margin_simulator_b3` — 1 class, multiple TypedDicts, cookie extraction, dedup
5. `mt5` — 1 class, no TypedDicts, nested `@type_checker` functions to preserve
6. `line_b3` — 7 classes, full composition redesign, 7 adapter files

**Gate after each module:** `make test_feat MODULE=<name>` must pass before starting the next.

---

## 9. What Is Not Changing

- No test logic (mocks, fixtures, assertions) — import paths only
- No public method signatures
- No business logic in any class
- No changes outside the 6 target modules and their test/example counterparts
