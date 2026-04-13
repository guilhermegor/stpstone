# CLAUDE.md — `stpstone/utils/`

This file provides guidance to Claude Code when working with code under `stpstone/utils/`.

## Package Structure (Mandatory)

Every feature under `utils/` must live in its own **package directory** (no `__init__.py`),
not a flat `.py` file. The minimum layout is:

```
utils/<area>/<feature>/
├── _ports.py           ← always present: Protocols and/or ABCs (Ports & Adapters)
├── _dto.py             ← present whenever a TypedDict is needed
└── <feature>.py        ← one public class per file; add more files as needed
```

The folder name mirrors the feature name in `snake_case`. Additional implementation files
(one public class each) are added alongside the primary feature file as the feature grows.

**Example — correct:**
```
utils/providers/br/anbimadata_api/
├── _ports.py
├── _dto.py
├── anbimadata_api_gen.py
└── anbimadata_api_funds.py
```

**Example — wrong:**
```
utils/providers/br/anbimadata_api.py   ← flat file with multiple classes
```

## `_dto.py` — Data Transfer Objects

`_dto.py` holds **TypedDicts** that represent data transfer shapes: API response envelopes,
structured function return types, or any data contract that crosses a layer boundary.

Rules:
- Create `_dto.py` as soon as any TypedDict is needed — never define TypedDicts inline
  inside an implementation file.
- Name each TypedDict `Return<MethodName>` for function return contracts, or
  `<Entity>Payload` for request/response envelopes.
- `_dto.py` may only import from the standard library and third-party packages.
  It must never import from sibling implementation files.

```python
# _dto.py
from typing import TypedDict


class ReturnAccessToken(TypedDict):
    """OAuth access token response envelope."""

    access_token: str
```

## `_ports.py` — Ports (Protocols and ABCs)

Following **Ports & Adapters (Hexagonal) Architecture**: a *port* is the contract the
application depends on; an *adapter* is the concrete class that fulfils it.

`_ports.py` holds **Protocols** and **ABCs** that define those contracts. The concrete
implementation files (`<feature>.py`, etc.) are the adapters.

Rules:
- Always present, even if it starts with a single Protocol.
- Decorate all Protocols with `@runtime_checkable` so `isinstance()` checks work in tests.
- Consumers should type-hint against the Protocol/ABC from `_ports.py`, not the concrete class.
- `_ports.py` may import from `_dto.py`, the standard library, and third-party packages.
  It must never import from sibling implementation files.

```python
# _ports.py
from typing import Any, Literal, Optional, Protocol, runtime_checkable

from stpstone.utils.<area>.<feature>._dto import ReturnSomething


@runtime_checkable
class IFeatureClient(Protocol):
    """Outbound port contract for <feature> clients."""

    def some_method(self) -> ReturnSomething: ...
```

## Import Rule (Circular-Import Prevention)

```
_dto.py         → stdlib, third-party only
_ports.py       → _dto.py, stdlib, third-party only
<feature>.py    → _dto.py, _ports.py, stdlib, third-party, other stpstone modules
```

Neither `_dto.py` nor `_ports.py` may import from the implementation files in the
same package.

## Verification

After adding or modifying any module under `utils/`, run:

```bash
poetry run ruff check stpstone/utils/<area>/<feature>/
poetry run pytest tests/unit/test_<feature>.py -v
```
