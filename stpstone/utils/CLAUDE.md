# CLAUDE.md — `stpstone/utils/`

This file provides guidance to Claude Code when working with code under `stpstone/utils/`.

## Package Structure (Mandatory)

Every feature under `utils/` must live in its own **package directory** (no `__init__.py`),
not a flat `.py` file. The full required artifact set is:

```
utils/<area>/<feature>/         ← feature package (no __init__.py)
├── _ports.py                   ← always present: Protocols / ABCs (Ports & Adapters)
├── _dto.py                     ← present whenever a TypedDict is needed
└── <feature>.py                ← one public class per file (adapter); add more as needed

examples/<feature>.py           ← runnable usage example at the repo root examples/ dir
tests/unit/test_<feature>.py    ← unit tests mirroring the module name
```

The folder name mirrors the feature name in `snake_case`. Additional implementation files
(one public class each) are added alongside the primary feature file as the feature grows.

**Example — correct:**
```
utils/providers/br/anbimadata_api/   ← feature package (children of providers/br/)
├── _ports.py
├── _dto.py
├── anbimadata_api_gen.py
└── anbimadata_api_funds.py

examples/anbimadata_api_gen.py
examples/anbimadata_api_funds.py

tests/unit/test_anbimadata_api_gen.py
tests/unit/test_anbimadata_api_funds.py
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

## Adapter Pattern Requirements

Every concrete implementation file (adapter) that uses `metaclass=TypeChecker` **must** also
include an explicit `isinstance` guard in `__init__` to fail fast when the port contract is
broken by a subclass override. Both the guard and any exceptions it raises **must** be
documented in the `__init__` docstring — the `test_feature.sh` AST checker flags undocumented
raises.

```python
# <feature>.py — adapter
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.<area>.<feature>._ports import IFeatureClient


class FeatureImpl(metaclass=TypeChecker):
    """Adapter that fulfils the ``IFeatureClient`` port.

    ``metaclass=TypeChecker`` wires up runtime argument-type enforcement on every
    public method. The explicit ``isinstance`` guard in ``__init__`` below provides
    an immediate ``NotImplementedError`` if a subclass accidentally breaks the port
    contract (e.g. a wrong return type on an overridden method).

    Parameters
    ----------
    ...

    Raises
    ------
    NotImplementedError
        If this class does not satisfy ``IFeatureClient`` — i.e. if a required
        interface method is missing or has an incompatible signature.
    """

    def __init__(self, ...) -> None:
        """Initialise the adapter.

        Parameters
        ----------
        ...

        Raises
        ------
        ValueError
            <explain when ValueError is raised>
        NotImplementedError
            If this class does not satisfy the ``IFeatureClient`` port.
        """
        # ... set attributes, make I/O calls ...

        # Port-contract guard: must come after all attribute assignments so that
        # structural isinstance checks against @runtime_checkable Protocols can
        # inspect the fully-constructed instance.
        if not isinstance(self, IFeatureClient):
            raise NotImplementedError(
                f"{type(self).__name__} does not satisfy IFeatureClient — "
                "implement all required methods with compatible signatures."
            )
```

**Why document `NotImplementedError` in both class and `__init__` docstrings?**
`test_feature.sh` parses every `raise` statement with Python's `ast` module and
cross-checks against the `Raises` section of the enclosing function's docstring. A
raise without a matching docstring entry is reported as `❌ Raised but not documented
exception NotImplementedError`, which fails the CI check. Class-level docstrings are
optional but recommended for clarity when the guard lives in `__init__`.

## Verification

After adding or modifying any module under `utils/`, run:

```bash
make test_feat MODULE=<feature>
# which internally runs:
#   codespell  stpstone/utils/<area>/<feature>/<feature>.py
#   pydocstyle stpstone/utils/<area>/<feature>/<feature>.py
#   ruff check stpstone/utils/<area>/<feature>/<feature>.py
#   pytest     tests/unit/test_<feature>.py -v
```
