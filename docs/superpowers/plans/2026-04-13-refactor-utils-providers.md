# Utils Providers Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.
>
> **Execution:** Each task is dispatched to a `py-dev` agent. Complete and verify one task
> before starting the next. `make test_feat MODULE=<name>` is the gate after every task.

**Goal:** Convert 6 flat `.py` utility modules into namespace packages following the Ports &
Adapters pattern defined in `stpstone/utils/CLAUDE.md`.

**Architecture:** Each flat file becomes a package directory (no `__init__.py`) with `_dto.py`
(TypedDicts), `_ports.py` (@runtime_checkable Protocols), and adapter file(s)
(`metaclass=TypeChecker` + `isinstance` port-contract guard in `__init__`). `line_b3` replaces
a 3-level inheritance chain with constructor injection across 7 files.

**Tech Stack:** Python 3.9+, `typing.Protocol`, `TypedDict`, `metaclass=TypeChecker`,
`backoff`, `requests`, `pandas`, `MetaTrader5`, `openai`

---

## File Structure Map

```
CREATE  stpstone/utils/llms/gpt/_dto.py
CREATE  stpstone/utils/llms/gpt/_ports.py
CREATE  stpstone/utils/llms/gpt/gpt.py
DELETE  stpstone/utils/llms/gpt.py
MODIFY  tests/unit/test_gpt.py               ← import path only
CREATE  examples/gpt.py

CREATE  stpstone/utils/providers/ww/reuters/_dto.py
CREATE  stpstone/utils/providers/ww/reuters/_ports.py
CREATE  stpstone/utils/providers/ww/reuters/reuters.py
DELETE  stpstone/utils/providers/ww/reuters.py
MODIFY  tests/unit/test_reuters.py           ← import path only
CREATE  examples/reuters.py

CREATE  stpstone/utils/providers/br/inoa/_dto.py
CREATE  stpstone/utils/providers/br/inoa/_ports.py
CREATE  stpstone/utils/providers/br/inoa/inoa.py
DELETE  stpstone/utils/providers/br/inoa.py
MODIFY  tests/unit/test_inoa.py              ← import path only
CREATE  examples/inoa.py

CREATE  stpstone/utils/providers/br/margin_simulator_b3/_dto.py
CREATE  stpstone/utils/providers/br/margin_simulator_b3/_ports.py
CREATE  stpstone/utils/providers/br/margin_simulator_b3/margin_simulator_b3.py
DELETE  stpstone/utils/providers/br/margin_simulator_b3.py
MODIFY  tests/unit/test_margin_simulator_b3.py  ← import paths only
CREATE  examples/margin_simulator_b3.py

CREATE  stpstone/utils/trading_platforms/mt5/_ports.py
CREATE  stpstone/utils/trading_platforms/mt5/mt5.py
DELETE  stpstone/utils/trading_platforms/mt5.py
MODIFY  tests/unit/test_mt5.py               ← import path only
CREATE  examples/mt5.py

CREATE  stpstone/utils/providers/br/line_b3/_dto.py
CREATE  stpstone/utils/providers/br/line_b3/_ports.py
CREATE  stpstone/utils/providers/br/line_b3/connection_api.py
CREATE  stpstone/utils/providers/br/line_b3/operations.py
CREATE  stpstone/utils/providers/br/line_b3/resources.py
CREATE  stpstone/utils/providers/br/line_b3/accounts_data.py
CREATE  stpstone/utils/providers/br/line_b3/documents_data.py
CREATE  stpstone/utils/providers/br/line_b3/professional.py
CREATE  stpstone/utils/providers/br/line_b3/profiles_data.py
DELETE  stpstone/utils/providers/br/line_b3.py
RENAME  tests/unit/test_line_b3.py → tests/unit/test_connection_api.py
CREATE  tests/unit/test_operations.py
CREATE  tests/unit/test_resources.py
CREATE  tests/unit/test_accounts_data.py
CREATE  tests/unit/test_documents_data.py
CREATE  tests/unit/test_professional.py
CREATE  tests/unit/test_profiles_data.py
CREATE  examples/line_b3.py
```

---

## Shared Pattern Reference

Every adapter follows this exact structure. Deviation fails `test_feature.sh`.

### `_dto.py` template
```python
"""TypedDicts for <Feature> client."""

from typing import TypedDict


class ReturnSomething(TypedDict):
    """Typed dictionary for <method> return value.

    Attributes
    ----------
    field : type
        Description.
    """

    field: type
```

### `_ports.py` template
```python
"""Protocols for <Feature> client."""

from typing import Protocol, runtime_checkable

from stpstone.utils.<area>.<feature>._dto import ReturnSomething


@runtime_checkable
class IFeatureClient(Protocol):
    """Structural protocol for <Feature> clients."""

    def some_method(self) -> ReturnSomething: ...
```

### Adapter port-contract guard (end of `__init__`)
```python
# Port-contract guard — must be last in __init__ after all attribute assignments.
if not isinstance(self, IFeatureClient):
    raise NotImplementedError(
        f"{type(self).__name__} does not satisfy IFeatureClient — "
        "implement all required methods with compatible signatures."
    )
```

Both the **class docstring** and the **`__init__` docstring** must include:
```
Raises
------
NotImplementedError
    If this class does not satisfy ``IFeatureClient``.
```

---

## Task 1: Refactor `gpt`

**Source:** `stpstone/utils/llms/gpt.py` — `GPT` class + `ReturnRunPrompt` TypedDict

**Files:**
- Create: `stpstone/utils/llms/gpt/_dto.py`
- Create: `stpstone/utils/llms/gpt/_ports.py`
- Create: `stpstone/utils/llms/gpt/gpt.py`
- Delete: `stpstone/utils/llms/gpt.py`
- Modify: `tests/unit/test_gpt.py`
- Create: `examples/gpt.py`

- [ ] **Step 1: Create `_dto.py`**

```python
# stpstone/utils/llms/gpt/_dto.py
"""TypedDicts for GPT client.

References
----------
.. [1] https://platform.openai.com/docs/guides/gpt
"""

from typing import TypedDict


class ReturnRunPrompt(TypedDict):
    """Typed dictionary for run_prompt message envelope.

    Attributes
    ----------
    role : str
        Role of the message sender (``"user"``, ``"system"``, or ``"assistant"``).
    content : list[dict]
        List of message content dictionaries.
    """

    role: str
    content: list[dict]
```

- [ ] **Step 2: Create `_ports.py`**

```python
# stpstone/utils/llms/gpt/_ports.py
"""Protocols for GPT client.

References
----------
.. [1] https://platform.openai.com/docs/guides/gpt
"""

from typing import Protocol, runtime_checkable

from openai.types.chat import ChatCompletion


@runtime_checkable
class IGPT(Protocol):
    """Structural protocol for any GPT-compatible client."""

    def run_prompt(self, list_tuple: list[tuple]) -> ChatCompletion: ...
```

- [ ] **Step 3: Create `gpt.py`**

Copy `GPT` class from the existing flat file verbatim, then:
1. Remove `ReturnRunPrompt` (now in `_dto.py`).
2. Add imports:
   ```python
   from stpstone.utils.llms.gpt._dto import ReturnRunPrompt
   from stpstone.utils.llms.gpt._ports import IGPT
   ```
3. Add `NotImplementedError` to both class and `__init__` docstring `Raises` sections.
4. Add port-contract guard as the last line of `__init__`:
   ```python
   if not isinstance(self, IGPT):
       raise NotImplementedError(
           f"{type(self).__name__} does not satisfy IGPT — "
           "implement run_prompt() with a compatible signature."
       )
   ```

- [ ] **Step 4: Update test import**

In `tests/unit/test_gpt.py`, change:
```python
# before
from stpstone.utils.llms.gpt import GPT
# after
from stpstone.utils.llms.gpt.gpt import GPT
```

- [ ] **Step 5: Create `examples/gpt.py`**

```python
"""Minimal usage example for GPT client."""

from unittest.mock import MagicMock, patch

from stpstone.utils.llms.gpt.gpt import GPT


def main() -> None:
    """Run a single-turn prompt against a GPT model."""
    with patch("stpstone.utils.llms.gpt.gpt.OpenAI") as mock_openai:
        mock_client = MagicMock()
        mock_openai.return_value = mock_client
        mock_client.chat.completions.create.return_value = MagicMock()

        gpt = GPT(api_key="sk-placeholder", str_model="gpt-4o", int_max_tokens=50)
        response = gpt.run_prompt([("text", "What is 2 + 2?")])
        print(response)


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Delete the old flat file**

```bash
git rm stpstone/utils/llms/gpt.py
```

- [ ] **Step 7: Verify**

```bash
make test_feat MODULE=gpt
```

Expected: `[✓] All checks passed for gpt`

- [ ] **Step 8: Commit**

```bash
git add stpstone/utils/llms/gpt/ tests/unit/test_gpt.py examples/gpt.py
git commit -m "refactor(gpt): convert flat module to namespace package"
```

---

## Task 2: Refactor `reuters`

**Source:** `stpstone/utils/providers/ww/reuters.py` — `Reuters` (stateless, no `__init__`) +
`ReturnToken` TypedDict

**Files:**
- Create: `stpstone/utils/providers/ww/reuters/_dto.py`
- Create: `stpstone/utils/providers/ww/reuters/_ports.py`
- Create: `stpstone/utils/providers/ww/reuters/reuters.py`
- Delete: `stpstone/utils/providers/ww/reuters.py`
- Modify: `tests/unit/test_reuters.py`
- Create: `examples/reuters.py`

- [ ] **Step 1: Create `_dto.py`**

```python
# stpstone/utils/providers/ww/reuters/_dto.py
"""TypedDicts for Reuters API client.

References
----------
.. [1] https://www.reuters.com/markets/currencies
"""

from typing import TypedDict


class ReturnToken(TypedDict):
    """Typed dictionary for token response.

    Attributes
    ----------
    access_token : str
        Access token for API authentication.
    token_type : str
        Type of the token.
    expires_in : int
        Expiration time in seconds.
    """

    access_token: str
    token_type: str
    expires_in: int
```

- [ ] **Step 2: Create `_ports.py`**

```python
# stpstone/utils/providers/ww/reuters/_ports.py
"""Protocols for Reuters API client.

References
----------
.. [1] https://www.reuters.com/markets/currencies
"""

from typing import Literal, Optional, Protocol, Union, runtime_checkable

from stpstone.utils.providers.ww.reuters._dto import ReturnToken


@runtime_checkable
class IReuters(Protocol):
    """Structural protocol for Reuters API clients."""

    def fetch_data(
        self,
        app: str,
        payload: Optional[dict],
        method: Literal["GET", "POST"],
        endpoint: str,
        timeout: Union[tuple, float, int],
    ) -> str: ...

    def token(self, api_key: str, deviceid: str) -> ReturnToken: ...

    def quotes(self, currency: str) -> dict: ...
```

- [ ] **Step 3: Create `reuters.py`**

Copy `Reuters` class verbatim, then:
1. Remove `ReturnToken` (now in `_dto.py`).
2. Add imports:
   ```python
   from stpstone.utils.providers.ww.reuters._dto import ReturnToken
   from stpstone.utils.providers.ww.reuters._ports import IReuters
   ```
3. Add a minimal `__init__` (the class currently has none):
   ```python
   def __init__(self) -> None:
       """Initialise the Reuters client.

       Raises
       ------
       NotImplementedError
           If this class does not satisfy the ``IReuters`` port.
       """
       if not isinstance(self, IReuters):
           raise NotImplementedError(
               f"{type(self).__name__} does not satisfy IReuters — "
               "implement fetch_data(), token(), and quotes()."
           )
   ```
4. Add `NotImplementedError` to the class docstring `Raises` section.

- [ ] **Step 4: Update test import**

In `tests/unit/test_reuters.py`, change:
```python
# before
from stpstone.utils.providers.ww.reuters import Reuters
# after
from stpstone.utils.providers.ww.reuters.reuters import Reuters
```

Also update the fixture that instantiates `Reuters()` — no signature change needed since
`__init__` takes no args.

- [ ] **Step 5: Create `examples/reuters.py`**

```python
"""Minimal usage example for Reuters API client."""

from unittest.mock import MagicMock, patch

from stpstone.utils.providers.ww.reuters.reuters import Reuters


def main() -> None:
    """Fetch a currency quote from Reuters."""
    with patch("stpstone.utils.providers.ww.reuters.reuters.request") as mock_req:
        mock_resp = MagicMock()
        mock_resp.text = '{"bid": 5.10, "ask": 5.12}'
        mock_req.return_value = mock_resp

        client = Reuters()
        print(client.quotes("BRL="))


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Delete the old flat file**

```bash
git rm stpstone/utils/providers/ww/reuters.py
```

- [ ] **Step 7: Verify**

```bash
make test_feat MODULE=reuters
```

Expected: `[✓] All checks passed for reuters`

- [ ] **Step 8: Commit**

```bash
git add stpstone/utils/providers/ww/reuters/ tests/unit/test_reuters.py examples/reuters.py
git commit -m "refactor(reuters): convert flat module to namespace package"
```

---

## Task 3: Refactor `inoa`

**Source:** `stpstone/utils/providers/br/inoa.py` — `AlphaTools` class + `ReturnGenericReq`
TypedDict

**Files:**
- Create: `stpstone/utils/providers/br/inoa/_dto.py`
- Create: `stpstone/utils/providers/br/inoa/_ports.py`
- Create: `stpstone/utils/providers/br/inoa/inoa.py`
- Delete: `stpstone/utils/providers/br/inoa.py`
- Modify: `tests/unit/test_inoa.py`
- Create: `examples/inoa.py`

- [ ] **Step 1: Create `_dto.py`**

```python
# stpstone/utils/providers/br/inoa/_dto.py
"""TypedDicts for INOA Alpha Tools API client."""

from typing import Any, TypedDict


class ReturnGenericReq(TypedDict):
    """Typed dictionary for generic request response.

    Attributes
    ----------
    items : list[dict[str, Any]]
        List of items returned from the API.
    """

    items: list[dict[str, Any]]
```

- [ ] **Step 2: Create `_ports.py`**

```python
# stpstone/utils/providers/br/inoa/_ports.py
"""Protocols for INOA Alpha Tools API client."""

from typing import Any, Literal, Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class IAlphaToolsClient(Protocol):
    """Structural protocol for INOA Alpha Tools API clients."""

    def generic_req(
        self,
        str_method: Literal["GET", "POST"],
        str_app: str,
        dict_params: dict[str, Any],
    ) -> list[dict[str, Any]]: ...

    @property
    def funds(self) -> pd.DataFrame: ...

    def quotes(self, list_ids: list[int]) -> pd.DataFrame: ...
```

- [ ] **Step 3: Create `inoa.py`**

Copy `AlphaTools` class verbatim, then:
1. Remove `ReturnGenericReq` (now in `_dto.py`).
2. Add imports:
   ```python
   from stpstone.utils.providers.br.inoa._dto import ReturnGenericReq
   from stpstone.utils.providers.br.inoa._ports import IAlphaToolsClient
   ```
3. Add `NotImplementedError` to both class and `__init__` docstring `Raises` sections.
4. Add port-contract guard as the last line of `__init__` (after `self.str_token = ...`):
   ```python
   if not isinstance(self, IAlphaToolsClient):
       raise NotImplementedError(
           f"{type(self).__name__} does not satisfy IAlphaToolsClient — "
           "implement generic_req(), funds, and quotes()."
       )
   ```

- [ ] **Step 4: Update test import**

In `tests/unit/test_inoa.py`, change:
```python
# before
from stpstone.utils.providers.br.inoa import AlphaTools
# after
from stpstone.utils.providers.br.inoa.inoa import AlphaTools
```

- [ ] **Step 5: Create `examples/inoa.py`**

```python
"""Minimal usage example for INOA Alpha Tools client."""

from datetime import datetime
from unittest.mock import MagicMock, patch

from stpstone.utils.providers.br.inoa.inoa import AlphaTools


def main() -> None:
    """Fetch fund list from INOA Alpha Tools."""
    with patch("stpstone.utils.providers.br.inoa.inoa.request") as mock_req:
        mock_resp = MagicMock()
        mock_resp.json.return_value = [
            {"id": 1, "name": "Fund A", "legal_id": "00.000.000/0001-00"}
        ]
        mock_resp.raise_for_status = MagicMock()
        mock_req.return_value = mock_resp

        client = AlphaTools(
            str_user="user",
            str_passw="pass",
            str_host="https://api.example.com/",
            str_instance="prod",
            date_start=datetime(2024, 1, 1),
            date_end=datetime(2024, 12, 31),
        )
        print(client.funds)


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Delete the old flat file**

```bash
git rm stpstone/utils/providers/br/inoa.py
```

- [ ] **Step 7: Verify**

```bash
make test_feat MODULE=inoa
```

Expected: `[✓] All checks passed for inoa`

- [ ] **Step 8: Commit**

```bash
git add stpstone/utils/providers/br/inoa/ tests/unit/test_inoa.py examples/inoa.py
git commit -m "refactor(inoa): convert flat module to namespace package"
```

---

## Task 4: Refactor `margin_simulator_b3`

**Source:** `stpstone/utils/providers/br/margin_simulator_b3.py` — `MarginSimulatorB3` class +
multiple TypedDicts (duplicate `SecurityGroup` present — keep the more complete definition
with `securityTypeCode` and `symbolList` fields).

**Files:**
- Create: `stpstone/utils/providers/br/margin_simulator_b3/_dto.py`
- Create: `stpstone/utils/providers/br/margin_simulator_b3/_ports.py`
- Create: `stpstone/utils/providers/br/margin_simulator_b3/margin_simulator_b3.py`
- Delete: `stpstone/utils/providers/br/margin_simulator_b3.py`
- Modify: `tests/unit/test_margin_simulator_b3.py`
- Create: `examples/margin_simulator_b3.py`

- [ ] **Step 1: Create `_dto.py`**

Move all TypedDicts from the source file. Deduplicate `SecurityGroup` — keep the second
definition (lines 65-70 of the original) which includes `securityTypeCode: int` and
`symbolList: list[str]`. Discard the first (line 30-37).

```python
# stpstone/utils/providers/br/margin_simulator_b3/_dto.py
"""TypedDicts for B3 Margin Simulator API client."""

from typing import Optional, TypedDict


class ReferenceData(TypedDict):
    """Typed dictionary for Reference Data payload.

    Attributes
    ----------
    referenceDataToken : str
        Token string for reference data.
    """

    referenceDataToken: str


class LiquidityResource(TypedDict):
    """Typed dictionary for Liquidity Resource payload.

    Attributes
    ----------
    value : int
        Liquidity resource value.
    """

    value: int


class Security(TypedDict):
    """Typed dictionary for Security payload.

    Attributes
    ----------
    symbol : str
        Instrument symbol.
    """

    symbol: str


class SecurityGroup(TypedDict):
    """Typed dictionary for Security Group payload.

    Attributes
    ----------
    positionTypeCode : int
        Position type code.
    securityTypeCode : int
        Security type code.
    symbolList : list[str]
        List of symbols in this group.
    """

    positionTypeCode: int
    securityTypeCode: int
    symbolList: list[str]


class SecurityGroupList(TypedDict):
    """Typed dictionary for Security Group List.

    Attributes
    ----------
    SecurityGroup : list[SecurityGroup]
        List of security groups.
    """

    SecurityGroup: list[SecurityGroup]


class Position(TypedDict):
    """Typed dictionary for Position payload.

    Attributes
    ----------
    longQuantity : int
        Long position quantity.
    shortQuantity : int
        Short position quantity.
    longPrice : int
        Long position price.
    shortPrice : int
        Short position price.
    """

    longQuantity: int
    shortQuantity: int
    longPrice: int
    shortPrice: int


class RiskPosition(TypedDict):
    """Typed dictionary for Risk Position payload.

    Attributes
    ----------
    Security : Security
        Security information.
    SecurityGroup : SecurityGroup
        Security group information.
    Position : Position
        Position information.
    """

    Security: Security
    SecurityGroup: SecurityGroup
    Position: Position


class ResultMarginSimulatorB3Payload(TypedDict):
    """Typed dictionary for Margin Simulator B3 full payload.

    Attributes
    ----------
    ReferenceData : ReferenceData
        Reference data token.
    LiquidityResource : LiquidityResource
        Liquidity resource value.
    RiskPositionList : list[RiskPosition]
        List of risk positions.
    """

    ReferenceData: ReferenceData
    LiquidityResource: LiquidityResource
    RiskPositionList: list[RiskPosition]


class ResultReferenceData(TypedDict):
    """Typed dictionary for Reference Data API response.

    Attributes
    ----------
    referenceDataToken : str
        Token returned by the reference data endpoint.
    liquidityResourceLimit : int
        Liquidity resource limit.
    SecurityGroupList : SecurityGroupList
        Available security groups.
    """

    referenceDataToken: str
    liquidityResourceLimit: int
    SecurityGroupList: SecurityGroupList


class Risk(TypedDict):
    """Typed dictionary for Risk calculation result.

    Attributes
    ----------
    totalDeficitSurplus : float
        Total deficit or surplus value.
    totalDeficitSurplusSubPortfolio_1 : float
        Sub-portfolio 1 deficit/surplus.
    totalDeficitSurplusSubPortfolio_2 : float
        Sub-portfolio 2 deficit/surplus.
    totalDeficitSurplusSubPortfolio_1_2 : float
        Combined sub-portfolio deficit/surplus.
    worstCaseSubPortfolio : int
        Worst case sub-portfolio index.
    potentialLiquidityResource : float
        Potential liquidity resource.
    totalCollateralValue : float
        Total collateral value.
    riskWithoutCollateral : float
        Risk without collateral.
    liquidityResource : float
        Available liquidity resource.
    calculationStatus : int
        Status code of the calculation.
    scenarioId : int
        Scenario identifier.
    """

    totalDeficitSurplus: float
    totalDeficitSurplusSubPortfolio_1: float
    totalDeficitSurplusSubPortfolio_2: float
    totalDeficitSurplusSubPortfolio_1_2: float
    worstCaseSubPortfolio: int
    potentialLiquidityResource: float
    totalCollateralValue: float
    riskWithoutCollateral: float
    liquidityResource: float
    calculationStatus: int
    scenarioId: int


class ResultRiskCalculationResponse(TypedDict):
    """Typed dictionary for Risk Calculation API response.

    Attributes
    ----------
    Risk : Risk
        Risk calculation results.
    BusinessStatusList : list or None
        List of business status entries, if any.
    """

    Risk: Risk
    BusinessStatusList: Optional[list]
```

- [ ] **Step 2: Create `_ports.py`**

```python
# stpstone/utils/providers/br/margin_simulator_b3/_ports.py
"""Protocols for B3 Margin Simulator API client."""

from typing import Protocol, runtime_checkable

from stpstone.utils.providers.br.margin_simulator_b3._dto import (
    ResultReferenceData,
    ResultRiskCalculationResponse,
)


@runtime_checkable
class IMarginSimulatorB3(Protocol):
    """Structural protocol for B3 Margin Simulator clients."""

    def risk_calculation(self) -> ResultRiskCalculationResponse: ...
```

- [ ] **Step 3: Create `margin_simulator_b3.py`**

Copy `MarginSimulatorB3` class verbatim, then apply these changes:

1. Remove all TypedDicts (now in `_dto.py`).
2. Add imports:
   ```python
   from stpstone.utils.providers.br.margin_simulator_b3._dto import (
       ResultReferenceData,
       ResultRiskCalculationResponse,
   )
   from stpstone.utils.providers.br.margin_simulator_b3._ports import IMarginSimulatorB3
   ```
3. Extract the hardcoded cookie string from `_get_reference_data` and `risk_calculation`
   into a class-level constant `_DEFAULT_COOKIES: str`. The value is the literal cookie
   string currently in the `'Cookie'` header of those methods.
4. Add `dict_cookies` parameter to `__init__`:
   ```python
   def __init__(
       self,
       dict_payload: ResultReferenceData,
       timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
       bool_verify: bool = True,
       dict_cookies: Optional[str] = None,
   ) -> None:
   ```
5. In `__init__` body, add before other assignments:
   ```python
   self._cookies = dict_cookies if dict_cookies is not None else self._DEFAULT_COOKIES
   ```
6. In `_get_reference_data` and `risk_calculation`, replace the inline `'Cookie': '<literal>'`
   with `'Cookie': self._cookies`.
7. Add `NotImplementedError` to class and `__init__` docstring `Raises` sections.
8. Add port-contract guard as the last line of `__init__`:
   ```python
   if not isinstance(self, IMarginSimulatorB3):
       raise NotImplementedError(
           f"{type(self).__name__} does not satisfy IMarginSimulatorB3 — "
           "implement risk_calculation()."
       )
   ```

- [ ] **Step 4: Update test imports**

In `tests/unit/test_margin_simulator_b3.py`, change:
```python
# before
from stpstone.utils.providers.br.margin_simulator_b3 import (
    MarginSimulatorB3,
    ResultReferenceData,
)
# after
from stpstone.utils.providers.br.margin_simulator_b3._dto import ResultReferenceData
from stpstone.utils.providers.br.margin_simulator_b3.margin_simulator_b3 import (
    MarginSimulatorB3,
)
```

- [ ] **Step 5: Create `examples/margin_simulator_b3.py`**

```python
"""Minimal usage example for B3 Margin Simulator client."""

from unittest.mock import MagicMock, patch

from stpstone.utils.providers.br.margin_simulator_b3._dto import (
    LiquidityResource,
    RiskPosition,
    ResultMarginSimulatorB3Payload,
    Security,
    SecurityGroup,
    Position,
)
from stpstone.utils.providers.br.margin_simulator_b3.margin_simulator_b3 import (
    MarginSimulatorB3,
)


def main() -> None:
    """Run a margin simulation with a placeholder payload."""
    payload: ResultMarginSimulatorB3Payload = {
        "ReferenceData": {"referenceDataToken": ""},
        "LiquidityResource": {"value": 0},
        "RiskPositionList": [],
    }
    ref_response = {
        "ReferenceData": {"referenceDataToken": "tok-placeholder"},
        "liquidityResourceLimit": 0,
        "SecurityGroupList": {"SecurityGroup": []},
    }
    risk_response = {
        "Risk": {
            "totalDeficitSurplus": 0.0,
            "totalDeficitSurplusSubPortfolio_1": 0.0,
            "totalDeficitSurplusSubPortfolio_2": 0.0,
            "totalDeficitSurplusSubPortfolio_1_2": 0.0,
            "worstCaseSubPortfolio": 0,
            "potentialLiquidityResource": 0.0,
            "totalCollateralValue": 0.0,
            "riskWithoutCollateral": 0.0,
            "liquidityResource": 0.0,
            "calculationStatus": 0,
            "scenarioId": 0,
        },
        "BusinessStatusList": None,
    }
    with (
        patch("stpstone.utils.providers.br.margin_simulator_b3.margin_simulator_b3.requests.get") as mock_get,
        patch("stpstone.utils.providers.br.margin_simulator_b3.margin_simulator_b3.requests.post") as mock_post,
    ):
        mock_get.return_value = MagicMock(json=MagicMock(return_value=ref_response))
        mock_get.return_value.raise_for_status = MagicMock()
        mock_post.return_value = MagicMock(json=MagicMock(return_value=risk_response))
        mock_post.return_value.raise_for_status = MagicMock()

        sim = MarginSimulatorB3(dict_payload=payload)
        print(sim.risk_calculation())


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Delete the old flat file**

```bash
git rm stpstone/utils/providers/br/margin_simulator_b3.py
```

- [ ] **Step 7: Verify**

```bash
make test_feat MODULE=margin_simulator_b3
```

Expected: `[✓] All checks passed for margin_simulator_b3`

- [ ] **Step 8: Commit**

```bash
git add stpstone/utils/providers/br/margin_simulator_b3/ \
        tests/unit/test_margin_simulator_b3.py \
        examples/margin_simulator_b3.py
git commit -m "refactor(margin_simulator_b3): convert to package; extract cookie param"
```

---

## Task 5: Refactor `mt5`

**Source:** `stpstone/utils/trading_platforms/mt5.py` — `MT5` class, no TypedDicts.
Contains nested `@type_checker`-decorated functions inside `get_symbols_info` and
`get_all_info_of_symbols` — preserve these exactly.

**Files:**
- Create: `stpstone/utils/trading_platforms/mt5/_ports.py`
- Create: `stpstone/utils/trading_platforms/mt5/mt5.py`
- Delete: `stpstone/utils/trading_platforms/mt5.py`
- Modify: `tests/unit/test_mt5.py`
- Create: `examples/mt5.py`

Note: no `_dto.py` needed — `MT5` has no TypedDicts.

- [ ] **Step 1: Create `_ports.py`**

```python
# stpstone/utils/trading_platforms/mt5/_ports.py
"""Protocols for MetaTrader5 trading platform interface."""

from datetime import datetime
from typing import Any, Optional, Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class IMT5(Protocol):
    """Structural protocol for MetaTrader5 interface clients."""

    def initialize(self, path: str, login: int, server: str, password: str) -> bool: ...

    def shutdown(self) -> None: ...

    def get_symbols_info(self, market_data: bool) -> Optional[pd.DataFrame]: ...

    def get_ticks_from(
        self,
        symbol: str,
        date_ref: datetime,
        ticks_qty: int,
        type_ticks: int,
    ) -> Optional[pd.DataFrame]: ...

    def get_last_tick(self, symbol: str) -> Optional[Any]: ...  # noqa ANN401
```

- [ ] **Step 2: Create `mt5.py`**

Copy `MT5` class verbatim (including the nested `type_2`, `type_3`, and `exp_time`
inner functions decorated with `@type_checker`), then:
1. Add import:
   ```python
   from stpstone.utils.trading_platforms.mt5._ports import IMT5
   ```
2. Add `NotImplementedError` to class and `__init__` docstring `Raises` sections.
3. Add port-contract guard as the last line of `__init__`:
   ```python
   if not isinstance(self, IMT5):
       raise NotImplementedError(
           f"{type(self).__name__} does not satisfy IMT5 — "
           "implement initialize(), shutdown(), get_symbols_info(), "
           "get_ticks_from(), and get_last_tick()."
       )
   ```

- [ ] **Step 3: Update test import**

In `tests/unit/test_mt5.py`, find the conditional import and change:
```python
# before  (inside conditional block)
from stpstone.utils.trading_platforms.mt5 import MT5
# after
from stpstone.utils.trading_platforms.mt5.mt5 import MT5
```

- [ ] **Step 4: Create `examples/mt5.py`**

```python
"""Minimal usage example for MetaTrader5 interface."""

import logging
from unittest.mock import MagicMock, patch

from stpstone.utils.trading_platforms.mt5.mt5 import MT5


def main() -> None:
    """Connect to MT5 and list available symbols."""
    logger = logging.getLogger(__name__)
    with patch("stpstone.utils.trading_platforms.mt5.mt5.mt5") as mock_mt5:
        mock_mt5.initialize.return_value = True
        mock_mt5.last_error.return_value = (0, "")

        client = MT5(logger=logger)
        ok = client.initialize(
            path="/path/to/terminal64.exe",
            login=12345678,
            server="Demo-Server",
            password="placeholder",
        )
        print(f"Connected: {ok}")
        client.shutdown()


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Delete the old flat file**

```bash
git rm stpstone/utils/trading_platforms/mt5.py
```

- [ ] **Step 6: Verify**

```bash
make test_feat MODULE=mt5
```

Expected: `[✓] All checks passed for mt5`

- [ ] **Step 7: Commit**

```bash
git add stpstone/utils/trading_platforms/mt5/ \
        tests/unit/test_mt5.py \
        examples/mt5.py
git commit -m "refactor(mt5): convert flat module to namespace package"
```

---

## Task 6: Refactor `line_b3`

**Source:** `stpstone/utils/providers/br/line_b3.py` — 7 classes with inheritance chain.
`ConnectionApi` → `Operations(ConnectionApi)` → `Resources(Operations)`;
`AccountsData(ConnectionApi)`, `DocumentsData(ConnectionApi)`,
`Professional(ConnectionApi)`, `ProfilesData(ConnectionApi)`.

**Composition target:**
```
ConnectionApi          no injected dep; owns auth, token, app_request
Operations             injects conn: IConnectionApi
Resources              injects ops: IOperations
                       (IOperations exposes app_request + exchange_limits)
AccountsData           injects conn: IConnectionApi
DocumentsData          injects conn: IConnectionApi
Professional           injects conn: IConnectionApi
ProfilesData           injects conn: IConnectionApi
```

**Files:**
- Create: `stpstone/utils/providers/br/line_b3/_dto.py` (stub — no TypedDicts)
- Create: `stpstone/utils/providers/br/line_b3/_ports.py`
- Create: `stpstone/utils/providers/br/line_b3/connection_api.py`
- Create: `stpstone/utils/providers/br/line_b3/operations.py`
- Create: `stpstone/utils/providers/br/line_b3/resources.py`
- Create: `stpstone/utils/providers/br/line_b3/accounts_data.py`
- Create: `stpstone/utils/providers/br/line_b3/documents_data.py`
- Create: `stpstone/utils/providers/br/line_b3/professional.py`
- Create: `stpstone/utils/providers/br/line_b3/profiles_data.py`
- Delete: `stpstone/utils/providers/br/line_b3.py`
- Rename: `tests/unit/test_line_b3.py` → `tests/unit/test_connection_api.py`
- Create: `tests/unit/test_operations.py`
- Create: `tests/unit/test_resources.py`
- Create: `tests/unit/test_accounts_data.py`
- Create: `tests/unit/test_documents_data.py`
- Create: `tests/unit/test_professional.py`
- Create: `tests/unit/test_profiles_data.py`
- Create: `examples/line_b3.py`

- [ ] **Step 1: Create `_dto.py` stub**

```python
# stpstone/utils/providers/br/line_b3/_dto.py
"""TypedDicts for B3 LINE API client.

No TypedDicts currently required — all request/response shapes use
plain ``dict`` and ``list[dict]``. This stub is present to satisfy the
mandatory package structure defined in ``stpstone/utils/CLAUDE.md``.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
.. [2] https://line.bvmfnet.com.br/#/endpoints
"""
```

- [ ] **Step 2: Create `_ports.py`**

```python
# stpstone/utils/providers/br/line_b3/_ports.py
"""Protocols for B3 LINE API client.

References
----------
.. [1] https://line.bvmfnet.com.br/#/endpoints
"""

from typing import Any, Literal, Optional, Protocol, Union, runtime_checkable


@runtime_checkable
class IConnectionApi(Protocol):
    """Protocol for B3 LINE API base connection."""

    def app_request(
        self,
        method: Literal["GET", "POST", "DELETE"],
        app: str,
        dict_params: Optional[dict[str, Any]],
        dict_payload: Optional[list[dict[str, Any]]],
        bool_parse_dict_params_data: bool,
        bool_retry_if_error: bool,
        float_secs_sleep: Optional[float],
        timeout: Union[tuple[float, float], float, int],
    ) -> Union[list[dict[str, Any]], int]: ...


@runtime_checkable
class IOperations(Protocol):
    """Protocol for B3 LINE market operations."""

    def app_request(
        self,
        method: Literal["GET", "POST", "DELETE"],
        app: str,
        dict_params: Optional[dict[str, Any]],
        dict_payload: Optional[list[dict[str, Any]]],
        bool_parse_dict_params_data: bool,
        bool_retry_if_error: bool,
        float_secs_sleep: Optional[float],
        timeout: Union[tuple[float, float], float, int],
    ) -> Union[list[dict[str, Any]], int]: ...

    def exchange_limits(self) -> Union[list[dict[str, Any]], int]: ...

    def groups_authorized_markets(self) -> Union[list[dict[str, Any]], int]: ...

    def intruments_per_group(self, group_id: str) -> Union[list[dict[str, Any]], int]: ...

    def authorized_markets_instruments(self) -> dict[str, Union[str, int, float]]: ...


@runtime_checkable
class IResources(Protocol):
    """Protocol for B3 LINE instrument resources."""

    def instrument_informations(self) -> Union[list[dict[str, Any]], int]: ...

    def instrument_id_by_symbol(self, symbol: str) -> Union[list[dict[str, Any]], int]: ...


@runtime_checkable
class IAccountsData(Protocol):
    """Protocol for B3 LINE account data operations."""

    def client_infos(self, account_code: str) -> Union[list[dict[str, Any]], int]: ...

    def spxi_get(self, account_id: str) -> Union[list[dict[str, Any]], int]: ...


@runtime_checkable
class IDocumentsData(Protocol):
    """Protocol for B3 LINE document operations."""

    def doc_info(self, doc_code: str) -> Union[list[dict[str, Any]], int]: ...

    def doc_profile(self, doc_id: str) -> dict[str, Union[str, int]]: ...


@runtime_checkable
class IProfessional(Protocol):
    """Protocol for B3 LINE professional data operations."""

    def professional_code_get(self) -> Union[list[dict[str, Any]], int]: ...


@runtime_checkable
class IProfilesData(Protocol):
    """Protocol for B3 LINE risk profile data."""

    def risk_profile(self) -> Union[list[dict[str, Any]], int]: ...
```

- [ ] **Step 3: Create `connection_api.py`**

Copy `ConnectionApi` class verbatim (methods: `_validate_credentials`, `_validate_url`,
`auth_header`, `access_token`, `app_request`), then:
1. Add import: `from stpstone.utils.providers.br.line_b3._ports import IConnectionApi`
2. Remove `class ConnectionApi(ConnectionApi)` inheritance — `ConnectionApi` is now standalone.
3. Add `NotImplementedError` to class and `__init__` docstrings.
4. Add port-contract guard at end of `__init__`:
   ```python
   if not isinstance(self, IConnectionApi):
       raise NotImplementedError(
           f"{type(self).__name__} does not satisfy IConnectionApi — "
           "implement app_request()."
       )
   ```

- [ ] **Step 4: Create `operations.py`**

Lift `Operations` methods from the source file. Replace `(ConnectionApi)` inheritance with
constructor injection:

```python
# stpstone/utils/providers/br/line_b3/operations.py
"""B3 LINE API — market operations adapter."""

from typing import Any, Union

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.providers.br.line_b3._ports import IConnectionApi, IOperations


class Operations(metaclass=TypeChecker):
    """Adapter for B3 LINE market exchange-limit operations.

    ...class docstring with Raises section including NotImplementedError...
    """

    def __init__(self, conn: IConnectionApi) -> None:
        """Initialise Operations with a LINE API connection.

        Parameters
        ----------
        conn : IConnectionApi
            Authenticated LINE API connection.

        Raises
        ------
        NotImplementedError
            If this class does not satisfy the ``IOperations`` port.
        """
        self._conn = conn
        if not isinstance(self, IOperations):
            raise NotImplementedError(
                f"{type(self).__name__} does not satisfy IOperations."
            )

    def app_request(self, *args: Any, **kwargs: Any) -> Union[list[dict[str, Any]], int]:
        """Delegate app_request to the injected connection.

        Parameters
        ----------
        *args : Any
            Positional arguments forwarded to ``IConnectionApi.app_request``.
        **kwargs : Any
            Keyword arguments forwarded to ``IConnectionApi.app_request``.

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Response data or HTTP status code.
        """
        return self._conn.app_request(*args, **kwargs)

    # Copy exchange_limits(), groups_authorized_markets(), intruments_per_group(),
    # and authorized_markets_instruments() verbatim from the source file,
    # replacing `self.app_request(...)` with `self._conn.app_request(...)`.
```

- [ ] **Step 5: Create `resources.py`**

```python
# stpstone/utils/providers/br/line_b3/resources.py
"""B3 LINE API — instrument resources adapter."""

from typing import Any, Union

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.providers.br.line_b3._ports import IOperations, IResources


class Resources(metaclass=TypeChecker):
    """Adapter for B3 LINE instrument information resources.

    ...class docstring with Raises section including NotImplementedError...
    """

    def __init__(self, ops: IOperations) -> None:
        """Initialise Resources with a LINE API operations object.

        Parameters
        ----------
        ops : IOperations
            Operations adapter providing app_request and exchange_limits.

        Raises
        ------
        NotImplementedError
            If this class does not satisfy the ``IResources`` port.
        """
        self._ops = ops
        if not isinstance(self, IResources):
            raise NotImplementedError(
                f"{type(self).__name__} does not satisfy IResources."
            )

    def instrument_informations(self) -> Union[list[dict[str, Any]], int]:
        """Retrieve basic instrument information.

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Instrument data or status code.
        """
        return self._ops.app_request(method="GET", app="/api/v1.0/symbol", bool_retry_if_error=True)

    def instrument_id_by_symbol(self, symbol: str) -> Union[list[dict[str, Any]], int]:
        """Retrieve instrument ID by symbol.

        Parameters
        ----------
        symbol : str
            Instrument symbol.

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Instrument data or status code.
        """
        return self._ops.app_request(method="GET", app=f"/api/v1.0/symbol/{symbol}")

    # instrument_infos_exchange_limits() copied verbatim from source, replacing:
    #   self.exchange_limits()         → self._ops.exchange_limits()
    #   self.instrument_informations() → self.instrument_informations()  (unchanged)
```

- [ ] **Step 6: Create remaining adapters**

For each of `AccountsData`, `DocumentsData`, `Professional`, `ProfilesData`:

Pattern for all four (substitute class name, port name, and methods):
```python
class AccountsData(metaclass=TypeChecker):
    def __init__(self, conn: IConnectionApi) -> None:
        self._conn = conn
        if not isinstance(self, IAccountsData):
            raise NotImplementedError(...)
    # copy methods verbatim; replace self.app_request(...) → self._conn.app_request(...)
```

Files to create with this pattern:
- `accounts_data.py` — `AccountsData` (injects `IConnectionApi`, port `IAccountsData`)
- `documents_data.py` — `DocumentsData` (injects `IConnectionApi`, port `IDocumentsData`)
- `professional.py` — `Professional` (injects `IConnectionApi`, port `IProfessional`)
- `profiles_data.py` — `ProfilesData` (injects `IConnectionApi`, port `IProfilesData`)

In every method that previously called `self.app_request(...)`, replace with
`self._conn.app_request(...)`.

- [ ] **Step 7: Rename and update test file for `ConnectionApi`**

```bash
git mv tests/unit/test_line_b3.py tests/unit/test_connection_api.py
```

In `tests/unit/test_connection_api.py`, update import:
```python
# before
from stpstone.utils.providers.br.line_b3 import ConnectionApi
# after
from stpstone.utils.providers.br.line_b3.connection_api import ConnectionApi
```

- [ ] **Step 8: Create test files for the remaining 6 adapters**

Create `tests/unit/test_operations.py`, `test_resources.py`, `test_accounts_data.py`,
`test_documents_data.py`, `test_professional.py`, `test_profiles_data.py`.

Each test file must:
- Import the adapter from its new path.
- Mock `IConnectionApi` (or `IOperations` for `Resources`) at the boundary.
- Test `__init__` (success + `NotImplementedError` guard path).
- Test at least one business method with a mocked `app_request` response.

Example skeleton for `test_operations.py`:
```python
"""Unit tests for Operations adapter."""

from unittest.mock import MagicMock

import pytest

from stpstone.utils.providers.br.line_b3.operations import Operations
from stpstone.utils.providers.br.line_b3._ports import IConnectionApi


@pytest.fixture
def mock_conn() -> IConnectionApi:
    conn = MagicMock(spec=IConnectionApi)
    conn.app_request.return_value = [{"id": "1", "name": "Group A"}]
    return conn


class TestOperations:
    def test_init_success(self, mock_conn: IConnectionApi) -> None:
        ops = Operations(conn=mock_conn)
        assert ops._conn is mock_conn

    def test_exchange_limits_delegates_to_conn(self, mock_conn: IConnectionApi) -> None:
        ops = Operations(conn=mock_conn)
        result = ops.exchange_limits()
        mock_conn.app_request.assert_called_once()
        assert isinstance(result, list)
```

Apply the same skeleton structure to the other 5 test files, adjusting the adapter
class, port, fixture, and tested methods.

- [ ] **Step 9: Create `examples/line_b3.py`**

```python
"""Minimal usage example for B3 LINE API client."""

from unittest.mock import MagicMock

from stpstone.utils.providers.br.line_b3._ports import IConnectionApi
from stpstone.utils.providers.br.line_b3.operations import Operations
from stpstone.utils.providers.br.line_b3.resources import Resources


def main() -> None:
    """Fetch instrument info via the composed LINE API client."""
    mock_conn = MagicMock(spec=IConnectionApi)
    mock_conn.app_request.return_value = [{"id": "1", "symbol": "PETR4"}]

    ops = Operations(conn=mock_conn)
    res = Resources(ops=ops)
    print(res.instrument_informations())


if __name__ == "__main__":
    main()
```

- [ ] **Step 10: Delete the old flat file**

```bash
git rm stpstone/utils/providers/br/line_b3.py
```

- [ ] **Step 11: Verify each sub-file**

Run `make test_feat` for each of the 7 adapter files:

```bash
make test_feat MODULE=connection_api
make test_feat MODULE=operations
make test_feat MODULE=resources
make test_feat MODULE=accounts_data
make test_feat MODULE=documents_data
make test_feat MODULE=professional
make test_feat MODULE=profiles_data
```

Each must output: `[✓] All checks passed for <module>`

- [ ] **Step 12: Commit**

```bash
git add stpstone/utils/providers/br/line_b3/ \
        tests/unit/test_connection_api.py \
        tests/unit/test_operations.py \
        tests/unit/test_resources.py \
        tests/unit/test_accounts_data.py \
        tests/unit/test_documents_data.py \
        tests/unit/test_professional.py \
        tests/unit/test_profiles_data.py \
        examples/line_b3.py
git rm tests/unit/test_line_b3.py  # already staged via git mv
git commit -m "refactor(line_b3): decompose inheritance into composition package"
```
