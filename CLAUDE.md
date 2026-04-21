# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**stpstone** is a Python package for financial data ETL, quantitative analytics, and derivatives pricing, with strong coverage of Brazilian markets (B3, ANBIMA, CVM) alongside US and global data sources.

## Commands

```bash
# Setup
make init_venv              # Create virtualenv with pyenv + Poetry
poetry install              # Install dependencies
make precommit_update       # Install pre-commit hooks

# Testing
make unit_tests             # Run all unit tests
make integration_tests      # Run integration tests
make test_cov               # Run tests + coverage report (80% minimum enforced)
make test_feat MODULE=<name> # Test a specific feature (runs ruff + codespell + pytest)
poetry run pytest tests/unit/path/to/test_file.py  # Run a single test file

# Linting
poetry run ruff check .     # Lint
poetry run ruff format .    # Format
poetry run codespell .      # Spell check

# Package
make bump_version           # Bump version
make install_dist_locally   # Build and install locally
```

## Architecture

### Module Layout

```
stpstone/
├── analytics/         # Quantitative models: derivatives pricing (Black-Scholes),
│                      #   probability distributions, portfolio optimization, risk
├── ingestion/
│   ├── abc/           # ABCIngestion base classes
│   └── countries/     # Concrete implementations: br/, us/, ww/
├── transformations/   # DataFrameValidator, BrDocsValidator, DFStandardization
└── utils/             # Shared: connections, parsers, pipelines, calendars, logging
```


### Type Validation (Metaclass)

`TypeChecker` (metaclass) and `ABCTypeCheckerMeta` enforce runtime type checking on method arguments and return values. Use `type_checker` decorator for standalone functions. Validation integrates with Pydantic.

### Pipeline Utilities

`utils/pipelines/` provides `generic_pipeline()` (sequential), `parallel_pipeline()` (threaded), and `streaming_pipeline()` (async) for composing transformation steps.

## Code Conventions

### Variable Naming (Financial Domain)

| Variable | Meaning |
|----------|---------|
| `int_wddy` | Working days per year (252) |
| `int_cddy` | Calendar days per year (365) |
| `int_nper_cd` / `int_nper_wd` | Number of periods (calendar/working days) |
| `float_pv` / `float_fv` | Present/future value (negative = buy, positive = sell) |
| `float_qty` | Quantity (positive = buy, negative = sell) |
| `float_notional` | Priced contract value |
| `date_ref` / `date_xpt` / `date_rdm` | Reference / maturity / redemption date |

### Style

- Line length: 99 characters
- Indent: tabs (not spaces)
- Quotes: double
- Docstrings: NumPy convention (`pydocstyle`)
- Imports: isort-sorted, 2 blank lines after imports
- Type annotations required on all function signatures (including `*args`, `**kwargs`, and `None` returns)

### Commits & Branches

Conventional Commits enforced by pre-commit: `type(scope): message`
Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `build`, `ci`, `perf`, `style`, `revert`, `bump`

Branch naming: `<purpose>/<description>` (e.g., `feat/user-auth`, `fix/rounding-error`, `bugfix/ingestion`)

### Testing

- Tests live in `tests/unit/` mirroring the source structure
- Use `make test_feat MODULE=<name>` when developing a feature end-to-end
- Write tests before implementation (TDD encouraged per CONTRIBUTING.md)
- Test normal operations, edge cases, error conditions, and type validation

### TypeVar vs Literal type aliases

`TypeVar` is only for **generic type parameters** — when the same type variable appears in
both input and output positions of a function/class, binding them together. Never use it
merely to name an annotation.

**Single positional constraint crashes at import (`TypeError: A single constraint is not allowed`):**
```python
# WRONG — raises TypeError on Python 3.9+
TypeStyleXLObject = TypeVar("TypeStyleXLObject", Literal["a", "b"])
```

**Use a plain Literal alias instead (Python 3.9-compatible, zero runtime cost):**
```python
# CORRECT
TypeStyleXLObject = Literal["a", "b"]
```

`TypeVar(..., bound=Literal[...])` is syntactically valid but still usually wrong: it
creates a generic variable constrained to a Literal union, which adds indirection with no
benefit when the variable is only used as a parameter annotation. Prefer the bare `Literal`
alias in those cases too. Only keep `bound=` when the TypeVar is genuinely used to link
input ↔ output types in a generic function or class.

### Core module imports

`stpstone/transformations/validation/metaclass_type_checker.py` is imported transitively by
**every** ingestion class. Never add a top-level import of a library that requires a system
C extension or native binary (e.g. `psycopg`, `pycurl`, `pyodbc`) to this file — it will
break `import TypeChecker` on any machine where that library is not installed, crashing the
entire integration test suite. Keep the import list in this module limited to pure-Python
stdlib and PyPI packages that carry no native-library requirement.

## Mandatory Verification After Every Change

**After every implementation, refactor, bugfix, or feature addition — no exceptions:**

1. Run `make test_feat MODULE=<module_name>` for **each** module that was created or modified.
2. After all modules pass, run `make lint` once globally.
3. Fix every error before declaring the task complete. Do not skip or defer.

This applies to Claude-generated code too — never trust that a subagent ran these. Always run them yourself and show the output.
