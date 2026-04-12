# Claude Code — Project Coding Standards

This file documents conventions Claude Code must follow when working in this repository. These guidelines apply regardless of programming language unless a language-specific section states otherwise.

---

## Module Structure

### One Class Per File

Each source file must contain exactly **one public class**.

- Public classes: one per file, named after the file (`user_service` → `UserService`).
- Private/shared base classes: allowed in their own file with a leading underscore prefix (`_base_ingestion`). Must not appear in the same file as a public class.
- Utility functions with no shared state or lifecycle: write them as module-level functions, not wrapped in a utility class.

**Why:** Single-class files make version history accurate, keep test files focused, and eliminate the implicit coupling that arises when two classes share a module boundary.

### Three Files per Feature Module

Every new or refactored feature module requires three files created together:

| File | Purpose |
|------|---------|
| Implementation | Business logic — the class or functions |
| Tests | Unit tests mirroring the implementation path |
| Example | Runnable usage demonstration (zero-config path) |

No module is complete until all three exist and pass CI.

---

## Code Quality

### Guard Clauses and Early Returns

Validate preconditions at the top of a function and return (or raise) immediately. Keep the happy path last and unindented. Avoid nested conditional chains.

### No Dead Code

Never comment out code. If something is removed, delete it. Version control preserves history.

### No Magic Values

Use named constants or enums for any value that is not self-evident from context.

### Explicit over Implicit

No hidden side effects. No magic conventions. Configuration via explicit parameters or environment variables, never hard-coded.

---

## Testing

- Unit tests cover pure logic; integration tests cover I/O boundaries.
- Mock at the boundary (network, filesystem, database) — never inside business logic.
- Test naming: `test_<unit>_<scenario>_<expected_outcome>`.
- Each test asserts one behaviour.
- Tests must be deterministic.

---

## Version Control

- Conventional Commits: `type(scope): message`
- Atomic commits: one logical change per commit.
- Never commit secrets, credentials, or local config files.
