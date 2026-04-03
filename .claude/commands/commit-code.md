---
allowed-tools: Bash, Read, Glob, Grep
description: Stage changes and create a conventional commit with structured message
argument-hint: <type> [scope] - e.g. feat auth | fix rounding | refactor ingestion
---

You are creating a git commit for this repository. Follow these steps exactly.

## 1. Gather context

Run these in parallel:
- `!git diff --staged`
- `!git diff`
- `!git status --short`
- `!git log --oneline -5`

## 2. Determine type and scope

The `$ARGUMENTS` string contains the commit type and optional scope provided by the user (e.g. `feat auth` or `fix`).

Valid types (from CONTRIBUTING.md): `build`, `ci`, `docs`, `feat`, `fix`, `perf`, `refactor`, `style`, `test`, `chore`, `revert`, `bump`.

If `$ARGUMENTS` is empty or the type is missing, infer it from the diff content.

## 3. Identify changed files

From the `git status` and `git diff` output, collect every file that was added, modified, or deleted. Group them by logical area (e.g. ingestion, analytics, tests, config).

## 4. Compose the commit message

Format (follow exactly — no deviations):

```
<type>(<scope>): <Title in sentence case, imperative mood, no period>

  - <concise topic describing what changed> → <file or files affected>
  - <concise topic describing what changed> → <file or files affected>
  ...
```

Rules:
- The title line must not exceed 72 characters.
- Omit `(<scope>)` if no scope was provided and none is obvious.
- Each bullet covers one logical change; combine trivially related files on the same bullet with `, `.
- Do not add boilerplate footers or `Co-Authored-By` lines unless explicitly asked.

## 5. Stage, commit, and push

1. If there are unstaged changes the user likely wants included, stage them with `git add` targeting specific files — never `git add -A` blindly. Ask the user if it is ambiguous which files to include.
2. Show the composed message to the user for confirmation before running `git commit`.
3. Run `git commit -m "$(cat <<'EOF' ... EOF)"` using a heredoc to preserve formatting.
4. Run `git push origin HEAD` to push the branch to the remote.
5. Report the resulting commit hash, one-line summary, and push status.
