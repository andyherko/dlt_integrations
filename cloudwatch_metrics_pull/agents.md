# @test-quality-agent

## Role
Maintain local quality guardrails and unit-test discipline for this repository.

## Hooks
- Hooks live in `.githooks/` and are configured via `git config core.hooksPath .githooks`.
- `pre-commit` must run `uv run pytest -m unit`.
- `pre-push` must run `uv run pytest`.

## When code changes
- If files under `src/` change, ensure:
  - Corresponding tests under `tests/unit/` exist or are updated.
  - `.githooks/pre-commit` and `.githooks/pre-push` still call `uv run pytest`.

## Never do
- Never remove or comment out hooks.
- Never suggest bypassing hooks with `--no-verify`.
- Never replace `uv run pytest` with plain `pytest`.
