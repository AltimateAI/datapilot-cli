# REVIEW.md — datapilot-cli

Python CLI (`altimate-datapilot-cli` on PyPI) that parses dbt `manifest.json`/`catalog.json`
artifacts across dbt manifest schema versions (vendored `dbt-artifacts-parser`, v1–v12) and runs
data-quality "insight" checks over dbt projects, usable standalone or as a pre-commit hook.

## Goal: catch what CI cannot

CI runs formatting/linting/tests. You exist for correctness bugs in dbt-manifest parsing across
schema versions, node-shape assumptions, and API/CLI behavior that no linter can see. If a diff is
clean and doesn't touch any of the areas below, say `lgtm` — don't invent nits.

## Don't duplicate CI — never flag

This repo's `pre-commit` + `tox` pipeline (ruff, black) already enforces, so never re-flag:
- Formatting (black) or import ordering (`ruff` rule `I`/isort).
- Unused imports/variables, bare `except`, raise-from style (`RUF`, `RSE`), bugbear patterns (`B`),
  comprehension style (`C4`), pathlib usage (`PTH`), quote style (`Q`), pytest style (`PT`).
- Naive `datetime` construction — ruff's `DTZ` rule already catches this.
- Basic `flake8-bandit` (`S`) security smells (e.g. `assert` in non-test code, `eval`/`exec`).
- Line length (140 cols, ruff-enforced).
- Whether tests exist/pass — `tox` runs the suite; that's CI's job, not yours.
Pre-existing issues in code the diff doesn't touch are out of scope — only review changed lines
and their direct call sites.

## Evidence standard — no speculation

Before flagging a manifest/catalog-parsing issue, trace which manifest version(s) the changed code
actually runs against and check the vendored schema in
`src/vendor/dbt_artifacts_parser/parsers/manifest/manifest_vN.py` for that version — don't assume
a field exists just because it's on `ManifestV12`. For CLI/API-client changes, trace whether the
call site actually surfaces failures to the user before claiming error handling is missing.

## Severity calibration

- **Critical**: will raise/crash on a real dbt manifest version this tool claims to support (e.g.
  missing field, wrong required-ness, wrong checksum shape), or silently drops/misreports an
  insight-check result.
- **Warning**: works today but is version-fragile, dialect-fragile, or duplicates logic that will
  drift (see focus areas below); or an API failure that's swallowed instead of surfaced to the CLI
  user.
- **Nit**: naming, magic strings that have an existing constant, minor duplication.

## Focus areas — bug classes this repo actually ships

1. **Pydantic `extra="forbid"` on manifest/catalog wrapper models** (root cause of #92, #48, and
   related fixes). New dbt manifest versions add fields the `Altimate*` wrapper models don't know
   about yet; a wrapper `BaseModel` with `extra="forbid"` raises `ValidationError` and crashes
   parsing the moment dbt ships a minor version bump. Flag any new/edited model in
   `schemas/manifest.py`-style files whose `model_config` sets `extra="forbid"` (or omits it while
   inheriting a forbidding base) instead of `extra="allow"`.

2. **Fields marked required on node wrapper models** (#98, #95, #96 — `identifier`,
   `resource_type` made optional after crashing on `Disabled*`/unit-test nodes). A field required
   on the "normal" node case often isn't populated on disabled nodes, unit-test nodes, or older
   manifest versions. Flag any new required (non-`Optional`) field added to an `Altimate*` node
   model; verify it's actually present across disabled/unit-test/seed/source variants, not just
   the common model case.

3. **Version-specific field access without a version guard** (e.g. `deferred` not present on v12
   `RPCNode`; behavior tied to a specific dbt/artifact-parser version). Flag direct attribute
   access on a vendored `manifest_vN` type inside code paths meant to be version-agnostic; verify
   against the specific `manifest_vN.py` the code targets.

4. **Checksum/hash shape assumptions** (#94 — `checksum` is a dict on some node types, a
   string/object on others). Flag `.checksum.name` / `.checksum.checksum`-style access without
   confirming that node type has that structure in that manifest version.

5. **Catalog/manifest lookups assumed non-`None`** ("catalog nodes can be `None`", catalog can
   have fewer columns than the manifest). `--catalog-path` is optional per the CLI, and
   `dbt docs generate` output can be stale relative to the manifest. Flag catalog/manifest
   `.nodes[x]` / `.columns[y]` access without a `None`/`KeyError` guard in insight-check code.

6. **Mutable default arguments** (explicitly flagged in review — "list is mutable, weird things can
   happen" — not reliably caught by this repo's ruff config). Flag `def f(x=[])`, `def f(x={})`, or
   a list/dict class attribute mutated in place across calls instead of copied per-instance.

7. **Hardcoded materialization/dialect assumptions in insight checks** ("dialects may have
   multiple materializations and it might flag something we don't want to flag", "use the VIEW
   constant, we have it somewhere"). Flag magic materialization strings (`"view"`, `"table"`,
   `"incremental"`) instead of the shared constant, and any check that assumes one dialect's
   materialization vocabulary applies to all configured warehouses.

8. **Duplicated logic across check/insight classes** ("put this in the base `DBTCheck` class so
   it's not repeated", "make it a reusable util"). Flag near-identical blocks copy-pasted across
   files under the insights/checks package instead of factored onto a shared base class or util.

9. **API-client failures not surfaced to the CLI user** ("should handle exceptions and not-200s
   nicely", "get the response code and log it", "return the status so we inform the user what
   happened"). The API client intentionally doesn't raise on non-2xx (see Known-intentional) — the
   bug class is the CLI layer silently swallowing that failure instead of reporting it to the
   user. Flag call sites that ignore or only debug-log a non-2xx/error API response.

10. **Hardcoded environment-specific defaults** ("needs to be injectable from the CLI/environment,
    not hardcoded"). Flag new hardcoded endpoint/host/instance defaults added to config or client
    code that aren't also exposed as a CLI flag or env var override.

11. **Enum/resource-type completeness across manifest versions** ("how is it mapping between the
    enums", "not sure all types have labels"). `AltimateResourceType`/`AltimateAccess`-style enums
    must track new dbt node/resource types introduced by an artifact-parser version bump. Flag a
    vendored-parser bump or new node type handled without a corresponding Altimate enum member.

12. **Release/version-bump workflow trigger conditions** (`.bumpversion.cfg`, `bump-version.yml`,
    `tag-release.yml` — "won't this always release a tag? should be manual"). Flag changes to
    release-workflow `on:` triggers; verify a routine merge to main can't unintentionally fire a
    tag/release.

## Repo invariants & landmines

- `Manifest = Union[ManifestV1, ..., ManifestV12]` (`schemas/manifest.py`) — code that pattern-
  matches or accesses fields without checking manifest version breaks on the boundary versions.
- `src/vendor/dbt_artifacts_parser` is a deliberately vendored/forked copy (see fix history: "use
  vendored dbt-artifacts-parser", "use forked dbt-artifacts-parser") — it exists because upstream
  didn't support something needed here; don't "clean up" divergence from upstream as if it were
  drift.
- Two distinct run modes with different data completeness: a full CLI run (`datapilot dbt
  project-health`) vs. the pre-commit hook, which partially generates manifest/catalog from only
  changed files. Manifest-loading changes need to work under both.
- Minimum supported Python is 3.7 per README — avoid relying on syntax/stdlib features unavailable
  there unless the actual tox/CI matrix has been checked.
- `--catalog-path` is optional; code paths must degrade gracefully (skip catalog-dependent
  insights) rather than assume catalog data is present.

## Known-intentional — don't flag

- `APIClient.get()` not raising on non-2xx responses — deliberate design ("safer as it passes
  things through," per review). Only flag if the *caller* drops the failure silently (focus area
  9), not the client's non-raising behavior itself.
- `print()` used for CLI-facing output — confirmed intentional ("this is the intended output, not
  really logging"). Don't suggest converting these to `logger` calls.
- The manifest wrapper not modeling unit-test nodes in certain paths — confirmed as not a real gap
  where raised ("there's no unit_test/UnitTest reference in the wrapper code" — deliberate scope).

## High-blast-radius — never auto-edit

- `src/vendor/dbt_artifacts_parser/**` — vendored upstream parser; changes must be deliberate forks
  with a stated reason, not incidental "fixes."
- `.bumpversion.cfg`, `bump-version.yml`, `tag-release.yml`, `release.sh`, `setup.py` — PyPI release
  plumbing for `altimate-datapilot-cli`.
- `pyproject.toml`, `tox.ini`, `.pre-commit-config.yaml` — changing these changes what's enforced
  repo-wide, not just this PR.
- `CHANGELOG.rst`, `AUTHORS.rst` — maintained by release tooling, not hand-edited in feature PRs.

## Comment style

- Point at exact lines; one finding per comment; most-severe first.
- Phrase as suggestions the author can accept or reject, not mandates — link a `diff` snippet if a
  concrete fix is short.
- Skip deep review of trivial diffs (README/CHANGELOG-only, formatting-only, dependency bumps with
  no vendored-parser impact).
