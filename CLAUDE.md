# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Read `README.md` first — it describes what the robot does, its three run modes, the required environment
variables and the business rules. This file covers what the README does not: where things live in the code and
what breaks if you change them.

## Commands

```bash
uv sync                       # install/resolve deps into .venv (Python 3.13)
uv run ruff check .           # lint — this is what CI runs
uv run ruff check --fix .
uv run python main.py --queue | --process | --finalize
```

There are no tests in this repo. CI (`.github/workflows/`) runs Ruff and a version-bump check: **every PR to
`main` must bump `version` in `pyproject.toml` to a strictly higher value**, or the `Check Version Number` job
fails. `.pylintrc` exists but no pylint job runs; Ruff is the enforced linter.

## Architecture

Standard AAK-MBU automation-server robot skeleton: `main.py` is a thin driver dispatching on
`--queue` / `--process` / `--finalize`, and `processes/` holds one module per lifecycle stage. Almost all of the
robot's real work is in `helpers/helper_functions.py`; the `processes/` modules are thin.

**`processes/queue_handler.py`** — emits exactly one work item per run. `concurrent_add` (semaphore + exponential
backoff bulk-add) is generic machinery retained from the template and is oversized for a single item; leave it
unless you have a reason. Deduplication happens in `main.populate_queue`, which pulls existing references from
the ATS API via `helpers/ats_functions.get_workqueue_items`.

**`processes/process_item.py` → `helpers/helper_functions.export_egenbefordring_from_hub`** — the SQL queries,
the per-submission validation (`process_submission`, `validate_entries`, `validate_leg`), the DataFrame
assembly and the SharePoint upload. Two things here are load-bearing:

- The `desired_order` column list is a schema contract with the downstream Udbetaling robot
  (`AAK-MBU/rpa-udbetaling-af-egenbefordring`, checked out alongside this one). Missing columns are backfilled
  with `""` so the shape is always identical. Renaming one breaks that robot in two places: `CPR_COLUMNS` in its
  `helpers/helper_functions.py` and `COLUMNS` in its `processes/finalize_process.py`.
- School and address comparison is normalization-heavy (`norm`, `remove_numbers`, `parse_selected_school`,
  Danish æ/ø/å folding) because both sides are free text. Changes there directly change who gets paid — recent
  commits have repeatedly loosened and re-tightened these checks.

### When the form changes

The SQL deliberately returns the raw `form_data` JSON rather than projecting columns, and `build_final_row` does
`row = dict(data)` — a straight passthrough. That is what makes form fields land in the right Excel column: a
field appears in the output purely because its name is listed in `desired_order`. So a pure rename in the form
needs only a `desired_order` edit, and nothing in SQL.

What a rename does *not* cover is a value the form splits across several fields. Those go through
`FIELD_SOURCES` / `get_form_field`, which maps one name to an ordered list of candidate form keys and returns
the first non-empty one. The list runs most to least trustworthy: `_mitid` fields are prefilled from the
citizen's MitID login and win, `_manuelt` fields are typed by hand and come next. It is not limited to two —
the child's CPR falls back a third time to `vaelg_barn`, which is why the candidates are a list rather than a
pair.

Each `FIELD_SOURCES` key is deliberately both the logical name the validation reads *and* the column name in
`desired_order`. `build_final_row` writes the collapsed value under that key, which is what keeps the sheet at
one column per value instead of one per source — the raw `_mitid` / `_manuelt` keys are still on the row from
the passthrough and are dropped simply by not appearing in `desired_order`. So adding a split field means adding
one `FIELD_SOURCES` entry and one `desired_order` entry under the same name.

The `_mitid` / `_manuelt` suffixes do **not** always mark two sources for one value, so do not pair fields up by
name alone. `beloebsmodtager_navn_mitid` / `cpr_beloebsmodtager_mitid` are the logged-in citizen, and
`anden_beloebsmodtager_navn_manuelt` / `cpr_anden_beloebsmodtager_manuelt` are a different person entirely — a
second recipient, added only when the payment should go to someone else. Each has exactly one source. Coalescing
them would silently mask a designated other recipient behind the logged-in citizen, since the MitID fields are
always populated. They belong in `desired_order` only, as passthrough columns.

The remaining hard-coded form keys in `process_submission` are `koerselsliste` (the driving entries),
`barn_distance_til_skole_api`, `skoleliste` and `skriv_dit_barns_skole_eller_dagtilbud`. Those are the ones to
check first after a form change.

### Legs are ticked off, distances come from elsewhere

`til_skole` and `til_hjem` on a `koerselsliste` entry are **checkmarks** (`"1"` / empty), not kilometre values —
they were kilometres in the pre-2026 form, and code or tests written against that older shape will silently
mis-validate. `is_checked` decides whether a leg was driven and deliberately whitelists the truthy values, so an
unrecognised value reads as *not driven* rather than inflating a payout.

Entries can arrive with an empty or missing `dato` — the form submits blank repeat rows. `parse_entry_date`
returns None for those and the loop skips them rather than aborting the whole export; a skipped row is only
reported to the reviewer when a leg was actually ticked on it, since that is driving the citizen loses.

The distance for a date comes from `get_entry_distance`: the per-entry `distance_manuelt_indtastet` when the
citizen overrode the lookup, otherwise the submission-level `barn_distance_til_skole_api`. Note this precedence
is the opposite of `FIELD_SOURCES` — here the manual value is the correction and wins; there MitID is the
verified source and wins. Distances are one-way per leg, matching `BevilgetKoereAfstand`.

Reported distance is only ever used to *flag* an over-claim. The amount in `process_submission` is
`valid_legs × bevilling["allowed_distance"] × takst`, always computed from the granted distance.

`beloeb_i_alt` is no longer a form field — the form stopped showing the citizen a predicted amount — so
`build_final_row` writes the robot's own calculation into it for every submission. Its `beloeb` parameter
defaults to `0.0` so that every rejection path pays out nothing without having to say so, including any added
later; only the final return in `process_submission` passes a real figure. That default also stops a partially
accumulated total from leaking out of a mid-loop rejection, such as a school mismatch found on the second date.

`aendret_beloeb_i_alt` now holds the same number as `beloeb_i_alt` whenever it is set at all. It is kept because
its emptiness is the signal: a filled cell means the submission was approved but corrected.

`takst` and `antal_dage` were dropped from both the form and the sheet — they existed to show the citizen a
predicted amount. The rate now lives only in `get_takst_for_date`, so changing it is a code change, and there is
no column to keep in step with it.

**`processes/error_handling.py`** — `main` maps `BusinessError` → `item.pending_user()` (no mail) and wraps
anything else in `ProcessError` → `item.fail()` + error email with a screenshot. `handle_error`/`ErrorContext`
is the single entry point. SMTP settings and recipients are read from RPA database constants at send time, not
from the constants in `helpers/config.py`.

**`processes/application_handler.py`** is a stub — this robot drives no desktop application, so
`startup`/`close`/`reset` are empty.

## Conventions

- User-facing strings (Excel comments, `evt_kommentar`) are **Danish**. Keep them Danish and match the existing
  phrasing — personnel read them directly in the spreadsheet.
- `main.py` contains a commented-out TLS-verification override block marked "REMOVE BEFORE DEPLOYMENT". Leave it
  commented.
- `helpers/config.py` also holds a second, commented-out upload to the `MBURPA` SharePoint site (see
  `MBURPA_SHAREPOINT_KWARGS` and the commented block in `process_item.py`). It is intentionally inactive.
