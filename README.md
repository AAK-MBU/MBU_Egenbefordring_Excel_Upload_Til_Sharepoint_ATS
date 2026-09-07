# Kørselsgodtgørelse for Skolekørsler — Create Excel & Upload to SharePoint

This robot is part of the 'MBU Koerselsgodtgoerelse Skolekoersler' process. It runs on
[Automation Server](https://github.com/odense-rpa/automation-server-client) (hence the `_ATS` suffix on the repo name).

Once a week it exports the previous week's *egenbefordring* (parent-driven school transport) claims from the
RPA database, validates each claim against the citizen's active bevilling, writes the result to an Excel file
and uploads it to SharePoint at `MBU - RPA - Egenbefordring/Delte dokumenter/General`. Personnel then review
the file and move it to `.../General/Til udbetaling`.

## Running

The robot is a single entry point with three modes, dispatched by command-line flag:

| Flag | Stage | What it does |
| --- | --- | --- |
| `--queue` | Populate | Adds one work item for the previous ISO week (skipped if its reference is already in the queue) |
| `--process` | Process | Exports, validates and uploads the Excel file for each queued item |
| `--finalize` | Finalize | Currently a no-op |

```bash
uv sync
uv run python main.py --queue
uv run python main.py --process
uv run python main.py --finalize
```

### Arguments

No process arguments. Configuration comes from environment variables:

| Variable | Purpose |
| --- | --- |
| `ATS_URL`, `ATS_TOKEN` | Automation Server API |
| `DBCONNECTIONSTRINGPROD` | SQL Server connection string |
| `TENANT`, `CLIENT_ID`, `APPREG_THUMBPRINT`, `GRAPH_CERT_PEM` | SharePoint certificate authentication |

A local `.env` file is picked up automatically.

## What the robot does

1. **Queue** — Computes the previous ISO week's Monday 00:00:00 → Sunday 23:59:59 range and queues a single item
   holding the file name, sheet name and date range. The file name
   (`Egenbefordring_<uge>_<ddmmyyyy>_<ddmmyyyy>`) doubles as the queue reference, so re-running `--queue` for a week
   that is already queued adds nothing.

2. **Export** — Reads submissions from `[RPA].[journalizing].[view_Journalizing]` where
   `form_type = 'indberetning_af_egenbefordring'` and `status = 'New'` within the date range.

   The form delivers several values from more than one source: fields suffixed `_mitid` are prefilled from the
   citizen's MitID login, fields suffixed `_manuelt` are typed by hand when no prefill is available, and the
   child's CPR number falls back once more to `vaelg_barn`. They are collapsed into a single value before
   export, so the sheet keeps one column per value; the MitID value takes precedence, then the manual one.

3. **Validate** — Each submission is matched against the child's bevillinger in `[RPA].[rpa].[BefordringsData]`
   (`BevillingAfKoerselstype = 'Egenbefordring'`), per driving date:

   - **Rejected** (`godkendt` = false) when the child's CPR number is missing from the submission, there is no
     active bevilling, more than one bevilling covers the same date, the bevilling has no school or address, the
     reported school does not match the bevilling's school, the reported street name and house number do not
     match the bevilling's address, or no valid driving legs remain.

     Addresses are compared on street name and house number only. Floor, door, postcode and city are dropped,
     because the citizen's address is prefilled from MitID while the bevilling's comes from BefordringsData and
     the two write the tail differently; punctuation and spacing are ignored too.
   - **Approved with adjustment** (`aendret_beloeb_i_alt` set, reason in `evt_kommentar`) when morning/afternoon
     driving was reported outside what was granted, the reported distance exceeds the granted km, or some dates
     fall outside the active bevillinger.
   - The amount is `antal gyldige ture × bevilget afstand × takst`, where the takst is 2,23 kr./km before
     1 January 2026 and 2,28 kr./km from that date onwards. The rate is hard-coded in `get_takst_for_date`, not
     taken from the form, so changing it means changing the code. It is always calculated from the *granted* distance,
     never from the reported one.

   The form no longer shows the citizen a predicted amount, so `beloeb_i_alt` is calculated by the robot for
   every submission rather than passed through from the form. A rejected submission gets 0. `aendret_beloeb_i_alt`
   is unchanged: it repeats the same figure, but only on submissions that were approved with a correction, so a
   reviewer can see at a glance which rows were adjusted.

   Each entry in `koerselsliste` is a date on which the citizen ticks off whether they drove to school
   (`til_skole`), home again (`til_hjem`), or both. The distance is not part of the tick-off: it is looked up
   automatically into `barn_distance_til_skole_api`, and the citizen can override it per date in
   `distance_manuelt_indtastet` when the lookup is wrong or missing. The manual value wins when both are
   present. Both distances are one-way, per leg, matching `BevilgetKoereAfstand` on the bevilling.

   `barn_distance_til_skole_api` is carried into the Excel as its own column so a reviewer can see the distance
   a violation was judged against; a per-date override is visible inside the `koerselsliste` column.

4. **Upload** — The rows are written to an in-memory `.xlsx` in a fixed column order and uploaded to SharePoint.
   The column order is a contract with the Udbetaling robot — do not reorder or rename columns without
   updating that robot.

## Development

```bash
uv sync                # install dependencies (Python 3.13)
uv run ruff check .    # lint — this is what CI enforces
```

Pull requests to `main` must bump `version` in `pyproject.toml`; a GitHub Action fails the PR otherwise.

## Related robots

The process is two robots working in sequence:

1. **Create Excel & Upload to SharePoint** — this repository. Exports and validates the week's claims, and puts
   the file in `General` for personnel to review. They move the reviewed file to `General/Til udbetaling`.
2. **Udbetaling** — reads the reviewed file, creates an outlay ticket in OPUS for every approved row with the
   citizen's receipt attached, then writes the file back to `General/Behandlet` or `General/Fejlet`:
   [rpa-udbetaling-af-egenbefordring](https://github.com/AAK-MBU/rpa-udbetaling-af-egenbefordring)

Both run on Automation Server. The `desired_order` column list in this repository is the schema contract between
them — see that robot's README for the two places that have to agree with it.
