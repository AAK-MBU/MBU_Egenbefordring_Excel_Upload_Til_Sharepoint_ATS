"""
Helper functions
"""

import json
import logging

import re

from datetime import datetime, timedelta, date
from io import BytesIO

import pandas as pd
import pyodbc

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------
# Date helpers
# --------------------------------------------------------------------
def get_week_dates(number_of_weeks: int = None):
    """
    Return start and end timestamps for a week.

    If number_of_weeks is provided, the calculation is offset that many
    weeks back from the current date. Otherwise, the current week is used.

    The returned range always spans from Monday 00:00:00 to Sunday 23:59:59.

    Args:
        number_of_weeks (int, optional): Number of weeks to subtract from today.

    Returns:
        tuple[datetime, datetime]: (start_of_week, end_of_week)
    """

    today = (
        datetime.now() - timedelta(weeks=number_of_weeks)
        if number_of_weeks
        else datetime.now()
    )

    start_of_week = today - timedelta(days=today.weekday())
    start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_week = start_of_week + timedelta(days=6, seconds=86399)

    return start_of_week, end_of_week


# --------------------------------------------------------------------
# Takst helpers
# --------------------------------------------------------------------
def get_takst_for_date(d: date) -> float:
    """
    Return the applicable reimbursement rate (takst) for a given date.

    The rate changes from 1 January 2026 and forward.

    Args:
        d (date): Date to evaluate.

    Returns:
        float: Takst value for the given date.
    """

    return 2.28 if d >= date(2026, 1, 1) else 2.23


def to_date(value):
    """
    Convert a datetime or date object to a date.

    This helper ensures consistent date comparisons when values
    may be returned as either datetime or date from the database.

    Args:
        value (datetime | date): Value to convert.

    Returns:
        date: Converted date value.

    Raises:
        TypeError: If the value is not a supported type.
    """

    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raise TypeError(f"Unsupported date type: {type(value)}")


# --------------------------------------------------------------------
# Form field access
# --------------------------------------------------------------------
# The form can deliver the same logical value from two different places:
# fields suffixed _mitid are prefilled from the citizen's MitID login, and
# fields suffixed _manuelt are typed by hand when no prefill is available
# (for example when the child is not registered under the logged-in parent).
#
# The variants are collapsed back into a single value before export, so the
# sheet keeps one column per value rather than one per source. The candidate
# list is ordered most to least trustworthy: MitID is the verified source and
# wins, a hand-typed value comes next, and anything after that is a last
# resort used only when the earlier sources are empty.
#
# Each key is both the logical field name used by the validation and the
# column name in the Excel output, so the two can never drift apart.
FIELD_SOURCES = {
    "barnets_navn": ["barnets_navn_mitid", "barnets_navn_manuelt"],
    "cpr_nummer_barn": [
        "cpr_nummer_barn_mitid",
        "cpr_nummer_barn_manuelt",
        "vaelg_barn",
    ],
    "barnets_adresse": ["barnets_adresse_mitid", "barnets_adresse_manuelt"],
}


def get_form_field(data: dict, field: str):
    """
    Read a logical form field, falling back across its possible sources.

    Returns the first non-empty value among the candidate keys in
    FIELD_SOURCES, so callers do not need to know whether the citizen's
    data arrived prefilled from MitID or was typed in manually.

    Args:
        data (dict): The submission's form data.
        field (str): Logical field name, a key of FIELD_SOURCES.

    Returns:
        The first non-empty value found, or None if no source is filled in.

    Raises:
        KeyError: If the logical field name is unknown.
    """

    for key in FIELD_SOURCES[field]:
        value = data.get(key)
        if value not in (None, ""):
            return value

    return None


# --------------------------------------------------------------------
# Core export
# --------------------------------------------------------------------
def export_egenbefordring_from_hub(
    connection_string: str,
    start_date: str = "",
    end_date: str = "",
    sheet_name: str = "",
):
    """
    Export egenbefordring submissions to an Excel file.

    The function:
    - Fetches submissions from journalizing within a date range
    - Validates each submission against active bevillinger
    - Applies business rules for approval and adjustment
    - Outputs a structured Excel sheet in a fixed column order

    Args:
        connection_string (str): SQL Server connection string.
        start_date (str): Start of date filter (inclusive).
        end_date (str): End of date filter (inclusive).
        sheet_name (str): Name of the Excel worksheet.

    Returns:
        bytes: Binary Excel file contents.
    """

    submissions_query = """
        SELECT
            form_id,
            CASE
                WHEN JSON_VALUE(form_data, '$.completed') IS NOT NULL
                    THEN JSON_VALUE(form_data, '$.completed')
                ELSE JSON_VALUE(form_data, '$.entity.completed[0].value')
            END AS modtagelsesdato,
            form_data
        FROM
            [RPA].[journalizing].[view_Journalizing]
        WHERE
            (
                TRY_CAST(JSON_VALUE(form_data, '$.completed') AS DATETIMEOFFSET) BETWEEN ? AND ?
                OR
                TRY_CAST(JSON_VALUE(form_data, '$.entity.completed[0].value') AS DATETIMEOFFSET) BETWEEN ? AND ?
            )
            AND form_type = 'indberetning_af_egenbefordring'
            AND status = 'New'
    """

    befordrings_query = """
        SELECT
            CPR,
            BevilgetKoereAfstand,
            TidspunktForBevilling,
            ElevensAdresse,
            SkoleNavnBefordring,
            SkolensAdresse,
            BevillingFra,
            BevillingTil
        FROM
            [RPA].[rpa].[BefordringsData]
        WHERE
            CPR = ?
            AND BevillingAfKoerselstype = 'Egenbefordring'
        ORDER BY
            BevillingFra
    """

    submissions = get_items_from_query_with_params(
        connection_string,
        submissions_query,
        [start_date, end_date, start_date, end_date],
    )

    final_rows = []

    for sub in submissions:
        final_rows.append(
            process_submission(
                sub=sub,
                connection_string=connection_string,
                befordrings_query=befordrings_query,
            )
        )

    df = pd.DataFrame(final_rows).where(pd.notnull, "")

    desired_order = [
        "barnets_navn",
        "cpr_nummer_barn",
        "barnets_adresse",
        "kunne_du_ikke_finde_skole_eller_dagtilbud_paa_listen_",
        "skoleliste",
        "skriv_dit_barns_skole_eller_dagtilbud",
        "barn_distance_til_skole_api",
        "beloebsmodtager_navn_mitid",
        "cpr_beloebsmodtager_mitid",
        "anden_beloebsmodtager_navn_manuelt",
        "cpr_anden_beloebsmodtager_manuelt",
        "total_km_beregnet",
        "beloeb_i_alt",
        "aendret_beloeb_i_alt",
        "modtagelsesdato",
        "godkendt",
        "godkendt_af",
        "behandlet_ok",
        "behandlet_fejl",
        "jeg_erklaerer_paa_tro_og_love_at_de_oplysninger_jeg_har_givet_er",
        "jeg_er_indforstaaet_med_at_aarhus_kommune_behandler_angivne_oply",
        "evt_kommentar",
        "koerselsliste",
        "attachments",
        "uuid",
    ]

    for col in desired_order:
        if col not in df.columns:
            df[col] = ""

    df = df[desired_order]

    stream = BytesIO()
    with pd.ExcelWriter(stream, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)

    return stream.getvalue()


# --------------------------------------------------------------------
# Submission processing
# --------------------------------------------------------------------
def process_submission(sub, connection_string, befordrings_query):
    """
    Process and validate a single egenbefordring submission.

    This function performs the core business logic:
    - Loads submission data and reported driving entries
    - Matches each entry to active bevillinger by date
    - Rejects overlapping or invalid bevillinger
    - Validates school, address, time-of-day, and distance rules
    - Calculates adjusted reimbursement when applicable

    The function returns a single flattened row suitable for Excel export.

    Args:
        sub (dict): Submission row from journalizing.
        connection_string (str): SQL Server connection string.
        befordrings_query (str): SQL query to fetch bevilling data.

    Returns:
        dict: Processed submission row.
    """

    form_id = sub.get("form_id")
    modtagelsesdato = sub.get("modtagelsesdato")

    form_data = json.loads(sub.get("form_data"))
    data = form_data.get("data", {})

    barnets_cpr = get_form_field(data, "cpr_nummer_barn")
    koerselsliste = data.get("koerselsliste", [])

    # Distance looked up automatically for the child; each entry may override it.
    api_distance = convert_value_to_float(data.get("barn_distance_til_skole_api"))

    if not barnets_cpr:
        return build_final_row(
            data=data,
            form_id=form_id,
            modtagelsesdato=modtagelsesdato,
            submission_valid=False,
            aendret_beloeb="",
            kommentar="Barnets CPR-nummer mangler i indberetningen",
        )

    elevens_adresse = str(norm(get_form_field(data, "barnets_adresse"))).split(",", 1)[0].strip().replace(" ", "").lower().replace("å", "aa").replace("ø", "oe").replace("æ", "ae")

    valgt_skole = data.get("skoleliste") or ""
    indtastet_skole = data.get("skriv_dit_barns_skole_eller_dagtilbud") or ""

    barnets_skole = valgt_skole.strip() or indtastet_skole.strip()

    bd_rows = get_items_from_query_with_params(
        connection_string=connection_string,
        query=befordrings_query,
        params=[barnets_cpr],
    )

    if not bd_rows:
        return build_final_row(
            data=data,
            form_id=form_id,
            modtagelsesdato=modtagelsesdato,
            submission_valid=False,
            aendret_beloeb="",
            kommentar="Ingen aktiv bevilling fundet",
        )

    bevillinger = normalize_bevillinger(bd_rows)

    total_valid_legs = 0
    total_beloeb = 0.0

    found_any_valid_bevilling = False
    overlapping_bevilling_found = False

    wrong_morgen = False
    wrong_efter = False
    distance_violation = False
    distance_example = None
    out_of_bevilling_dates = False
    entries_without_date = False

    for entry in koerselsliste:
        entry_date = parse_entry_date(entry.get("dato"))

        if entry_date is None:
            # A blank repeat row carries no date and nothing to pay out. Only
            # worth reporting when the citizen actually ticked a leg on it,
            # since that is driving we cannot match to a bevilling.
            if is_checked(entry.get("til_skole")) or is_checked(
                entry.get("til_hjem")
            ):
                entries_without_date = True
            continue

        matches = find_bevillinger_for_date(bevillinger, entry_date)

        if not matches:
            out_of_bevilling_dates = True
            continue

        if len(matches) > 1:
            overlapping_bevilling_found = True
            break  # 🚫 immediate hard stop

        found_any_valid_bevilling = True
        bevilling = matches[0]

        adresse_paa_fundet_bevilling = str(norm(bevilling.get("bevilget_addresse"))).split(",", 1)[0].strip().replace(" ", "").lower().replace("å", "aa").replace("ø", "oe").replace("æ", "ae")

        # --- School comparison (supports split schools) ---
        submission_school_name = parse_selected_school(
            barnets_skole
        )

        bevilling_school_name = str(bevilling.get("bevilget_skole") or "")

        if bevilling_school_name in (None, "", 0):
            return build_final_row(
                data=data,
                form_id=form_id,
                modtagelsesdato=modtagelsesdato,
                submission_valid=False,
                aendret_beloeb="",
                kommentar="Barnets skole forekommer ikke af bevilling",
            )

        if adresse_paa_fundet_bevilling in (None, "", 0):
            return build_final_row(
                data=data,
                form_id=form_id,
                modtagelsesdato=modtagelsesdato,
                submission_valid=False,
                aendret_beloeb="",
                kommentar="Barnets adresse forekommer ikke af bevilling",
            )

        if norm(submission_school_name) not in norm(bevilling_school_name):
            return build_final_row(
                data=data,
                form_id=form_id,
                modtagelsesdato=modtagelsesdato,
                submission_valid=False,
                aendret_beloeb="",
                kommentar="Indberettet skole matcher ikke barnets bevilling",
            )

        if remove_numbers(elevens_adresse) != remove_numbers(adresse_paa_fundet_bevilling):
            return build_final_row(
                data=data,
                form_id=form_id,
                modtagelsesdato=modtagelsesdato,
                submission_valid=False,
                aendret_beloeb="",
                kommentar="Indberettet adresse matcher ikke barnets bevilling",
            )

        validation = validate_entries(
            entries=[entry],
            allowed_morgen=bevilling["allowed_morgen"],
            allowed_efter=bevilling["allowed_efter"],
            allowed_distance=bevilling["allowed_distance"],
            api_distance=api_distance,
        )

        if validation["wrong_morgen"]:
            wrong_morgen = True
        if validation["wrong_efter"]:
            wrong_efter = True
        if validation["distance_violation"]:
            distance_violation = True
            distance_example = validation["distance_example"]

        valid_legs = validation["valid_legs"]
        total_valid_legs += valid_legs

        if valid_legs:
            takst = get_takst_for_date(entry_date)
            total_beloeb += valid_legs * bevilling["allowed_distance"] * takst

    comments = []

    # 🚫 Hard rejection cases
    if overlapping_bevilling_found:
        comments.append(
            "Borger har flere aktive egenbefordrings-bevillinger på samme dato"
        )
        return build_final_row(
            data=data,
            form_id=form_id,
            modtagelsesdato=modtagelsesdato,
            submission_valid=False,
            aendret_beloeb="",
            kommentar="; ".join(comments),
        )

    if not found_any_valid_bevilling:
        if entries_without_date and not out_of_bevilling_dates:
            comments.append(
                "Indberetningen indeholder ingen gyldige kørselsdatoer"
            )
        else:
            comments.append(
                "Indberettet kørsel ligger udenfor aktiv bevilling"
            )
        return build_final_row(
            data=data,
            form_id=form_id,
            modtagelsesdato=modtagelsesdato,
            submission_valid=False,
            aendret_beloeb="",
            kommentar="; ".join(comments),
        )

    # ⚠️ Adjustable errors
    if wrong_morgen:
        comments.append(
            "Borger har indtastet morgen, men har kun bevilget eftermiddag"
        )

    if wrong_efter:
        comments.append(
            "Borger har indtastet eftermiddag, men har kun bevilget morgen"
        )

    if distance_violation and distance_example:
        reported, allowed = distance_example
        comments.append(
            f"Borger har indtastet {reported} km men har kun bevilget {allowed} km"
        )

    if out_of_bevilling_dates:
        comments.append(
            "Borger har indtastet kørsel på datoer uden for aktive bevillinger"
        )

    if entries_without_date:
        comments.append(
            "Borger har indtastet kørsel uden dato"
        )

    submission_valid = total_valid_legs > 0

    beloeb = round(total_beloeb, 2)

    if submission_valid and comments:
        aendret_beloeb = beloeb
    else:
        aendret_beloeb = ""

    return build_final_row(
        data=data,
        form_id=form_id,
        modtagelsesdato=modtagelsesdato,
        submission_valid=submission_valid,
        aendret_beloeb=aendret_beloeb,
        kommentar="; ".join(comments),
        beloeb=beloeb,
    )


# --------------------------------------------------------------------
# Bevilling helpers
# --------------------------------------------------------------------
def normalize_bevillinger(rows):
    """
    Normalize raw bevilling rows into a structured, comparable format.

    Each bevilling is converted into a dictionary containing:
    - Active date range
    - Allowed time slots (morgen / eftermiddag)
    - Allowed distance
    - Approved school and address information

    This normalization simplifies later per-day matching and validation.

    Args:
        rows (list[dict]): Raw database rows for bevillinger.

    Returns:
        list[dict]: Normalized bevilling dictionaries.
    """

    bevillinger = []

    for r in rows:
        tid = (r.get("TidspunktForBevilling") or "").lower()

        bevillinger.append(
            {
                "from": to_date(r["BevillingFra"]),
                "to": to_date(r["BevillingTil"]),
                "allowed_morgen": "morgen" in tid,
                "allowed_efter": "eftermiddag" in tid,
                "allowed_distance": convert_value_to_float(
                    r.get("BevilgetKoereAfstand")
                ) or 0,
                "bevilget_skole": r.get("SkoleNavnBefordring"),
                "skolens_adresse": r.get("SkolensAdresse"),
                "bevilget_addresse": r.get("ElevensAdresse"),
            }
        )

    return bevillinger


def find_bevillinger_for_date(bevillinger, d):
    """
    Find all bevillinger that are active on a given date.

    Args:
        bevillinger (list[dict]): Normalized bevillinger.
        d (date): Date to match.

    Returns:
        list[dict]: Bevillinger active on the given date.
    """

    return [
        b for b in bevillinger
        if b["from"] <= d <= b["to"]
    ]


# --------------------------------------------------------------------
# Validation helpers
# --------------------------------------------------------------------
def parse_entry_date(value):
    """
    Parse the date of a single koerselsliste entry.

    The form can submit rows with an empty or malformed date, for example
    a repeat row the citizen left blank. Those must not abort the whole
    export, so an unusable value yields None for the caller to handle.

    Args:
        value (any): Raw "dato" value from the entry.

    Returns:
        date | None: Parsed date, or None if the value is unusable.
    """

    if value in (None, ""):
        return None

    try:
        return datetime.fromisoformat(str(value)).date()
    except ValueError:
        return None


def is_checked(value) -> bool:
    """
    Determine whether a checkbox-style form value is ticked.

    The form reports a driven leg as "1" and an undriven one as an empty
    value. Anything unrecognised counts as not driven, so an unexpected
    value can never inflate a reimbursement.

    Args:
        value (any): Raw value from the form.

    Returns:
        bool: True if the box is ticked.
    """

    return str(value).strip().lower() in {"1", "true", "ja", "x", "on"}


def get_entry_distance(entry: dict, api_distance: float | None) -> float | None:
    """
    Resolve the reported one-way distance for a single driving date.

    The distance to school is normally looked up automatically, but the
    citizen can type it in per date when the lookup is wrong or missing.
    A manually entered value therefore takes precedence.

    Args:
        entry (dict): One entry from koerselsliste.
        api_distance (float | None): Automatically retrieved distance.

    Returns:
        float | None: Distance to use, or None if neither source has one.
    """

    manual = convert_value_to_float(entry.get("distance_manuelt_indtastet"))
    if manual is not None and manual > 0:
        return manual

    return api_distance


def validate_entries(
    entries, allowed_morgen, allowed_efter, allowed_distance, api_distance
):
    """
    Validate reported driving entries for a single submission date.

    Each entry ticks off which legs were driven that day; the distance is
    taken from the automatic lookup unless the citizen overrode it. Each
    leg is checked for:
    - Allowed morning / afternoon driving
    - Distance violations

    The function aggregates validation flags to support both
    hard rejections and adjustable corrections.

    Args:
        entries (list[dict]): Driving entries for a single date.
        allowed_morgen (bool): Whether morning driving is allowed.
        allowed_efter (bool): Whether afternoon driving is allowed.
        allowed_distance (float): Maximum approved distance per leg.
        api_distance (float | None): Automatically retrieved distance.

    Returns:
        dict: Validation results and counters.
    """

    wrong_morgen = False
    wrong_efter = False
    distance_violation = False
    distance_example = None
    valid_legs = 0

    for entry in entries:
        reported_distance = get_entry_distance(entry, api_distance)

        drove_til_skole = is_checked(entry.get("til_skole"))
        drove_til_hjem = is_checked(entry.get("til_hjem"))

        is_valid, is_wrong, distance_violation, example = validate_leg(
            drove_til_skole,
            allowed_morgen,
            reported_distance,
            allowed_distance,
            distance_violation,
        )
        if is_wrong:
            wrong_morgen = True
        if is_valid:
            valid_legs += 1
        if example:
            distance_example = example

        is_valid, is_wrong, distance_violation, example = validate_leg(
            drove_til_hjem,
            allowed_efter,
            reported_distance,
            allowed_distance,
            distance_violation,
        )
        if is_wrong:
            wrong_efter = True
        if is_valid:
            valid_legs += 1
        if example:
            distance_example = example

    return {
        "wrong_morgen": wrong_morgen,
        "wrong_efter": wrong_efter,
        "distance_violation": distance_violation,
        "distance_example": distance_example,
        "valid_legs": valid_legs,
    }


def validate_leg(
    driven, allowed, reported_distance, allowed_distance, distance_violation
):
    """
    Validate a single driving leg.

    Determines whether the leg:
    - Was ticked off as driven
    - Is allowed for the given time slot
    - Exceeds the approved distance

    Distance violations are flagged but do not invalidate
    the leg entirely, allowing for adjusted reimbursement.

    Args:
        driven (bool): Whether the citizen ticked this leg.
        allowed (bool): Whether this leg type is allowed.
        reported_distance (float | None): Distance reported for the date.
        allowed_distance (float): Approved maximum distance per leg.
        distance_violation (bool): Existing violation state.

    Returns:
        tuple: (is_valid, is_wrong_time, distance_violation, example)
    """

    if not driven:
        return False, False, distance_violation, None

    if not allowed:
        return False, True, distance_violation, None

    if (
        reported_distance is None
        or allowed_distance <= 0
        or distance_violation
        or reported_distance <= allowed_distance
    ):
        return True, False, distance_violation, None

    return True, False, True, (reported_distance, allowed_distance)


# --------------------------------------------------------------------
# Row helpers
# --------------------------------------------------------------------
def build_final_row(
    data,
    form_id,
    modtagelsesdato,
    submission_valid,
    aendret_beloeb,
    kommentar,
    beloeb=0.0,
):
    """
    Build the final flattened row for Excel export.

    Combines original form data with system-generated fields
    such as approval flags, calculated amount, and comments.

    The form no longer shows the citizen a predicted amount, so
    beloeb_i_alt is calculated here for every submission. It defaults to
    0.0 because a rejected submission pays out nothing, which also keeps
    any future rejection path from accidentally reporting an amount.

    Args:
        data (dict): Original form data.
        form_id (str): Submission UUID.
        modtagelsesdato (str): Submission timestamp.
        submission_valid (bool): Whether the submission is approved.
        aendret_beloeb (float | str): Adjusted reimbursement amount.
        kommentar (str): Processing comments.
        beloeb (float): Calculated reimbursement amount.

    Returns:
        dict: Final row for Excel output.
    """

    row = dict(data)

    # Collapse each split-source field into the single column it maps to.
    for column in FIELD_SOURCES:
        row[column] = get_form_field(data, column)

    row["modtagelsesdato"] = modtagelsesdato
    row["uuid"] = form_id
    row["beloeb_i_alt"] = beloeb
    row["aendret_beloeb_i_alt"] = aendret_beloeb
    row["godkendt"] = "X" if submission_valid else ""
    row["godkendt_af"] = ""
    row["behandlet_ok"] = ""
    row["behandlet_fejl"] = ""
    row["evt_kommentar"] = kommentar

    row.setdefault("koerselsliste", data.get("koerselsliste"))
    row.setdefault("attachments", data.get("attachments"))

    return row


# --------------------------------------------------------------------
# DB + conversion helpers
# --------------------------------------------------------------------
def get_items_from_query_with_params(connection_string, query, params):
    """
    Execute a parameterized SQL query and return results as dictionaries.

    Ensures:
    - Safe parameter binding
    - Automatic column-to-value mapping
    - Consistent string cleanup

    Args:
        connection_string (str): SQL Server connection string.
        query (str): SQL query with placeholders.
        params (list): Parameters for the query.

    Returns:
        list[dict]: Query results.
    """

    try:
        with pyodbc.connect(connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params or [])
                rows = cursor.fetchall()
                columns = [c[0] for c in cursor.description]

                return [
                    {
                        col: val.strip() if isinstance(val, str) else val
                        for col, val in zip(columns, row)
                    }
                    for row in rows
                ]

    except Exception:
        logger.exception("Database error")
        raise


def convert_value_to_float(v):
    """
    Safely convert a value to float.

    Handles:
    - None or empty values
    - Comma-based decimal separators

    Args:
        v (any): Value to convert.

    Returns:
        float | None: Converted value or None if invalid.
    """

    if v in (None, ""):
        return None

    try:
        return float(str(v).replace(",", "."))
    except Exception:
        return None


def parse_selected_school(raw_school: str):
    """
    Parse a school selection that may contain a sub-address.

    Examples:
        'Langagerskolen (Bøgeskov Høvej)'
            -> ('Langagerskolen', 'Bøgeskov Høvej')

        'Lystrup Skole'
            -> ('Lystrup Skole', None)

    Args:
        raw_school (str): Selected school value.

    Returns:
        str
    """

    if not raw_school:
        return ""

    raw_school = raw_school.strip()

    if "(" in raw_school and raw_school.endswith(")"):
        name, road = raw_school.rsplit("(", 1)
        return name.strip()

    return raw_school


def extract_road_name(address: str):
    """
    Extract the road name from a full address string.

    Removes:
    - House numbers
    - Postal code and city

    Example:
        'Bøgeskov Høvej 15, 8220 Brabrand'
            -> 'Bøgeskov Høvej'

    Args:
        address (str): Full address.

    Returns:
        str: Road name only.
    """

    if not address:
        return ""

    # Take first part before comma
    road_part = address.split(",")[0]

    # Remove trailing house number(s)
    return "".join(
        c for c in road_part
        if not c.isdigit()
    ).strip()


# Normalize for comparison
def norm(v):
    """
    Normalize a value for safe string comparison.

    Converts None to empty string, lowercases the value,
    and strips surrounding whitespace.

    Args:
        v (any): Value to normalize.

    Returns:
        str: Normalized string.
    """

    return (v or "").lower().strip()


def remove_numbers(s: str) -> str:
    """
    Remove all numeric characters from a string.

    Primarily used to compare addresses while ignoring
    house numbers and floor indicators.

    Args:
        s (str): Input string.

    Returns:
        str: String without digits.
    """

    return re.sub(r"\d+", "", s or "").strip()
