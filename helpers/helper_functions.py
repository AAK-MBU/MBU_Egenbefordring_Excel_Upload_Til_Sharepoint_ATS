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
            AND form_type = 'egenbefordring_ifm_til_skolekoer'
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
        "adresse1",
        "anden_beloebsmodtager_",
        "antal_dage",
        "antal_km_i_alt",
        "barnets_navn",
        "beloeb_i_alt",
        "cpr_barnet",
        "cpr_nr",
        "cpr_nr_paaanden",
        "jeg_erklaerer_paa_tro_og_love_at_de_oplysninger_jeg_har_givet_er",
        "jeg_er_indforstaaet_med_at_aarhus_kommune_behandler_angivne_oply",
        "kilometer_i_alt_fra_skole",
        "kilometer_i_alt_til_skole",
        "kunne_du_ikke_finde_skole_eller_dagtilbud_paa_listen_",
        "navn_paa_anden_beloebsmodtager",
        "navn_paa_beloebsmodtager",
        "skoleliste",
        "skriv_dit_barns_skole_eller_dagtilbud",
        "takst",
        "computed_twig_tjek_for_ugenummer",
        "modtagelsesdato",
        "aendret_beloeb_i_alt",
        "godkendt",
        "godkendt_af",
        "behandlet_ok",
        "behandlet_fejl",
        "evt_kommentar",
        "test",
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

    barnets_cpr = data.get("cpr_barnet")
    koerselsliste = data.get("test", [])

    # elevens_adresse = str(data.get("adresse1")) or ""
    elevens_adresse = str(norm(data.get("adresse1"))).split(",", 1)[0].strip().replace(" ", "")

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

    for entry in koerselsliste:
        entry_date = datetime.fromisoformat(entry["dato"]).date()

        matches = find_bevillinger_for_date(bevillinger, entry_date)

        if not matches:
            out_of_bevilling_dates = True
            continue

        if len(matches) > 1:
            overlapping_bevilling_found = True
            break  # 🚫 immediate hard stop

        found_any_valid_bevilling = True
        bevilling = matches[0]

        # adresse_paa_fundet_bevilling = str(bevilling.get("bevilget_addresse")).split(",", 1)[0].strip()
        adresse_paa_fundet_bevilling = str(norm(bevilling.get("bevilget_addresse"))).split(",", 1)[0].strip().replace(" ", "")

        # --- School comparison (supports split schools) ---
        submission_school_name, submission_school_road = parse_selected_school(
            barnets_skole
        )

        bevilling_school_name = str(bevilling.get("bevilget_skole") or "")
        bevilling_school_address = str(bevilling.get("skolens_adresse") or "")
        bevilling_road = extract_road_name(bevilling_school_address)

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

        if norm(submission_school_name) != norm(bevilling_school_name):
            return build_final_row(
                data=data,
                form_id=form_id,
                modtagelsesdato=modtagelsesdato,
                submission_valid=False,
                aendret_beloeb="",
                kommentar="Indberettet skole matcher ikke barnets bevilling",
            )

        # Only check road if submission specified one
        if submission_school_road:
            if norm(submission_school_road) != norm(bevilling_road):
                return build_final_row(
                    data=data,
                    form_id=form_id,
                    modtagelsesdato=modtagelsesdato,
                    submission_valid=False,
                    aendret_beloeb="",
                    kommentar="Indberettet skoleadresse matcher ikke barnets bevilling",
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
            test_list=[entry],
            allowed_morgen=bevilling["allowed_morgen"],
            allowed_efter=bevilling["allowed_efter"],
            allowed_distance=bevilling["allowed_distance"],
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

    submission_valid = total_valid_legs > 0

    if submission_valid and comments:
        aendret_beloeb = round(total_beloeb, 2)
    else:
        aendret_beloeb = ""

    return build_final_row(
        data=data,
        form_id=form_id,
        modtagelsesdato=modtagelsesdato,
        submission_valid=submission_valid,
        aendret_beloeb=aendret_beloeb,
        kommentar="; ".join(comments),
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
def validate_entries(test_list, allowed_morgen, allowed_efter, allowed_distance):
    """
    Validate reported driving entries for a single submission date.

    Each entry is checked for:
    - Allowed morning / afternoon driving
    - Distance violations
    - Count of valid driving legs

    The function aggregates validation flags to support both
    hard rejections and adjustable corrections.

    Args:
        test_list (list[dict]): Driving entries for a single date.
        allowed_morgen (bool): Whether morning driving is allowed.
        allowed_efter (bool): Whether afternoon driving is allowed.
        allowed_distance (float): Maximum approved distance.

    Returns:
        dict: Validation results and counters.
    """

    wrong_morgen = False
    wrong_efter = False
    distance_violation = False
    distance_example = None
    valid_legs = 0

    for entry in test_list:
        km_til = convert_value_to_float(entry.get("til_skole"))
        km_fra = convert_value_to_float(entry.get("til_hjem"))

        is_valid, is_wrong, distance_violation, example = validate_leg(
            km_til, allowed_morgen, allowed_distance, distance_violation
        )
        if is_wrong:
            wrong_morgen = True
        if is_valid:
            valid_legs += 1
        if example:
            distance_example = example

        is_valid, is_wrong, distance_violation, example = validate_leg(
            km_fra, allowed_efter, allowed_distance, distance_violation
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


def validate_leg(km, allowed, allowed_distance, distance_violation):
    """
    Validate a single driving leg.

    Determines whether the leg:
    - Is present and positive
    - Is allowed for the given time slot
    - Exceeds the approved distance

    Distance violations are flagged but do not invalidate
    the leg entirely, allowing for adjusted reimbursement.

    Args:
        km (float | None): Reported distance.
        allowed (bool): Whether this leg type is allowed.
        allowed_distance (float): Approved maximum distance.
        distance_violation (bool): Existing violation state.

    Returns:
        tuple: (is_valid, is_wrong_time, distance_violation, example)
    """

    if km is None or km <= 0:
        return False, False, distance_violation, None

    if not allowed:
        return False, True, distance_violation, None

    if allowed_distance <= 0 or distance_violation or km <= allowed_distance:
        return True, False, distance_violation, None

    return True, False, True, (km, allowed_distance)


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
):
    """
    Build the final flattened row for Excel export.

    Combines original form data with system-generated fields
    such as approval flags, adjusted amount, and comments.

    Args:
        data (dict): Original form data.
        form_id (str): Submission UUID.
        modtagelsesdato (str): Submission timestamp.
        submission_valid (bool): Whether the submission is approved.
        aendret_beloeb (float | str): Adjusted reimbursement amount.
        kommentar (str): Processing comments.

    Returns:
        dict: Final row for Excel output.
    """

    row = dict(data)

    row["modtagelsesdato"] = modtagelsesdato
    row["uuid"] = form_id
    row["aendret_beloeb_i_alt"] = aendret_beloeb
    row["godkendt"] = "X" if submission_valid else ""
    row["godkendt_af"] = ""
    row["behandlet_ok"] = ""
    row["behandlet_fejl"] = ""
    row["evt_kommentar"] = kommentar

    row.setdefault("test", data.get("test"))
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
        tuple[str, str | None]: (school_name, road_name)
    """

    if not raw_school:
        return "", None

    raw_school = raw_school.strip()

    if "(" in raw_school and raw_school.endswith(")"):
        name, road = raw_school.rsplit("(", 1)
        return name.strip(), road[:-1].strip()

    return raw_school, None


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