"""
Helper functions
"""

import json
import logging
from datetime import datetime, timedelta
from io import BytesIO

import pandas as pd
import pyodbc

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------
# Date helpers
# --------------------------------------------------------------------
def get_week_dates(number_of_weeks: int = None):
    """
    Returns the start and end dates of the current week.

    The week starts Monday 00:00:00 and ends Sunday 23:59:59.
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
# Core export
# --------------------------------------------------------------------
def export_egenbefordring_from_hub(
    connection_string: str,
    start_date: str = "",
    end_date: str = "",
    sheet_name: str = "",
):
    """
    Retrieves Egenbefordring submissions, validates morning/afternoon and distance,
    recalculates payout when needed, and exports a fully structured Excel sheet.
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
            BevillingFra,
            BevillingTil
        FROM
            [RPA].[rpa].[BefordringsData]
        WHERE
            CPR = ?
            AND BevillingAfKoerselstype = 'Egenbefordring'
            AND GETDATE() BETWEEN BevillingFra AND BevillingTil
        ORDER BY
            BevilgetKoereAfstand DESC
    """

    submissions = get_items_from_query_with_params(
        connection_string,
        submissions_query,
        [start_date, end_date, start_date, end_date],
    )

    logger.info(f"Loaded {len(submissions)} submissions")

    final_rows = []

    for sub in submissions:
        row = process_submission(
            sub=sub,
            connection_string=connection_string,
            befordrings_query=befordrings_query,
        )

        final_rows.append(row)

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
    Process each individual submission.

    Rule:
    - Exactly ONE active egenbefordrings-bevilling is required
    """

    form_id = sub.get("form_id")
    modtagelsesdato = sub.get("modtagelsesdato")

    form_data = json.loads(sub.get("form_data"))
    data = form_data.get("data", {})

    barnets_cpr = data.get("cpr_barnet")

    bd_rows = get_items_from_query_with_params(connection_string=connection_string, query=befordrings_query, params=[barnets_cpr])

    # 0 active bevillinger
    if not bd_rows:
        return build_final_row(
            data=data,
            form_id=form_id,
            modtagelsesdato=modtagelsesdato,
            submission_valid=False,
            aendret_beloeb="",
            kommentar="Ingen aktiv bevilling fundet",
        )

    # 2+ active bevillinger
    if len(bd_rows) > 1:
        return build_final_row(
            data=data,
            form_id=form_id,
            modtagelsesdato=modtagelsesdato,
            submission_valid=False,
            aendret_beloeb="",
            kommentar="Borger har 2 aktive bevillinger til egenbefordring",
        )

    # Exactly one active bevilling
    bevilling = bd_rows[0]

    tid = (bevilling.get("TidspunktForBevilling") or "").lower()
    allowed_morgen = "morgen" in tid
    allowed_efter = "eftermiddag" in tid

    allowed_distance = convert_value_to_float(
        bevilling.get("BevilgetKoereAfstand")
    )

    validation = validate_entries(
        test_list=data.get("test", []),
        allowed_morgen=allowed_morgen,
        allowed_efter=allowed_efter,
        allowed_distance=allowed_distance or 0,
    )

    comments = []

    if validation["wrong_morgen"]:
        comments.append("Borger har indtastet morgen, men har kun bevilget eftermiddag")

    if validation["wrong_efter"]:
        comments.append("Borger har indtastet eftermiddag, men har kun bevilget morgen")

    if validation["distance_violation"]:
        reported, allowed = validation["distance_example"]
        comments.append(
            f"Borger har indtastet {reported} km men har kun bevilget {allowed} km"
        )

    kommentar = "; ".join(comments)

    aendret_beloeb = ""
    submission_valid = True

    if allowed_distance and comments:
        aendret_beloeb = round(
            validation["valid_legs"] * allowed_distance * 2.23, 2
        )

    return build_final_row(
        data=data,
        form_id=form_id,
        modtagelsesdato=modtagelsesdato,
        submission_valid=submission_valid,
        aendret_beloeb=aendret_beloeb,
        kommentar=kommentar,
    )


# --------------------------------------------------------------------
# Validation helpers
# --------------------------------------------------------------------
def validate_entries(
    test_list,
    allowed_morgen,
    allowed_efter,
    allowed_distance,
):
    """
    Helper function to validate the citizens driving entries.
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
            km=km_til,
            allowed=allowed_morgen,
            allowed_distance=allowed_distance,
            distance_violation=distance_violation,
        )

        if is_wrong:
            wrong_morgen = True
        if is_valid:
            valid_legs += 1
        if example:
            distance_example = example

        is_valid, is_wrong, distance_violation, example = validate_leg(
            km=km_fra,
            allowed=allowed_efter,
            allowed_distance=allowed_distance,
            distance_violation=distance_violation,
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
    km,
    allowed,
    allowed_distance,
    distance_violation,
):
    """
    Validate a single leg (morning or afternoon).
    """

    if km is None or km <= 0:
        return False, False, distance_violation, None

    if not allowed:
        return False, True, distance_violation, None

    if allowed_distance <= 0 or distance_violation or km <= allowed_distance:
        return True, False, distance_violation, None

    return True, False, True, (km, allowed_distance)


# --------------------------------------------------------------------
# Row / output helpers
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
    Build the final Excel row.
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
def get_items_from_query_with_params(
    connection_string,
    query,
    params,
):
    """
    Execute parametrized SQL query and return rows as dicts.
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
    Safely convert value to float.
    """

    if v in (None, ""):
        return None

    try:
        return float(str(v).replace(",", "."))

    except Exception:
        return None
