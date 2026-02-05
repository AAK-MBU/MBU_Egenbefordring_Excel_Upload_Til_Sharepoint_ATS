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
    return 2.28 if d >= date(2026, 1, 1) else 2.23


def to_date(value):
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
    return [
        b for b in bevillinger
        if b["from"] <= d <= b["to"]
    ]


# --------------------------------------------------------------------
# Validation helpers
# --------------------------------------------------------------------
def validate_entries(test_list, allowed_morgen, allowed_efter, allowed_distance):
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
    if v in (None, ""):
        return None

    try:
        return float(str(v).replace(",", "."))
    except Exception:
        return None


def parse_selected_school(raw_school: str):
    """
    Splits 'Langagerskolen (Bøgeskov Høvej)' into:
    ('Langagerskolen', 'Bøgeskov Høvej')

    If no parentheses exist:
    ('Langagerskolen', None)
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
    'Bøgeskov Høvej 15, 8220 Brabrand'
    -> 'Bøgeskov Høvej'
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
    return (v or "").lower().strip()


def remove_numbers(s: str) -> str:
    return re.sub(r"\d+", "", s or "").strip()