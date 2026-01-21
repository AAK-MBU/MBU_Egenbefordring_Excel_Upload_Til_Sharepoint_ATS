"""
Helper functions
"""

import sys

import json
import logging
from datetime import datetime, timedelta
from io import BytesIO

import pandas as pd
import pyodbc

logger = logging.getLogger(__name__)


def get_week_dates(number_of_weeks: int = None):
    """
    Returns the start and end dates of the current week.

    The week is considered to start on Monday at 00:00:00 and end on Sunday at 23:59:59.
    If number_of_weeks is provided, it adjusts the current date by subtracting the specified number of weeks.
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


def export_egenbefordring_from_hub(
    connection_string: str,
    file_name: str,
    sheet_name: str,
    start_date: str = "",
    end_date: str = "",
):
    """
    Retrieves Egenbefordring submissions, validates morning/afternoon and distance,
    recalculates payout when needed, and exports a fully structured Excel sheet.
    """

    # --------------------------------------------------------------------
    # 1. Fetch submissions
    # --------------------------------------------------------------------
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
                (
                    TRY_CAST(JSON_VALUE(form_data, '$.completed') AS DATETIMEOFFSET) BETWEEN ? AND ?
                )
                OR
                (
                    TRY_CAST(JSON_VALUE(form_data, '$.entity.completed[0].value') AS DATETIMEOFFSET) BETWEEN ? AND ?
                )
            )
            AND form_type = 'egenbefordring_ifm_til_skolekoer'
    """

    befordrings_query = """
        SELECT
            CPR,
            KortestGaaAfstand,
            BevilgetKoereAfstand,
            TidspunktForBevilling
        FROM
            [RPA].[rpa].[BefordringsData]
        WHERE
            CPR = ?
            AND BevillingAfKoerselstype = 'Egenbefordring'
            AND GETDATE() BETWEEN BevillingFra AND BevillingTil
        ORDER BY
            BevilgetKoereAfstand DESC,
            KortestGaaAfstand DESC
    """

    submissions = get_items_from_query_with_params(
        connection_string,
        submissions_query,
        [start_date, end_date, start_date, end_date],
    )

    logger.info(f"Loaded {len(submissions)} submissions")

    final_rows = []

    # --------------------------------------------------------------------
    # 2. Loop through submissions
    # --------------------------------------------------------------------
    for sub in submissions:
        form_id = sub.get("form_id")
        modtagelsesdato = sub.get("modtagelsesdato")

        form_data = json.loads(sub.get("form_data"))
        data = form_data.get("data", {})

        barnets_cpr = data.get("cpr_barnet")

        # Fetch child's befordring rows
        bd_rows = get_items_from_query_with_params(
            connection_string=connection_string,
            query=befordrings_query,
            params=[barnets_cpr],
        )

        if bd_rows:
            allowed_morgen = False
            allowed_efter = False

            for br in bd_rows or []:
                tid = (br.get("TidspunktForBevilling") or "").lower()

                if tid == "morgen":
                    allowed_morgen = True

                elif tid == "eftermiddag":
                    allowed_efter = True

                elif "morgen" in tid and "eftermiddag" in tid:
                    allowed_morgen = True
                    allowed_efter = True

            # ---------------------------
            # Determine allowed distance
            # ---------------------------
            allowed_distance = None

            # Priority 1: BevilgetKoereAfstand
            for br in bd_rows:
                if br.get("BevilgetKoereAfstand"):
                    allowed_distance = convert_value_to_float(br["BevilgetKoereAfstand"])

                    break

            # Priority 2: KortestGaaAfstand
            if allowed_distance is None:
                for br in bd_rows:
                    if br.get("KortestGaaAfstand"):
                        allowed_distance = convert_value_to_float(br["KortestGaaAfstand"])

                        break

            if allowed_distance is None:
                # We have rows, but no usable distance at all
                no_bevilling_found = True
                allowed_distance = 0

            else:
                no_bevilling_found = False

            # ---------------------------
            # Validate per day
            # ---------------------------
            test_list = data.get("test", [])

            # We only hard-fail ("ikke godkendt") if there is no bevilling at all.
            submission_valid = not no_bevilling_found

            # Flags for aggregated comments
            wrong_morgen = False
            wrong_efter = False
            distance_violation = False
            distance_example = None  # (reported, allowed)

            valid_legs = 0  # number of legs we actually pay for

            for entry in test_list:
                km_til = convert_value_to_float(entry.get("til_skole"))
                km_fra = convert_value_to_float(entry.get("til_hjem"))

                # ------------------------
                # MORGEN (til_skole)
                # ------------------------
                if km_til is not None and km_til > 0:
                    if not allowed_morgen:
                        # Wrong time slot, no payment for this leg
                        wrong_morgen = True
                    else:
                        # Time-of-day allowed ⇒ we pay for this leg
                        valid_legs += 1

                        # Check if they overtyped distance
                        if allowed_distance > 0 and km_til > allowed_distance and not distance_violation:
                            distance_violation = True
                            distance_example = (km_til, allowed_distance)

                # ------------------------
                # EFT (til_hjem)
                # ------------------------
                if km_fra is not None and km_fra > 0:
                    if not allowed_efter:
                        wrong_efter = True
                    else:
                        valid_legs += 1

                        if allowed_distance > 0 and km_fra > allowed_distance and not distance_violation:
                            distance_violation = True
                            distance_example = (km_fra, allowed_distance)

            # ---------------------------
            # Build evt_kommentar + aendret beløb
            # ---------------------------
            comments = []

            if no_bevilling_found:
                submission_valid = False
                comments.append("Ingen bevilling fundet")

            if wrong_morgen:
                comments.append("Borger har indtastet morgen, men har kun bevilget eftermiddag.")

            if wrong_efter:
                comments.append("Borger har indtastet eftermiddag, men har kun bevilget morgen.")

            if distance_violation and distance_example:
                reported, allowed = distance_example
                comments.append(
                    f"Borger har indtastet {reported} km men har kun bevilget {allowed} km."
                )

            kommentar = "; ".join(comments) if comments else ""

            # Recalculate beløb only if we actually have a bevilling + distance
            aendret_beloeb = ""

            if not no_bevilling_found and allowed_distance > 0:
                # If there were *any* comments (time or distance), we correct
                if comments:
                    try:
                        aendret_beloeb = round(valid_legs * allowed_distance * 2.23, 2)
                    except Exception:
                        aendret_beloeb = ""
                else:
                    # No violations → keep as empty (original beløb used)
                    aendret_beloeb = ""

        else:
            submission_valid = False
            aendret_beloeb = ""
            kommentar = "Ingen bevilling fundet"

        # ---------------------------
        # Build Excel row
        # ---------------------------
        row_dict = dict(data)
        row_dict["modtagelsesdato"] = modtagelsesdato
        row_dict["uuid"] = form_id

        row_dict["aendret_beloeb_i_alt"] = aendret_beloeb
        row_dict["godkendt"] = "X" if submission_valid else ""
        row_dict["godkendt_af"] = ""
        row_dict["behandlet_ok"] = ""
        row_dict["behandlet_fejl"] = ""
        row_dict["evt_kommentar"] = kommentar

        row_dict.setdefault("test", data.get("test"))
        row_dict.setdefault("attachments", data.get("attachments"))

        final_rows.append(row_dict)

    # --------------------------------------------------------------------
    # 3. Build DataFrame
    # --------------------------------------------------------------------
    df = build_dataframe(final_rows=final_rows)

    # --------------------------------------------------------------------
    # 4. Save Excel locally
    # --------------------------------------------------------------------
    output_path = f"./{file_name}.xlsx"
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)

    # --------------------------------------------------------------------
    # 5. Upload Excel to SharePoint
    # --------------------------------------------------------------------
    excel_bytes = dataframe_to_excel_bytes(df=df, sheet_name=sheet_name)

    return excel_bytes


def get_items_from_query_with_params(
    connection_string,
    query: str,
    params: list | tuple | None,
):
    """
    Executes a parameterized SQL query and returns rows as list of dicts.
    """

    result = []

    try:
        with pyodbc.connect(connection_string) as conn:
            with conn.cursor() as cursor:

                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                rows = cursor.fetchall()

                columns = [column[0] for column in cursor.description]

                result = [
                    {
                        column: value.strip() if isinstance(value, str) else value
                        for column, value in zip(columns, row)
                    }
                    for row in rows
                ]

    except pyodbc.Error as e:
        logger.info(f"Database error: {str(e)}")
        logger.info(f"{connection_string}")
        raise

    except Exception as e:
        logger.info(f"Unexpected error: {str(e)}")
        raise

    return result or None


def convert_value_to_float(v):
    """
    Docstring for convert_value_to_float
    
    :param v: Description
    """

    if v is None or v == "":
        return None

    try:
        return float(str(v).replace(",", "."))

    except Exception as e:
        logger.info(f"Hit an exception when converting to float: {e}")

        return None


def build_dataframe(final_rows: list) -> pd.DataFrame:
    """
    Docstring for build_dataframe

    :param final_rows: Description
    :type final_rows: list
    """

    df = pd.DataFrame(final_rows)
    df = df.where(pd.notnull(df), "")

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

    return df


def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str) -> bytes:
    """Convert a DataFrame to an Excel file and return the content as bytes."""

    stream = BytesIO()

    with pd.ExcelWriter(stream, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)

    return stream.getvalue()
