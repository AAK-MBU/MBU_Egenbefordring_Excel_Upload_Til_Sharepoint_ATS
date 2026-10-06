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
BARNETS_ADRESSE_SOURCES = ["barnets_adresse_mitid", "barnets_adresse_manuelt"]

FIELD_SOURCES = {
    "barnets_navn": ["barnets_navn_mitid", "barnets_navn_manuelt"],
    "cpr_nummer_barn": [
        "cpr_nummer_barn_mitid",
        "cpr_nummer_barn_manuelt",
        "vaelg_barn",
    ],
    "barnets_adresse": BARNETS_ADRESSE_SOURCES,
    # The form always fills this in now. The fallback is only for
    # submissions made before the field was added, which can still be
    # sitting with status 'New'; it can go once those have been drained.
    "adresse_der_blev_koert_til_fra": [
        "adresse_der_blev_koert_til_fra",
        *BARNETS_ADRESSE_SOURCES,
    ],
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
    befordring_connection_string: str = "",
):
    """
    Export egenbefordring submissions to an Excel file.

    The function:
    - Fetches submissions from journalizing within a date range
    - Validates each submission against active bevillinger
    - Applies business rules for approval and adjustment
    - Outputs a structured Excel sheet in a fixed column order

    Args:
        connection_string (str): RPA database, where the submissions live.
        start_date (str): Start of date filter (inclusive).
        end_date (str): End of date filter (inclusive).
        sheet_name (str): Name of the Excel worksheet.
        befordring_connection_string (str): Befordringssystemet, where the
            bevillinger live. Two databases, so two connection strings.

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

    # Bevillinger come from Befordringssystemet, not from the old
    # [RPA].[rpa].[BefordringsData] export.
    #
    # The grant lives on the KØRSELSRÆKKE, not the bevilling: each række
    # carries its own validity period, time of day, granted distance and
    # weekdays, and a bevilling can hold several of them. That is the detail
    # the old table flattened away, and it is why this query returns one row
    # per række rather than per bevilling.
    #
    # What the new source makes checkable that the old one could not:
    #
    #   ugedage          Koersel_Ugedag_LINK records which days were granted.
    #                    BefordringsData had no equivalent, so a claim for a
    #                    day nobody granted was simply paid.
    #   tidspunkt        a lookup value rather than free text, so "morgen" and
    #                    "eftermiddag" no longer have to be substring-matched
    #                    out of whatever a caseworker typed.
    #   gyldighed        per række, so a date is matched against the period
    #                    that actually grants it.
    #
    # STRING_AGG for the weekdays keeps this one row per række; splitting the
    # link table out would multiply the rows and the Python side would have to
    # regroup them.
    befordrings_query = """
        SELECT
            b.cpr_elev                                          AS CPR,
            k.bevilget_koereafstand_pr_vej                      AS BevilgetKoereAfstand,
            t.tidspunkt_tekst                                   AS TidspunktForBevilling,
            ba.adresse_tekst                                    AS ElevensAdresse,
            COALESCE(sm.matrikel_navn, uu.ungdomsuddannelse_navn)     AS SkoleNavnBefordring,
            COALESCE(sm.matrikel_adresse, uu.ungdomsuddannelse_adresse) AS SkolensAdresse,
            k.gyldig_fra                                        AS BevillingFra,
            k.gyldig_til                                        AS BevillingTil,
            k.kommentar                                         AS Kommentar,
            (
                SELECT STRING_AGG(u.dag_tekst, ',')
                FROM   [befordring].[Koersel_Ugedag_LINK] kul
                JOIN   [befordring].[Ugedag] u ON u.dag_id = kul.dag_id
                WHERE  kul.koersel_id = k.koersel_id
            )                                                   AS Ugedage
        FROM       [befordring].[Koersel]          k
        INNER JOIN [befordring].[Bevilling]        b  ON b.bevilling_id        = k.bevilling_id
        INNER JOIN [befordring].[Befordringstype]  bt ON bt.befordringstype_id = k.befordringstype_id
        LEFT JOIN  [befordring].[Tidspunkt]        t  ON t.tidspunkt_id        = k.tidspunkt_id
        LEFT JOIN  [befordring].[Adresse]          ba ON ba.adresse_id         = b.adresse_id
        LEFT JOIN  [befordring].[Skolematrikel]    sm ON sm.matrikel_id        = b.matrikel_id
        LEFT JOIN  [befordring].[Ungdomsuddannelse] uu ON uu.ungdomsuddannelse_id = b.ungdomsuddannelse_id
        WHERE
            b.cpr_elev = ?
            /* Compared without spaces because the lookup is not consistent
               about them - the same reason $lib/koerselstype folds them. */
            AND REPLACE(bt.befordringstype_tekst, ' ', '') = 'Egenbefordring'
            /* Soft-deleted rows on either side grant nothing. */
            AND k.aktiv = 1
            AND b.aktiv = 1
        ORDER BY
            k.gyldig_fra
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
                connection_string=befordring_connection_string or connection_string,
                befordrings_query=befordrings_query,
            )
        )

    df = pd.DataFrame(final_rows).where(pd.notnull, "")

    desired_order = [
        "barnets_navn",
        "cpr_nummer_barn",
        "barnets_adresse",
        "adresse_der_blev_koert_til_fra",
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
    - Validates school, address, time-of-day, and distance rules against
      the granted koerselsraekker in Befordringssystemet
    - Calculates adjusted reimbursement when applicable

    The function returns a single flattened row suitable for Excel export.

    Args:
        sub (dict): Submission row from journalizing.
        connection_string (str): Befordringssystemet connection string — the
            bevillinger are read from there, not from the RPA database the
            submission itself came from.
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

    # Nothing knows what a klub leg is granted or priced at, so a submission
    # claiming one cannot be settled automatically. Better a visible manual
    # check than paying the school legs and dropping the klub one in silence.
    klub_koersel = any(
        is_leg_driven(entry, "fra_klub_til_hjem") for entry in koerselsliste
    )

    if not barnets_cpr:
        return build_final_row(
            data=data,
            form_id=form_id,
            modtagelsesdato=modtagelsesdato,
            submission_valid=False,
            kommentar="Barnets CPR-nummer mangler i indberetningen",
        )

    elevens_adresse = normalize_address(get_form_field(data, "barnets_adresse"))

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
            kommentar="Ingen aktiv bevilling fundet",
        )

    bevillinger = normalize_bevillinger(bd_rows)

    total_valid_legs = 0
    total_beloeb = 0.0

    found_any_valid_bevilling = False
    conflicting_bevillinger = False

    wrong_morgen = False
    wrong_efter = False
    distance_violation = False
    distance_example = None
    out_of_bevilling_dates = False
    entries_without_date = False
    unreadable_tidspunkt = False
    bevilling_kommentar = None
    # Dates driven on a weekday the bevilling does not grant. Collected rather
    # than breaking out, so the reviewer sees every one of them at once.
    ugedag_afvigelser = []

    for entry in koerselsliste:
        entry_date = parse_entry_date(entry.get("dato"))

        if entry_date is None:
            # A blank repeat row carries no date and nothing to pay out. Only
            # worth reporting when the citizen actually ticked a leg on it,
            # since that is driving we cannot match to a bevilling.
            if is_leg_driven(entry, "til_skole") or is_leg_driven(
                entry, "til_hjem"
            ):
                entries_without_date = True
            continue

        # Everything covering the date, before narrowing by weekday. The
        # comment check runs on this wider set: a caseworker's note on ANY
        # række covering the day is worth stopping for, whether or not that
        # række grants the day itself.
        daekkende = find_bevillinger_for_date(bevillinger, entry_date)

        if not daekkende:
            out_of_bevilling_dates = True
            continue

        # A case worker only writes in Kommentar when something about the
        # bevilling needs saying, so the row is never auto-approved.
        kommenterede = [m for m in daekkende if str(m.get("kommentar") or "").strip()]

        if kommenterede:
            bevilling_kommentar = str(kommenterede[0]["kommentar"]).strip()
            break  # 🚫 immediate hard stop

        # Narrowed to the rækker that grant THIS weekday. A student can hold
        # several kørselsrækker at once — Monday/Tuesday on one, Wednesday/
        # Thursday on another — and each date has to be judged against the one
        # that actually grants it. Resolving before narrowing made every such
        # student look like a conflict.
        matches = raekker_for_dato(bevillinger, entry_date)

        if not matches:
            # The date is inside a bevilling's period but on a weekday none of
            # them grants. Collected rather than breaking, so the reviewer
            # sees every such date at once, and routed to manual handling
            # rather than rejected — see the ugedag_afvigelser block below.
            ugedag_afvigelser.append(entry_date)
            continue

        # tidspunkt comes from a lookup now, so this can only fire on a
        # kørselsrække with no tidspunkt at all. Such a række grants nothing,
        # and merging it would drop the row silently - including its distance.
        if any(not m["allowed_morgen"] and not m["allowed_efter"] for m in matches):
            unreadable_tidspunkt = True
            break  # 🚫 immediate hard stop

        bevilling = resolve_bevilling_for_date(matches)

        if bevilling is None:
            conflicting_bevillinger = True
            break  # 🚫 immediate hard stop

        found_any_valid_bevilling = True

        # Morning and afternoon are the UNION across the rækker granting this
        # day: one række may grant the trip to school and another the trip
        # home. Taking either alone would reject half the driving.
        allowed_morgen_dagen, allowed_efter_dagen = tidspunkter_for_dato(matches)

        adresse_paa_fundet_bevilling = normalize_address(bevilling.get("bevilget_addresse"))

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
                kommentar="Barnets skole forekommer ikke af bevilling",
            )

        if adresse_paa_fundet_bevilling in (None, "", 0):
            return build_final_row(
                data=data,
                form_id=form_id,
                modtagelsesdato=modtagelsesdato,
                submission_valid=False,
                kommentar="Barnets adresse forekommer ikke af bevilling",
            )

        if norm(submission_school_name) not in norm(bevilling_school_name):
            return build_final_row(
                data=data,
                form_id=form_id,
                modtagelsesdato=modtagelsesdato,
                submission_valid=False,
                kommentar="Indberettet skole matcher ikke barnets bevilling",
            )

        if elevens_adresse != adresse_paa_fundet_bevilling:
            return build_final_row(
                data=data,
                form_id=form_id,
                modtagelsesdato=modtagelsesdato,
                submission_valid=False,
                kommentar="Indberettet adresse matcher ikke barnets bevilling",
            )

        validation = validate_entries(
            entries=[entry],
            allowed_morgen=allowed_morgen_dagen,
            allowed_efter=allowed_efter_dagen,
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
    if klub_koersel:
        comments.append(
            "Kørsel fra klub til hjem kan ikke behandles automatisk"
        )
        return build_final_row(
            data=data,
            form_id=form_id,
            modtagelsesdato=modtagelsesdato,
            submission_valid=False,
            kommentar="; ".join(comments),
        )

    if bevilling_kommentar:
        comments.append(
            f"Bevillingen har en kommentar og skal behandles manuelt: {bevilling_kommentar}"
        )
        return build_final_row(
            data=data,
            form_id=form_id,
            modtagelsesdato=modtagelsesdato,
            submission_valid=False,
            kommentar="; ".join(comments),
        )

    if unreadable_tidspunkt:
        comments.append(
            "Bevillingens tidspunkt er hverken morgen eller eftermiddag"
        )
        return build_final_row(
            data=data,
            form_id=form_id,
            modtagelsesdato=modtagelsesdato,
            submission_valid=False,
            kommentar="; ".join(comments),
        )

    if ugedag_afvigelser:
        datoer = ", ".join(d.strftime("%d-%m-%Y") for d in sorted(set(ugedag_afvigelser)))
        comments.append(
            f"Der er indberettet kørsel på ugedage, bevillingen ikke dækker: {datoer}"
        )
        return build_final_row(
            data=data,
            form_id=form_id,
            modtagelsesdato=modtagelsesdato,
            submission_valid=False,
            kommentar="; ".join(comments),
        )

    if conflicting_bevillinger:
        comments.append(
            "Borger har flere aktive egenbefordrings-bevillinger "
            "med forskellige værdier på samme dato"
        )
        return build_final_row(
            data=data,
            form_id=form_id,
            modtagelsesdato=modtagelsesdato,
            submission_valid=False,
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

    # Never hand a reviewer a rejected row with no reason on it.
    if not submission_valid and not comments:
        comments.append(
            "Der er ikke indberettet kørsel på nogen af de angivne datoer"
        )

    return build_final_row(
        data=data,
        form_id=form_id,
        modtagelsesdato=modtagelsesdato,
        submission_valid=submission_valid,
        kommentar="; ".join(comments),
        beloeb=round(total_beloeb, 2),
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
                "kommentar": r.get("Kommentar"),
                "allowed_ugedage": parse_ugedage(r.get("Ugedage")),
            }
        )

    return bevillinger


# Weekday numbers as date.weekday() gives them: Monday is 0.
UGEDAG_NUMRE = {
    "mandag": 0,
    "tirsdag": 1,
    "onsdag": 2,
    "torsdag": 3,
    "fredag": 4,
}

# "Alle" in the Ugedag lookup is described as "Alle hverdage" — Monday to
# Friday, not literally every day. There is no Lørdag or Søndag in the table
# at all, so no kørselsrække can ever grant a weekend.
ALLE_HVERDAGE = frozenset(UGEDAG_NUMRE.values())


def parse_ugedage(value) -> frozenset:
    """
    The weekday numbers a kørselsrække grants.

    Args:
        value: Comma-separated Danish weekday names, as the query aggregates
            them, or None.

    Returns:
        frozenset[int]: Weekday numbers, Monday = 0.

        An EMPTY set means "no weekdays recorded", which is not the same as
        "no weekdays granted" — see weekday_mismatch, which treats it as
        unknown and checks nothing rather than rejecting everything.
    """

    if not value:
        return frozenset()

    numre = set()

    for navn in str(value).split(","):
        nøgle = navn.strip().lower()

        if nøgle == "alle":
            numre |= ALLE_HVERDAGE
        elif nøgle in UGEDAG_NUMRE:
            numre.add(UGEDAG_NUMRE[nøgle])

    return frozenset(numre)


def weekday_mismatch(bevilling, d) -> bool:
    """
    Whether a driving date falls on a weekday the bevilling does not grant.

    Args:
        bevilling (dict): A normalized bevilling.
        d (date): The date driven.

    Returns:
        bool: True when the day is outside the granted weekdays.

    Notes:
        A bevilling with NO weekdays recorded answers False. Every bevilling
        converted from the old data lacks them, and treating "not recorded" as
        "nothing granted" would reject every one of those claims — which is
        the opposite of what this check is for.

        A mismatch is NEVER an automatic rejection. It routes the submission
        to manual handling with the reason spelled out, because this is the
        first rule in the robot derived from data nobody has validated at
        scale, and the cost of being wrong is refusing a citizen money they
        are owed. Once the reviewers have seen how often it fires, and on
        what, it can be tightened.
    """

    granted = bevilling.get("allowed_ugedage") or frozenset()

    if not granted:
        return False

    return d.weekday() not in granted


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


def bevilling_values(bevilling) -> tuple:
    """
    The fields of a bevilling that decide validation and payment.

    Used to tell a row duplicated verbatim apart from two rows that really
    say different things. Date ranges are excluded: two rows covering the
    same date with otherwise identical values grant the same thing.

    TIME OF DAY is excluded too, and weekdays were never in here. A student
    legitimately holds several kørselsrækker at once — morning on Monday and
    Tuesday, afternoon on Wednesday and Thursday, both on Friday — and those
    are three different grants, not three conflicting ones. Comparing the time
    flags made every such student a conflict, and the whole submission went to
    manual handling every week.

    What is left is what genuinely cannot be reconciled: two rows covering the
    same day that disagree about the distance, the school or the address. There
    is no way to tell which one the citizen drove under.

    The time flags are instead UNIONED across the rækker that grant the day —
    see tidspunkter_for_dato.

    Args:
        bevilling (dict): A normalized bevilling.

    Returns:
        tuple: Comparable summary of the row.
    """

    return (
        bevilling["allowed_distance"],
        norm(bevilling.get("bevilget_skole")),
        normalize_address(bevilling.get("bevilget_addresse")),
    )


def resolve_bevilling_for_date(matches):
    """
    Reduce the bevillinger active on one date to the one that applies.

    Rows holding identical values are the same grant recorded more than
    once, so they collapse to the first. As soon as two rows differ in
    anything the validation uses there is no way to tell which one the
    citizen drove under, so the submission goes to manual handling.

    Args:
        matches (list[dict]): Bevillinger active on the date.

    Returns:
        dict | None: The bevilling to use, or None if the rows differ.
    """

    first = matches[0]

    for bevilling in matches[1:]:
        if bevilling_values(bevilling) != bevilling_values(first):
            return None

    return first


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


# Which form fields carry the distance for one leg of a koerselsliste entry.
#
# The citizen types a distance for every leg they drove and leaves the rest
# blank, so the presence of a value is what says the leg happened and the
# value itself is the claim. There is no tick-off field and no automatic
# lookup in this decision: nothing the citizen did not type can put a leg on
# the claim or set the km it is judged at.
#
# Several names per leg because the form asks differently depending on which
# legs are being reported - one combined field when both legs were driven,
# separate ones when only one was. First match wins.
LEG_SOURCES = {
    "til_skole": [
        "distance_koert_til_skole",
        "distance_hjem_og_skole_manuelt_indtastet",
        "distance_til_skole_manuelt_indtastet",
        "distance_manuelt_indtastet",
    ],
    "til_hjem": [
        "distance_koert_fra_skole_til_hjem",
        "distance_hjem_og_skole_manuelt_indtastet",
        "distance_til_hjem_manuelt_indtastet",
        "distance_manuelt_indtastet",
    ],
    # Detected only. Nothing prices a klub leg yet - see process_submission.
    "fra_klub_til_hjem": [
        "distance_koert_fra_klub_til_hjem",
        "distance_hjem_og_klub_manuelt_indtastet",
    ],
}


def get_reported_distance(entry: dict, leg: str) -> float | None:
    """
    The distance the citizen themselves gave for one leg of one date.

    This is the only distance the robot judges a leg by. The granted km
    on the koerselsraekke in Befordringssystemet is what it is judged
    against, and what the payout is calculated from.

    Args:
        entry (dict): One entry from koerselsliste.
        leg (str): Which leg, a key of LEG_SOURCES.

    Returns:
        float | None: Their value, or None if they gave none.

    Raises:
        KeyError: If the leg name is unknown.
    """

    for field in LEG_SOURCES[leg]:
        distance = convert_value_to_float(entry.get(field))

        if distance is not None and distance > 0:
            return distance

    return None


def is_leg_driven(entry: dict, leg: str) -> bool:
    """
    Decide whether the citizen drove one leg of one date.

    The citizen fills in a distance only for a leg they actually drove, so
    a value present is what says it happened.

    Args:
        entry (dict): One entry from koerselsliste.
        leg (str): Which leg, a key of LEG_SOURCES.

    Returns:
        bool: True if the leg was driven.
    """

    return get_reported_distance(entry, leg) is not None


def validate_entries(entries, allowed_morgen, allowed_efter, allowed_distance):
    """
    Validate reported driving entries for a single submission date.

    Each entry carries the distance the citizen typed for each leg they
    drove that day. Each such leg is checked for:
    - Allowed morning / afternoon driving
    - Distance violations against the granted km

    The function aggregates validation flags to support both
    hard rejections and adjustable corrections.

    Args:
        entries (list[dict]): Driving entries for a single date.
        allowed_morgen (bool): Whether morning driving is allowed.
        allowed_efter (bool): Whether afternoon driving is allowed.
        allowed_distance (float): Approved maximum distance per leg.

    Returns:
        dict: Validation results and counters.
    """

    wrong_morgen = False
    wrong_efter = False
    distance_violation = False
    distance_example = None
    valid_legs = 0

    for entry in entries:
        is_valid, is_wrong, distance_violation, example = validate_leg(
            is_leg_driven(entry, "til_skole"),
            allowed_morgen,
            get_reported_distance(entry, "til_skole"),
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
            is_leg_driven(entry, "til_hjem"),
            allowed_efter,
            get_reported_distance(entry, "til_hjem"),
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

    aendret_beloeb_i_alt is always left blank. The Udbetaling robot pays
    it in preference to beloeb_i_alt whenever it holds a value, so it is
    the caseworker's override and nothing else: a value this robot wrote
    there would outrank a correction they made in beloeb_i_alt. It used
    to carry this robot's own figure, back when beloeb_i_alt was the
    amount the form predicted for the citizen - with the form no longer
    predicting one, that figure belongs in beloeb_i_alt and there is no
    second amount to report.

    Args:
        data (dict): Original form data.
        form_id (str): Submission UUID.
        modtagelsesdato (str): Submission timestamp.
        submission_valid (bool): Whether the submission is approved.
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
    row["aendret_beloeb_i_alt"] = ""
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


def normalize_address(value) -> str:
    """
    Reduce an address to street name and house number for comparison.

    The two addresses being compared come from different systems - the
    citizen's is prefilled from their MitID login, the bevilling's comes
    from BefordringsData - so they agree on the street and number but not
    on how the rest is written. Everything that varies is dropped:

    - punctuation and spacing, so "H. C. Andersens Vej" matches "HC
      Andersens Vej" and "15 3. th" matches "15, 3. th"
    - the floor and door, and the postcode and city, which the two systems
      abbreviate differently ("Aarhus N" vs "Aarhus Nord") or omit entirely

    Everything from the first number onwards is treated as the house
    number, which both ends the street name and discards the tail. A
    single letter directly after it is kept, so 15A and 15B stay distinct.

    Args:
        value (any): Raw address value.

    Returns:
        str: Comparable form, e.g. "aabogade15". Empty if there is no address.
    """

    address = norm(value).replace("å", "aa").replace("ø", "oe").replace("æ", "ae")
    address = address.split(",", 1)[0]

    number_match = re.search(r"\d+", address)

    if not number_match:
        return re.sub(r"[^a-z0-9]", "", address)

    street = re.sub(r"[^a-z0-9]", "", address[: number_match.start()])
    number = number_match.group(0)

    suffix_match = re.match(r"\s*([a-z])(?![a-z])", address[number_match.end():])

    if suffix_match:
        number += suffix_match.group(1)

    return street + number


def raekker_for_dato(bevillinger, d):
    """
    The kørselsrækker that grant driving on a given date.

    Args:
        bevillinger (list[dict]): Normalized bevillinger.
        d (date): The date driven.

    Returns:
        list[dict]: Rækker covering the date AND granting its weekday.

    Notes:
        Narrowing by weekday BEFORE anything else is what makes a split week
        work. A student with Monday/Tuesday on one række and Wednesday/
        Thursday on another has two grants, not a conflict — but only if each
        date is judged against the række that actually grants it.

        A række with no weekdays recorded is kept for every day. Those are the
        bevillinger converted from the old data, where the information simply
        does not exist; excluding them would reject every claim against one.
    """

    return [
        b
        for b in find_bevillinger_for_date(bevillinger, d)
        if not weekday_mismatch(b, d)
    ]


def tidspunkter_for_dato(raekker) -> tuple[bool, bool]:
    """
    Whether morning and afternoon driving is granted on a date.

    Args:
        raekker (list[dict]): The rækker granting that date.

    Returns:
        tuple[bool, bool]: (morgen, eftermiddag).

    Notes:
        A UNION, not a single række's value. Where one række grants the
        morning leg and another the afternoon one on the same day, the
        citizen is granted both — taking either row alone would reject half
        the driving they were entitled to.
    """

    return (
        any(b["allowed_morgen"] for b in raekker),
        any(b["allowed_efter"] for b in raekker),
    )
