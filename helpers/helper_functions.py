"""
Helper functions
"""

import json

import logging

from datetime import datetime, timedelta

import pandas as pd
import pyodbc

from io import BytesIO

logger = logging.getLogger(__name__)


def export_egenbefordring_from_hub(connection_string: str, sheet_name: str, start_date: str = "", end_date: str = ""):
    """
    Retrieves 'Egenbefordring' data for the selected week range and returns an Excel file as bytes.

    Returns:
        bytes: Excel file content
        str: File name (without .xlsx extension)
    """

    # Columns modifications from your original logic
    add_columns = {
        "aendret_beloeb_i_alt": [],
        "godkendt": [],
        "godkendt_af": [],
        "behandlet_ok": [],
        "behandlet_fejl": [],
        "evt_kommentar": [],
    }

    remove_columns = ["koerselsliste_tomme_felter_tjek_"]
    move_columns_to_last = ["test", "attachments", "uuid"]

    # -------------------------------------------------------------
    # 2. Query database
    # -------------------------------------------------------------
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    query = f"""
        SELECT
            form_id,
            CASE
                WHEN JSON_VALUE(form_data, '$.completed') IS NOT NULL THEN JSON_VALUE(form_data, '$.completed')
                ELSE JSON_VALUE(form_data, '$.entity.completed[0].value')
            END as [modtagelsesdato],
            form_data
        FROM
            [RPA].[journalizing].[view_Journalizing]
        WHERE
            (
                TRY_CAST(JSON_Value(form_data, '$.completed') AS DATETIMEOFFSET) >= '{start_date}'
                AND TRY_CAST(JSON_Value(form_data, '$.completed') AS DATETIMEOFFSET) <= '{end_date}'
            )
            OR
            (
                TRY_CAST(JSON_Value(form_data, '$.entity.completed[0].value') AS DATETIMEOFFSET) >= '{start_date}'
                AND TRY_CAST(JSON_Value(form_data, '$.entity.completed[0].value') AS DATETIMEOFFSET) <= '{end_date}'
            )
            AND form_type = 'egenbefordring_ifm_til_skolekoer'
    """

    logger.info(f"\n\n{query}\n\n")

    cursor.execute(query)
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    # -------------------------------------------------------------
    # 3. Build DataFrame(s)
    # -------------------------------------------------------------
    all_dataframes = []

    for row in rows:
        form_id = row.form_id
        received_date = row.modtagelsesdato

        dt = datetime.fromisoformat(received_date)
        formatted_dt = dt.strftime("%Y-%m-%d %H:%M:%S")

        raw_json = json.loads(row.form_data)
        df = pd.json_normalize(raw_json["data"], sep="_", max_level=0)

        df["modtagelsesdato"] = formatted_dt
        df["uuid"] = form_id

        all_dataframes.append(df)

    if not all_dataframes:
        return None, None

    final_df = pd.concat(all_dataframes, ignore_index=True)

    # -------------------------------------------------------------
    # 4. Apply your existing modifiers
    # -------------------------------------------------------------
    final_df = modify_dataframe(
        final_df,
        add_columns=add_columns,
        remove_columns=remove_columns,
        move_columns_to_last=move_columns_to_last
    )

    # -------------------------------------------------------------
    # 5. Convert → Excel bytes (in-memory)
    # -------------------------------------------------------------
    excel_bytes = dataframe_to_excel_bytes(final_df, sheet_name)

    return excel_bytes


def get_week_dates(number_of_weeks: int = None):
    """
    Returns the start and end dates of the current week.

    The week is considered to start on Monday at 00:00:00 and end on Sunday at 23:59:59.
    If number_of_weeks is provided, it adjusts the current date by subtracting the specified number of weeks.

    Args:
        number_of_weeks (int, optional): Number of weeks to subtract from the current date.

    Returns:
        tuple: A tuple containing two datetime objects:
               - start_of_week: the start of the current week (Monday)
               - end_of_week: the end of the current week (Sunday)
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


def modify_dataframe(dataframe_data, add_columns=None, remove_columns=None, move_columns_to_last=None):
    """
    Modifies the DataFrame by adding, removing, or moving columns.

    Args:
        dataframe_data (pd.DataFrame): The DataFrame to modify.
        add_columns (dict, optional): Columns to add.
        remove_columns (list, optional): Columns to remove.
        move_columns_to_last (list, optional): Columns to move to the last position.

    Returns:
        pd.DataFrame: Modified DataFrame.
    """

    if add_columns:
        for col_name, col_data in add_columns.items():
            if len(col_data) == 0:
                col_data = [None] * len(dataframe_data)

            if len(col_data) != len(dataframe_data):
                raise ValueError(f"Length of values for column '{col_name}' ({len(col_data)}) does not match length of DataFrame ({len(dataframe_data)}).")

            dataframe_data[col_name] = col_data

    if remove_columns:
        dataframe_data.drop(columns=remove_columns, inplace=True)

    if move_columns_to_last:
        for col in move_columns_to_last:
            if col in dataframe_data.columns:
                cols = list(dataframe_data.columns)

                cols.append(cols.pop(cols.index(col)))

                dataframe_data = dataframe_data[cols]

            else:
                raise ValueError(f"The column '{col}' does not exist in the DataFrame.")

    return dataframe_data


def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str) -> bytes:
    """Convert a DataFrame to an Excel file and return the content as bytes."""

    stream = BytesIO()

    with pd.ExcelWriter(stream, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)

    return stream.getvalue()
