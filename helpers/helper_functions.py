"""
Helper functions
"""

import os
import json

import logging

from datetime import datetime, timedelta

import pandas as pd
import pyodbc

import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows

logger = logging.getLogger(__name__)


def export_egenbefordring_from_hub(connection_string: str, temp_path: str, number_of_weeks: int = None):
    """
    Retrieves 'Egenbefordring' data for the current week from the database and exports it to an Excel file.

    Args:
        connection_string (str): The database connection string.
        temp_path (str): The path where the Excel file will be saved.

    The function performs the following steps:
        - Retrieves the start and end dates for the current week.
        - Queries the database for records that fall within the week.
        - Normalizes and formats the JSON data retrieved.
        - Exports the normalized data to an Excel file with the current week's details.
    """

    current_week_start, current_week_end = get_week_dates(
        number_of_weeks=number_of_weeks
    )

    start_date = current_week_start.strftime("%Y-%m-%d %H:%M:%S")
    end_date = current_week_end.strftime("%Y-%m-%d %H:%M:%S")

    current_week_number = datetime.date(
        datetime.now() - timedelta(weeks=number_of_weeks)
        if number_of_weeks
        else datetime.now()
    ).isocalendar()[1]

    file_name = f"Egenbefordring_{current_week_number}_{current_week_start.strftime('%d%m%Y')}_{current_week_end.strftime('%d%m%Y')}"

    xl_sheet_name = f"{current_week_number}_{datetime.now().year}"

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
    result = cursor.fetchall()

    file_path = rf"{temp_path}\{file_name}.xlsx"

    for row in result:
        form_id = row.form_id

        received_date = row.modtagelsesdato

        datetime_obj = datetime.fromisoformat(received_date)
        formatted_datetime_str = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

        json_data = json.loads(row.form_data)
        json_data_normalized = pd.json_normalize(
            json_data["data"], sep="_", max_level=0
        )

        json_data_normalized["modtagelsesdato"] = formatted_datetime_str
        json_data_normalized["uuid"] = form_id

        export_to_excel(
            file_path,
            f"{xl_sheet_name}",
            json_data_normalized,
            add_columns,
            remove_columns,
            move_columns_to_last,
        )

    cursor.close()
    conn.close()

    return file_path, file_name


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


def export_to_excel(file_path, sheet_name, dataframe_data, add_columns=None, remove_columns=None, move_columns_to_last=None):
    """
    Exports a pandas DataFrame to an Excel file. If the file exists, it appends the data to the specified sheet.
    If the file does not exist, it creates a new Excel file with the data.

    Args:
        file_path (str): The path to the Excel file.
        sheet_name (str): The name of the sheet to append the data to.
        dataframe_data (pd.DataFrame): The pandas DataFrame containing the data to export.
        add_columns (dict, optional): Dictionary of columns to add, where keys are column names and values are the data for the columns.
        remove_columns (list, optional): List of column names to remove from the DataFrame.
        move_columns_to_last (list, optional): List of column names to move to the last position.

    Raises:
        ValueError: If the sheet name does not exist in the existing workbook or if the lengths of add_columns values do not match the DataFrame length.
    """

    dataframe_data = modify_dataframe(dataframe_data, add_columns, remove_columns, move_columns_to_last)

    if os.path.isfile(file_path):
        append_to_existing_sheet(file_path, sheet_name, dataframe_data)

    else:
        create_new_excel(file_path, sheet_name, dataframe_data)


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


def append_to_existing_sheet(file_path, sheet_name, dataframe_data):
    """
    Appends data to an existing sheet in an Excel file.

    Args:
        file_path (str): The path to the Excel file.
        sheet_name (str): The name of the sheet to append the data to.
        dataframe_data (pd.DataFrame): The pandas DataFrame containing the data to append.

    Raises:
        ValueError: If the sheet name does not exist in the existing workbook.
    """

    workbook = openpyxl.load_workbook(file_path)
    if sheet_name not in workbook.sheetnames:
        raise ValueError(f"The sheet name '{sheet_name}' does not exist in the workbook.")

    sheet = workbook[sheet_name]
    for row in dataframe_to_rows(dataframe_data, header=False, index=False):
        row = [str(cell) if cell is not None else "" for cell in row]

        sheet.append(row)

    workbook.save(file_path)

    workbook.close()


def create_new_excel(file_path, sheet_name, dataframe_data):
    """
    Creates a new Excel file with the provided data.

    Args:
        file_path (str): The path to the Excel file.
        sheet_name (str): The name of the sheet to create.
        dataframe_data (pd.DataFrame): The pandas DataFrame containing the data to export.
    """

    with pd.ExcelWriter(path=file_path, engine='openpyxl') as writer:  # pylint: disable=abstract-class-instantiated
        dataframe_data.to_excel(writer, index=False, sheet_name=sheet_name)
