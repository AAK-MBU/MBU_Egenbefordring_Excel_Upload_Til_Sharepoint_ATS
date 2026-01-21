"""Module to handle item processing"""
# from mbu_rpa_core.exceptions import ProcessError, BusinessError

import sys

import os
import logging

from mbu_msoffice_integration.sharepoint_class import Sharepoint

from helpers import config, helper_functions

logger = logging.getLogger(__name__)


def process_item(item_data: dict, item_reference: str):
    """Function to handle item processing"""

    assert item_data, "Item data is required"
    assert item_reference, "Item reference is required"

    sharepoint_api = Sharepoint(**config.SHAREPOINT_KWARGS)

    db_connection_string = os.getenv("DBCONNECTIONSTRINGPROD")

    file_name = item_data.get("file_name")
    sheet_name = item_data.get("sheet_name")
    start_date = item_data.get("start_date")
    end_date = item_data.get("end_date")

    logger.info("Exporting data from sql table")
    bytes_data = helper_functions.export_egenbefordring_from_hub(
        connection_string=db_connection_string,
        file_name=file_name,
        sheet_name=sheet_name,
        start_date=start_date,
        end_date=end_date,
    )

    mburpa_sharepoint_api = Sharepoint(**config.MBURPA_SHAREPOINT_KWARGS)
    mburpa_sharepoint_api.upload_file_from_bytes(binary_content=bytes_data, file_name=f"{file_name}.xlsx", folder_name="Egenbefordring")

    sys.exit()

    logger.info(f"Upload file to sharepoint: {file_name}")
    sharepoint_api.upload_file_from_bytes(binary_content=bytes_data, file_name=f"{file_name}.xlsx", folder_name=config.FOLDER_NAME)
