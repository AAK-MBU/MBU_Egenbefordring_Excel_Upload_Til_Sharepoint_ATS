"""Module to handle item processing"""
# from mbu_rpa_core.exceptions import ProcessError, BusinessError

import logging

from mbu_msoffice_integration.sharepoint_class import Sharepoint

from helpers import config

logger = logging.getLogger(__name__)


def process_item(item_data: dict, item_reference: str):
    """Function to handle item processing"""

    assert item_data, "Item data is required"
    assert item_reference, "Item reference is required"

    file_path = item_data.get("file_path")

    logger.info(f"Upload file to sharepoint: {file_path}")

    sp = Sharepoint(**config.SHAREPOINT_KWARGS)

    sp.upload_file(folder_name=config.FOLDER_NAME, file_path=file_path)
