"""Module to handle process finalization"""
# from mbu_rpa_core.exceptions import ProcessError, BusinessError

import logging

import shutil

from helpers import config

logger = logging.getLogger(__name__)


def finalize_process():
    """Function to handle process finalization"""

    logger.info("Remove tmp-folder.")
    shutil.rmtree(config.TMP_PATH)
