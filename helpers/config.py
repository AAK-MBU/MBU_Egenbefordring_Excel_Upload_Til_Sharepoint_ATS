"""Module for general configurations of the process"""

import os

MAX_RETRY = 10

# ----------------------
# Queue population settings
# ----------------------
MAX_CONCURRENCY = 100  # tune based on backend capacity
MAX_RETRIES = 3  # transient failure retries per item
RETRY_BASE_DELAY = 0.5  # seconds (exponential backoff)


# The number of times the robot retries on an error before terminating.
MAX_RETRY_COUNT = 3

# Whether the robot should be marked as failed if MAX_RETRY_COUNT is reached.
FAIL_ROBOT_ON_TOO_MANY_ERRORS = True

# Error screenshot config
SMTP_SERVER = "smtp.aarhuskommune.local"
SMTP_PORT = 25
SCREENSHOT_SENDER = "robot@friend.dk"

# Constant/Credential names
ERROR_EMAIL = "Error Email"

# SHAREPOINT stuff
# ----------------
SHAREPOINT_SITE_URL = "https://aarhuskommune.sharepoint.com/"

# SHAREPOINT_SITE_NAME = "MBU-RPA-Egenbefordring"
SHAREPOINT_SITE_NAME = "MBURPA"

DOCUMENT_LIBRARY = "Delte dokumenter"

SHAREPOINT_KWARGS = {
    "tenant": os.getenv("TENANT"),
    "client_id": os.getenv("CLIENT_ID"),
    "thumbprint": os.getenv("APPREG_THUMBPRINT"),
    "cert_path": os.getenv("GRAPH_CERT_PEM"),
    "site_url": f"{SHAREPOINT_SITE_URL}",
    "site_name": f"{SHAREPOINT_SITE_NAME}",
    "document_library": f"{DOCUMENT_LIBRARY}",
}

# FOLDER_NAME = "General"
FOLDER_NAME = "Egenbefordring"
