import os
import json
import logging
import numpy as np
import pandas as pd
import gspread

from typing import Optional
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

from utils.sheet_manager import get_current_sheet_id

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVICE_ACCOUNT_JSON_STRING = os.getenv("GOOGLE_CREDENTIALS_JSON")


def load_sheet(sheet_name: str) -> Optional[pd.DataFrame]:
    """
    Loads a worksheet from a Google Sheet and returns it as a clean DataFrame.

    Parameters:
        sheet_name (str): Worksheet/tab name to load

    Returns:
        Optional[pd.DataFrame]: DataFrame of the worksheet data, or None if not found
    """
    sheet_id = get_current_sheet_id("final_sheet")

    logger.info("Authenticating with Google Sheets API")
    try:
        service_account_info = json.loads(SERVICE_ACCOUNT_JSON_STRING)
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        credentials = Credentials.from_service_account_info(service_account_info, scopes=scopes)
        client = gspread.authorize(credentials)
    except Exception as e:
        logger.error("Authentication failed: %s", e)
        return None

    logger.info("Opening Google Sheet: %s", sheet_id)
    try:
        sheet = client.open_by_key(sheet_id)
    except Exception as e:
        logger.error("Invalid Google Sheet ID or inaccessible sheet: %s", e)
        return None

    try:
        logger.info("Loading worksheet: %s", sheet_name)
        worksheet = sheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        logger.warning("Worksheet '%s' not found", sheet_name)
        return None
    except Exception as e:
        logger.error("Error loading worksheet '%s': %s", sheet_name, e)
        return None

    logger.info("Worksheet found. Fetching values...")
    data = worksheet.get_all_values()

    if not data:
        logger.warning("Worksheet '%s' is empty", sheet_name)
        return None

    # First row is header
    headers = data[0]
    rows = data[1:]

    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=headers)

    # Basic sanitization
    df.replace(["", " ", "None"], np.nan, inplace=True)
    df = df.apply(lambda col: pd.to_datetime(col, errors="ignore") if col.dtype == object else col)

    logger.info("Loaded worksheet '%s' with %d rows and %d columns",
                sheet_name, df.shape[0], df.shape[1])

    return df