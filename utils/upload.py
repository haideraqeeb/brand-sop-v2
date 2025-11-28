import os
import logging
import json
import numpy as np
import pandas as pd

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

from utils.sheet_manager import get_current_sheet_id

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVICE_ACCOUNT_JSON_STRING = os.getenv("GOOGLE_CREDENTIALS_JSON")


def sanitize_dataframe(df):
    # First pass: convert datetime columns to strings
    df_copy = df.copy()
    for col in df_copy.columns:
        if df_copy[col].dtype == 'datetime64[ns]':
            df_copy[col] = df_copy[col].astype(str)
    
    # Second pass: fill ALL NaN with empty string
    df_copy = df_copy.fillna('')
    
    # Third pass: replace inf with empty string
    df_copy = df_copy.replace([np.inf, -np.inf], '')
    
    # Fourth pass: convert each value to proper Python types
    result = []
    for _, row in df_copy.iterrows():
        clean_row = []
        for val in row:
            # Handle different types
            if pd.isna(val):
                clean_row.append('')
            elif isinstance(val, (np.integer, np.floating)):
                if np.isnan(val) or np.isinf(val):
                    clean_row.append('')
                else:
                    clean_row.append(float(val))  # Convert to Python float
            elif isinstance(val, pd.Timestamp):
                clean_row.append(str(val))
            elif isinstance(val, (list, tuple, dict, pd.Series)):
                # If it's a complex type, convert to string
                clean_row.append(str(val))
            else:
                # For everything else (strings, etc.)
                clean_row.append(str(val) if val != '' else '')
        result.append(clean_row)
    
    return result


def upload(df, table_name, sheet_name):
    """
    Upload the sheet into the given sheet ID and sheet name

    Parameters:
        df (pd.DataFrame): The DataFrame which needs to be uploaded
        table_name (str): The type of table which needs to be uploaded, eg: master_sheet
        sheet_name (str): The name of the worksheet to upload to, eg: Pivot Table
    """
    SHEET_ID = get_current_sheet_id(table_name)
    WORKSHEET_TITLE = sheet_name

    logger.info("Starting upload to Google Sheet: %s, worksheet: %s", SHEET_ID, WORKSHEET_TITLE)

    # Authenticate
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    logger.info("Authenticating with Google Sheets API")
    service_account_info = json.loads(SERVICE_ACCOUNT_JSON_STRING)
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    client = gspread.authorize(credentials)

    logger.info("Opening Google Sheet by key")
    sheet = client.open_by_key(SHEET_ID)

    try:
        logger.info("Trying to add new worksheet: %s", WORKSHEET_TITLE)
        worksheet = sheet.add_worksheet(title=WORKSHEET_TITLE, rows="100", cols="20")
        logger.info("Worksheet created: %s", WORKSHEET_TITLE)

    except Exception as e:
        logger.info("Worksheet already exists. Selecting and clearing worksheet: %s", WORKSHEET_TITLE)
        worksheet = sheet.worksheet(WORKSHEET_TITLE)
        worksheet.clear()

    logger.info("Preparing DataFrame for upload")
    
    # Check if this is a pivot table (case-insensitive check)
    is_pivot = "pivot" in sheet_name.lower()
    
    if is_pivot:
        logger.info("Detected pivot table - using simple conversion")
        
        # Reset index to convert it to a column (NOT drop=True!)
        df_copy = df.reset_index()
        
        # Fill NaN values
        df_copy = df_copy.fillna('')
        
        # Get headers
        headers = df_copy.columns.tolist()
        
        # Convert values to list - simple approach
        data_rows = df_copy.values.tolist()
        
        # Convert numpy types to Python types
        clean_data_rows = []
        for row in data_rows:
            clean_row = []
            for val in row:
                if isinstance(val, (np.integer, np.int64, np.int32)):
                    clean_row.append(int(val))
                elif isinstance(val, (np.floating, np.float64, np.float32)):
                    clean_row.append(float(val))
                elif pd.isna(val) or val == '':
                    clean_row.append('')
                else:
                    clean_row.append(str(val))
            clean_data_rows.append(clean_row)
        
        values = [headers] + clean_data_rows
        
        logger.info(f"Pivot table prepared with {len(values)} rows (including header)")
        logger.info(f"Headers: {headers}")
        logger.info(f"First data row: {clean_data_rows[0] if clean_data_rows else 'No data'}")
        
    else:
        logger.info("Regular table - applying full sanitization")
        # Get headers from the original DataFrame
        headers = df.columns.tolist()
        
        # Get sanitized values (returns a list of lists)
        clean_values = sanitize_dataframe(df)
        
        # Combine headers and body
        values = [headers] + clean_values

    logger.info("Uploading data to worksheet")
    worksheet.update("A1", values)
    logger.info("Upload complete")


def append_rolling_data(df, table_name, sheet_name, max_rows=300000):
    """
    Appends data to a sheet, ensuring the total row count does not exceed max_rows.
    Deletes oldest rows BEFORE appending to avoid exceeding Google Sheets' cell limit.
    
    NOTE: 
    - max_rows includes the header row
    - Default max_rows set to 300,000 to avoid hitting Google Sheets' 10M cell limit per workbook
    - For a 26-column sheet: 300k rows × 26 cols = 7.8M cells (safe margin for workbook)
    - Row 1 is always the header and is never deleted
    - Uses gspread delete_rows which has INCLUSIVE start and end indices
    - IMPORTANT: Deletes old data BEFORE appending to avoid cell limit errors during append
    """
    # Validate input
    if df is None or df.empty:
        logger.warning("Empty DataFrame provided to append_rolling_data. Skipping append.")
        return
    
    SHEET_ID = get_current_sheet_id(table_name)
    
    # Authenticate
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    
    service_account_info = json.loads(SERVICE_ACCOUNT_JSON_STRING)
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    
    client = gspread.authorize(credentials)
    
    logger.info("Opening Google Sheet for Rolling Append: %s", SHEET_ID)
    sheet = client.open_by_key(SHEET_ID)

    # Get or create worksheet
    try:
        worksheet = sheet.worksheet(sheet_name)
        logger.info("Worksheet '%s' found", sheet_name)
    except gspread.WorksheetNotFound:
        logger.info("Worksheet '%s' not found. Creating new.", sheet_name)
        worksheet = sheet.add_worksheet(title=sheet_name, rows="100", cols="20")
        
        # Initialize new worksheet with headers and data
        headers = [df.columns.tolist()]
        data_rows = sanitize_dataframe(df)
        values = headers + data_rows
        
        worksheet.update("A1", values)
        logger.info("New worksheet created and populated with %d rows (including header)", len(values))
        return

    # Get current data count
    current_data = worksheet.get_all_values()
    current_row_count = len(current_data)  # Includes header
    new_row_count = len(df)  # Data rows only, no header
    
    logger.info("Current rows (with header): %d, New rows: %d, Limit: %d", 
                current_row_count, new_row_count, max_rows)

    # Calculate what the final row count should be after append
    final_row_count = min(current_row_count + new_row_count, max_rows)
    
    # Calculate how many rows to delete BEFORE appending
    # We want to make room for new data while staying under max_rows
    if current_row_count + new_row_count > max_rows:
        rows_to_delete = current_row_count + new_row_count - max_rows
        logger.info("Will exceed limit. Deleting %d oldest data rows BEFORE appending...", rows_to_delete)
        
        # Calculate max deletable rows (can't delete header at row 1)
        max_deletable = current_row_count - 1  # -1 for header
        
        if rows_to_delete > max_deletable:
            logger.warning("Need to delete %d rows but only %d data rows exist. Deleting all data rows.", 
                         rows_to_delete, max_deletable)
            rows_to_delete = max_deletable
        
        if rows_to_delete > 0:
            start_index = 2
            end_index = 2 + rows_to_delete - 1  # Inclusive end
            
            logger.info("Deleting rows %d to %d (%d rows) to make room...", start_index, end_index, rows_to_delete)
            worksheet.delete_rows(start_index, end_index)
            logger.info("Deletion complete. Sheet now has %d rows (including header)", current_row_count - rows_to_delete)

    # Now append new data (we've made room)
    logger.info("Appending %d new rows...", new_row_count)
    data_to_add = sanitize_dataframe(df)
    
    worksheet.append_rows(data_to_add, value_input_option="USER_ENTERED")
    logger.info("Append complete. Added %d rows. Final count should be ~%d rows.", len(data_to_add), final_row_count)