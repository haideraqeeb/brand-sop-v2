import os
import logging
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Supabase client
supabase: Client = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

logger.info("Supabase client initialized successfully")

def update_sheet_id(new_sheet_id: str, table_name: str):
    """
    Update the sheet_id in the sheet table (updates the first/only row).
    Also updates the updated_at timestamp.
    
    Args:
        new_sheet_id: The new value for sheet_id
        table_name: The table name
        
    Returns:
        Response from Supabase
    """
    try:
        logger.info("Fetching existing row from sheet table")
        existing = supabase.table(table_name).select('id, sheet_id').limit(1).execute()
        
        if not existing.data or len(existing.data) == 0:
            logger.warning("No rows found in sheet table. Please insert a row first.")
            return None
        
        row_id = existing.data[0]['id']
        old_value = existing.data[0]['sheet_id']
        
        logger.info(f"Updating row ID {row_id} from sheet_id={old_value} to sheet_id={new_sheet_id}")
        response = supabase.table(table_name).update({
            'sheet_id': new_sheet_id,
            'updated_at': 'now()'
        }).eq('id', row_id).execute()
        
        logger.info(f"Successfully updated sheet_id. Row ID: {row_id}, Old: {old_value}, New: {new_sheet_id}")
        return response
        
    except Exception as e:
        logger.error(f"Error updating sheet_id: {e}", exc_info=True)
        return None

def get_current_sheet_id(table_name: str):
    """
    Get the current sheet_id value.

    Args:
        table_name: The table name
    
    Returns:
        Current sheet_id or None if error
    """
    try:
        logger.info("Fetching current sheet_id from sheet table")
        response = supabase.table(table_name).select('sheet_id').limit(1).execute()
        
        if response.data and len(response.data) > 0:
            current = response.data[0]['sheet_id']
            logger.info(f"Current sheet_id: {current}")
            return current
        else:
            logger.warning("No data found in sheet table")
            return None
            
    except Exception as e:
        logger.error(f"Error fetching sheet_id: {e}", exc_info=True)
        return None