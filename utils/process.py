import os
import logging

import pandas as pd
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def convert_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts the DataFrame by adding the necessary columns and removing irrelevant rows 
    as defined in the document

    Parameters:
        df (pd.DataFrame): The DataFrame that needs to be processed

    Returns:
        pd.DataFrame: The processed DataFrame with only the relevant columns and rows
    """
    logger.info("Converting DataFrame columns and filtering rows.")
    # The columns which need to copied
    columns_to_copy = [
        "Customer Name",
        "Created Date",
        "Location",
        "Employee Name",
        "Employee Number",
        "Amount payable",
        "Status",
        "POD Date",
        "Delivery Payment type",
        "Pincode",
        "Order Category"
    ]

    # Mapping columns from old df -> new df
    column_mapping = {
        "Customer Name": "Brand/Customer Name",
        "Created Date": "Creation Date",
        "Location": "Partner Name/Location",
        "Employee Name": "Rider Name/Employee Name",
        "Employee Number": "Rider ID/Employee Number",
        "Amount payable": "COD Amount/Amount payable",
        "Status": "Fulfillment Status/Status",
        "POD Date": "Terminal Time/POD Date",
        "Delivery Payment type": "Delivery Payment type",
        "Pincode": "Pincode",
        "Order Category": "Order Category"
    }

    # Rename the columns which need to be added
    df_to_add = df[[c for c in columns_to_copy if c in df.columns]].rename(columns=column_mapping)
    logger.info("DataFrame conversion complete.")
    return df_to_add

def create_pivot(
        df: pd.DataFrame, 
        dates: list[datetime],
        brand: str
    ) -> pd.DataFrame:
    """
    Creates the pivot which calculates the payout amount for the brands

    Parameters:
        df (pd.DataFrame): The data which needs to be processed
        date (datetime): List of start and end dates for the creation of the pivot
        brand (str): The brand for which the pivot is being generated

    Returns:
        pd.DataFrame: The pivot made from the given parameters 
    """
    logger.info(f"Creating pivot for {brand}, from the dates {dates[0]} to {dates[1]}")

    mask = (df['Created Date'].dt.date >= dates[0]) & (df['Created Date'].dt.date <= dates[1])

    df = df.loc[mask]

    df = df[df["Customer Name"] == brand]

    logger.info(f"Filtered DataFrame to {len(df)} rows based on dates from {dates[0]} to {dates[1]} and brand {brand}.")

    # Get the df with the column names changed
    new_df = convert_df(df)

    # Additional required processing
    new_df.insert(0, "Source", "LOADSHARE")
    new_df.insert(1, "Reference ID/Waybill No", df["Waybill No"].reindex(new_df.index))

    new_df["COD Amount/Amount payable"] = pd.to_numeric(new_df["COD Amount/Amount payable"], errors='coerce').fillna(0)

    os.makedirs("temp", exist_ok=True)
    new_df.to_csv("temp/breakdown.csv")

    # Create pivot table
    pivot = pd.pivot_table(
        new_df,
        index="Brand/Customer Name",
        values="COD Amount/Amount payable",
        aggfunc="sum",
        columns=None,
        fill_value=0
    )

    logger.info("Pivot table created.")

    return pivot