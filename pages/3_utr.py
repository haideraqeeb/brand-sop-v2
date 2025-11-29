import streamlit as st
import pandas as pd
import tempfile
import os

from utils.creator import process_breakdown, get_company_config
from utils.breakdown import load_sheet
from utils.upload import upload
from utils.db import fetch

st.set_page_config(layout="wide")

st.markdown("""
    <style>
    [data-testid="stSidebarNav"] { display: none; }
    </style>
""", unsafe_allow_html=True)

st.sidebar.title("Navigation")

# Sidebar
if st.sidebar.button("Data Dump", use_container_width=True):
    st.switch_page("pages/1_data.py")

if st.sidebar.button("Create Pivot", use_container_width=True):
    st.switch_page("pages/2_pivot.py")

if st.sidebar.button("Create UTR", use_container_width=True):
    st.switch_page("pages/3_utr.py")

if st.sidebar.button("Update Config", use_container_width=True):
    st.switch_page("pages/4_config.py")

# Main content
st.title("Create UTR Sheet")

# Load configs
try:
    configs = fetch("configs/utr_config.json")
    
    if not configs:
        st.error("No configurations found. Please add configurations first.")
        st.stop()
    
    # Get list of company names
    company_names = [config['company_name'] for config in configs]
    
    # Company selection
    selected_company = st.selectbox(
        "Select Company",
        options=company_names,
        help="Select the company for which you want to create the UTR sheet"
    )
    
    if st.button("Create UTR Sheet", type="primary", use_container_width=True):
        with st.spinner(f"Processing UTR for {selected_company}..."):
            try:
                # Step 1: Load breakdown sheet
                breakdown_sheet_name = f"Breakdown: {selected_company}"
                st.info(f"Loading breakdown sheet: {breakdown_sheet_name}")
                
                df_breakdown = load_sheet(breakdown_sheet_name)
                
                if df_breakdown is None or df_breakdown.empty:
                    st.error(f"Breakdown sheet '{breakdown_sheet_name}' does not exist or is empty. Please create the breakdown sheet first.")
                    st.stop()
                
                st.success(f"✓ Loaded breakdown with {len(df_breakdown)} rows")
                
                # Step 2: Get company config
                config = get_company_config(configs, selected_company)
                
                # Step 3: Create temporary CSV file from breakdown DataFrame
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as tmp_csv:
                    df_breakdown.to_csv(tmp_csv.name, index=False)
                    temp_csv_path = tmp_csv.name
                
                # Step 4: Create temporary Excel file path
                with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_excel:
                    temp_excel_path = tmp_excel.name
                
                # Step 5: Process breakdown and create Excel
                st.info("Creating UTR Excel file...")
                process_breakdown(
                    breakdown_csv_path=temp_csv_path,
                    company_name=selected_company,
                    output_excel_path=temp_excel_path
                )
                
                # Step 6: Read the created Excel file
                df_utr = pd.read_excel(temp_excel_path)
                
                # Step 7: Upload to Google Sheets
                utr_sheet_name = f"UTR: {selected_company}"
                st.info(f"Uploading to sheet: {utr_sheet_name}")
                
                upload(df_utr, "final_sheet", utr_sheet_name)
                
                # Step 8: Clean up temporary files
                try:
                    os.unlink(temp_csv_path)
                    os.unlink(temp_excel_path)
                except:
                    pass
                
                st.success(f"✓ UTR sheet created successfully: {utr_sheet_name}")
                
            except ValueError as ve:
                st.error(f"Configuration error: {str(ve)}")
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                st.exception(e)
                
except Exception as e:
    st.error(f"Failed to load configurations: {str(e)}")
    st.exception(e)