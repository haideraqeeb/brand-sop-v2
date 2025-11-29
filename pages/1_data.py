import streamlit as st
import pandas as pd

from utils.sheet_manager import get_current_sheet_id, update_sheet_id
from utils.upload import append_rolling_data

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


st.title("Data Dump")

st.markdown("### Sheet ID Configuration")
current_id = get_current_sheet_id("brand_dump_sheet")

st.markdown("#### Current Sheet ID")
st.code(current_id, language="text")

st.markdown("#### Update Sheet ID")
new_id = st.text_input("Enter new Sheet ID", value=current_id)

if st.button("Save Sheet ID"):
    if new_id.strip() == "":
        st.error("Sheet ID cannot be empty.")
    else:
        update_sheet_id(new_id, "brand_dump_sheet")
        st.success("Sheet ID updated successfully.")
        st.rerun()

st.write("---")

st.subheader("Add data to the Loadshare dump")
drs_report = st.file_uploader("Upload DRS Report", type=["csv", "xlsx"])

if drs_report:
    if drs_report.name.endswith(".csv"):
        drs_df = pd.read_csv(drs_report)
    else:
        drs_df = pd.read_excel(drs_report)
    
    col1, col2 = st.columns([1, 4])
    
    with col1:
        # Button to trigger the update
        if st.button("Upload Data", type="primary"):
            with st.spinner("Uploading Data"):
                try:
                    append_rolling_data(
                        df=drs_df, 
                        table_name="brand_dump_sheet", 
                        sheet_name="Loadshare Dump", 
                        max_rows=200000
                    )
                    st.success("Data Uploaded.")
                except Exception as e:
                    st.error(f"An error occurred: {e}")