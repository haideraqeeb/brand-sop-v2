import os
import json
import shutil
from datetime import date, timedelta

import streamlit as st
import pandas as pd
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

from utils.sheet_manager import get_current_sheet_id
from utils.upload import upload
from utils.process import create_pivot

load_dotenv()

SERVICE_ACCOUNT_JSON_STRING = os.getenv("GOOGLE_CREDENTIALS_JSON")

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

st.title("Create Pivot")

# cache
@st.cache_resource
def get_gspread_client(service_account_info):
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    return gspread.authorize(credentials)


@st.cache_data(ttl=14400)  # 4 hours; change as needed
def load_dump_sheet(dump_sheet_id, _client):
    sheet = _client.open_by_key(dump_sheet_id)
    worksheet = sheet.worksheet("Loadshare Dump")
    data = worksheet.get_all_values()

    df = pd.DataFrame(data[1:], columns=data[0])
    df["Created Date"] = pd.to_datetime(df["Created Date"])
    return df

dump_sheet_id = get_current_sheet_id("brand_dump_sheet")
service_account_info = json.loads(SERVICE_ACCOUNT_JSON_STRING)

with st.spinner("Loading data..."):
    client = get_gspread_client(service_account_info)
    df = load_dump_sheet(dump_sheet_id, client)

brands_list = df["Customer Name"].unique().tolist()
selected_brand = st.selectbox("Select Brand", brands_list)

st.write(f"Selected Brand: {selected_brand}")

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date", value=date.today())

with col2:
    end_date = st.date_input(
        "End Date",
        value=date.today() + timedelta(days=1),
        min_value=start_date,
    )

if st.button("Process"):
    st.success("Processing pivot...")

    pivot = create_pivot(
        df=df,
        dates=[start_date, end_date],
        brand=selected_brand
    )

    st.dataframe(pivot)

    upload(
        pivot,
        table_name="final_sheet",
        sheet_name=f"Pivot: {selected_brand}"
    )

    # breakdown will exist only if create_pivot generated it
    breakdown_path = "temp/breakdown.csv"

    if os.path.exists(breakdown_path):
        breakdown = pd.read_csv(breakdown_path, index_col=0)

        if not breakdown.empty:
            upload(
                breakdown,
                table_name="final_sheet",
                sheet_name=f"Breakdown: {selected_brand}"
            )

    # Cleanup
    if os.path.exists("temp"):
        shutil.rmtree("temp")

    st.success("Processing completed.")
