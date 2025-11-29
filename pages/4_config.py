import os
import json
import shutil

import streamlit as st

from utils.db import fetch, upload

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


st.title("Create Company UTR Config")

try:
    available_columns = [
    "Source",
    "Reference ID/Waybill No",
    "Brand/Customer Name",
    "Creation Date",
    "Partner Name/Location",
    "Rider Name/Employee Name",
    "Rider ID/Employee Number",
    "COD Amount/Amount payable",
    "Fulfillment Status/Status",
    "Terminal Time/POD Date",
    "Delivery Payment type",
    "Pincode",
    "Order Category"
]
except Exception as e:
    st.error(f"Error loading columns from breakdown.py: {e}")
    st.stop()

st.subheader("1. Select or Enter Company Name")
company_name = st.text_input("Company Name")

if not company_name:
    st.stop()

st.subheader("2. Add Header Lines")

num_headers = st.number_input(
    "Number of Header Lines",
    min_value=0,
    max_value=20,
    value=1
)

header_inputs = []
for i in range(num_headers):
    header_inputs.append(st.text_input(f"Header Line {i+1}", key=f"header_{i}"))

st.subheader("3. Number of Empty Line Gaps After Headers")

line_gaps = st.number_input("Line Gaps", min_value=0, max_value=10, value=1)

st.subheader("4. Column Mapping")

st.write("Now map this company's column names to your internal data columns.")

num_company_columns = st.number_input(
    "How many columns does this company table contain?",
    min_value=1,
    max_value=100,
)

column_mapping = {}
for i in range(num_company_columns):
    col_left, col_right = st.columns(2)

    with col_left:
        company_col_name = st.text_input(
            f"Company Column {i+1} Name",
            key=f"company_col_{i}"
        )

    with col_right:
        internal_col = st.selectbox(
            f"Map to internal column",
            [""] + available_columns,
            key=f"internal_col_{i}"
        )

    if company_col_name and internal_col:
        column_mapping[company_col_name] = internal_col


st.subheader("5. UTR Column Name")
utr_column_name = st.text_input("UTR Column Name", value="UTR")

config = {
    "company_name": company_name,
    "headers": header_inputs,
    "line_gaps": line_gaps,
    "column_mapping": column_mapping,
    "utr_column_name": utr_column_name
}

CONFIG_FILE = "configs/utr_config.json"

if st.button("Save Config"):
    # Load existing configs
    existing_configs = fetch(CONFIG_FILE)

    # Update existing or append new config
    updated = False
    for idx, item in enumerate(existing_configs):
        if item.get("company_name") == company_name:
            existing_configs[idx] = config
            updated = True
            break

    if not updated:
        existing_configs.append(config)

    # Save back to file
    os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(existing_configs, f, indent=4)

    upload(CONFIG_FILE)

    shutil.rmtree("configs")

    if updated:
        st.success("Configuration updated successfully.")
    else:
        st.success("Configuration added successfully.")