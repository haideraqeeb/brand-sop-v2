import streamlit as st

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