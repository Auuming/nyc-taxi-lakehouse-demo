import streamlit as st

from data import clear_dashboard_cache


st.set_page_config(
    page_title="NYC Taxi Lakehouse",
    page_icon="🚕",
    layout="wide",
)

st.title("NYC Taxi Lakehouse")
st.caption(
    "Business analytics and engineering insights generated from "
    "Iceberg Gold tables."
)

st.markdown(
    """
Use the pages in the sidebar:

1. **Overview** — daily and monthly business metrics.
2. **Zone Flows** — pickup/drop-off activity and flow matrix.
3. **Payment and Vendor** — payment methods, vendor comparisons, and flows.
4. **Data Quality** — quarantine, warning, month-offset, and null-pattern evidence.
"""
)

st.info(
    "Build the Gold layer before opening the pages: "
    "`python pipelines/build_gold.py`"
)

if st.button("Refresh cached Gold data"):
    clear_dashboard_cache()
    st.success("Dashboard cache cleared.")
