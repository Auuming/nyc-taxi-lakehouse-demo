from pathlib import Path
import sys

import streamlit as st

DASHBOARD_DIR = Path(__file__).resolve().parents[1]
if str(DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_DIR))

from charts import bar_chart, flow_bubble_chart
from data import (
    as_category_strings,
    filter_variant_and_month_basis,
    load_gold_table,
    month_basis_selector,
    require_nonempty,
    variant_selector,
)


st.set_page_config(page_title="Payment and Vendor", page_icon="💳", layout="wide")
st.title("Payment and Vendor Analysis")

variant = variant_selector(key="payment_vendor_variant")
month_basis = month_basis_selector(key="payment_vendor_month_basis")

payment = filter_variant_and_month_basis(
    load_gold_table("gold.payment_monthly"),
    data_variant=variant,
    month_basis=month_basis,
)
vendor = filter_variant_and_month_basis(
    load_gold_table("gold.vendor_monthly"),
    data_variant=variant,
    month_basis=month_basis,
)
payment_flow = filter_variant_and_month_basis(
    load_gold_table("gold.payment_zone_flow_monthly"),
    data_variant=variant,
    month_basis=month_basis,
)
vendor_flow = filter_variant_and_month_basis(
    load_gold_table("gold.vendor_zone_flow_monthly"),
    data_variant=variant,
    month_basis=month_basis,
)
null_profile = load_gold_table("gold.payment_null_profile")
null_profile = null_profile[null_profile["data_variant"] == variant].copy()

require_nonempty(payment, "gold.payment_monthly")
require_nonempty(vendor, "gold.vendor_monthly")

months = ["All"] + sorted(payment["year_month"].dropna().unique().tolist())
month = st.sidebar.selectbox("Month", months)
if month != "All":
    payment = payment[payment["year_month"] == month]
    vendor = vendor[vendor["year_month"] == month]
    payment_flow = payment_flow[payment_flow["year_month"] == month]
    vendor_flow = vendor_flow[vendor_flow["year_month"] == month]

left, right = st.columns(2)
with left:
    payment_totals = (
        payment.groupby("payment_type", as_index=False)
        .agg(trip_count=("trip_count", "sum"))
    )
    payment_totals = as_category_strings(payment_totals, ["payment_type"])
    st.plotly_chart(
        bar_chart(
            payment_totals,
            x="payment_type",
            y="trip_count",
            title="Trips by payment type",
            integer_values=True,
            show_values=True,
            labels={"payment_type": "Payment type", "trip_count": "Trips"},
        ),
        use_container_width=True,
    )

with right:
    vendor_totals = (
        vendor.groupby("vendor_id", as_index=False)
        .agg(trip_count=("trip_count", "sum"))
    )
    vendor_totals = as_category_strings(vendor_totals, ["vendor_id"])
    st.plotly_chart(
        bar_chart(
            vendor_totals,
            x="vendor_id",
            y="trip_count",
            title="Trips by vendor",
            integer_values=True,
            show_values=True,
            labels={"vendor_id": "Vendor ID", "trip_count": "Trips"},
        ),
        use_container_width=True,
    )

st.subheader("Payment type 0 evidence")
payment_zero = null_profile[null_profile["payment_type"] == 0].copy()
if payment_zero.empty:
    st.info("No payment type 0 rows are present for this data variant.")
else:
    st.plotly_chart(
        bar_chart(
            payment_zero,
            x="field_name",
            y="null_percent",
            title="Null percentage for payment type 0 fields",
            labels={"field_name": "Field", "null_percent": "Null percent"},
        ),
        use_container_width=True,
    )
    st.dataframe(payment_zero, use_container_width=True, hide_index=True)

st.subheader("Filtered zone flow")
flow_mode = st.radio("Flow dimension", ["Payment type", "Vendor"], horizontal=True)

if flow_mode == "Payment type":
    options = sorted(int(value) for value in payment_flow["payment_type"].dropna().unique())
    if not options:
        st.info("No payment-specific flows match the selected filters.")
        st.stop()
    selected = st.selectbox("Payment type", options)
    flow = payment_flow[payment_flow["payment_type"] == selected]
    title = f"Zone flow for payment type {selected}"
else:
    options = sorted(int(value) for value in vendor_flow["vendor_id"].dropna().unique())
    if not options:
        st.info("No vendor-specific flows match the selected filters.")
        st.stop()
    selected = st.selectbox("Vendor", options)
    flow = vendor_flow[vendor_flow["vendor_id"] == selected]
    title = f"Zone flow for vendor {selected}"

top_n = st.slider("Top flows to display", 20, 500, 100, 20)
flow = flow.nlargest(top_n, "trip_count")

st.plotly_chart(
    flow_bubble_chart(flow, title=title, color="year_month"),
    use_container_width=True,
)
