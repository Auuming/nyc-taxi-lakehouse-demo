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


st.set_page_config(page_title="Zone Flows", layout="wide")
st.title("Pickup and Drop-off Zone Flows")
st.caption(
    "Location IDs are categorical Taxi Zone identifiers. "
    "The bubble chart is a flow matrix, not a geographic map."
)

variant = variant_selector(key="zone_variant")
month_basis = month_basis_selector(key="zone_month_basis")

flow = filter_variant_and_month_basis(
    load_gold_table("gold.zone_flow_monthly"),
    data_variant=variant,
    month_basis=month_basis,
)
pickup = filter_variant_and_month_basis(
    load_gold_table("gold.pickup_zone_monthly"),
    data_variant=variant,
    month_basis=month_basis,
)
dropoff = filter_variant_and_month_basis(
    load_gold_table("gold.dropoff_zone_monthly"),
    data_variant=variant,
    month_basis=month_basis,
)
require_nonempty(flow, "gold.zone_flow_monthly")

months = ["All"] + sorted(flow["year_month"].dropna().unique().tolist())
month = st.sidebar.selectbox("Month", months)
if month != "All":
    flow = flow[flow["year_month"] == month]
    pickup = pickup[pickup["year_month"] == month]
    dropoff = dropoff[dropoff["year_month"] == month]

top_n = st.sidebar.slider("Top flows", 20, 500, 100, 20)

pickup_ids = ["All"] + sorted(
    int(value) for value in flow["pickup_location_id"].dropna().unique()
)
dropoff_ids = ["All"] + sorted(
    int(value) for value in flow["dropoff_location_id"].dropna().unique()
)
selected_pickup = st.sidebar.selectbox("Pickup location", pickup_ids)
selected_dropoff = st.sidebar.selectbox("Drop-off location", dropoff_ids)

filtered = flow
if selected_pickup != "All":
    filtered = filtered[filtered["pickup_location_id"] == selected_pickup]
if selected_dropoff != "All":
    filtered = filtered[filtered["dropoff_location_id"] == selected_dropoff]
filtered = filtered.nlargest(top_n, "trip_count")

st.plotly_chart(
    flow_bubble_chart(filtered, title="Pickup-to-drop-off flow matrix"),
    use_container_width=True,
)

left, right = st.columns(2)
with left:
    top_pickup = (
        pickup.groupby("pickup_location_id", as_index=False)
        .agg(trip_count=("trip_count", "sum"))
        .nlargest(20, "trip_count")
        .sort_values("trip_count")
    )
    top_pickup = as_category_strings(top_pickup, ["pickup_location_id"])
    st.plotly_chart(
        bar_chart(
            top_pickup,
            x="trip_count",
            y="pickup_location_id",
            title="Top pickup zones",
            orientation="h",
            integer_values=True,
            show_values=True,
            labels={"pickup_location_id": "Pickup location ID", "trip_count": "Trips"},
            percent_base=float(pickup["trip_count"].sum()),
        ),
        use_container_width=True,
    )

with right:
    top_dropoff = (
        dropoff.groupby("dropoff_location_id", as_index=False)
        .agg(trip_count=("trip_count", "sum"))
        .nlargest(20, "trip_count")
        .sort_values("trip_count")
    )
    top_dropoff = as_category_strings(top_dropoff, ["dropoff_location_id"])
    st.plotly_chart(
        bar_chart(
            top_dropoff,
            x="trip_count",
            y="dropoff_location_id",
            title="Top drop-off zones",
            orientation="h",
            integer_values=True,
            show_values=True,
            labels={"dropoff_location_id": "Drop-off location ID", "trip_count": "Trips"},
            percent_base=float(dropoff["trip_count"].sum()),
        ),
        use_container_width=True,
    )

st.subheader("Filtered flow data")
st.dataframe(filtered, use_container_width=True, hide_index=True)
