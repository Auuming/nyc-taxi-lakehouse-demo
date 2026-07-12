from pathlib import Path
import sys

import streamlit as st

DASHBOARD_DIR = Path(__file__).resolve().parents[1]
if str(DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_DIR))

from charts import line_chart
from data import (
    filter_variant_and_month_basis,
    load_gold_table,
    month_basis_selector,
    require_nonempty,
    variant_selector,
)


st.set_page_config(page_title="Overview", layout="wide")
st.title("Business Overview")

variant = variant_selector(key="overview_variant")
month_basis = month_basis_selector(key="overview_month_basis")

daily = load_gold_table("gold.daily_trip_summary")
monthly = load_gold_table("gold.monthly_kpi")
require_nonempty(daily, "gold.daily_trip_summary")
require_nonempty(monthly, "gold.monthly_kpi")

daily = daily[daily["data_variant"] == variant].copy()
monthly = filter_variant_and_month_basis(
    monthly,
    data_variant=variant,
    month_basis=month_basis,
)

months = sorted(monthly["year_month"].dropna().unique().tolist())
selected_months = st.sidebar.multiselect("Months", months, default=months)

if selected_months:
    monthly = monthly[monthly["year_month"].isin(selected_months)]
    if month_basis == "pickup_month":
        daily = daily[
            daily["trip_date"].astype(str).str.slice(0, 7).isin(selected_months)
        ]

if month_basis == "source_month":
    st.info(
        "Monthly KPIs use raw source-file month. Daily charts remain based on "
        "pickup date because a raw file month does not define daily dates."
    )

total_trips = int(monthly["trip_count"].sum()) if not monthly.empty else 0
total_revenue = float(monthly["total_revenue"].sum()) if not monthly.empty else 0.0
weighted_distance = (
    (monthly["avg_trip_distance"] * monthly["trip_count"]).sum() / total_trips
    if total_trips else 0.0
)
weighted_duration = (
    (monthly["avg_trip_duration_min"] * monthly["trip_count"]).sum() / total_trips
    if total_trips else 0.0
)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Trips", f"{total_trips:,}")
c2.metric("Revenue", f"${total_revenue:,.2f}")
c3.metric("Average distance", f"{weighted_distance:,.2f} mi")
c4.metric("Average duration", f"{weighted_duration:,.2f} min")

left, right = st.columns(2)
with left:
    fig = line_chart(
        daily,
        x="trip_date",
        y="trip_count",
        title="Daily trips by pickup date",
        labels={"trip_date": "Pickup date", "trip_count": "Trips"},
    )
    fig.update_yaxes(tickformat=",d")
    st.plotly_chart(fig, use_container_width=True)
with right:
    st.plotly_chart(
        line_chart(
            daily,
            x="trip_date",
            y="total_revenue",
            title="Daily revenue by pickup date",
            labels={"trip_date": "Pickup date", "total_revenue": "Revenue"},
        ),
        use_container_width=True,
    )

st.subheader("Monthly KPIs")
st.dataframe(monthly.sort_values("year_month"), use_container_width=True, hide_index=True)
