from pathlib import Path
import sys

import plotly.express as px
import streamlit as st

DASHBOARD_DIR = Path(__file__).resolve().parents[1]
if str(DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_DIR))

from charts import pipeline_funnel, quality_bar
from data import MONTH_BASIS_LABELS, load_gold_table, month_basis_selector, require_nonempty


st.set_page_config(page_title="Data Quality", layout="wide")
st.title("Data Quality and Engineering Insights")

quality = load_gold_table("gold.data_quality_summary")
null_profile = load_gold_table("gold.payment_null_profile")
require_nonempty(quality, "gold.data_quality_summary")

overall = quality[quality["month_basis"] == "overall"].copy()

pipeline = overall[overall["metric_category"] == "pipeline"]
st.plotly_chart(pipeline_funnel(pipeline), use_container_width=True)

left, right = st.columns(2)
with left:
    rejected = overall[
        overall["metric_category"].isin(["rejection_reason", "rejection_detail"])
    ]
    st.plotly_chart(
        quality_bar(rejected, title="Rejected rows by reason", value_column="metric_value"),
        use_container_width=True,
    )
with right:
    warnings = overall[overall["metric_category"] == "silver_warning"]
    st.plotly_chart(
        quality_bar(warnings, title="Silver warning percentages", value_column="metric_percent"),
        use_container_width=True,
    )

st.subheader("Zero-value effects")

overall_impact = overall[overall["metric_category"] == "zero_filter_impact"]
overall_remaining = overall[overall["metric_category"] == "zero_filter_remaining"]

left, right = st.columns(2)
with left:
    st.plotly_chart(
        quality_bar(overall_impact, title="Overall zero-value percentages"),
        use_container_width=True,
    )
with right:
    st.plotly_chart(
        quality_bar(overall_remaining, title="Rows retained by data variant"),
        use_container_width=True,
    )

quality_month_basis = month_basis_selector(key="quality_month_basis", sidebar=False)
monthly_zero = quality[
    (quality["month_basis"] == quality_month_basis)
    & quality["metric_category"].isin(
        ["zero_filter_impact_month", "zero_filter_remaining_month"]
    )
].copy()

if monthly_zero.empty:
    st.info("No monthly zero-filter metrics are available.")
else:
    st.caption(f"Monthly grouping: {MONTH_BASIS_LABELS[quality_month_basis]}")

    impact = monthly_zero[
        monthly_zero["metric_category"] == "zero_filter_impact_month"
    ]
    impact_fig = px.line(
        impact,
        x="source_month",
        y="metric_percent",
        color="metric_name",
        markers=True,
        title="Zero-value percentage by month",
        labels={
            "source_month": "Month",
            "metric_percent": "Percent",
            "metric_name": "Zero metric",
        },
    )
    st.plotly_chart(impact_fig, use_container_width=True)

    remaining = monthly_zero[
        monthly_zero["metric_category"] == "zero_filter_remaining_month"
    ]
    remaining_fig = px.line(
        remaining,
        x="source_month",
        y="metric_percent",
        color="metric_name",
        markers=True,
        title="Rows retained by data variant and month",
        labels={
            "source_month": "Month",
            "metric_percent": "Percent retained",
            "metric_name": "Data variant",
        },
    )
    st.plotly_chart(remaining_fig, use_container_width=True)

st.subheader("Source month offset")

overall_offset = overall[overall["metric_category"] == "month_offset"]
file_offset = overall[overall["metric_category"] == "month_offset_file"]

st.plotly_chart(
    quality_bar(
        overall_offset,
        title="Pickup month offset across all source files",
        value_column="metric_percent",
    ),
    use_container_width=True,
)

if not file_offset.empty:
    offset_fig = px.bar(
        file_offset.sort_values(["source_month", "metric_name"]),
        x="source_month",
        y="metric_percent",
        color="metric_name",
        barmode="group",
        title="Month offset by raw source-file month",
        labels={
            "source_month": "Raw source-file month",
            "metric_percent": "Percent",
            "metric_name": "Offset",
        },
    )
    st.plotly_chart(offset_fig, use_container_width=True)
    st.dataframe(
        file_offset[
            ["source_month", "source_file", "metric_name", "metric_value", "metric_percent"]
        ].sort_values(["source_month", "metric_name"]),
        use_container_width=True,
        hide_index=True,
    )

st.subheader("Payment type 0 null pattern")
payment_zero = null_profile[
    (null_profile["data_variant"] == "all")
    & (null_profile["payment_type"] == 0)
].copy()
if payment_zero.empty:
    st.info("No payment type 0 profile is available.")
else:
    st.plotly_chart(
        quality_bar(
            payment_zero.rename(
                columns={"field_name": "metric_name", "null_percent": "metric_percent"}
            ),
            title="Payment type 0 null percentages",
        ),
        use_container_width=True,
    )
    st.dataframe(payment_zero, use_container_width=True, hide_index=True)

st.subheader("Raw quality metrics")
categories = sorted(quality["metric_category"].dropna().unique().tolist())
selected_categories = st.multiselect(
    "Metric categories",
    options=categories,
    default=categories,
)
show_overall = st.checkbox("Include overall metrics", value=True)
raw = quality[quality["metric_category"].isin(selected_categories)].copy()
if not show_overall:
    raw = raw[raw["month_basis"] != "overall"]
st.dataframe(
    raw.sort_values(
        ["month_basis", "source_month", "metric_category", "metric_name"],
        na_position="last",
    ),
    use_container_width=True,
    hide_index=True,
)
