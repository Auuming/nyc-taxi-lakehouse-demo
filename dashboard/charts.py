from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def line_chart(
    df: pd.DataFrame,
    *,
    x: str,
    y: str,
    title: str,
    labels: dict[str, str] | None = None,
):
    return px.line(
        df,
        x=x,
        y=y,
        markers=True,
        title=title,
        labels=labels or {},
    )


def bar_chart(
    df: pd.DataFrame,
    *,
    x: str,
    y: str,
    title: str,
    color: str | None = None,
    orientation: str = "v",
    labels: dict[str, str] | None = None,
    integer_values: bool = False,
    show_values: bool = False,
):
    fig = px.bar(
        df,
        x=x,
        y=y,
        color=color,
        orientation=orientation,
        title=title,
        labels=labels or {},
    )

    if orientation == "v":
        fig.update_xaxes(type="category")
        if integer_values:
            fig.update_yaxes(tickformat=",d")
        if show_values:
            fig.update_traces(texttemplate="%{y:,.0f}", textposition="outside")
    else:
        fig.update_yaxes(type="category")
        if integer_values:
            fig.update_xaxes(tickformat=",d")
        if show_values:
            fig.update_traces(texttemplate="%{x:,.0f}", textposition="outside")

    return fig


def flow_bubble_chart(
    df: pd.DataFrame,
    *,
    title: str,
    color: str | None = "year_month",
):
    fig = px.scatter(
        df,
        x="pickup_location_id",
        y="dropoff_location_id",
        size="trip_count",
        color=color if color and color in df.columns else None,
        hover_data={
            "trip_count": ":,",
            "total_revenue": ":,.2f",
            "avg_trip_distance": ":.2f",
            "avg_trip_duration_min": ":.2f",
        },
        title=title,
        labels={
            "pickup_location_id": "Pickup location ID",
            "dropoff_location_id": "Drop-off location ID",
            "trip_count": "Trips",
            "year_month": "Month",
        },
    )
    fig.update_xaxes(tickformat="d")
    fig.update_yaxes(tickformat="d")
    return fig


def quality_bar(
    df: pd.DataFrame,
    *,
    title: str,
    value_column: str = "metric_percent",
):
    fig = px.bar(
        df,
        x="metric_name",
        y=value_column,
        title=title,
        labels={
            "metric_name": "Metric",
            value_column: (
                "Percent" if value_column == "metric_percent" else "Rows"
            ),
        },
    )
    if value_column == "metric_value":
        fig.update_yaxes(tickformat=",d")
    return fig


def pipeline_funnel(df: pd.DataFrame):
    order = ["bronze_rows", "silver_rows", "rows_rejected_unique"]
    data = (
        df[df["metric_name"].isin(order)]
        .drop_duplicates("metric_name")
        .set_index("metric_name")
        .reindex(order)
        .dropna(subset=["metric_value"])
        .reset_index()
    )
    return go.Figure(
        go.Funnel(
            y=data["metric_name"],
            x=data["metric_value"],
            textinfo="value+percent initial",
        )
    ).update_layout(title="Pipeline row flow")
