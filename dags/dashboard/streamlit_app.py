"""
dashboard/streamlit_app.py

Public, always-on copy of the RetailLake Superset dashboard.

Reads ONLY the Gold layer (a Parquet snapshot written by
scripts/export_gold.py), mirroring the project rule that the dashboard never
touches Raw, Bronze or Silver. Same charts and the same five filters as the
Superset version: date, channel, category, store region and loyalty tier.

Run locally:   streamlit run dashboard/streamlit_app.py
"""

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

DATA_DIR = Path(__file__).parent / "data"
REPO_URL = "https://github.com/Ravindi373/smart-retail-data-platform"

# --- palette (validated categorical order; colour follows the entity) -------
INK, INK_2, MUTED = "#0b0b0b", "#52514e", "#898781"
GRID, AXIS, SURFACE = "#e1e0d9", "#c3c2b7", "#fcfcfb"
BLUE = "#2a78d6"
CHANNEL_COLORS = {"In-store": "#2a78d6", "Online": "#eb6834", "Mobile app": "#1baf7a"}
CHANNEL_LABELS = {"in_store": "In-store", "online": "Online", "mobile_app": "Mobile app"}
NO_STORE = "Online / app (no store)"
FONT = 'system-ui, -apple-system, "Segoe UI", sans-serif'

st.set_page_config(page_title="RetailLake Dashboard", page_icon="🛒", layout="wide")


# --- data -------------------------------------------------------------------
@st.cache_data
def load():
    sales = pd.read_parquet(DATA_DIR / "sales_flat.parquet")
    sales["channel"] = sales["channel"].map(CHANNEL_LABELS).fillna(sales["channel"])
    # online / mobile sales have no physical store, so region is NULL in Gold
    sales["region"] = sales["region"].fillna(NO_STORE)
    sales["date_key"] = pd.to_datetime(sales["date_key"])
    inv = pd.read_parquet(DATA_DIR / "inventory_flat.parquet")
    inv["date_key"] = pd.to_datetime(inv["date_key"])
    q_summary = pd.read_parquet(DATA_DIR / "quality_summary.parquet")
    q_score = pd.read_parquet(DATA_DIR / "quality_scorecard.parquet")
    return sales, inv, q_summary, q_score


sales, inventory, q_summary, q_score = load()


def style(fig, height=340):
    """Recessive axes and grid, thin marks, system sans, no chart junk."""
    fig.update_layout(
        height=height, margin=dict(l=8, r=8, t=8, b=8),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=FONT, color=INK_2, size=13),
        hoverlabel=dict(font_family=FONT, bgcolor="white", bordercolor=AXIS),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, title=None),
        bargap=0.25,
    )
    fig.update_xaxes(showgrid=False, linecolor=AXIS, tickfont_color=MUTED, title=None)
    fig.update_yaxes(gridcolor=GRID, zeroline=False, tickfont_color=MUTED, title=None)
    return fig


def money(x):
    return f"{x/1e6:,.2f}M" if abs(x) >= 1e6 else f"{x:,.0f}"


# --- sidebar filters ---------------------------------------------------------
st.sidebar.header("Filters")
dmin, dmax = sales["date_key"].min().date(), sales["date_key"].max().date()
picked = st.sidebar.date_input("Date range", (dmin, dmax), min_value=dmin, max_value=dmax)
start, end = (picked if isinstance(picked, tuple) and len(picked) == 2 else (dmin, dmax))


def multiselect(label, values):
    opts = sorted(values.dropna().unique())
    chosen = st.sidebar.multiselect(label, opts, placeholder="All")
    return chosen or opts  # empty selection means "all"


channels = multiselect("Channel", sales["channel"])
categories = multiselect("Category", sales["category"])
regions = multiselect("Store region", sales["region"])
tiers = multiselect("Loyalty tier", sales["loyalty_tier"])

st.sidebar.caption(
    "Empty filter = all values. Online and mobile-app sales have no store, "
    f"so they appear under **{NO_STORE}**."
)

f = sales[
    sales["date_key"].between(pd.Timestamp(start), pd.Timestamp(end))
    & sales["channel"].isin(channels)
    & sales["category"].isin(categories)
    & sales["region"].isin(regions)
    & sales["loyalty_tier"].isin(tiers)
]
inv_regions = [r for r in regions if r != NO_STORE]
inv_f = inventory[
    inventory["date_key"].between(pd.Timestamp(start), pd.Timestamp(end))
    & inventory["category"].isin(categories)
    & inventory["region"].isin(inv_regions)
]

# --- header ------------------------------------------------------------------
st.title("RetailLake — Smart Retail Dashboard")
st.markdown(
    f"Sales, inventory and data-quality view built on the **Gold** layer of the "
    f"[RetailLake medallion pipeline]({REPO_URL}) "
    f"(Raw → Bronze → Silver → Gold, orchestrated by Airflow, modelled with dbt). "
    f"Data covers **{dmin:%d %b %Y} – {dmax:%d %b %Y}**."
)

tab_sales, tab_inv, tab_quality = st.tabs(["Sales", "Inventory", "Data quality"])

# --- sales -------------------------------------------------------------------
with tab_sales:
    if f.empty:
        st.info("No sales match these filters.")
    else:
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Net sales", money(f["net_sales"].sum()))
        k2.metric("Sales lines", f"{len(f):,}")
        k3.metric("Units sold", f"{int(f['quantity'].sum()):,}")
        k4.metric("Avg. line value", f"{f['net_sales'].mean():,.2f}")

        head_l, head_r = st.columns([3, 2])
        head_l.subheader("Sales trend")
        grain = head_r.segmented_control(
            "Granularity", ["Daily", "Weekly", "Monthly"], default="Weekly",
            label_visibility="collapsed",
        ) or "Weekly"
        split = head_r.toggle("Split by channel", value=False)
        period = {"Daily": "D", "Weekly": "W", "Monthly": "M"}[grain]
        tf = f.assign(period=f["date_key"].dt.to_period(period))
        keys = ["period"] + (["channel"] if split else [])
        trend = tf.groupby(keys, observed=True)["net_sales"].sum().reset_index()
        # hide partial first/last weeks or months: they'd show up as fake dips
        lo, hi = pd.Timestamp(start), pd.Timestamp(end)
        full = (trend["period"].dt.start_time >= lo) & (trend["period"].dt.end_time.dt.normalize() <= hi)
        dropped = trend.loc[~full, "period"].nunique()
        trend = trend[full].assign(date_key=lambda d: d["period"].dt.start_time)
        fig = px.line(
            trend, x="date_key", y="net_sales",
            color="channel" if split else None,
            color_discrete_map=CHANNEL_COLORS,
            category_orders={"channel": list(CHANNEL_COLORS)},
            labels={"date_key": "Date", "net_sales": "Net sales", "channel": "Channel"},
        )
        fig.update_traces(line_width=2, hovertemplate="%{x|%d %b %Y}<br>%{y:,.2f}<extra>%{fullData.name}</extra>")
        if not split:
            fig.update_traces(line_color=BLUE, name="Net sales")
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(style(fig), width="stretch")
        if dropped:
            unit = {"Weekly": "week", "Monthly": "month"}.get(grain, "day")
            st.caption(f"Incomplete {unit}s at the edges of the date range are hidden "
                       "so they don't read as a drop in sales.")

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Revenue by channel")
            by_ch = f.groupby("channel", as_index=False)["net_sales"].sum().sort_values("net_sales")
            by_ch["share"] = by_ch["net_sales"] / by_ch["net_sales"].sum()
            fig = go.Figure(go.Bar(
                x=by_ch["net_sales"], y=by_ch["channel"], orientation="h",
                marker=dict(color=[CHANNEL_COLORS.get(c, BLUE) for c in by_ch["channel"]],
                            cornerradius=4),
                text=[f"{money(v)} · {s:.0%}" for v, s in zip(by_ch["net_sales"], by_ch["share"], strict=True)],
                textposition="outside", textfont=dict(color=INK_2),
                hovertemplate="%{y}<br>%{x:,.2f}<extra></extra>", cliponaxis=False,
            ))
            fig.update_xaxes(showticklabels=False, showgrid=False, range=[0, max(fig.data[0].x) * 1.35])
            fig.update_yaxes(showgrid=False)
            st.plotly_chart(style(fig, 260), width="stretch")

            st.subheader("Revenue by category")
            by_cat = f.groupby("category", as_index=False)["net_sales"].sum().sort_values("net_sales")
            fig = go.Figure(go.Bar(
                x=by_cat["net_sales"], y=by_cat["category"], orientation="h",
                marker=dict(color=BLUE, cornerradius=4),
                text=[money(v) for v in by_cat["net_sales"]], textposition="outside",
                textfont=dict(color=INK_2), hovertemplate="%{y}<br>%{x:,.2f}<extra></extra>",
                cliponaxis=False,
            ))
            fig.update_xaxes(showticklabels=False, showgrid=False, range=[0, max(fig.data[0].x) * 1.35])
            fig.update_yaxes(showgrid=False)
            st.plotly_chart(style(fig, 300), width="stretch")

        with c2:
            st.subheader("Top 10 products by net sales")
            top = (f.groupby(["product_name", "category"], as_index=False)
                    .agg(net_sales=("net_sales", "sum"), units=("quantity", "sum"))
                    .nlargest(10, "net_sales").sort_values("net_sales"))
            fig = go.Figure(go.Bar(
                x=top["net_sales"], y=top["product_name"], orientation="h",
                marker=dict(color=BLUE, cornerradius=4),
                customdata=top[["category", "units"]],
                text=[money(v) for v in top["net_sales"]], textposition="outside",
                textfont=dict(color=INK_2), cliponaxis=False,
                hovertemplate="%{y}<br>%{customdata[0]}<br>Net sales %{x:,.2f}"
                              "<br>Units %{customdata[1]:,}<extra></extra>",
            ))
            fig.update_xaxes(showticklabels=False, showgrid=False, range=[0, max(fig.data[0].x) * 1.35])
            fig.update_yaxes(showgrid=False)
            fig.update_yaxes(tickfont=dict(color=INK_2, size=12))
            st.plotly_chart(style(fig, 600), width="stretch")

# --- inventory ---------------------------------------------------------------
with tab_inv:
    st.caption("Inventory comes from daily warehouse snapshots, so the channel and "
               "loyalty-tier filters don't apply here.")
    if inv_f.empty:
        st.info("No inventory snapshots match these filters.")
    else:
        risk = inv_f[inv_f["is_stockout_risk"]].copy()
        k1, k2, k3 = st.columns(3)
        k1.metric("Snapshots", f"{len(inv_f):,}")
        k2.metric("At stockout risk", f"{len(risk):,}")
        k3.metric("Risk rate", f"{len(risk)/len(inv_f):.1%}")

        st.subheader("Stockout / low-stock items")
        st.markdown("Items where quantity on hand is at or below the reorder point.")
        risk["gap"] = risk["reorder_point"] - risk["quantity_on_hand"]
        risk = risk.sort_values(["gap", "date_key"], ascending=[False, False])
        st.dataframe(
            risk[["date_key", "store_id", "region", "product_name", "category",
                  "quantity_on_hand", "reorder_point", "gap"]],
            hide_index=True, width="stretch",
            column_config={
                "date_key": st.column_config.DateColumn("Snapshot date", format="DD MMM YYYY"),
                "store_id": "Store", "region": "Region", "product_name": "Product",
                "category": "Category",
                "quantity_on_hand": st.column_config.NumberColumn("On hand", format="%d"),
                "reorder_point": st.column_config.NumberColumn("Reorder point", format="%d"),
                "gap": st.column_config.NumberColumn("Units short", format="%d",
                                                     help="Reorder point minus on-hand quantity"),
            },
        )

        st.subheader("Stockout-risk items by region")
        by_reg = (risk.groupby("region").size().reindex(sorted(inv_f["region"].unique()), fill_value=0)
                  .rename("items").reset_index().sort_values("items"))
        fig = go.Figure(go.Bar(
            x=by_reg["items"], y=by_reg["region"], orientation="h",
            marker=dict(color=BLUE, cornerradius=4), text=by_reg["items"],
            textposition="outside", textfont=dict(color=INK_2), cliponaxis=False,
            hovertemplate="%{y}<br>%{x} items<extra></extra>",
        ))
        fig.update_xaxes(showticklabels=False, showgrid=False, range=[0, max(fig.data[0].x) * 1.35])
        fig.update_yaxes(showgrid=False)
        st.plotly_chart(style(fig, 240), width="stretch")

# --- data quality ------------------------------------------------------------
with tab_quality:
    st.caption("Pipeline health from the latest Silver run. Invalid rows are quarantined, "
               "never silently dropped. These figures are not affected by the filters.")
    total_in, total_q = q_score["rows_in"].sum(), q_score["rows_quarantined"].sum()
    k1, k2, k3 = st.columns(3)
    k1.metric("Rows checked", f"{total_in:,}")
    k2.metric("Rows quarantined", f"{total_q:,}")
    k3.metric("Overall pass rate", f"{(total_in - total_q) / total_in:.1%}")

    c1, c2 = st.columns([1, 1])
    with c1:
        st.subheader("Quality scorecard by source")
        sc = q_score.sort_values("pass_rate_pct")
        st.dataframe(
            sc[["source_name", "rows_in", "rows_clean", "rows_quarantined", "pass_rate_pct"]],
            hide_index=True, width="stretch",
            column_config={
                "source_name": "Source",
                "rows_in": st.column_config.NumberColumn("Rows in", format="%d"),
                "rows_clean": st.column_config.NumberColumn("Clean", format="%d"),
                "rows_quarantined": st.column_config.NumberColumn("Quarantined", format="%d"),
                "pass_rate_pct": st.column_config.ProgressColumn(
                    "Pass rate", format="%.1f%%", min_value=0, max_value=100),
            },
        )
    with c2:
        st.subheader("Why rows were quarantined")
        qs = q_summary.copy()
        qs["label"] = qs["source_name"] + " · " + qs["failed_rule"].str.replace("_", " ")
        qs = qs.sort_values("quarantined_count")
        fig = go.Figure(go.Bar(
            x=qs["quarantined_count"], y=qs["label"], orientation="h",
            marker=dict(color=BLUE, cornerradius=4), text=qs["quarantined_count"],
            textposition="outside", textfont=dict(color=INK_2), cliponaxis=False,
            hovertemplate="%{y}<br>%{x} rows<extra></extra>",
        ))
        fig.update_xaxes(showticklabels=False, showgrid=False, range=[0, max(fig.data[0].x) * 1.35])
        fig.update_yaxes(showgrid=False)
        fig.update_yaxes(tickfont=dict(color=INK_2, size=12))
        st.plotly_chart(style(fig, 380), width="stretch")

st.divider()
st.caption(
    f"Built by Ravindi · CCA Data Engineer internship project · "
    f"[Source code]({REPO_URL}). Data is synthetic (generated with a fixed seed); "
    f"customer PII is hashed in the Silver layer and never reaches this dashboard."
)
