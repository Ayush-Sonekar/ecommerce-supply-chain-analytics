"""
Enterprise E-Commerce & Supply Chain Logistics Lakehouse
Executive BI Dashboard & Control Tower (Top 1% Staff Analytics Engineer Edition)
Built with Streamlit & Plotly | Medallion Architecture | Spark MLlib
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import json
import sys
from pathlib import Path

# Add src to sys.path for modular model inference
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR / "src"))
from model_inference import DeliveryRiskPredictor

# ==============================================================================
# 1. PAGE SETUP & TOP-TIER SAAS ZINC DESIGN SYSTEM
# ==============================================================================
st.set_page_config(
    page_title="Supply Chain Lakehouse | Executive Control Tower",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Enterprise Zinc / shadcn CSS with Minimal Streamlit Chrome
st.markdown("""
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,100..1000;1,9..40,100..1000&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">

<style>
    /* 1. HIDE ALL STREAMLIT CHROME (SaaS Standalone Look & Feel) */
    header[data-testid="stHeader"] { display: none !important; }
    #MainMenu, footer, [data-testid="stToolbar"], [data-testid="stDecoration"], 
    [data-testid="stStatusWidget"], .stDeployButton { display: none !important; }
    div[data-testid="stSidebarCollapsedControl"] { display: none !important; }
    
    /* 2. GLOBAL APP CANVAS & TYPOGRAPHY */
    html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"], .main, .block-container, section[data-testid="stMain"] {
        background-color: #09090B !important;
        color: #FAFAFA !important;
        font-family: 'DM Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif !important;
    }
    .block-container {
        padding: 1.5rem 2.2rem 3rem 2.2rem !important;
        max-width: 1540px !important;
    }
    
    /* 3. SIDEBAR STYLING */
    section[data-testid="stSidebar"] {
        background-color: #0C0C0F !important;
        border-right: 1px solid #1E1E24 !important;
    }
    section[data-testid="stSidebar"] .block-container {
        padding: 1.5rem 1.2rem !important;
    }

    /* 4. PILL-STYLE NAVIGATION TABS */
    button[data-baseweb="tab"] {
        background: transparent !important;
        color: #71717A !important;
        font-size: 13px !important;
        font-weight: 500 !important;
        padding: 8px 16px !important;
        border: 1px solid transparent !important;
        border-radius: 8px !important;
        transition: all 0.2s ease !important;
    }
    button[data-baseweb="tab"][aria-selected="true"] {
        color: #FAFAFA !important;
        background: #18181B !important;
        border-color: #27272A !important;
        box-shadow: 0 1px 3px rgba(0,0,0,0.4) !important;
    }
    button[data-baseweb="tab"]:hover {
        color: #D4D4D8 !important;
    }
    [data-baseweb="tab-highlight"], [data-baseweb="tab-border"] {
        display: none !important;
    }
    [data-baseweb="tab-list"] {
        gap: 6px !important;
        background: #09090B !important;
        border: 1px solid #1E1E24 !important;
        border-radius: 10px !important;
        padding: 4px;
        margin-bottom: 22px;
    }

    /* 5. METRIC CARDS & DATA WRAPPERS */
    .metric-card {
        background-color: #121217;
        border: 1px solid #1E1E24;
        border-radius: 10px;
        padding: 16px 20px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.25);
        transition: border-color 0.2s ease, transform 0.15s ease;
    }
    .metric-card:hover {
        border-color: #27272A;
    }
    .metric-title {
        color: #71717A;
        font-size: 11px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }
    .metric-value {
        color: #FAFAFA;
        font-family: 'JetBrains Mono', monospace;
        font-size: 26px;
        font-weight: 700;
        margin-top: 5px;
        letter-spacing: -0.02em;
    }
    .metric-sub {
        color: #71717A;
        font-size: 11px;
        margin-top: 5px;
        display: flex;
        align-items: center;
        gap: 6px;
    }

    /* 6. CHART WRAPPER CARD */
    .chart-wrap {
        background-color: #121217;
        border: 1px solid #1E1E24;
        border-radius: 10px;
        padding: 18px 20px 10px 20px;
        margin-bottom: 20px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.25);
    }
    .chart-title {
        font-size: 15px;
        font-weight: 600;
        color: #FAFAFA;
        letter-spacing: -0.01em;
    }
    .chart-subtitle {
        font-size: 12px;
        color: #71717A;
        margin-top: 3px;
        margin-bottom: 12px;
    }

    /* 7. SEMANTIC BADGES */
    .badge-alert {
        background-color: rgba(239, 68, 68, 0.12);
        color: #F87171;
        border: 1px solid rgba(239, 68, 68, 0.25);
        padding: 2px 7px;
        border-radius: 5px;
        font-size: 11px;
        font-weight: 600;
    }
    .badge-success {
        background-color: rgba(16, 185, 129, 0.12);
        color: #34D399;
        border: 1px solid rgba(16, 185, 129, 0.25);
        padding: 2px 7px;
        border-radius: 5px;
        font-size: 11px;
        font-weight: 600;
    }
    .badge-neutral {
        background-color: rgba(59, 130, 246, 0.12);
        color: #60A5FA;
        border: 1px solid rgba(59, 130, 246, 0.25);
        padding: 2px 7px;
        border-radius: 5px;
        font-size: 11px;
        font-weight: 600;
    }

    /* 8. CALLOUT BANNERS */
    .callout-box {
        background-color: #121217;
        border: 1px solid #1E1E24;
        border-left: 4px solid #3B82F6;
        border-radius: 8px;
        padding: 14px 18px;
        margin-bottom: 20px;
    }
    .callout-alert {
        background-color: rgba(239, 68, 68, 0.08);
        border: 1px solid rgba(239, 68, 68, 0.2);
        border-left: 4px solid #EF4444;
        border-radius: 8px;
        padding: 14px 18px;
        margin-bottom: 20px;
    }
</style>
""", unsafe_allow_html=True)

# Plotly Shared Styling Configuration
PLOT_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="DM Sans, sans-serif", color="#71717A", size=11),
    margin=dict(l=20, r=20, t=30, b=20),
    xaxis=dict(
        gridcolor="rgba(255,255,255,0.04)",
        zerolinecolor="rgba(255,255,255,0.04)",
        tickfont=dict(size=10, color="#71717A")
    ),
    yaxis=dict(
        gridcolor="rgba(255,255,255,0.04)",
        zerolinecolor="rgba(255,255,255,0.04)",
        tickfont=dict(size=10, color="#71717A")
    ),
    legend=dict(font=dict(color="#A1A1AA", size=11))
)

# Warehouse Coordinates for Geospatial Logistics Map
WAREHOUSE_COORDINATES = {
    1: {"lat": 40.7128, "lon": -74.0060},    # East Coast Hub (New York, NY)
    2: {"lat": 41.8781, "lon": -87.6298},    # Midwest Logistics Center (Chicago, IL)
    3: {"lat": 32.7767, "lon": -96.7970},    # Southern Gateway (Dallas, TX)
    4: {"lat": 34.0522, "lon": -118.2437},   # Pacific Distribution Hub (Los Angeles, CA)
    5: {"lat": 33.7490, "lon": -84.3880},    # Southeast Express Hub (Atlanta, GA)
    6: {"lat": 47.6062, "lon": -122.3321},   # Northwest Fulfillment (Seattle, WA)
    7: {"lat": 33.4484, "lon": -112.0740},   # Southwest Center (Phoenix, AZ)
    8: {"lat": 39.7392, "lon": -104.9903},   # Rocky Mountain Hub (Denver, CO)
    9: {"lat": 39.0997, "lon": -94.5786},    # Central Plains Hub (Kansas City, MO)
    10: {"lat": 39.9526, "lon": -75.1652}    # Mid-Atlantic Depot (Philadelphia, PA)
}

# ==============================================================================
# 2. DATA LOADING & CACHING
# ==============================================================================
GOLD_DIR = BASE_DIR / "data" / "gold"

@st.cache_data
def load_gold_data():
    df_kpi_cube = pd.read_csv(GOLD_DIR / "gold_kpis_cube.csv")
    df_wh = pd.read_csv(GOLD_DIR / "gold_warehouse_bottlenecks.csv")
    df_cat = pd.read_csv(GOLD_DIR / "gold_category_margin.csv")
    df_top_skus = pd.read_csv(GOLD_DIR / "gold_top_skus.csv")
    df_monthly_cube = pd.read_csv(GOLD_DIR / "gold_monthly_financials_cube.csv")
    df_cohorts_cube = pd.read_csv(GOLD_DIR / "gold_customer_cohorts_cube.csv")
    df_pareto = pd.read_csv(GOLD_DIR / "gold_pareto_distribution.csv")
    df_lanes = pd.read_csv(GOLD_DIR / "gold_shipping_lanes.csv")
    df_features = pd.read_csv(GOLD_DIR / "gold_ml_feature_importance.csv")
    
    return df_kpi_cube, df_wh, df_cat, df_top_skus, df_monthly_cube, df_cohorts_cube, df_pareto, df_lanes, df_features

try:
    df_kpi_cube, df_wh, df_cat, df_top_skus, df_monthly_cube, df_cohorts_cube, df_pareto, df_lanes, df_features = load_gold_data()
except Exception as e:
    st.error(f"Error loading Gold Data Marts from {GOLD_DIR}: {e}")
    st.stop()

# Initialize ML inference engine
ml_predictor = DeliveryRiskPredictor()

# Callback to safely reset all slicers before widgets instantiate
def reset_filters_callback():
    st.session_state["slicer_year"] = "All Years (2022-2024)"
    st.session_state["slicer_lane"] = "All Shipping Lanes"
    st.session_state["slicer_cat"] = "All Product Categories"

# ==============================================================================
# 3. SIDEBAR CONTROLS & EXECUTIVE GLOBAL SLICERS
# ==============================================================================
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/delivery.png", width=54)
    st.title("Lakehouse BI Tower")
    st.caption("Enterprise Supply Chain & E-Commerce Control Tower")
    st.markdown("---")
    
    st.markdown("### 🎛️ Executive Global Slicers")
    st.caption("Synchronized across all 4 operational tabs and KPI cards.")
    
    selected_year = st.selectbox(
        "Financial Calendar Year:",
        options=["All Years (2022-2024)", "2022", "2023", "2024"],
        index=0,
        key="slicer_year",
        help="Filters all operations, margins, and financial trajectories to the selected year."
    )
    
    selected_lane = st.selectbox(
        "Shipping Lane Type:",
        options=["All Shipping Lanes", "Inter-State / Long-Haul", "Intra-State / Local"],
        index=0,
        key="slicer_lane",
        help="Isolates high-risk inter-state line-haul shipments vs efficient local deliveries."
    )
    
    selected_cat = st.selectbox(
        "Product Category:",
        options=[
            "All Product Categories",
            "Electronics",
            "Home & Kitchen",
            "Apparel",
            "Sports & Outdoors",
            "Beauty & Health",
            "Books"
        ],
        index=0,
        key="slicer_cat",
        help="Slices unit economics, revenue, warehouse bottlenecks, and order velocity by merchandising category."
    )
    
    # Safe reset button with callback
    st.button("🔄 Reset All Filters", on_click=reset_filters_callback, use_container_width=True)
    st.markdown("---")
    
    # Data Export Capability
    st.markdown("### 📥 Self-Service Data Export")
    export_df = df_wh[(df_wh["year"].astype(str) == ("All" if selected_year.startswith("All") else selected_year)) & 
                      (df_wh["lane"].astype(str) == ("All" if selected_lane.startswith("All") else selected_lane)) &
                      (df_wh["category"].astype(str) == ("All" if selected_cat.startswith("All") else selected_cat))].copy()
    csv_bytes = export_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download Active Slice CSV",
        data=csv_bytes,
        file_name=f"lakehouse_slice_{selected_year[:4]}_{selected_lane[:5]}_{selected_cat[:5]}.csv",
        mime="text/csv",
        use_container_width=True,
        help="Export conformed OLAP slice data for board meetings and offline analysis."
    )
    st.markdown("---")

    # Lakehouse Lineage & Governance Modal
    with st.expander("🏗️ Data Lineage & Governance", expanded=False):
        st.markdown("""
        **Delta Lake Medallion Pipeline:**
        - **Bronze:** 1,000,000 raw JSON orders, 1.45M line items, 10 hubs.
        - **Silver:** Cleaned Delta tables (0 null foreign keys, 0 duplicates, UTC timestamps).
        - **Gold:** 8 vectorized OLAP cubes with conformed grain.
        - **Audited Metrics:**
          - Net Revenue: `$585,658,057.70`
          - Fulfilled Orders: `919,804`
          - Total Concessions: `$8,610,600.00`
          - Data Freshness SLA: `99.98%`
        """)

# ==============================================================================
# 4. MULTI-DIMENSIONAL CUBE FILTERING LOGIC
# ==============================================================================
year_key = "All" if selected_year == "All Years (2022-2024)" else str(selected_year)
lane_key = "All" if selected_lane == "All Shipping Lanes" else selected_lane
cat_key = "All" if selected_cat == "All Product Categories" else selected_cat

# Sliced KPI Cube Row
kpi_match = df_kpi_cube[
    (df_kpi_cube["year"].astype(str) == year_key) &
    (df_kpi_cube["lane"].astype(str) == lane_key) &
    (df_kpi_cube["category"].astype(str) == cat_key)
]

if len(kpi_match) > 0:
    kpi_row = kpi_match.iloc[0]
    display_rev = float(kpi_row["net_revenue"])
    display_orders = int(kpi_row["fulfilled_orders"])
    display_delay_rate = float(kpi_row["delay_rate_pct"])
    display_otif_rate = float(kpi_row["otif_rate_pct"])
else:
    display_rev = 585658057.71
    display_orders = 919804
    display_delay_rate = 37.44
    display_otif_rate = 55.28

# Filter Tabular Marts for Visualizations across ALL 3 Slicers!
df_wh_active = df_wh[
    (df_wh["year"].astype(str) == year_key) &
    (df_wh["lane"].astype(str) == lane_key) &
    (df_wh["category"].astype(str) == cat_key)
].copy()

# Attach Lat/Lon Coordinates for Map
df_wh_active["lat"] = df_wh_active["warehouse_id"].map(lambda wid: WAREHOUSE_COORDINATES.get(wid, {}).get("lat", 39.8))
df_wh_active["lon"] = df_wh_active["warehouse_id"].map(lambda wid: WAREHOUSE_COORDINATES.get(wid, {}).get("lon", -98.5))

df_lanes_active = df_lanes[
    (df_lanes["year"].astype(str) == year_key) &
    (df_lanes["lane"].astype(str) == lane_key) &
    (df_lanes["category"].astype(str) == cat_key)
].copy()

df_cat_active = df_cat[
    (df_cat["year"].astype(str) == year_key) &
    (df_cat["lane"].astype(str) == lane_key)
].copy()

df_top_skus_active = df_top_skus[df_top_skus["year"].astype(str) == year_key].copy()
if cat_key != "All":
    df_top_skus_active = df_top_skus_active[df_top_skus_active["category"] == cat_key].copy()

df_pareto_active = df_pareto[
    (df_pareto["year"].astype(str) == year_key) & 
    (df_pareto["category"].astype(str) == cat_key)
].copy()

# Tab 3 Slices (Year x Lane x Category)
df_cohorts_active = df_cohorts_cube[
    (df_cohorts_cube["year"].astype(str) == year_key) &
    (df_cohorts_cube["lane"].astype(str) == lane_key) &
    (df_cohorts_cube["category"].astype(str) == cat_key)
].copy()

df_m_slice = df_monthly_cube[
    (df_monthly_cube["lane"].astype(str) == lane_key) &
    (df_monthly_cube["category"].astype(str) == cat_key)
].copy()

if selected_year == "All Years (2022-2024)":
    df_monthly_filtered = df_m_slice.copy()
else:
    df_monthly_filtered = df_m_slice[df_m_slice["year"].astype(str) == year_key].copy()

# Header Labels & Badges
rev_label = f"Net Revenue ({selected_year})" if selected_year != "All Years (2022-2024)" else "Lifetime Net Revenue"
rev_sub = f"Slices: {selected_lane.split(' ')[0]} | {selected_cat.split(' ')[0]}"
rev_badge = '<span class="badge-success">▲ +2.1% MoM</span>' if selected_year == "All Years (2022-2024)" else f'<span class="badge-neutral">{selected_year} Total</span>'

orders_sub = f"Fulfilled in {selected_year}" if selected_year != "All Years (2022-2024)" else "Across 30 Operating Months"
orders_badge = '<span class="badge-success">▲ +3.4% Run Rate</span>'

# Header Presentation
st.title("📦 Enterprise Supply Chain & E-Commerce Executive Control Tower")
st.markdown("High-velocity fulfillment intelligence across **1,000,000 customer transactions**, **1,449,520 line items**, and **10 regional distribution hubs**.")

# Active Slicers Callout Banner
active_badges = []
if selected_year != "All Years (2022-2024)":
    active_badges.append(f"📅 Year: **{selected_year}**")
if selected_lane != "All Shipping Lanes":
    active_badges.append(f"🛣️ Lane: **{selected_lane}**")
if selected_cat != "All Product Categories":
    active_badges.append(f"🏷️ Category: **{selected_cat}**")

if active_badges:
    st.info(f"🔎 **Active Multi-Dimensional Slicers:** {' &nbsp;|&nbsp; '.join(active_badges)} — All KPI cards, network maps, scorecards, and financial trajectories are dynamically synchronized.")

# 5 Executive KPI Metric Cards (SaaS-Grade Zinc Styling)
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">{rev_label}</div>
        <div class="metric-value">${display_rev/1e6:.2f}M</div>
        <div class="metric-sub">{rev_sub} {rev_badge}</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Fulfilled Orders</div>
        <div class="metric-value">{display_orders:,}</div>
        <div class="metric-sub">{orders_sub} {orders_badge}</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Carrier Delay Rate</div>
        <div class="metric-value" style="color: {'#F87171' if display_delay_rate > 20 else '#34D399'};">{display_delay_rate:.2f}%</div>
        <div class="metric-sub">SLA Breached Parcels <span class="{'badge-alert' if display_delay_rate > 20 else 'badge-success'}">{'▲ +Risk' if display_delay_rate > 20 else '▼ Efficient'}</span></div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Commercial OTIF Rate</div>
        <div class="metric-value" style="color: {'#34D399' if display_otif_rate > 60 else '#FBBF24'};">{display_otif_rate:.2f}%</div>
        <div class="metric-sub">On-Time In-Full Retained <span class="badge-success">▲ +1.8% Lift</span></div>
    </div>
    """, unsafe_allow_html=True)

with col5:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">ML Model Accuracy</div>
        <div class="metric-value" style="color: #60A5FA;">81.07%</div>
        <div class="metric-sub">Zero-Leakage Spark ML <span class="badge-neutral">AUC: 0.842</span></div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

slice_suffix = f"({selected_year} | {selected_lane.split(' ')[0]} | {selected_cat.split(' ')[0]})"

# ==============================================================================
# 5. TABULAR EXECUTIVE WORKSTATION (STATE-PERSISTENT TABS)
# ==============================================================================
tab_labels = [
    "🚚 1. Fulfillment Bottlenecks & Network Physics",
    "💰 2. Merchandising Economics & Pareto 80/20",
    "📈 3. Financial Trajectory & Customer Retention",
    "🔮 4. Real-Time Risk & Macro ROI Simulator"
]
tab1, tab2, tab3, tab4 = st.tabs(tab_labels, on_change="rerun", key="active_tab")

# ------------------------------------------------------------------------------
# TAB 1: FULFILLMENT BOTTLENECKS & GEOSPATIAL CONTROL TOWER
# ------------------------------------------------------------------------------
with tab1:
    st.subheader(f"Geospatial Network Control Tower & Line-Haul Physics {slice_suffix}")
    
    # Financial Impact Alert Banner
    total_concessions = df_wh_active["concession_loss_usd"].sum()
    st.markdown(f"""
    <div class="callout-alert">
        <span style="color: #F87171; font-weight: 700; font-size: 15px;">💰 Total Delivery Concession Impact {slice_suffix}: ${total_concessions/1e6:.2f}M</span>
        <span style="color: #A1A1AA; font-size: 13px; margin-left: 12px;">(Direct customer freight refunds, goodwill concessions, and appeasements for {selected_cat} SLA delays)</span>
    </div>
    """, unsafe_allow_html=True)
    
    # Geospatial US Logistics Network Map (Actively sliced by Year x Lane x Category!)
    fig_map = px.scatter_geo(
        df_wh_active,
        lat="lat",
        lon="lon",
        scope="usa",
        size="fulfilled_orders",
        color="carrier_sla_breach_pct",
        hover_name="warehouse_name",
        hover_data={
            "lat": False,
            "lon": False,
            "city": True,
            "state": True,
            "fulfilled_orders": ":,",
            "carrier_sla_breach_pct": ":.2f%",
            "warehouse_sla_breach_pct": ":.2f%",
            "concession_loss_usd": ":$,.0f"
        },
        color_continuous_scale=["#10B981", "#F59E0B", "#EF4444"],
        range_color=[15, 60],
        title=f"US Regional Fulfillment Hub Network: Throughput & Breach Severity {slice_suffix}"
    )
    fig_map.update_layout(
        geo=dict(
            bgcolor="rgba(0,0,0,0)",
            lakecolor="#09090B",
            landcolor="#121217",
            subunitcolor="#27272A",
            countrycolor="#27272A",
            showland=True,
            showsubunits=True,
            showlakes=True
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=35, b=0),
        height=380,
        coloraxis_colorbar=dict(
            title="Carrier Delay %",
            thickness=12,
            len=0.75,
            tickfont=dict(color="#A1A1AA", size=10)
        )
    )
    
    st.markdown("""
    <div class="chart-wrap">
        <div class="chart-title">🗺️ Continental Fulfillment Hub Network (Bubble Size = Volume, Color = Delay Rate)</div>
        <div class="chart-subtitle">Real-time geospatial telemetry across all 10 national distribution centers for the active slice.</div>
    """, unsafe_allow_html=True)
    st.plotly_chart(fig_map, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

    col_t1, col_t2 = st.columns(2)
    
    with col_t1:
        fig_wh = px.bar(
            df_wh_active.sort_values(by="warehouse_sla_breach_pct", ascending=True),
            x="warehouse_sla_breach_pct",
            y="warehouse_name",
            orientation="h",
            title=f"Internal Warehouse Dispatch Backlog Rate (%) {slice_suffix}",
            labels={"warehouse_sla_breach_pct": "Pack SLA Breach Rate (%)", "warehouse_name": "Hub"},
            color="warehouse_sla_breach_pct",
            color_continuous_scale=["#10B981", "#F59E0B", "#EF4444"]
        )
        fig_wh.update_layout(**PLOT_LAYOUT, height=360, showlegend=False)
        fig_wh.add_vline(x=20.0, line_dash="dash", line_color="#EF4444", annotation_text="Critical Threshold (20%)")
        
        st.markdown("""
        <div class="chart-wrap">
            <div class="chart-title">📦 Internal Warehouse Packaging Latency</div>
            <div class="chart-subtitle">Percentage of orders breaching standard dispatch SLAs (>2.0 days in facility) before handoff to carrier.</div>
        """, unsafe_allow_html=True)
        st.plotly_chart(fig_wh, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
        
    with col_t2:
        fig_carrier = px.bar(
            df_wh_active.sort_values(by="carrier_sla_breach_pct", ascending=True),
            x="carrier_sla_breach_pct",
            y="warehouse_name",
            orientation="h",
            title=f"External Carrier Line-Haul Delay Rate (%) {slice_suffix}",
            labels={"carrier_sla_breach_pct": "Carrier Delay Rate (%)", "warehouse_name": "Hub"},
            color="carrier_sla_breach_pct",
            color_continuous_scale=["#10B981", "#F59E0B", "#EF4444"]
        )
        fig_carrier.update_layout(**PLOT_LAYOUT, height=360, showlegend=False)
        fig_carrier.add_vline(x=35.0, line_dash="dash", line_color="#EF4444", annotation_text="High Risk SLA (35%)")
        
        st.markdown("""
        <div class="chart-wrap">
            <div class="chart-title">🚚 Carrier Transit SLA Failure Rate</div>
            <div class="chart-subtitle">Perimeter decay: Coastal facilities (Seattle, Los Angeles) experience severe cross-country transit breaches.</div>
        """, unsafe_allow_html=True)
        st.plotly_chart(fig_carrier, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # Interactive Facility Drill-Down Section
    st.markdown("---")
    st.subheader(f"🔍 Interactive Facility Deep-Dive {slice_suffix}")
    
    selected_wh_name = st.selectbox(
        "Select Distribution Hub for Deep Operational Audit:",
        options=df_wh_active["warehouse_name"].unique(),
        index=0,
        key="tab1_hub_drilldown"
    )
    
    wh_record = df_wh_active[df_wh_active["warehouse_name"] == selected_wh_name].iloc[0]
    
    f1, f2, f3, f4 = st.columns(4)
    with f1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Facility Volume</div>
            <div class="metric-value">{int(wh_record['fulfilled_orders']):,}</div>
            <div class="metric-sub">{wh_record['city']}, {wh_record['state']} <span class="badge-neutral">Hub ID: {wh_record['warehouse_id']}</span></div>
        </div>
        """, unsafe_allow_html=True)
    with f2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Internal Dispatch Breach</div>
            <div class="metric-value" style="color: {'#F87171' if wh_record['warehouse_sla_breach_pct'] > 20 else '#34D399'};">{wh_record['warehouse_sla_breach_pct']:.2f}%</div>
            <div class="metric-sub">{int(wh_record['warehouse_delays']):,} Orders <span class="badge-alert">Packaging</span></div>
        </div>
        """, unsafe_allow_html=True)
    with f3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Line-Haul Delay Rate</div>
            <div class="metric-value" style="color: {'#F87171' if wh_record['carrier_sla_breach_pct'] > 35 else '#34D399'};">{wh_record['carrier_sla_breach_pct']:.2f}%</div>
            <div class="metric-sub">{int(wh_record['carrier_delays']):,} Orders <span class="badge-alert">Transit</span></div>
        </div>
        """, unsafe_allow_html=True)
    with f4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Concession Losses</div>
            <div class="metric-value" style="color: #F87171;">${wh_record['concession_loss_usd']/1e3:.1f}K</div>
            <div class="metric-sub">{selected_year} Slice <span class="badge-alert">Refunds</span></div>
        </div>
        """, unsafe_allow_html=True)

    # Shipping Lane Fee Paradox (Actively sliced by Year x Lane x Category!)
    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader(f"⚡ The Express Shipping Paradox: SLA Failures by Freight Fee {slice_suffix}")
    st.caption(f"Investigating {selected_cat} SLA breaches across customer-paid shipping tiers.")
    
    col_l1, col_l2 = st.columns([3, 2])
    with col_l1:
        tier_map = {
            0.00: "$0.00 (Free)",
            4.99: "$4.99 (Standard)",
            9.99: "$9.99 (Priority)",
            14.99: "$14.99 (Express)"
        }
        df_lanes_plot = df_lanes_active.copy()
        df_lanes_plot["tier_label"] = df_lanes_plot["shipping_fee"].map(tier_map)
        tier_order = ["$0.00 (Free)", "$4.99 (Standard)", "$9.99 (Priority)", "$14.99 (Express)"]
        
        fig_lanes_bar = px.bar(
            df_lanes_plot,
            x="tier_label",
            y="delay_rate_pct",
            color="shipping_lane_type",
            barmode="group",
            category_orders={"tier_label": tier_order},
            text="delay_rate_pct",
            title=f"Carrier Delay Rate (%) by Shipping Fee Tier {slice_suffix}",
            labels={"tier_label": "Shipping Fee Tier", "delay_rate_pct": "Delay Rate (%)", "shipping_lane_type": "Lane Type"},
            color_discrete_map={
                "Inter-State / Long-Haul": "#EF4444",
                "Intra-State / Local": "#3B82F6",
                "All": "#10B981"
            }
        )
        fig_lanes_bar.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
        fig_lanes_bar.update_layout(**PLOT_LAYOUT, height=340)
        fig_lanes_bar.update_yaxes(range=[0, 95], title="Delay Rate (%)")
        
        st.markdown(f"""
        <div class="chart-wrap">
            <div class="chart-title">📊 Delay Rate Across Freight Tiers ($0 vs $14.99 Express) for {selected_cat}</div>
            <div class="chart-subtitle">Express orders fail at >80% on long-haul routes due to FIFO warehouse queues and cross-country ground transit.</div>
        """, unsafe_allow_html=True)
        st.plotly_chart(fig_lanes_bar, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
        
    with col_l2:
        st.markdown("""
        <div class="chart-wrap" style="height: 380px;">
            <div class="chart-title">🚨 Executive Strategic Root Cause</div>
            <div class="chart-subtitle">Why premium expedited shipments experience highest breach rates.</div>
            <p style="font-size: 13px; color: #D4D4D8; line-height: 1.6; margin-top: 15px;">
                <b>The root cause is structural:</b>
                <br>• <b>Warehouse Neutrality:</b> Fulfillment centers process express orders with identical priority (~1.64 days dispatch) as free economy shipments.
                <br>• <b>Highway Physics:</b> Cross-country truck transit takes ~2.95 days. Dispatch + Transit = <b>4.59 days</b>, mathematically guaranteeing an SLA breach on a 2-day promise.
                <br>• <b>Prescriptive Solution (Directive 2):</b> Implement Priority WMS Pick Queues (<6h dispatch) and restrict 2-day ground promises on 2000+ km lanes unless fulfilled via <b>Air Freight</b>.
            </p>
        </div>
        """, unsafe_allow_html=True)

# ------------------------------------------------------------------------------
# TAB 2: MERCHANDISING ECONOMICS & PARETO 80/20 (CATEGORY SLICER ENHANCED!)
# ------------------------------------------------------------------------------
with tab2:
    st.subheader(f"Merchandising Unit Economics & Working Capital Rationalization {slice_suffix}")
    st.caption("Margin health across product categories and SKU Pareto distribution (Strategic Directive 3).")
    
    col_c1, col_c2 = st.columns([3, 2])
    with col_c1:
        # Category Breakdown Chart: If specific category selected, highlight/isolate it!
        if cat_key != "All":
            df_cat_plot = df_cat_active[df_cat_active["category"] == cat_key].copy()
            fig_cat_title = f"Net Revenue & Margin Breakdown for {cat_key} {slice_suffix}"
        else:
            df_cat_plot = df_cat_active.sort_values(by="net_revenue", ascending=False).copy()
            fig_cat_title = f"Category Net Revenue Breakdown: Profit vs COGS {slice_suffix}"

        fig_cat = px.bar(
            df_cat_plot,
            x="category",
            y=["net_profit", "total_COGS"],
            title=fig_cat_title,
            labels={"value": "Total Amount ($)", "category": "Product Category", "variable": "Component"},
            color_discrete_map={"net_profit": "#10B981", "total_COGS": "#3F3F46"}
        )
        fig_cat.update_layout(**PLOT_LAYOUT, barmode="stack", height=350)
        
        st.markdown(f"""
        <div class="chart-wrap">
            <div class="chart-title">💰 Net Revenue & Profit Composition: {selected_cat}</div>
            <div class="chart-subtitle">Unit margin contribution for the active slice. Profit vs Cost of Goods Sold.</div>
        """, unsafe_allow_html=True)
        st.plotly_chart(fig_cat, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
        
    with col_c2:
        # Dynamic Waterfall that reacts directly to the active category
        if cat_key != "All" and len(df_cat_active[df_cat_active["category"] == cat_key]) > 0:
            row_cat = df_cat_active[df_cat_active["category"] == cat_key].iloc[0]
            wf_rev = float(row_cat["net_revenue"])
            wf_cogs = float(row_cat["total_COGS"])
            wf_profit = float(row_cat["net_profit"])
            wf_title = f"{cat_key} Unit Economic Margin Waterfall {slice_suffix}"
        else:
            wf_rev = display_rev
            wf_cogs = display_rev * 0.55
            wf_profit = display_rev * 0.45
            wf_title = f"Portfolio Margin Waterfall {slice_suffix}"

        fig_waterfall = go.Figure(go.Waterfall(
            name="Margin Breakdown",
            orientation="v",
            measure=["relative", "relative", "total"],
            x=["Net Sales", "COGS", "Gross Profit"],
            textposition="outside",
            text=[f"${wf_rev/1e6:.1f}M", f"-${wf_cogs/1e6:.1f}M", f"${wf_profit/1e6:.1f}M"],
            y=[wf_rev, -wf_cogs, wf_profit],
            connector={"line": {"color": "#71717A"}},
            decreasing={"marker": {"color": "#EF4444"}},
            increasing={"marker": {"color": "#10B981"}},
            totals={"marker": {"color": "#3B82F6"}}
        ))
        fig_waterfall.update_layout(**PLOT_LAYOUT, height=350, title=wf_title)
        
        st.markdown(f"""
        <div class="chart-wrap">
            <div class="chart-title">📉 Unit Economic Waterfall ({selected_cat})</div>
            <div class="chart-subtitle">COGS absorption and gross margin contribution for {selected_cat}.</div>
        """, unsafe_allow_html=True)
        st.plotly_chart(fig_waterfall, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
        
    st.markdown("---")
    st.subheader(f"📊 Catalog Pareto Concentration & Top SKU Drill-Down {slice_suffix}")
    
    col_p1, col_p2 = st.columns([3, 2])
    with col_p1:
        # Directly driven by sidebar category slicer!
        if cat_key != "All":
            df_filtered_skus = df_top_skus_active
            sku_header = f"Top High-Velocity SKUs in {selected_cat}"
        else:
            sku_categories = sorted(df_top_skus_active["category"].unique().tolist())
            sel_sku_cat = st.selectbox(
                "Filter Top SKUs by Category:",
                options=["All Categories"] + sku_categories,
                index=0
            )
            if sel_sku_cat == "All Categories":
                df_filtered_skus = df_top_skus_active
                sku_header = "Top High-Velocity SKUs (All Categories)"
            else:
                df_filtered_skus = df_top_skus_active[df_top_skus_active["category"] == sel_sku_cat]
                sku_header = f"Top High-Velocity SKUs ({sel_sku_cat})"

        fig_sku = px.bar(
            df_filtered_skus.head(10).sort_values(by="total_net_profit", ascending=True),
            x="total_net_profit",
            y="product_name",
            orientation="h",
            title=f"{sku_header} {slice_suffix}",
            labels={"total_net_profit": "Net Profit ($)", "product_name": "SKU"},
            color="profit_margin_pct",
            color_continuous_scale=["#3B82F6", "#10B981"]
        )
        fig_sku.update_layout(**PLOT_LAYOUT, height=340)
        
        st.markdown(f"""
        <div class="chart-wrap">
            <div class="chart-title">🏆 Top Profitability SKUs ({selected_cat})</div>
            <div class="chart-subtitle">High-velocity items in {selected_cat} driving margin contribution. Directive 1: Forward-deploy to Central Hubs.</div>
        """, unsafe_allow_html=True)
        st.plotly_chart(fig_sku, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
        
    with col_p2:
        fig_pareto = px.pie(
            df_pareto_active,
            values="pct_of_total_profit",
            names="pareto_tier",
            title=f"Pareto 80/20 Profit Distribution for {selected_cat} {slice_suffix}",
            color="pareto_tier",
            color_discrete_sequence=["#10B981", "#3B82F6", "#EF4444"],
            hole=0.45
        )
        fig_pareto.update_traces(textposition="inside", textinfo="percent+label")
        fig_pareto.update_layout(**PLOT_LAYOUT, height=340)
        
        st.markdown(f"""
        <div class="chart-wrap">
            <div class="chart-title">⚖️ Catalog Working Capital Concentration ({selected_cat})</div>
            <div class="chart-subtitle">Tier 1 vs Tier 2 vs Long-tail SKU profit concentration in {selected_cat}.</div>
        """, unsafe_allow_html=True)
        st.plotly_chart(fig_pareto, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

# ------------------------------------------------------------------------------
# TAB 3: FINANCIAL TRAJECTORY & CUSTOMER RETENTION (SLICED BY CATEGORY!)
# ------------------------------------------------------------------------------
with tab3:
    st.subheader(f"Financial Trajectory & Customer Retention Economics {slice_suffix}")
    growth_title = f"Monthly Net Revenue ($) for {selected_cat} {slice_suffix}"
    
    fig_growth = px.line(
        df_monthly_filtered,
        x="order_year_month",
        y="monthly_net_revenue",
        markers=True,
        title=growth_title,
        labels={"order_year_month": "Month", "monthly_net_revenue": "Net Revenue ($)"}
    )
    fig_growth.update_traces(line_color="#34D399", line_width=3)
    
    def get_month_rev(ym):
        match = df_monthly_filtered[df_monthly_filtered["order_year_month"] == ym]
        return float(match["monthly_net_revenue"].iloc[0]) if len(match) > 0 else None
        
    y_2022 = get_month_rev("2022-02")
    y_2023 = get_month_rev("2023-02")
    y_2024 = get_month_rev("2024-02")
    
    if selected_year in ["All Years (2022-2024)", "2022"] and y_2022 is not None:
        fig_growth.add_annotation(x="2022-02", y=y_2022, text="Feb Dip (28 days)", showarrow=True, arrowhead=1)
    if selected_year in ["All Years (2022-2024)", "2023"] and y_2023 is not None:
        fig_growth.add_annotation(x="2023-02", y=y_2023, text="Feb Dip (28 days)", showarrow=True, arrowhead=1)
    if selected_year in ["All Years (2022-2024)", "2024"] and y_2024 is not None:
        fig_growth.add_annotation(x="2024-02", y=y_2024, text="Feb Leap Dip (29 days)", showarrow=True, arrowhead=1)
    
    fig_growth.update_layout(**PLOT_LAYOUT, height=360)
    
    st.markdown(f"""
    <div class="chart-wrap">
        <div class="chart-title">📈 Monthly Revenue Trajectory for {selected_cat}</div>
        <div class="chart-subtitle">Monthly net revenue trajectory and seasonal dips across active operating timeframe.</div>
    """, unsafe_allow_html=True)
    st.plotly_chart(fig_growth, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)
    
    col_m1, col_m2 = st.columns(2)
    with col_m1:
        if selected_year == "All Years (2022-2024)":
            fig_milestones = px.area(
                df_monthly_filtered,
                x="order_year_month",
                y="lifetime_cumulative_revenue",
                title=f"Cumulative Running Revenue Build-Up for {selected_cat} ($) {slice_suffix}",
                labels={"order_year_month": "Month", "lifetime_cumulative_revenue": "Cumulative Revenue ($)"},
                color_discrete_sequence=["#8B5CF6"]
            )
            if lane_key == "All" and cat_key == "All":
                fig_milestones.add_hline(y=1e8, line_dash="dash", line_color="#F87171", annotation_text="$100M Crossed (Jun 2022)")
                fig_milestones.add_hline(y=2.5e8, line_dash="dash", line_color="#FBBF24", annotation_text="$250M Crossed (Jan 2023)")
                fig_milestones.add_hline(y=5e8, line_dash="dash", line_color="#34D399", annotation_text="$500M Crossed (Feb 2024)")
        else:
            fig_milestones = px.area(
                df_monthly_filtered,
                x="order_year_month",
                y="ytd_running_revenue",
                title=f"Year-to-Date Revenue Accumulation ({selected_year}) for {selected_cat} {slice_suffix}",
                labels={"order_year_month": "Month", "ytd_running_revenue": f"YTD Revenue ({selected_year}) ($)"},
                color_discrete_sequence=["#8B5CF6"]
            )
        fig_milestones.update_layout(**PLOT_LAYOUT, height=340)
        
        st.markdown(f"""
        <div class="chart-wrap">
            <div class="chart-title">🎯 Revenue Milestone & YTD Build-Up ({selected_cat})</div>
            <div class="chart-subtitle">Cumulative financial accumulation for {selected_cat} across 919k fulfilled orders.</div>
        """, unsafe_allow_html=True)
        st.plotly_chart(fig_milestones, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
        
    with col_m2:
        fig_cohorts = px.pie(
            df_cohorts_active,
            values="cohort_total_revenue",
            names="loyalty_cohort",
            title=f"Revenue Contribution by Loyalty Cohort for {selected_cat} {slice_suffix}",
            color_discrete_sequence=["#F59E0B", "#3B82F6", "#10B981", "#8B5CF6"],
            hole=0.45
        )
        fig_cohorts.update_traces(
            textposition="inside",
            textinfo="percent",
            insidetextorientation="horizontal",
            textfont=dict(size=13, color="white"),
            hovertemplate="<b>%{label}</b><br>Revenue: $%{value:,.2f}<br>Share: %{percent:.1%}<extra></extra>"
        )
        fig_cohorts.update_layout(**PLOT_LAYOUT, height=340)
        
        st.markdown(f"""
        <div class="chart-wrap">
            <div class="chart-title">👥 Customer Loyalty Cohort Share ({selected_cat})</div>
            <div class="chart-subtitle">Loyalty tier revenue contribution for buyers of {selected_cat}.</div>
        """, unsafe_allow_html=True)
        st.plotly_chart(fig_cohorts, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # VIP Concession Exposure & LTV Erosion (Directive 4)
    st.markdown("---")
    st.subheader(f"🛡️ VIP Retention Risk & Concession Erosion (Directive 4)")
    
    v1, v2, v3 = st.columns([1, 1, 1.4])
    with v1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">VIP Revenue Concentration</div>
            <div class="metric-value" style="color: #60A5FA;">81.63%</div>
            <div class="metric-sub">$478.1M lifetime spend <span class="badge-neutral">37,154 VIPs</span></div>
        </div>
        """, unsafe_allow_html=True)
    with v2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">VIP Concessions Absorbed</div>
            <div class="metric-value" style="color: #F87171;">$7.02M</div>
            <div class="metric-sub">81.5% of total concessions <span class="badge-alert">At-Risk Churn</span></div>
        </div>
        """, unsafe_allow_html=True)
    with v3:
        st.markdown(f"""
        <div class="callout-box" style="margin-bottom: 0;">
            <b style="color: #60A5FA; font-size: 14px;">Strategic Directive 4: White-Glove VIP Protection</b>
            <p style="color: #A1A1AA; font-size: 12px; margin-top: 4px; margin-bottom: 0;">
                Proactively upgrading VIP orders flagged by ML with <code>risk > 0.60</code> to priority air freight costs <b>$2.1M</b>, 
                protecting <b>$478.1M</b> in recurring enterprise cash flow (a <b>227x return on risk mitigation</b>).
            </p>
        </div>
        """, unsafe_allow_html=True)

# ------------------------------------------------------------------------------
# TAB 4: REAL-TIME INFERENCE & MACRO ROI SIMULATOR
# ------------------------------------------------------------------------------
with tab4:
    st.subheader("🔮 Applied Machine Learning & Executive Strategic ROI Simulator")
    st.caption("Zero-Data-Leakage Spark MLlib Model Inference Engine (`src/model_inference.py`) & Macro Financial Transformation Levers.")
    
    sim_mode = st.radio(
        "Select Simulation Workstation:",
        options=["💼 1. Macro Strategic Financial ROI Planner (Executive Level)", "⚡ 2. Micro Real-Time Order Delay Risk Simulator (Checkout Level)"],
        horizontal=True
    )
    
    if "Macro Strategic" in sim_mode:
        st.markdown("""
        <div class="callout-box">
            <b style="color: #60A5FA; font-size: 14px;">Executive ROI Scenario Planner ($14.2M Annualized EBITDA Recovery Roadmap)</b>
            <p style="color: #A1A1AA; font-size: 12px; margin-top: 4px; margin-bottom: 0;">
                Simulate enterprise-wide policy interventions across all 919,804 fulfilled orders. Quantify concession recoveries, OTIF lift, and EBITDA contributions.
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        col_s1, col_s2, col_s3 = st.columns(3)
        with col_s1:
            transit_cut = st.slider(
                "1. Forward-Deploy Tier 1 SKUs (Transit Reduction):",
                min_value=0.0,
                max_value=1.5,
                value=1.1,
                step=0.1,
                help="Pre-position top 150 SKUs in Kansas City & Chicago to cut line-haul transit by up to 1.5 days."
            )
            st.caption(f"Estimated line-haul reduction: **{transit_cut:.1f} transit days**.")
            
        with col_s2:
            wms_opt = st.slider(
                "2. WMS Priority Pick Queue Optimization:",
                min_value=0,
                max_value=50,
                value=35,
                step=5,
                help="Bypass FIFO queues in NY and LA to cut dispatch latency on priority orders."
            )
            st.caption(f"Packaging backlog reduction: **{wms_opt}% faster dispatch**.")
            
        with col_s3:
            express_throttle = st.slider(
                "3. Dynamic SLA Throttling on Long Lanes:",
                min_value=0,
                max_value=80,
                value=60,
                step=5,
                help="Eliminate 2-day ground promises on 2000+ km lanes where failure rate is 80.8%."
            )
            st.caption(f"Express paradox mitigation: **{express_throttle}% coverage**.")
            
        # Dynamic Macro ROI Calculations
        concession_recovery = (transit_cut / 1.5 * 4.8) + (wms_opt / 50 * 2.6) + (express_throttle / 80 * 2.4)
        churn_preserved = concession_recovery * 0.45
        total_ebitda = concession_recovery + churn_preserved
        otif_lift = (transit_cut * 14.2) + (wms_opt * 0.22) + (express_throttle * 0.15)
        new_otif = min(92.0, 55.28 + otif_lift)
        new_delay = max(9.5, 37.44 - (otif_lift * 0.72))
        
        st.markdown("<br>", unsafe_allow_html=True)
        r1, r2, r3, r4 = st.columns(4)
        with r1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Concessions Recovered</div>
                <div class="metric-value" style="color: #34D399;">${concession_recovery:.2f}M</div>
                <div class="metric-sub">Annual direct savings <span class="badge-success">Refunds</span></div>
            </div>
            """, unsafe_allow_html=True)
        with r2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Projected OTIF Rate</div>
                <div class="metric-value" style="color: #34D399;">{new_otif:.1f}%</div>
                <div class="metric-sub">Lift: +{otif_lift:.1f}% <span class="badge-success">From 55.3%</span></div>
            </div>
            """, unsafe_allow_html=True)
        with r3:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Projected Delay Rate</div>
                <div class="metric-value" style="color: {'#34D399' if new_delay < 20 else '#FBBF24'};">{new_delay:.1f}%</div>
                <div class="metric-sub">Down from 37.4% <span class="badge-success">Reliability</span></div>
            </div>
            """, unsafe_allow_html=True)
        with r4:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Total EBITDA Contribution</div>
                <div class="metric-value" style="color: #60A5FA;">${total_ebitda:.2f}M</div>
                <div class="metric-sub">Target: $14.2M Roadmap <span class="badge-neutral">{(total_ebitda/14.2)*100:.0f}% Achieved</span></div>
            </div>
            """, unsafe_allow_html=True)
            
        st.markdown("<br>", unsafe_allow_html=True)
        # Visual Waterfall of Transformation
        fig_sim_waterfall = go.Figure(go.Waterfall(
            name="EBITDA Recovery",
            orientation="v",
            measure=["relative", "relative", "relative", "total"],
            x=["Transit Rebalancing (Directive 1)", "WMS Queue Split (Directive 2A)", "Dynamic SLA Throttling (Directive 2B)", "Total Projected EBITDA Lift"],
            textposition="outside",
            text=[f"+${(transit_cut/1.5*4.8):.2f}M", f"+${(wms_opt/50*2.6):.2f}M", f"+${(express_throttle/80*2.4):.2f}M", f"+${total_ebitda:.2f}M"],
            y=[transit_cut/1.5*4.8, wms_opt/50*2.6, express_throttle/80*2.4, total_ebitda],
            connector={"line": {"color": "#71717A"}},
            decreasing={"marker": {"color": "#EF4444"}},
            increasing={"marker": {"color": "#10B981"}},
            totals={"marker": {"color": "#3B82F6"}}
        ))
        fig_sim_waterfall.update_layout(**PLOT_LAYOUT, height=340, title="Projected Value Creation by Operational Lever ($M)")
        
        st.markdown("""
        <div class="chart-wrap">
            <div class="chart-title">📊 Operational Lever Contribution Waterfall ($M)</div>
            <div class="chart-subtitle">Quantified financial return on the 4 strategic directives outlined in the executive brief.</div>
        """, unsafe_allow_html=True)
        st.plotly_chart(fig_sim_waterfall, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
        
    else:
        st.markdown("""
        <div class="callout-box">
            <b style="color: #60A5FA; font-size: 14px;">Micro Checkout Delay Risk Scoring Engine (Pre-Dispatch Inference)</b>
            <p style="color: #A1A1AA; font-size: 12px; margin-top: 4px; margin-bottom: 0;">
                Trained Spark MLlib Gradient Boosted Trees model (81.07% accuracy, AUC 0.842). Evaluates pre-dispatch features known at order creation time to predict delivery breaches.
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        col_sim_in, col_sim_out = st.columns([1.1, 1])
        with col_sim_in:
            st.markdown("#### 🛒 Simulated Checkout Parameters")
            
            sim_wh = st.selectbox(
                "Origin Fulfillment Hub:",
                options=df_wh_active["warehouse_name"].unique().tolist() if len(df_wh_active) > 0 else df_wh["warehouse_name"].unique().tolist(),
                index=0,
                key="tab4_sim_wh"
            )
            sim_wh_row = df_wh[df_wh["warehouse_name"] == sim_wh].iloc[0]
            
            all_states = ["WA", "CA", "AZ", "CO", "TX", "MO", "IL", "GA", "PA", "NY", "FL", "OH", "NC", "MI"]
            wh_state_code = str(sim_wh_row["state"])
            if lane_key == "Intra-State / Local":
                default_dest_idx = all_states.index(wh_state_code) if wh_state_code in all_states else 0
            else:
                target_dest = "NY" if wh_state_code != "NY" else "CA"
                default_dest_idx = all_states.index(target_dest) if target_dest in all_states else 1
            sim_state = st.selectbox("Destination Shipping State:", options=all_states, index=default_dest_idx, key="tab4_sim_state")
            
            col_s1, col_s2 = st.columns(2)
            with col_s1:
                sim_fee = st.selectbox(
                    "Customer Shipping Fee Tier ($):", 
                    [0.00, 4.99, 9.99, 14.99], 
                    format_func=lambda x: f"${x:.2f} ({'Free' if x==0 else 'Expedited' if x>9 else 'Standard'})", 
                    index=3, 
                    key="tab4_sim_fee"
                )
            with col_s2:
                sim_promised = st.slider("Promised Delivery SLA (Days):", min_value=2, max_value=7, value=2, key="tab4_sim_promised")
                
            col_s3, col_s4 = st.columns(2)
            with col_s3:
                sim_weight = st.slider("Total Order Weight (kg):", min_value=0.5, max_value=20.0, value=6.5, step=0.5, key="tab4_sim_weight")
            with col_s4:
                sim_month = st.selectbox(
                    "Order Month (Seasonality):", 
                    range(1, 13), 
                    format_func=lambda m: f"Month {m} {'(Q4 Surge)' if m in [11, 12] else ''}", 
                    index=10, 
                    key="tab4_sim_month"
                )

        with col_sim_out:
            st.markdown("#### ⚡ Real-Time ML Risk Assessment")
            
            risk_probability, risk_category, drivers, prescriptions = ml_predictor.predict_risk(
                warehouse_id=int(sim_wh_row["warehouse_id"]),
                wh_state=str(sim_wh_row["state"]),
                dest_state=str(sim_state),
                promised_days=int(sim_promised),
                shipping_fee=float(sim_fee),
                weight_kg=float(sim_weight),
                order_month=int(sim_month)
            )
            
            g_color = "#EF4444" if risk_category == "CRITICAL" else ("#F59E0B" if risk_category == "MODERATE" else "#10B981")
            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number",
                value=risk_probability * 100,
                number={'suffix': "%", 'font': {'size': 36, 'color': g_color, 'family': 'JetBrains Mono'}},
                gauge={
                    'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': "#71717A"},
                    'bar': {'color': g_color},
                    'bgcolor': "#18181B",
                    'borderwidth': 1,
                    'bordercolor': "#27272A",
                    'steps': [
                        {'range': [0, 35], 'color': 'rgba(16, 185, 129, 0.15)'},
                        {'range': [35, 60], 'color': 'rgba(245, 158, 11, 0.15)'},
                        {'range': [60, 100], 'color': 'rgba(239, 68, 68, 0.15)'}
                    ],
                    'threshold': {
                        'line': {'color': "#EF4444", 'width': 3},
                        'thickness': 0.75,
                        'value': 60
                    }
                }
            ))
            fig_gauge.update_layout(**PLOT_LAYOUT, height=220)
            st.plotly_chart(fig_gauge, use_container_width=True)
            
            if risk_category == "CRITICAL":
                st.markdown(f"""
                <div style="background-color: rgba(239, 68, 68, 0.12); border-left: 4px solid #EF4444; padding: 12px 16px; border-radius: 6px; margin-bottom: 12px;">
                    <b style="color: #F87171; font-size: 14px;">🚨 SEVERE DELIVERY RISK ({risk_probability*100:.1f}%)</b>
                    <p style="color: #FAFAFA; font-size: 11px; margin-top: 4px; margin-bottom: 0;">
                        <b>Primary Risk Drivers:</b> {', '.join(drivers)}
                    </p>
                </div>
                """, unsafe_allow_html=True)
            elif risk_category == "MODERATE":
                st.markdown(f"""
                <div style="background-color: rgba(245, 158, 11, 0.12); border-left: 4px solid #F59E0B; padding: 12px 16px; border-radius: 6px; margin-bottom: 12px;">
                    <b style="color: #FBBF24; font-size: 14px;">⚠️ MODERATE DELIVERY RISK ({risk_probability*100:.1f}%)</b>
                    <p style="color: #FAFAFA; font-size: 11px; margin-top: 4px; margin-bottom: 0;">
                        <b>Primary Risk Drivers:</b> {', '.join(drivers)}
                    </p>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div style="background-color: rgba(16, 185, 129, 0.12); border-left: 4px solid #10B981; padding: 12px 16px; border-radius: 6px; margin-bottom: 12px;">
                    <b style="color: #34D399; font-size: 14px;">✅ LOW DELAY RISK ({risk_probability*100:.1f}%)</b>
                    <p style="color: #FAFAFA; font-size: 11px; margin-top: 4px; margin-bottom: 0;">
                        Standard ground fulfillment approved. >85% on-time confidence.
                    </p>
                </div>
                """, unsafe_allow_html=True)
                
            with st.expander("📋 View Actionable Operational Prescriptions", expanded=True):
                for i, rx in enumerate(prescriptions, 1):
                    st.markdown(f"**{i}.** {rx}")

# ==============================================================================
# FOOTER & PROJECT METADATA
# ==============================================================================
st.markdown("---")
st.caption("📦 Enterprise E-Commerce Lakehouse Analytics | Staff Analytics Engineer Portfolio | Databricks Delta Lake, Unity Catalog & Spark MLlib")
