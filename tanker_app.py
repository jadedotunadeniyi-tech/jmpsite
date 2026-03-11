"""
=============================================================
  OIL TANKER DAUGHTER VESSEL OPERATION — STREAMLIT DASHBOARD
  Wraps: tanker_simulation_v5.py
=============================================================
  Run locally:
      streamlit run tanker_app.py

  Deploy (Streamlit Community Cloud):
      1. Push tanker_app.py + tanker_simulation_v5.py + requirements.txt to GitHub
      2. share.streamlit.io → New app → tanker_app.py → Deploy

  Google Sheets — TWO tabs required:
  ─────────────────────────────────────────────────────────
  Tab 1  "volumes"   — one row per daily 8am update
    Columns: timestamp | chapel_bbl | jasmines_bbl | westmore_bbl |
             duke_bbl | starturn_bbl | bryanston_bbl | alkebulan_bbl |
             greeneagle_bbl | production_bph | sim_days

  Tab 2  "fleet"     — one row per vessel per daily 8am update
    Columns: timestamp | vessel | status | location | cargo_bbl | notes
    vessel   → exact name: Sherlock, Laphroaig, Rathbone, Bedford,
               Balham, Woodstock, Bagshot, Watson
    status   → valid code: IDLE_A | LOADING | SAILING_AB | SAILING_CROSS_BW_AC |
               SAILING_BW_TO_FWY | SAILING_AB_LEG2 | SAILING_B_TO_FWY |
               SAILING_FWY_TO_BW | SAILING_CROSS_BW_IN_AC | SAILING_BW_TO_A |
               SAILING_BA | WAITING_RETURN_STOCK | PF_LOADING | PF_SWAP |
               SAILING_D_CHANNEL | SAILING_CH_TO_BW_OUT | SAILING_CROSS_BW_OUT |
               SAILING_B_TO_BW_IN | SAILING_CROSS_BW_IN | SAILING_BW_TO_CH_IN | SAILING_CH_TO_D |
               WAITING_BERTH_B | BERTHING_B | DISCHARGING | CAST_OFF_B |
               WAITING_FAIRWAY | BERTHING_A | HOSE_CONNECT_A | HOSE_CONNECT_B |
               DOCUMENTING | WAITING_DEAD_STOCK | WAITING_CAST_OFF | CAST_OFF
    location → free text, e.g. "Bryanston", "Fairway Buoy",
               "En Route SanBarth→BIA", "Chapel"
=============================================================
"""

import sys, os, types, colorsys, time, json
import re
import io
import csv
import math
import hashlib
import binascii
import itertools
import datetime as _dt
import unittest.mock as _mock
from datetime import datetime

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Tanker Ops v5",
    page_icon="🛢️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  /* ── Global light-mode base ─────────────────────────────────────── */
  html, body, [class*="css"] {
    font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
  }
  .block-container {
    padding-top: 1.2rem;
    padding-bottom: 2rem;
    background: #f8f9fb;
  }

  /* ── Sidebar ────────────────────────────────────────────────────── */
  div[data-testid="stSidebarContent"] {
    background: #1a2744;
    border-right: 1px solid #243460;
  }
  div[data-testid="stSidebarContent"] * {
    color: #c8d6f0 !important;
  }
  div[data-testid="stSidebarContent"] h1,
  div[data-testid="stSidebarContent"] h2,
  div[data-testid="stSidebarContent"] h3,
  div[data-testid="stSidebarContent"] strong,
  div[data-testid="stSidebarContent"] b {
    color: #e8eef8 !important;
  }
  div[data-testid="stSidebarContent"] label {
    color: #9db3d8 !important;
    font-size: 12px !important;
    font-weight: 600 !important;
  }
  div[data-testid="stSidebarContent"] input,
  div[data-testid="stSidebarContent"] div[data-baseweb="input"] {
    background: #243460 !important;
    color: #e8eef8 !important;
    border: 1px solid #344d80 !important;
    border-radius: 6px !important;
  }
  div[data-testid="stSidebarContent"] input::placeholder {
    color: #5a7ab0 !important;
  }
  div[data-testid="stSidebarContent"] button {
    background: #2d4070 !important;
    color: #c8d6f0 !important;
    border: 1px solid #3d5490 !important;
  }
  div[data-testid="stSidebarContent"] button:hover {
    background: #344d80 !important;
  }
  div[data-testid="stSidebarContent"] div[data-testid="stFileUploader"] {
    background: #1f2f58 !important;
    border: 1px dashed #344d80 !important;
    border-radius: 6px !important;
  }
  div[data-testid="stSidebarContent"] div[data-baseweb="select"] div {
    background: #243460 !important;
    color: #e8eef8 !important;
    border-color: #344d80 !important;
  }
  div[data-testid="stSidebarContent"] .stMarkdown p,
  div[data-testid="stSidebarContent"] .stMarkdown li,
  div[data-testid="stSidebarContent"] caption,
  div[data-testid="stSidebarContent"] small {
    color: #8aa4cc !important;
  }
  div[data-testid="stSidebarContent"] hr {
    border-color: #2d4070 !important;
  }

  /* ── KPI cards ──────────────────────────────────────────────────── */
  .kpi-card {
    background: #ffffff;
    border-radius: 10px;
    padding: 14px 10px;
    text-align: center;
    border: 1px solid #e2e8f0;
    margin-bottom: 6px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.07);
  }
  .kpi-label {
    color: #64748b;
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: .07em;
    margin-bottom: 4px;
  }
  .kpi-value {
    color: #0f172a;
    font-size: 22px;
    font-weight: 800;
  }
  .kpi-sub {
    color: #94a3b8;
    font-size: 11px;
    margin-top: 3px;
  }

  /* ── Section headers ────────────────────────────────────────────── */
  .sec-hdr {
    background: linear-gradient(90deg, #1a2744 0%, #243460 100%);
    border-left: 4px solid #3b82f6;
    padding: 8px 16px;
    border-radius: 6px;
    margin: 24px 0 12px;
    color: #ffffff;
    font-weight: 700;
    font-size: 14px;
    letter-spacing: .03em;
    box-shadow: 0 2px 6px rgba(26,39,68,0.18);
  }

  /* ── Pill badges ────────────────────────────────────────────────── */
  .pill {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 999px;
    font-size: 11px;
    font-weight: 700;
    margin: 2px;
  }

  /* ── Alert boxes ────────────────────────────────────────────────── */
  .alert-warn {
    background: #fffbeb;
    border: 1px solid #f59e0b;
    border-left: 4px solid #f59e0b;
    border-radius: 6px;
    padding: 10px 14px;
    color: #92400e;
    font-size: 13px;
    margin: 6px 0;
  }
  .alert-info {
    background: #eff6ff;
    border: 1px solid #3b82f6;
    border-left: 4px solid #3b82f6;
    border-radius: 6px;
    padding: 10px 14px;
    color: #1e40af;
    font-size: 13px;
    margin: 6px 0;
  }
  .alert-ok {
    background: #f0fdf4;
    border: 1px solid #22c55e;
    border-left: 4px solid #22c55e;
    border-radius: 6px;
    padding: 10px 14px;
    color: #14532d;
    font-size: 13px;
    margin: 6px 0;
  }

  /* ── Optimizer ──────────────────────────────────────────────────── */
  .opt-best {
    background: linear-gradient(135deg, #f0fdf4, #eff6ff);
    border: 1px solid #22c55e;
    border-radius: 12px;
    padding: 20px 24px;
    margin: 10px 0;
    box-shadow: 0 2px 12px rgba(34,197,94,0.12);
  }
  .opt-score { font-size: 52px; font-weight: 900; color: #16a34a; line-height: 1; }
  .opt-badge {
    display: inline-block;
    background: #16a34a;
    color: #fff;
    border-radius: 5px;
    padding: 2px 10px;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: .05em;
    margin-left: 10px;
    vertical-align: middle;
  }
  .opt-param {
    background: #f1f5f9;
    border: 1px solid #e2e8f0;
    border-radius: 6px;
    padding: 6px 12px;
    display: inline-block;
    margin: 3px;
    font-size: 12px;
    color: #1e40af;
    font-weight: 600;
  }
  .score-bar-wrap { background: #e2e8f0; border-radius: 4px; height: 12px; overflow: hidden; margin: 2px 0; }
  .score-bar { height: 12px; border-radius: 4px; transition: width .3s; }

  /* ── Fleet status cards ─────────────────────────────────────────── */
  .vcard {
    border-radius: 10px;
    padding: 13px 15px;
    border-left-width: 4px;
    border-left-style: solid;
    margin-bottom: 10px;
    background: #ffffff;
    box-shadow: 0 1px 4px rgba(0,0,0,0.08);
  }
  .vcard-name   { font-weight: 700; font-size: 14px; color: #0f172a; margin-bottom: 3px; }
  .vcard-status { font-size: 12px; color: #374151; margin-bottom: 2px; }
  .vcard-loc    { font-size: 11px; color: #64748b; margin-bottom: 6px; }
  .vcard-bar-bg { background: #e2e8f0; border-radius: 4px; height: 6px; }
  .vcard-bar-fg { height: 6px; border-radius: 4px; }

  /* ── Recommendation cards ───────────────────────────────────────── */
  .rec-card {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 18px 20px;
    margin: 8px 0;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
  }
  .rec-title  { color: #0f172a; font-size: 13px; font-weight: 700; margin-bottom: 7px; }
  .rec-body   { color: #475569; font-size: 12px; line-height: 1.7; }
  .rec-metric { margin-top: 8px; font-size: 11px; color: #94a3b8; }
  .hl-yellow  { color: #d97706; font-weight: 700; }
  .hl-green   { color: #16a34a; font-weight: 700; }
  .hl-blue    { color: #2563eb; font-weight: 700; }

  /* ── Summary section cards ──────────────────────────────────────── */
  .summary-card {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 20px 22px;
    margin: 8px 0;
    box-shadow: 0 1px 6px rgba(0,0,0,0.06);
  }
  .summary-card h4 {
    color: #1a2744;
    font-size: 13px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: .06em;
    margin: 0 0 10px 0;
    padding-bottom: 8px;
    border-bottom: 2px solid #e2e8f0;
  }
  .summary-card p, .summary-card li {
    color: #374151;
    font-size: 13px;
    line-height: 1.75;
    margin: 4px 0;
  }
  .summary-card ul { padding-left: 18px; margin: 6px 0; }
  .summary-tag {
    display: inline-block;
    padding: 2px 9px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 700;
    margin: 2px 3px 2px 0;
  }
  .tag-green  { background: #dcfce7; color: #14532d; }
  .tag-amber  { background: #fef9c3; color: #713f12; }
  .tag-red    { background: #fee2e2; color: #7f1d1d; }
  .tag-blue   { background: #dbeafe; color: #1e3a8a; }
  .tag-navy   { background: #1a2744; color: #c8d6f0; }

  /* ── Main area text contrast ────────────────────────────────────── */
  .stMarkdown p, .stMarkdown li { color: #1e293b; font-size: 13px; }
  .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 { color: #0f172a; }
  .stDataFrame { border-radius: 8px; overflow: hidden; }
  .stCaption, caption { color: #64748b !important; }
</style>
""", unsafe_allow_html=True)

# ── Colour palettes ────────────────────────────────────────────────────────────
VESSEL_COLORS = {
    "Sherlock" : "#ff6b6b",   # coral red     — boosted from #e74c3c
    "Laphroaig": "#2ecc71",   # emerald green — unchanged
    "Rathbone" : "#c77dff",   # light violet  — boosted from #9b59b6
    "Bedford"  : "#f39c12",   # amber         — unchanged
    "Balham"   : "#1abc9c",   # teal          — unchanged
    "Woodstock": "#ff4d8d",   # hot pink      — boosted from #e91e63
    "Bagshot"  : "#00bcd4",   # cyan          — unchanged
    "Watson"   : "#b0bec5",   # silver        — lightened from #95a5a6
}
STORAGE_COLORS = {
    "Chapel"  : "#f1c40f",   # golden yellow  — unchanged
    "JasmineS": "#bf7fff",   # soft lavender  — boosted from #8e44ad
    "Westmore": "#2ecc71",   # emerald green  — brightened from #27ae60
    "Duke"    : "#5dade2",   # sky blue       — lightened from #3498db
    "Starturn": "#f07030",   # vivid orange   — brightened from #d35400
}
MOTHER_COLORS = {
    "Bryanston" : "#1abc9c",  # bright teal   — boosted from #16a085
    "Alkebulan" : "#ff5555",  # vivid red     — boosted from #c0392b
    "GreenEagle": "#c084fc",  # vivid purple  — boosted from #7d3c98
}
STATUS_LIGHTNESS = {
    "IDLE_A":2.0,"WAITING_STOCK":1.8,"WAITING_BERTH_A":1.7,"WAITING_DEAD_STOCK":1.6,
    "BERTHING_A":1.3,"HOSE_CONNECT_A":1.1,"LOADING":1.0,"PF_LOADING":1.0,"PF_SWAP":0.9,
    "DOCUMENTING":0.9,"WAITING_CAST_OFF":0.85,"CAST_OFF":0.8,
    "SAILING_AB":0.68,"SAILING_CROSS_BW_AC":0.72,"SAILING_BW_TO_FWY":0.7,"SAILING_AB_LEG2":0.65,"SAILING_B_TO_FWY":0.68,"SAILING_FWY_TO_BW":0.67,"SAILING_CROSS_BW_IN_AC":0.72,"SAILING_BW_TO_A":0.65,
    "SAILING_D_CHANNEL":0.68,"SAILING_CH_TO_BW_OUT":0.67,"SAILING_CROSS_BW_OUT":0.72,
    "SAILING_B_TO_BW_IN":0.7,"SAILING_CROSS_BW_IN":0.72,
    "SAILING_BW_TO_CH_IN":0.67,"SAILING_CH_TO_D":0.65,
    "WAITING_FAIRWAY":0.6,"WAITING_BERTH_B":0.6,"WAITING_MOTHER_RETURN":0.55,
    "WAITING_MOTHER_CAPACITY":0.5,"WAITING_RETURN_STOCK":0.52,
    "BERTHING_B":0.5,"HOSE_CONNECT_B":0.45,"DISCHARGING":0.4,
    "CAST_OFF_B":0.38,"SAILING_BA":0.5,"IDLE_B":0.55,"SAILING_B_TO_F":0.65,
    "WAITING_DAYLIGHT":1.5,"WAITING_TIDAL":1.45,
}
STATUS_LABELS = {
    # ── At SanBarth / Sego / Awoba / Dawes — Storage ─────────────────────────────────────────
    "IDLE_A"              : "Idle at storage (SanBarth/Sego/Awoba/Dawes)",
    "WAITING_STOCK"       : "SanBarth — Waiting, low stock",
    "WAITING_DEAD_STOCK"  : "SanBarth — Waiting, dead-stock threshold",
    "WAITING_BERTH_A"     : "SanBarth — Waiting for berth",
    "BERTHING_A"          : "SanBarth — Berthing at storage",
    "HOSE_CONNECT_A"      : "SanBarth — Hose connection",
    "LOADING"             : "SanBarth — Loading cargo",
    "DOCUMENTING"         : "SanBarth — Documentation",
    "WAITING_CAST_OFF"    : "Waiting for daylight cast-off window",
    "SAILING_B_TO_F"      : "BIA → Ibom (swap takeover transit)",
    "CAST_OFF"            : "SanBarth — Cast off from storage",
    # ── Ibom (Offshore loading) ────────────────────────────────────────────
    "PF_LOADING"          : "Ibom — Loading at offshore buoy",
    "PF_SWAP"             : "Ibom — Vessel swap in progress",
    # ── Sailing Point A/C → BIA (4-leg route via breakwater and fairway buoy) ─────────────────
    "SAILING_AB"          : "Sailing Point A/C → Breakwater (1.5h)",
    "SAILING_CROSS_BW_AC"  : "Crossing Breakwater outbound (0.5h, tidal)",
    "SAILING_BW_TO_FWY"    : "After crossing → Fairway Buoy (2h)",
    "SAILING_AB_LEG2"      : "Fairway Buoy → BIA (2h)",
    "SAILING_B_TO_FWY"     : "Returning BIA → Fairway Buoy (2h)",
    "SAILING_FWY_TO_BW"    : "Fairway Buoy → Breakwater (2h)",
    "SAILING_CROSS_BW_IN_AC": "Crossing Breakwater inbound (0.5h, tidal)",
    "SAILING_BW_TO_A"      : "After crossing → Point A/C (1.5h)",
    "WAITING_TIDAL"       : "Waiting — tidal crossing window",
    "WAITING_DAYLIGHT"    : "Waiting — daylight window",
    # ── Sailing SanBarth → Awoba via Cawthorne passage (outbound) ───────────────────────

    # ── At BIA (Mother vessels) ───────────────────────────────────────────
    "WAITING_FAIRWAY"     : "BIA — Holding at fairway buoy",
    "WAITING_BERTH_B"     : "BIA — Waiting for berth at mother",
    "BERTHING_B"          : "BIA — Berthing at mother vessel",
    "HOSE_CONNECT_B"      : "BIA — Hose connection at mother",
    "DISCHARGING"         : "BIA — Discharging to mother",
    "CAST_OFF_B"          : "BIA — Cast off from mother",
    "IDLE_B"              : "BIA — Idle at mother vessel",
    "WAITING_MOTHER_RETURN"   : "BIA — Waiting, mother at export",
    "WAITING_MOTHER_CAPACITY" : "BIA — Waiting, mother full",
    "WAITING_RETURN_STOCK"    : "BIA — Waiting for return assignment",
    # ── Sailing BIA → SanBarth/Sego/Dawes (return via main breakwater) ───────────────────────
    "SAILING_BA"          : "Returning to storage (SanBarth/Sego/Dawes)",
    # ── Sailing Awoba → BIA via Cawthorne passage (inbound) ────────────────────────
    "SAILING_D_CHANNEL"    : "Awoba outbound — Point D → Cawthorne Channel (3h)",
    "SAILING_CH_TO_BW_OUT" : "Awoba outbound — Channel → Breakwater (1h)",
    "SAILING_CROSS_BW_OUT" : "Awoba outbound — Crossing Breakwater (0.5h)",
    "SAILING_B_TO_BW_IN"   : "Awoba return — BIA → clear breakwater (1.5h)",
    "SAILING_CROSS_BW_IN"  : "Awoba return — Crossing Breakwater inbound (0.5h)",
    "SAILING_BW_TO_CH_IN"  : "Awoba return — Breakwater → Cawthorne Channel (1h)",
    "SAILING_CH_TO_D"      : "Awoba return — Cawthorne Channel → Point D (3h)",
}

# Grouped structure for the startup status selector — organised by location
STATUS_GROUPS = [
    ("📍 At SanBarth / Sego / Awoba / Dawes — Storage", [
        ("IDLE_A",           "🟢 Idle at storage"),
        ("WAITING_STOCK",    "⏳ Waiting — low stock"),
        ("WAITING_DEAD_STOCK","⏳ Waiting — dead-stock threshold"),
        ("WAITING_BERTH_A",  "⏳ Waiting for berth"),
        ("BERTHING_A",       "🔗 Berthing at storage"),
        ("HOSE_CONNECT_A",   "🔧 Hose connection"),
        ("LOADING",          "⛽ Loading cargo"),
        ("DOCUMENTING",      "📄 Documentation"),
        ("WAITING_CAST_OFF", "⏳ Waiting — cast-off window"),
        ("CAST_OFF",         "↩️ Cast off from storage"),
    ]),
    ("⚓ Ibom — Offshore Loading", [
        ("PF_LOADING",       "⛽ Loading at offshore buoy"),
        ("PF_SWAP",          "🔁 Vessel swap in progress"),
    ]),
    ("🚢 Sailing Point A/C → BIA (outbound via breakwater)", [
        ("SAILING_AB",          "🚢 Point A/C → Breakwater (1.5h)"),
        ("SAILING_CROSS_BW_AC", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("SAILING_BW_TO_FWY",   "🚢 Breakwater → Fairway Buoy (2h)"),
        ("SAILING_AB_LEG2",     "🚢 Fairway Buoy → BIA (2h)"),
        ("WAITING_TIDAL",       "🌊 Waiting — tidal crossing window"),
        ("WAITING_DAYLIGHT",    "🌙 Waiting — daylight window"),
        ("WAITING_FAIRWAY",     "⚓ Holding at Fairway Buoy"),
    ]),
    ("🌊 Sailing Awoba (D) → BIA via Cawthorne (outbound)", [
        ("SAILING_D_CHANNEL",    "🚢 Point D → Cawthorne Channel (3h, tidal)"),
        ("SAILING_CH_TO_BW_OUT", "🚢 Channel → Breakwater (1h, tidal)"),
        ("SAILING_CROSS_BW_OUT", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]),
    ("🛢️ At BIA — Mother Vessels", [
        ("WAITING_FAIRWAY",      "⚓ Holding at fairway buoy"),
        ("WAITING_BERTH_B",      "⏳ Waiting for berth"),
        ("BERTHING_B",           "🔗 Berthing at mother"),
        ("HOSE_CONNECT_B",       "🔧 Hose connection at mother"),
        ("DISCHARGING",          "⬇️ Discharging to mother"),
        ("CAST_OFF_B",           "↩️ Cast off from mother"),
        ("IDLE_B",               "🟢 Idle at mother vessel"),
        ("WAITING_MOTHER_RETURN","⏳ Waiting — mother at export"),
        ("WAITING_MOTHER_CAPACITY","⏳ Waiting — mother full"),
        ("WAITING_RETURN_STOCK", "⏳ Waiting — return assignment"),
    ]),
    ("🔄 Returning BIA → Point A/C (via breakwater)", [
        ("SAILING_B_TO_FWY",       "🔄 BIA → Fairway Buoy (2h)"),
        ("SAILING_FWY_TO_BW",      "🔄 Fairway Buoy → Breakwater (2h)"),
        ("SAILING_CROSS_BW_IN_AC", "🔄 Crossing Breakwater inbound (0.5h, tidal)"),
        ("SAILING_BW_TO_A",        "🔄 Breakwater → Point A/C (1.5h)"),
        ("SAILING_BA",             "🔄 Returning to Starturn/Dawes (direct)"),
        ("WAITING_TIDAL",          "🌊 Waiting — tidal crossing window"),
        ("WAITING_DAYLIGHT",       "🌙 Waiting — daylight window"),
        ("WAITING_FAIRWAY",        "⚓ Holding at Fairway Buoy (return)"),
    ]),
    ("🌊 Returning BIA → Awoba (D) via Cawthorne (inbound)", [
        ("SAILING_B_TO_BW_IN",   "🚢 BIA → clear breakwater (1.5h)"),
        ("SAILING_CROSS_BW_IN",  "🚢 Crossing Breakwater inbound (0.5h, tidal)"),
        ("SAILING_BW_TO_CH_IN",  "🚢 Breakwater → Cawthorne Channel (1h, tidal)"),
        ("SAILING_CH_TO_D",      "🚢 Channel → Point D (3h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]),
]
# ── Location catalogue with status filtering ──────────────────────────────────
# Each location carries:
#   display     – shown in the dropdown
#   sim_value   – what the sim/log uses as the "location" string
#   field_zone  – which storage/area zone this belongs to
#   statuses    – ordered list of (status_code, label) valid at that location
# Groups are ordered: most common first, so default index lands on IDLE_A.

LOCATION_CATALOGUE = [
    # ── SanBarth storage berths ────────────────────────────────────────────────
    # Statuses follow the full lifecycle order: arrive → berth → hose → load → docs → cast-off
    {"display": "Chapel (SanBarth)",    "sim_value": "Chapel",
     "field_zone": "SanBarth",
     "statuses": [
        ("LOADING",            "⛽ Loading — in progress"),
        ("DOCUMENTING",        "📄 Documentation in progress"),
        ("WAITING_CAST_OFF",   "⏳ Loading complete — awaiting cast-off window"),
        ("CAST_OFF",           "↩️ Cast off (departing storage)"),
        ("IDLE_A",             "🟢 Idle at berth — ready to load"),
        ("HOSE_CONNECT_A",     "🔧 Hose connection underway"),
        ("BERTHING_A",         "🔗 Berthing in progress"),
        ("WAITING_BERTH_A",    "⏳ Arrived — waiting for berth slot"),
        ("WAITING_STOCK",      "⏳ At berth — waiting for stock to build"),
        ("WAITING_DEAD_STOCK", "⏳ At berth — stock below dead-stock threshold"),
    ]},
    {"display": "JasmineS (SanBarth)", "sim_value": "JasmineS",
     "field_zone": "SanBarth",
     "statuses": [
        ("LOADING",            "⛽ Loading — in progress"),
        ("DOCUMENTING",        "📄 Documentation in progress"),
        ("WAITING_CAST_OFF",   "⏳ Loading complete — awaiting cast-off window"),
        ("CAST_OFF",           "↩️ Cast off (departing storage)"),
        ("IDLE_A",             "🟢 Idle at berth — ready to load"),
        ("HOSE_CONNECT_A",     "🔧 Hose connection underway"),
        ("BERTHING_A",         "🔗 Berthing in progress"),
        ("WAITING_BERTH_A",    "⏳ Arrived — waiting for berth slot"),
        ("WAITING_STOCK",      "⏳ At berth — waiting for stock to build"),
        ("WAITING_DEAD_STOCK", "⏳ At berth — stock below dead-stock threshold"),
    ]},
    # ── Sego (Westmore) ────────────────────────────────────────────────────────
    {"display": "Westmore (Sego)",     "sim_value": "Westmore",
     "field_zone": "Sego",
     "statuses": [
        ("LOADING",            "⛽ Loading — in progress"),
        ("DOCUMENTING",        "📄 Documentation in progress"),
        ("WAITING_CAST_OFF",   "⏳ Loading complete — awaiting cast-off window"),
        ("CAST_OFF",           "↩️ Cast off (departing storage)"),
        ("IDLE_A",             "🟢 Idle at berth — ready to load"),
        ("HOSE_CONNECT_A",     "🔧 Hose connection underway"),
        ("BERTHING_A",         "🔗 Berthing in progress"),
        ("WAITING_BERTH_A",    "⏳ Arrived — waiting for berth slot"),
        ("WAITING_STOCK",      "⏳ At berth — waiting for stock to build"),
        ("WAITING_DEAD_STOCK", "⏳ At berth — stock below dead-stock threshold"),
    ]},
    # ── Awoba (Duke) — via Cawthorne ──────────────────────────────────────────
    {"display": "Duke (Awoba)",        "sim_value": "Duke",
     "field_zone": "Awoba",
     "statuses": [
        ("LOADING",            "⛽ Loading — in progress"),
        ("DOCUMENTING",        "📄 Documentation in progress"),
        ("WAITING_CAST_OFF",   "⏳ Loading complete — awaiting cast-off window"),
        ("CAST_OFF",           "↩️ Cast off (departing storage)"),
        ("IDLE_A",             "🟢 Idle at berth — ready to load"),
        ("HOSE_CONNECT_A",     "🔧 Hose connection underway"),
        ("BERTHING_A",         "🔗 Berthing in progress"),
        ("WAITING_BERTH_A",    "⏳ Arrived — waiting for berth slot"),
        ("WAITING_STOCK",      "⏳ At berth — waiting for stock to build"),
        ("WAITING_DEAD_STOCK", "⏳ At berth — stock below dead-stock threshold"),
    ]},
    # ── Dawes (Starturn) ──────────────────────────────────────────────────────
    {"display": "Starturn (Dawes)",    "sim_value": "Starturn",
     "field_zone": "Dawes",
     "statuses": [
        ("LOADING",            "⛽ Loading — in progress"),
        ("DOCUMENTING",        "📄 Documentation in progress"),
        ("WAITING_CAST_OFF",   "⏳ Loading complete — awaiting cast-off window"),
        ("CAST_OFF",           "↩️ Cast off (departing storage)"),
        ("IDLE_A",             "🟢 Idle at berth — ready to load"),
        ("HOSE_CONNECT_A",     "🔧 Hose connection underway"),
        ("BERTHING_A",         "🔗 Berthing in progress"),
        ("WAITING_BERTH_A",    "⏳ Arrived — waiting for berth slot"),
        ("WAITING_STOCK",      "⏳ At berth — waiting for stock to build"),
        ("WAITING_DEAD_STOCK", "⏳ At berth — stock below dead-stock threshold"),
    ]},
    # ── Ibom offshore ─────────────────────────────────────────────────────────
    {"display": "Ibom (Offshore Buoy)","sim_value": "Ibom",
     "field_zone": "Ibom",
     "statuses": [
        ("PF_LOADING",         "⛽ Loading at offshore buoy — in progress"),
        ("PF_SWAP",            "🔁 Vessel swap / handover in progress"),
        ("IDLE_A",             "🟢 Idle / standby at buoy"),
        ("WAITING_DAYLIGHT",   "🌙 Waiting — daylight window"),
        ("WAITING_TIDAL",      "🌊 Waiting — tidal window"),
    ]},
    # ── En route → BIA — Leg 1: storage → Breakwater ─────────────────────────
    {"display": "Sailing → Bryanston (A/C outbound)", "sim_value": "En Route SanBarth→BIA",
     "field_zone": "Transit", "target_mother": "Bryanston", "target_storage": None,
     "statuses": [
        ("SAILING_AB",          "🚢 Leg 1: Point A/C → Breakwater (1.5h)"),
        ("SAILING_CROSS_BW_AC", "🚢 Leg 2: Crossing Breakwater outbound (0.5h)"),
        ("SAILING_BW_TO_FWY",   "🚢 Leg 3: Breakwater → Fairway Buoy (2h)"),
        ("WAITING_TIDAL",       "🌊 Holding — waiting for tidal window"),
        ("WAITING_DAYLIGHT",    "🌙 Holding — waiting for daylight window"),
        ("WAITING_RETURN_STOCK","⏳ Holding — return destination assignment"),
    ]},
    {"display": "Sailing → Alkebulan (A/C outbound)", "sim_value": "En Route SanBarth→BIA",
     "field_zone": "Transit", "target_mother": "Alkebulan", "target_storage": None,
     "statuses": [
        ("SAILING_AB",          "🚢 Leg 1: Point A/C → Breakwater (1.5h)"),
        ("SAILING_CROSS_BW_AC", "🚢 Leg 2: Crossing Breakwater outbound (0.5h)"),
        ("SAILING_BW_TO_FWY",   "🚢 Leg 3: Breakwater → Fairway Buoy (2h)"),
        ("WAITING_TIDAL",       "🌊 Holding — waiting for tidal window"),
        ("WAITING_DAYLIGHT",    "🌙 Holding — waiting for daylight window"),
        ("WAITING_RETURN_STOCK","⏳ Holding — return destination assignment"),
    ]},
    {"display": "Sailing → GreenEagle (A/C outbound)", "sim_value": "En Route SanBarth→BIA",
     "field_zone": "Transit", "target_mother": "GreenEagle", "target_storage": None,
     "statuses": [
        ("SAILING_AB",          "🚢 Leg 1: Point A/C → Breakwater (1.5h)"),
        ("SAILING_CROSS_BW_AC", "🚢 Leg 2: Crossing Breakwater outbound (0.5h)"),
        ("SAILING_BW_TO_FWY",   "🚢 Leg 3: Breakwater → Fairway Buoy (2h)"),
        ("WAITING_TIDAL",       "🌊 Holding — waiting for tidal window"),
        ("WAITING_DAYLIGHT",    "🌙 Holding — waiting for daylight window"),
        ("WAITING_RETURN_STOCK","⏳ Holding — return destination assignment"),
    ]},
    # ── En route → BIA — Leg 2: at or near Fairway Buoy ──────────────────────
    {"display": "Approaching Bryanston (Fairway Buoy)", "sim_value": "Fairway Buoy",
     "field_zone": "Transit", "target_mother": "Bryanston", "target_storage": None,
     "statuses": [
        ("SAILING_BW_TO_FWY",   "🚢 Breakwater → Fairway Buoy (2h)"),
        ("SAILING_AB_LEG2",     "🚢 Fairway Buoy → BIA (2h)"),
        ("WAITING_FAIRWAY",     "⚓ Arrived after 19:00 — holding at Fairway Buoy overnight"),
        ("WAITING_BERTH_B",     "⏳ Arrived fairway — waiting for mother berth"),
        ("WAITING_MOTHER_RETURN","⏳ Waiting — mother vessel away at export"),
        ("WAITING_MOTHER_CAPACITY","⏳ Waiting — mother vessel full"),
        ("WAITING_DAYLIGHT",    "🌙 Holding — waiting for daylight window"),
    ]},
    {"display": "Approaching Alkebulan (Fairway Buoy)", "sim_value": "Fairway Buoy",
     "field_zone": "Transit", "target_mother": "Alkebulan", "target_storage": None,
     "statuses": [
        ("SAILING_BW_TO_FWY",   "🚢 Breakwater → Fairway Buoy (2h)"),
        ("SAILING_AB_LEG2",     "🚢 Fairway Buoy → BIA (2h)"),
        ("WAITING_FAIRWAY",     "⚓ Arrived after 19:00 — holding at Fairway Buoy overnight"),
        ("WAITING_BERTH_B",     "⏳ Arrived fairway — waiting for mother berth"),
        ("WAITING_MOTHER_RETURN","⏳ Waiting — mother vessel away at export"),
        ("WAITING_MOTHER_CAPACITY","⏳ Waiting — mother vessel full"),
        ("WAITING_DAYLIGHT",    "🌙 Holding — waiting for daylight window"),
    ]},
    {"display": "Approaching GreenEagle (Fairway Buoy)", "sim_value": "Fairway Buoy",
     "field_zone": "Transit", "target_mother": "GreenEagle", "target_storage": None,
     "statuses": [
        ("SAILING_BW_TO_FWY",   "🚢 Breakwater → Fairway Buoy (2h)"),
        ("SAILING_AB_LEG2",     "🚢 Fairway Buoy → BIA (2h)"),
        ("WAITING_FAIRWAY",     "⚓ Arrived after 19:00 — holding at Fairway Buoy overnight"),
        ("WAITING_BERTH_B",     "⏳ Arrived fairway — waiting for mother berth"),
        ("WAITING_MOTHER_RETURN","⏳ Waiting — mother vessel away at export"),
        ("WAITING_MOTHER_CAPACITY","⏳ Waiting — mother vessel full"),
        ("WAITING_DAYLIGHT",    "🌙 Holding — waiting for daylight window"),
    ]},
    # ── Cawthorne outbound (SanBarth → Awoba) — target_mother embedded ────────
    {"display": "Breakwater outbound → Bryanston", "sim_value": "Breakwater (outbound)",
     "field_zone": "Transit", "target_mother": "Bryanston", "target_storage": None,
     "statuses": [
        ("SAILING_CH_TO_BW_OUT", "🚢 Channel → Breakwater (1h, tidal)"),
        ("SAILING_CROSS_BW_OUT", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]},
    {"display": "Breakwater outbound → Alkebulan", "sim_value": "Breakwater (outbound)",
     "field_zone": "Transit", "target_mother": "Alkebulan", "target_storage": None,
     "statuses": [
        ("SAILING_CH_TO_BW_OUT", "🚢 Channel → Breakwater (1h, tidal)"),
        ("SAILING_CROSS_BW_OUT", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]},
    {"display": "Breakwater outbound → GreenEagle", "sim_value": "Breakwater (outbound)",
     "field_zone": "Transit", "target_mother": "GreenEagle", "target_storage": None,
     "statuses": [
        ("SAILING_CH_TO_BW_OUT", "🚢 Channel → Breakwater (1h, tidal)"),
        ("SAILING_CROSS_BW_OUT", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]},
    {"display": "Cawthorne Channel outbound → Bryanston", "sim_value": "Cawthorne Channel (outbound)",
     "field_zone": "Transit", "target_mother": "Bryanston", "target_storage": None,
     "statuses": [
        ("SAILING_D_CHANNEL",    "🚢 Point D → Cawthorne Channel (3h, tidal)"),
        ("SAILING_CH_TO_BW_OUT", "🚢 Channel → Breakwater (1h, tidal)"),
        ("SAILING_CROSS_BW_OUT", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]},
    {"display": "Cawthorne Channel outbound → Alkebulan", "sim_value": "Cawthorne Channel (outbound)",
     "field_zone": "Transit", "target_mother": "Alkebulan", "target_storage": None,
     "statuses": [
        ("SAILING_D_CHANNEL",    "🚢 Point D → Cawthorne Channel (3h, tidal)"),
        ("SAILING_CH_TO_BW_OUT", "🚢 Channel → Breakwater (1h, tidal)"),
        ("SAILING_CROSS_BW_OUT", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]},
    {"display": "Cawthorne Channel outbound → GreenEagle", "sim_value": "Cawthorne Channel (outbound)",
     "field_zone": "Transit", "target_mother": "GreenEagle", "target_storage": None,
     "statuses": [
        ("SAILING_D_CHANNEL",    "🚢 Point D → Cawthorne Channel (3h, tidal)"),
        ("SAILING_CH_TO_BW_OUT", "🚢 Channel → Breakwater (1h, tidal)"),
        ("SAILING_CROSS_BW_OUT", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]},
    # ── At BIA ────────────────────────────────────────────────────────────────
    {"display": "BIA — Fairway Buoy",  "sim_value": "Fairway",
     "field_zone": "BIA",
     "statuses": [
        ("SAILING_AB_LEG2",        "🚢 Inbound — Fairway Buoy → BIA (2h)"),
        ("WAITING_FAIRWAY",        "⚓ Arrived after 19:00 — holding overnight at Fairway Buoy"),
        ("WAITING_BERTH_B",        "⏳ Arrived fairway — waiting for mother berth"),
        ("WAITING_MOTHER_RETURN",  "⏳ Waiting — mother vessel away at export"),
        ("WAITING_MOTHER_CAPACITY","⏳ Waiting — mother vessel full"),
        ("WAITING_RETURN_STOCK",   "⏳ Waiting — return destination assignment"),
        ("WAITING_DAYLIGHT",       "🌙 Waiting — daylight window"),
        ("IDLE_B",                 "🟢 Idle at BIA — no berth assigned yet"),
    ]},
    {"display": "Bryanston (BIA)",     "sim_value": "Bryanston",
     "field_zone": "BIA",
     "statuses": [
        ("DISCHARGING",            "⬇️ Discharge — in progress"),
        ("HOSE_CONNECT_B",         "🔧 Hose connection underway"),
        ("BERTHING_B",             "🔗 Berthing in progress"),
        ("WAITING_BERTH_B",        "⏳ Arrived — waiting for berth slot"),
        ("WAITING_MOTHER_CAPACITY","⏳ Berthed — waiting for mother capacity"),
        ("CAST_OFF_B",             "↩️ Discharge complete — cast off from mother"),
        ("IDLE_B",                 "🟢 Idle at mother — discharge complete"),
        ("WAITING_CAST_OFF",       "⏳ Discharge complete — awaiting cast-off window"),
        ("WAITING_MOTHER_RETURN",  "⏳ Waiting — mother vessel away at export"),
    ]},
    {"display": "Alkebulan (BIA)",     "sim_value": "Alkebulan",
     "field_zone": "BIA",
     "statuses": [
        ("DISCHARGING",            "⬇️ Discharge — in progress"),
        ("HOSE_CONNECT_B",         "🔧 Hose connection underway"),
        ("BERTHING_B",             "🔗 Berthing in progress"),
        ("WAITING_BERTH_B",        "⏳ Arrived — waiting for berth slot"),
        ("WAITING_MOTHER_CAPACITY","⏳ Berthed — waiting for mother capacity"),
        ("CAST_OFF_B",             "↩️ Discharge complete — cast off from mother"),
        ("IDLE_B",                 "🟢 Idle at mother — discharge complete"),
        ("WAITING_CAST_OFF",       "⏳ Discharge complete — awaiting cast-off window"),
        ("WAITING_MOTHER_RETURN",  "⏳ Waiting — mother vessel away at export"),
    ]},
    {"display": "GreenEagle (BIA)",    "sim_value": "GreenEagle",
     "field_zone": "BIA",
     "statuses": [
        ("DISCHARGING",            "⬇️ Discharge — in progress"),
        ("HOSE_CONNECT_B",         "🔧 Hose connection underway"),
        ("BERTHING_B",             "🔗 Berthing in progress"),
        ("WAITING_BERTH_B",        "⏳ Arrived — waiting for berth slot"),
        ("WAITING_MOTHER_CAPACITY","⏳ Berthed — waiting for mother capacity"),
        ("CAST_OFF_B",             "↩️ Discharge complete — cast off from mother"),
        ("IDLE_B",                 "🟢 Idle at mother — discharge complete"),
        ("WAITING_CAST_OFF",       "⏳ Discharge complete — awaiting cast-off window"),
        ("WAITING_MOTHER_RETURN",  "⏳ Waiting — mother vessel away at export"),
    ]},
    # ── Returning from BIA — one entry per storage destination ──────────────
    # Chapel and JasmineS are both point A: sim picks between them by stock level
    {"display": "Returning → SanBarth (Chapel/JasmineS)", "sim_value": "En Route BIA→Storage",
     "field_zone": "Transit", "target_storage": "Chapel", "target_mother": None,
     "statuses": [
        ("SAILING_B_TO_FWY",       "🔄 Leg 1: BIA → Fairway Buoy (2h)"),
        ("SAILING_FWY_TO_BW",      "🔄 Leg 2: Fairway Buoy → Breakwater (2h)"),
        ("SAILING_CROSS_BW_IN_AC", "🔄 Leg 3: Crossing Breakwater inbound (0.5h)"),
        ("SAILING_BW_TO_A",        "🔄 Leg 4: Breakwater → SanBarth (1.5h)"),
        ("WAITING_TIDAL",          "🌊 Holding — waiting for tidal window"),
        ("WAITING_DAYLIGHT",       "🌙 Holding — waiting for daylight window"),
        ("WAITING_RETURN_STOCK",   "⏳ Holding — return destination assignment"),
    ]},
    {"display": "Returning → Westmore (Sego)", "sim_value": "En Route BIA→Storage",
     "field_zone": "Transit", "target_storage": "Westmore", "target_mother": None,
     "statuses": [
        ("SAILING_B_TO_FWY",       "🔄 Leg 1: BIA → Fairway Buoy (2h)"),
        ("SAILING_FWY_TO_BW",      "🔄 Leg 2: Fairway Buoy → Breakwater (2h)"),
        ("SAILING_CROSS_BW_IN_AC", "🔄 Leg 3: Crossing Breakwater inbound (0.5h)"),
        ("SAILING_BW_TO_A",        "🔄 Leg 4: Breakwater → Westmore (1.5h)"),
        ("WAITING_TIDAL",          "🌊 Holding — waiting for tidal window"),
        ("WAITING_DAYLIGHT",       "🌙 Holding — waiting for daylight window"),
        ("WAITING_RETURN_STOCK",   "⏳ Holding — return destination assignment"),
    ]},
    {"display": "Returning → Starturn (Dawes)", "sim_value": "En Route BIA→Storage",
     "field_zone": "Transit", "target_storage": "Starturn", "target_mother": None,
     "statuses": [
        ("SAILING_B_TO_FWY",       "🔄 Leg 1: BIA → Fairway Buoy (2h)"),
        ("SAILING_FWY_TO_BW",      "🔄 Leg 2: Fairway Buoy → Breakwater (2h)"),
        ("SAILING_CROSS_BW_IN_AC", "🔄 Leg 3: Crossing Breakwater inbound (0.5h)"),
        ("SAILING_BA",             "🔄 Leg 4: Inbound → Starturn (Dawes)"),
        ("WAITING_TIDAL",          "🌊 Holding — waiting for tidal window"),
        ("WAITING_DAYLIGHT",       "🌙 Holding — waiting for daylight window"),
        ("WAITING_RETURN_STOCK",   "⏳ Holding — return destination assignment"),
    ]},
    # ── Cawthorne inbound (Awoba/Duke → BIA) — target_mother embedded ─────────
    {"display": "Cawthorne Channel → BIA via Bryanston", "sim_value": "Cawthorne Channel (outbound)",
     "field_zone": "Transit", "target_storage": "Duke", "target_mother": "Bryanston",
     "statuses": [
        ("SAILING_D_CHANNEL",    "🚢 Point D → Cawthorne Channel (3h, tidal)"),
        ("SAILING_CH_TO_BW_OUT", "🚢 Channel → Breakwater (1h, tidal)"),
        ("SAILING_CROSS_BW_OUT", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]},
    {"display": "Cawthorne Channel → BIA via Alkebulan", "sim_value": "Cawthorne Channel (outbound)",
     "field_zone": "Transit", "target_storage": "Duke", "target_mother": "Alkebulan",
     "statuses": [
        ("SAILING_D_CHANNEL",    "🚢 Point D → Cawthorne Channel (3h, tidal)"),
        ("SAILING_CH_TO_BW_OUT", "🚢 Channel → Breakwater (1h, tidal)"),
        ("SAILING_CROSS_BW_OUT", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]},
    {"display": "Cawthorne Channel → BIA via GreenEagle", "sim_value": "Cawthorne Channel (outbound)",
     "field_zone": "Transit", "target_storage": "Duke", "target_mother": "GreenEagle",
     "statuses": [
        ("SAILING_D_CHANNEL",    "🚢 Point D → Cawthorne Channel (3h, tidal)"),
        ("SAILING_CH_TO_BW_OUT", "🚢 Channel → Breakwater (1h, tidal)"),
        ("SAILING_CROSS_BW_OUT", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]},
    {"display": "Returning Duke from BIA via Bryanston", "sim_value": "En Route BIA→Storage",
     "field_zone": "Transit", "target_storage": "Duke", "target_mother": "Bryanston",
     "statuses": [
        ("SAILING_B_TO_BW_IN",  "🚢 BIA → clear breakwater (1.5h)"),
        ("SAILING_CROSS_BW_IN", "🚢 Crossing Breakwater inbound (0.5h, tidal)"),
        ("SAILING_BW_TO_CH_IN", "🚢 Breakwater → Cawthorne Channel (1h, tidal)"),
        ("SAILING_CH_TO_D",     "🚢 Channel → Point D (3h, tidal)"),
        ("WAITING_TIDAL",       "🌊 Waiting — tidal/daylight window"),
    ]},
    {"display": "Returning Duke from BIA via Alkebulan", "sim_value": "En Route BIA→Storage",
     "field_zone": "Transit", "target_storage": "Duke", "target_mother": "Alkebulan",
     "statuses": [
        ("SAILING_B_TO_BW_IN",  "🚢 BIA → clear breakwater (1.5h)"),
        ("SAILING_CROSS_BW_IN", "🚢 Crossing Breakwater inbound (0.5h, tidal)"),
        ("SAILING_BW_TO_CH_IN", "🚢 Breakwater → Cawthorne Channel (1h, tidal)"),
        ("SAILING_CH_TO_D",     "🚢 Channel → Point D (3h, tidal)"),
        ("WAITING_TIDAL",       "🌊 Waiting — tidal/daylight window"),
    ]},
    {"display": "Returning Duke from BIA via GreenEagle", "sim_value": "En Route BIA→Storage",
     "field_zone": "Transit", "target_storage": "Duke", "target_mother": "GreenEagle",
     "statuses": [
        ("SAILING_B_TO_BW_IN",  "🚢 BIA → clear breakwater (1.5h)"),
        ("SAILING_CROSS_BW_IN", "🚢 Crossing Breakwater inbound (0.5h, tidal)"),
        ("SAILING_BW_TO_CH_IN", "🚢 Breakwater → Cawthorne Channel (1h, tidal)"),
        ("SAILING_CH_TO_D",     "🚢 Channel → Point D (3h, tidal)"),
        ("WAITING_TIDAL",       "🌊 Waiting — tidal/daylight window"),
    ]},
]

# Pre-built lookups derived from catalogue
LOC_DISPLAY_LIST  = [e["display"]    for e in LOCATION_CATALOGUE]
LOC_BY_DISPLAY    = {e["display"]: e for e in LOCATION_CATALOGUE}

# Vessels restricted to SanBarth and Sego only (Watson)
SANBARTH_LOC_DISPLAYS = [e["display"] for e in LOCATION_CATALOGUE
                          if e["field_zone"] in ("SanBarth", "BIA", "Transit")]

# Zone badges for display
ZONE_BADGE = {
    "SanBarth": ("🟡", "#f1c40f"),
    "Sego":     ("🟢", "#2ecc71"),
    "Awoba":    ("🔵", "#5dade2"),
    "Dawes":    ("🟠", "#f07030"),
    "Ibom":     ("🟣", "#bf7fff"),
    "BIA":      ("🔴", "#ff6b6b"),
    "Transit":  ("⚪", "#94a3b8"),
}

STATUS_ICONS = {
    "LOADING":"⛽","PF_LOADING":"⛽","DISCHARGING":"⬇️",
    "SAILING_AB":"🚢","SAILING_CROSS_BW_AC":"🚢","SAILING_BW_TO_FWY":"🚢","SAILING_AB_LEG2":"🚢","SAILING_BA":"🔄","SAILING_B_TO_FWY":"🔄","SAILING_FWY_TO_BW":"🔄","SAILING_CROSS_BW_IN_AC":"🔄","SAILING_BW_TO_A":"🔄",
    "SAILING_D_CHANNEL":"🚢","SAILING_CH_TO_BW_OUT":"🚢","SAILING_CROSS_BW_OUT":"🚢",
    "SAILING_B_TO_BW_IN":"🚢","SAILING_CROSS_BW_IN":"🚢","SAILING_BW_TO_CH_IN":"🚢","SAILING_CH_TO_D":"🚢",
    "WAITING_FAIRWAY":"⚓","WAITING_BERTH_B":"⏳","WAITING_BERTH_A":"⏳",
    "BERTHING_A":"🔗","BERTHING_B":"🔗","HOSE_CONNECT_A":"🔧","HOSE_CONNECT_B":"🔧",
    "IDLE_A":"🟢","IDLE_B":"🟡","CAST_OFF":"↩️","CAST_OFF_B":"↩️","DOCUMENTING":"📄",
    "WAITING_CAST_OFF":"⏳","WAITING_DEAD_STOCK":"⏳","WAITING_RETURN_STOCK":"⏳",
    "PF_SWAP":"🔁","WAITING_DAYLIGHT":"🌙","WAITING_TIDAL":"🌊","WAITING_STOCK":"⏳",
    "WAITING_MOTHER_RETURN":"⏳","WAITING_MOTHER_CAPACITY":"⏳",
}


def _shade(hex_color, factor):
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i:i+2], 16)/255 for i in (0, 2, 4))
    hh, l, s = colorsys.rgb_to_hls(r, g, b)
    l2 = max(0.0, min(1.0, l * factor))
    r2, g2, b2 = colorsys.hls_to_rgb(hh, l2, s)
    return "#{:02x}{:02x}{:02x}".format(int(r2*255), int(g2*255), int(b2*255))


def _hex_to_rgba(hex_color, alpha=0.13):
    """Convert '#rrggbb' → 'rgba(r,g,b,alpha)' — compatible with all Plotly versions."""
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
    return f"rgba({r},{g},{b},{alpha})"


def vcolor(name, status):
    return _shade(VESSEL_COLORS.get(name, "#95a5a6"), STATUS_LIGHTNESS.get(status, 1.0))


# =============================================================================
# ── SIMULATION ENGINE LOADER ──────────────────────────────────────────────────
# =============================================================================

@st.cache_resource(show_spinner="Loading simulation engine…")
def _load_mod(_file_hash: str = ""):
    """
    _file_hash is derived from the sim file content so Streamlit Cloud
    automatically busts the cache whenever the file is updated on deploy.
    """
    sim_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "tanker_simulation_v5.py")
    if not os.path.exists(sim_path):
        st.error("❌ tanker_simulation_v5.py not found next to tanker_app.py")
        st.stop()
    source = open(sim_path).read()
    marker = "# -----------------------------------------------------------------\n# RUN SIMULATION"
    if marker in source:
        source = source.split(marker)[0]
    for m in ["matplotlib", "matplotlib.pyplot", "matplotlib.patches"]:
        if m not in sys.modules:
            sys.modules[m] = _mock.MagicMock()
    mod = types.ModuleType("tanker_sim_v5")
    mod.__file__ = sim_path
    exec(compile(source, sim_path, "exec"), mod.__dict__)
    return mod


def _load_mod_current():
    """Return the sim module keyed on current file hash (busts stale cache)."""
    sim_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "tanker_simulation_v5.py")
    try:
        file_hash = hashlib.md5(open(sim_path, "rb").read()).hexdigest()
    except Exception:
        file_hash = "v6"
    return _load_mod(file_hash)


@st.cache_data(ttl=0, show_spinner="Running simulation…")
def run_sim(sim_days, chapel, jasmines, westmore, duke, starturn,
            bryanston, alkebulan, greeneagle,
            bryanston_api: float = 0.0,
            alkebulan_api: float = 0.0,
            greeneagle_api: float = 0.0,
            prod_chapel=0, prod_jasmines=0, prod_westmore=0,
            prod_duke=0, prod_starturn=0, prod_ibom=0,
            production_overrides_json: str = None,
            vessel_states_json=None,
            tide_csv_bytes: bytes = None,
            sim_start_date: str = None,
            _sim_version: str = "",
            opt_params_json: str = None,
            startup_day_disable_point_b_priority: bool = True,
            startup_day_manual_nominations_json: str = None,
            point_b_startup_seed_json: str = None,
            mother_export_seed_json: str = None):
    """
    Run simulation with independent production rates per storage and Ibom.
    vessel_states_json: JSON str of {vessel: {status, cargo_bbl}} or None.
    tide_csv_bytes: raw CSV bytes for tidal constraint (or None to disable).
    sim_start_date: ISO date string (YYYY-MM-DD) — day 0 of the simulation (defaults to today).
    opt_params_json: JSON str of optimizer params to apply (dead_stock_factor,
        ibom_trigger_bbl, export_sail_window_start, berthing_start, berthing_end).
        When provided, these override the sim module constants for this run only.
    production_overrides_json: JSON list of date-window production overrides.
        Each item: {start_date, end_date, rates:{storage_name:bph}}.
    startup_day_disable_point_b_priority: disable Point B auto-priority on Day 1 only.
    startup_day_manual_nominations_json: JSON str of vessel->mother nominations
        used on Day 1 when startup_day_disable_point_b_priority is enabled.
    point_b_startup_seed_json: JSON str of vessel->mother seed map to force
        selected vessels to start fully loaded at Point B (validation mode).
    mother_export_seed_json: JSON str of {mother: days} — mothers that start at
        export for the given number of days, blocking daughter berthing until return.
    """
    mod = _load_mod_current()

    # ── Set simulation epoch ────────────────────────────────────────────
    if sim_start_date:
        _epoch = _dt.date.fromisoformat(sim_start_date)
    else:
        _epoch = _dt.date.today()
    if hasattr(mod, "set_sim_epoch"):
        mod.set_sim_epoch(_epoch)

    # Load tide table if provided
    if tide_csv_bytes is not None:
        import tempfile as _tmpf, os as _os
        with _tmpf.NamedTemporaryFile(delete=False, suffix=".csv") as _tf:
            _tf.write(tide_csv_bytes)
            _tp = _tf.name
        try:
            mod.load_tide_table(_tp)
        except Exception:
            pass
        finally:
            try: _os.unlink(_tp)
            except: pass
    elif hasattr(mod, "_TIDE_TABLE"):
        mod._TIDE_TABLE = None
    _save_attrs = ["SIMULATION_DAYS","STORAGE_INIT_BBL","MOTHER_INIT_BBL",
                   "PRODUCTION_RATE_BPH","WESTMORE_PRODUCTION_RATE_BPH",
                   "DUKE_PRODUCTION_RATE_BPH","STARTURN_PRODUCTION_RATE_BPH"]
    _runtime_keys = [
        "STARTUP_DAY_DISABLE_POINT_B_PRIORITY",
        "STARTUP_DAY_POINT_B_MANUAL_NOMINATIONS",
        "POINT_B_DISTRIBUTION_TEST_MODE",
        "POINT_B_TEST_STARTUP_FULL_LOAD_NOMINATIONS",
        "PRODUCTION_RATE_OVERRIDES",
    ]
    orig = {k: getattr(mod, k) for k in _save_attrs if hasattr(mod, k)}
    for _rk in _runtime_keys:
        if hasattr(mod, _rk):
            orig[_rk] = getattr(mod, _rk)
    orig["_ibom_rate"] = getattr(mod, "IBOM_LOAD_RATE_BPH",
                         getattr(mod, "POINT_F_LOAD_RATE_BPH", 165))
    mod.SIMULATION_DAYS                  = sim_days
    mod.STORAGE_INIT_BBL                 = chapel
    mod.MOTHER_INIT_BBL                  = 0
    mod.PRODUCTION_RATE_BPH              = prod_chapel
    mod.WESTMORE_PRODUCTION_RATE_BPH     = prod_westmore
    mod.DUKE_PRODUCTION_RATE_BPH         = prod_duke
    mod.STARTURN_PRODUCTION_RATE_BPH     = prod_starturn
    if hasattr(mod, "IBOM_LOAD_RATE_BPH"):    mod.IBOM_LOAD_RATE_BPH    = prod_ibom
    if hasattr(mod, "POINT_F_LOAD_RATE_BPH"): mod.POINT_F_LOAD_RATE_BPH = prod_ibom

    # Optional custom production windows (date range specific rates)
    _prod_overrides = []
    if production_overrides_json:
        try:
            _raw_overrides = json.loads(production_overrides_json)
        except Exception:
            _raw_overrides = []
        if isinstance(_raw_overrides, list):
            _prod_overrides = _raw_overrides
    if hasattr(mod, "PRODUCTION_RATE_OVERRIDES"):
        mod.PRODUCTION_RATE_OVERRIDES = _prod_overrides

    # ── Runtime control: Day-1 Point B manual nomination exception ────────────
    _mother_names = set(getattr(mod, "MOTHER_NAMES", []))
    _vessel_names = set(getattr(mod, "VESSEL_NAMES", []))
    _manual_nom = {}
    if startup_day_manual_nominations_json:
        try:
            _manual_nom_raw = json.loads(startup_day_manual_nominations_json)
        except Exception:
            _manual_nom_raw = {}
        if isinstance(_manual_nom_raw, dict):
            for _vn, _mn in _manual_nom_raw.items():
                if _vn in _vessel_names and _mn in _mother_names:
                    _manual_nom[_vn] = _mn
    if hasattr(mod, "STARTUP_DAY_DISABLE_POINT_B_PRIORITY"):
        mod.STARTUP_DAY_DISABLE_POINT_B_PRIORITY = bool(startup_day_disable_point_b_priority)
    if hasattr(mod, "STARTUP_DAY_POINT_B_MANUAL_NOMINATIONS"):
        mod.STARTUP_DAY_POINT_B_MANUAL_NOMINATIONS = dict(_manual_nom)

    # ── Optional startup seed for targeted Point B validation ─────────────────
    _seed_nom = {}
    if point_b_startup_seed_json:
        try:
            _seed_nom_raw = json.loads(point_b_startup_seed_json)
        except Exception:
            _seed_nom_raw = {}
        if isinstance(_seed_nom_raw, dict):
            for _vn, _mn in _seed_nom_raw.items():
                if _vn in _vessel_names and _mn in _mother_names:
                    _seed_nom[_vn] = _mn
    if hasattr(mod, "POINT_B_TEST_STARTUP_FULL_LOAD_NOMINATIONS"):
        mod.POINT_B_TEST_STARTUP_FULL_LOAD_NOMINATIONS = dict(_seed_nom)
    if hasattr(mod, "POINT_B_DISTRIBUTION_TEST_MODE"):
        mod.POINT_B_DISTRIBUTION_TEST_MODE = bool(_seed_nom)

    # ── Optional mother export seed — block mothers away at export on Day 1 ──────
    # mother_export_seed_json: {mother_name: days_at_export}
    # Reserves each named mother's berth for (days × 24) hours at t=0 so the
    # sim treats that mother as unavailable until she returns from export.
    _mother_export = {}
    if mother_export_seed_json:
        try:
            _raw_exp = json.loads(mother_export_seed_json)
        except Exception:
            _raw_exp = {}
        if isinstance(_raw_exp, dict):
            for _mn, _days in _raw_exp.items():
                if _mn in _mother_names and isinstance(_days, (int, float)) and _days > 0:
                    _mother_export[_mn] = float(_days)

    # ── Apply optimizer scenario params (if a specific scenario was selected) ──
    # Save originals so we can restore after the run (run_sim mutates the module).
    _opt_orig = {}
    _OPT_KEYS = [
        ("DEAD_STOCK_FACTOR",        "dead_stock_factor"),
        ("POINT_F_MIN_TRIGGER_BBL",  "ibom_trigger_bbl"),
        ("EXPORT_SAIL_WINDOW_START", "export_sail_window_start"),
        ("BERTHING_START",           "berthing_start"),
        ("BERTHING_END",             "berthing_end"),
    ]
    if opt_params_json:
        _opt_pr = json.loads(opt_params_json)
        for _mod_key, _pr_key in _OPT_KEYS:
            if hasattr(mod, _mod_key):
                _opt_orig[_mod_key] = getattr(mod, _mod_key)
                setattr(mod, _mod_key, _opt_pr[_pr_key])

    # Pass epoch directly into Simulation (requires updated sim file).
    # Falls back gracefully if an older sim file is deployed.
    try:
        sim = mod.Simulation(epoch=_epoch)
    except TypeError:
        # Old sim file: set module global then instantiate
        if hasattr(mod, "set_sim_epoch"):
            mod.set_sim_epoch(_epoch)
        sim = mod.Simulation()

    # Seed storage volumes — clamp to capacity and credit any excess as
    # pre-existing overflow so it shows in spill metrics from t=0.
    for _sn, _vol in [("Chapel", chapel), ("JasmineS", jasmines),
                       ("Westmore", westmore), ("Duke", duke), ("Starturn", starturn)]:
        _cap  = mod.STORAGE_CAPACITY_BY_NAME[_sn]
        _over = max(0, _vol - _cap)
        sim.storage_bbl[_sn]         = min(_vol, _cap)
        if _over > 0:
            sim.storage_overflow_bbl[_sn] = sim.storage_overflow_bbl.get(_sn, 0.0) + _over
            sim.storage_overflow_events  += 1
            sim.total_spilled            += _over
    mod.STORAGE_PRODUCTION_RATE_BY_NAME["Chapel"]   = prod_chapel
    mod.STORAGE_PRODUCTION_RATE_BY_NAME["JasmineS"] = prod_jasmines
    mod.STORAGE_PRODUCTION_RATE_BY_NAME["Westmore"] = prod_westmore
    mod.STORAGE_PRODUCTION_RATE_BY_NAME["Duke"]     = prod_duke
    mod.STORAGE_PRODUCTION_RATE_BY_NAME["Starturn"] = prod_starturn
    sim.mother_bbl["Bryanston"]  = min(bryanston,  mod.MOTHER_CAPACITY_BBL)
    sim.mother_bbl["Alkebulan"]  = min(alkebulan,  mod.MOTHER_CAPACITY_BBL)
    sim.mother_bbl["GreenEagle"] = min(greeneagle, mod.MOTHER_CAPACITY_BBL)
    # Seed initial API gravity for mother vessel stock (only meaningful when stock > 0)
    if bryanston  > 0 and bryanston_api  > 0:
        sim.mother_api["Bryanston"]  = float(bryanston_api)
    if alkebulan  > 0 and alkebulan_api  > 0:
        sim.mother_api["Alkebulan"]  = float(alkebulan_api)
    if greeneagle > 0 and greeneagle_api > 0:
        sim.mother_api["GreenEagle"] = float(greeneagle_api)

    if vessel_states_json:
        vs = json.loads(vessel_states_json)
        _sp_map = getattr(mod, "STORAGE_POINT", {})
        for v in sim.vessels:
            if v.name in vs:
                d = vs[v.name]
                if d.get("status"):
                    v.status = d["status"]
                if d.get("cargo_bbl") is not None:
                    _raw_cargo = int(d["cargo_bbl"])
                    _vcap      = v.cargo_capacity
                    _over      = max(0, _raw_cargo - _vcap)
                    v.cargo_bbl = min(_raw_cargo, _vcap)
                    if _over > 0:
                        # Credit excess cargo as pre-existing spill (no specific
                        # storage attribution — use a vessel-level overflow key)
                        _vspill_key = f"vessel_{v.name}"
                        sim.storage_overflow_bbl[_vspill_key] = (
                            sim.storage_overflow_bbl.get(_vspill_key, 0.0) + _over
                        )
                        sim.storage_overflow_events += 1
                        sim.total_spilled           += _over
                # Seed current storage for vessels already at a storage location
                _loc = d.get("location")
                if _loc and _loc in _sp_map:
                    v.target_point     = _sp_map[_loc]
                    v.assigned_storage = _loc
                elif _loc:
                    # Non-storage locations (BIA/Fairway/transit) must not inherit
                    # prior startup storage assignment.
                    if _loc in {"B", "Fairway", "Fairway Buoy"}:
                        v.target_point = "B"
                # Override target_point for transit vessels heading to a specific storage
                _ts = d.get("target_storage")
                if _ts and _ts in _sp_map:
                    v.target_point = _sp_map[_ts]
                    # assigned_storage stays None — vessel not yet arrived
                # Ensure target_point always matches assigned_storage for IDLE_A vessels
                # (guards against default "A" overriding Duke/Starturn placements)
                if v.status == "IDLE_A" and v.assigned_storage and v.assigned_storage in _sp_map:
                    v.target_point = _sp_map[v.assigned_storage]
                _tm = d.get("target_mother")
                if _tm and _tm in getattr(mod, "MOTHER_NAMES", []):
                    v.assigned_mother = _tm

                # ── Partial-discharge resume ──────────────────────────────────
                # If operator entered a volume already transferred to the mother,
                # credit it to the mother now and adjust the vessel's remaining
                # cargo and next_event_time so the sim only pumps the remainder.
                if v.status in {"HOSE_CONNECT_B", "DISCHARGING"}:
                    _xfr = int(d.get("already_transferred_bbl", 0))
                    _full_cargo = v.cargo_bbl             # total cargo this voyage
                    _disch_h    = getattr(mod, "DISCHARGE_HOURS",       12.0)
                    _hose_h     = getattr(mod, "HOSE_CONNECTION_HOURS",  2.0)
                    _mother_names_now = set(getattr(mod, "MOTHER_NAMES", []))
                    _selected_m = v.assigned_mother if v.assigned_mother in _mother_names_now else None
                    if _selected_m is None:
                        # Keep vessel state as provided; sim-side Point B logic now
                        # enforces explicit mother assignment without fallback.
                        continue

                    if _xfr > 0 and _full_cargo > 0:
                        _xfr = min(_xfr, _full_cargo)   # clamp to cargo size
                        # Credit already-transferred volume to mother
                        _prev_mother_bbl = sim.mother_bbl.get(_selected_m, 0.0)
                        _prev_mother_api = sim.mother_api.get(_selected_m, 0.0)
                        _vessel_api_val  = sim.vessel_api.get(v.name, 0.0)
                        sim.mother_bbl[_selected_m] = _prev_mother_bbl + _xfr
                        # Blend vessel API into the volume already on mother
                        if sim.mother_bbl[_selected_m] > 0:
                            sim.mother_api[_selected_m] = (
                                (_prev_mother_bbl * _prev_mother_api + _xfr * _vessel_api_val)
                                / sim.mother_bbl[_selected_m]
                            )
                        # Debit from daughter — only remainder still to pump
                        v.cargo_bbl = _full_cargo - _xfr

                    # Set next_event_time for remaining pump duration.
                    # HOSE_CONNECT_B: hose not yet open — add hose time before pump starts.
                    # DISCHARGING:    already pumping — scale remaining cargo proportionally.
                    if v.cargo_bbl <= 0:
                        # Nothing left — go straight to complete
                        v.next_event_time = 0.0
                    elif v.status == "HOSE_CONNECT_B":
                        # Hose still connecting — full remaining cargo after hose done
                        v.next_event_time = _hose_h
                    else:
                        # Mid-discharge — remaining time proportional to remaining cargo
                        _remaining_frac   = v.cargo_bbl / _full_cargo if _full_cargo > 0 else 0
                        v.next_event_time = _remaining_frac * _disch_h

                    # Reserve mother berth for the remaining window
                    _berth_end = v.next_event_time + (0 if v.status == "DISCHARGING" else _disch_h)
                    sim.mother_berth_free_at[_selected_m] = max(
                        sim.mother_berth_free_at.get(_selected_m, 0.0), _berth_end)
                    sim.next_mother_berthing_start_at = max(
                        sim.next_mother_berthing_start_at, _berth_end)

                # (LOADING partial-cargo resume is handled by the universal
                #  post-processing pass that runs after all defaults are applied)

    # ── Default Bedford / Balham starting state ────────────────────────────────
    # Applied only when the UI has not manually overridden these vessels.
    # Bedford  : PF_LOADING at Ibom, 30k bbl (below 65k swap trigger)
    # Balham   : BERTHING_B at Alkebulan, 85k bbl ready to discharge
    # After discharge Balham sails to SanBarth and runs A→B cycles freely.
    # When Bedford exceeds 65k bbl the swap triggers; Balham then sails B→F
    # to relieve Bedford, who subsequently mirrors the SanBarth A→B cycle.
    _vs_override = json.loads(vessel_states_json) if vessel_states_json else {}
    _BERTH_DELAY  = getattr(mod, "BERTHING_DELAY_HOURS",   0.5)
    _HOSE_HOURS   = getattr(mod, "HOSE_CONNECTION_HOURS",  2.0)
    _DISCH_HOURS  = getattr(mod, "DISCHARGE_HOURS",        12.0)
    _BALHAM_END   = _BERTH_DELAY + _HOSE_HOURS + _DISCH_HOURS   # ≈14.5 h

    _POINT_B_START_VESSELS = {"Sherlock", "Laphroaig", "Rathbone"}
    # Bagshot and Watson are hardcoded to start alongside their dedicated mothers.
    # They are intentionally excluded from _POINT_B_START_VESSELS so this block
    # does NOT reset them to WAITING_RETURN_STOCK.  Their startup blocks follow below.

    for v in sim.vessels:
        if v.name == "Bedford" and "Bedford" not in _vs_override:
            v.status            = "PF_LOADING"
            v.target_point      = "F"
            v.cargo_bbl         = 30_000   # below 65k trigger — keeps loading
            v.next_event_time   = 0.0
            v._voyage_assigned  = True
            v.current_voyage    = 1
            # Ibom API is constant — set directly so vessel card shows correct value
            sim.vessel_api[v.name] = getattr(mod, "IBOM_API", 32.0)

        elif v.name == "Balham" and "Balham" not in _vs_override:
            v.status            = "BERTHING_B"
            v.target_point      = "B"
            v.cargo_bbl         = 85_000   # Balham loaded at Ibom (no Point A cap)
            v.assigned_mother   = "Alkebulan"
            v.next_event_time   = _BERTH_DELAY   # berthing completes after 0.5 h
            v._voyage_assigned  = True
            v.current_voyage    = 1
            # Balham loads from Ibom (Point F) — cargo API is constant Ibom API
            sim.vessel_api[v.name] = getattr(mod, "IBOM_API", 32.0)
            # Reserve Alkebulan's berth for the full discharge window so no
            # other vessel attempts to berth there at t=0
            sim.mother_berth_free_at["Alkebulan"] = max(
                sim.mother_berth_free_at.get("Alkebulan", 0.0), _BALHAM_END
            )
            # Serial discharge: next vessel may not berth until Balham finishes
            sim.next_mother_berthing_start_at = max(
                sim.next_mother_berthing_start_at, _BALHAM_END
            )

        elif v.name in _POINT_B_START_VESSELS and v.name not in _vs_override:
            # Default: start at BIA waiting for return-stock allocation
            v.status           = "WAITING_RETURN_STOCK"
            v.target_point     = "B"
            v.cargo_bbl        = 0
            v.next_event_time  = 0.0
            v._voyage_assigned = True
            v.current_voyage   = 1

        # ── Hardcoded startup: Bagshot alongside Bryanston ──────────────────
        # Bagshot always starts hose-connected at Bryanston, ready to discharge.
        # This is intentionally hardcoded and must NOT be changed by the
        # _POINT_B_START_VESSELS block or any other default-state logic.
        elif v.name == "Bagshot" and "Bagshot" not in _vs_override:
            _bg_cap = getattr(mod, "VESSEL_CAPACITIES", {}).get(
                "Bagshot", getattr(mod, "DAUGHTER_CARGO_BBL", 85_000))
            v.status           = "HOSE_CONNECT_B"
            v.target_point     = "B"
            v.cargo_bbl        = _bg_cap        # 43,000 bbl full cargo
            v.assigned_mother  = "Bryanston"    # HARDCODED — do not change
            v.next_event_time  = 0.0            # fires at t=0 → immediately starts discharging
            v._voyage_assigned = True
            v.current_voyage   = 1
            # Reserve Bryanston's berth for the full hose+discharge window
            _bg_end = _HOSE_HOURS + _DISCH_HOURS
            sim.mother_berth_free_at["Bryanston"] = max(
                sim.mother_berth_free_at.get("Bryanston", 0.0), _bg_end
            )
            sim.next_mother_berthing_start_at = max(
                sim.next_mother_berthing_start_at, _bg_end
            )

        # ── Hardcoded startup: Watson alongside GreenEagle ──────────────────
        # Watson always starts hose-connected at GreenEagle, ready to discharge.
        # This is intentionally hardcoded and must NOT be changed by the
        # _POINT_B_START_VESSELS block or any other default-state logic.
        elif v.name == "Watson" and "Watson" not in _vs_override:
            _wt_cap = getattr(mod, "VESSEL_CAPACITIES", {}).get(
                "Watson", getattr(mod, "DAUGHTER_CARGO_BBL", 85_000))
            v.status           = "HOSE_CONNECT_B"
            v.target_point     = "B"
            v.cargo_bbl        = _wt_cap        # 85,000 bbl full cargo
            v.assigned_mother  = "GreenEagle"   # HARDCODED — do not change
            v.next_event_time  = 0.0            # fires at t=0 → immediately starts discharging
            v._voyage_assigned = True
            v.current_voyage   = 1
            # Reserve GreenEagle's berth for the full hose+discharge window
            _wt_end = _HOSE_HOURS + _DISCH_HOURS
            sim.mother_berth_free_at["GreenEagle"] = max(
                sim.mother_berth_free_at.get("GreenEagle", 0.0), _wt_end
            )
            sim.next_mother_berthing_start_at = max(
                sim.next_mother_berthing_start_at, _wt_end
            )

    # Sim-level Ibom tracking: Bedford active, no swap pending
    sim.point_f_active_loader     = "Bedford"
    sim.point_f_swap_pending_for  = None
    sim.point_f_swap_triggered_by = None

    # ── Mother export seed: block mothers away at export from t=0 ─────────────
    # For each mother in _mother_export, reserve her berth for (days × 24) hours.
    # Any daughters nominated to that mother will berth only after she returns.
    for _exp_mn, _exp_days in _mother_export.items():
        _exp_h = _exp_days * 24.0
        sim.mother_berth_free_at[_exp_mn] = max(
            sim.mother_berth_free_at.get(_exp_mn, 0.0), _exp_h
        )
        sim.next_mother_berthing_start_at = max(
            sim.next_mother_berthing_start_at, _exp_h
        )

    # ── Universal LOADING partial-cargo resume pass ────────────────────────
    # For every vessel seeded in LOADING status, treat cargo_bbl as the
    # volume already on board.  Compute remaining load time, deduct only
    # the remaining balance from the assigned storage, credit already-loaded
    # volume to total_loaded, and reserve the storage berth accordingly.
    # This covers both manual overrides (vessel_states_json) and hardcoded
    # defaults (e.g. vessels seeded in LOADING state at a specific storage).
    # Build a rate map from sim module constants (all storages, no fallback gaps)
    _stor_rate_map = {
        getattr(mod, "STORAGE_PRIMARY_NAME",     "Chapel"):   getattr(mod, "CHAPEL_LOAD_RATE_BPH",   7_083),
        getattr(mod, "STORAGE_SECONDARY_NAME",   "JasmineS"): getattr(mod, "JASMINES_LOAD_RATE_BPH", 7_083),
        getattr(mod, "STORAGE_TERTIARY_NAME",    "Westmore"): getattr(mod, "WESTMORE_LOAD_RATE_BPH", 2_500),
        getattr(mod, "STORAGE_QUATERNARY_NAME",  "Duke"):     getattr(mod, "DUKE_LOAD_RATE_BPH",     3_500),
        getattr(mod, "STORAGE_QUINARY_NAME",     "Starturn"): getattr(mod, "STARTURN_LOAD_RATE_BPH", 2_500),
    }
    _sp_map_post   = getattr(mod, "STORAGE_POINT", {})
    _scap_map      = getattr(mod, "STORAGE_CAPACITY_BY_NAME", {})

    for _v in sim.vessels:
        if _v.status != "LOADING" or not _v.assigned_storage:
            continue
        _stor    = _v.assigned_storage
        # Apply Point A loading cap for Bedford/Balham
        _pt_a_cap_vessels = getattr(mod, "POINT_A_LOAD_CAP_VESSELS", {"Bedford", "Balham"})
        _pt_a_cap_bbl     = getattr(mod, "POINT_A_LOAD_CAP_BBL",     63_000)
        _sp_map_load      = getattr(mod, "STORAGE_POINT", {})
        _is_pt_a = (_sp_map_load.get(_stor) == "A")
        _vcap    = (_pt_a_cap_bbl
                    if (_v.name in _pt_a_cap_vessels and _is_pt_a)
                    else _v.cargo_capacity)
        _loaded  = min(_v.cargo_bbl, _vcap)       # already on board (clamped)
        _remain  = max(0.0, _vcap - _loaded)       # still to load

        # Full and remaining load durations — rate-based for every storage
        _rate     = _stor_rate_map.get(_stor, getattr(mod, "LOAD_HOURS", 12))
        _full_h   = _vcap   / _rate if isinstance(_rate, (int, float)) and _rate > 0 else getattr(mod, "LOAD_HOURS", 12)
        _remain_h = _remain / _rate if isinstance(_rate, (int, float)) and _rate > 0 else _full_h

        _v.assigned_load_hours = _full_h
        _v.next_event_time     = _remain_h         # sim fires when loading completes

        # Deduct only the balance not yet loaded from storage
        _cur_stock = sim.storage_bbl.get(_stor, 0.0)
        _cap_s     = _scap_map.get(_stor, float("inf"))
        sim.storage_bbl[_stor] = max(0.0, min(_cur_stock, _cap_s) - _remain)

        # Credit already-loaded volume toward total_loaded metric
        sim.total_loaded = getattr(sim, "total_loaded", 0) + _loaded

        # Reserve berth for the remaining loading window
        sim.storage_berth_free_at[_stor] = max(
            sim.storage_berth_free_at.get(_stor, 0.0), _remain_h
        )
        # Ensure target_point is consistent with assigned_storage
        if _stor in _sp_map_post:
            _v.target_point = _sp_map_post[_stor]

        _v.cargo_bbl = _loaded   # keep partial value; LOADING handler sets full at completion

        # Emit a synthetic LOADING_START event at t=0 so the JMP and Loading Plan
        # displays can correctly map this vessel to its storage.
        # Guard against duplicate startup logs when the simulator has already
        # emitted the same t=0 LOADING_START for vessels seeded in LOADING state.
        # Set vessel_api to the storage's current API — vessel carries storage API exactly.
        _stor_api_now = getattr(mod, "STORAGE_API", {}).get(_stor, 0.0)
        sim.vessel_api[_v.name] = _stor_api_now
        _stor_stock_now = sim.storage_bbl.get(_stor, 0.0)
        _t0_str = sim.hours_to_dt(0).strftime("%Y-%m-%d %H:%M")
        _startup_loading_exists = any(
            _row.get("Event") == "LOADING_START"
            and _row.get("Vessel") == _v.name
            and _row.get("Time") == _t0_str
            for _row in getattr(sim, "log", [])
        )
        if not _startup_loading_exists:
            sim.log_event(0, _v.name, "LOADING_START",
                          f"Loading {_vcap:,} bbl @ {_stor_api_now:.2f}° API | {_stor}: {_stor_stock_now:,.0f} bbl "
                          f"(started at t=0, {_loaded:,.0f} bbl already on board, "
                          f"remaining {_remain_h:.1f}h)",
                          voyage_num=getattr(_v, "current_voyage", 1))

    for k, v in orig.items():
        if k != "_ibom_rate": setattr(mod, k, v)
    _r = orig["_ibom_rate"]
    if hasattr(mod, "IBOM_LOAD_RATE_BPH"):    mod.IBOM_LOAD_RATE_BPH    = _r
    if hasattr(mod, "POINT_F_LOAD_RATE_BPH"): mod.POINT_F_LOAD_RATE_BPH = _r

    log_df, tl_df = sim.run()

    # Restore any optimizer params that were temporarily applied
    for _mod_key, _orig_val in _opt_orig.items():
        setattr(mod, _mod_key, _orig_val)

    summary = dict(
        loadings        = int(len(log_df[log_df.Event == "LOADING_START"])),
        discharges      = int(len(log_df[log_df.Event == "DISCHARGE_START"])),
        loaded          = int(sim.total_loaded),
        exported        = float(sim.total_exported),
        produced        = float(sim.total_produced),
        spilled         = float(sim.total_spilled),
        exports         = int(len(log_df[log_df.Event == "EXPORT_COMPLETE"])),
        ovf_events      = int(sim.storage_overflow_events),
        vessel_names    = [v.name for v in sim.vessels],
        spill_by_storage= {k: float(v) for k, v in sim.storage_overflow_bbl.items()},
        **{f"final_{k}": float(v) for k, v in sim.storage_bbl.items()},
        **{f"final_{k}": float(v) for k, v in sim.mother_bbl.items()},
        storage_api     = {k: round(float(v), 2) for k, v in getattr(sim, "final_storage_api", {}).items()},
        mother_api      = {k: round(float(v), 2) for k, v in getattr(sim, "final_mother_api",  {}).items()},
        vessel_api      = {k: round(float(v), 2) for k, v in getattr(sim, "final_vessel_api",  {}).items()},
        avg_exported_api= round(float(getattr(sim, "avg_exported_api", 0.0)), 2),
        vessel_cargo    = {v.name: round(v.cargo_bbl) for v in sim.vessels},
    )
    return log_df, tl_df, summary


@st.cache_data(ttl=3600, show_spinner=False)
def run_optimizer(base_params_json: str):
    """
    Heuristic parameter sweep — no external OptimizationEngine required.
    Sweeps dead_stock_factor, ibom_trigger_bbl, export_sail_window_start,
    berthing_start, berthing_end across a grid and scores each scenario.
    Returns (best_slim_json, results_table_json).
    """
    base = json.loads(base_params_json)
    _tide_hex   = base.pop("_tide_csv_bytes_hex", None)
    _tide_bytes = None
    if _tide_hex:
        _tide_bytes = binascii.unhexlify(_tide_hex)
    _start_iso  = base.pop("_sim_start_date", None) or ""

    # ── Parameter grid ──────────────────────────────────────────────────
    _dead_stock_factors      = [1.50, 1.75, 2.00]
    _ibom_triggers           = [45_000, 55_000, 65_000, 75_000]
    _export_window_starts    = [6, 8, 10]
    _berthing_configs        = [(6, 18), (6, 20), (7, 18)]   # (start, end)

    _grid = list(itertools.product(
        _dead_stock_factors,
        _ibom_triggers,
        _export_window_starts,
        _berthing_configs,
    ))

    mod = _load_mod_current()
    _orig_dsf  = getattr(mod, "DEAD_STOCK_FACTOR",         1.75)
    _orig_pftrig = getattr(mod, "POINT_F_MIN_TRIGGER_BBL", 65_000)
    _orig_expw = getattr(mod, "EXPORT_SAIL_WINDOW_START",  6)
    _orig_bstart = getattr(mod, "BERTHING_START",          6)
    _orig_bend   = getattr(mod, "BERTHING_END",            18)

    def _restore():
        if hasattr(mod, "DEAD_STOCK_FACTOR"):         mod.DEAD_STOCK_FACTOR         = _orig_dsf
        if hasattr(mod, "POINT_F_MIN_TRIGGER_BBL"):   mod.POINT_F_MIN_TRIGGER_BBL   = _orig_pftrig
        if hasattr(mod, "EXPORT_SAIL_WINDOW_START"):  mod.EXPORT_SAIL_WINDOW_START  = _orig_expw
        if hasattr(mod, "BERTHING_START"):             mod.BERTHING_START            = _orig_bstart
        if hasattr(mod, "BERTHING_END"):               mod.BERTHING_END              = _orig_bend

    def _score(S, log_df, tl_df):
        sim_days  = base.get("sim_days", 14)
        storage_names = ["Chapel", "JasmineS", "Westmore", "Duke", "Starturn"]
        total_loaded  = S.get("loaded", 0)
        total_exported = S.get("exported", 0)
        spilled = S.get("spilled", 0)

        # ── Primary Objective: crash stock drawdown speed ───────────────
        initial_total_stock = sum(float(base.get(sn.lower(), 0)) for sn in storage_names)
        final_total_stock = sum(float(S.get(f"final_{sn}", 0.0)) for sn in storage_names)
        drawdown_bbl = max(0.0, initial_total_stock - final_total_stock)
        drawdown_pct = 100.0 * drawdown_bbl / max(1.0, initial_total_stock)

        if all(sn in tl_df.columns for sn in storage_names) and not tl_df.empty:
            total_storage_series = tl_df[storage_names].sum(axis=1)
            # Reward early crash-down in the first 24h window.
            early_steps = max(1, min(len(total_storage_series), 48))
            early_min = float(total_storage_series.iloc[:early_steps].min())
            early_drawdown_pct = 100.0 * max(0.0, initial_total_stock - early_min) / max(1.0, initial_total_stock)
        else:
            early_drawdown_pct = drawdown_pct

        crash_score = min(100.0, 0.65 * drawdown_pct + 0.35 * early_drawdown_pct)

        # ── Safety Objective: suppress overflow + high-risk stock exposure ─
        critical_by_storage = getattr(mod, "STORAGE_CRITICAL_THRESHOLD_BY_NAME", {})
        cap_by_storage = {
            "Chapel": 800000,
            "JasmineS": 290000,
            "Westmore": 270000,
            "Duke": 228000,
            "Starturn": 228000,
        }
        risk_fracs = []
        risk_by_storage = {}
        borderline_fracs = []
        borderline_by_storage = {}
        if not tl_df.empty:
            for sn in storage_names:
                if sn not in tl_df.columns:
                    continue
                s_col = tl_df[sn].astype(float)
                crit = float(critical_by_storage.get(sn, 0.0))
                cap = float(cap_by_storage.get(sn, max(1.0, s_col.max())))
                if crit > 0:
                    _rf = float((s_col > crit).mean())
                    risk_fracs.append(_rf)
                    risk_by_storage[sn] = _rf
                _bf = float((s_col >= 0.90 * cap).mean())
                borderline_fracs.append(_bf)
                borderline_by_storage[sn] = _bf
        risk_avg = (sum(risk_fracs) / len(risk_fracs)) if risk_fracs else 0.0
        borderline_avg = (sum(borderline_fracs) / len(borderline_fracs)) if borderline_fracs else 0.0
        max_risk = max(risk_by_storage.values()) if risk_by_storage else 0.0
        max_borderline = max(borderline_by_storage.values()) if borderline_by_storage else 0.0
        persistent_hotspots = sum(
            1 for sn in storage_names
            if risk_by_storage.get(sn, 0.0) > 0.18 or borderline_by_storage.get(sn, 0.0) > 0.28
        )
        spill_penalty = min(85.0, (spilled / max(1.0, total_loaded + 1.0)) * 750.0)
        risk_penalty = min(30.0, risk_avg * 100.0 * 0.55)
        borderline_penalty = min(25.0, borderline_avg * 100.0 * 0.45)
        # Prevent average-masking: punish single-location sustained risk strongly.
        max_risk_penalty = min(20.0, max_risk * 100.0 * 0.22)
        max_borderline_penalty = min(12.0, max_borderline * 100.0 * 0.14)
        hotspot_penalty = min(15.0, float(persistent_hotspots) * 5.0)
        overflow_score = max(
            0.0,
            100.0
            - spill_penalty
            - risk_penalty
            - borderline_penalty
            - max_risk_penalty
            - max_borderline_penalty
            - hotspot_penalty,
        )

        # ── Utilisation Objective: avoid idle daughters when stock exists ──
        idle_sts = {
            "IDLE_A", "IDLE_B", "WAITING_BERTH_A", "WAITING_BERTH_B",
            "WAITING_STOCK", "WAITING_DEAD_STOCK", "WAITING_RETURN_STOCK",
            "WAITING_MOTHER_CAPACITY", "WAITING_MOTHER_RETURN"
        }
        vessel_cols = [vn for vn in S.get("vessel_names", []) if vn in tl_df.columns]
        total_slots = 0
        idle_slots = 0
        for vn in vessel_cols:
            col = tl_df[vn]
            total_slots += len(col)
            idle_slots += int(col.isin(idle_sts).sum())
        idle_frac = idle_slots / max(1, total_slots)

        # Penalize idle time specifically when any storage has evac-capable stock.
        min_vcap = min(getattr(mod, "VESSEL_CAPACITIES", {"_": 85000}).values())
        evac_threshold = float(getattr(mod, "DEAD_STOCK_FACTOR", 1.75)) * float(min_vcap)
        if all(sn in tl_df.columns for sn in storage_names) and vessel_cols:
            stock_available = (tl_df[storage_names].max(axis=1) >= evac_threshold)
            any_idle = pd.Series(False, index=tl_df.index)
            for vn in vessel_cols:
                any_idle = any_idle | tl_df[vn].isin(idle_sts)
            idle_with_stock_frac = float((stock_available & any_idle).mean())
        else:
            idle_with_stock_frac = 1.0

        util_base = max(0.0, 100.0 * (1.0 - idle_frac * 1.9))
        idle_with_stock_penalty = min(35.0, idle_with_stock_frac * 100.0 * 0.45)
        idle_score = max(0.0, util_base - idle_with_stock_penalty)

        # ── Fair Allocation Objective: service all storage locations fairly ─
        prod_map = {
            "Chapel": float(base.get("prod_chapel", 0) or 0),
            "JasmineS": float(base.get("prod_jasmines", 0) or 0),
            "Westmore": float(base.get("prod_westmore", 0) or 0),
            "Duke": float(base.get("prod_duke", 0) or 0),
            "Starturn": float(base.get("prod_starturn", 0) or 0),
        }
        total_prod = sum(prod_map.values())
        if total_prod <= 0:
            target_share = {sn: 1.0 / len(storage_names) for sn in storage_names}
        else:
            target_share = {sn: prod_map[sn] / total_prod for sn in storage_names}

        load_counts = {sn: 0 for sn in storage_names}
        if not log_df.empty and "Event" in log_df.columns and "Detail" in log_df.columns:
            _loads = log_df[log_df["Event"] == "LOADING_START"]
            if not _loads.empty:
                _stor = _loads["Detail"].astype(str).str.extract(r"\|\s*([A-Za-z]+):")[0]
                _stor = _stor[_stor.isin(storage_names)]
                if not _stor.empty:
                    vc = _stor.value_counts()
                    for sn in storage_names:
                        load_counts[sn] = int(vc.get(sn, 0))

        total_load_events = sum(load_counts.values())
        if total_load_events <= 0:
            fairness_score = 0.0
        else:
            actual_share = {sn: load_counts[sn] / total_load_events for sn in storage_names}
            # L1-distance fairness (0=perfect match to target share, 1=max mismatch)
            mismatch = 0.5 * sum(abs(actual_share[sn] - target_share[sn]) for sn in storage_names)
            fairness_score = max(0.0, 100.0 * (1.0 - mismatch))
            # Extra penalty for neglecting risky/borderline storages entirely.
            neglected_penalty = 0.0
            for sn in storage_names:
                if load_counts[sn] == 0 and (risk_by_storage.get(sn, 0) > 0.15 or borderline_by_storage.get(sn, 0) > 0.20):
                    _sev = max(risk_by_storage.get(sn, 0.0), borderline_by_storage.get(sn, 0.0))
                    neglected_penalty += 12.0 + (14.0 * _sev)
            fairness_score = max(0.0, fairness_score - min(35.0, neglected_penalty))

        # ── Secondary efficiency metrics ─────────────────────────────────
        n_exports = S.get("exports", 0)
        export_score = min(100.0, n_exports * 20.0)   # 5 exports = 100

        # Avg cycle hours
        cyc_ev = log_df[log_df["Event"] == "ARRIVED_LOADING_POINT"] if not log_df.empty else pd.DataFrame()
        if len(cyc_ev) >= 2:
            times = sorted(cyc_ev["Time"].tolist())
            gaps  = [(pd.Timestamp(times[i+1]) - pd.Timestamp(times[i])).total_seconds()/3600
                     for i in range(len(times)-1) if
                     (pd.Timestamp(times[i+1]) - pd.Timestamp(times[i])).total_seconds()/3600 < 120]
            avg_cycle = math.fsum(gaps)/len(gaps) if gaps else 48.0
        else:
            avg_cycle = 48.0
        turnaround_score = max(0.0, 100.0 - max(0.0, avg_cycle - 24.0) * 2.0)

        # Composite objective prioritises safety and fair multi-location service,
        # while preserving rapid drawdown and utilisation pressure.
        composite = (
            crash_score * 0.28
            + overflow_score * 0.42
            + idle_score * 0.15
            + fairness_score * 0.12
            + export_score * 0.02
            + turnaround_score * 0.01
        )

        # ── Bottlenecks ──────────────────────────────────────────────────
        bottlenecks = []
        if drawdown_pct < 25: bottlenecks.append("Slow stock crash-down")
        if idle_with_stock_frac > 0.20: bottlenecks.append("Idle daughters while stock available")
        if spilled > 0:       bottlenecks.append(f"Storage overflow ({spilled:,.0f} bbl)")
        if risk_avg > 0.25:   bottlenecks.append("Sustained high-risk storage levels")
        if fairness_score < 55: bottlenecks.append("Unfair storage allocation pattern")
        if avg_cycle > 60:    bottlenecks.append("Long cycle times")
        if n_exports == 0:    bottlenecks.append("No exports completed")

        # ── Vessel utilisation ───────────────────────────────────────────
        vu = {}
        for vn in S.get("vessel_names", []):
            if vn in tl_df.columns:
                col = tl_df[vn]
                active = (~col.isin(idle_sts | {"IDLE_A","IDLE_B"})).sum()
                vu[vn] = round(100.0 * active / max(1, len(col)), 1)

        # ── Storage utilisation ──────────────────────────────────────────
        su = {}
        for sn in ["Chapel","JasmineS","Westmore","Duke","Starturn"]:
            if sn in tl_df.columns:
                col = tl_df[sn].dropna()
                cap_col = f"{sn}_cap"
                cap = 290_000 if sn == "JasmineS" else 270_000 if sn == "Westmore" else 228_000
                su[sn] = {
                    "avg_pct":      round(100.0 * col.mean() / cap, 1),
                    "peak_pct":     round(100.0 * col.max()  / cap, 1),
                    "overflow_bbl": int(S.get("spill_by_storage",{}).get(sn, 0)),
                }

        return dict(
            composite=round(composite,2),
            throughput_score=round(crash_score,2),
            idle_score=round(idle_score,2),
            overflow_score=round(overflow_score,2),
            fairness_score=round(fairness_score,2),
            export_score=round(export_score,2),
            turnaround_score=round(turnaround_score,2),
            total_loaded_bbl=int(total_loaded),
            total_exported_bbl=float(total_exported),
            total_spilled_bbl=float(spilled),
            stock_drawdown_bbl=float(drawdown_bbl),
            stock_drawdown_pct=round(drawdown_pct,2),
            early_drawdown_pct=round(early_drawdown_pct,2),
            stock_risk_frac=round(risk_avg,4),
            idle_with_stock_frac=round(idle_with_stock_frac,4),
            avg_cycle_hours=round(avg_cycle,1),
            bottlenecks=bottlenecks,
            vessel_utilisation=vu,
            storage_utilisation=su,
        )

    all_results = []
    try:
        for rank, (dsf, pft, expw, (bstart, bend)) in enumerate(_grid, 1):
            try:
                if hasattr(mod, "DEAD_STOCK_FACTOR"):        mod.DEAD_STOCK_FACTOR        = dsf
                if hasattr(mod, "POINT_F_MIN_TRIGGER_BBL"):  mod.POINT_F_MIN_TRIGGER_BBL  = pft
                if hasattr(mod, "EXPORT_SAIL_WINDOW_START"): mod.EXPORT_SAIL_WINDOW_START = expw
                if hasattr(mod, "BERTHING_START"):            mod.BERTHING_START           = bstart
                if hasattr(mod, "BERTHING_END"):              mod.BERTHING_END             = bend

                _log, _tl, S = run_sim(
                    sim_days            = base.get("sim_days", 14),
                    chapel              = base.get("chapel",    100_000),
                    jasmines            = base.get("jasmines",  100_000),
                    westmore            = base.get("westmore",  100_000),
                    duke                = base.get("duke",       40_000),
                    starturn            = base.get("starturn",   30_000),
                    bryanston           = base.get("bryanston",       0),
                    alkebulan           = base.get("alkebulan",       0),
                    greeneagle          = base.get("greeneagle",      0),
                    prod_chapel         = base.get("prod_chapel",  2500),
                    prod_jasmines       = base.get("prod_jasmines",2500),
                    prod_westmore       = base.get("prod_westmore",2500),
                    prod_duke           = base.get("prod_duke",    500),
                    prod_starturn       = base.get("prod_starturn",350),
                    prod_ibom           = base.get("prod_ibom",    165),
                    vessel_states_json  = None,
                    tide_csv_bytes      = _tide_bytes,
                    sim_start_date      = _start_iso,
                    _sim_version        = f"opt_{rank}",
                )
                sc = _score(S, _log, _tl)
                all_results.append(dict(
                    rank=rank,
                    label=f"dsf={dsf:.2f} pft={pft//1000}k expw={expw}h b={bstart}-{bend}",
                    params=dict(dead_stock_factor=dsf, ibom_trigger_bbl=int(pft),
                                export_sail_window_start=int(expw),
                                berthing_start=int(bstart), berthing_end=int(bend)),
                    score=sc,
                ))
            except Exception:
                continue
    finally:
        _restore()

    if not all_results:
        # Fallback: one default run
        _log, _tl, S = run_sim(
            sim_days=base.get("sim_days",14), chapel=base.get("chapel",100_000),
            jasmines=base.get("jasmines",100_000), westmore=base.get("westmore",100_000),
            duke=base.get("duke",40_000), starturn=base.get("starturn",30_000),
            bryanston=0, alkebulan=0, greeneagle=0,
            prod_chapel=base.get("prod_chapel",2500), prod_jasmines=base.get("prod_jasmines",2500),
            prod_westmore=base.get("prod_westmore",2500), prod_duke=base.get("prod_duke",500),
            prod_starturn=base.get("prod_starturn",350), prod_ibom=base.get("prod_ibom",165),
            vessel_states_json=None, tide_csv_bytes=_tide_bytes,
            sim_start_date=_start_iso, _sim_version="opt_fallback",
        )
        sc = _score(S, _log, _tl)
        all_results.append(dict(rank=1, label="default",
            params=dict(dead_stock_factor=1.75, ibom_trigger_bbl=65000,
                        export_sail_window_start=6, berthing_start=6, berthing_end=18),
            score=sc))

    all_results.sort(key=lambda r: r["score"]["composite"], reverse=True)
    for i, r in enumerate(all_results, 1):
        r["rank"] = i

    best = all_results[0]
    best_slim = dict(params=best["params"], score=best["score"],
                     rank=best["rank"], label=best["label"])

    rows = []
    for r in all_results:
        sc = r["score"]; pr = r["params"]
        rows.append({
            "Rank": r["rank"], "Score": sc["composite"],
            "Stock Drawdown": sc["throughput_score"], "Fleet Util": sc["idle_score"],
            "Storage Safety": sc["overflow_score"], "Fair Allocation": sc["fairness_score"], "Export": sc["export_score"],
            "Turnaround": sc["turnaround_score"],
            "Loaded (bbl)": sc["total_loaded_bbl"],
            "Spilled (bbl)": sc["total_spilled_bbl"],
            "Avg Cycle (h)": sc["avg_cycle_hours"],
            "dead_stock_x": pr["dead_stock_factor"],
            "pf_trigger_k": pr["ibom_trigger_bbl"] // 1000,
            "exp_window_h": pr["export_sail_window_start"],
            "berth_start_h": pr["berthing_start"],
            "berth_end_h": pr["berthing_end"],
        })
    tbl = pd.DataFrame(rows)
    return json.dumps(best_slim), tbl.to_json(orient="records")


# =============================================================================
# ── GOOGLE SHEETS ─────────────────────────────────────────────────────────────
# =============================================================================

def _gs_client(creds_json):
    import gspread
    from google.oauth2.service_account import Credentials
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds  = Credentials.from_service_account_info(json.loads(creds_json), scopes=scopes)
    return gspread.authorize(creds)


def gs_load_volumes(sheet_id, creds_json):
    """
    Load latest row from the 'volumes' tab.
    Returns dict: chapel, jasmines, westmore, duke, starturn,
                  bryanston, alkebulan, greeneagle,
      prod_chapel, prod_jasmines, prod_westmore,
      prod_duke, prod_starturn, prod_ibom, sim_days
    """
    try:
        gc = _gs_client(creds_json)
        try:
            ws = gc.open_by_key(sheet_id).worksheet("volumes")
        except Exception:
            ws = gc.open_by_key(sheet_id).sheet1
        rows = ws.get_all_records()
        if not rows:
            return {}
        latest = rows[-1]
        mapping = {
            "chapel_bbl"    : "chapel",
            "jasmines_bbl"  : "jasmines",
            "westmore_bbl"  : "westmore",
            "duke_bbl"      : "duke",
            "starturn_bbl"  : "starturn",
            "bryanston_bbl" : "bryanston",
            "alkebulan_bbl" : "alkebulan",
            "greeneagle_bbl": "greeneagle",
            "prod_chapel_bph"  : "prod_chapel",
            "prod_jasmines_bph": "prod_jasmines",
            "prod_westmore_bph": "prod_westmore",
            "prod_duke_bph"    : "prod_duke",
            "prod_starturn_bph": "prod_starturn",
            "prod_ibom_bph"  : "prod_ibom",
            "sim_days"      : "sim_days",
        }
        out = {}
        for sc, ak in mapping.items():
            val = latest.get(sc, "")
            if val not in ("", None):
                try:
                    out[ak] = int(float(str(val).replace(",", "")))
                except ValueError:
                    pass
        # capture raw timestamp for display
        ts = latest.get("timestamp", "")
        if ts:
            out["_timestamp"] = str(ts)
        return out
    except ImportError:
        st.sidebar.warning("Install gspread: pip install gspread google-auth")
    except Exception as e:
        st.sidebar.error(f"Sheets (volumes) error: {e}")
    return {}


def gs_load_fleet(sheet_id, creds_json):
    """
    Load 'fleet' tab. Returns DataFrame with columns:
    vessel | status | location | cargo_bbl | notes | mother_status
    One row per vessel (latest timestamp wins).
    """
    try:
        gc = _gs_client(creds_json)
        try:
            ws = gc.open_by_key(sheet_id).worksheet("fleet")
        except Exception:
            return pd.DataFrame()
        rows = ws.get_all_records()
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        if "vessel" not in df.columns:
            return pd.DataFrame()
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            df = df.sort_values("timestamp", na_position="first")
            df = df.groupby("vessel", as_index=False).last()
        for col in ["cargo_bbl"]:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",",""), errors="coerce"
                ).fillna(0).astype(int)
        for col in ["status","location","notes","mother_status"]:
            if col not in df.columns:
                df[col] = ""
            df[col] = df[col].fillna("").astype(str).str.strip()
        return df
    except ImportError:
        pass
    except Exception as e:
        st.sidebar.error(f"Sheets (fleet) error: {e}")
    return pd.DataFrame()


# =============================================================================
# ── CAPACITY RECOMMENDATION ENGINE ───────────────────────────────────────────
# =============================================================================

def capacity_recommendations(S, params, tl_df, mod):
    """
    Analyse simulation results and produce structured fleet/capacity recommendations.
    Returns list of dicts: {type, severity, title, body, metric}
    severity: 0=ok  1=low  2=medium  3=high
    """
    recs = []

    total_spilled = S["spilled"]
    ovf_events    = S["ovf_events"]
    sim_days      = params["sim_days"]
    prod_chapel   = params.get("prod_chapel",   mod.PRODUCTION_RATE_BPH)
    prod_jasmines = params.get("prod_jasmines", mod.PRODUCTION_RATE_BPH)
    prod_westmore = params.get("prod_westmore", mod.WESTMORE_PRODUCTION_RATE_BPH)
    prod_duke     = params.get("prod_duke",     mod.DUKE_PRODUCTION_RATE_BPH)
    prod_starturn = params.get("prod_starturn", mod.STARTURN_PRODUCTION_RATE_BPH)
    prod_ibom   = params.get("prod_ibom",   getattr(mod, "IBOM_LOAD_RATE_BPH",
                             getattr(mod, "POINT_F_LOAD_RATE_BPH", 165)))
    total_prod_bpd = (prod_chapel + prod_jasmines + prod_westmore + prod_duke + prod_starturn + prod_ibom) * 24

    loadings     = S["loadings"]
    total_loaded = S["loaded"]

    # ── No overflow ───────────────────────────────────────────────────────────
    if total_spilled <= 0 and ovf_events == 0:
        recs.append(dict(
            type="ok", severity=0,
            title="✅ No overflow — current fleet is sufficient",
            body=(
                f"The simulation ran {sim_days} days with total production of "
                f"<span class='hl-green'>{total_prod_bpd:,.0f} bbl/day</span> "
                f"across all storage points and recorded zero overflow or spill. "
                f"The fleet completed <b>{loadings} lifts</b> clearing all production. "
                f"No additional assets are required at current production rates."
            ),
            metric=None,
        ))
        return recs

    # ── Derived metrics ───────────────────────────────────────────────────────
    spill_per_day     = total_spilled / sim_days
    spill_pct         = total_spilled / max(total_prod_bpd * sim_days, 1) * 100
    avg_cargo         = total_loaded / loadings if loadings else mod.DAUGHTER_CARGO_BBL
    lifts_per_day     = loadings / sim_days

    # Approximate round-trip cycle time in hours
    # load(12) + doc(4) + cast-off(0.2) + sail SanBarth→BIA(8) + berth(0.5) + hose(2) + disch(12) + cast-off(0.2) + sail BIA→SanBarth(6)
    rt_hours          = 44.9
    trips_per_day     = 24 / rt_hours           # one vessel does ~0.534 round-trips/day

    # Extra throughput gap in bbl/day
    throughput_gap    = spill_per_day

    # How many vessel-equivalents does the gap represent?
    bbl_per_vessel_day = trips_per_day * avg_cargo
    vessel_equivalents = throughput_gap / max(bbl_per_vessel_day, 1)

    # Spill by storage
    spill_by = S.get("spill_by_storage", {})
    worst = sorted([(k,v) for k,v in spill_by.items() if v > 0], key=lambda x:-x[1])

    # ── Rec 0: overflow summary ───────────────────────────────────────────────
    sev = 1 if spill_pct < 2 else (2 if spill_pct < 8 else 3)
    worst_str = "; ".join(f"{k}: {v:,.0f} bbl" for k,v in worst[:3]) or "various"
    recs.append(dict(
        type="overflow_summary", severity=sev,
        title=f"⚠️ {total_spilled:,.0f} bbl overflow in {sim_days} days "
              f"({spill_pct:.1f}% of production)",
        body=(
            f"Average overflow rate: <span class='hl-yellow'>{spill_per_day:,.0f} bbl/day</span>. "
            f"Worst affected storage: <b>{worst_str}</b>. "
            f"Fleet averaged <b>{lifts_per_day:.2f} lifts/day</b> at "
            f"<b>{avg_cargo:,.0f} bbl/lift</b>. "
            f"Each vessel delivers ~<b>{bbl_per_vessel_day:,.0f} bbl/day</b> of throughput "
            f"at a {rt_hours:.0f}h round-trip cycle. "
            f"The throughput gap is equivalent to "
            f"<span class='hl-yellow'>{vessel_equivalents:.2f} vessel-equivalents</span>."
        ),
        metric=f"{total_spilled:,.0f} bbl lost ({spill_pct:.1f}% of production)",
    ))

    # ── Rec 1: additional daughter vessel ─────────────────────────────────────
    # Standard vessel sizes available in the fleet
    STANDARD_SIZES = [42_000, 43_000, 44_000, 63_000, 65_000, 85_000]

    # Raw bbl/vessel needed to close the gap
    raw_gap = throughput_gap / trips_per_day

    if vessel_equivalents <= 1.3:
        # One vessel can close the gap — find the right size
        best_size = min(STANDARD_SIZES, key=lambda s: abs(s - raw_gap))
        # Would it actually close the gap?
        prevented = best_size * trips_per_day * sim_days
        shortfall = max(0, total_spilled - prevented)
        coverage  = min(prevented / total_spilled * 100, 100)

        recs.append(dict(
            type="daughter_vessel", severity=sev,
            title=f"🚢 Add 1 × {best_size:,} bbl daughter vessel",
            body=(
                f"To eliminate the {spill_per_day:,.0f} bbl/day throughput gap, "
                f"one additional <span class='hl-blue'>{best_size:,} bbl daughter vessel</span> "
                f"is recommended. "
                f"At a ~{rt_hours:.0f}h round-trip cycle this vessel would deliver "
                f"~<span class='hl-green'>{best_size * trips_per_day:,.0f} bbl/day</span> "
                f"of extra lifting capacity, covering an estimated "
                f"<span class='hl-green'>{coverage:.0f}%</span> of the projected overflow. "
                + (f"A residual ~{shortfall:,.0f} bbl gap would remain — "
                   f"consider an <b>85,000 bbl vessel</b> for full coverage."
                   if shortfall > 5_000 else
                   "This vessel size is expected to fully eliminate the overflow.")
                + f" Permitted storage points for a new vessel should be confirmed "
                  f"against operational routing constraints."
            ),
            metric=f"{best_size:,} bbl vessel → ~{coverage:.0f}% overflow eliminated",
        ))
    else:
        # Need more than one vessel
        n    = int(vessel_equivalents) + 1
        size = 85_000
        recs.append(dict(
            type="daughter_vessel", severity=3,
            title=f"🚢 Add {n} × 85,000 bbl daughter vessels (significant shortfall)",
            body=(
                f"The throughput gap of <span class='hl-yellow'>{spill_per_day:,.0f} bbl/day</span> "
                f"is equivalent to <span class='hl-yellow'>{vessel_equivalents:.1f} vessel-equivalents</span>. "
                f"A minimum of <span class='hl-blue'>{n} additional 85,000 bbl vessels</span> "
                f"are required to close the gap. Combined they would add "
                f"~<span class='hl-green'>{n * size * trips_per_day:,.0f} bbl/day</span> "
                f"of lifting capacity. If adding {n} vessels is not operationally feasible, "
                f"a storage buffer tanker (see below) may bridge the gap while a permanent "
                f"fleet solution is arranged."
            ),
            metric=f"{n} × 85,000 bbl vessels needed",
        ))

    # ── Rec 2: storage buffer tanker ─────────────────────────────────────────
    if worst:
        top_store, top_spill = worst[0]
        top_pct = top_spill / total_spilled * 100

        # Count overflow hours at worst storage from timeline
        ovf_col     = f"{top_store}_Overflow_Accum_bbl"
        burst_hours = 0
        if ovf_col in tl_df.columns:
            burst_hours = int((tl_df[ovf_col].diff().fillna(0) > 0).sum() * 0.5)

        # Buffer is useful when:
        # (a) one point dominates — it's a local bottleneck not a fleet-wide gap
        # (b) overflow is concentrated in time — it's burst/daylight-window driven
        buffer_useful = top_pct >= 50 or burst_hours > 20

        if buffer_useful:
            # Size the buffer: absorb worst-day overflow × 1.5 safety factor
            worst_day_spill = top_spill / sim_days * 1.5
            BUFFER_SIZES    = [65_000, 85_000, 150_000, 270_000]
            buf_size        = min(BUFFER_SIZES, key=lambda s: abs(s - worst_day_spill))

            # Which vessels could load from a buffer at this point?
            perm_map = {
                "Chapel"  : list(mod.VESSEL_NAMES),
                "JasmineS": list(mod.VESSEL_NAMES),
                "Westmore": sorted(mod.WESTMORE_PERMITTED_VESSELS),
                "Duke"    : sorted(mod.DUKE_PERMITTED_VESSELS),
                "Starturn": sorted(mod.STARTURN_PERMITTED_VESSELS),
            }
            permitted = perm_map.get(top_store, [])

            recs.append(dict(
                type="storage_buffer", severity=2,
                title=f"🏗️ Alternative / complement: {buf_size:,} bbl storage buffer tanker at {top_store}",
                body=(
                    f"<b>{top_store}</b> accounts for "
                    f"<span class='hl-yellow'>{top_pct:.0f}%</span> of total overflow "
                    f"({top_spill:,.0f} bbl), with overflow occurring across roughly "
                    f"<b>{burst_hours}h</b> of burst windows. "
                    f"Mooring a <span class='hl-blue'>{buf_size:,} bbl storage buffer tanker</span> "
                    f"at {top_store} would absorb production during windows when no daughter vessel "
                    f"is available — daylight berthing restrictions, queue congestion, or "
                    f"dead-stock wait times. "
                    f"Unlike adding a daughter vessel, a buffer tanker requires no "
                    f"additional round-trip voyages to BIA; it extends hold time at source "
                    f"and is offloaded later by the existing fleet "
                    f"(permitted vessels: {', '.join(permitted)}). "
                    f"Best deployed <b>alongside</b> an additional daughter vessel "
                    f"for full coverage, or as a standalone measure if peak-burst overflow "
                    f"is the primary driver."
                ),
                metric=f"{buf_size:,} bbl buffer covers ~{burst_hours}h of overflow windows",
            ))
        else:
            recs.append(dict(
                type="storage_buffer", severity=1,
                title="🏗️ Storage buffer tanker: lower priority in this scenario",
                body=(
                    f"Overflow is distributed across multiple storage points "
                    f"({', '.join(k for k,_ in worst)}), indicating a fleet-wide "
                    f"throughput shortfall rather than a single-point bottleneck. "
                    f"A storage buffer tanker at one point would not address the root cause. "
                    f"Prioritise the additional daughter vessel recommendation above."
                ),
                metric=None,
            ))

    # ── Rec 3: operational levers ─────────────────────────────────────────────
    dead_stock_ratio = getattr(mod, "DEAD_STOCK_FACTOR", 1.75)
    recs.append(dict(
        type="operational", severity=1,
        title="📋 Operational levers — no new assets required",
        body=(
            f"<b>1. Reduce dead-stock threshold:</b> Currently set at "
            f"<b>×{dead_stock_ratio}</b> of cargo volume. Reducing to ×1.5 allows loading "
            f"to begin ~{(dead_stock_ratio-1.5)*avg_cargo/max(prod_chapel+prod_jasmines,1):.1f}h earlier per voyage, "
            f"increasing effective throughput without new vessels.<br>"
            f"<b>2. Extend operational window:</b> Berthing is restricted to "
            f"{getattr(mod,'BERTHING_START',6):02d}:00–{getattr(mod,'BERTHING_END',18):02d}:00. "
            f"Extending by even 1h each side adds ~{lifts_per_day * 2 / 24 * avg_cargo:,.0f} bbl/day "
            f"of additional capacity.<br>"
            f"<b>3. Reduce documentation time:</b> Current 4h documentation + 12h load = 16h at berth. "
            f"A 1h reduction in documentation time frees ~{1/rt_hours*24*avg_cargo:,.0f} bbl/vessel/day.<br>"
            f"<b>4. Review mother export timing:</b> Ensure export voyages are not blocking "
            f"discharge berths during peak loading windows at storage points."
        ),
        metric=None,
    ))

    return recs


def render_recommendations(recs):
    SEV_BORDER = {0:"#238636", 1:"#9e6a03", 2:"#bd561d", 3:"#6e1616"}
    SEV_BADGE  = {0:"✅ OK", 1:"ℹ️ LOW", 2:"⚠️ MEDIUM", 3:"🔴 HIGH"}
    for rec in recs:
        border  = SEV_BORDER.get(rec["severity"], "#e2e8f0")
        badge   = SEV_BADGE.get(rec["severity"], "")
        met_html = (
            f'<div class="rec-metric">Estimated impact: '
            f'<span class="hl-yellow">{rec["metric"]}</span></div>'
        ) if rec.get("metric") else ""
        st.markdown(f"""
        <div class="rec-card" style="border-color:{border}">
          <div style="display:flex;justify-content:space-between;align-items:flex-start">
            <div class="rec-title">{rec["title"]}</div>
            <div style="font-size:10px;color:#484f58;margin-left:12px;white-space:nowrap">{badge}</div>
          </div>
          <div class="rec-body">{rec["body"]}</div>
          {met_html}
        </div>
        """, unsafe_allow_html=True)


# =============================================================================
# ── CHARTS ────────────────────────────────────────────────────────────────────
# =============================================================================

_DARK = dict(paper_bgcolor="#ffffff", plot_bgcolor="#f8f9fb",
             font=dict(color="#1e293b"))
_MARGIN = dict(l=60, r=20, t=46, b=30)   # default margin — override per chart
_GRID = dict(gridcolor="#e2e8f0")


def chart_storage(tl_df):
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        subplot_titles=("SanBarth — Chapel & JasmineS",
                        "Sego / Awoba / Dawes — Westmore · Duke · Starturn"),
        vertical_spacing=0.1,
    )
    for name, col, dash in [("Chapel","Chapel_bbl","solid"),
                              ("JasmineS","JasmineS_bbl","dot")]:
        fig.add_trace(go.Scatter(x=tl_df.Time, y=tl_df[col], name=name,
            line=dict(color=STORAGE_COLORS[name], width=2, dash=dash)), row=1, col=1)
    for name, col in [("Westmore","Westmore_bbl"),
                       ("Duke","Duke_bbl"),("Starturn","Starturn_bbl")]:
        fig.add_trace(go.Scatter(x=tl_df.Time, y=tl_df[col], name=name,
            line=dict(color=STORAGE_COLORS[name], width=2)), row=2, col=1)
    fig.update_layout(height=460, margin=_MARGIN, **_DARK, legend=dict(bgcolor="#ffffff", bordercolor="#e2e8f0"))
    fig.update_yaxes(tickformat=",", **_GRID, title_text="bbl")
    fig.update_xaxes(**_GRID)
    return fig


def chart_overflow(tl_df):
    ovf_cols = [c for c in tl_df.columns if "Overflow_Accum" in c]
    if not ovf_cols:
        return None
    name_map = {
        "Chapel_Overflow_Accum_bbl":"Chapel","JasmineS_Overflow_Accum_bbl":"JasmineS",
        "Westmore_Overflow_Accum_bbl":"Westmore","Duke_Overflow_Accum_bbl":"Duke",
        "Starturn_Overflow_Accum_bbl":"Starturn","Ibom_Overflow_Accum_bbl":"Ibom",
    }
    fig = go.Figure()
    for col in ovf_cols:
        fig.add_trace(go.Scatter(x=tl_df.Time, y=tl_df[col],
                                  name=name_map.get(col, col),
                                  stackgroup="o", line=dict(width=1.5)))
    fig.update_layout(height=240, margin=_MARGIN, title="Cumulative Overflow — all storage points",
                      **_DARK, legend=dict(bgcolor="#ffffff"))
    fig.update_yaxes(tickformat=",", **_GRID, title_text="bbl")
    fig.update_xaxes(**_GRID)
    return fig


def chart_util(tl_df):
    items = [("Chapel","Chapel_bbl",270_000),("JasmineS","JasmineS_bbl",290_000),
             ("Westmore","Westmore_bbl",270_000),("Duke","Duke_bbl",90_000),
             ("Starturn","Starturn_bbl",70_000)]
    fig = go.Figure()
    for name, col, c in items:
        if col in tl_df.columns:
            fig.add_trace(go.Scatter(x=tl_df.Time, y=(tl_df[col]/c*100).round(1),
                name=name, line=dict(color=STORAGE_COLORS[name], width=1.8)))
    fig.add_hline(y=90, line=dict(color="#ef4444", dash="dash"),
                  annotation_text="90%", annotation_font_color="#ef4444")
    fig.update_layout(title="Storage Utilisation %", height=240, margin=_MARGIN, **_DARK,
                      yaxis=dict(**_GRID, title_text="%", range=[0,105]),
                      xaxis=_GRID, legend=dict(bgcolor="#ffffff"))
    return fig


def chart_mothers(tl_df, export_trigger, cap):
    fills = {"Bryanston" :"rgba(26,188,156,0.12)",
             "Alkebulan" :"rgba(255,85,85,0.12)",
             "GreenEagle":"rgba(192,132,252,0.12)"}
    fig = go.Figure()
    for name, col in [("Bryanston","Bryanston_bbl"),
                       ("Alkebulan","Alkebulan_bbl"),
                       ("GreenEagle","GreenEagle_bbl")]:
        fig.add_trace(go.Scatter(x=tl_df.Time, y=tl_df[col], name=name,
            fill="tozeroy", fillcolor=fills[name],
            line=dict(color=MOTHER_COLORS[name], width=2)))
    fig.add_hline(y=export_trigger,
                  line=dict(color="#ff5555", dash="dash", width=1.5),
                  annotation_text=f"Export trigger ({export_trigger:,} bbl)",
                  annotation_font_color="#e74c3c")
    fig.add_hline(y=cap, line=dict(color="#7f1d1d", dash="dot"),
                  annotation_text=f"Capacity ({cap:,} bbl)",
                  annotation_font_color="#fca5a5")
    fig.update_layout(height=300, margin=_MARGIN, title="BIA Mother Vessels — Volume",
                      **_DARK, legend=dict(bgcolor="#ffffff"))
    fig.update_yaxes(tickformat=",", **_GRID, title_text="bbl")
    fig.update_xaxes(**_GRID)
    return fig


def chart_gantt(tl_df, vessel_names, log_df=None):
    """
    Proper continuous-span Gantt.
    Each block of consecutive same-category status slots is collapsed into a
    single horizontal bar, making the chart readable across any horizon.
    """
    _LOAD_ST   = {"LOADING","BERTHING_A","HOSE_CONNECT_A","CAST_OFF","DOCUMENTING",
                  "WAITING_CAST_OFF"}
    _SAIL_OUT  = {"SAILING_AB","SAILING_CROSS_BW_AC","SAILING_BW_TO_FWY","SAILING_AB_LEG2",
                  "SAILING_D_CHANNEL","SAILING_CH_TO_BW_OUT","SAILING_CROSS_BW_OUT",
                  "SAILING_B_TO_F"}
    _BIA_ST    = {"DISCHARGING","BERTHING_B","HOSE_CONNECT_B","CAST_OFF_B","IDLE_B","WAITING_CAST_OFF"}
    _RETURN_ST = {"SAILING_BA","SAILING_BW_TO_A","SAILING_B_TO_FWY","SAILING_FWY_TO_BW","SAILING_CROSS_BW_IN_AC","SAILING_B_TO_BW_IN","SAILING_CROSS_BW_IN",
                  "SAILING_BW_TO_CH_IN","SAILING_CH_TO_D"}
    _WAIT_ST   = {"WAITING_STOCK","WAITING_DEAD_STOCK","WAITING_BERTH_A",
                  "WAITING_BERTH_B","WAITING_MOTHER_RETURN","WAITING_MOTHER_CAPACITY",
                  "WAITING_RETURN_STOCK","WAITING_FAIRWAY","WAITING_TIDAL",
                  "WAITING_DAYLIGHT"}
    _IBOM_ST   = {"PF_LOADING","PF_SWAP"}

    # Activity category -> display label, color
    _CATS = {
        "Loading"   : ("#2ecc71", "⛽ Loading"),
        "Outbound"  : ("#3b82f6", "🚢 Outbound (→BIA)"),
        "At BIA"    : ("#a855f7", "⚓ At BIA / Discharging"),
        "Returning" : ("#14b8a6", "↩️ Returning"),
        "Waiting"   : ("#f59e0b", "⏳ Waiting"),
        "Ibom"      : ("#f97316", "🛢️ Ibom Offshore"),
        "Idle"      : ("#475569", "💤 Idle"),
    }

    def _cat(st):
        if st in _LOAD_ST:   return "Loading"
        if st in _SAIL_OUT:  return "Outbound"
        if st in _BIA_ST:    return "At BIA"
        if st in _RETURN_ST: return "Returning"
        if st in _WAIT_ST:   return "Waiting"
        if st in _IBOM_ST:   return "Ibom"
        return "Idle"

    fig = go.Figure()
    vessels_ordered = list(reversed(vessel_names))   # top vessel = first in list
    y_pos = {n: i for i, n in enumerate(vessels_ordered)}
    _legend_added = set()
    SLOTS_PER_DAY = 48  # 30-min intervals

    for vn in vessel_names:
        if vn not in tl_df.columns:
            continue
        vc  = VESSEL_COLORS.get(vn, "#95a5a6")
        sub = tl_df[["Day", "Time", vn]].dropna(subset=[vn]).copy()
        if sub.empty:
            continue

        sub["xf"]  = (sub["Day"] - 1) + sub["Time"].apply(
            lambda d: (d.hour + d.minute / 60) / 24
        )
        sub["cat"] = sub[vn].apply(_cat)
        # Collapse consecutive same-category rows into single spans
        sub["blk"] = (sub["cat"] != sub["cat"].shift()).cumsum()

        yi = y_pos[vn]

        for (cat, _blk), grp in sub.groupby(["cat","blk"], sort=False):
            x0  = float(grp["xf"].iloc[0])
            x1  = float(grp["xf"].iloc[-1]) + (1.0 / SLOTS_PER_DAY)
            dur = x1 - x0
            col = vc if cat == "Loading" else _CATS[cat][0]

            rep_st  = grp[vn].mode().iloc[0]
            lbl     = STATUS_LABELS.get(rep_st, rep_st)
            d0, d1  = int(grp["Day"].iloc[0]), int(grp["Day"].iloc[-1])
            day_lbl = f"Day {d0}" if d0 == d1 else f"Day {d0}–{d1}"
            t0      = grp["Time"].iloc[0].strftime("%H:%M")
            t1      = grp["Time"].iloc[-1].strftime("%H:%M")
            hover   = (
                f"<b>{vn}</b>  ·  <b>{cat}</b><br>"
                f"{lbl}<br>"
                f"{day_lbl}  {t0} → {t1}<br>"
                f"Duration: {dur*24:.1f} h"
            )

            show_leg = cat not in _legend_added
            if show_leg:
                _legend_added.add(cat)

            fig.add_trace(go.Bar(
                x=[dur], y=[yi], base=[x0],
                orientation="h", width=0.60,
                marker=dict(
                    color=col, opacity=0.90,
                    line=dict(color="rgba(0,0,0,0.3)", width=0.6),
                ),
                name=_CATS[cat][1],
                legendgroup=cat,
                showlegend=show_leg,
                hovertemplate=hover + "<extra></extra>",
            ))

    # Day grid lines
    max_day = int(tl_df["Day"].max()) if not tl_df.empty else 1
    for d in range(0, max_day + 2):
        fig.add_vline(x=d, line=dict(color="rgba(255,255,255,0.07)", width=1))

    # Alternating week shading
    for w in range(0, (max_day // 7) + 2):
        if w % 2 == 1:
            fig.add_vrect(
                x0=w*7, x1=min((w+1)*7, max_day+1),
                fillcolor="rgba(255,255,255,0.025)", line_width=0, layer="below"
            )

    tick_step = 1 if max_day <= 10 else (2 if max_day <= 20 else (5 if max_day <= 60 else 7))
    tick_vals = list(range(0, max_day + 1, tick_step))

    fig.update_layout(
        height=max(360, 54 * len(vessel_names) + 90),
        barmode="overlay", bargap=0,
        plot_bgcolor="#0f1a35", paper_bgcolor="#0f1a35",
        margin=dict(l=110, r=20, t=50, b=50),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.01,
            xanchor="left", x=0,
            bgcolor="rgba(15,26,53,0.85)", bordercolor="#344d80", borderwidth=1,
            font=dict(color="#e2e8f0", size=11),
        ),
        xaxis=dict(
            title=dict(text="Simulation Day", font=dict(color="#94a3b8", size=12)),
            tickvals=tick_vals, ticktext=[str(v) for v in tick_vals],
            tickfont=dict(color="#94a3b8", size=10),
            range=[-0.5, max_day + 0.5],
            gridcolor="rgba(255,255,255,0.06)", zerolinecolor="rgba(255,255,255,0.1)",
            showgrid=True,
        ),
        yaxis=dict(
            tickvals=list(y_pos.values()),
            ticktext=[
                "<span style=\"color:{};font-weight:700\">{}</span>".format(
                    VESSEL_COLORS.get(n, "#e2e8f0"), n
                )
                for n in vessels_ordered
            ],
            tickfont=dict(size=12),
            gridcolor="rgba(255,255,255,0.07)",
            range=[-0.6, len(vessel_names) - 0.4],
            showgrid=True,
        ),
        hoverlabel=dict(
            bgcolor="#1e3a5f", bordercolor="#3b82f6",
            font=dict(color="#f1f5f9", size=12),
        ),
        font=dict(color="#e2e8f0"),
    )
    return fig


def chart_voyage_bars(log_df, vessel_names):
    ld = log_df[log_df.Event=="LOADING_START"].groupby("Vessel").size().reindex(vessel_names, fill_value=0)
    dc = log_df[log_df.Event=="DISCHARGE_START"].groupby("Vessel").size().reindex(vessel_names, fill_value=0)
    fig = go.Figure([
        go.Bar(name="Loadings",   x=vessel_names, y=ld.values, opacity=0.9,
               marker_color=[VESSEL_COLORS.get(n,"#aaa") for n in vessel_names]),
        go.Bar(name="Discharges", x=vessel_names, y=dc.values, opacity=0.9,
               marker_color=[_shade(VESSEL_COLORS.get(n,"#aaa"),0.55) for n in vessel_names]),
    ])
    fig.update_layout(barmode="group", title="Voyages per Vessel",
                      height=260, margin=_MARGIN, **_DARK, yaxis=_GRID, legend=dict(bgcolor="#ffffff"))
    return fig


# =============================================================================
# ── UI HELPERS ────────────────────────────────────────────────────────────────
# =============================================================================

def sec(title):
    st.markdown(f'<div class="sec-hdr">{title}</div>', unsafe_allow_html=True)


def kpi(label, value, sub=None):
    sub_html = f'<div class="kpi-sub">{sub}</div>' if sub else ""
    st.markdown(f"""
    <div class="kpi-card">
      <div class="kpi-label">{label}</div>
      <div class="kpi-value">{value}</div>
      {sub_html}
    </div>""", unsafe_allow_html=True)


def _int(v, fallback=0):
    """Safe int conversion."""
    try:
        return int(float(str(v).replace(",","")))
    except Exception:
        return fallback


# =============================================================================
# ── FLEET STATUS RENDERING ────────────────────────────────────────────────────
# =============================================================================

def render_fleet_cards(vessel_names, fleet_df, manual_states, mod):
    """Render one status card per daughter vessel in a 4-column grid."""
    _pt_a_cap_vessels = getattr(mod, "POINT_A_LOAD_CAP_VESSELS", {"Bedford", "Balham"})
    _pt_a_cap_bbl     = getattr(mod, "POINT_A_LOAD_CAP_BBL", 63_000)
    _sp_map           = getattr(mod, "STORAGE_POINT", {})
    cols = st.columns(4)
    for i, vn in enumerate(vessel_names):
        base  = VESSEL_COLORS.get(vn, "#95a5a6")
        vcap  = mod.VESSEL_CAPACITIES.get(vn, mod.DAUGHTER_CARGO_BBL)

        # Resolve data source: Sheets > manual > default
        if not fleet_df.empty and vn in fleet_df["vessel"].values:
            row    = fleet_df[fleet_df["vessel"]==vn].iloc[0]
            status = str(row.get("status","IDLE_A"))
            loc    = str(row.get("location","—"))
            cargo  = _int(row.get("cargo_bbl", 0))
            notes  = str(row.get("notes",""))
            badge  = '<span style="font-size:10px;color:#56d364">● live</span>'
        elif vn in manual_states:
            ms     = manual_states[vn]
            status = ms.get("status","IDLE_A")
            loc    = ms.get("location","—")
            cargo  = ms.get("cargo_bbl", 0)
            notes  = ms.get("notes","")
            badge  = '<span style="font-size:10px;color:#8b949e">● manual</span>'
        else:
            status = "IDLE_A"; loc = "—"; cargo = 0; notes = ""; badge = ""

        # Apply Point A loading cap for Bedford/Balham so the card shows
        # the operational ceiling (63k) not the physical capacity (85k)
        _loc_storage = (manual_states.get(vn, {}).get("location") or
                        (fleet_df[fleet_df["vessel"]==vn].iloc[0].get("location","")
                         if not fleet_df.empty and vn in fleet_df["vessel"].values else ""))
        _at_pt_a = _sp_map.get(_loc_storage) == "A"
        if vn in _pt_a_cap_vessels and _at_pt_a:
            vcap = min(vcap, _pt_a_cap_bbl)
        icon     = STATUS_ICONS.get(status, "❓")
        label    = STATUS_LABELS.get(status, status)
        pct      = max(4, min(100, int(cargo/vcap*100))) if vcap else 0
        bar_col  = vcolor(vn, status)
        notes_h  = (f'<div style="font-size:10px;color:#484f58;margin-top:3px">'
                    f'{notes}</div>') if notes else ""

        with cols[i % 4]:
            st.markdown(f"""
            <div class="vcard" style="border-left-color:{base}">
              <div style="display:flex;justify-content:space-between">
                <span class="vcard-name" style="color:{base}">{vn}</span>
                {badge}
              </div>
              <div class="vcard-status">{icon} {label}</div>
              <div class="vcard-loc">📍 {loc}</div>
              <div style="font-size:11px;color:#484f58;margin-bottom:5px">
                {cargo:,} / {vcap:,} bbl
              </div>
              <div class="vcard-bar-bg">
                <div class="vcard-bar-fg" style="background:{bar_col};width:{pct}%"></div>
              </div>
              {notes_h}
            </div>
            """, unsafe_allow_html=True)


def render_mother_cards(gs_vols, manual_mother, mod):
    """Render one status card per mother vessel in a 3-column row."""
    cols = st.columns(3)
    for i, (mn, mk) in enumerate([
        ("Bryanston","bryanston"), ("Alkebulan","alkebulan"), ("GreenEagle","greeneagle")
    ]):
        bbl   = gs_vols.get(mk) or manual_mother.get(mk, 0)
        cap   = mod.MOTHER_CAPACITY_BBL
        exp_t = mod.MOTHER_EXPORT_TRIGGER
        pct   = max(4, min(100, int(bbl/cap*100))) if cap else 0
        color = MOTHER_COLORS.get(mn,"#aaa")
        above = bbl >= exp_t
        flag  = ('<span style="color:#e74c3c;font-size:11px">▲ above export trigger</span>'
                 if above else
                 '<span style="color:#56d364;font-size:11px">▼ below export trigger</span>')

        with cols[i]:
            st.markdown(f"""
            <div class="vcard" style="border-left-color:{color}">
              <div class="vcard-name" style="color:{color}">🛢️ {mn}</div>
              <div style="font-size:22px;font-weight:700;color:#f0f6fc;margin:4px 0">
                {bbl:,} <span style="font-size:12px;color:#484f58">bbl</span>
              </div>
              <div style="font-size:11px;color:#484f58;margin-bottom:5px">
                {pct}% of {cap:,} bbl capacity
              </div>
              <div class="vcard-bar-bg" style="height:8px;margin-bottom:6px">
                <div class="vcard-bar-fg" style="background:{color};width:{pct}%;height:8px"></div>
              </div>
              {flag}
            </div>
            """, unsafe_allow_html=True)


# =============================================================================
# ── MAIN ──────────────────────────────────────────────────────────────────────
# =============================================================================

def main():
    mod = _load_mod_current()

    # ── Auto-clear cache when a new sim version is deployed ────────────
    _deployed_ver = getattr(mod, "SIM_VERSION", "unknown")
    if st.session_state.get("_sim_version_loaded") != _deployed_ver:
        st.cache_data.clear()
        st.session_state["_sim_version_loaded"] = _deployed_ver


    # Constants
    SCAP        = mod.STORAGE_CAPACITY_BY_NAME
    MOTHER_CAP  = int(mod.MOTHER_CAPACITY_BBL)
    EXPORT_TRIG = int(mod.MOTHER_EXPORT_TRIGGER)
    ALL_VESSELS = list(mod.VESSEL_NAMES)
    ALL_STATUS  = [code for _, items in STATUS_GROUPS
                   for code, _ in items]  # ordered by operational flow

    # ── Header ────────────────────────────────────────────────────────────────
    h1, h2 = st.columns([1, 11])
    with h1: st.markdown("# 🛢️")
    with h2:
        st.markdown("## Oil Tanker Daughter Vessel Operations — Live Dashboard")
        st.caption(
            "v5 · 8 vessels · 5 storage points SanBarth/Sego/Awoba/Dawes · "
            "3 mother vessels (Bryanston, Alkebulan, GreenEagle) · "
            "Ibom Bedford/Balham · Cawthorne Channel routing"
        )
    st.divider()

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### ⚙️ Simulation Parameters")
        _today = _dt.date.today()
        sim_start_date = st.date_input(
            "📅 Simulation Start Date",
            value=_today,
            min_value=_dt.date(2020, 1, 1),
            max_value=_dt.date(2035, 12, 31),
            format="DD/MM/YYYY",
            help=(
                "Day 1 of the forecast. Defaults to today.\n\n"
                "All event timestamps, chart axes and tidal "
                "lookups are anchored to this date."
            ),
            key="sim_start_date",
        )
        _dur_presets = {
            "1 day":    1,
            "3 days":   3,
            "1 week":   7,
            "2 weeks":  14,
            "1 month":  30,
            "2 months": 60,
            "3 months": 90,
            "6 months": 180,
            "9 months": 270,
            "12 months":365,
            "Custom…":  None,
        }
        _dur_sel = st.selectbox(
            "Simulation Duration",
            list(_dur_presets.keys()),
            index=4,   # default: 1 month
            key="dur_preset",
        )
        if _dur_presets[_dur_sel] is None:
            sim_days = st.number_input(
                "Custom days (1 – 365)", min_value=1, max_value=365,
                value=30, step=1, key="dur_custom"
            )
        else:
            sim_days = _dur_presets[_dur_sel]
        st.caption(f"▶ Running {sim_days} day{'s' if sim_days!=1 else ''} "
                   f"({'~'+str(round(sim_days/30.4,1))+' mo' if sim_days>=14 else str(sim_days)+'d'})")

        st.markdown("---")
        st.markdown("### Point B Startup Rule")
        startup_day_disable_point_b_priority = st.toggle(
            "Disable Point B auto-priority on startup day (Day 1 only)",
            value=True,
            help=(
                "Day 1 only: Point B assignment is manual via nominations below. "
                "Day 2 onward: strict automatic prioritization resumes."
            ),
            key="startup_day_disable_point_b_priority",
        )

        _mother_opts = list(getattr(mod, "MOTHER_NAMES", ["Bryanston", "Alkebulan", "GreenEagle"]))
        _nom_vessels = list(getattr(mod, "VESSEL_NAMES", []))
        _EXPORT_TOKEN  = "Export Operation"
        _daughter_opts = _nom_vessels + [_EXPORT_TOKEN]
        _mother_defaults = {
            "Bryanston":  ["Sherlock"],
            "Alkebulan":  ["Laphroaig"],
            "GreenEagle": ["Watson"],
        }

        st.markdown("**📦 Production Rates (bbl/hr)**")
        _r1c1, _r1c2 = st.columns(2)
        with _r1c1:
            prod_chapel   = st.number_input("Chapel (SanBarth)",   0, 5000, int(mod.PRODUCTION_RATE_BPH),         step=50, key="pr_chapel")
        with _r1c2:
            prod_jasmines = st.number_input("JasmineS (SanBarth)", 0, 5000, int(mod.PRODUCTION_RATE_BPH),         step=50, key="pr_jasmines")
        _r2c1, _r2c2 = st.columns(2)
        with _r2c1:
            prod_westmore = st.number_input("Westmore (Sego)", 0, 2000, int(mod.WESTMORE_PRODUCTION_RATE_BPH), step=50, key="pr_westmore")
        with _r2c2:
            prod_duke     = st.number_input("Duke (Awoba)",     0, 1000, int(mod.DUKE_PRODUCTION_RATE_BPH),     step=10, key="pr_duke")
        _r3c1, _r3c2 = st.columns(2)
        with _r3c1:
            prod_starturn = st.number_input("Starturn (Dawes)", 0, 500,  int(mod.STARTURN_PRODUCTION_RATE_BPH), step=10, key="pr_starturn")
        with _r3c2:
                        prod_ibom   = st.number_input("Ibom",         0, 500,
                        int(getattr(mod, "IBOM_LOAD_RATE_BPH",
                            getattr(mod, "POINT_F_LOAD_RATE_BPH", 165))),
                        step=10, key="pr_ibom")

        st.markdown("---")
        st.markdown("### Production Override Window")
        enable_prod_window_override = st.toggle(
            "Apply custom production rates for a date range",
            value=False,
            help=(
                "Use this to temporarily override storage production rates within a specific "
                "calendar window (inclusive). Example: set all rates to 0 bph from Mar 10 to Mar 18."
            ),
            key="enable_prod_window_override",
        )
        production_overrides = []
        if enable_prod_window_override:
            _pw_c1, _pw_c2 = st.columns(2)
            with _pw_c1:
                prod_window_start = st.date_input(
                    "Override start date",
                    value=sim_start_date,
                    key="prod_window_start",
                    format="DD/MM/YYYY",
                )
            with _pw_c2:
                prod_window_end = st.date_input(
                    "Override end date",
                    value=sim_start_date,
                    key="prod_window_end",
                    format="DD/MM/YYYY",
                )

            st.caption("Override rates (bbl/hr) applied only inside this date range.")
            _pw_r1c1, _pw_r1c2 = st.columns(2)
            with _pw_r1c1:
                ovr_chapel = st.number_input("Chapel override", 0, 5000, int(prod_chapel), step=50, key="ovr_pr_chapel")
            with _pw_r1c2:
                ovr_jasmines = st.number_input("JasmineS override", 0, 5000, int(prod_jasmines), step=50, key="ovr_pr_jasmines")
            _pw_r2c1, _pw_r2c2 = st.columns(2)
            with _pw_r2c1:
                ovr_westmore = st.number_input("Westmore override", 0, 2000, int(prod_westmore), step=50, key="ovr_pr_westmore")
            with _pw_r2c2:
                ovr_duke = st.number_input("Duke override", 0, 1000, int(prod_duke), step=10, key="ovr_pr_duke")
            ovr_starturn = st.number_input("Starturn override", 0, 500, int(prod_starturn), step=10, key="ovr_pr_starturn")

            _start_d = prod_window_start
            _end_d = prod_window_end
            if _end_d < _start_d:
                _start_d, _end_d = _end_d, _start_d
            production_overrides = [{
                "start_date": _start_d.isoformat(),
                "end_date": _end_d.isoformat(),
                "rates": {
                    "Chapel": ovr_chapel,
                    "JasmineS": ovr_jasmines,
                    "Westmore": ovr_westmore,
                    "Duke": ovr_duke,
                    "Starturn": ovr_starturn,
                },
            }]
            st.caption(
                f"Override active: {_start_d.strftime('%d/%m/%Y')} to {_end_d.strftime('%d/%m/%Y')}"
            )

        st.markdown("---")
        st.markdown("### 📊 Google Sheets Sync")
        st.caption("Connect your daily 08:00 ops sheet.")
        use_gs   = st.toggle("Enable Google Sheets")
        sheet_id = st.text_input("Sheet ID", disabled=not use_gs,
                                  placeholder="Long ID from sheet URL…")
        creds_f  = st.file_uploader("Service Account JSON", type=["json"],
                                     disabled=not use_gs)

        with st.expander("📋 Full sheet schema"):
            st.markdown("""
**The Google Sheet needs two tabs:**

---
**Tab 1 — name it exactly `volumes`**
One row per daily 08:00 update. Columns:
```
timestamp
chapel_bbl
jasmines_bbl
westmore_bbl
duke_bbl
starturn_bbl
bryanston_bbl
alkebulan_bbl
greeneagle_bbl
production_bph
sim_days
```
Example row:
```
2025-06-01 08:00 | 185000 | 210000 | 145000
| 45000 | 32000 | 320000 | 0 | 0 | 1700 | 30
```

---
**Tab 2 — name it exactly `fleet`**
One row per vessel per 08:00 update. Columns:
```
timestamp | vessel | status | location | cargo_bbl | notes
```
Valid status codes (copy exactly):
`IDLE_A` · `LOADING` · `SAILING_AB` · `SAILING_AB_LEG2`
`WAITING_BERTH_B` · `BERTHING_B` · `DISCHARGING` · `CAST_OFF_B`
`SAILING_BA` · `WAITING_RETURN_STOCK` · `PF_LOADING` · `PF_SWAP`
`SAILING_D_CHANNEL` · `WAITING_FAIRWAY` · `HOSE_CONNECT_A`
`HOSE_CONNECT_B` · `BERTHING_A` · `DOCUMENTING`
`WAITING_DEAD_STOCK` · `WAITING_CAST_OFF` · `CAST_OFF` · `WAITING_TIDAL`

Example rows:
```
2025-06-01 08:00 | Sherlock | DISCHARGING | Bryanston | 85000 |
2025-06-01 08:00 | Laphroaig | SAILING_AB | En Route SanBarth→BIA | 85000 |
2025-06-01 08:00 | Rathbone | LOADING | Chapel | 0 |
2025-06-01 08:00 | Woodstock | IDLE_A | Duke | 0 |
```

---
**Service account setup:**
1. [console.cloud.google.com](https://console.cloud.google.com) → Enable **Sheets API**
2. IAM → Service Accounts → Create → download JSON key
3. Share the sheet with the service-account email (Viewer)
4. Paste Sheet ID (from URL) + upload JSON key here
""")

        with st.expander("🚀 Deploy as public URL"):
            st.markdown("""
1. Push `tanker_app.py` + `tanker_simulation_v5.py` + `requirements.txt` to GitHub
2. [share.streamlit.io](https://share.streamlit.io) → New app → select `tanker_app.py`
3. Deploy — get a permanent public URL in ~2 min

**requirements.txt:**
```
streamlit>=1.32.0
pandas>=2.0.0
plotly>=5.18.0
gspread>=6.0.0
google-auth>=2.27.0
```
""")

        st.markdown("---")
        st.markdown("### 🔄 Auto-Refresh")
        auto_ref = st.toggle("Enable auto-refresh")
        ref_secs = st.slider("Interval (s)", 30, 600, 300, disabled=not auto_ref)

        st.markdown("---")
        st.markdown("### 🌊 Tidal Constraint")
        st.caption("Upload the tidal prediction CSV to enforce the breakwater crossing rule.")
        tide_file = st.file_uploader(
            "Tidal CSV  (Date · Time · Tide_Height_m)",
            type=["csv"], key="tide_uploader",
            help=(
                "Required columns: Date (DD/MM/YYYY) | Time (HH:MM) | Tide_Height_m\n\n"
                "Vessels cross the breakwater only if tide is above 1.6 m within daylight (06:00-18:00).\n"
                "• SanBarth→BIA: breakwater is 2 h from SanBarth\n"
                "• BIA→SanBarth: breakwater is 4 h from BIA"
            )
        )
        if tide_file is not None:
            st.success("✅ Tidal data loaded — breakwater constraint active")
            st.caption("🌊 Breakwater crossing: tide >1.6 m · daylight only (06:00-18:00)")
        else:
            st.info("ℹ️ No tidal file uploaded — daylight-only rule applies")

        st.markdown("---")
        st.markdown("### 🧠 Optimization Engine")
        st.caption(
            "Systematically sweep all valid parameter combinations and "
            "auto-select the highest-scoring configuration."
        )
        run_opt = st.toggle(
            "Run Optimizer",
            help=(
                "Evaluates all combinations across:\n"
                "• Dead-stock factor (4 values)\n"
                "• Ibom swap trigger (5 values)\n"
                "• Export sail window (3 values)\n"
                "• Berthing start/end (2×2 values)\n"
                "= 240 scenarios total.\n\n"
                "Results are cached — only reruns when parameters change."
            ),
        )
        if run_opt:
            st.caption("⏱️ ~60–120s first run · cached thereafter")
    st.caption("🔒 Daylight-operation constraints (06:00–18:00) are non-negotiable")

    # ── Load Google Sheets ────────────────────────────────────────────────────
    gs_vols    = {}
    fleet_df   = pd.DataFrame()
    creds_json = None

    if use_gs and sheet_id and creds_f:
        creds_json = creds_f.read().decode("utf-8")
        with st.spinner("Connecting to Google Sheets…"):
            gs_vols  = gs_load_volumes(sheet_id, creds_json)
            fleet_df = gs_load_fleet(sheet_id, creds_json)
        if gs_vols:
            ts = gs_vols.pop("_timestamp", "")
            st.sidebar.success(
                f"✅ Volumes: {len(gs_vols)} fields synced"
                + (f" (as of {ts})" if ts else ""))
        if not fleet_df.empty:
            st.sidebar.success(f"✅ Fleet: {len(fleet_df)} vessel rows synced")

    # ==========================================================================
    # ── SECTION 1: LIVE 08:00 FLEET STATUS ───────────────────────────────────
    # ==========================================================================
    sec("📡 Live Fleet Status — 08:00 Position Report")

    # Source badge
    if gs_vols or not fleet_df.empty:
        st.markdown(
            '<div class="alert-ok">🟢 Data sourced from Google Sheets. '
            'The simulation is seeded from these 08:00 field positions and volumes.</div>',
            unsafe_allow_html=True)
    else:
        st.markdown(
            '<div class="alert-info">ℹ️ No Google Sheets connection active. '
            'Enter today\'s 08:00 positions manually below, or enable Sheets in the sidebar.</div>',
            unsafe_allow_html=True)

    # ── Manual entry fallback ─────────────────────────────────────────────────
    manual_states = {}
    manual_mother = {}
    manual_mother_api = {}  # API gravity of stock on each mother vessel at 08:00

    sheets_has_all = (not fleet_df.empty
                      and all(vn in fleet_df["vessel"].values for vn in ALL_VESSELS))

    missing_vessels = [vn for vn in ALL_VESSELS
                       if fleet_df.empty or vn not in fleet_df["vessel"].values]

    # Always show storage + mother volume entry if not fully from Sheets
    if not gs_vols:
        with st.expander("✏️ Enter 08:00 storage & mother volumes",
                          expanded=not use_gs):
            st.markdown("**Storage Volumes at 08:00 (bbl)**")
            st.caption("You may enter volumes above capacity — the excess will be credited to overflow at simulation start.")
            sv = st.columns(5)
            manual_storage = {}
            for j,(sn,cv) in enumerate([
                ("Chapel",270_000),("JasmineS",290_000),("Westmore",270_000),
                ("Duke",90_000),("Starturn",70_000)
            ]):
                with sv[j]:
                    _entered = st.number_input(
                        sn, 0, cv * 2, cv//2, step=5_000, key=f"sv_{sn}",
                        help=f"Capacity: {cv:,} bbl. Values above capacity are treated as pre-existing overflow.")
                    manual_storage[sn] = _entered
                    if _entered > cv:
                        st.caption(f"⚠️ {_entered - cv:,} bbl over capacity → overflow")

            st.markdown("**Mother Vessel Volumes at 08:00 (bbl)**")
            mv = st.columns(3)
            for j,mn in enumerate(["Bryanston","Alkebulan","GreenEagle"]):
                with mv[j]:
                    manual_mother[mn.lower()] = st.number_input(
                        mn, 0, MOTHER_CAP, 0, step=10_000, key=f"mv_{mn}")

            st.markdown("**Mother Vessel Stock API Gravity at 08:00 (°API)**")
            st.caption("Set the API gravity of existing stock on each mother vessel. Ignored when stock is zero.")
            _mapi_ui_cols = st.columns(3)
            _mother_api_defaults = {"Bryanston": 32.00, "Alkebulan": 32.00, "GreenEagle": 32.00}
            manual_mother_api = {}
            for _j2, _mn2 in enumerate(["Bryanston", "Alkebulan", "GreenEagle"]):
                with _mapi_ui_cols[_j2]:
                    _stock_vol = manual_mother.get(_mn2.lower(), 0)
                    _api_val = st.number_input(
                        f"{_mn2} API°",
                        min_value=0.0, max_value=60.0,
                        value=float(_mother_api_defaults[_mn2]) if _stock_vol > 0 else 0.0,
                        step=0.1, format="%.2f",
                        key=f"mapi_{_mn2}",
                        help=f"API gravity of stock currently on {_mn2}. Only used when stock > 0 bbl.",
                        disabled=(_stock_vol == 0),
                    )
                    manual_mother_api[_mn2.lower()] = _api_val
                    if _stock_vol > 0:
                        st.caption(f"{_stock_vol:,} bbl \u00b7 {_api_val:.2f}\u00b0API")
    else:
        manual_storage = {}  # gs_vols will be used directly

    if missing_vessels:
        with st.expander(
            f"✏️ Enter 08:00 vessel positions ({len(missing_vessels)} vessels not in Sheets)",
            expanded=not use_gs
        ):
            st.caption(
                "Select each vessel's location and the simulation will automatically "
                "show only the statuses valid at that location.")

            # ── Zone legend ───────────────────────────────────────────────────
            _zone_html = " ".join(
                f'<span style="display:inline-flex;align-items:center;gap:4px;' +
                f'background:{_zc[1]}22;border:1px solid {_zc[1]}55;' +
                f'border-radius:5px;padding:2px 8px;font-size:11px;font-weight:600;' +
                f'color:{_zc[1]};margin:2px">{_zc[0]} {_zn}</span>'
                for _zn, _zc in ZONE_BADGE.items()
            )
            st.markdown(
                f'<div style="margin:6px 0 14px;line-height:2">{_zone_html}</div>',
                unsafe_allow_html=True)

            # ── Column headers ────────────────────────────────────────────────
            _hc = st.columns([2, 4, 3, 2])
            with _hc[0]: st.markdown(
                '<div style="font-size:11px;font-weight:700;color:#64748b;' +
                'text-transform:uppercase;letter-spacing:.06em;padding-bottom:4px">' +
                'Vessel</div>', unsafe_allow_html=True)
            with _hc[1]: st.markdown(
                '<div style="font-size:11px;font-weight:700;color:#64748b;' +
                'text-transform:uppercase;letter-spacing:.06em;padding-bottom:4px">' +
                'Location</div>', unsafe_allow_html=True)
            with _hc[2]: st.markdown(
                '<div style="font-size:11px;font-weight:700;color:#64748b;' +
                'text-transform:uppercase;letter-spacing:.06em;padding-bottom:4px">' +
                'Status (filtered by location)</div>', unsafe_allow_html=True)
            with _hc[3]: st.markdown(
                '<div style="font-size:11px;font-weight:700;color:#64748b;' +
                'text-transform:uppercase;letter-spacing:.06em;padding-bottom:4px">' +
                'Cargo (bbl)</div>', unsafe_allow_html=True)
            st.markdown('<hr style="margin:0 0 8px;border-color:#e2e8f0">', unsafe_allow_html=True)

            for vn in missing_vessels:
                vcap = mod.VESSEL_CAPACITIES.get(vn, mod.DAUGHTER_CARGO_BBL)
                vcol = VESSEL_COLORS.get(vn, "#aaa")

                # Watson loads from Point A (SanBarth) and Point C (Sego/Westmore)
                _is_watson       = (vn == "Watson")
                _is_ibom_vessel  = (vn in ("Bedford", "Balham"))
                _is_point_b_default = (vn in ("Sherlock", "Laphroaig", "Rathbone",
                                               "Bagshot", "Watson"))
                if _is_watson:
                    _loc_opts = [e["display"] for e in LOCATION_CATALOGUE
                                 if e["field_zone"] in ("SanBarth", "Sego", "BIA", "Transit")]
                elif _is_ibom_vessel:
                    # Bedford/Balham support SanBarth (Chapel/JasmineS) when Ibom
                    # swap is not active; so allow all locations except Sego/Awoba/Dawes
                    _loc_opts = [e["display"] for e in LOCATION_CATALOGUE
                                 if e["field_zone"] in ("SanBarth", "Ibom", "BIA", "Transit")]
                else:
                    _loc_opts  = LOC_DISPLAY_LIST

                rc = st.columns([2, 4, 3, 2])

                # ── Col 0: Vessel pill ────────────────────────────────────────
                with rc[0]:
                    _zone = LOC_BY_DISPLAY.get(
                        st.session_state.get(f"vl_{vn}", _loc_opts[0]), {}
                    ).get("field_zone", "Transit")
                    _zbadge, _zcol = ZONE_BADGE.get(_zone, ("⚪", "#94a3b8"))
                    st.markdown(
                        f'<div style="padding-top:32px">' +
                        f'<span class="pill" style="background:{vcol};color:#fff;' +
                        f'font-size:12px;padding:4px 12px">{vn}</span>' +
                        f'<br><span style="font-size:10px;color:{_zcol};font-weight:600;' +
                        f'margin-top:4px;display:block">{_zbadge} {_zone}</span>' +
                        f'</div>',
                        unsafe_allow_html=True)

                # ── Col 1: Location dropdown ──────────────────────────────────
                with rc[1]:
                    _default_loc_i = 0
                    # Default Ibom vessels to Ibom; Point B vessels to BIA Fairway;
                    # Woodstock — free dispatch (no hardcoded default location)
                    if _is_ibom_vessel:
                        try: _default_loc_i = _loc_opts.index("Ibom (Offshore Buoy)")
                        except ValueError: pass
                    elif _is_point_b_default:
                        try: _default_loc_i = _loc_opts.index("BIA — Fairway Buoy")
                        except ValueError: pass
                    _sel_loc = st.selectbox(
                        "Location", _loc_opts,
                        index=_default_loc_i,
                        key=f"vl_{vn}",
                        label_visibility="collapsed",
                        help=(
                            "Choose where this vessel is at 08:00. "
                            "The status list will update to show only valid options "
                            "for that location.\n\n"
                            "Watson loads from Point A (SanBarth/Chapel/JasmineS) and Point C (Sego/Westmore) to Point B."
                            if _is_watson else
                            "Bedford/Balham support SanBarth (Chapel/JasmineS) when Ibom "
                            "swap is not active. During an active swap trigger they are "
                            "held at Point A awaiting the Ibom handover."
                            if _is_ibom_vessel else
                            "Choose where this vessel is at 08:00. "
                            "The status list updates to show only valid options "
                            "for that location."
                        )
                    )
                    _loc_entry    = LOC_BY_DISPLAY[_sel_loc]
                    lc            = _loc_entry["sim_value"]
                    _loc_statuses = _loc_entry["statuses"]
                    # Zone badge under the dropdown
                    _z  = _loc_entry["field_zone"]
                    _zb, _zc2 = ZONE_BADGE.get(_z, ("⚪","#94a3b8"))
                    st.markdown(
                        f'<div style="font-size:10px;color:{_zc2};font-weight:600;' +
                        f'margin-top:2px">{_zb} {_z} zone</div>',
                        unsafe_allow_html=True)

                # ── Col 2: Status dropdown (location-filtered) ────────────────
                with rc[2]:
                    _stat_labels = [lbl for _, lbl in _loc_statuses]
                    _stat_codes  = {lbl: code for code, lbl in _loc_statuses}
                    # Default Point B vessels to "Waiting — return stock low";
                    # Woodstock — free dispatch (no hardcoded default status)
                    _stat_default_i = 0
                    if _is_point_b_default:
                        _wrs_lbl = "⏳ Waiting — return stock low"
                        if _wrs_lbl in _stat_labels:
                            _stat_default_i = _stat_labels.index(_wrs_lbl)
                    _sel_stat    = st.selectbox(
                        "Status", _stat_labels,
                        index=_stat_default_i,
                        key=f"vs_{vn}",
                        label_visibility="collapsed",
                        help="Only statuses valid at the selected location are shown."
                    )
                    st_v = _stat_codes.get(_sel_stat, _loc_statuses[0][0])

                # ── Col 3: Cargo ──────────────────────────────────────────────
                with rc[3]:
                    # Suggest full load for vessels that appear loaded
                    _cargo_default = (
                        vcap if "Discharging" in _sel_stat or "Loading" in _sel_stat
                        else 0
                    )
                    cg = st.number_input(
                        "Cargo", 0, vcap * 2, _cargo_default,
                        step=1_000, key=f"vc_{vn}",
                        label_visibility="collapsed",
                        help=f"Capacity: {vcap:,} bbl. Values above capacity are treated as pre-existing overflow."
                    )
                    if cg > vcap:
                        st.caption(f"⚠️ {cg - vcap:,} bbl over capacity → overflow")

                manual_states[vn] = {
                    "status":         st_v,
                    "cargo_bbl":      cg,
                    "location":       lc,
                    "target_storage": _loc_entry.get("target_storage"),
                    "target_mother":  _loc_entry.get("target_mother"),
                    "notes":          "",
                }
                st.markdown('<div style="height:4px"></div>', unsafe_allow_html=True)

    # ── Render fleet cards ────────────────────────────────────────────────────
    render_fleet_cards(ALL_VESSELS, fleet_df, manual_states, mod)

    st.markdown("<br>**🛢️ Mother Vessels at 08:00**", unsafe_allow_html=True)
    render_mother_cards(gs_vols, manual_mother, mod)

    # ==========================================================================
    # ── Resolve simulation parameters ─────────────────────────────────────────
    # ==========================================================================
    def _p(gs_key, man_dict, man_key, fallback):
        return gs_vols.get(gs_key) or man_dict.get(man_key, fallback)

    params = dict(
        sim_days      = gs_vols.get("sim_days",       sim_days),
        prod_chapel   = gs_vols.get("prod_chapel",    prod_chapel),
        prod_jasmines = gs_vols.get("prod_jasmines",  prod_jasmines),
        prod_westmore = gs_vols.get("prod_westmore",  prod_westmore),
        prod_duke     = gs_vols.get("prod_duke",      prod_duke),
        prod_starturn = gs_vols.get("prod_starturn",  prod_starturn),
        prod_ibom   = gs_vols.get("prod_ibom",    prod_ibom),
        chapel    = _p("chapel",    manual_storage, "Chapel",   SCAP["Chapel"]//2),
        jasmines  = _p("jasmines",  manual_storage, "JasmineS", SCAP["JasmineS"]//2),
        westmore  = _p("westmore",  manual_storage, "Westmore", SCAP["Westmore"]//2),
        duke      = _p("duke",      manual_storage, "Duke",     SCAP["Duke"]//2),
        starturn  = _p("starturn",  manual_storage, "Starturn", SCAP["Starturn"]//2),
        bryanston = _p("bryanston", manual_mother, "bryanston", 0),
        alkebulan = _p("alkebulan", manual_mother, "alkebulan", 0),
        greeneagle= _p("greeneagle",manual_mother, "greeneagle",0),
        bryanston_api  = gs_vols.get("bryanston_api",  manual_mother_api.get("bryanston",  0.0)),
        alkebulan_api  = gs_vols.get("alkebulan_api",  manual_mother_api.get("alkebulan",  0.0)),
        greeneagle_api = gs_vols.get("greeneagle_api", manual_mother_api.get("greeneagle", 0.0)),
    )

    # ── Pull confirmed positions from the vessel_positions page (pages/ companion) ─
    # vessel_positions.py writes to these session_state keys when the operator
    # presses "Confirm & Send to Simulation" on that page.
    _vp_states = st.session_state.get("vp_vessel_states")
    _vp_mvols  = st.session_state.get("vp_mother_vols", {})
    _vp_mapis  = st.session_state.get("vp_mother_apis", {})
    if _vp_states and st.session_state.get("vp_confirmed"):
        for _vn, _vd in _vp_states.items():
            # Only apply if not already supplied by Sheets or the manual entry form
            if _vn not in manual_states and (fleet_df.empty or _vn not in fleet_df["vessel"].values):
                manual_states[_vn] = _vd
        for _mk, _mv in _vp_mvols.items():
            if _mk not in gs_vols:
                manual_mother[_mk] = _mv
        for _mk, _ma in _vp_mapis.items():
            if _mk not in manual_mother_api:
                manual_mother_api[_mk] = _ma
        st.sidebar.success("🚢 Positions loaded from position entry page", icon="✅")

    # Build vessel_states_json
    vs_dict = {}
    for vn in ALL_VESSELS:
        # Manual UI entries must override any fleet sheet defaults.
        if vn in manual_states:
            ms = manual_states[vn]
            vs_dict[vn] = {
                "status":                ms.get("status", "IDLE_A"),
                "cargo_bbl":             ms.get("cargo_bbl", 0),
                "already_transferred_bbl": ms.get("already_transferred_bbl", 0),
                "location":              ms.get("location"),
                "target_storage":        ms.get("target_storage"),
                "target_mother":         ms.get("target_mother"),
            }
        elif not fleet_df.empty and vn in fleet_df["vessel"].values:
            row = fleet_df[fleet_df["vessel"]==vn].iloc[0]
            vs_dict[vn] = {"status": str(row.get("status","IDLE_A")),
                           "cargo_bbl": _int(row.get("cargo_bbl",0))}
    vessel_states_json = json.dumps(vs_dict) if vs_dict else None

    # ==========================================================================
    # ── Point B Startup Nominations (main area — needs full width) ────────────
    # ==========================================================================
    startup_day_manual_nominations = {}   # {vessel → mother}
    mother_export_seed             = {}   # {mother → days}
    with st.expander("📋 Startup Day Manual Point B Nominations", expanded=startup_day_disable_point_b_priority):
        st.caption(
            "For each mother vessel, select which daughters discharge to her on Day 1. "
            "Choose **Export Operation** to mark the mother as away at export for a custom number of days."
        )
        for _mn in _mother_opts:
            _sel_daughters = st.multiselect(
                f"🛢️ {_mn}",
                options=_daughter_opts,
                default=[d for d in _mother_defaults.get(_mn, []) if d in _daughter_opts],
                key=f"startup_nom_mother_{_mn}",
                placeholder="Select daughters or Export Operation…",
            )
            if _EXPORT_TOKEN in _sel_daughters:
                _exp_days = st.number_input(
                    f"{_mn} — days at export",
                    min_value=1, max_value=30, value=3, step=1,
                    key=f"startup_export_days_{_mn}",
                    help=f"Number of days {_mn} is away at export from t=0, blocking daughter berthing.",
                )
                mother_export_seed[_mn] = int(_exp_days)
            for _vn in _sel_daughters:
                if _vn != _EXPORT_TOKEN:
                    startup_day_manual_nominations[_vn] = _mn

    enable_point_b_startup_seed = st.toggle(
        "Validation Seed: Use startup manual nominations (full-load at BIA)",
        value=False,
        help=(
            "When enabled, every daughter nominated above starts at t=0 in "
            "HOSE_CONNECT_B with full cargo assigned to the selected mother. "
            "Mothers marked 'Export Operation' are blocked for the given number "
            "of days before accepting new daughters."
        ),
        key="enable_point_b_startup_seed",
    )
    point_b_startup_seed = {}
    if enable_point_b_startup_seed:
        point_b_startup_seed = dict(startup_day_manual_nominations)
        if not point_b_startup_seed and not mother_export_seed:
            st.warning(
                "Validation seed enabled, but no daughters or export operations have been configured.",
                icon="⚠️",
            )

    # ==========================================================================
    # ── Run simulation ────────────────────────────────────────────────────────
    # ==========================================================================
    _tide_bytes = tide_file.read() if tide_file is not None else None
    # Serialise to ISO string — ensures reliable @st.cache_data hashing
    _start_iso_str = sim_start_date.isoformat() if hasattr(sim_start_date, "isoformat") else _dt.date.today().isoformat()

    # Use selected optimizer scenario params if one was chosen, otherwise use best.
    # NOTE: run_optimizer() runs AFTER this point (in the display section), so best_pr
    # is not yet defined here. We persist it in session_state so it is available on the
    # next Streamlit rerun after the optimizer completes.
    _sel_scen    = st.session_state.get("selected_opt_scenario")
    _cached_best = st.session_state.get("_best_opt_params")
    if run_opt and _sel_scen:
        _opt_params_for_run = json.dumps({
            "dead_stock_factor":        _sel_scen["dead_stock_factor"],
            "ibom_trigger_bbl":         _sel_scen["ibom_trigger_bbl"],
            "export_sail_window_start": _sel_scen["export_sail_window_start"],
            "berthing_start":           _sel_scen["berthing_start"],
            "berthing_end":             _sel_scen["berthing_end"],
        })
    elif run_opt and _cached_best:
        # Use best params from the previous optimizer run (persisted across reruns)
        _opt_params_for_run = json.dumps(_cached_best)
    else:
        _opt_params_for_run = None

    _startup_nom_json = json.dumps(startup_day_manual_nominations) if startup_day_manual_nominations else None
    _point_b_seed_json = json.dumps(point_b_startup_seed) if point_b_startup_seed else None
    _mother_export_seed_json = json.dumps(mother_export_seed) if mother_export_seed else None
    _production_overrides_json = json.dumps(production_overrides) if production_overrides else None

    log_df, tl_df, S = run_sim(
        sim_days            = params["sim_days"],
        chapel              = params["chapel"],
        jasmines            = params["jasmines"],
        westmore            = params["westmore"],
        duke                = params["duke"],
        starturn            = params["starturn"],
        bryanston           = params["bryanston"],
        alkebulan           = params["alkebulan"],
        greeneagle          = params["greeneagle"],
        bryanston_api       = params["bryanston_api"],
        alkebulan_api       = params["alkebulan_api"],
        greeneagle_api      = params["greeneagle_api"],
        prod_chapel         = params["prod_chapel"],
        prod_jasmines       = params["prod_jasmines"],
        prod_westmore       = params["prod_westmore"],
        prod_duke           = params["prod_duke"],
        prod_starturn       = params["prod_starturn"],
        prod_ibom           = params["prod_ibom"],
        production_overrides_json = _production_overrides_json,
        vessel_states_json  = vessel_states_json,
        tide_csv_bytes      = _tide_bytes,
        sim_start_date      = _start_iso_str,
        _sim_version        = getattr(mod, "SIM_VERSION", ""),
        opt_params_json     = _opt_params_for_run,
        startup_day_disable_point_b_priority = startup_day_disable_point_b_priority,
        startup_day_manual_nominations_json  = _startup_nom_json,
        point_b_startup_seed_json            = _point_b_seed_json,
        mother_export_seed_json              = _mother_export_seed_json,
    )
    vnames = S["vessel_names"]


    # ==========================================================================
    # ── SECTION 1b: TODAY'S VESSEL SCHEDULE SUMMARY ───────────────────────────
    # ==========================================================================
    sec("📋 Today's Vessel Schedule Summary")


    try:
        _today_date = _dt.date.fromisoformat(_start_iso_str)
    except Exception:
        _today_date = _dt.date.today()

    # ── Status categorisation sets ─────────────────────────────────────────────
    _LOAD_ST    = {"LOADING","BERTHING_A","HOSE_CONNECT_A","WAITING_BERTH_A","IDLE_A",
                   "WAITING_STOCK","WAITING_DEAD_STOCK","WAITING_CAST_OFF","CAST_OFF",
                   "DOCUMENTING","PF_LOADING","PF_SWAP"}
    _RETURN_ST  = {"SAILING_BA","SAILING_BW_TO_A","SAILING_B_TO_FWY","SAILING_FWY_TO_BW","SAILING_CROSS_BW_IN_AC","SAILING_B_TO_BW_IN","SAILING_CROSS_BW_IN","SAILING_BW_TO_CH_IN","SAILING_CH_TO_D"}
    _TRANSIT_ST = {"SAILING_AB","SAILING_CROSS_BW_AC","SAILING_BW_TO_FWY","SAILING_AB_LEG2","WAITING_TIDAL","WAITING_DAYLIGHT","SAILING_D_CHANNEL","SAILING_CH_TO_BW_OUT","SAILING_CROSS_BW_OUT","WAITING_FAIRWAY","SAILING_B_TO_F"}
    _BIA_ST     = {"BERTHING_B","HOSE_CONNECT_B","DISCHARGING","CAST_OFF_B","IDLE_B",
                   "WAITING_BERTH_B","WAITING_MOTHER_RETURN","WAITING_MOTHER_CAPACITY",
                   "WAITING_RETURN_STOCK","WAITING_CAST_OFF"}

    # ── Get 08:00 vessel statuses from timeline ────────────────────────────────
    _d1_tl = tl_df[tl_df["Day"] == 1]
    # t=0 is now 08:00 — the very first timeline slot IS 08:00 (index 0)
    _t08_idx = 0
    _t08     = _d1_tl.iloc[_t08_idx] if not _d1_tl.empty else None

    _d1_log  = log_df[log_df["Day"] == 1]

    def _st08(vn):
        if _t08 is not None and vn in _t08.index:
            return str(_t08[vn])
        return "IDLE_A"

    def _pcargo(detail):
        m = re.search(r"([\d,]+) bbl", detail)
        return int(m.group(1).replace(",", "")) if m else 0

    def _pstorage_from_detail(detail):
        m = re.search(r"\| (\w+):", detail)
        return m.group(1) if m else "?"

    def _status_short(st):
        _map = {
            "LOADING": "Loading", "HOSE_CONNECT_A": "Hose Connect",
            "BERTHING_A": "Berthing", "WAITING_BERTH_A": "Waiting Berth",
            "IDLE_A": "Idle", "DOCUMENTING": "Documentation",
            "WAITING_CAST_OFF": "Awaiting Cast-off", "CAST_OFF": "Cast off",
            "WAITING_STOCK": "Waiting (Low Stock)", "WAITING_DEAD_STOCK": "Waiting (Dead Stock)",
            "PF_LOADING": "Loading (Ibom)", "PF_SWAP": "Vessel Swap",
            "SAILING_AB":           "Point A/C → Breakwater (1.5h)",
            "SAILING_CROSS_BW_AC":  "Crossing Breakwater",
            "SAILING_BW_TO_FWY":    "Breakwater → Fairway (2h)",
            "SAILING_AB_LEG2":      "Fairway → BIA (2h)",
            "SAILING_B_TO_FWY":     "BIA → Fairway (2h)",
            "SAILING_FWY_TO_BW":    "Fairway → Breakwater (2h)",
            "SAILING_CROSS_BW_IN_AC": "Crossing Breakwater",
            "SAILING_BW_TO_A":      "Breakwater → Point A/C (1.5h)",
            "WAITING_TIDAL": "Waiting (Tidal)", "WAITING_DAYLIGHT": "Waiting (Daylight)",
            "WAITING_FAIRWAY": "Holding Fairway", "SAILING_BA": "Returning",
            "BERTHING_B": "Berthing", "HOSE_CONNECT_B": "Hose Connect",
            "DISCHARGING": "Discharging", "CAST_OFF_B": "Cast off",
            "IDLE_B": "Idle at Mother", "WAITING_BERTH_B": "Waiting Berth",
            "WAITING_MOTHER_RETURN": "Waiting (Mother Away)",
            "WAITING_MOTHER_CAPACITY": "Waiting (Mother Full)",
            "WAITING_CAST_OFF": "Waiting — Night Cast-off Hold",
            "SAILING_B_TO_F":        "Sailing BIA → Ibom (swap)",
            "SAILING_D_CHANNEL":    "Awoba → Channel (3h)",
            "SAILING_CH_TO_BW_OUT": "Channel → Breakwater (1h)",
            "SAILING_CROSS_BW_OUT": "Crossing Breakwater",
            "SAILING_B_TO_BW_IN":   "BIA → Breakwater (1.5h)",
            "SAILING_CROSS_BW_IN":  "Crossing Breakwater",
            "SAILING_BW_TO_CH_IN":  "Breakwater → Channel (1h)",
            "SAILING_CH_TO_D":      "Channel → Point D (3h)",
        }
        return _map.get(st, st.replace("_", " ").title())

    # ── Build four section lists ───────────────────────────────────────────────
    _loading, _returning, _transit, _discharging = [], [], [], []

    for _vn in ALL_VESSELS:
        _st = _st08(_vn)

        # Loading events today
        _lev = _d1_log[(_d1_log["Vessel"] == _vn) & (_d1_log["Event"] == "LOADING_START")]
        _bev = _d1_log[(_d1_log["Vessel"] == _vn) & (_d1_log["Event"].isin(["BERTHING_START_A","WAITING_BERTH_A"]))]
        # Mother assignment — first try MOTHER_PRIORITY_ASSIGNMENT (normal voyage),
        # then fall back to HOSE_CONNECTION_START_B / DISCHARGE_START for vessels
        # that start the sim already alongside a mother (Bagshot, Watson).
        _mev = log_df[( log_df["Vessel"] == _vn) & ( log_df["Event"] == "MOTHER_PRIORITY_ASSIGNMENT")]
        if _mev.empty:
            _mev = log_df[(log_df["Vessel"] == _vn) &
                          (log_df["Event"].isin(["HOSE_CONNECTION_START_B", "DISCHARGE_START"]))]
        # Return allocation (first in whole log)
        _rev = log_df[( log_df["Vessel"] == _vn) & ( log_df["Event"] == "RETURN_POINT_ALLOCATED")]
        # Fairway ETA (first in whole log)
        _fev = log_df[( log_df["Vessel"] == _vn) & ( log_df["Event"] == "ARRIVED_FAIRWAY")]
        # Discharge events today
        _dev = _d1_log[(_d1_log["Vessel"] == _vn) & (_d1_log["Event"] == "DISCHARGE_START")]

        def _vapi_at08(vn):
            """Return vessel cargo API at 08:00 from timeline, or 0.0."""
            if _t08 is not None:
                _col = f"{vn}_api"
                if _col in _t08.index:
                    return round(float(_t08[_col]), 2)
            return 0.0

        if _st in _LOAD_ST:
            _storage = "?"
            _cargo   = 0
            _slabel  = _status_short(_st)

            # Primary: LOADING_START event on Day 1
            if not _lev.empty:
                _d = _lev.iloc[0]["Detail"]
                _storage = _pstorage_from_detail(_d)
                _cargo   = _pcargo(_d)
                _slabel  = "Loading"

            # Fallback A: LOADING_START anywhere in the full log before/at 08:00 Day 1
            # (catches vessels whose load started at t=0, i.e. Day 0)
            elif _st in {"LOADING", "DOCUMENTING", "CAST_OFF", "WAITING_CAST_OFF"}:
                _lev_all = log_df[
                    (log_df["Vessel"] == _vn) &
                    (log_df["Event"]  == "LOADING_START")
                ]
                # Most-recent load event at or before 08:00 Day 1
                _lev_before = _lev_all[
                    ((_lev_all["Day"] == 1) & (_lev_all["Hour"] <= "08:00")) |
                    (_lev_all["Day"] < 1)
                ]
                _lev_use = (_lev_before if not _lev_before.empty else _lev_all)
                if not _lev_use.empty:
                    _d = _lev_use.iloc[-1]["Detail"]
                    _storage = _pstorage_from_detail(_d)
                    _cargo   = _pcargo(_d)
                    _slabel  = "Loading"

            # Fallback B: BERTHING_START_A / WAITING_BERTH_A — Day 1 first, then full log
            if _storage == "?" and not _bev.empty:
                _d = _bev.iloc[0]["Detail"]
                _m = re.search(r"(?:at|window at) (\w+)", _d)
                if _m: _storage = _m.group(1)
                _slabel = _status_short(_bev.iloc[0]["Event"])

            if _storage == "?" and _st in {"BERTHING_A", "HOSE_CONNECT_A",
                                            "WAITING_BERTH_A", "WAITING_STOCK",
                                            "WAITING_DEAD_STOCK", "IDLE_A"}:
                _bev_all = log_df[
                    (log_df["Vessel"] == _vn) &
                    (log_df["Event"].isin(["BERTHING_START_A", "WAITING_BERTH_A"]))
                ]
                _bev_before = _bev_all[
                    ((_bev_all["Day"] == 1) & (_bev_all["Hour"] <= "08:00")) |
                    (_bev_all["Day"] < 1)
                ]
                _bev_use = (_bev_before if not _bev_before.empty else _bev_all)
                if not _bev_use.empty:
                    _d = _bev_use.iloc[-1]["Detail"]
                    _m = re.search(r"(?:at|window at) (\w+)", _d)
                    if _m: _storage = _m.group(1)
                    _slabel = _status_short(_bev_use.iloc[-1]["Event"])

            # Fallback C: PF_LOADING
            if _st == "PF_LOADING":
                _storage = "Ibom"
                _slabel  = "Loading (Ibom)"

            _loading.append({"vessel": _vn, "storage": _storage,
                              "status": _slabel, "cargo": _cargo,
                              "api": _vapi_at08(_vn)})

        elif _st in _RETURN_ST:
            _ret_stor = "?"
            _eta_s    = "TBD"

            # ── Find the most-recent RETURN_POINT_ALLOCATED before/at 08:00 Day 1 ──
            # The log is sorted by time; pick the last allocation ≤ 08:00 Day 1
            _rev_all = log_df[(log_df["Vessel"] == _vn) &
                               (log_df["Event"]  == "RETURN_POINT_ALLOCATED")]
            if not _rev_all.empty:
                # Filter to events whose time ≤ 08:00 Day 1 (i.e. Day==1, Hour<=08:00,
                # OR any earlier day) — take the last one
                _rev_before = _rev_all[
                    ((_rev_all["Day"] == 1) & (_rev_all["Hour"] <= "08:00")) |
                    (_rev_all["Day"] < 1)
                ]
                _rev_use = (_rev_before if not _rev_before.empty else _rev_all).iloc[-1]
                _d = _rev_use["Detail"]
                _m2 = re.search(r"eligible storage: (\w+)", _d)
                if _m2:
                    _ret_stor = _m2.group(1)
                else:
                    # Fallback: derive storage from target_point in detail
                    _mp = re.search(r"Allocated to Point ([A-F])", _d)
                    _pt_map = {"A": "Chapel", "C": "Westmore", "D": "Duke",
                               "E": "Starturn", "F": "Ibom"}
                    if _mp:
                        _ret_stor = _pt_map.get(_mp.group(1), "?")

            # ── ETA: find first ARRIVED_LOADING_POINT for this vessel AFTER 08:00 ──
            _eta_all = log_df[(log_df["Vessel"] == _vn) &
                               (log_df["Event"]  == "ARRIVED_LOADING_POINT")]
            _eta_fut = _eta_all[
                ((_eta_all["Day"] == 1) & (_eta_all["Hour"] > "08:00")) |
                (_eta_all["Day"] > 1)
            ]
            if not _eta_fut.empty:
                _eta_row  = _eta_fut.iloc[0]
                _eta_day  = int(_eta_row["Day"])
                _eta_time = _eta_row["Time"][11:16]
                # If arrival is on a future day, show day+time
                _eta_s = _eta_time if _eta_day == 1 else f"D{_eta_day} {_eta_time}"
            else:
                # ── Fallback: estimate from tl_df — find when status leaves _RETURN_ST ──
                _tl_v = tl_df[[c for c in tl_df.columns if c in (_vn,) or c in ("Day","Time","Hour")]]
                if _vn in tl_df.columns:
                    _ret_rows = tl_df[(tl_df["Day"] == 1) & (tl_df[_vn].isin(_RETURN_ST))]
                    if not _ret_rows.empty:
                        _arr_idx = _ret_rows.index[-1] + 1
                        if _arr_idx < len(tl_df):
                            _arr_row = tl_df.loc[_arr_idx]
                            _arr_day = int(_arr_row["Day"])
                            _eta_s   = (_arr_row["Time"].strftime("%H:%M")
                                        if _arr_day == 1
                                        else f"D{_arr_day} {_arr_row['Time'].strftime('%H:%M')}")

            _returning.append({"vessel": _vn, "storage": _ret_stor, "eta": _eta_s})

        elif _st in _TRANSIT_ST:
            _mother  = "TBD"
            _eta_bia = "TBD"
            if not _mev.empty:
                _detail = _mev.iloc[0]["Detail"]
                # MOTHER_PRIORITY_ASSIGNMENT: "...assigned to Bryanston..."
                # HOSE_CONNECTION_START_B:    "Hose connected at GreenEagle..."
                # DISCHARGE_START:            "Discharging N bbl | GreenEagle: ..."
                _m = (re.search(r"assigned to (\w+)", _detail)
                      or re.search(r"Hose connected at (\w+)", _detail)
                      or re.search(r"\|\s*(\w+):", _detail))
                if _m: _mother = _m.group(1)
            if not _fev.empty:
                _eta_bia = _fev.iloc[0]["Time"][11:16]
            _transit.append({"vessel": _vn, "mother": _mother,
                              "eta_bia": _eta_bia, "status": _status_short(_st)})

        elif _st in _BIA_ST:
            _mother = "?"
            if not _mev.empty:
                _detail = _mev.iloc[0]["Detail"]
                # MOTHER_PRIORITY_ASSIGNMENT: "...assigned to Bryanston..."
                # HOSE_CONNECTION_START_B:    "Hose connected at GreenEagle..."
                # DISCHARGE_START:            "Discharging N bbl | GreenEagle: ..."
                _m = (re.search(r"assigned to (\w+)", _detail)
                      or re.search(r"Hose connected at (\w+)", _detail)
                      or re.search(r"\|\s*(\w+):", _detail))
                if _m: _mother = _m.group(1)
            _slabel = "Discharging" if not _dev.empty else _status_short(_st)
            _cargo  = _pcargo(_dev.iloc[0]["Detail"]) if not _dev.empty else 0
            _discharging.append({"vessel": _vn, "mother": _mother,
                                  "status": _slabel, "cargo": _cargo,
                                  "api": _vapi_at08(_vn)})

    # ── Colour helpers ─────────────────────────────────────────────────────────
    def _vc(vn):
        return VESSEL_COLORS.get(vn, "#94a3b8")

    def _mc(mn):
        return MOTHER_COLORS.get(mn, "#94a3b8")

    def _sc(sn):
        return STORAGE_COLORS.get(sn, "#94a3b8")

    def _kk(bbl):
        if not bbl: return ""
        if bbl >= 1000: return f"{bbl//1000}k bbl"
        return f"{bbl} bbl"

    # ── HTML render ────────────────────────────────────────────────────────────
    _ncols = max(len(_loading), len(_returning), len(_transit), len(_discharging), 5)

    def _pill(vn, extra=""):
        c = _vc(vn)
        return (f'<span style="display:inline-block;background:{c};color:#fff;'
                f'font-weight:700;font-size:11px;padding:3px 10px;border-radius:4px;'
                f'letter-spacing:.02em">{vn}</span>'
                + (f' <span style="font-size:10px;color:#374151;font-weight:500">{extra}</span>' if extra else ""))

    def _mpill(mn):
        c = _mc(mn)
        return (f'<span style="display:inline-block;background:{c}22;color:{c};'
                f'border:1.5px solid {c};font-weight:700;font-size:11px;'
                f'padding:3px 10px;border-radius:4px">{mn}</span>')

    def _spill(sn):
        c = _sc(sn)
        return (f'<span style="display:inline-block;background:{c}22;color:{c};'
                f'border:1.5px solid {c};font-weight:600;font-size:11px;'
                f'padding:2px 8px;border-radius:4px">{sn}</span>')

    def _badge(txt, bg="#e2e8f0", fg="#374151"):
        return (f'<span style="display:inline-block;background:{bg};color:{fg};'
                f'font-size:10px;font-weight:600;padding:2px 7px;'
                f'border-radius:3px;white-space:nowrap">{txt}</span>')

    def _tdv(vn):
        """Vessel name cell — coloured left border."""
        c = _vc(vn)
        return (f'<td style="border-left:4px solid {c};padding:6px 10px;'
                f'background:#fff;white-space:nowrap">'
                f'<span style="font-weight:700;font-size:12px;color:#0f172a">{vn}</span></td>')

    def _tde(content=""):
        return f'<td style="padding:6px 10px;background:#fff">{content}</td>'

    def _tde_alt(content=""):
        return f'<td style="padding:6px 10px;background:#f8f9fb">{content}</td>'

    # ── Section header style ───────────────────────────────────────────────────
    _SEC_STYLES = {
        "loading":    ("#1a6b3c", "#d1fae5", "#bbf7d0"),  # green
        "returning":  ("#92400e", "#fef3c7", "#fde68a"),  # amber
        "transit":    ("#1e3a8a", "#dbeafe", "#bfdbfe"),  # blue
        "discharging":("#5b21b6", "#ede9fe", "#ddd6fe"),  # purple
    }

    def _sec_hdr(label, key, cols):
        dark, light, mid = _SEC_STYLES[key]
        return (f'<th colspan="{cols}" style="background:{dark};color:#fff;'
                f'text-align:center;padding:7px 12px;font-size:12px;'
                f'font-weight:800;letter-spacing:.08em;text-transform:uppercase;'
                f'border:1px solid {dark}">{label}</th>')

    def _col_hdr(label, key):
        dark, light, mid = _SEC_STYLES[key]
        return (f'<th style="background:{mid};color:{dark};text-align:left;'
                f'padding:5px 10px;font-size:10px;font-weight:700;'
                f'letter-spacing:.06em;text-transform:uppercase;'
                f'border:1px solid #e2e8f0;white-space:nowrap">{label}</th>')

    # ── CSS ────────────────────────────────────────────────────────────────────
    _tss_css = """
<style>
.tss-wrap{overflow-x:auto;border-radius:8px;box-shadow:0 2px 12px rgba(0,0,0,.08);margin:4px 0 16px}
.tss-title{text-align:center;font-size:15px;font-weight:800;color:#0f172a;
           padding:10px 16px;background:linear-gradient(135deg,#f8fafc,#e2e8f0);
           border-bottom:2px solid #cbd5e1;letter-spacing:.04em}
.tss-table{border-collapse:collapse;width:100%;font-family:'Segoe UI',system-ui,sans-serif}
.tss-table td{vertical-align:middle;border:1px solid #e2e8f0;min-width:90px}
.tss-table tr:hover td{filter:brightness(.97)}
.tss-empty td{background:#f8f9fb!important}
.tss-divider{width:6px;background:#e2e8f0;padding:0!important;border:none!important}
</style>"""
    st.markdown(_tss_css, unsafe_allow_html=True)

    # ── Build the table ────────────────────────────────────────────────────────
    _rows = max(len(_loading), len(_transit), len(_discharging))
    _rows = max(_rows, 5)   # minimum 5 rows so empty sections show

    def _pad(lst, n):
        return lst + [None] * (n - len(lst))

    _L = _pad(_loading,    _rows)
    _T = _pad(_transit,    _rows)
    _D = _pad(_discharging, _rows)

    _html = ['<div class="tss-wrap">']
    _html.append(f'<div class="tss-title">📋 Today\'s Vessel Schedule Summary &nbsp;|&nbsp; '
                 f'<span style="font-size:12px;font-weight:600;color:#475569">'
                 f'{_today_date.strftime("%A, %-d %B %Y")}</span></div>')
    _html.append('<table class="tss-table">')

    # Row 1: section mega-headers
    _html.append(
        '<tr>'
        + _sec_hdr("🟢 Loading Plan",    "loading",    3)
        + '<td class="tss-divider"></td>'
        + _sec_hdr("🔵 Transit to BIA",  "transit",    3)
        + '<td class="tss-divider"></td>'
        + _sec_hdr("🟣 Discharging Plan","discharging",3)
        + '</tr>'
    )
    # Row 2: column sub-headers
    _html.append(
        '<tr>'
        + _col_hdr("Daughter Vessel", "loading")
        + _col_hdr("Storage",         "loading")
        + _col_hdr("Status",          "loading")
        + '<td class="tss-divider"></td>'
        + _col_hdr("Daughter Vessel", "transit")
        + _col_hdr("Mother Allocation","transit")
        + _col_hdr("ETA to BIA",      "transit")
        + '<td class="tss-divider"></td>'
        + _col_hdr("Daughter Vessel", "discharging")
        + _col_hdr("Mother Vessel",   "discharging")
        + _col_hdr("Status",          "discharging")
        + '</tr>'
    )

    # Data rows
    for _i in range(_rows):
        _l = _L[_i]
        _t = _T[_i]
        _d = _D[_i]
        _bg = "#fff" if _i % 2 == 0 else "#f8f9fb"

        def _cell(content, bg=_bg):
            return f'<td style="padding:7px 10px;background:{bg};vertical-align:middle;border:1px solid #e2e8f0">{content}</td>'

        def _vcell(vn, bg=_bg):
            c = _vc(vn)
            return (f'<td style="padding:7px 10px;background:{bg};border-left:4px solid {c};'
                    f'border-top:1px solid #e2e8f0;border-bottom:1px solid #e2e8f0;'
                    f'border-right:1px solid #e2e8f0;vertical-align:middle">'
                    f'<span style="font-weight:700;font-size:12px;color:#0f172a">{vn}</span></td>')

        _row = "<tr>"

        # ── Loading section ────────────────────────────────────────────────────
        if _l:
            _row += _vcell(_l["vessel"])
            _row += _cell(_spill(_l["storage"]) if _l["storage"] != "?" else "—")
            # Status badge
            _st_bg = {"Loading": "#d1fae5", "Hose Connect": "#fef9c3",
                      "Berthing": "#dbeafe", "Waiting Berth": "#fde8d8",
                      "Loading (Ibom)": "#d1fae5"}.get(_l["status"], "#f1f5f9")
            _st_fg = {"Loading": "#14532d", "Hose Connect": "#713f12",
                      "Berthing": "#1e3a8a", "Waiting Berth": "#9a3412",
                      "Loading (Ibom)": "#14532d"}.get(_l["status"], "#374151")
            _stxt = _l["status"]
            if _l["cargo"]: _stxt += f" | {_kk(_l['cargo'])}"
            if _l.get("api"): _stxt += f" | API {_l['api']:.2f}°"
            _row += _cell(_badge(_stxt, _st_bg, _st_fg))
        else:
            _row += _cell("") + _cell("") + _cell("")

        _row += '<td class="tss-divider" style="width:6px;background:#e2e8f0;border:none"></td>'

        # ── Transit section ────────────────────────────────────────────────────
        if _t:
            _row += _vcell(_t["vessel"])
            _mc_col = _mc(_t["mother"])
            _mother_disp = (_mpill(_t["mother"]) if _t["mother"] != "TBD"
                           else _badge("TBD", "#f1f5f9", "#64748b"))
            _row += _cell(_mother_disp)
            _row += _cell(_badge(_t["eta_bia"], "#dbeafe", "#1e3a8a") if _t["eta_bia"] != "TBD" else _badge("TBD", "#f1f5f9", "#64748b"))
        else:
            _row += _cell("") + _cell("") + _cell("")

        _row += '<td class="tss-divider" style="width:6px;background:#e2e8f0;border:none"></td>'

        # ── Discharging section ────────────────────────────────────────────────
        if _d:
            _row += _vcell(_d["vessel"])
            _row += _cell(_mpill(_d["mother"]) if _d["mother"] != "?" else "—")
            _ds_bg = {"Discharging": "#ede9fe", "Hose Connect": "#fef9c3",
                      "Berthing": "#dbeafe", "Waiting Berth": "#fde8d8",
                      "Idle at Mother": "#f0fdf4"}.get(_d["status"], "#f1f5f9")
            _ds_fg = {"Discharging": "#5b21b6", "Hose Connect": "#713f12",
                      "Berthing": "#1e3a8a", "Waiting Berth": "#9a3412",
                      "Idle at Mother": "#14532d"}.get(_d["status"], "#374151")
            _dstxt = _d["status"]
            if _d["cargo"]: _dstxt += f" | {_kk(_d['cargo'])}"
            if _d.get("api"): _dstxt += f" | API {_d['api']:.2f}°"
            _row += _cell(_badge(_dstxt, _ds_bg, _ds_fg))
        else:
            _row += _cell("") + _cell("") + _cell("")

        _row += "</tr>"
        _html.append(_row)

    _html.append("</table></div>")
    _tss_html = "\n".join(_html)
    st.markdown(_tss_html, unsafe_allow_html=True)

    # ── Quick legend row ───────────────────────────────────────────────────────
    _leg = '<div style="display:flex;flex-wrap:wrap;gap:6px;margin:0 0 12px;align-items:center">'
    _leg += '<span style="font-size:11px;font-weight:700;color:#475569">Vessels:</span>'
    for _vn2 in ALL_VESSELS:
        _c2 = _vc(_vn2)
        _leg += (f'<span style="background:{_c2};color:#fff;border-radius:4px;'
                 f'padding:2px 9px;font-size:10px;font-weight:700">{_vn2}</span>')
    _leg += '<span style="font-size:11px;font-weight:700;color:#475569;margin-left:10px">Storage:</span>'
    for _sn2, _sc2 in STORAGE_COLORS.items():
        _leg += (f'<span style="background:{_sc2}22;color:{_sc2};border:1px solid {_sc2};'
                 f'border-radius:4px;padding:2px 9px;font-size:10px;font-weight:700">{_sn2}</span>')
    _leg += '<span style="font-size:11px;font-weight:700;color:#475569;margin-left:10px">Mothers:</span>'
    for _mn2, _mc2 in MOTHER_COLORS.items():
        _leg += (f'<span style="background:{_mc2}22;color:{_mc2};border:1.5px solid {_mc2};'
                 f'border-radius:4px;padding:2px 9px;font-size:10px;font-weight:700">{_mn2}</span>')
    _leg += '</div>'
    st.markdown(_leg, unsafe_allow_html=True)

    # ── Export buttons ─────────────────────────────────────────────────────────
    _ex1, _ex2 = st.columns([1,1])

    # Self-contained HTML for download / print-to-image
    _full_tss = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>Today's Vessel Schedule — {_today_date.strftime('%d %b %Y')}</title>
<style>
body{{margin:20px;background:#fff;font-family:'Segoe UI',Arial,sans-serif}}
{_tss_css.replace('<style>','').replace('</style>','')}
.tss-table td,.tss-table th{{min-width:80px}}
</style>
</head><body>
{_tss_html}
<div style="margin-top:10px;font-size:9px;color:#94a3b8">
Generated {_dt.datetime.now().strftime('%Y-%m-%d %H:%M')} | Tanker Operations Simulation v5
</div>
</body></html>"""

    with _ex1:
        st.download_button(
            "📥 Download Schedule (HTML → open in browser to save as image/PDF)",
            data=_full_tss.encode("utf-8"),
            file_name=f"vessel_schedule_{_today_date.isoformat()}.html",
            mime="text/html",
            help="Download as HTML. Open in Chrome/Edge → Ctrl+P → Save as PDF, or use the browser screenshot tool for PNG."
        )
    with _ex2:
        # CSV of the schedule
        _sched_rows = []
        for _x in _loading:
            _sched_rows.append({"Section":"Loading Plan","Vessel":_x["vessel"],"Storage":_x["storage"],"Status":_x["status"],"Cargo_bbl":_x["cargo"],"Mother":"","ETA":""})
        for _x in _transit:
            _sched_rows.append({"Section":"Transit to BIA","Vessel":_x["vessel"],"Storage":"","Status":_x["status"],"Cargo_bbl":"","Mother":_x["mother"],"ETA":_x["eta_bia"]})
        for _x in _discharging:
            _sched_rows.append({"Section":"Discharging","Vessel":_x["vessel"],"Storage":"","Status":_x["status"],"Cargo_bbl":_x["cargo"],"Mother":_x["mother"],"ETA":""})
        st.download_button(
            "📥 Download Schedule (CSV)",
            data=pd.DataFrame(_sched_rows).to_csv(index=False).encode(),
            file_name=f"vessel_schedule_{_today_date.isoformat()}.csv",
            mime="text/csv"
        )


    # ==========================================================================
    # ── SECTION: TIDAL PREDICTION ─────────────────────────────────────────────
    # ==========================================================================
    sec("🌊 Tidal Prediction — Declared Daylight Tides")


    if _tide_bytes is None:
        st.info("ℹ️ No tidal file uploaded — upload a tidal CSV in the sidebar to see declared daylight tides and activate the breakwater constraint.")
    else:
        # Parse tide bytes directly — no sim module dependency
        try:
            _tide_min_m = 1.6
            _DSTART     = 6
            _DEND       = 18
            _SIM_HOUR_OFFSET = 8.0
            _sim_epoch  = _dt.datetime(_today_date.year, _today_date.month, _today_date.day, 8, 0)  # t=0 = 08:00
            _sim_days_t = params.get("sim_days", 14)

            def _parse_tide_bytes(raw_bytes, epoch_dt):
                """Parse raw tide CSV bytes → {abs_hour: height} dict."""
                text   = raw_bytes.decode("utf-8-sig", errors="replace")
                sample = text[:2048]
                delim  = "," if sample.count(",") >= sample.count(";") else ";"
                reader = csv.DictReader(io.StringIO(text), delimiter=delim)
                rows   = [{k.strip().lower().replace(" ","_"): v.strip()
                           for k, v in r.items()} for r in reader]
                if not rows:
                    return {}
                date_col   = next((k for k in rows[0] if "date" in k), None)
                time_col   = next((k for k in rows[0] if "time" in k), None)
                height_col = next((k for k in rows[0]
                                   if any(x in k for x in ("height","tide","level","_m"))), None)
                if not (date_col and time_col and height_col):
                    return {}
                raw_pts = {}
                for row in rows:
                    try:
                        ds = row[date_col]; ts = row[time_col]; hs = row[height_col]
                        if not hs: continue
                        if "/" in ds:
                            p = ds.split("/")
                            d = (_dt.datetime(int(p[2]),int(p[1]),int(p[0]))
                                 if len(p[2])==4
                                 else _dt.datetime(int(p[0]),int(p[1]),int(p[2])))
                        else:
                            d = _dt.datetime.fromisoformat(ds.split("T")[0])
                        hh, mm = int(ts[:2]), int(ts[3:5])
                        dt  = d.replace(hour=hh, minute=mm)
                        ht  = float(re.sub(r"[^0-9.\-]", "", hs))
                        diff = (dt - epoch_dt).total_seconds() / 3600.0
                        raw_pts[round(diff * 2) / 2] = ht
                    except Exception:
                        continue
                if not raw_pts:
                    return {}
                # Linear interpolation onto 0.5 h grid
                sk = sorted(raw_pts)
                full = {}
                _slot_start = int(sk[0] * 2)
                _slot_end = int(sk[-1] * 2) + 2
                for slot in [x * 0.5 for x in range(_slot_start, _slot_end)]:
                    if slot in raw_pts:
                        full[slot] = raw_pts[slot]
                    else:
                        lo = max((k for k in sk if k <= slot), default=None)
                        hi = min((k for k in sk if k >= slot), default=None)
                        if lo is not None and hi is not None and hi != lo:
                            f = (slot - lo) / (hi - lo)
                            full[slot] = raw_pts[lo] + f * (raw_pts[hi] - raw_pts[lo])
                        elif lo is not None:
                            full[slot] = raw_pts[lo]
                        elif hi is not None:
                            full[slot] = raw_pts[hi]
                return full

            _tide_tbl = _parse_tide_bytes(_tide_bytes, _sim_epoch)
            _tide_ok  = bool(_tide_tbl)

            if not _tide_ok:
                st.warning("⚠️ Tidal file uploaded but could not be parsed. Check column names: Date (DD/MM/YYYY) · Time (HH:MM) · Tide_Height_m")
            else:
                _sim_days_t = params.get("sim_days", 14)

                # ── Build daily summary ──────────────────────────────────────
                _daily_rows = []
                for _day in range(_sim_days_t):
                    _date_d    = _today_date + _dt.timedelta(days=_day)
                    _day_start = _day * 24.0 - _SIM_HOUR_OFFSET
                    _day_end   = _day_start + 24.0

                    # All half-hour slots for this calendar day
                    _slots = {h: v for h, v in _tide_tbl.items()
                              if _day_start <= h < _day_end}

                    if not _slots:
                        _daily_rows.append({
                            "Date": _date_d.strftime("%a %d %b"),
                            "High Tide": "—", "High Time": "—",
                            "Low Tide": "—", "Low Time": "—",
                            "Declared Daylight Tides (>1.6m)": "No tidal data for this day",
                            "Declared Tides": 0,
                        })
                        continue

                    # High and low tide
                    _peak_h    = max(_slots, key=_slots.get)
                    _trough_h  = min(_slots, key=_slots.get)
                    _peak_dt   = (_sim_epoch + _dt.timedelta(hours=_peak_h)).strftime("%H:%M")
                    _trough_dt = (_sim_epoch + _dt.timedelta(hours=_trough_h)).strftime("%H:%M")

                    _declared = []
                    for _h in sorted(_slots):
                        _hod = (_h + _SIM_HOUR_OFFSET) % 24
                        _hgt = _slots[_h]
                        if _DSTART <= _hod < _DEND and _hgt > _tide_min_m:
                            _tm = (_sim_epoch + _dt.timedelta(hours=_h)).strftime("%H:%M")
                            _declared.append(f"{_tm} ({_hgt:.2f} m)")

                    _declared_str = "  ·  ".join(_declared) if _declared else "❌ No declared daylight tide >1.6m"

                    _daily_rows.append({
                        "Date":   _date_d.strftime("%a %d %b"),
                        "High Tide": f"{_slots[_peak_h]:.2f} m",
                        "High Time": _peak_dt,
                        "Low Tide":  f"{_slots[_trough_h]:.2f} m",
                        "Low Time":  _trough_dt,
                        "Declared Daylight Tides (>1.6m)": _declared_str,
                        "Declared Tides": len(_declared),
                    })

                _tide_df = pd.DataFrame(_daily_rows)

                # ── Metric strip ────────────────────────────────────────────
                _no_cross_days = (_tide_df["Declared Tides"] == 0).sum()
                _avg_declared  = _tide_df["Declared Tides"].mean()
                _total_declared = _tide_df["Declared Tides"].sum()
                _tc1, _tc2, _tc3, _tc4 = st.columns(4)
                with _tc1: kpi("Sim Days Covered", f'{len(_daily_rows)}')
                with _tc2: kpi("Avg Declared Tides/Day",  f'{_avg_declared:.1f}')
                with _tc3: kpi("Total Declared Tides",    f'{int(_total_declared)}')
                with _tc4: kpi("Restricted Days",
                               f'{_no_cross_days}',
                               sub="days with no declared daylight tide" if _no_cross_days else "✅ all days have declared tides")

                # ── Threshold reminder ───────────────────────────────────────
                st.markdown(
                    f'<div style="background:#f0f9ff;border:1px solid #3b82f6;border-radius:8px;'
                    f'padding:10px 16px;margin:8px 0 12px;font-size:12px;color:#1e40af">'
                    f'🌊 <b>Breakwater crossing rule:</b> vessels may only depart when tide is '
                    f'<b>>{_tide_min_m:.1f} m</b> '
                    f'AND within daylight (<b>{_DSTART:02d}:00–{_DEND:02d}:00</b>). '
                    f'Only daylight tide points above threshold are declared. '
                    f'The simulation enforces this for all outbound departures from SanBarth '
                    f'and return sailings from BIA.</div>',
                    unsafe_allow_html=True)

                # ── Daily table ──────────────────────────────────────────────
                def _tide_row_color(row):
                    # Use the declared tide count stored in a parallel list by index
                    idx = row.name
                    w = _daily_rows[idx]["Declared Tides"] if idx < len(_daily_rows) else 1
                    if w == 0:
                        return ['background-color:#fef2f2;color:#991b1b'] * len(row)
                    elif w == 1:
                        return ['background-color:#fef9c3;color:#713f12'] * len(row)
                    else:
                        return ['background-color:#f0fdf4;color:#14532d'] * len(row)

                _tide_display_df = _tide_df.drop(columns=["Declared Tides"]).reset_index(drop=True)
                _tide_display = _tide_display_df.style.apply(_tide_row_color, axis=1)
                st.dataframe(_tide_display, hide_index=True, use_container_width=True)

                # ── Intraday chart for selected day ──────────────────────────
                st.markdown("**📈 Intraday tidal profile — select a day to inspect:**")
                _day_opts   = [r["Date"] for r in _daily_rows]
                _sel_day_lbl = st.selectbox("Day", _day_opts, key="tide_day_sel",
                                            label_visibility="collapsed")
                _sel_day_idx = _day_opts.index(_sel_day_lbl)
                _sd_start    = _sel_day_idx * 24.0 - _SIM_HOUR_OFFSET
                _sd_end      = _sd_start + 24.0
                _sd_slots    = {h: v for h, v in _tide_tbl.items()
                                if _sd_start <= h < _sd_end}

                if _sd_slots:
                    import plotly.graph_objects as _pgo
                    _sd_hours  = sorted(_sd_slots)
                    _sd_hods   = [((h + _SIM_HOUR_OFFSET) % 24) for h in _sd_hours]
                    _sd_heights = [_sd_slots[h] for h in _sd_hours]
                    _sd_labels  = [f"{int(h):02d}:{int((h%1)*60):02d}" for h in _sd_hods]

                    _fig_t = _pgo.Figure()
                    # Tide curve
                    _fig_t.add_trace(_pgo.Scatter(
                        x=_sd_hods, y=_sd_heights,
                        mode="lines", name="Tide height (m)",
                        line=dict(color="#3b82f6", width=2.5),
                        hovertemplate="%{text}: %{y:.2f} m<extra></extra>",
                        text=_sd_labels,
                    ))
                    # Threshold line
                    _fig_t.add_hline(
                        y=_tide_min_m, line_dash="dash",
                        line_color="#ef4444", line_width=1.5,
                        annotation_text=f"Min crossing {_tide_min_m:.1f} m",
                        annotation_position="bottom right",
                        annotation_font_color="#ef4444",
                    )
                    # Daylight shading
                    _fig_t.add_vrect(x0=_DSTART, x1=_DEND,
                        fillcolor="rgba(253,224,71,0.12)", line_width=0,
                        annotation_text="Daylight window", annotation_position="top left",
                        annotation_font_color="#b45309", annotation_font_size=10)
                    # Valid crossing zones (tide > threshold AND daylight) — green fill
                    _in_zone = False
                    _zone_x0 = None
                    for _i, (_hod, _hgt) in enumerate(zip(_sd_hods, _sd_heights)):
                        _ok = _hgt > _tide_min_m and _DSTART <= _hod < _DEND
                        if _ok and not _in_zone:
                            _zone_x0 = _hod; _in_zone = True
                        elif not _ok and _in_zone:
                            _fig_t.add_vrect(x0=_zone_x0, x1=_hod,
                                fillcolor="rgba(34,197,94,0.18)", line_width=0)
                            _in_zone = False
                    if _in_zone:
                        _fig_t.add_vrect(x0=_zone_x0, x1=_DEND,
                            fillcolor="rgba(34,197,94,0.18)", line_width=0)

                    _fig_t.update_layout(
                        height=280, margin=dict(l=40, r=20, t=30, b=40),
                        paper_bgcolor="#0f1a35", plot_bgcolor="#0f1a35",
                        font=dict(color="#cbd5e1", size=11),
                        xaxis=dict(title="Hour of day", tickmode="linear",
                                   tick0=0, dtick=2, gridcolor="#1e2d4a",
                                   range=[0, 24]),
                        yaxis=dict(title="Height (m)", gridcolor="#1e2d4a"),
                        legend=dict(bgcolor="rgba(0,0,0,0)", font_size=10),
                        showlegend=True,
                    )
                    st.plotly_chart(_fig_t, use_container_width=True, config={"displayModeBar": False})
                    # Summary for selected day
                    _sel_row = _daily_rows[_sel_day_idx]
                    _cwin_txt = _sel_row["Declared Daylight Tides (>1.6m)"]
                    st.caption(
                        f"**{_sel_day_lbl}** — High: {_sel_row['High Tide']} at {_sel_row['High Time']} · "
                        f"Low: {_sel_row['Low Tide']} at {_sel_row['Low Time']} · "
                        f"Declared daylight tides: {_cwin_txt}"
                    )
        except Exception as _e_tide:
            st.warning(f"⚠️ Could not render tidal prediction: {_e_tide}")

    # ==========================================================================
    # ── SECTION 0: OPTIMIZATION ENGINE ───────────────────────────────────────
    # ==========================================================================
    sec("🧠 Optimization Engine — Heuristic Parameter Search")

    if not run_opt:
        # Clear any previously persisted best params so stale optimizer results
        # don't influence the simulation when the optimizer is switched off.
        st.session_state.pop("_best_opt_params", None)
        st.session_state.pop("selected_opt_scenario", None)
        st.markdown(
            '<div class="alert-info">ℹ️ Optimizer is off. '
            'Enable <b>Run Optimizer</b> in the sidebar to sweep 240 parameter '
            'combinations and auto-select the best configuration.</div>',
            unsafe_allow_html=True)
    else:

        base_params_for_opt = {k: int(v) if isinstance(v, float) and v == int(v) else v
                               for k, v in params.items()}
        if _tide_bytes is not None:
            base_params_for_opt["_tide_csv_bytes_hex"] = binascii.hexlify(_tide_bytes).decode()
        base_params_for_opt["_sim_start_date"] = _start_iso_str if _start_iso_str else ""
        opt_cache_key = json.dumps(base_params_for_opt, sort_keys=True)

        with st.spinner("🔍 Running optimization sweep (daylight-constrained scenarios)…"):
            best_json, tbl_json = run_optimizer(opt_cache_key)

        best_r  = json.loads(best_json)
        opt_tbl = pd.read_json(io.StringIO(tbl_json), orient="records")
        best_sc = best_r["score"]
        best_pr = best_r["params"]
        # Persist best params so the run_sim() call (earlier in this rerun) can use
        # them on the NEXT Streamlit rerun without needing best_pr to be defined first.
        st.session_state["_best_opt_params"] = best_pr

        # ── Non-negotiables banner ───────────────────────────────────────────
        st.markdown(
            '<div style=\"background:#fff8f8;border:1px solid #ef4444;border-radius:8px;padding:12px 16px;margin-bottom:12px;\"><span style=\"color:#dc2626;font-weight:600;font-size:13px;\">🔒 Non-Negotiables</span><span style=\"color:#64748b;font-size:12px;margin-left:10px\">— locked constraints never varied by the optimizer</span><div style=\"margin-top:8px;display:flex;flex-wrap:wrap;gap:8px;\"><span style=\"background:#fee2e2;border:1px solid #ef4444;border-radius:5px;padding:4px 10px;color:#dc2626;font-size:12px;\">🌅 Daylight operations: 06:00 – 18:00</span><span style=\"background:#fee2e2;border:1px solid #ef4444;border-radius:5px;padding:4px 10px;color:#dc2626;font-size:12px;\">⚓ Berthing window: 06:00 – 18:00</span><span style=\"background:#fee2e2;border:1px solid #ef4444;border-radius:5px;padding:4px 10px;color:#dc2626;font-size:12px;\">🔗 Cast-off window: 06:00 – 18:00</span><span style=\"background:#fee2e2;border:1px solid #ef4444;border-radius:5px;padding:4px 10px;color:#dc2626;font-size:12px;\">🚢 Export departure: no earlier than 06:00</span><span style=\"background:#fee2e2;border:1px solid #ef4444;border-radius:5px;padding:4px 10px;color:#dc2626;font-size:12px;\">🕔 Day 2+ daily storage reassessment: 05:00</span></div></div>',
            unsafe_allow_html=True)

        # ── Tidal status widget ──────────────────────────────────────────
        _tide_html_color = "#3b82f6" if _tide_bytes else "#94a3b8"
        _tide_html_text  = (
            "🌊 Tidal Constraint Active — breakwater tide >1.6 m (daylight 06:00-18:00) · 2h from SanBarth · 4h from BIA"
            if _tide_bytes else
            "⚠️ No tidal file uploaded — daylight-only rules applied in this sweep"
        )
        st.markdown(
            f'<div style="background:#f8faff;border:1px solid {_tide_html_color};'
            f'border-radius:8px;padding:10px 16px;margin-bottom:12px;">'
            f'<span style="color:#79c0ff;font-size:13px;">{_tide_html_text}</span></div>',
            unsafe_allow_html=True)

        # ── Best configuration banner ─────────────────────────────────────────
        st.markdown(
            f'<div class="opt-best">'
            f'<div style="display:flex;align-items:center;margin-bottom:12px">'
            f'  <div class="opt-score">{best_sc["composite"]:.1f}</div>'
            f'  <div style="margin-left:14px">'
            f'    <span class="opt-badge">OPTIMAL CONFIGURATION</span><br>'
            f'    <span style="color:#8b949e;font-size:12px">composite score / 100 · '
            f'    ranked #1 of {len(opt_tbl)} scenarios evaluated</span>'
            f'  </div>'
            f'</div>'
            f'<div style="margin-bottom:10px;color:#e6edf3;font-size:13px">'
            f'  <b>Selected parameters:</b>'
            f'</div>'
            f'<span class="opt-param">dead-stock factor: '
            f'  <b>×{best_pr["dead_stock_factor"]:.2f}</b></span>'
            f'<span class="opt-param">Ibom trigger: '
            f'  <b>{best_pr["ibom_trigger_bbl"]:,} bbl</b></span>'
            f'<span class="opt-param">export window start: '
            f'  <b>{best_pr["export_sail_window_start"]:02d}:00</b></span>'
            f'<span class="opt-param">berthing window: '
            f'  <b>{best_pr["berthing_start"]:02d}:00 – {best_pr["berthing_end"]:02d}:00</b></span>'
            f'</div>',
            unsafe_allow_html=True)

        # ── Sub-score breakdown ───────────────────────────────────────────────
        sub_cols = st.columns(5)
        SCORE_DIMS = [
            ("Stock Drawdown", best_sc["throughput_score"], "#56d364", "28% weight"),
            ("Fleet Utilisation", best_sc["idle_score"],    "#f1c40f", "15% weight"),
            ("Storage Safety", best_sc["overflow_score"],   "#79c0ff", "42% weight"),
            ("Fair Allocation", best_sc["fairness_score"],  "#c084fc", "12% weight"),
            ("Turnaround", best_sc["turnaround_score"],     "#fb923c", "1% weight"),
        ]
        for col, (label, val, color, weight) in zip(sub_cols, SCORE_DIMS):
            with col:
                bar_pct = max(2, int(val))
                st.markdown(
                    f'<div class="kpi-card">'
                    f'  <div class="kpi-label">{label}<br>'
                    f'    <span style="color:#484f58">{weight}</span></div>'
                    f'  <div class="kpi-value" style="color:{color}">{val:.1f}</div>'
                    f'  <div class="score-bar-wrap">'
                    f'    <div class="score-bar" '
                    f'         style="width:{bar_pct}%;background:{color}"></div>'
                    f'  </div>'
                    f'</div>',
                    unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # ── Raw metrics from best run ─────────────────────────────────────────
        rm_cols = st.columns(4)
        with rm_cols[0]: kpi("Loaded (best config)",    f'{best_sc["total_loaded_bbl"]:,} bbl')
        with rm_cols[1]: kpi("Exported (best config)",  f'{best_sc["total_exported_bbl"]:,.0f} bbl')
        with rm_cols[2]: kpi("Spilled (best config)",   f'{best_sc["total_spilled_bbl"]:,.0f} bbl',
                              sub="✅ none" if best_sc["total_spilled_bbl"] == 0 else "⚠️ overflow")
        avg_cyc = best_sc.get("avg_cycle_hours")
        with rm_cols[3]: kpi("Avg Cycle (best config)",
                              f'{avg_cyc:.1f}h' if avg_cyc else "—")
        st.caption(
            f"Stock drawdown: {best_sc.get('stock_drawdown_bbl', 0):,.0f} bbl "
            f"({best_sc.get('stock_drawdown_pct', 0):.1f}%) · "
            f"Early (24h) drawdown: {best_sc.get('early_drawdown_pct', 0):.1f}%"
        )

        # ── Bottlenecks ───────────────────────────────────────────────────────
        bns = best_sc.get("bottlenecks", [])
        if bns:
            st.markdown(
                '<div class="alert-warn">⚠️ <b>Bottlenecks detected in best config:</b> '
                + " · ".join(bns) + "</div>",
                unsafe_allow_html=True)
        else:
            st.markdown(
                '<div class="alert-ok">✅ No significant bottlenecks in optimal configuration</div>',
                unsafe_allow_html=True)

        # ── Vessel utilisation heatmap ────────────────────────────────────────
        vu = best_sc.get("vessel_utilisation", {})
        if vu:
            st.markdown("**Vessel utilisation — optimal configuration:**")
            vu_cols = st.columns(len(vu))
            for ci, (vn, util_pct) in enumerate(vu.items()):
                with vu_cols[ci]:
                    color = "#56d364" if util_pct >= 70 else ("#f1c40f" if util_pct >= 45 else "#f85149")
                    st.markdown(
                        f'<div class="kpi-card">'
                        f'  <div class="kpi-label">{vn}</div>'
                        f'  <div class="kpi-value" style="color:{color}">{util_pct:.0f}%</div>'
                        f'  <div class="score-bar-wrap">'
                        f'    <div class="score-bar" '
                        f'         style="width:{max(2,int(util_pct))}%;background:{color}"></div>'
                        f'  </div>'
                        f'</div>',
                        unsafe_allow_html=True)

        # ── Storage performance table ─────────────────────────────────────────
        su = best_sc.get("storage_utilisation", {})
        if su:
            st.markdown("**Storage performance — optimal configuration:**")
            su_df = pd.DataFrame([
                {"Storage": sn,
                 "Avg Util %": d["avg_pct"],
                 "Peak Util %": d["peak_pct"],
                 "Overflow (bbl)": f'{d["overflow_bbl"]:,}',
                 "Status": "⚠️ overflow" if d["overflow_bbl"] > 0 else "✅ clean"}
                for sn, d in su.items()
            ])
            st.dataframe(su_df, width='stretch', hide_index=True)

        # ── Scenario comparison table ─────────────────────────────────────────
        with st.expander("📊 All scenarios ranked — click to compare"):
            # ── Scenario selector UI ───────────────────────────────────────────
            # Show which scenario is currently active (if any was selected)
            _active_scen = st.session_state.get("selected_opt_scenario")
            if _active_scen:
                _asc = _active_scen
                st.markdown(
                    f'<div style="background:#f0fdf4;border:1px solid #22c55e;border-radius:8px;'
                    f'padding:10px 14px;margin-bottom:10px;font-size:13px;">'
                    f'▶ <b>Running Scenario #{_asc["rank"]}</b> — '
                    f'Score {_asc["score"]:.1f} | '
                    f'Dead-stock ×{_asc["dead_stock_factor"]:.2f} | '
                    f'Ibom trigger {_asc["ibom_trigger_bbl"]:,} bbl | '
                    f'Export window {_asc["export_sail_window_start"]:02d}:00 | '
                    f'Berthing {_asc["berthing_start"]:02d}:00–{_asc["berthing_end"]:02d}:00'
                    f'&nbsp;&nbsp;<span style="color:#64748b;font-size:11px">'
                    f'(not the optimal — manually selected)</span></div>',
                    unsafe_allow_html=True,
                )
                if st.button("✖ Clear — revert to optimal", key="clear_opt_scenario"):
                    st.session_state.pop("selected_opt_scenario", None)
                    st.rerun()
            else:
                st.markdown(
                    '<div style="background:#f8faff;border:1px solid #94a3b8;border-radius:8px;'
                    'padding:8px 14px;margin-bottom:10px;font-size:12px;color:#475569;">'
                    '💡 Click <b>▶ Run</b> on any row to initialise the simulation with that scenario\'s parameters.</div>',
                    unsafe_allow_html=True,
                )

            display_cols = [
                "Rank", "Score", "Stock Drawdown", "Fleet Util", "Storage Safety",
                "Fair Allocation", "Export", "Turnaround", "Loaded (bbl)", "Spilled (bbl)",
                "Avg Cycle (h)", "dead_stock_x", "pf_trigger_k",
                "exp_window_h", "berth_start_h", "berth_end_h",
            ]
            tbl_show = opt_tbl[display_cols].copy()
            tbl_show["Score"]        = tbl_show["Score"].round(1)
            tbl_show["Stock Drawdown"] = tbl_show["Stock Drawdown"].round(1)
            tbl_show["Fleet Util"]     = tbl_show["Fleet Util"].round(1)
            tbl_show["Storage Safety"] = tbl_show["Storage Safety"].round(1)
            tbl_show["Fair Allocation"] = tbl_show["Fair Allocation"].round(1)
            tbl_show["Export"]       = tbl_show["Export"].round(1)
            tbl_show["Turnaround"]   = tbl_show["Turnaround"].round(1)
            st.dataframe(
                tbl_show.head(50), width='stretch',
                hide_index=True,
                column_config={
                    "Score":          st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%.1f"),
                    "Stock Drawdown": st.column_config.NumberColumn("Stock Drawdown", format="%.1f"),
                    "Storage Safety": st.column_config.NumberColumn("Storage Safety", format="%.1f"),
                    "Loaded (bbl)":st.column_config.NumberColumn("Loaded", format="%,d"),
                    "Spilled (bbl)":st.column_config.NumberColumn("Spilled", format="%,d"),
                },
            )

            # ── Per-scenario run buttons ────────────────────────────────────────
            st.markdown("**▶ Select a scenario to run:**")
            _top_n = min(50, len(opt_tbl))
            _btn_cols = st.columns(min(_top_n, 10))   # up to 10 buttons per row
            for _si in range(_top_n):
                _row   = opt_tbl.iloc[_si]
                _rank  = int(_row["Rank"])
                _score = round(float(_row["Score"]), 1)
                _col   = _btn_cols[_si % 10]
                _is_active = (_active_scen is not None and _active_scen["rank"] == _rank)
                _lbl   = f"#{_rank} ({_score})" if not _is_active else f"✓ #{_rank}"
                with _col:
                    if st.button(_lbl, key=f"run_scen_{_rank}",
                                 type="primary" if _is_active else "secondary",
                                 help=f"Rank {_rank} | Score {_score} | "
                                      f"DSF ×{_row['dead_stock_x']:.2f} | "
                                      f"Ibom {int(_row['pf_trigger_k'])}k bbl | "
                                      f"Export {int(_row['exp_window_h']):02d}:00 | "
                                      f"Berth {int(_row['berth_start_h']):02d}:00–{int(_row['berth_end_h']):02d}:00"):
                        st.session_state["selected_opt_scenario"] = {
                            "rank":                    _rank,
                            "score":                   _score,
                            "dead_stock_factor":       float(_row["dead_stock_x"]),
                            "ibom_trigger_bbl":        int(_row["pf_trigger_k"]) * 1000,
                            "export_sail_window_start":int(_row["exp_window_h"]),
                            "berthing_start":          int(_row["berth_start_h"]),
                            "berthing_end":            int(_row["berth_end_h"]),
                        }
                        st.rerun()

            st.caption(
                f"Showing top 50 of {len(opt_tbl)} scenarios. "
                f"ibom_trigger_k = Ibom trigger ÷ 1000. "
                f"dead_stock_x = loading threshold multiplier."
            )

        # ── Download best config ──────────────────────────────────────────────
        best_export = {
            "optimal_params": best_pr,
            "scores":         best_sc,
            "all_scenarios":  json.loads(tbl_json),
        }
        st.download_button(
            "📥 Download Optimization Report (JSON)",
            data=json.dumps(best_export, indent=2),
            file_name="tanker_optimization_report.json",
            mime="application/json",
        )

    # ==========================================================================
    # ── SECTION 2: SIMULATION KPIs ───────────────────────────────────────────
    # ==========================================================================
    _start_lbl = _dt.date.fromisoformat(_start_iso_str).strftime('%-d %b %Y') if _start_iso_str else 'Today'
    sec(f"📈 {params['sim_days']}-Day Simulation Forecast — from {_start_lbl}")

    # ── Active scenario banner ─────────────────────────────────────────────────
    _active_kpi_scen = st.session_state.get("selected_opt_scenario")
    if run_opt and _active_kpi_scen:
        _aksc = _active_kpi_scen
        _kpi_cols_b = st.columns([8, 2])
        with _kpi_cols_b[0]:
            st.markdown(
                f'<div style="background:#fefce8;border:1px solid #f59e0b;border-radius:8px;'
                f'padding:9px 14px;margin-bottom:8px;font-size:13px;">'
                f'⚡ <b>Running Scenario #{_aksc["rank"]}</b> '
                f'(Score {_aksc["score"]:.1f}) — '
                f'Dead-stock ×{_aksc["dead_stock_factor"]:.2f} · '
                f'Ibom trigger {_aksc["ibom_trigger_bbl"]:,} bbl · '
                f'Export {_aksc["export_sail_window_start"]:02d}:00 · '
                f'Berthing {_aksc["berthing_start"]:02d}:00–{_aksc["berthing_end"]:02d}:00'
                f'</div>',
                unsafe_allow_html=True,
            )
        with _kpi_cols_b[1]:
            if st.button("✖ Revert to optimal", key="clear_opt_kpi"):
                st.session_state.pop("selected_opt_scenario", None)
                st.rerun()
    elif run_opt:
        st.markdown(
            f'<div style="background:#f0fdf4;border:1px solid #22c55e;border-radius:8px;'
            f'padding:9px 14px;margin-bottom:8px;font-size:13px;">'
            f'✅ <b>Running optimal scenario</b> — '
            f'Score {best_sc["composite"]:.1f} | '
            f'Dead-stock ×{best_pr["dead_stock_factor"]:.2f} · '
            f'Ibom trigger {best_pr["ibom_trigger_bbl"]:,} bbl · '
            f'Export {best_pr["export_sail_window_start"]:02d}:00 · '
            f'Berthing {best_pr["berthing_start"]:02d}:00–{best_pr["berthing_end"]:02d}:00'
            f'</div>',
            unsafe_allow_html=True,
        )

    k1 = st.columns(5)
    with k1[0]: kpi("Total Loadings",   str(S["loadings"]))
    with k1[1]: kpi("Total Discharges", str(S["discharges"]))
    with k1[2]: kpi("Volume Loaded",    f"{S['loaded']:,} bbl")
    with k1[3]: kpi("Volume Exported",  f"{S['exported']:,.0f} bbl")
    with k1[4]: kpi("Export Voyages",   str(S["exports"]))

    st.markdown("<br>", unsafe_allow_html=True)
    k2 = st.columns(5)
    with k2[0]: kpi("Total Produced",  f"{S['produced']:,.0f} bbl")
    with k2[1]: kpi("Total Spilled",   f"{S['spilled']:,.0f} bbl",
                     sub="⚠️ overflow detected" if S["spilled"]>0 else "✅ no spill")
    with k2[2]: kpi("Overflow Events", str(S["ovf_events"]))
    all_stor = sum(S.get(f"final_{n}",0) for n in ["Chapel","JasmineS","Westmore","Duke","Starturn"])
    all_moth = sum(S.get(f"final_{n}",0) for n in ["Bryanston","Alkebulan","GreenEagle"])
    with k2[3]: kpi("Final All Storage", f"{all_stor:,.0f} bbl")
    with k2[4]: kpi("Final All Mothers", f"{all_moth:,.0f} bbl")

    st.markdown("<br>", unsafe_allow_html=True)
    k3 = st.columns(5)
    _sapi = S.get("storage_api", {})
    _mapi = S.get("mother_api",  {})
    _xapi = S.get("avg_exported_api", 0.0)
    with k3[0]: kpi("Chapel API",    f"{_sapi.get('Chapel',   0.0):.2f}°", sub="end of period")
    with k3[1]: kpi("JasmineS API",  f"{_sapi.get('JasmineS', 0.0):.2f}°", sub="end of period")
    with k3[2]: kpi("Westmore API",  f"{_sapi.get('Westmore',  0.0):.2f}°", sub="end of period")
    _all_moth_vol = sum(S.get(f"final_{n}", 0) for n in ["Bryanston","Alkebulan","GreenEagle"])
    _blended_moth = (
        sum(S.get(f"final_{n}", 0) * _mapi.get(n, 0.0)
            for n in ["Bryanston","Alkebulan","GreenEagle"]) / _all_moth_vol
        if _all_moth_vol > 0 else 0.0
    )
    with k3[3]: kpi("Mother Blended API", f"{_blended_moth:.2f}°", sub="all mothers combined")
    with k3[4]: kpi("Exported API",  f"{_xapi:.2f}°" if _xapi else "—", sub="weighted avg of exports")

    # ==========================================================================
    # ── SECTION 3: CAPACITY RECOMMENDATIONS ──────────────────────────────────
    # ==========================================================================
    sec("💡 Capacity & Fleet Recommendations")
    recs = capacity_recommendations(S, params, tl_df, mod)
    render_recommendations(recs)

    if S["spilled"] > 0:
        spill_by = S.get("spill_by_storage",{})
        rows_sp = [{"Storage":k,"Overflow (bbl)":f"{v:,.0f}",
                    "% of Total":f"{v/S['spilled']*100:.1f}%"}
                   for k,v in sorted(spill_by.items(), key=lambda x:-x[1]) if v>0]
        if rows_sp:
            st.markdown("**Overflow breakdown by storage point:**")
            st.dataframe(pd.DataFrame(rows_sp), width='content', hide_index=True)

    # ==========================================================================
    # ── SECTION 3b: COMBINED OPERATIONS SUMMARY ───────────────────────────────
    # ==========================================================================
    sec("📝 Combined Operations Summary")

    # ── Derive narrative values ───────────────────────────────────────────────
    _sim_d   = params["sim_days"]
    _loaded  = S.get("loaded", 0)
    _exported= S.get("exported", 0)
    _spilled = S.get("spilled", 0)
    _loadings= S.get("loadings", 0)
    _discs   = S.get("discharges", 0)
    _produced= S.get("produced", 0)
    _eff     = (_loaded / max(_produced, 1)) * 100
    _lifts_pd= _loadings / max(_sim_d, 1)
    _ovf_ev  = S.get("ovf_events", 0)
    _spill_pct = (_spilled / max(_produced, 1)) * 100
    _final_storage = sum(S.get(f"final_{n}", 0) for n in ["Chapel","JasmineS","Westmore","Duke","Starturn"])
    _final_mothers = sum(S.get(f"final_{n}", 0) for n in ["Bryanston","Alkebulan","GreenEagle"])

    # Storage by field
    _stor_finals = {n: S.get(f"final_{n}", 0) for n in ["Chapel","JasmineS","Westmore","Duke","Starturn"]}
    _capacities  = {"Chapel":270_000,"JasmineS":290_000,"Westmore":270_000,"Duke":90_000,"Starturn":70_000}
    _tightest    = min(_stor_finals, key=lambda k: _capacities[k] - _stor_finals[k] if _capacities[k]>0 else 1e9)
    _most_empty  = min(_stor_finals, key=lambda k: _stor_finals[k])

    # Challenge flags
    _has_spill   = _spilled > 0
    _has_overflow= _ovf_ev  > 0
    _low_eff     = _eff < 75
    _high_eff    = _eff >= 92
    _tight_cap   = any((_stor_finals[n] / _capacities[n]) > 0.88 for n in _stor_finals)
    _low_storage = any(_stor_finals[n] < _capacities[n] * 0.12 for n in _stor_finals)

    # Vessel days active (rough proxy: loadings × avg cycle)
    _vessels_active = len(params.get("vessel_states", {})) if params.get("vessel_states") else 8

    sc1, sc2 = st.columns([3, 2])

    with sc1:
        # ── Operations narrative card ─────────────────────────────────────────
        _eff_tag  = "tag-green" if _high_eff else ("tag-amber" if _eff >= 75 else "tag-red")
        _spill_tag= "tag-green" if not _has_spill else "tag-red"
        st.markdown(f"""
<div class="summary-card">
  <h4>🛢️ Operational Overview — {_sim_d}-Day Period</h4>
  <p>Over the simulated <strong>{_sim_d}-day period</strong>, the daughter vessel fleet completed
  <strong>{_loadings} loading lifts</strong> across all storage fields and discharged
  <strong>{_discs} times</strong> to the mother vessel tankers at BIA.
  A total of <strong>{_loaded:,.0f} bbl</strong> was lifted from storage and
  <strong>{_exported:,.0f} bbl</strong> exported to the export terminal.</p>
  <p>Field production injected <strong>{_produced:,.0f} bbl</strong> into storage across the period,
  at a combined average of <strong>{_produced/_sim_d:,.0f} bbl/day</strong>.
  Fleet lifting efficiency — the proportion of produced volumes successfully lifted —
  was <strong>{_eff:.1f}%</strong>
  <span class="summary-tag {_eff_tag}">{'✅ Strong' if _high_eff else ('⚠️ Moderate' if _eff>=75 else '❌ Low')}</span>.
  Overflow events recorded: <strong>{_ovf_ev}</strong>
  <span class="summary-tag {_spill_tag}">{'✅ No spill' if not _has_spill else f'⚠️ {_spilled:,.0f} bbl spilled'}</span>.</p>
  <p>At period end, combined storage held <strong>{_final_storage:,.0f} bbl</strong>
  across all five fields, with mother vessels retaining <strong>{_final_mothers:,.0f} bbl</strong>.
  Average lift rate: <strong>{_lifts_pd:.2f} lifts/day</strong>.</p>
</div>""", unsafe_allow_html=True)

        # ── Challenges card ───────────────────────────────────────────────────
        _challenges = []
        if _has_spill:
            _challenges.append(f"<li><strong>Storage overflow</strong> — {_spilled:,.0f} bbl lost to overflow "
                f"({_spill_pct:.1f}% of total production). The <strong>{_tightest}</strong> tank was nearest "
                f"capacity at period end. Consider earlier vessel scheduling or increased lift frequency.</li>")
        if _low_eff:
            _challenges.append("<li><strong>Low lifting efficiency</strong> — The fleet was unable to keep "
                "pace with production inflow. This is typically caused by berthing congestion, tidal "
                "constraints, or insufficient vessel count during peak production windows.</li>")
        if _tight_cap:
            tight_names = [n for n in _stor_finals if (_stor_finals[n]/_capacities[n])>0.88]
            _challenges.append(f"<li><strong>High tank utilisation</strong> at "
                f"{', '.join(tight_names)} — tanks above 88% capacity increase spill risk. "
                f"Prioritise these fields in dispatch sequencing.</li>")
        if _low_storage:
            low_names = [n for n in _stor_finals if _stor_finals[n] < _capacities[n]*0.12]
            _challenges.append(f"<li><strong>Low closing stock</strong> at "
                f"{', '.join(low_names)} — below 12% capacity at period end. Verify production "
                f"continuity and ensure no unplanned field shutdowns are pending.</li>")
        if not _challenges:
            _challenges.append("<li>No significant operational challenges identified in this simulation period. "
                "All storage levels, lifting efficiency, and overflow metrics are within acceptable bounds.</li>")

        st.markdown(f"""
<div class="summary-card">
  <h4>⚠️ Challenges & Risks</h4>
  <ul>{''.join(_challenges)}</ul>
</div>""", unsafe_allow_html=True)

    with sc2:
        # ── Vessel requirement card ───────────────────────────────────────────
        _req_vessels = max(4, round(_lifts_pd * 4.5))   # rough estimate: avg 4.5d cycle
        _req_tag = "tag-green" if _req_vessels <= 6 else ("tag-amber" if _req_vessels <= 8 else "tag-red")
        st.markdown(f"""
<div class="summary-card">
  <h4>🚢 Vessel Requirements</h4>
  <p>Based on <strong>{_lifts_pd:.2f} lifts/day</strong> and an average voyage cycle of
  ~4–5 days (load + sail + discharge + return), the operation requires an estimated
  <strong>{_req_vessels} active daughter vessels</strong>
  <span class="summary-tag {_req_tag}">Fleet size estimate</span>.</p>
  <ul>
    <li><strong>SanBarth/Sego/Awoba/Dawes</strong> — standard routes via BIA and/or
    Cawthorne passage; cycle ~4–5 days per vessel</li>
    <li><strong>Ibom</strong> — offshore buoy; Bedford &amp; Balham on rotation with swap trigger at {S.get("ibom_trigger",65000):,.0f} bbl. When no swap is active, Bedford/Balham support SanBarth (Chapel/JasmineS) loading.</li>
    <li><strong>Watson</strong> — restricted to SanBarth (Chapel/JasmineS) and Sego (Westmore)</li>
    <li><strong>Mother tankers</strong> — 3 vessels (Bryanston, Alkebulan, GreenEagle)
    required to be available at BIA to maintain discharge throughput</li>
  </ul>
  <p>Vessel availability below {max(4, _req_vessels-1)} active daughters will likely
  result in storage accumulation and increased overflow risk.</p>
</div>""", unsafe_allow_html=True)

        # ── Stability maintenance card ────────────────────────────────────────
        st.markdown(f"""
<div class="summary-card">
  <h4>🔒 Stability Factors to Maintain</h4>
  <ul>
    <li><strong>Tidal schedule adherence</strong> — all crossings of the main
                SanBarth→BIA breakwater require tide &gt;1.6 m during daylight. Departures must
    be planned against the tidal window; delays compound across the fleet.</li>
    <li><strong>Cawthorne passage coordination</strong> — Awoba-bound vessels
    use a 3-leg tidal passage; any slot missed adds ~6–12h to the cycle.</li>
    <li><strong>Mother vessel turnaround</strong> — export voyages must complete
    before the next daughter batch arrives. A delayed export creates a queue at BIA
    that backs up all storage fields.</li>
    <li><strong>Stock threshold discipline</strong> — vessels should not berth at
    storage below the 175% minimum-stock threshold; premature berthing locks a
    berth slot without completing a load.</li>
    <li><strong>Production continuity</strong> — any unplanned shutdown at Chapel,
    JasmineS, or Westmore (highest volume fields) materially reduces available
    lifting volume and stresses downstream scheduling.</li>
    <li><strong>Ibom swap protocol</strong> — the active/standby rotation must
    execute cleanly; a missed swap leaves one vessel idle and reduces Ibom
    throughput by ~50%.</li>
  </ul>
</div>""", unsafe_allow_html=True)

    # ── Potential issues banner ───────────────────────────────────────────────
    _issues = []
    if _has_spill:
        _issues.append(f'<span class="summary-tag tag-red">🔴 Overflow risk — {_spilled:,.0f} bbl</span>')
    if _tight_cap:
        _issues.append('<span class="summary-tag tag-amber">🟡 High tank utilisation</span>')
    if _low_eff:
        _issues.append('<span class="summary-tag tag-amber">🟡 Low lifting efficiency</span>')
    if _low_storage:
        _issues.append('<span class="summary-tag tag-amber">🟡 Low closing stock</span>')
    if not _issues:
        _issues.append('<span class="summary-tag tag-green">🟢 All metrics within normal bounds</span>')

    st.markdown(
        f'<div style="margin:10px 0 4px;font-size:12px;font-weight:700;color:#1a2744;">'
        f'Potential Issues Flagged:</div>'
        + " ".join(_issues),
        unsafe_allow_html=True
    )

    
        # ==========================================================================
    # ── SECTION 4: STORAGE FORECAST CHARTS ───────────────────────────────────
    # ==========================================================================
    sec("📦 Storage Volume Forecast")
    st.plotly_chart(chart_storage(tl_df), width='stretch')

    oc1, oc2 = st.columns(2)
    with oc1:
        of = chart_overflow(tl_df)
        if of: st.plotly_chart(of, width='stretch')
    with oc2:
        st.plotly_chart(chart_util(tl_df), width='stretch')

    sec("📦 Forecast End-of-Period Storage Levels")
    sc = st.columns(5)
    storage_items = [("Chapel","A",270_000),("JasmineS","A",290_000),
                     ("Westmore","C",270_000),("Duke","D",90_000),("Starturn","E",70_000)]
    _s_api = S.get("storage_api", {})
    for i,(name,pt,cv) in enumerate(storage_items):
        fv    = S.get(f"final_{name}", 0)
        pct   = fv / cv * 100
        _api  = _s_api.get(name, 0.0)
        with sc[i]:
            kpi(f"{name} (Pt {pt})", f"{fv:,.0f} bbl",
                sub=f"{pct:.0f}% full · <b>API {_api:.2f}°</b>")

    # ==========================================================================
    # ── SECTION 5: MOTHER VESSEL FORECAST ────────────────────────────────────
    # ==========================================================================
    sec("🛢️ Mother Vessel Forecast — BIA")
    st.plotly_chart(chart_mothers(tl_df, EXPORT_TRIG, MOTHER_CAP), width='stretch')
    _m_api = S.get("mother_api", {})
    mc = st.columns(3)
    for i,(mn,mk) in enumerate([("Bryanston","bryanston"),
                                  ("Alkebulan","alkebulan"),
                                  ("GreenEagle","greeneagle")]):
        with mc[i]:
            fv    = S.get(f"final_{mn}", 0)
            start = params.get(mk, 0)
            d     = fv - start
            col_s = "#56d364" if d >= 0 else "#f85149"
            _mapi = _m_api.get(mn, 0.0)
            _api_txt = f" · <b>API {_mapi:.2f}°</b>" if fv > 0 else ""
            kpi(mn, f"{fv:,.0f} bbl",
                sub=f'<span style="color:{col_s}">{"▲" if d>=0 else "▼"} {d:+,.0f} bbl vs 08:00</span>{_api_txt}')

    # ==========================================================================
    # ── SECTION 6: GANTT ─────────────────────────────────────────────────────
    # ==========================================================================
    sec("⛴️ Vessel Activity Timeline (Gantt)")
    st.plotly_chart(chart_gantt(tl_df, vnames, log_df=log_df), width='stretch')

    with st.expander("🎨 Colour key"):
        ck = st.columns(4)
        for i,vn in enumerate(vnames):
            with ck[i%4]:
                base = VESSEL_COLORS.get(vn,"#aaa")
                st.markdown(
                    f'<span class="pill" style="background:{base};color:#fff">{vn}</span>',
                    unsafe_allow_html=True)
                for sc_code, lbl in [("IDLE_A","Idle"),("LOADING","Loading"),
                    ("PF_LOADING","Ibom"),("SAILING_AB","Sailing → mother"),
                    ("DISCHARGING","Discharging"),("SAILING_BA","Returning"),
                    ("WAITING_DEAD_STOCK","Waiting dead-stock")]:
                    st.markdown(
                        f'<span style="background:{vcolor(vn,sc_code)};'
                        f'padding:1px 8px;border-radius:3px;font-size:11px">'
                        f'&nbsp;</span> {lbl}', unsafe_allow_html=True)

    # ==========================================================================
    # ── SECTION 7: VOYAGE COUNTS ──────────────────────────────────────────────
    # ==========================================================================
    sec("📊 Voyage Counts per Vessel")
    v1, v2 = st.columns([3,2])
    with v1:
        st.plotly_chart(chart_voyage_bars(log_df, vnames), width='stretch')
    with v2:
        rows_v = []
        for vn in vnames:
            vl = log_df[log_df.Vessel==vn]
            ld = len(vl[vl.Event=="LOADING_START"])
            dc = len(vl[vl.Event=="DISCHARGE_START"])
            vc = mod.VESSEL_CAPACITIES.get(vn,mod.DAUGHTER_CARGO_BBL)
            rows_v.append({"Vessel":vn,"Loads":ld,"Discharges":dc,
                            "Vol Loaded":f"{ld*vc:,} bbl","Cargo Cap":f"{vc:,}"})
        st.dataframe(pd.DataFrame(rows_v), width='stretch', hide_index=True)

    # ==========================================================================
    # ── SECTION 8: PER-VESSEL TABS ────────────────────────────────────────────
    # ==========================================================================
    sec("🚢 Per-Vessel Event Log")
    vtabs = st.tabs(vnames)
    for vtab, vn in zip(vtabs, vnames):
        with vtab:
            vlog  = log_df[log_df.Vessel==vn].copy()
            loads = vlog[vlog.Event=="LOADING_START"]
            discs = vlog[vlog.Event=="DISCHARGE_START"]
            vcap  = mod.VESSEL_CAPACITIES.get(vn,mod.DAUGHTER_CARGO_BBL)
            base  = VESSEL_COLORS.get(vn,"#aaa")
            ml,mr = st.columns([1,3])
            with ml:
                st.markdown(
                    f'<span class="pill" style="background:{base};color:#fff;'
                    f'font-size:15px;padding:5px 16px">{vn}</span><br><br>',
                    unsafe_allow_html=True)
                kpi("Voyages", str(len(loads)))
                st.markdown("<br>",unsafe_allow_html=True)
                kpi("Cargo Capacity", f"{vcap:,} bbl")
                st.markdown("<br>",unsafe_allow_html=True)
                kpi("Vol Loaded", f"{len(loads)*vcap:,} bbl")
                st.markdown("<br>",unsafe_allow_html=True)
                kpi("Vol Discharged", f"{len(discs)*vcap:,} bbl")
                st.markdown("<br>**Storages used:**", unsafe_allow_html=True)
                used = (vlog[vlog.Event=="LOADING_START"]["Detail"]
                        .str.extract(r"Loading \d[,\d]+ bbl \| (\w+):")
                        .dropna()[0].value_counts())
                for sn,cnt in used.items():
                    st.markdown(
                        f'<span class="pill" style="background:{STORAGE_COLORS.get(sn,"#aaa")};'
                        f'color:#fff">{sn} ×{cnt}</span>', unsafe_allow_html=True)
            with mr:
                show  = ["Time","Day","Voyage","Event","Detail"]
                extra = [c for c in ["Chapel_bbl","JasmineS_bbl","Duke_bbl",
                                     "Starturn_bbl","Mother_bbl","Vessel_api"] if c in vlog.columns]
                # Rename Vessel_api for clarity in the table
                _vlog_show = vlog[show+extra].copy()
                if "Vessel_api" in _vlog_show.columns:
                    _vlog_show = _vlog_show.rename(columns={"Vessel_api": "Cargo API°"})
                    # Only show API when vessel is carrying cargo (non-zero rows)
                    _vlog_show["Cargo API°"] = _vlog_show["Cargo API°"].replace(0.0, pd.NA)
                st.dataframe(_vlog_show, width='stretch', height=380)
            # API summary for this vessel
            with ml:
                _load_api_rows = vlog[vlog.Event=="LOADING_START"]
                if "Vessel_api" in _load_api_rows.columns and not _load_api_rows.empty:
                    _avg_vapi = _load_api_rows["Vessel_api"].replace(0, pd.NA).mean()
                    if pd.notna(_avg_vapi):
                        st.markdown("<br>", unsafe_allow_html=True)
                        kpi("Avg Cargo API", f"{_avg_vapi:.2f}°")

    # ==========================================================================
    # ── SECTION 9: STORAGE POINT TABS ────────────────────────────────────────
    # ==========================================================================
    sec("📍 Storage Breakdown")
    stabs = st.tabs(["Chapel (A)","JasmineS (A)","Westmore (C)","Duke (D)","Starturn (E)"])
    st_info = [
        ("Chapel","Chapel_bbl","Chapel_Overflow_Accum_bbl",270_000,"A",sorted(mod.VESSEL_NAMES)),
        ("JasmineS","JasmineS_bbl","JasmineS_Overflow_Accum_bbl",290_000,"A",sorted(mod.VESSEL_NAMES)),
        ("Westmore","Westmore_bbl","Westmore_Overflow_Accum_bbl",270_000,"C",sorted(mod.WESTMORE_PERMITTED_VESSELS)),
        ("Duke","Duke_bbl","Duke_Overflow_Accum_bbl",90_000,"D",sorted(mod.DUKE_PERMITTED_VESSELS)),
        ("Starturn","Starturn_bbl","Starturn_Overflow_Accum_bbl",70_000,"E",sorted(mod.STARTURN_PERMITTED_VESSELS)),
    ]
    for stab,(sname,vc,ovfc,cv,pt,perm) in zip(stabs,st_info):
        with stab:
            sf = go.Figure()
            if vc in tl_df.columns:
                sf.add_trace(go.Scatter(x=tl_df.Time, y=tl_df[vc], name=f"{sname} Volume",
                    fill="tozeroy", fillcolor=_hex_to_rgba(STORAGE_COLORS[sname]),
                    line=dict(color=STORAGE_COLORS[sname], width=2)))
            if ovfc in tl_df.columns:
                sf.add_trace(go.Scatter(x=tl_df.Time, y=tl_df[ovfc], name="Overflow (accum)",
                    line=dict(color="#ef4444", dash="dot", width=1.5)))
            sf.add_hline(y=cv, line=dict(color="#ef4444",dash="dash"),
                         annotation_text=f"Capacity {cv:,} bbl")
            sf.update_layout(height=230, margin=dict(l=50,r=20,t=20,b=30), **_DARK,
                             yaxis=dict(tickformat=",",**_GRID),
                             xaxis=_GRID, legend=dict(bgcolor="#ffffff"))
            st.plotly_chart(sf, width='stretch')
            sloads = log_df[(log_df.Event=="LOADING_START") &
                            (log_df.Detail.str.contains(sname, na=False))]
            c1,c2 = st.columns(2)
            with c1:
                kpi(f"Loadings from {sname}", str(len(sloads)),
                    sub=f"Permitted: {', '.join(perm)}")
            with c2:
                if not sloads.empty:
                    st.dataframe(sloads.groupby("Vessel").size()
                                 .reset_index(name="Loads"), width='stretch', hide_index=True)

    # ==========================================================================
    # ── SECTION 10: SEQUENCE & IBOM LOGS ──────────────────────────────────
    # ==========================================================================
    sec("🔀 Mother Vessel Discharge Sequence Log")
    seq = log_df[log_df.Event.isin(["BERTHING_START_B","MOTHER_SEQUENCE_ASSIGNMENT",
                                     "MOTHER_PRIORITY_ASSIGNMENT"])]
    st.dataframe(seq[["Time","Day","Vessel","Voyage","Event","Detail"]]
                 if not seq.empty else pd.DataFrame(columns=["Time","Day","Vessel","Event","Detail"]), width='stretch', height=300)

    sec("🔁 Ibom Bedford / Balham Swap Log")
    pf = log_df[log_df.Event.isin(
        ["IBOM_SWAP_TRIGGER","IBOM_SWAP_START","IBOM_SWAP_COMPLETE"])]
    if pf.empty:
        st.caption("No Ibom swaps in this simulation period.")
    else:
        st.dataframe(pf[["Time","Day","Vessel","Voyage","Event","Detail"]], width='stretch', height=240)

    # ==========================================================================
    # ── SECTION 11: FULL EVENT LOG ────────────────────────────────────────────
    # ==========================================================================
    sec("📋 Full Event Log")
    f1,f2,f3,f4 = st.columns(4)
    all_ents = vnames + ["Chapel","JasmineS","Westmore","Duke","Starturn",
                         "Bryanston","Alkebulan","GreenEagle"]
    with f1: vf   = st.multiselect("Vessel / Entity", all_ents, [], key="vf")
    with f2: ef   = st.multiselect("Event type", sorted(log_df.Event.dropna().unique()), [], key="ef")
    _slider_max = max(2, params["sim_days"])
    with f3: dr = st.slider("Day range", 1, _slider_max, (1, min(params["sim_days"], _slider_max)))
    with f4: srch = st.text_input("Search Detail", placeholder="e.g. Chapel, Bryanston…")

    filt = log_df[log_df.Day.between(dr[0],dr[1])].copy()
    if vf:   filt = filt[filt.Vessel.isin(vf)]
    if ef:   filt = filt[filt.Event.isin(ef)]
    if srch: filt = filt[filt.Detail.str.contains(srch, case=False, na=False)]

    show_c = ["Time","Day","Vessel","Voyage","Event","Detail"]
    extra  = [c for c in ["Chapel_bbl","JasmineS_bbl","Westmore_bbl",
                           "Duke_bbl","Starturn_bbl","Mother_bbl",
                           "Vessel_api","Chapel_api","JasmineS_api",
                           "Westmore_api","Duke_api","Starturn_api",
                           "Alkebulan_api","Bryanston_api","GreenEagle_api"] if c in filt.columns]
    _filt_show = filt[show_c+extra].rename(columns={
        "Vessel_api": "Cargo API°", "Chapel_api": "Chapel API°",
        "JasmineS_api": "JasmineS API°", "Westmore_api": "Westmore API°",
        "Duke_api": "Duke API°", "Starturn_api": "Starturn API°",
        "Alkebulan_api": "Alkebulan API°", "Bryanston_api": "Bryanston API°",
        "GreenEagle_api": "GreenEagle API°",
    })
    # Zero API values not meaningful — blank them
    for _ac in [c for c in _filt_show.columns if c.endswith("API°")]:
        _filt_show[_ac] = _filt_show[_ac].replace(0.0, pd.NA)
    st.dataframe(_filt_show, width='stretch', height=440)
    st.caption(f"Showing {len(filt):,} of {len(log_df):,} events")


    # ==========================================================================
    # ── SECTION 12b: JOURNEY MANAGEMENT PLAN ──────────────────────────────────
    # ==========================================================================
    sec("🗺️ Journey Management Plan")


    # ── Helper: derive plan start date ────────────────────────────────────────
    try:
        _jmp_start = _dt.date.fromisoformat(_start_iso_str)
    except Exception:
        _jmp_start = _dt.date.today()

    _jmp_days = min(params["sim_days"], 14)   # cap display at 14 days

    # ── Build per-day data from log_df and tl_df ──────────────────────────────
    _storage_cols = ["Chapel_bbl","JasmineS_bbl","Westmore_bbl","Duke_bbl","Starturn_bbl"]
    _mother_cols  = ["Bryanston_bbl","Alkebulan_bbl","GreenEagle_bbl"]
    _storage_names = ["Chapel","JasmineS","Westmore","Duke","Starturn"]
    _mother_names  = ["Bryanston","Alkebulan","GreenEagle"]

    def _parse_cargo(detail):
        m = re.search(r"([\d,]+) bbl", detail)
        return int(m.group(1).replace(",","")) if m else 0

    def _parse_storage(detail):
        m = re.search(r"\| (\w+):", detail)
        return m.group(1) if m else ""

    def _parse_mother(detail):
        m = re.search(r"\| (\w+):", detail)
        return m.group(1) if m else ""

    def _kkk(bbl):
        """Format bbl to abbreviated thousands."""
        if bbl >= 1_000_000: return f"{bbl/1_000_000:.1f}M"
        if bbl >= 1_000:     return f"{bbl//1000}k"
        return str(bbl)

    # Pre-index events by day.
    # t=0 is 08:00 Day 1 — there are no Day-0 events. Kept as empty list
    # for forward-compatibility in case a very early event slips through.
    _day0_loadings = log_df[
        (log_df["Day"] < 1) & (log_df["Event"] == "LOADING_START")
    ].to_dict("records")

    _ev = {}
    for _day in range(1, _jmp_days + 1):
        _d = log_df[log_df["Day"] == _day]
        _day_loadings = _d[_d["Event"]=="LOADING_START"].to_dict("records")
        # Merge Day-0 loadings into Day 1, deduplicating by vessel
        if _day == 1:
            _d1_vessel_set = {r["Vessel"] for r in _day_loadings}
            _day_loadings  = _day_loadings + [
                r for r in _day0_loadings if r["Vessel"] not in _d1_vessel_set
            ]
        _ev[_day] = {
            "loadings":  _day_loadings,
            "returning": _d[_d["Event"]=="ARRIVED_LOADING_POINT"].to_dict("records"),
            "fairway":   _d[_d["Event"]=="ARRIVED_FAIRWAY"].to_dict("records"),
            "berthing_b":_d[_d["Event"]=="BERTHING_START_B"].to_dict("records"),
            "discharge": _d[_d["Event"]=="DISCHARGE_START"].to_dict("records"),
        }
        # Opening stock: 08:00 row for this day — t=0 is 08:00, so index 0 is already 08:00
        _t = tl_df[tl_df["Day"] == _day]
        _api_cols  = ["Chapel_api","JasmineS_api","Westmore_api","Duke_api","Starturn_api"]
        _mapi_cols = ["Bryanston_api","Alkebulan_api","GreenEagle_api"]
        if not _t.empty:
            _f = _t.iloc[0]
            _ev[_day]["stocks"] = {
                n: int(_f[c]) for n, c in zip(_storage_names, _storage_cols)
            }
            _ev[_day]["m_stocks"] = {
                n: int(_f[c]) for n, c in zip(_mother_names, _mother_cols)
            }
            _ev[_day]["stock_apis"] = {
                n: round(float(_f[c]), 2) if c in _f.index else 0.0
                for n, c in zip(_storage_names, _api_cols)
            }
            _ev[_day]["m_stock_apis"] = {
                n: round(float(_f[c]), 2) if c in _f.index else 0.0
                for n, c in zip(_mother_names, _mapi_cols)
            }
        else:
            _ev[_day]["stocks"]      = {n: 0   for n in _storage_names}
            _ev[_day]["m_stocks"]    = {n: 0   for n in _mother_names}
            _ev[_day]["stock_apis"]  = {n: 0.0 for n in _storage_names}
            _ev[_day]["m_stock_apis"]= {n: 0.0 for n in _mother_names}

    # ── CSS for the plan table ─────────────────────────────────────────────────
    st.markdown("""
<style>
  .jmp-wrap{overflow-x:auto;padding:4px 0}
  .jmp-table{border-collapse:collapse;min-width:100%;font-size:11px;
             font-family:'Segoe UI',system-ui,sans-serif}
  .jmp-table th{background:#1a2744;color:#ffffff;padding:5px 8px;
                text-align:center;font-size:10px;font-weight:700;
                letter-spacing:.04em;border:1px solid #344d80;white-space:nowrap}
  .jmp-table th.sec-hdr-cell{background:#0f1a35;font-size:10px;
                              letter-spacing:.06em;text-transform:uppercase}
  .jmp-table td{padding:5px 7px;border:1px solid #e2e8f0;vertical-align:top;
                white-space:nowrap;min-width:70px}
  .jmp-table tr:nth-child(even) td{background:#f8f9fb}
  .jmp-table tr:nth-child(odd)  td{background:#ffffff}
  .jmp-date{font-weight:700;color:#1a2744;font-size:11px}
  .jmp-stock{font-size:10px;font-weight:600;color:#374151}
  .jmp-entry{display:inline-block;border-radius:4px;padding:2px 6px;
             margin:1px 0;font-size:10px;font-weight:600;color:#fff;
             white-space:nowrap;line-height:1.5}
  .jmp-idle{color:#94a3b8;font-size:10px;font-style:italic}
  .jmp-bia-entry{display:inline-block;border-radius:4px;padding:2px 6px;
                 margin:1px 0;font-size:10px;font-weight:600;
                 white-space:nowrap;line-height:1.5}
</style>""", unsafe_allow_html=True)

    # ── Column structure (mirrors the image) ──────────────────────────────────
    # We render as HTML table for full visual control + PNG export
    _vc = VESSEL_COLORS
    _mc = MOTHER_COLORS

    def _chip(vessel, text, bg=None):
        c = bg or _vc.get(vessel, "#94a3b8")
        return f'<span class="jmp-entry" style="background:{c}">{text}</span>'

    def _mchip(mother, text):
        c = _mc.get(mother, "#94a3b8")
        return f'<span class="jmp-bia-entry" style="background:{c}22;color:{c};border:1px solid {c}66">{text}</span>'

    def _idle():
        return '<span class="jmp-idle">—</span>'

    # ── Build table HTML ───────────────────────────────────────────────────────
    _html = ['<div class="jmp-wrap"><table class="jmp-table">']

    # Header row 1 — section labels
    _html.append(
        '<tr>'
        '<th rowspan="2" class="sec-hdr-cell">Date</th>'
        '<th colspan="5" class="sec-hdr-cell">Opening Stock (bbl)</th>'
        '<th colspan="5" class="sec-hdr-cell">Loading Plan</th>'
        '<th colspan="2" class="sec-hdr-cell">Returning to Load</th>'
        '<th colspan="1" class="sec-hdr-cell">Arriving BIA</th>'
        '<th colspan="3" class="sec-hdr-cell">Discharging Plan</th>'
        '</tr>'
    )
    # Header row 2 — column names
    _html.append(
        '<tr>'
        # Stock
        '<th>Chapel</th><th>JasmineS</th><th>Westmore</th><th>Duke</th><th>Starturn</th>'
        # Loading plan per storage
        '<th>Chapel</th><th>JasmineS</th><th>Westmore</th><th>Duke</th><th>Starturn</th>'
        # Returning
        '<th>Vessel &rarr; Storage</th><th>ETA</th>'
        # BIA arrival
        '<th>Vessel (ETA)</th>'
        # Discharge per mother
        '<th>Bryanston</th><th>Alkebulan</th><th>GreenEagle</th>'
        '</tr>'
    )

    for _day in range(1, _jmp_days + 1):
        _date = _jmp_start + _dt.timedelta(days=_day - 1)
        _de = _ev[_day]
        _stocks  = _de["stocks"]
        _mstocks = _de["m_stocks"]

        # ── Date cell ─────────────────────────────────────────────────────────
        _date_cell = f'<td class="jmp-date">{_date.strftime("%-d %b %Y")}<br><span style="font-size:9px;color:#64748b">{_date.strftime("%a")}</span></td>'

        # ── Stock cells ───────────────────────────────────────────────────────
        # Thresholds from ops colour-code chart:
        #   Safe (green)  < lower_limit
        #   Borderline (amber)  lower_limit – upper_limit
        #   Unsafe (red)  > upper_limit
        _STOCK_THRESHOLDS = {
            "Chapel":    (189_000, 228_000),
            "JasmineS":  (189_000, 228_000),
            "Westmore":  (189_000, 228_000),
            "Ibom":      ( 70_000,  84_400),
            "Starturn":  ( 45_500,  54_860),
            "Duke":      ( 63_000,  76_000),   # proportional: ~70% & ~84% of 90k
        }
        def _scell(name):
            v    = _stocks.get(name, 0)
            api  = _de.get("stock_apis", {}).get(name, 0.0)
            lo, hi = _STOCK_THRESHOLDS.get(name, (189_000, 228_000))
            if v < lo:
                bg, col, label = "#166534", "#bbf7d0", "Safe"
            elif v <= hi:
                bg, col, label = "#854d0e", "#fef08a", "Borderline"
            else:
                bg, col, label = "#991b1b", "#fecaca", "Unsafe"
            _api_str = f'<br><span style="color:{col};font-size:8px;opacity:0.8">API {api:.2f}°</span>' if v > 0 else ""
            return (
                f'<td style="background:{bg};text-align:center">' +
                f'<span style="color:{col};font-weight:700;font-size:10px">{_kkk(v)}</span>' +
                f'<br><span style="color:{col};font-size:8px;opacity:0.85">{label}</span>' +
                _api_str +
                '</td>'
            )

        _stock_cells = "".join(_scell(n) for n in _storage_names)

        # ── Loading plan cells — one column per storage ───────────────────────
        def _lcell(storage):
            entries = [r for r in _de["loadings"] if _parse_storage(r["Detail"])==storage]
            if not entries: return f'<td>{_idle()}</td>'
            inner = "<br>".join(
                _chip(r["Vessel"],
                      f"{r['Vessel']} | {r['Time'][11:16]} | {_kkk(_parse_cargo(r['Detail']))}")
                for r in entries
            )
            return f"<td>{inner}</td>"

        _load_cells = "".join(_lcell(n) for n in _storage_names)

        # ── Returning to load ─────────────────────────────────────────────────
        _rets = _de["returning"]
        if _rets:
            _ret_vessel = "<br>".join(
                _chip(r["Vessel"], f"{r['Vessel']} → {r['Detail'].split('Arrived ')[-1].split(' —')[0]}")
                for r in _rets
            )
            _ret_eta = "<br>".join(r["Time"][11:16] for r in _rets)
        else:
            _ret_vessel = _idle()
            _ret_eta    = _idle()

        # ── Arriving BIA ──────────────────────────────────────────────────────
        _fwy = _de["fairway"]
        if _fwy:
            _bia_arr = "<br>".join(
                _chip(r["Vessel"], f"{r['Vessel']} ({r['Time'][11:16]})")
                for r in _fwy
            )
        else:
            _bia_arr = _idle()

        # ── Discharge plan cells — one column per mother ──────────────────────
        def _dcell(mother):
            entries  = [r for r in _de["discharge"] if _parse_mother(r["Detail"])==mother]
            ms       = _mstocks.get(mother, 0)
            mapi     = _de.get("m_stock_apis", {}).get(mother, 0.0)
            _api_bit = f' · API {mapi:.2f}°' if ms > 0 else ""
            if not entries:
                stk = f'<span style="font-size:9px;color:#94a3b8">Stock: {_kkk(ms)}{_api_bit}</span>'
                return f'<td style="text-align:center">{_idle()}<br>{stk}</td>'
            stk = f'<span style="font-size:9px;color:#64748b;display:block;margin-bottom:2px">Stock: {_kkk(ms)}{_api_bit}</span>'
            inner = "<br>".join(
                _chip(r["Vessel"],
                      f"{r['Vessel']} | {r['Time'][11:16]} | {_kkk(_parse_cargo(r['Detail']))}")
                for r in entries
            )
            return f"<td>{stk}{inner}</td>"

        _disch_cells = "".join(_dcell(n) for n in _mother_names)

        _html.append(
            f"<tr>{_date_cell}{_stock_cells}{_load_cells}"
            f"<td>{_ret_vessel}</td><td style='text-align:center'>{_ret_eta}</td>"
            f"<td>{_bia_arr}</td>"
            f"{_disch_cells}</tr>"
        )

    _html.append("</table></div>")
    _table_html = "\n".join(_html)
    st.markdown(_table_html, unsafe_allow_html=True)

    # ── Legend ─────────────────────────────────────────────────────────────────
    _leg_html = '<div style="margin:10px 0 4px;display:flex;flex-wrap:wrap;gap:6px;align-items:center">'
    _leg_html += '<span style="font-size:11px;font-weight:700;color:#1a2744;margin-right:4px">Vessel colours:</span>'
    for _vn, _vc2 in VESSEL_COLORS.items():
        _leg_html += f'<span style="background:{_vc2};color:#fff;border-radius:4px;padding:2px 8px;font-size:10px;font-weight:700">{_vn}</span>'
    _leg_html += '</div>'
    st.markdown(_leg_html, unsafe_allow_html=True)

    # ── PNG Download ───────────────────────────────────────────────────────────
    st.markdown("---")
    _jmp_title_str = f"Journey Management Plan — {_jmp_start.strftime('%d %b %Y')} ({_jmp_days} days)"

    # Build a self-contained HTML page for the image
    _full_html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  body{{margin:16px;background:#fff;font-family:'Segoe UI',system-ui,Arial,sans-serif}}
  h2{{color:#1a2744;font-size:15px;margin:0 0 10px;font-weight:800;letter-spacing:.03em}}
  {chr(10).join([
    ".jmp-wrap{overflow-x:auto;padding:4px 0}",
    ".jmp-table{border-collapse:collapse;min-width:100%;font-size:11px}",
    ".jmp-table th{background:#1a2744;color:#fff;padding:5px 8px;text-align:center;font-size:10px;font-weight:700;letter-spacing:.04em;border:1px solid #344d80;white-space:nowrap}",
    ".jmp-table th.sec-hdr-cell{background:#0f1a35}",
    ".jmp-table td{padding:5px 7px;border:1px solid #e2e8f0;vertical-align:top;white-space:nowrap}",
    ".jmp-table tr:nth-child(even) td{background:#f8f9fb}",
    ".jmp-table tr:nth-child(odd) td{background:#ffffff}",
    ".jmp-date{font-weight:700;color:#1a2744;font-size:11px}",
    ".jmp-entry{display:inline-block;border-radius:4px;padding:2px 6px;margin:1px 0;font-size:10px;font-weight:600;color:#fff;white-space:nowrap;line-height:1.5}",
    ".jmp-idle{color:#94a3b8;font-size:10px;font-style:italic}",
    ".jmp-bia-entry{display:inline-block;border-radius:4px;padding:2px 6px;margin:1px 0;font-size:10px;font-weight:600;white-space:nowrap;line-height:1.5}",
  ])}
</style>
</head>
<body>
<h2>🗺️ {_jmp_title_str}</h2>
{_table_html}
<div style="margin:8px 0 4px;display:flex;flex-wrap:wrap;gap:6px">
{"".join(f'<span style="background:{c};color:#fff;border-radius:4px;padding:2px 8px;font-size:10px;font-weight:700">{v}</span>' for v,c in VESSEL_COLORS.items())}
</div>
<div style="font-size:9px;color:#94a3b8;margin-top:8px">Generated: {_dt.datetime.now().strftime("%Y-%m-%d %H:%M")} | Tanker Operations v5</div>
</body></html>"""

    _dc1, _dc2, _dc3 = st.columns([2,2,2])
    with _dc1:
        # HTML download (always works, can be printed-to-PDF in browser)
        st.download_button(
            "📥 Download as HTML (open in browser → Print → Save as PDF/Image)",
            data=_full_html.encode("utf-8"),
            file_name=f"journey_plan_{_jmp_start.isoformat()}.html",
            mime="text/html",
            help="Download the Journey Management Plan as a self-contained HTML file. Open in your browser and use Ctrl+P (or Cmd+P) to print or save as PDF."
        )
    with _dc2:
        # CSV export of raw plan data
        _jmp_rows = []
        for _day in range(1, _jmp_days + 1):
            _date = _jmp_start + _dt.timedelta(days=_day - 1)
            _de2 = _ev[_day]
            for r in _de2["loadings"]:
                _jmp_rows.append({"Date": _date, "Section": "Loading", "Vessel": r["Vessel"],
                    "Location": _parse_storage(r["Detail"]), "Time": r["Time"][11:16],
                    "Cargo_bbl": _parse_cargo(r["Detail"]), "Mother": ""})
            for r in _de2["discharge"]:
                _jmp_rows.append({"Date": _date, "Section": "Discharge", "Vessel": r["Vessel"],
                    "Location": "", "Time": r["Time"][11:16],
                    "Cargo_bbl": _parse_cargo(r["Detail"]), "Mother": _parse_mother(r["Detail"])})
            for r in _de2["returning"]:
                _jmp_rows.append({"Date": _date, "Section": "Returning", "Vessel": r["Vessel"],
                    "Location": r["Detail"].split("Arrived ")[-1].split(" —")[0],
                    "Time": r["Time"][11:16], "Cargo_bbl": 0, "Mother": ""})
            for r in _de2["fairway"]:
                _jmp_rows.append({"Date": _date, "Section": "BIA Arrival", "Vessel": r["Vessel"],
                    "Location": "Fairway", "Time": r["Time"][11:16], "Cargo_bbl": 0, "Mother": ""})
        _jmp_csv = pd.DataFrame(_jmp_rows).to_csv(index=False).encode()
        st.download_button(
            "📥 Download Plan as CSV",
            data=_jmp_csv,
            file_name=f"journey_plan_{_jmp_start.isoformat()}.csv",
            mime="text/csv"
        )
    with _dc3:
        st.caption(
            "💡 **PNG export tip:** Download the HTML file above, open it in Chrome or Edge, "
            "then right-click → Print → Change destination to 'Save as PDF'. "
            "Alternatively use browser screenshot tools for PNG."
        )


    # ==========================================================================
    # ── SECTION 12: DOWNLOADS ─────────────────────────────────────────────────
    # ==========================================================================
    sec("⬇️ Download Results")
    d1,d2,d3 = st.columns(3)
    with d1:
        st.download_button("📥 Full Event Log (CSV)",
                           log_df.to_csv(index=False).encode(),
                           "tanker_event_log_v5.csv","text/csv")
    with d2:
        st.download_button("📥 Timeline Snapshots (CSV)",
                           tl_df.to_csv(index=False).encode(),
                           "tanker_timeline_v5.csv","text/csv")
    with d3:
        rows = [
        ["Simulation Start Date", _dt.date.fromisoformat(_start_iso_str).strftime('%d/%m/%Y')],
        ["Simulation Days",      params["sim_days"]],
            ["Total Loadings",       S["loadings"]],
            ["Total Discharges",     S["discharges"]],
            ["Volume Loaded (bbl)",  S["loaded"]],
            ["Volume Exported (bbl)",S["exported"]],
            ["Volume Produced (bbl)",S["produced"]],
            ["Volume Spilled (bbl)", S["spilled"]],
            ["Overflow Events",      S["ovf_events"]],
        ]
        for name,pt,cv in storage_items:
            rows.append([f"Final {name} (bbl)", S.get(f"final_{name}",0)])
        for mn in ["Bryanston","Alkebulan","GreenEagle"]:
            rows.append([f"Final {mn} (bbl)", S.get(f"final_{mn}",0)])
        for rec in recs:
            rows.append([f"Rec [{rec['type']}]", rec["title"]])
        st.download_button(
            "📥 Summary + Recommendations (CSV)",
            pd.DataFrame(rows,columns=["Metric","Value"]).to_csv(index=False).encode(),
            "tanker_summary_v5.csv","text/csv")

    # ==========================================================================
    # ── AUTO-REFRESH ──────────────────────────────────────────────────────────
    # ==========================================================================
    if auto_ref:
        ph = st.empty()
        for rem in range(ref_secs, 0, -1):
            ph.caption(f"🔄 Auto-refreshing in {rem}s…")
            time.sleep(1)
        ph.caption("🔄 Refreshing…")
        st.cache_data.clear()
        st.rerun()

    # ── Footer ────────────────────────────────────────────────────────────────
    st.divider()
    st.caption(
        f"Tanker Operations Simulation v5 · "
        f"Baseline: 08:00 position report · "
        f"Last run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} · "
        f"Vessels: {', '.join(vnames)}"
    )


if __name__ == "__main__":
    main()
