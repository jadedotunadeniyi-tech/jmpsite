"""
pages/vessel_positions.py
─────────────────────────────────────────────────────────────────────────────
08:00 Vessel Position Entry  —  multi-page companion to tanker_app.py

REPO STRUCTURE
─────────────────────────────────────────────────────────────────────────────
    your_repo/
    ├── tanker_app.py
    ├── tanker_simulation_v5.py
    ├── requirements.txt
    └── pages/
        └── vessel_positions.py   ← this file

Streamlit adds it automatically to the sidebar navigation.

HOW IT CONNECTS TO THE SIM
─────────────────────────────────────────────────────────────────────────────
Pressing "Confirm & Send to Simulation" writes:

    st.session_state["vp_vessel_states"]  – {vessel: {status, cargo_bbl,
                                              location, target_storage,
                                              target_mother}}
    st.session_state["vp_mother_vols"]    – {bryanston: int, …}
    st.session_state["vp_mother_apis"]    – {bryanston: float, …}
    st.session_state["vp_confirmed"]      – True

tanker_app.py reads those keys via the integration shim shown at the
bottom of this page. Paste the shim once — no other changes needed.
─────────────────────────────────────────────────────────────────────────────
"""

import os, sys, types
import unittest.mock as _mock
import streamlit as st
import pandas as pd


# ── Sim module loader (for live VESSEL_CAPACITIES / MOTHER_CAPACITY_BBL) ──────

@st.cache_resource(show_spinner=False)
def _load_sim_mod():
    # Works whether this file is in pages/ or the root directory
    for candidate in [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tanker_simulation_v5.py"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "tanker_simulation_v5.py"),
    ]:
        if os.path.exists(candidate):
            source = open(candidate).read()
            marker = "# -----------------------------------------------------------------\n# RUN SIMULATION"
            if marker in source:
                source = source.split(marker)[0]
            for m in ["matplotlib", "matplotlib.pyplot", "matplotlib.patches"]:
                if m not in sys.modules:
                    sys.modules[m] = _mock.MagicMock()
            mod = types.ModuleType("tanker_sim_vp")
            mod.__file__ = candidate
            exec(compile(source, candidate, "exec"), mod.__dict__)
            return mod
    return None


# ── Static constants (mirrored from tanker_app.py) ────────────────────────────

VESSEL_COLORS = {
    "Sherlock" : "#ff6b6b",
    "Laphroaig": "#2ecc71",
    "Rathbone" : "#c77dff",
    "Bedford"  : "#f39c12",
    "Balham"   : "#1abc9c",
    "Woodstock": "#ff4d8d",
    "Bagshot"  : "#00bcd4",
    "Watson"   : "#b0bec5",
}
MOTHER_COLORS = {
    "Bryanston" : "#1abc9c",
    "Alkebulan" : "#ff5555",
    "GreenEagle": "#c084fc",
}
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
    "SAILING_AB":"🚢","SAILING_CROSS_BW_AC":"🚢","SAILING_BW_TO_FWY":"🚢",
    "SAILING_AB_LEG2":"🚢","SAILING_BA":"🔄","SAILING_B_TO_FWY":"🔄",
    "SAILING_FWY_TO_BW":"🔄","SAILING_CROSS_BW_IN_AC":"🔄","SAILING_BW_TO_A":"🔄",
    "SAILING_D_CHANNEL":"🚢","SAILING_CH_TO_BW_OUT":"🚢","SAILING_CROSS_BW_OUT":"🚢",
    "SAILING_B_TO_BW_IN":"🚢","SAILING_CROSS_BW_IN":"🚢",
    "SAILING_BW_TO_CH_IN":"🚢","SAILING_CH_TO_D":"🚢",
    "WAITING_FAIRWAY":"⚓","WAITING_BERTH_B":"⏳","WAITING_BERTH_A":"⏳",
    "BERTHING_A":"🔗","BERTHING_B":"🔗","HOSE_CONNECT_A":"🔧","HOSE_CONNECT_B":"🔧",
    "IDLE_A":"🟢","IDLE_B":"🟡","CAST_OFF":"↩️","CAST_OFF_B":"↩️","DOCUMENTING":"📄",
    "WAITING_CAST_OFF":"⏳","WAITING_DEAD_STOCK":"⏳","WAITING_RETURN_STOCK":"⏳",
    "PF_SWAP":"🔁","WAITING_DAYLIGHT":"🌙","WAITING_TIDAL":"🌊","WAITING_STOCK":"⏳",
    "WAITING_MOTHER_RETURN":"⏳","WAITING_MOTHER_CAPACITY":"⏳",
}

ALL_VESSELS = ["Sherlock","Laphroaig","Rathbone","Bedford","Balham","Woodstock","Bagshot","Watson"]
ALL_MOTHERS = ["Bryanston","Alkebulan","GreenEagle"]
MOTHER_CAP_FALLBACK = 2_000_000

# Location catalogue — same structure as tanker_app.py's LOCATION_CATALOGUE
LOCATION_CATALOGUE = [
    {"display":"Chapel (SanBarth)",    "sim_value":"Chapel",    "field_zone":"SanBarth",
     "statuses":[("IDLE_A","🟢 Idle at berth — ready to load"),("LOADING","⛽ Loading — in progress"),
                 ("HOSE_CONNECT_A","🔧 Hose connection"),("BERTHING_A","🔗 Berthing in progress"),
                 ("WAITING_BERTH_A","⏳ Waiting for berth"),("WAITING_STOCK","⏳ Waiting for stock"),
                 ("WAITING_DEAD_STOCK","⏳ Stock below dead-stock threshold"),
                 ("DOCUMENTING","📄 Documentation"),("WAITING_CAST_OFF","⏳ Awaiting cast-off"),
                 ("CAST_OFF","↩️ Cast off")]},
    {"display":"JasmineS (SanBarth)", "sim_value":"JasmineS",  "field_zone":"SanBarth",
     "statuses":[("IDLE_A","🟢 Idle at berth — ready to load"),("LOADING","⛽ Loading — in progress"),
                 ("HOSE_CONNECT_A","🔧 Hose connection"),("BERTHING_A","🔗 Berthing in progress"),
                 ("WAITING_BERTH_A","⏳ Waiting for berth"),("WAITING_STOCK","⏳ Waiting for stock"),
                 ("WAITING_DEAD_STOCK","⏳ Stock below dead-stock threshold"),
                 ("DOCUMENTING","📄 Documentation"),("WAITING_CAST_OFF","⏳ Awaiting cast-off"),
                 ("CAST_OFF","↩️ Cast off")]},
    {"display":"Westmore (Sego)",     "sim_value":"Westmore",  "field_zone":"Sego",
     "statuses":[("IDLE_A","🟢 Idle at berth — ready to load"),("LOADING","⛽ Loading — in progress"),
                 ("HOSE_CONNECT_A","🔧 Hose connection"),("BERTHING_A","🔗 Berthing in progress"),
                 ("WAITING_BERTH_A","⏳ Waiting for berth"),("WAITING_STOCK","⏳ Waiting for stock"),
                 ("WAITING_DEAD_STOCK","⏳ Stock below dead-stock threshold"),
                 ("DOCUMENTING","📄 Documentation"),("WAITING_CAST_OFF","⏳ Awaiting cast-off"),
                 ("CAST_OFF","↩️ Cast off")]},
    {"display":"Duke (Awoba)",        "sim_value":"Duke",      "field_zone":"Awoba",
     "statuses":[("IDLE_A","🟢 Idle at berth — ready to load"),("LOADING","⛽ Loading — in progress"),
                 ("HOSE_CONNECT_A","🔧 Hose connection"),("BERTHING_A","🔗 Berthing in progress"),
                 ("WAITING_BERTH_A","⏳ Waiting for berth"),("WAITING_STOCK","⏳ Waiting for stock"),
                 ("WAITING_DEAD_STOCK","⏳ Stock below dead-stock threshold"),
                 ("DOCUMENTING","📄 Documentation"),("WAITING_CAST_OFF","⏳ Awaiting cast-off"),
                 ("CAST_OFF","↩️ Cast off")]},
    {"display":"Starturn (Dawes)",    "sim_value":"Starturn",  "field_zone":"Dawes",
     "statuses":[("IDLE_A","🟢 Idle at berth — ready to load"),("LOADING","⛽ Loading — in progress"),
                 ("HOSE_CONNECT_A","🔧 Hose connection"),("BERTHING_A","🔗 Berthing in progress"),
                 ("WAITING_BERTH_A","⏳ Waiting for berth"),("WAITING_STOCK","⏳ Waiting for stock"),
                 ("WAITING_DEAD_STOCK","⏳ Stock below dead-stock threshold"),
                 ("DOCUMENTING","📄 Documentation"),("WAITING_CAST_OFF","⏳ Awaiting cast-off"),
                 ("CAST_OFF","↩️ Cast off")]},
    {"display":"Ibom (Offshore Buoy)","sim_value":"Ibom",      "field_zone":"Ibom",
     "statuses":[("PF_LOADING","⛽ Loading at offshore buoy"),("IDLE_A","🟢 Idle / standby at buoy"),
                 ("PF_SWAP","🔁 Vessel swap in progress"),("WAITING_DAYLIGHT","🌙 Waiting — daylight"),
                 ("WAITING_TIDAL","🌊 Waiting — tidal window")]},
    # BIA
    {"display":"BIA — Fairway Buoy",  "sim_value":"Fairway",   "field_zone":"BIA",
     "statuses":[("WAITING_FAIRWAY","⚓ Holding at Fairway Buoy"),
                 ("SAILING_AB_LEG2","🚢 Inbound — Fairway → BIA (2h)"),
                 ("WAITING_BERTH_B","⏳ Waiting for mother berth"),
                 ("WAITING_MOTHER_RETURN","⏳ Waiting — mother away at export"),
                 ("WAITING_MOTHER_CAPACITY","⏳ Waiting — mother full"),
                 ("WAITING_RETURN_STOCK","⏳ Waiting — return stock low"),
                 ("WAITING_DAYLIGHT","🌙 Waiting — daylight window"),
                 ("IDLE_B","🟢 Idle at BIA")]},
    {"display":"Bryanston (BIA)",     "sim_value":"Bryanston", "field_zone":"BIA", "target_mother":"Bryanston",
     "statuses":[("DISCHARGING","⬇️ Discharge in progress"),("HOSE_CONNECT_B","🔧 Hose connection"),
                 ("BERTHING_B","🔗 Berthing in progress"),("WAITING_BERTH_B","⏳ Waiting for berth"),
                 ("WAITING_MOTHER_CAPACITY","⏳ Waiting — mother capacity"),
                 ("CAST_OFF_B","↩️ Discharge complete — cast off"),
                 ("IDLE_B","🟢 Idle at mother"),("WAITING_CAST_OFF","⏳ Awaiting cast-off"),
                 ("WAITING_MOTHER_RETURN","⏳ Waiting — mother away")]},
    {"display":"Alkebulan (BIA)",     "sim_value":"Alkebulan", "field_zone":"BIA", "target_mother":"Alkebulan",
     "statuses":[("DISCHARGING","⬇️ Discharge in progress"),("HOSE_CONNECT_B","🔧 Hose connection"),
                 ("BERTHING_B","🔗 Berthing in progress"),("WAITING_BERTH_B","⏳ Waiting for berth"),
                 ("WAITING_MOTHER_CAPACITY","⏳ Waiting — mother capacity"),
                 ("CAST_OFF_B","↩️ Discharge complete — cast off"),
                 ("IDLE_B","🟢 Idle at mother"),("WAITING_CAST_OFF","⏳ Awaiting cast-off"),
                 ("WAITING_MOTHER_RETURN","⏳ Waiting — mother away")]},
    {"display":"GreenEagle (BIA)",    "sim_value":"GreenEagle","field_zone":"BIA", "target_mother":"GreenEagle",
     "statuses":[("DISCHARGING","⬇️ Discharge in progress"),("HOSE_CONNECT_B","🔧 Hose connection"),
                 ("BERTHING_B","🔗 Berthing in progress"),("WAITING_BERTH_B","⏳ Waiting for berth"),
                 ("WAITING_MOTHER_CAPACITY","⏳ Waiting — mother capacity"),
                 ("CAST_OFF_B","↩️ Discharge complete — cast off"),
                 ("IDLE_B","🟢 Idle at mother"),("WAITING_CAST_OFF","⏳ Awaiting cast-off"),
                 ("WAITING_MOTHER_RETURN","⏳ Waiting — mother away")]},
    # Outbound transit
    {"display":"Sailing → Bryanston (A/C outbound)", "sim_value":"En Route SanBarth→BIA",
     "field_zone":"Transit","target_mother":"Bryanston",
     "statuses":[("SAILING_AB","🚢 Leg 1: A/C → Breakwater (1.5h)"),
                 ("SAILING_CROSS_BW_AC","🚢 Leg 2: Crossing Breakwater outbound (0.5h)"),
                 ("SAILING_BW_TO_FWY","🚢 Leg 3: Breakwater → Fairway Buoy (2h)"),
                 ("WAITING_TIDAL","🌊 Holding — tidal window"),
                 ("WAITING_DAYLIGHT","🌙 Holding — daylight window"),
                 ("WAITING_RETURN_STOCK","⏳ Holding — return stock too low")]},
    {"display":"Sailing → Alkebulan (A/C outbound)", "sim_value":"En Route SanBarth→BIA",
     "field_zone":"Transit","target_mother":"Alkebulan",
     "statuses":[("SAILING_AB","🚢 Leg 1: A/C → Breakwater (1.5h)"),
                 ("SAILING_CROSS_BW_AC","🚢 Leg 2: Crossing Breakwater outbound (0.5h)"),
                 ("SAILING_BW_TO_FWY","🚢 Leg 3: Breakwater → Fairway Buoy (2h)"),
                 ("WAITING_TIDAL","🌊 Holding — tidal window"),
                 ("WAITING_DAYLIGHT","🌙 Holding — daylight window"),
                 ("WAITING_RETURN_STOCK","⏳ Holding — return stock too low")]},
    {"display":"Sailing → GreenEagle (A/C outbound)", "sim_value":"En Route SanBarth→BIA",
     "field_zone":"Transit","target_mother":"GreenEagle",
     "statuses":[("SAILING_AB","🚢 Leg 1: A/C → Breakwater (1.5h)"),
                 ("SAILING_CROSS_BW_AC","🚢 Leg 2: Crossing Breakwater outbound (0.5h)"),
                 ("SAILING_BW_TO_FWY","🚢 Leg 3: Breakwater → Fairway Buoy (2h)"),
                 ("WAITING_TIDAL","🌊 Holding — tidal window"),
                 ("WAITING_DAYLIGHT","🌙 Holding — daylight window"),
                 ("WAITING_RETURN_STOCK","⏳ Holding — return stock too low")]},
    {"display":"Approaching Bryanston (Fairway Buoy)", "sim_value":"Fairway Buoy",
     "field_zone":"Transit","target_mother":"Bryanston",
     "statuses":[("SAILING_AB_LEG2","🚢 Fairway Buoy → BIA (2h)"),
                 ("WAITING_FAIRWAY","⚓ Holding at Fairway Buoy overnight"),
                 ("WAITING_BERTH_B","⏳ Waiting for mother berth"),
                 ("WAITING_MOTHER_RETURN","⏳ Waiting — mother away"),
                 ("WAITING_MOTHER_CAPACITY","⏳ Waiting — mother full"),
                 ("WAITING_DAYLIGHT","🌙 Waiting — daylight")]},
    {"display":"Approaching Alkebulan (Fairway Buoy)", "sim_value":"Fairway Buoy",
     "field_zone":"Transit","target_mother":"Alkebulan",
     "statuses":[("SAILING_AB_LEG2","🚢 Fairway Buoy → BIA (2h)"),
                 ("WAITING_FAIRWAY","⚓ Holding at Fairway Buoy overnight"),
                 ("WAITING_BERTH_B","⏳ Waiting for mother berth"),
                 ("WAITING_MOTHER_RETURN","⏳ Waiting — mother away"),
                 ("WAITING_MOTHER_CAPACITY","⏳ Waiting — mother full"),
                 ("WAITING_DAYLIGHT","🌙 Waiting — daylight")]},
    {"display":"Approaching GreenEagle (Fairway Buoy)", "sim_value":"Fairway Buoy",
     "field_zone":"Transit","target_mother":"GreenEagle",
     "statuses":[("SAILING_AB_LEG2","🚢 Fairway Buoy → BIA (2h)"),
                 ("WAITING_FAIRWAY","⚓ Holding at Fairway Buoy overnight"),
                 ("WAITING_BERTH_B","⏳ Waiting for mother berth"),
                 ("WAITING_MOTHER_RETURN","⏳ Waiting — mother away"),
                 ("WAITING_MOTHER_CAPACITY","⏳ Waiting — mother full"),
                 ("WAITING_DAYLIGHT","🌙 Waiting — daylight")]},
    # Cawthorne outbound (Duke → BIA)
    {"display":"Cawthorne Channel → Bryanston","sim_value":"Cawthorne Channel (outbound)",
     "field_zone":"Transit","target_mother":"Bryanston","target_storage":"Duke",
     "statuses":[("SAILING_D_CHANNEL","🚢 Point D → Channel (3h, tidal)"),
                 ("SAILING_CH_TO_BW_OUT","🚢 Channel → Breakwater (1h, tidal)"),
                 ("SAILING_CROSS_BW_OUT","🚢 Crossing Breakwater outbound (0.5h, tidal)"),
                 ("WAITING_TIDAL","🌊 Waiting — tidal window")]},
    {"display":"Cawthorne Channel → Alkebulan","sim_value":"Cawthorne Channel (outbound)",
     "field_zone":"Transit","target_mother":"Alkebulan","target_storage":"Duke",
     "statuses":[("SAILING_D_CHANNEL","🚢 Point D → Channel (3h, tidal)"),
                 ("SAILING_CH_TO_BW_OUT","🚢 Channel → Breakwater (1h, tidal)"),
                 ("SAILING_CROSS_BW_OUT","🚢 Crossing Breakwater outbound (0.5h, tidal)"),
                 ("WAITING_TIDAL","🌊 Waiting — tidal window")]},
    {"display":"Cawthorne Channel → GreenEagle","sim_value":"Cawthorne Channel (outbound)",
     "field_zone":"Transit","target_mother":"GreenEagle","target_storage":"Duke",
     "statuses":[("SAILING_D_CHANNEL","🚢 Point D → Channel (3h, tidal)"),
                 ("SAILING_CH_TO_BW_OUT","🚢 Channel → Breakwater (1h, tidal)"),
                 ("SAILING_CROSS_BW_OUT","🚢 Crossing Breakwater outbound (0.5h, tidal)"),
                 ("WAITING_TIDAL","🌊 Waiting — tidal window")]},
    # Return transit
    {"display":"Returning → Chapel/JasmineS (SanBarth)","sim_value":"En Route BIA→Storage",
     "field_zone":"Transit","target_storage":"Chapel",
     "statuses":[("SAILING_B_TO_FWY","🔄 BIA → Fairway Buoy (2h)"),
                 ("SAILING_FWY_TO_BW","🔄 Fairway Buoy → Breakwater (2h)"),
                 ("SAILING_CROSS_BW_IN_AC","🔄 Crossing Breakwater inbound (0.5h)"),
                 ("SAILING_BW_TO_A","🔄 Breakwater → SanBarth (1.5h)"),
                 ("WAITING_TIDAL","🌊 Holding — tidal window"),
                 ("WAITING_DAYLIGHT","🌙 Holding — daylight window"),
                 ("WAITING_RETURN_STOCK","⏳ Holding — return stock too low")]},
    {"display":"Returning → Westmore (Sego)","sim_value":"En Route BIA→Storage",
     "field_zone":"Transit","target_storage":"Westmore",
     "statuses":[("SAILING_B_TO_FWY","🔄 BIA → Fairway Buoy (2h)"),
                 ("SAILING_FWY_TO_BW","🔄 Fairway Buoy → Breakwater (2h)"),
                 ("SAILING_CROSS_BW_IN_AC","🔄 Crossing Breakwater inbound (0.5h)"),
                 ("SAILING_BW_TO_A","🔄 Breakwater → Westmore (1.5h)"),
                 ("WAITING_TIDAL","🌊 Holding — tidal window"),
                 ("WAITING_DAYLIGHT","🌙 Holding — daylight window"),
                 ("WAITING_RETURN_STOCK","⏳ Holding — return stock too low")]},
    {"display":"Returning → Starturn (Dawes)","sim_value":"En Route BIA→Storage",
     "field_zone":"Transit","target_storage":"Starturn",
     "statuses":[("SAILING_B_TO_FWY","🔄 BIA → Fairway Buoy (2h)"),
                 ("SAILING_FWY_TO_BW","🔄 Fairway Buoy → Breakwater (2h)"),
                 ("SAILING_CROSS_BW_IN_AC","🔄 Crossing Breakwater inbound (0.5h)"),
                 ("SAILING_BA","🔄 Inbound → Starturn (Dawes)"),
                 ("WAITING_TIDAL","🌊 Holding — tidal window"),
                 ("WAITING_DAYLIGHT","🌙 Holding — daylight window"),
                 ("WAITING_RETURN_STOCK","⏳ Holding — return stock too low")]},
    {"display":"Returning Duke from BIA","sim_value":"En Route BIA→Storage",
     "field_zone":"Transit","target_storage":"Duke",
     "statuses":[("SAILING_B_TO_BW_IN","🚢 BIA → clear breakwater (1.5h)"),
                 ("SAILING_CROSS_BW_IN","🚢 Crossing Breakwater inbound (0.5h, tidal)"),
                 ("SAILING_BW_TO_CH_IN","🚢 Breakwater → Channel (1h, tidal)"),
                 ("SAILING_CH_TO_D","🚢 Channel → Point D (3h, tidal)"),
                 ("WAITING_TIDAL","🌊 Waiting — tidal window")]},
]

LOC_BY_DISPLAY   = {e["display"]: e for e in LOCATION_CATALOGUE}
LOC_DISPLAY_LIST = [e["display"] for e in LOCATION_CATALOGUE]

VESSEL_LOC_FILTER = {
    "Watson":  {"SanBarth","Sego","BIA","Transit"},
    "Bedford": {"SanBarth","Ibom","BIA","Transit"},
    "Balham":  {"SanBarth","Ibom","BIA","Transit"},
}
VESSEL_DEFAULT_LOC = {
    "Bedford":  "Ibom (Offshore Buoy)",
    "Balham":   "Ibom (Offshore Buoy)",
    "Woodstock":"Duke (Awoba)",
    "Sherlock": "BIA — Fairway Buoy",
    "Laphroaig":"BIA — Fairway Buoy",
    "Rathbone": "BIA — Fairway Buoy",
    "Bagshot":  "BIA — Fairway Buoy",
    "Watson":   "BIA — Fairway Buoy",
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def loc_opts_for(vn):
    allowed = VESSEL_LOC_FILTER.get(vn)
    if allowed:
        return [e["display"] for e in LOCATION_CATALOGUE if e["field_zone"] in allowed]
    return LOC_DISPLAY_LIST

def default_loc_idx(vn, opts):
    d = VESSEL_DEFAULT_LOC.get(vn, opts[0])
    try: return opts.index(d)
    except ValueError: return 0

def default_status_idx(vn, statuses):
    codes = [c for c, _ in statuses]
    if vn in ("Sherlock","Laphroaig","Rathbone","Bagshot","Watson"):
        if "WAITING_RETURN_STOCK" in codes: return codes.index("WAITING_RETURN_STOCK")
    if vn == "Bedford"  and "PF_LOADING"  in codes: return codes.index("PF_LOADING")
    if vn == "Balham"   and "BERTHING_B"  in codes: return codes.index("BERTHING_B")
    if vn == "Woodstock" and "LOADING"    in codes: return codes.index("LOADING")
    return 0

def col_hdrs(labels, widths):
    for col, lbl in zip(st.columns(widths), labels):
        col.markdown(
            f'<div style="font-size:9px;font-weight:800;color:#94a3b8;'
            f'letter-spacing:.09em;text-transform:uppercase;padding-bottom:2px">'
            f'{lbl}</div>', unsafe_allow_html=True)


# ── Vessel row renderer ────────────────────────────────────────────────────────

def render_vessel_row(vn, mod):
    vcap  = (getattr(mod,"VESSEL_CAPACITIES",{}).get(vn, 85_000) if mod else 85_000)
    vcol  = VESSEL_COLORS.get(vn, "#aaa")
    opts  = loc_opts_for(vn)
    rc    = st.columns([2, 4, 3, 2])

    with rc[0]:
        cur = st.session_state.get(f"vp_vl_{vn}", opts[default_loc_idx(vn, opts)])
        zone = LOC_BY_DISPLAY.get(cur, {}).get("field_zone","Transit")
        zb, zc = ZONE_BADGE.get(zone, ("⚪","#94a3b8"))
        st.markdown(
            f'<div style="padding-top:28px">'
            f'<span style="background:{vcol};color:#fff;border-radius:18px;'
            f'padding:4px 13px;font-weight:700;font-size:12px">{vn}</span>'
            f'<span style="font-size:10px;font-weight:700;color:{zc};display:block;margin-top:4px">'
            f'{zb} {zone}</span></div>', unsafe_allow_html=True)

    with rc[1]:
        sel_loc = st.selectbox("Loc", opts, index=default_loc_idx(vn, opts),
                               key=f"vp_vl_{vn}", label_visibility="collapsed")
        loc_entry    = LOC_BY_DISPLAY[sel_loc]
        loc_statuses = loc_entry["statuses"]

    with rc[2]:
        stat_labels = [lbl for _, lbl in loc_statuses]
        stat_codes  = {lbl: code for code, lbl in loc_statuses}
        def_idx     = default_status_idx(vn, loc_statuses)
        sel_stat = st.selectbox("Status", stat_labels, index=def_idx,
                                key=f"vp_vs_{vn}", label_visibility="collapsed",
                                help="Only statuses valid at the selected location are shown.")
        st_code = stat_codes[sel_stat]

    with rc[3]:
        cargo_def = vcap if ("Discharging" in sel_stat or "Loading" in sel_stat) else 0
        cg = st.number_input("Cargo", 0, vcap*2, cargo_def, step=1_000,
                             key=f"vp_vc_{vn}", label_visibility="collapsed",
                             help=f"Capacity: {vcap:,} bbl")
        if cg > vcap:
            st.caption(f"⚠️ {cg-vcap:,} bbl over cap → overflow")

    # ── Partial-discharge panel ────────────────────────────────────────────────
    # Shown when a vessel is mid-discharge (HOSE_CONNECT_B or DISCHARGING).
    # Operator enters volume already pumped into the mother; we debit this from
    # the daughter's cargo so the sim correctly computes remaining pump time.
    already_xfr = 0
    if st_code in {"HOSE_CONNECT_B", "DISCHARGING"} and cg > 0:
        _disch_hours = 12.0  # fixed full-cargo discharge duration (DISCHARGE_HOURS)
        _hose_hours  = 2.0   # hose connection time before pumping starts (HOSE_CONNECTION_HOURS)
        _mother_name = loc_entry.get("target_mother", "mother vessel")

        st.markdown(
            '<div style="background:#f0f9ff;border:1px solid #bae6fd;border-left:3px solid #0ea5e9;'
            'border-radius:6px;padding:8px 12px;margin:4px 0 2px 0">',
            unsafe_allow_html=True)

        _xfr_cols = st.columns([3, 4, 3])
        with _xfr_cols[0]:
            st.markdown(
                '<div style="font-size:10px;font-weight:800;color:#0369a1;letter-spacing:.07em;'
                'text-transform:uppercase;padding-bottom:2px">⬇ Already received by '
                f'{_mother_name}</div>', unsafe_allow_html=True)
            already_xfr = st.number_input(
                "Already xfr", 0, int(cg), 0, step=1_000,
                key=f"vp_xfr_{vn}", label_visibility="collapsed",
                help=f"Volume already pumped into {_mother_name}. "
                     f"This is debited from the {cg:,} bbl cargo to calculate remaining pump time.")

        with _xfr_cols[1]:
            remaining = max(0, cg - already_xfr)
            if st_code == "HOSE_CONNECT_B":
                # Hose not yet open — full discharge still ahead plus hose time
                remaining_pump_h = _hose_hours + _disch_hours
                phase_label = f"Hose connecting ({_hose_hours:.0f}h) + full discharge ({_disch_hours:.0f}h)"
            else:
                # Already pumping — only the untransferred volume remains
                remaining_pump_h = (remaining / cg * _disch_hours) if cg > 0 else 0.0
                phase_label = f"Pumping: {remaining:,} bbl remaining"

            hrs = int(remaining_pump_h)
            mins = int((remaining_pump_h % 1) * 60)
            time_str = f"{hrs}h {mins:02d}m"

            pct_done = (already_xfr / cg * 100) if cg > 0 else 0
            bar_filled = int(pct_done / 5)
            bar = "█" * bar_filled + "░" * (20 - bar_filled)

            st.markdown(
                f'<div style="font-size:10px;font-weight:700;color:#0c4a6e;margin-bottom:3px">'
                f'{phase_label}</div>'
                f'<div style="font-family:monospace;font-size:11px;color:#0369a1">{bar} {pct_done:.0f}%</div>'
                f'<div style="font-size:11px;color:#075985;margin-top:2px">'
                f'Time to complete: <b>{time_str}</b></div>',
                unsafe_allow_html=True)

        with _xfr_cols[2]:
            if already_xfr > 0:
                st.markdown(
                    f'<div style="font-size:10px;color:#0369a1;font-weight:700;margin-top:4px">'
                    f'Transferred<br>'
                    f'<span style="font-size:16px;color:#0284c7">{already_xfr:,}</span> bbl<br>'
                    f'<span style="color:#64748b;font-size:10px">of {cg:,} bbl cargo</span></div>',
                    unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)

    return {"status": st_code, "cargo_bbl": int(cg),
            "already_transferred_bbl": int(already_xfr),
            "location": loc_entry["sim_value"],
            "target_storage": loc_entry.get("target_storage"),
            "target_mother":  loc_entry.get("target_mother")}


# ── Mother row renderer ────────────────────────────────────────────────────────

def render_mother_row(mn, mod):
    cap   = int(getattr(mod,"MOTHER_CAPACITY_BBL", MOTHER_CAP_FALLBACK)) if mod else MOTHER_CAP_FALLBACK
    color = MOTHER_COLORS.get(mn,"#aaa")
    mc    = st.columns([2,2,2])
    with mc[0]:
        st.markdown(
            f'<div style="padding-top:28px">'
            f'<span style="background:{color};color:#fff;border-radius:18px;'
            f'padding:4px 13px;font-weight:700;font-size:12px">🛢️ {mn}</span>'
            f'</div>', unsafe_allow_html=True)
    with mc[1]:
        bbl = st.number_input(f"{mn} vol", 0, cap, 0, step=10_000,
                              key=f"vp_mv_{mn}", label_visibility="collapsed",
                              help=f"Opening stock on {mn}. Capacity: {cap:,} bbl")
    with mc[2]:
        api_val = st.number_input(f"{mn} API°", 0.0, 60.0,
                                  value=32.0 if bbl > 0 else 0.0,
                                  step=0.1, format="%.2f",
                                  key=f"vp_mapi_{mn}", label_visibility="collapsed",
                                  disabled=(bbl==0),
                                  help=f"API gravity of stock on {mn}. Ignored when stock = 0.")
        if bbl > 0:
            st.caption(f"{bbl:,} bbl · {api_val:.2f}°API")
    return int(bbl), float(api_val)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    # CSS
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;600;700;800&display=swap');
html,body,[class*="css"]{font-family:'DM Sans','Segoe UI',sans-serif}
.block-container{padding-top:1rem;background:#f8f9fb}
.vp-hdr{background:linear-gradient(90deg,#1a2744,#243460);border-left:4px solid #3b82f6;
  padding:10px 18px;border-radius:7px;margin-bottom:14px;color:#fff;
  font-weight:700;font-size:15px;letter-spacing:.03em}
.ok-banner{background:#f0fdf4;border:1px solid #22c55e;border-left:4px solid #22c55e;
  border-radius:7px;padding:10px 16px;color:#14532d;font-size:13px;margin:8px 0}
</style>""", unsafe_allow_html=True)

    st.markdown('<div class="vp-hdr">✏️ Enter 08:00 Vessel Positions</div>',
                unsafe_allow_html=True)
    st.caption(
        "Select each vessel's location — only valid statuses for that location are shown. "
        "Mother volumes set BIA opening stock. "
        "Press **Confirm** then switch to **Tanker Ops** — the sim seeds from these positions automatically."
    )

    mod = _load_sim_mod()
    if mod is None:
        st.warning("⚠️ tanker_simulation_v5.py not found — using default 85,000 bbl vessel capacities.")

    # Confirmed banner
    if st.session_state.get("vp_confirmed"):
        st.markdown(
            '<div class="ok-banner">✅ Positions confirmed and sent to simulation. '
            'Switch to <b>Tanker Ops</b> in the sidebar. Edit below and re-confirm to update.</div>',
            unsafe_allow_html=True)

    # Zero-cargo badge
    n_zero = sum(1 for vn in ALL_VESSELS if st.session_state.get(f"vp_vc_{vn}", 0) == 0)
    if n_zero:
        st.markdown(
            f'<span style="background:#fef3c7;color:#92400e;border-radius:10px;'
            f'padding:2px 10px;font-size:12px;font-weight:600">'
            f'⚠️ {n_zero} vessel{"s" if n_zero!=1 else ""} at 0 bbl</span>',
            unsafe_allow_html=True)
        st.markdown("")

    # Zone legend
    zone_html = " ".join(
        f'<span style="display:inline-flex;align-items:center;gap:4px;background:{zc}22;'
        f'border:1px solid {zc}55;border-radius:5px;padding:2px 9px;font-size:11px;'
        f'font-weight:600;color:{zc};margin:2px">{zb} {zn}</span>'
        for zn,(zb,zc) in ZONE_BADGE.items())
    st.markdown(f'<div style="margin:4px 0 14px;line-height:2.2">{zone_html}</div>',
                unsafe_allow_html=True)

    # ── Daughter vessels ───────────────────────────────────────────────────────
    st.markdown('<div style="font-size:13px;font-weight:800;color:#1a2332;margin-bottom:6px">'
                '🚢 Daughter Vessels</div>', unsafe_allow_html=True)
    col_hdrs(["VESSEL","LOCATION","STATUS (filtered by location)","CARGO (bbl)"], [2,4,3,2])
    st.markdown('<hr style="margin:2px 0 8px;border-color:#e2e8f0">', unsafe_allow_html=True)

    vessel_states = {}
    for vn in ALL_VESSELS:
        vessel_states[vn] = render_vessel_row(vn, mod)
        st.markdown('<div style="height:2px"></div>', unsafe_allow_html=True)

    # ── Mother vessels ─────────────────────────────────────────────────────────
    st.divider()
    st.markdown('<div style="font-size:13px;font-weight:800;color:#1a2332;margin-bottom:4px">'
                '🛢️ Mother Vessels at BIA — Volume &amp; API</div>', unsafe_allow_html=True)
    st.caption("Set opening stock on each mother vessel. API gravity is used for blend tracking.")
    col_hdrs(["MOTHER VESSEL","VOLUME (bbl)","API GRAVITY (°)"], [2,2,2])
    st.markdown('<hr style="margin:2px 0 8px;border-color:#3d566e">', unsafe_allow_html=True)

    mother_vols, mother_apis = {}, {}
    for mn in ALL_MOTHERS:
        bbl, api = render_mother_row(mn, mod)
        mother_vols[mn.lower()] = bbl
        mother_apis[mn.lower()] = api
        st.markdown('<div style="height:2px"></div>', unsafe_allow_html=True)

    # ── Confirm / Reset ────────────────────────────────────────────────────────
    st.divider()
    c1, c2, _ = st.columns([3,2,5])
    with c1:
        confirm = st.button("✅ Confirm & Send to Simulation",
                            type="primary", use_container_width=True)
    with c2:
        if st.button("🔄 Reset All", use_container_width=True):
            for vn in ALL_VESSELS:
                for sfx in ("vl","vs","vc","xfr"):
                    st.session_state.pop(f"vp_{sfx}_{vn}", None)
            for mn in ALL_MOTHERS:
                st.session_state.pop(f"vp_mv_{mn}", None)
                st.session_state.pop(f"vp_mapi_{mn}", None)
            for k in ("vp_confirmed","vp_vessel_states","vp_mother_vols","vp_mother_apis"):
                st.session_state.pop(k, None)
            st.rerun()

    if confirm:
        st.session_state["vp_vessel_states"] = vessel_states
        st.session_state["vp_mother_vols"]   = mother_vols
        st.session_state["vp_mother_apis"]   = mother_apis
        st.session_state["vp_confirmed"]      = True
        st.success("✅ Positions confirmed! Switch to **Tanker Ops** in the sidebar.", icon="🚢")

    # ── Summary table ──────────────────────────────────────────────────────────
    with st.expander("📊 Position summary table", expanded=False):
        rows = []
        for vn in ALL_VESSELS:
            vs = vessel_states.get(vn, {})
            icon = STATUS_ICONS.get(vs.get("status",""),"❓")
            xfr  = vs.get("already_transferred_bbl", 0)
            rows.append({"Vessel":vn, "Location":vs.get("location","—"),
                         "Status":f'{icon} {vs.get("status","—")}',
                         "Cargo bbl":f'{vs.get("cargo_bbl",0):,}',
                         "Xfr'd bbl": f'{xfr:,}' if xfr else "—",
                         "→ Mother":vs.get("target_mother") or "—",
                         "→ Storage":vs.get("target_storage") or "—"})
        for mn in ALL_MOTHERS:
            rows.append({"Vessel":f"🛢️ {mn}","Location":"BIA","Status":"Mother vessel",
                         "Cargo bbl":f'{mother_vols.get(mn.lower(),0):,}',
                         "→ Mother":"—","→ Storage":"—"})
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # ── Integration shim ───────────────────────────────────────────────────────
    with st.expander("🔧 One-time integration shim — paste once into tanker_app.py", expanded=False):
        st.markdown(
            "Paste this block into `tanker_app.py` **immediately before** the "
            "`# Build vessel_states_json` comment (around line 2738):"
        )
        st.code("""\
# ── Pull positions from vessel_positions page if confirmed ────────────────
_vp_states = st.session_state.get("vp_vessel_states")
_vp_mvols  = st.session_state.get("vp_mother_vols", {})
_vp_mapis  = st.session_state.get("vp_mother_apis", {})

if _vp_states and st.session_state.get("vp_confirmed"):
    for _vn, _vd in _vp_states.items():
        if _vn not in manual_states and (
                fleet_df.empty or _vn not in fleet_df["vessel"].values):
            manual_states[_vn] = _vd
    for _mk, _mv in _vp_mvols.items():
        if _mk not in gs_vols:
            manual_mother[_mk] = _mv
    for _mk, _ma in _vp_mapis.items():
        if _mk not in manual_mother_api:
            manual_mother_api[_mk] = _ma
    st.sidebar.success("🚢 Positions loaded from position entry page", icon="✅")
# ─────────────────────────────────────────────────────────────────────────────
""", language="python")
        st.caption("That's the only change needed in tanker_app.py.")


if __name__ == "__main__":
    main()
