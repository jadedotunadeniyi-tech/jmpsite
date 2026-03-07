"""
=============================================================
OIL TANKER DAUGHTER VESSEL OPERATION SIMULATION  (v5)
=============================================================
Simulates the continuous loading/offloading cycle between:
    - Storage Vessel (Chapel / Point A)  - capacity 800,000 bbls
    - Daughter Vessels: Sherlock, Laphroaig, Rathbone, Bedford, Balham, Woodstock, Bagshot
    - Mother Vessel (Bryanston / Point B) - capacity 550,000 bbls

v5 changes — Multi-point independent storage loading at Point A/C/D/E:
    Point A has two active storage load points (Chapel and JasmineS).
    Point C has one active storage load point (Westmore).
    Point D has one active storage load point (Duke).
    Point E has one active storage load point (Starturn).
    Each load point has its own berth timeline and stock level.
    Daughter vessels may berth/load from either load point based on
    available stock and berth timing, allowing parallel operations.
=============================================================
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from datetime import datetime, timedelta, date as _date
import random

# Version identifier — read by tanker_app.py to auto-clear Streamlit cache
# on deployment. Bump this string whenever the sim logic changes in a way
# that would invalidate cached run_sim() results.
SIM_VERSION = "5.4"

# -----------------------------------------------------------------
# PRODUCTION API GRAVITY (degrees API per source)
# -----------------------------------------------------------------
STORAGE_API = {
    "Chapel"  : 29.00,
    "JasmineS": 43.36,
    "Westmore" : 31.10,
    "Duke"    : 41.20,
    "Starturn" : 39.54,
}
IBOM_API = 32.00   # Point F (Bedford / Balham)

# -----------------------------------------------------------------
# SIMULATION EPOCH  (set via set_sim_epoch before instantiating)
# -----------------------------------------------------------------
_SIM_EPOCH = datetime(2025, 1, 1, 8, 0)   # default; overridden by set_sim_epoch() — t=0 = 08:00
SIM_HOUR_OFFSET = 8  # t=0 is 08:00 wall-clock; add this to sim-hours before window comparisons

def set_sim_epoch(d):
    """Set the calendar start date for the simulation (accepts date or datetime).
    t=0 is anchored to 08:00 on the given date so all displayed times start at 08:00.
    """
    global _SIM_EPOCH
    if isinstance(d, _date) and not isinstance(d, datetime):
        d = datetime(d.year, d.month, d.day, 8, 0)  # anchor t=0 to 08:00
    elif isinstance(d, datetime) and d.hour == 0 and d.minute == 0:
        d = d.replace(hour=8)  # upgrade midnight datetime to 08:00
    _SIM_EPOCH = d

# -----------------------------------------------------------------
# TIDAL TABLE  (loaded via load_tide_table before instantiating)
# -----------------------------------------------------------------
# _TIDE_TABLE maps absolute_hour (float) -> tide_height_m (float).
# If None, tidal gating is disabled and only daylight applies.
_TIDE_TABLE = None          # {float: float}  hour -> height
TIDE_MIN_CROSSING_M = 1.6   # minimum high-tide height for breakwater crossing

def load_tide_table(csv_path):
    """
    Parse a tidal prediction CSV into _TIDE_TABLE.
    Expected columns (case-insensitive, flexible separators):
        Date       — DD/MM/YYYY  or  YYYY-MM-DD
        Time       — HH:MM
        Tide_Height_m (or Height, or Level)
    Rows are interpolated onto every 0.5 h slot covering the sim period.
    """
    global _TIDE_TABLE
    import csv as _csv, re as _re

    raw = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        # Detect delimiter
        sample = f.read(2048); f.seek(0)
        delim = "," if sample.count(",") >= sample.count(";") else ";"
        reader = _csv.DictReader(f, delimiter=delim)
        # Normalise column names
        for row in reader:
            norm = {k.strip().lower().replace(" ","_"): v.strip() for k,v in row.items()}
            raw.append(norm)

    if not raw:
        _TIDE_TABLE = None
        return

    # Find column names
    date_col   = next((k for k in raw[0] if "date" in k), None)
    time_col   = next((k for k in raw[0] if "time" in k), None)
    height_col = next((k for k in raw[0]
                       if any(x in k for x in ("height","tide","level","m_"))), None)
    if not (date_col and time_col and height_col):
        _TIDE_TABLE = None
        return

    parsed = {}   # datetime -> float
    for row in raw:
        try:
            ds = row[date_col]; ts = row[time_col]; hs = row[height_col]
            if not hs: continue
            # Parse date
            if "/" in ds:
                parts = ds.split("/")
                if len(parts[2]) == 4:   # DD/MM/YYYY
                    dt_date = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
                else:                    # YYYY/MM/DD
                    dt_date = datetime(int(parts[0]), int(parts[1]), int(parts[2]))
            else:
                dt_date = datetime.fromisoformat(ds.split("T")[0])
            # Parse time
            hh, mm = int(ts[:2]), int(ts[3:5])
            dt = dt_date.replace(hour=hh, minute=mm)
            height = float(_re.sub(r"[^0-9.\-]","", hs))
            parsed[dt] = height
        except Exception:
            continue

    if not parsed:
        _TIDE_TABLE = None
        return

    # Build absolute-hour lookup keyed on hours-since-_SIM_EPOCH
    table = {}
    for dt, h in parsed.items():
        diff = (dt - _SIM_EPOCH).total_seconds() / 3600.0
        table[round(diff * 2) / 2] = h   # snap to nearest 0.5 h

    # Interpolate to fill every 0.5 h slot in the sim window (0 .. 365*24)
    if table:
        sorted_keys = sorted(table)
        full = {}
        for slot in [x * 0.5 for x in range(int(sorted_keys[-1] * 2) + 2)]:
            if slot in table:
                full[slot] = table[slot]
            else:
                # linear interpolation between nearest neighbours
                lo = max((k for k in sorted_keys if k <= slot), default=None)
                hi = min((k for k in sorted_keys if k >= slot), default=None)
                if lo is not None and hi is not None and hi != lo:
                    t_frac = (slot - lo) / (hi - lo)
                    full[slot] = table[lo] + t_frac * (table[hi] - table[lo])
                elif lo is not None:
                    full[slot] = table[lo]
                elif hi is not None:
                    full[slot] = table[hi]
        _TIDE_TABLE = full
    else:
        _TIDE_TABLE = None

# -----------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------
SIMULATION_DAYS        = 30          # How many days to simulate
DAUGHTER_CARGO_BBL     = 85_000      # Fixed load per voyage

# Optional validation scenario for Point B distribution logic.
# Keep disabled by default; enable per run from the app when needed.
POINT_B_DISTRIBUTION_TEST_MODE = False
POINT_B_DISTRIBUTION_TEST_DAYS = 3

VESSEL_NAMES           = [
    "Sherlock",    # loads 1st in every cycle
    "Laphroaig",   # loads 2nd
    "Rathbone",    # loads 3rd
    "Bedford",     # loads 4th
    "Balham",      # loads 5th
    "Woodstock",   # loads 6th
    "Bagshot",     # loads 7th
    "Watson",      # loads 8th (Point A/C)
]
VESSEL_CAPACITIES      = {
    "Rathbone" : 44_000,
    "Bedford"  : 85_000,
    "Balham"   : 85_000,
    "Woodstock": 42_000,
    "Bagshot"  : 43_000,
    "Watson"   : 85_000,
}
NUM_DAUGHTERS          = len(VESSEL_NAMES)

MAX_DAUGHTER_CARGO = max(VESSEL_CAPACITIES.values(), default=DAUGHTER_CARGO_BBL)

STORAGE_CAPACITY_BBL   = 270_000
MOTHER_CAPACITY_BBL    = 550_000
STORAGE_INIT_BBL       = 400_000
MOTHER_INIT_BBL        = 0
PRODUCTION_RATE_BPH    = 1_700        # barrels per hour (replaces daily rate)
WESTMORE_PRODUCTION_RATE_BPH = 833
DUKE_PRODUCTION_RATE_BPH = 250
DUKE_STORAGE_CAPACITY_BBL = 90_000
STARTURN_PRODUCTION_RATE_BPH = 83
STARTURN_STORAGE_CAPACITY_BBL = 70_000
DUKE_STARTURN_DEAD_STOCK_BBL = 42_000
DEAD_STOCK_FACTOR      = 1.75         # vessel must wait until 175% of its cargo is available
# Point A/C ↔ BIA route leg durations (per journey plan)
SAIL_HOURS_A_TO_BW      = 1.5   # Point A/C → Breakwater
SAIL_HOURS_CROSS_BW_AC  = 0.5   # Cross Breakwater (daylight/tidal)
SAIL_HOURS_BW_TO_FWY    = 2.0   # After crossing → Fairway Buoy (daylight)
SAIL_HOURS_FWY_TO_B     = 2.0   # Fairway Buoy → BIA (daylight)
SAIL_HOURS_B_TO_FWY     = 2.0   # BIA → Fairway Buoy
SAIL_HOURS_FWY_TO_BW    = 2.0   # Fairway Buoy → Breakwater
SAIL_HOURS_BW_TO_A      = 1.5   # After crossing → Point A/C
# Legacy aliases
SAIL_HOURS_A_TO_B = SAIL_HOURS_A_TO_BW + SAIL_HOURS_CROSS_BW_AC + SAIL_HOURS_BW_TO_FWY + SAIL_HOURS_FWY_TO_B
SAIL_HOURS_B_TO_A = SAIL_HOURS_B_TO_FWY + SAIL_HOURS_FWY_TO_BW + SAIL_HOURS_CROSS_BW_AC + SAIL_HOURS_BW_TO_A
SAIL_HOURS_B_TO_F      = 3   # BIA → Ibom (offshore buoy)
# Point D ↔ BIA route leg durations (per journey plan)
SAIL_HOURS_D_TO_CH      = 3.0   # D → Cawthorne Channel (daylight/tidal)
SAIL_HOURS_CH_TO_BW_OUT = 1.0   # Cawthorne Channel → Breakwater (daylight/tidal)
SAIL_HOURS_CROSS_BW     = 0.5   # Cross Breakwater (daylight/tidal)
SAIL_HOURS_BW_TO_B      = 1.5   # Clear breakwater → BIA
SAIL_HOURS_B_TO_BW      = 1.5   # BIA → clear breakwater
SAIL_HOURS_BW_TO_CH_IN  = 1.0   # Breakwater → Cawthorne Channel (daylight/tidal)
SAIL_HOURS_CH_TO_D      = 3.0   # Cawthorne Channel → Point D (daylight/tidal)
# Keep legacy names for any remaining references
SAIL_HOURS_D_TO_CHANNEL = SAIL_HOURS_D_TO_CH
SAIL_HOURS_CHANNEL_TO_B = SAIL_HOURS_CH_TO_BW_OUT + SAIL_HOURS_CROSS_BW + SAIL_HOURS_BW_TO_B
BERTHING_DELAY_HOURS   = 0.5
POST_BERTHING_START_GAP_HOURS = 0.5
POST_MOTHER_BERTHING_START_GAP_HOURS = 1.0
HOSE_CONNECTION_HOURS  = 2.0
LOAD_HOURS             = 12
# Per-storage loading rates (bph) — used by storage_load_hours()
CHAPEL_LOAD_RATE_BPH   = 7_083   # 85,000 bbl / 12h
JASMINES_LOAD_RATE_BPH = 7_083   # 85,000 bbl / 12h
WESTMORE_LOAD_RATE_BPH = 2_500
DUKE_LOAD_RATE_BPH     = 3_500
STARTURN_LOAD_RATE_BPH = 2_500
POINT_F_LOAD_RATE_BPH  = 165
POINT_F_SWAP_HOURS     = 2
POINT_F_MIN_TRIGGER_BBL = 65_000
STARTURN_PRE_TANK_TOP_TRIGGER_RATIO = 0.90
DUKE_PRE_TANK_TOP_TRIGGER_RATIO = 0.90
PRE_TANK_TOP_TRIGGER_RATIO_DEFAULT = 0.90
DUKE_MIN_REMAINING_BBL = 7_500
STARTURN_MIN_REMAINING_BBL = 5_000
DISCHARGE_HOURS        = 12
CAST_OFF_HOURS         = 0.2
CAST_OFF_START         = 6
CAST_OFF_END           = 17.5
BERTHING_START         = 6
BERTHING_END           = 18
DAYLIGHT_START         = 6
DAYLIGHT_END           = 18
MOTHER_EXPORT_TRIGGER  = MOTHER_CAPACITY_BBL - MAX_DAUGHTER_CARGO
MOTHER_EXPORT_VOLUME   = 400_000
TIME_STEP_HOURS        = 0.5
EXPORT_RATE_BPH        = 16_000
EXPORT_DOC_HOURS       = 2
EXPORT_SAIL_HOURS      = 6
EXPORT_SAIL_WINDOW_START = 6
EXPORT_SAIL_WINDOW_END   = 15
EXPORT_HOSE_HOURS       = 4
EXPORT_SERIES_BUFFER_HOURS = 56

STORAGE_PRIMARY_NAME = "Chapel"
STORAGE_SECONDARY_NAME = "JasmineS"
STORAGE_TERTIARY_NAME = "Westmore"
STORAGE_QUATERNARY_NAME = "Duke"
STORAGE_QUINARY_NAME = "Starturn"
WESTMORE_PERMITTED_VESSELS = {"Sherlock", "Laphroaig", "Bagshot", "Rathbone", "Watson"}
DUKE_PERMITTED_VESSELS = {"Woodstock", "Bagshot", "Rathbone"}
STARTURN_PERMITTED_VESSELS = {"Woodstock", "Rathbone"}
POINT_A_ONLY_VESSELS = set()   # Watson now also loads from Point C (Westmore)

# Bedford and Balham are 85k vessels but may only load 63k bbl at Point A
# (Chapel / JasmineS).  Their full capacity is used at all other points.
POINT_A_LOAD_CAP_VESSELS = {"Bedford", "Balham"}
POINT_A_LOAD_CAP_BBL     = 63_000
STORAGE_NAMES = [
    STORAGE_PRIMARY_NAME,
    STORAGE_SECONDARY_NAME,
    STORAGE_TERTIARY_NAME,
    STORAGE_QUATERNARY_NAME,
    STORAGE_QUINARY_NAME,
]
STORAGE_POINT = {
    STORAGE_PRIMARY_NAME: "A",
    STORAGE_SECONDARY_NAME: "A",
    STORAGE_TERTIARY_NAME: "C",
    STORAGE_QUATERNARY_NAME: "D",
    STORAGE_QUINARY_NAME: "E",
}
STORAGE_CAPACITY_BY_NAME = {name: STORAGE_CAPACITY_BBL for name in STORAGE_NAMES}
STORAGE_CAPACITY_BY_NAME[STORAGE_SECONDARY_NAME] = 290_000
STORAGE_CAPACITY_BY_NAME[STORAGE_TERTIARY_NAME] = 270_000
STORAGE_CAPACITY_BY_NAME[STORAGE_QUATERNARY_NAME] = DUKE_STORAGE_CAPACITY_BBL
STORAGE_CAPACITY_BY_NAME[STORAGE_QUINARY_NAME] = STARTURN_STORAGE_CAPACITY_BBL
STORAGE_PRODUCTION_RATE_BY_NAME = {name: PRODUCTION_RATE_BPH for name in STORAGE_NAMES}
STORAGE_PRODUCTION_RATE_BY_NAME[STORAGE_TERTIARY_NAME] = WESTMORE_PRODUCTION_RATE_BPH
STORAGE_PRODUCTION_RATE_BY_NAME[STORAGE_QUATERNARY_NAME] = DUKE_PRODUCTION_RATE_BPH
STORAGE_PRODUCTION_RATE_BY_NAME[STORAGE_QUINARY_NAME] = STARTURN_PRODUCTION_RATE_BPH
# Optional temporary production overrides by date window.
# Format:
# [
#   {
#     "start_date": "YYYY-MM-DD",
#     "end_date": "YYYY-MM-DD",
#     "rates": {"Chapel": 0, "JasmineS": 0, ...}
#   }
# ]
PRODUCTION_RATE_OVERRIDES = []
STORAGE_CRITICAL_THRESHOLD_BY_NAME = {
    STORAGE_PRIMARY_NAME: 270_000,
    STORAGE_SECONDARY_NAME: 290_000,
    STORAGE_TERTIARY_NAME: 270_000,
    STORAGE_QUATERNARY_NAME: 90_000,
    STORAGE_QUINARY_NAME: 70_000,
}
MOTHER_PRIMARY_NAME = "Bryanston"
MOTHER_SECONDARY_NAME = "Alkebulan"
MOTHER_TERTIARY_NAME = "GreenEagle"
MOTHER_NAMES = [MOTHER_PRIMARY_NAME, MOTHER_SECONDARY_NAME, MOTHER_TERTIARY_NAME]

# Startup-day (Day 1) Point B nomination override.
# When enabled, Point B auto-prioritization is disabled only on Day 1 and
# assignment must come from this manual vessel->mother mapping.
# Day 2+ always uses the standard strict Point B prioritization rules.
STARTUP_DAY_DISABLE_POINT_B_PRIORITY = True
STARTUP_DAY_POINT_B_MANUAL_NOMINATIONS = {
    # Example:
    # "Sherlock": MOTHER_PRIMARY_NAME,
}

# Test seed: force selected vessels to Point B at full load on startup.
POINT_B_TEST_STARTUP_FULL_LOAD_NOMINATIONS = {
    # Example:
    # "Sherlock": MOTHER_PRIMARY_NAME,
}

# -----------------------------------------------------------------
# STATE TRACKING
# -----------------------------------------------------------------
STATUS_CODES = {
    "IDLE_A"            : "Idle at assigned loading point (A/C/D/E)",
    "WAITING_BERTH_A"   : "Waiting for berthing window at assigned loading point",
    "BERTHING_A"        : "Berthing at assigned loading point",
    "HOSE_CONNECT_A"    : "Hose connection at assigned loading point",
    "LOADING"           : "Loading at assigned loading point",
    "DOCUMENTING"       : "Documentation after loading",
    "CAST_OFF"          : "Cast-off from storage vessel",
    "CAST_OFF_B"        : "Cast-off from mother vessel",
    "WAITING_CAST_OFF"  : "Waiting for cast-off window",
    "EXPORT_DOC"        : "Mother export documentation",
    "EXPORT_SAIL"       : "Sailing to export terminal",
    "EXPORT_HOSE"       : "Hose connection at export terminal",
    "SAILING_AB"          : "Sailing A/C -> Breakwater (1.5h)",
    "SAILING_CROSS_BW_AC" : "Crossing Breakwater A/C outbound (0.5h, tidal)",
    "SAILING_BW_TO_FWY"   : "After crossing -> Fairway Buoy (2h)",
    "SAILING_AB_LEG2"     : "Sailing Fairway Buoy -> BIA (2h)",
    "SAILING_B_TO_FWY"    : "Returning BIA -> Fairway Buoy (2h)",
    "SAILING_FWY_TO_BW"   : "Fairway Buoy -> Breakwater (2h)",
    "SAILING_CROSS_BW_IN_AC": "Crossing Breakwater A/C inbound (0.5h, tidal)",
    "SAILING_BW_TO_A"     : "After crossing -> Point A/C (1.5h)",
    "SAILING_D_CHANNEL"          : "Sailing D -> Cawthorne Channel (3h)",
    "SAILING_CH_TO_BW_OUT"       : "Sailing Cawthorne Ch -> Breakwater (1h)",
    "SAILING_CROSS_BW_OUT"       : "Crossing Breakwater outbound (0.5h)",
    "SAILING_BW_TO_B"            : "Sailing clear breakwater -> BIA (1.5h)",
    "SAILING_B_TO_BW_IN"         : "Sailing BIA -> clear breakwater (1.5h)",
    "SAILING_CROSS_BW_IN"        : "Crossing Breakwater inbound (0.5h)",
    "SAILING_BW_TO_CH_IN"        : "Sailing Breakwater -> Cawthorne Ch (1h)",
    "SAILING_CH_TO_D"            : "Sailing Cawthorne Ch -> Point D (3h)",
    "WAITING_BERTH_B"   : "Waiting for berthing window at Point B mother",
    "BERTHING_B"        : "Berthing at Point B mother",
    "HOSE_CONNECT_B"    : "Hose connection at Point B mother",
    "IDLE_B"            : "Idle at Point B mother",
    "DISCHARGING"       : "Discharging to Point B mother",
    "SAILING_BA"        : "Returning B -> selected loading point (A/C/D/E)",
    "WAITING_DAYLIGHT"  : "Waiting for Daylight Window",
    "WAITING_FAIRWAY"   : "Waiting at Fairway Buoy",
    "WAITING_MOTHER_CAPACITY" : "Waiting for space on mother vessel",
    "WAITING_MOTHER_RETURN" : "Waiting for mother to return from export",
    "WAITING_DEAD_STOCK"    : "Berthed — waiting for dead-stock threshold",
    "WAITING_RETURN_STOCK"  : "Waiting at Point B until return destination can load immediately",
    "SAILING_B_TO_F"        : "Sailing BIA -> Ibom (swap takeover)",
    "PF_LOADING"            : "Loading at Point F",
    "PF_SWAP"               : "Point F swap/takeover in progress",
}


class DaughterVessel:
    def __init__(self, name, start_offset_hours=0, cargo_capacity=None):
        self.name = name
        self.cargo_capacity = cargo_capacity if cargo_capacity is not None else DAUGHTER_CARGO_BBL
        self.cargo_bbl = 0
        self.status = "IDLE_A"
        self.operation_start = None
        self.operation_end   = None
        self.next_event_time = start_offset_hours
        self.current_voyage = 0
        self.queue_position = None
        self.assigned_storage = None
        self.assigned_load_hours = None
        self.assigned_mother = None
        self.target_point = "A"
        # FIX 1: track exact arrival hour at Point B for FIFO queue ordering
        self.arrival_at_b = None

    def __repr__(self):
        return f"{self.name}[{self.status}|cargo={self.cargo_bbl:,}bbl]"


class Simulation:
    def __init__(self):
        self.storage_bbl = {
            name: min(STORAGE_INIT_BBL, STORAGE_CAPACITY_BY_NAME[name])
            for name in STORAGE_NAMES
        }
        self.mother_bbl = {name: MOTHER_INIT_BBL for name in MOTHER_NAMES}
        self.total_exported = 0
        self.total_produced = 0
        self.total_spilled = 0
        self.storage_overflow_bbl = {name: 0.0 for name in STORAGE_NAMES}
        self.point_f_overflow_accum_bbl = 0.0
        self.storage_overflow_events = 0

        # ── API gravity tracking ──────────────────────────────────────────
        # storage_api: weighted-average API of current inventory in each storage
        self.storage_api = {name: STORAGE_API.get(name, 0.0) for name in STORAGE_NAMES}
        # vessel_api: weighted-average API of cargo on board each vessel (by name)
        self.vessel_api  = {}   # populated when vessels are created
        # mother_api: weighted-average API of inventory in each mother vessel
        self.mother_api  = {name: 0.0 for name in MOTHER_NAMES}
        self.total_exported_api_bbl = 0.0   # sum(vol * api) for export tracking
        self.log = []
        self.timeline = []
        self.voyage_counter = 0

        self.storage_berth_free_at = {name: 0.0 for name in STORAGE_NAMES}
        self.next_storage_berthing_start_at = {
            point: 0.0 for point in sorted(set(STORAGE_POINT.values()))
        }
        self.mother_berth_free_at = {name: 0.0 for name in MOTHER_NAMES}
        self.next_mother_berthing_start_at = 0.0
        self.mother_available_at = {name: 0.0 for name in MOTHER_NAMES}

        self.export_ready = {name: False for name in MOTHER_NAMES}
        self.export_ready_since = {name: None for name in MOTHER_NAMES}
        self.export_state = {name: None for name in MOTHER_NAMES}
        self.export_start_time = {name: None for name in MOTHER_NAMES}
        self.export_end_time = {name: None for name in MOTHER_NAMES}
        self.next_export_allowed_at = 0.0
        self.last_export_mother = None
        self.point_b_day_assigned_mothers = {}
        self.storage_critical_active = {name: False for name in STORAGE_NAMES}
        self.point_f_vessels = ["Bedford", "Balham"]
        self.point_f_active_loader = "Balham"
        self.point_f_swap_pending_for = None
        self.point_f_swap_triggered_by = None
        self.production_rate_overrides = self._build_production_override_rules()

        offsets = [0] * NUM_DAUGHTERS      # all vessels wake up simultaneously
        self.vessels = []
        for i in range(NUM_DAUGHTERS):
            name = VESSEL_NAMES[i]
            cap = VESSEL_CAPACITIES.get(name, DAUGHTER_CARGO_BBL)
            self.vessels.append(DaughterVessel(name, offsets[i], cargo_capacity=cap))
        self.total_loaded = 0
        # Initialise vessel API to zero (no cargo on board at start).
        # PF_LOADING vessels are initialised to IBOM_API since Ibom
        # production has a constant API — no blending needed.
        for vv in self.vessels:
            self.vessel_api[vv.name] = 0.0
        # Vessels that start at Point B (idle, no cargo, waiting for return allocation)
        _POINT_B_START = {"Sherlock", "Laphroaig", "Rathbone"}
        _seeded_startups = set()
        if POINT_B_DISTRIBUTION_TEST_MODE:
            _seeded_startups = set(POINT_B_TEST_STARTUP_FULL_LOAD_NOMINATIONS.keys())

        for vv in self.vessels:
            if vv.name == "Bedford":
                # Bedford starts as active Ibom loader, cargo below swap trigger
                vv.status          = "PF_LOADING"
                vv.target_point    = "F"
                vv.cargo_bbl       = 30_000
                vv.next_event_time = 0.0
                vv._voyage_assigned = True
                vv.current_voyage   = 1
                self.vessel_api[vv.name] = IBOM_API  # constant; no blending needed
            elif vv.name == "Balham":
                # Balham starts berthing Alkebulan at BIA, full load (loaded from Ibom)
                vv.status           = "BERTHING_B"
                vv.target_point     = "B"
                vv.cargo_bbl        = 85_000
                vv.assigned_mother  = "Alkebulan"
                vv.next_event_time  = BERTHING_DELAY_HOURS   # completes berthing at 0.5 h
                vv._voyage_assigned = True
                vv.current_voyage   = 1
                # Balham loads from Ibom (Point F) — cargo API is constant Ibom API
                self.vessel_api[vv.name] = IBOM_API
            elif vv.name == "Woodstock":
                # Woodstock starts loading at Duke (Point D) from 0 bbl
                _wk_cap       = VESSEL_CAPACITIES.get("Woodstock", DAUGHTER_CARGO_BBL)
                _wk_load_h    = _wk_cap / DUKE_LOAD_RATE_BPH   # ≈ 12h full load time
                # Already-loaded = 0 bbl (default); remaining = full load duration
                _wk_already   = 0
                _wk_remaining = _wk_cap - _wk_already
                _wk_remain_h  = _wk_remaining / DUKE_LOAD_RATE_BPH
                vv.status           = "LOADING"
                vv.target_point     = "D"
                vv.assigned_storage = STORAGE_QUATERNARY_NAME   # "Duke"
                vv.cargo_bbl        = _wk_already               # already on board
                vv.assigned_load_hours = _wk_load_h
                vv.next_event_time  = _wk_remain_h              # fires when load complete
                vv._voyage_assigned = True
                vv.current_voyage   = 1
                # Deduct only remaining balance from storage
                self.storage_bbl[STORAGE_QUATERNARY_NAME] -= _wk_remaining
                self.total_loaded += _wk_already
                # Reserve Duke berth for remaining load window
                self.storage_berth_free_at[STORAGE_QUATERNARY_NAME] = _wk_remain_h
                self.next_storage_berthing_start_at["D"] = (
                    BERTHING_DELAY_HOURS + POST_BERTHING_START_GAP_HOURS
                )
                # Set Woodstock's vessel API to Duke's storage API at load start
                _wk_load_api = STORAGE_API.get(STORAGE_QUATERNARY_NAME, 0.0)
                self.vessel_api[vv.name] = _wk_load_api
                # Emit synthetic LOADING_START so event-based displays
                # (Loading Plan, JMP) correctly map Woodstock → Duke
                self.log_event(0, vv.name, "LOADING_START",
                               f"Loading {_wk_cap:,} bbl @ {_wk_load_api:.2f}° API | {STORAGE_QUATERNARY_NAME}: "
                               f"{self.storage_bbl[STORAGE_QUATERNARY_NAME]:,.0f} bbl "
                               f"(started at t=0, remaining {_wk_remain_h:.1f}h)",
                               voyage_num=vv.current_voyage)
            elif vv.name == "Bagshot" and vv.name not in _seeded_startups:
                # Bagshot starts hose-connected at Bryanston, ready to commence discharge
                _bg_cap = VESSEL_CAPACITIES.get("Bagshot", DAUGHTER_CARGO_BBL)
                vv.status           = "HOSE_CONNECT_B"
                vv.target_point     = "B"
                vv.cargo_bbl        = _bg_cap
                vv.assigned_mother  = MOTHER_PRIMARY_NAME   # Bryanston
                vv.next_event_time  = 0.0                   # fires immediately at t=0
                vv._voyage_assigned = True
                vv.current_voyage   = 1
                self.vessel_api[vv.name] = STORAGE_API.get(STORAGE_PRIMARY_NAME, 0.0)
                self.log_event(0, vv.name, "HOSE_CONNECTION_START_B",
                               f"Hose connected at {MOTHER_PRIMARY_NAME} — ready to commence discharge "
                               f"({_bg_cap:,} bbl, started at t=0)",
                               voyage_num=vv.current_voyage,
                               mother=MOTHER_PRIMARY_NAME)
            elif vv.name == "Watson" and vv.name not in _seeded_startups:
                # Watson starts hose-connected at GreenEagle, ready to commence discharge
                _wt_cap = VESSEL_CAPACITIES.get("Watson", DAUGHTER_CARGO_BBL)
                vv.status           = "HOSE_CONNECT_B"
                vv.target_point     = "B"
                vv.cargo_bbl        = _wt_cap
                vv.assigned_mother  = MOTHER_TERTIARY_NAME  # GreenEagle
                vv.next_event_time  = 0.0                   # fires immediately at t=0
                vv._voyage_assigned = True
                vv.current_voyage   = 1
                self.vessel_api[vv.name] = STORAGE_API.get(STORAGE_PRIMARY_NAME, 0.0)
                self.log_event(0, vv.name, "HOSE_CONNECTION_START_B",
                               f"Hose connected at {MOTHER_TERTIARY_NAME} — ready to commence discharge "
                               f"({_wt_cap:,} bbl, started at t=0)",
                               voyage_num=vv.current_voyage,
                               mother=MOTHER_TERTIARY_NAME)
            elif vv.name in _POINT_B_START:
                # Start at BIA waiting for return-stock allocation
                vv.status           = "WAITING_RETURN_STOCK"
                vv.target_point     = "B"
                vv.cargo_bbl        = 0
                vv.next_event_time  = 0.0
                vv._voyage_assigned = True
                vv.current_voyage   = 1

        if POINT_B_DISTRIBUTION_TEST_MODE:
            for vv in self.vessels:
                nominated_mother = POINT_B_TEST_STARTUP_FULL_LOAD_NOMINATIONS.get(vv.name)
                if not nominated_mother:
                    continue
                _cap = vv.cargo_capacity
                vv.status = "HOSE_CONNECT_B"
                vv.target_point = "B"
                vv.cargo_bbl = _cap
                vv.assigned_mother = nominated_mother
                vv.next_event_time = 0.0
                vv._voyage_assigned = True
                vv.current_voyage = max(vv.current_voyage, 1)
                self.vessel_api[vv.name] = STORAGE_API.get(STORAGE_PRIMARY_NAME, 0.0)
                self.log_event(
                    0,
                    vv.name,
                    "HOSE_CONNECTION_START_B",
                    f"Point-B test seed active: hose connected at {nominated_mother} — ready to commence discharge ({_cap:,} bbl, started at t=0)",
                    voyage_num=vv.current_voyage,
                    mother=nominated_mother,
                )

        # Reserve Point B berth timelines for any t=0 vessels already active on a mother.
        initial_gate_end = 0.0
        for vv in self.vessels:
            mother_name = vv.assigned_mother
            if mother_name not in MOTHER_NAMES:
                continue
            if vv.status == "BERTHING_B":
                _end = BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + DISCHARGE_HOURS
            elif vv.status == "HOSE_CONNECT_B":
                _end = HOSE_CONNECTION_HOURS + DISCHARGE_HOURS
            elif vv.status == "DISCHARGING":
                _end = DISCHARGE_HOURS
            else:
                continue
            self.mother_berth_free_at[mother_name] = max(self.mother_berth_free_at[mother_name], _end)
            initial_gate_end = max(initial_gate_end, _end)
        self.next_mother_berthing_start_at = initial_gate_end
        # Sim-level Ibom tracking: Bedford active, no swap pending
        self.point_f_active_loader     = "Bedford"
        self.point_f_swap_pending_for  = None
        self.point_f_swap_triggered_by = None

    @staticmethod
    def blend_api(vol_a, api_a, vol_b, api_b):
        """Return volume-weighted blended API. Returns api_b if vol_a is zero."""
        total = vol_a + vol_b
        if total <= 0:
            return api_b if api_b else api_a
        return (vol_a * api_a + vol_b * api_b) / total

    def point_f_other_vessel(self, vessel_name):
        return next((name for name in self.point_f_vessels if name != vessel_name), None)

    def point_f_active_loading_bbl(self):
        for vv in self.vessels:
            if vv.name == self.point_f_active_loader and vv.status in {"PF_LOADING", "IDLE_A"}:
                return vv.cargo_bbl
        return 0.0

    def total_storage_bbl(self):
        return sum(self.storage_bbl.values())

    def total_mother_bbl(self):
        return sum(self.mother_bbl.values())

    def _build_production_override_rules(self):
        rules = []
        for raw_rule in PRODUCTION_RATE_OVERRIDES:
            if not isinstance(raw_rule, dict):
                continue
            start_s = raw_rule.get("start_date")
            end_s = raw_rule.get("end_date")
            raw_rates = raw_rule.get("rates") or {}
            if not (start_s and end_s and isinstance(raw_rates, dict)):
                continue
            try:
                start_d = datetime.fromisoformat(str(start_s)).date()
                end_d = datetime.fromisoformat(str(end_s)).date()
            except Exception:
                continue
            if end_d < start_d:
                start_d, end_d = end_d, start_d
            rates = {}
            for storage_name, rate_val in raw_rates.items():
                if storage_name not in STORAGE_NAMES:
                    continue
                try:
                    rates[storage_name] = max(0.0, float(rate_val))
                except Exception:
                    continue
            if rates:
                rules.append({
                    "start": start_d,
                    "end": end_d,
                    "rates": rates,
                })
        return rules

    def production_rate_bph_at(self, storage_name, t):
        base_rate = STORAGE_PRODUCTION_RATE_BY_NAME.get(storage_name, 0.0)
        if not self.production_rate_overrides:
            return base_rate
        current_date = self.hours_to_dt(t).date()
        for rule in self.production_rate_overrides:
            if rule["start"] <= current_date <= rule["end"]:
                if storage_name in rule["rates"]:
                    return rule["rates"][storage_name]
        return base_rate

    def storage_load_hours(self, storage_name, cargo_bbl):
        """Return loading duration in hours for cargo_bbl loaded at storage_name."""
        _RATE_MAP = {
            STORAGE_PRIMARY_NAME:   CHAPEL_LOAD_RATE_BPH,
            STORAGE_SECONDARY_NAME: JASMINES_LOAD_RATE_BPH,
            STORAGE_TERTIARY_NAME:  WESTMORE_LOAD_RATE_BPH,
            STORAGE_QUATERNARY_NAME: DUKE_LOAD_RATE_BPH,
            STORAGE_QUINARY_NAME:   STARTURN_LOAD_RATE_BPH,
        }
        rate = _RATE_MAP.get(storage_name)
        if rate:
            return cargo_bbl / rate
        return LOAD_HOURS  # fallback for unknown storages

    def effective_load_cap(self, vessel_name, storage_name):
        """Return the loading volume cap for vessel at storage_name.
        Bedford and Balham are capped at POINT_A_LOAD_CAP_BBL (63k) when
        loading at Point A (Chapel / JasmineS).  Full capacity elsewhere.
        Pass storage_name="__any__" to get full capacity (non-Point-A probe).
        """
        vessel = next((v for v in self.vessels if v.name == vessel_name), None)
        full_cap = vessel.cargo_capacity if vessel else DAUGHTER_CARGO_BBL
        if (vessel_name in POINT_A_LOAD_CAP_VESSELS
                and STORAGE_POINT.get(storage_name) == "A"):
            return min(full_cap, POINT_A_LOAD_CAP_BBL)
        return full_cap

    def loading_start_threshold(self, storage_name, cargo_bbl):
        if storage_name in (STORAGE_QUATERNARY_NAME, STORAGE_QUINARY_NAME):
            required = max(cargo_bbl + DUKE_STARTURN_DEAD_STOCK_BBL,
                           STORAGE_CRITICAL_THRESHOLD_BY_NAME[storage_name])
            if storage_name == STORAGE_QUATERNARY_NAME:
                required = max(required, cargo_bbl + DUKE_MIN_REMAINING_BBL)
            if storage_name == STORAGE_QUINARY_NAME:
                required = max(required, cargo_bbl + STARTURN_MIN_REMAINING_BBL)
            return min(required, STORAGE_CAPACITY_BY_NAME[storage_name])
        required = DEAD_STOCK_FACTOR * cargo_bbl
        return min(required, STORAGE_CAPACITY_BY_NAME[storage_name])

    def storage_allowed_for_vessel(self, storage_name, vessel_name):
        if vessel_name in POINT_A_ONLY_VESSELS and STORAGE_POINT.get(storage_name) != "A":
            return False
        if storage_name == STORAGE_TERTIARY_NAME and vessel_name not in WESTMORE_PERMITTED_VESSELS:
            return False
        if storage_name == STORAGE_QUATERNARY_NAME and vessel_name not in DUKE_PERMITTED_VESSELS:
            return False
        if storage_name == STORAGE_QUINARY_NAME and vessel_name not in STARTURN_PERMITTED_VESSELS:
            return False
        return True

    def storage_min_remaining_after_load(self, storage_name):
        if storage_name == STORAGE_QUATERNARY_NAME:
            return DUKE_MIN_REMAINING_BBL
        if storage_name == STORAGE_QUINARY_NAME:
            return STARTURN_MIN_REMAINING_BBL
        return 0.0

    def return_allocation_candidate(self, cargo_bbl, vessel_name, point_restrict=None):
        # Consider every storage this vessel is permitted to load from.
        allowed_storages = [
            name for name in STORAGE_NAMES
            if self.storage_allowed_for_vessel(name, vessel_name)
            and (point_restrict is None or STORAGE_POINT.get(name) == point_restrict)
        ]
        # Per-storage effective cap: Bedford/Balham are capped at 63k at Point A
        cap_by_storage = {
            name: self.effective_load_cap(vessel_name, name)
            for name in allowed_storages
        }
        threshold_by_storage = {
            name: self.loading_start_threshold(name, cap_by_storage[name])
            for name in allowed_storages
        }

        pre_tank_top_candidates = []

        trigger_ratio_by_storage = {
            STORAGE_QUATERNARY_NAME: DUKE_PRE_TANK_TOP_TRIGGER_RATIO,
            STORAGE_QUINARY_NAME: STARTURN_PRE_TANK_TOP_TRIGGER_RATIO,
        }
        for storage_name in allowed_storages:
            stock = self.storage_bbl[storage_name]
            _stor_cap = STORAGE_CAPACITY_BY_NAME[storage_name]
            _load_cap = cap_by_storage[storage_name]  # effective load volume
            trigger_ratio = trigger_ratio_by_storage.get(storage_name, PRE_TANK_TOP_TRIGGER_RATIO_DEFAULT)
            pre_tank_top_trigger = _stor_cap * trigger_ratio
            reserve_required = self.storage_min_remaining_after_load(storage_name)
            if (
                stock >= pre_tank_top_trigger
                and stock >= (_load_cap + reserve_required)
            ):
                pre_tank_top_candidates.append(storage_name)

        if pre_tank_top_candidates:
            selected_pre_tank_top = max(
                pre_tank_top_candidates,
                key=lambda name: (
                    self.storage_bbl[name] / STORAGE_CAPACITY_BY_NAME[name],
                    self.storage_bbl[name],
                    name,
                ),
            )
            return selected_pre_tank_top, threshold_by_storage[selected_pre_tank_top], threshold_by_storage

        eligible = [
            name for name in allowed_storages
            if self.storage_bbl[name] >= threshold_by_storage[name]
        ]
        if not eligible:
            return None, None, threshold_by_storage

        def rank_key(storage_name):
            stock = self.storage_bbl[storage_name]
            critical = STORAGE_CRITICAL_THRESHOLD_BY_NAME[storage_name]
            above_critical = 0 if stock >= critical else 1
            critical_distance = abs(stock - critical)
            return (above_critical, critical_distance, -stock, storage_name)

        selected = min(eligible, key=rank_key)
        return selected, threshold_by_storage[selected], threshold_by_storage

    # -- Helpers ----------------------------------------------------------
    def hours_to_dt(self, h):
        return _SIM_EPOCH + timedelta(hours=h)

    def tide_height_at(self, hour):
        """Return tidal height at a given sim hour, or None if no table loaded."""
        if _TIDE_TABLE is None:
            return None
        slot = round(hour * 2) / 2
        # Walk forward up to 1h to find nearest populated slot
        for delta in [0, 0.5, -0.5, 1.0, -1.0]:
            h = _TIDE_TABLE.get(slot + delta)
            if h is not None:
                return h
        return None

    def tide_ok_at(self, hour):
        """True if tidal height is sufficient for crossing (or no table loaded)."""
        if _TIDE_TABLE is None:
            return True
        h = self.tide_height_at(hour)
        return h is not None and h > TIDE_MIN_CROSSING_M

    def tide_high_ok_at(self, hour):
        """True when hour is a local high-tide slot above crossing threshold."""
        if _TIDE_TABLE is None:
            return True
        h_now = self.tide_height_at(hour)
        if h_now is None or h_now <= TIDE_MIN_CROSSING_M:
            return False
        h_prev = self.tide_height_at(hour - 0.5)
        h_next = self.tide_height_at(hour + 0.5)
        prev_ok = (h_prev is None) or (h_now >= h_prev)
        next_ok = (h_next is None) or (h_now >= h_next)
        return prev_ok and next_ok

    def tidal_period_label(self, hour):
        wall_h = (hour + SIM_HOUR_OFFSET) % 24
        if 6 <= wall_h < 12:
            return "morning tide"
        if 12 <= wall_h < 16:
            return "afternoon tide"
        if 16 <= wall_h < 18:
            return "evening tide"
        return "outside tidal window"

    def tidal_periods_available_for_day(self, hour):
        """Return available high-tide periods for the calendar day of `hour`."""
        if _TIDE_TABLE is None:
            return "morning tide / afternoon tide / evening tide"
        day_key = int(hour // 24)
        day_start = day_key * 24
        labels = []
        for slot in [day_start + 0.5 * i for i in range(48)]:
            wall_h = (slot + SIM_HOUR_OFFSET) % 24
            if not (DAYLIGHT_START <= wall_h < DAYLIGHT_END):
                continue
            if self.tide_high_ok_at(slot):
                lbl = self.tidal_period_label(slot)
                if lbl not in labels:
                    labels.append(lbl)
        return " / ".join(labels) if labels else "no valid high-tide crossing period"

    def next_tidal_sail(self, current_hour):
        """
        Return the earliest hour >= current_hour that satisfies BOTH:
          - daylight (DAYLIGHT_START <= (h+SIM_HOUR_OFFSET)%24 < DAYLIGHT_END)
          - high tide > TIDE_MIN_CROSSING_M  (skipped if no table)
        Scans forward in 0.5 h steps for up to 7 days.
        """
        # Fast path: no tidal table — fall back to pure daylight check
        if _TIDE_TABLE is None:
            return self.next_daylight_sail(current_hour)
        t = self.next_daylight_sail(current_hour)
        for _ in range(336):   # max 7 days * 48 half-hour steps
            wall_h = (t + SIM_HOUR_OFFSET) % 24
            if DAYLIGHT_START <= wall_h < DAYLIGHT_END and self.tide_high_ok_at(t):
                return t
            t += 0.5
            # skip to next daylight start if outside window
            wall_h2 = (t + SIM_HOUR_OFFSET) % 24
            if not (DAYLIGHT_START <= wall_h2 < DAYLIGHT_END):
                t = self.next_daylight_sail(t)
        return self.next_daylight_sail(current_hour)

    def next_daylight_sail(self, current_hour):
        """Return earliest sim-hour >= current_hour within wall-clock daylight
        window [DAYLIGHT_START, DAYLIGHT_END)."""
        wall_h = (current_hour + SIM_HOUR_OFFSET) % 24
        if DAYLIGHT_START <= wall_h < DAYLIGHT_END:
            return current_hour
        days_elapsed = int(current_hour // 24)
        sim_dl_today = days_elapsed * 24 + (DAYLIGHT_START - SIM_HOUR_OFFSET)
        if current_hour <= sim_dl_today:
            return sim_dl_today
        else:
            return sim_dl_today + 24

    def next_export_sail_start(self, current_hour):
        """Return earliest sim-hour >= current_hour within wall-clock export
        sail window [EXPORT_SAIL_WINDOW_START, EXPORT_SAIL_WINDOW_END)."""
        wall_h = (current_hour + SIM_HOUR_OFFSET) % 24
        if EXPORT_SAIL_WINDOW_START <= wall_h < EXPORT_SAIL_WINDOW_END:
            return current_hour
        days_elapsed = int(current_hour // 24)
        sim_ex_today = days_elapsed * 24 + (EXPORT_SAIL_WINDOW_START - SIM_HOUR_OFFSET)
        if current_hour <= sim_ex_today:
            return sim_ex_today
        else:
            return sim_ex_today + 24

    def next_cast_off_window(self, current_hour):
        """Return earliest sim-hour >= current_hour that falls within the
        wall-clock cast-off window [CAST_OFF_START, CAST_OFF_END).
        Converts sim-hours → wall-clock via SIM_HOUR_OFFSET before comparing."""
        wall_h = (current_hour + SIM_HOUR_OFFSET) % 24
        if CAST_OFF_START <= wall_h < CAST_OFF_END:
            return current_hour
        days_elapsed = int(current_hour // 24)
        # Sim-hour that corresponds to CAST_OFF_START on the same calendar day
        sim_co_today = days_elapsed * 24 + (CAST_OFF_START - SIM_HOUR_OFFSET)
        if current_hour <= sim_co_today:
            return sim_co_today
        else:
            return sim_co_today + 24

    def is_any_vessel_casting_off(self, point=None):
        for v in self.vessels:
            if point is None:
                if v.status in ["WAITING_CAST_OFF", "CAST_OFF", "CAST_OFF_B"]:
                    return True
            elif point == "B":
                if v.status == "CAST_OFF_B":
                    return True
            else:
                if v.status in ["WAITING_CAST_OFF", "CAST_OFF"] and v.target_point == point:
                    return True
        return False

    def is_valid_berthing_time(self, hour, point=None):
        """Return True if hour falls within the berthing window and no cast-off is active."""
        wall_h = (hour + SIM_HOUR_OFFSET) % 24
        return BERTHING_START <= wall_h < BERTHING_END and not self.is_any_vessel_casting_off(point)

    def next_berthing_window(self, current_hour, point=None):
        """Return the earliest sim-hour >= current_hour that falls within the
        wall-clock berthing window [BERTHING_START, BERTHING_END) with no cast-off conflict.
        Uses SIM_HOUR_OFFSET to convert sim-hours to wall-clock hours."""
        wall_h = (current_hour + SIM_HOUR_OFFSET) % 24
        if BERTHING_START <= wall_h < BERTHING_END and not self.is_any_vessel_casting_off(point):
            return current_hour
        days = int(current_hour // 24)
        # Sim-hour that corresponds to BERTHING_START wall-clock on this day
        sim_bs_today = days * 24 + (BERTHING_START - SIM_HOUR_OFFSET)
        if current_hour <= sim_bs_today:
            candidate = sim_bs_today
        else:
            candidate = sim_bs_today + 24
        # Step forward one day at a time until cast-off conflict clears
        for _ in range(14):
            if not self.is_any_vessel_casting_off(point):
                return candidate
            candidate += 24
        return candidate

    def log_event(self, t, vessel_name, event, detail="", voyage_num=None, mother=None):
        # Resolve mother: explicit arg → vessel's current assigned_mother → None
        if mother is None:
            _v = next((vv for vv in self.vessels if vv.name == vessel_name), None)
            if _v is not None:
                mother = _v.assigned_mother
        # Snapshot the vessel's current cargo API for this log row
        _v_for_api = next((vv for vv in self.vessels if vv.name == vessel_name), None)
        _vessel_api_snap = round(self.vessel_api.get(vessel_name, 0.0), 2) if _v_for_api else 0.0
        self.log.append({
            "Time"       : self.hours_to_dt(t).strftime("%Y-%m-%d %H:%M"),
            "Day"        : int(t // 24) + 1,
            "Hour"       : f"{int(t % 24):02d}:{int((t % 1)*60):02d}",
            "Vessel"     : vessel_name,
            "Voyage"     : voyage_num,
            "Event"      : event,
            "Detail"     : detail,
            "Mother"     : mother,
            "Vessel_api" : _vessel_api_snap,
            "Storage_bbl": round(self.total_storage_bbl()),
            "Chapel_bbl": round(self.storage_bbl[STORAGE_PRIMARY_NAME]),
            "JasmineS_bbl": round(self.storage_bbl[STORAGE_SECONDARY_NAME]),
            "Westmore_bbl": round(self.storage_bbl[STORAGE_TERTIARY_NAME]),
            "Duke_bbl": round(self.storage_bbl[STORAGE_QUATERNARY_NAME]),
            "Starturn_bbl": round(self.storage_bbl[STORAGE_QUINARY_NAME]),
            "Storage_Overflow_Accum_bbl": round(sum(self.storage_overflow_bbl.values())),
            "Chapel_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_PRIMARY_NAME]),
            "JasmineS_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_SECONDARY_NAME]),
            "Westmore_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_TERTIARY_NAME]),
            "Duke_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_QUATERNARY_NAME]),
            "Starturn_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_QUINARY_NAME]),
            "PointF_Overflow_Accum_bbl": round(self.point_f_overflow_accum_bbl),
            "PointF_Active_Loading_bbl": round(self.point_f_active_loading_bbl()),
            "Mother_bbl" : round(self.total_mother_bbl()),
            "Bryanston_bbl": round(self.mother_bbl[MOTHER_PRIMARY_NAME]),
            "Alkebulan_bbl": round(self.mother_bbl[MOTHER_SECONDARY_NAME]),
            "GreenEagle_bbl": round(self.mother_bbl[MOTHER_TERTIARY_NAME]),
            "Total_Exported_bbl": self.total_exported,
        })

    # -- Main simulation loop ---------------------------------------------
    def run(self):
        total_hours = SIMULATION_DAYS * 24
        t = 0.0

        while t <= total_hours:
            # 1. Continuous production at all storage locations (non-stop)
            for storage_name in STORAGE_NAMES:
                prod_rate = self.production_rate_bph_at(storage_name, t)
                prod = prod_rate * TIME_STEP_HOURS
                cap = STORAGE_CAPACITY_BY_NAME[storage_name]
                prod_api = STORAGE_API.get(storage_name, 0.0)
                self.total_produced += prod
                projected = self.storage_bbl[storage_name] + prod
                # Blend incoming production API into storage
                self.storage_api[storage_name] = self.blend_api(
                    self.storage_bbl[storage_name], self.storage_api[storage_name],
                    prod, prod_api)
                if projected > cap:
                    overflow_amount = projected - cap
                    self.total_spilled += overflow_amount
                    self.storage_overflow_bbl[storage_name] += overflow_amount
                    self.storage_overflow_events += 1
                    self.storage_bbl[storage_name] = cap
                else:
                    self.storage_bbl[storage_name] = projected

            # Point F accumulation during swap/takeover gap (reporting only)
            if self.point_f_active_loader is None and self.point_f_swap_pending_for is not None:
                self.point_f_overflow_accum_bbl += POINT_F_LOAD_RATE_BPH * TIME_STEP_HOURS

            # 2. Advance each vessel's state machine
            for v in self.vessels:
                if t < v.next_event_time:
                    continue

                if v.status == "PF_LOADING":
                    increment = POINT_F_LOAD_RATE_BPH * TIME_STEP_HOURS
                    if v.cargo_bbl < v.cargo_capacity:
                        v.cargo_bbl = min(v.cargo_capacity, v.cargo_bbl + increment)
                    # Ibom API is constant — assign directly rather than blending
                    # from 0.0, which would produce incorrect intermediate values
                    self.vessel_api[v.name] = IBOM_API
                    if v.cargo_bbl > POINT_F_MIN_TRIGGER_BBL:
                        alternate = self.point_f_other_vessel(v.name)
                        if self.point_f_swap_pending_for != alternate:
                            self.point_f_swap_pending_for = alternate
                            self.point_f_swap_triggered_by = v.name
                            self.log_event(
                                t,
                                v.name,
                                "POINT_F_SWAP_TRIGGER",
                                f"Point F trigger at {v.cargo_bbl:,.0f} bbl (> {POINT_F_MIN_TRIGGER_BBL:,.0f}); "
                                f"{alternate} requested to take over after current voyage",
                                voyage_num=v.current_voyage,
                            )

                        alternate_vessel = next((vv for vv in self.vessels if vv.name == alternate), None)
                        alternate_arrived = (
                            alternate_vessel is not None
                            and alternate_vessel.status == "IDLE_A"
                            and alternate_vessel.target_point == "F"
                            and alternate_vessel.cargo_bbl <= 0
                        )
                        daylight_now = DAYLIGHT_START <= ((t + SIM_HOUR_OFFSET) % 24) < DAYLIGHT_END

                        if alternate_arrived and daylight_now:
                            self.point_f_active_loader = None
                            alternate_vessel.status = "PF_SWAP"
                            alternate_vessel.target_point = "F"
                            alternate_vessel.next_event_time = t + POINT_F_SWAP_HOURS
                            self.log_event(
                                t,
                                alternate_vessel.name,
                                "POINT_F_SWAP_START",
                                f"Point F takeover starts ({POINT_F_SWAP_HOURS}h)",
                                voyage_num=alternate_vessel.current_voyage,
                            )
                            v.status = "CAST_OFF"
                            v.next_event_time = t
                            continue
                    v.next_event_time = t + TIME_STEP_HOURS
                    continue

                if v.status == "PF_SWAP":
                    self.point_f_active_loader = v.name
                    self.point_f_swap_pending_for = None
                    self.point_f_swap_triggered_by = None
                    v.target_point = "F"   # ensure active Ibom loader stays on Point F
                    returned_from_overflow = min(self.point_f_overflow_accum_bbl, max(0.0, v.cargo_capacity - v.cargo_bbl))
                    v.cargo_bbl += returned_from_overflow
                    self.point_f_overflow_accum_bbl -= returned_from_overflow
                    v.status = "PF_LOADING"
                    self.vessel_api[v.name] = IBOM_API  # constant; assign directly
                    self.log_event(
                        t,
                        v.name,
                        "POINT_F_SWAP_COMPLETE",
                        f"Point F swap complete; returned {returned_from_overflow:,.0f} bbl overflow to loader | "
                        f"trigger rule: swap when load exceeds {POINT_F_MIN_TRIGGER_BBL:,.0f} bbl",
                        voyage_num=v.current_voyage,
                    )
                    v.next_event_time = t + TIME_STEP_HOURS
                    continue

                if v.status == "IDLE_A":
                    if v.name == self.point_f_active_loader:
                        # This vessel is the designated Ibom loader — route to PF_LOADING
                        if v.cargo_bbl < v.cargo_capacity:
                            v.cargo_bbl = min(
                                v.cargo_capacity,
                                v.cargo_bbl + (POINT_F_LOAD_RATE_BPH * TIME_STEP_HOURS),
                            )
                        wall_h = (t + SIM_HOUR_OFFSET) % 24
                        if not (DAYLIGHT_START <= wall_h < DAYLIGHT_END):
                            next_light = self.next_daylight_sail(t)
                            self.log_event(
                                t,
                                v.name,
                                "WAITING_DAYLIGHT",
                                f"Point F loading waits for daylight at {self.hours_to_dt(next_light).strftime('%Y-%m-%d %H:%M')}",
                                voyage_num=v.current_voyage,
                            )
                            v.next_event_time = t + TIME_STEP_HOURS
                            continue
                        v.status = "PF_LOADING"
                        v.target_point = "F"
                        self.vessel_api[v.name] = IBOM_API
                        v.next_event_time = t
                        continue

                    # Bedford/Balham: if not the active Ibom loader and not
                    # en route to Ibom for a swap, dispatch to SanBarth (Point A).
                    if (v.name in self.point_f_vessels
                            and self.point_f_active_loader != v.name
                            and v.target_point != "F"):
                        v.target_point = "A"  # load Chapel/JasmineS

                    # Only assign a new voyage number on a fresh cycle start.
                    if not hasattr(v, '_voyage_assigned') or not v._voyage_assigned:
                        self.voyage_counter += 1
                        v.current_voyage = self.voyage_counter
                        v._voyage_assigned = True
                    cap = v.cargo_capacity   # default; overridden per-storage below

                    # If vessel was manually seeded at a specific storage (e.g.
                    # Woodstock placed at Duke with IDLE_A), honour that assignment
                    # immediately: skip the stock-gate and go straight to berthing.
                    # The dead-stock check in HOSE_CONNECT_A will hold loading until
                    # the threshold is met, exactly as it does for all vessels.
                    if v.assigned_storage and self.storage_allowed_for_vessel(v.assigned_storage, v.name):
                        _pre_assigned = v.assigned_storage
                        cap           = self.effective_load_cap(v.name, _pre_assigned)
                        _pre_stock    = self.storage_bbl[_pre_assigned]
                        _pre_point    = STORAGE_POINT.get(_pre_assigned, "A")
                        _pre_thresh   = self.loading_start_threshold(_pre_assigned, cap)
                        _pre_berth_t  = self.next_berthing_window(t, point=_pre_point)
                        _pre_start    = max(
                            _pre_berth_t,
                            self.storage_berth_free_at[_pre_assigned],
                            self.next_storage_berthing_start_at[_pre_point],
                        )
                        # Final daylight guard — gate values may be outside berthing window
                        _pre_start = self.next_berthing_window(_pre_start, point=_pre_point)
                        load_hours = self.storage_load_hours(_pre_assigned, cap)
                        v.assigned_load_hours = load_hours
                        v.status = "BERTHING_A"
                        v.target_point = _pre_point
                        self.storage_berth_free_at[_pre_assigned] = (
                            _pre_start + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + load_hours
                        )
                        self.next_storage_berthing_start_at[_pre_point] = (
                            _pre_start + BERTHING_DELAY_HOURS + POST_BERTHING_START_GAP_HOURS
                        )
                        v.next_event_time = _pre_start + BERTHING_DELAY_HOURS
                        slot = VESSEL_NAMES.index(v.name) + 1
                        self.log_event(_pre_start, v.name, "BERTHING_START_A",
                                       f"Berthing at {_pre_assigned} (pre-assigned, 30 min procedure) "
                                       f"[rotation slot {slot} of {NUM_DAUGHTERS}]",
                                       voyage_num=v.current_voyage)
                        continue

                    eligible_storage_names = [
                        name for name in STORAGE_NAMES
                        if STORAGE_POINT.get(name) == v.target_point
                    ]
                    if not eligible_storage_names:
                        eligible_storage_names = STORAGE_NAMES

                    candidate_storages = []
                    for storage_name in eligible_storage_names:
                        if not self.storage_allowed_for_vessel(storage_name, v.name):
                            continue
                        cap = self.effective_load_cap(v.name, storage_name)
                        stock = self.storage_bbl[storage_name]
                        if stock < cap:
                            continue
                        storage_point = STORAGE_POINT.get(storage_name, "A")
                        threshold_required = self.loading_start_threshold(storage_name, cap)
                        berth_t = self.next_berthing_window(t, point=storage_point)
                        start = max(
                            berth_t,
                            self.storage_berth_free_at[storage_name],
                            self.next_storage_berthing_start_at[storage_point],
                        )
                        # Final daylight guard — gate values may be outside berthing window
                        start = self.next_berthing_window(start, point=storage_point)
                        candidate_storages.append((storage_name, stock, berth_t, start, threshold_required))

                    if candidate_storages:
                        # ── Dead-stock rule ─────────────────────────────
                        # The vessel berths and connects hoses normally, but
                        # loading cannot commence until 175% of the cargo
                        # volume is available.  We enforce this here: if the
                        # stock is above the simple threshold but below the
                        # dead-stock threshold the vessel still proceeds to
                        # berth — the waiting-for-stock logic in HOSE_CONNECT_A
                        # will hold it at berth until the threshold is met.
                        candidate_storages.sort(
                            key=lambda x: (
                                0 if x[1] >= x[4] else 1,
                                x[3],
                                -x[1]
                            )
                        )
                        selected_storage, selected_stock, berth_t, start, threshold_required = candidate_storages[0]
                        wait_berth_window = berth_t - t
                        v.assigned_storage = selected_storage
                        load_hours = self.storage_load_hours(selected_storage, cap)
                        v.assigned_load_hours = load_hours

                        # The berth is reserved. We do NOT pre-commit stock
                        # here because the dead-stock rule may delay the
                        # actual loading start — stock is committed only once
                        # the 175% threshold is confirmed in HOSE_CONNECT_A.
                        v.status = "BERTHING_A"
                        selected_point = STORAGE_POINT.get(selected_storage, "A")
                        self.storage_berth_free_at[selected_storage] = (
                            start + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + load_hours
                        )
                        self.next_storage_berthing_start_at[selected_point] = (
                            start + BERTHING_DELAY_HOURS + POST_BERTHING_START_GAP_HOURS
                        )
                        v.next_event_time = start + BERTHING_DELAY_HOURS

                        slot = VESSEL_NAMES.index(v.name) + 1
                        if wait_berth_window > 0.1:
                            self.log_event(t, v.name, "WAITING_BERTH_A",
                                           f"Waiting for berthing window at {selected_storage} | "
                                           f"Available at {self.hours_to_dt(berth_t).strftime('%Y-%m-%d %H:%M')}",
                                           voyage_num=v.current_voyage)
                        self.log_event(start, v.name, "BERTHING_START_A",
                                       f"Berthing at {selected_storage} (30 min procedure) "
                                       f"[rotation slot {slot} of {NUM_DAUGHTERS}]",
                                       voyage_num=v.current_voyage)
                    else:
                        v.next_event_time = t + 0.5   # poll frequently so queue doesn't stall
                        threshold_by_storage = {
                            name: self.loading_start_threshold(name, cap)
                            for name in eligible_storage_names
                        }
                        min_threshold = min(threshold_by_storage.values()) if threshold_by_storage else cap
                        storage_levels = ", ".join(
                            f"{name}: {self.storage_bbl[name]:,.0f} bbl" for name in eligible_storage_names
                        )
                        self.log_event(t, v.name, "WAITING_STOCK",
                                       f"Insufficient stock at Point {v.target_point} ({storage_levels} available, "
                                       f"need {cap:,} bbl min / {min_threshold:,.0f} bbl loading-start threshold) — waiting",
                                       voyage_num=v.current_voyage)

                elif v.status == "BERTHING_A":
                    v.status = "HOSE_CONNECT_A"
                    v.next_event_time = t + HOSE_CONNECTION_HOURS
                    berth_storage = v.assigned_storage or STORAGE_PRIMARY_NAME
                    self.log_event(t, v.name, "HOSE_CONNECTION_START_A",
                                   f"Hose connection initiated at {berth_storage} (2 hours)",
                                   voyage_num=v.current_voyage)

                elif v.status == "HOSE_CONNECT_A":
                    # ── Dead-stock rule enforced here ───────────────────
                    # Loading can only commence when storage holds at least
                    # the storage-specific loading-start threshold.
                    # For Duke/Starturn this is critical stock;
                    # other storages use 175% dead-stock. The cargo was
                    # NOT pre-committed in IDLE_A for the berth reservation;
                    # it is committed here once the threshold is satisfied.
                    selected_storage = v.assigned_storage or STORAGE_PRIMARY_NAME
                    cap = self.effective_load_cap(v.name, selected_storage)
                    threshold_required = self.loading_start_threshold(selected_storage, cap)
                    load_hours = v.assigned_load_hours if v.assigned_load_hours is not None else LOAD_HOURS
                    # Recompute load_hours based on effective cap (handles Point A cap)
                    load_hours = self.storage_load_hours(selected_storage, cap)
                    if self.storage_bbl[selected_storage] < threshold_required:
                        # Stay at berth; poll every 30 min until stock builds
                        v.next_event_time = t + 0.5
                        self.log_event(t, v.name, "WAITING_DEAD_STOCK",
                                       f"Berthed but waiting for loading-start threshold "
                                       f"({threshold_required:,.0f} bbl required, "
                                       f"{self.storage_bbl[selected_storage]:,.0f} bbl available at {selected_storage})",
                                       voyage_num=v.current_voyage)
                        continue
                    if (
                        selected_storage == STORAGE_QUATERNARY_NAME
                        and (self.storage_bbl[selected_storage] - cap) < DUKE_MIN_REMAINING_BBL
                    ):
                        v.next_event_time = t + 0.5
                        self.log_event(
                            t,
                            v.name,
                            "WAITING_DEAD_STOCK",
                            f"Berthed but waiting for Duke reserve rule "
                            f"({DUKE_MIN_REMAINING_BBL:,.0f} bbl must remain after loading; "
                            f"current post-load would be {self.storage_bbl[selected_storage] - cap:,.0f} bbl)",
                            voyage_num=v.current_voyage,
                        )
                        continue
                    if (
                        selected_storage == STORAGE_QUINARY_NAME
                        and (self.storage_bbl[selected_storage] - cap) < STARTURN_MIN_REMAINING_BBL
                    ):
                        v.next_event_time = t + 0.5
                        self.log_event(
                            t,
                            v.name,
                            "WAITING_DEAD_STOCK",
                            f"Berthed but waiting for Starturn reserve rule "
                            f"({STARTURN_MIN_REMAINING_BBL:,.0f} bbl must remain after loading; "
                            f"current post-load would be {self.storage_bbl[selected_storage] - cap:,.0f} bbl)",
                            voyage_num=v.current_voyage,
                        )
                        continue
                    # Threshold met — commit stock and start loading
                    self.storage_bbl[selected_storage] -= cap
                    # Vessel receives a full cargo from storage — its API equals the
                    # storage point's current API exactly (no blending at load point).
                    _load_api = self.storage_api.get(selected_storage, 0.0)
                    self.vessel_api[v.name] = _load_api
                    v.cargo_bbl = cap
                    self.total_loaded += cap
                    v.status = "LOADING"
                    self.storage_berth_free_at[selected_storage] = max(
                        self.storage_berth_free_at[selected_storage], t + load_hours
                    )
                    v.next_event_time = t + load_hours
                    self.log_event(t, v.name, "LOADING_START",
                                   f"Loading {cap:,} bbl @ {_load_api:.2f}° API | {selected_storage}: "
                                   f"{self.storage_bbl[selected_storage]:,.0f} bbl "
                                   f"(loading-start threshold {threshold_required:,.0f} bbl met, rate duration {load_hours:.1f}h)",
                                   voyage_num=v.current_voyage)

                elif v.status == "LOADING":
                    # Ensure cargo_bbl reflects the full completed load.
                    # Use effective_load_cap so Bedford/Balham at Point A
                    # complete at 63k, not their physical 85k capacity.
                    _load_stor = v.assigned_storage or STORAGE_PRIMARY_NAME
                    v.cargo_bbl = self.effective_load_cap(v.name, _load_stor)
                    v.status = "DOCUMENTING"
                    v.next_event_time = t + 4
                    self.log_event(t, v.name, "LOADING_COMPLETE",
                                   f"Cargo: {v.cargo_bbl:,} bbl | Begin 4h documentation",
                                   voyage_num=v.current_voyage)
                    self.log_event(t, v.name, "DOCUMENTATION_START",
                                   "4 hours allocated for paperwork",
                                   voyage_num=v.current_voyage)

                elif v.status == "DOCUMENTING":
                    cast_off_t = self.next_cast_off_window(t)
                    wait_co = cast_off_t - t
                    v.status = "CAST_OFF"
                    v.next_event_time = cast_off_t + CAST_OFF_HOURS
                    self.log_event(t, v.name, "DOCUMENTATION_COMPLETE",
                                   f"Ready for cast-off | Procedure starts "
                                   f"{self.hours_to_dt(cast_off_t).strftime('%H:%M')} (wait {wait_co:.1f}h)",
                                   voyage_num=v.current_voyage)
                    if wait_co > 0:
                        self.log_event(t, v.name, "WAITING_CAST_OFF",
                                       f"Cast-off window opens at "
                                       f"{self.hours_to_dt(cast_off_t).strftime('%Y-%m-%d %H:%M')}",
                                       voyage_num=v.current_voyage)

                elif v.status == "CAST_OFF":
                    sail_t = self.next_tidal_sail(t)
                    wait = sail_t - t
                    if v.target_point == "D":
                        v.status = "SAILING_D_CHANNEL"
                        v.next_event_time = sail_t + SAIL_HOURS_D_TO_CH
                    else:
                        # A/C → B: 4-leg route via breakwater and fairway buoy
                        v.status = "SAILING_AB"
                        v.next_event_time = sail_t + SAIL_HOURS_A_TO_BW
                    self.log_event(t, v.name, "CAST_OFF_COMPLETE",
                                   f"Cast-off complete | Departure "
                                   f"{self.hours_to_dt(sail_t).strftime('%H:%M')} (wait {wait:.1f}h)",
                                   voyage_num=v.current_voyage)
                    if wait > 0:
                        self.log_event(t, v.name, "WAITING_TIDAL",
                                       f"Daylight/tide window opens at "
                                       f"{self.hours_to_dt(sail_t).strftime('%Y-%m-%d %H:%M')} "
                                       f"({self.tidal_period_label(sail_t)}; available today: "
                                       f"{self.tidal_periods_available_for_day(sail_t)})",
                                       voyage_num=v.current_voyage)

                elif v.status == "SAILING_D_CHANNEL":
                    # Arrived Cawthorne Channel — next leg to Breakwater needs tidal gate
                    arrival = t
                    self.log_event(arrival, v.name, "ARRIVED_CAWTHORNE_CHANNEL",
                                   "Reached Cawthorne Channel (3h from Point D)",
                                   voyage_num=v.current_voyage)
                    depart_ch = self.next_tidal_sail(arrival)
                    wait_ch = depart_ch - arrival
                    if wait_ch > 0:
                        self.log_event(arrival, v.name, "WAITING_TIDAL",
                                       f"Cawthorne Channel: waiting for daylight/tide at "
                                       f"{self.hours_to_dt(depart_ch).strftime('%Y-%m-%d %H:%M')} "
                                       f"({self.tidal_period_label(depart_ch)})",
                                       voyage_num=v.current_voyage)
                    v.status = "SAILING_CH_TO_BW_OUT"
                    v.next_event_time = depart_ch + SAIL_HOURS_CH_TO_BW_OUT

                elif v.status == "SAILING_CH_TO_BW_OUT":
                    # Arrived at Breakwater (outbound) — tidal gate for crossing
                    arrival = t
                    self.log_event(arrival, v.name, "ARRIVED_BREAKWATER_OUT",
                                   "Reached breakwater (outbound, 1h from Cawthorne Channel)",
                                   voyage_num=v.current_voyage)
                    depart_bw = self.next_tidal_sail(arrival)
                    wait_bw = depart_bw - arrival
                    if wait_bw > 0:
                        self.log_event(arrival, v.name, "WAITING_TIDAL",
                                       f"Breakwater: waiting for daylight/tide at "
                                       f"{self.hours_to_dt(depart_bw).strftime('%Y-%m-%d %H:%M')} "
                                       f"({self.tidal_period_label(depart_bw)})",
                                       voyage_num=v.current_voyage)
                    v.status = "SAILING_CROSS_BW_OUT"
                    v.next_event_time = depart_bw + SAIL_HOURS_CROSS_BW

                elif v.status == "SAILING_CROSS_BW_OUT":
                    # Crossed breakwater outbound — final run to BIA (no tidal gate)
                    arrival = t
                    self.log_event(arrival, v.name, "CROSSED_BREAKWATER_OUT",
                                   "Crossed breakwater (0.5h) — clear breakwater, running to BIA (1.5h)",
                                   voyage_num=v.current_voyage)
                    v.status = "SAILING_AB_LEG2"
                    v.next_event_time = arrival + SAIL_HOURS_BW_TO_B

                elif v.status == "SAILING_AB":
                    # Arrived at Breakwater (outbound from A/C) — tidal gate to cross
                    arrival = t
                    self.log_event(arrival, v.name, "ARRIVED_BREAKWATER_AC_OUT",
                                   "Reached breakwater (1.5h from Point A/C)",
                                   voyage_num=v.current_voyage)
                    depart_bw = self.next_tidal_sail(arrival)
                    wait_bw   = depart_bw - arrival
                    if wait_bw > 0:
                        self.log_event(arrival, v.name, "WAITING_TIDAL",
                                       f"Breakwater: waiting for daylight/tide at "
                                       f"{self.hours_to_dt(depart_bw).strftime('%Y-%m-%d %H:%M')} "
                                       f"({self.tidal_period_label(depart_bw)})",
                                       voyage_num=v.current_voyage)
                    v.status = "SAILING_CROSS_BW_AC"
                    v.next_event_time = depart_bw + SAIL_HOURS_CROSS_BW_AC

                elif v.status == "SAILING_CROSS_BW_AC":
                    # Crossed breakwater outbound — run to fairway buoy (daylight only)
                    arrival = t
                    self.log_event(arrival, v.name, "CROSSED_BREAKWATER_AC_OUT",
                                   "Crossed breakwater outbound (0.5h) — heading to Fairway Buoy (2h)",
                                   voyage_num=v.current_voyage)
                    depart_fwy = self.next_daylight_sail(arrival)
                    wait_fwy   = depart_fwy - arrival
                    if wait_fwy > 0:
                        self.log_event(arrival, v.name, "WAITING_DAYLIGHT",
                                       f"Post-breakwater: waiting for daylight at "
                                       f"{self.hours_to_dt(depart_fwy).strftime('%Y-%m-%d %H:%M')}",
                                       voyage_num=v.current_voyage)
                    v.status = "SAILING_BW_TO_FWY"
                    v.next_event_time = depart_fwy + SAIL_HOURS_BW_TO_FWY

                elif v.status == "SAILING_BW_TO_FWY":
                    # Arrived Fairway Buoy outbound (A/C → BIA).
                    # Rule: only hold overnight if arrival is AFTER 19:00.
                    # Vessels arriving at or before 19:00 proceed directly to BIA
                    # (the 2h sail is acceptable even if it arrives after dark).
                    FAIRWAY_HOLD_HOUR = 19  # hold overnight if arrival hour >= this
                    arrival    = t
                    arrival_hod = (arrival + SIM_HOUR_OFFSET) % 24   # wall-clock hour-of-day
                    self.log_event(arrival, v.name, "ARRIVED_FAIRWAY",
                                   "Reached Fairway Buoy (2h from breakwater) — running to BIA (2h)",
                                   voyage_num=v.current_voyage)
                    if arrival_hod >= FAIRWAY_HOLD_HOUR:
                        # Arrived after 19:00 wall-clock — hold until next daylight
                        depart_bia   = self.next_daylight_sail(arrival + 0.01)
                        wait_bia     = depart_bia - arrival
                        self.log_event(arrival, v.name, "WAITING_FAIRWAY",
                                       f"Arrived after 19:00 ({self.hours_to_dt(arrival).strftime('%H:%M')}) — "
                                       f"holding at Fairway Buoy until {self.hours_to_dt(depart_bia).strftime('%Y-%m-%d %H:%M')}",
                                       voyage_num=v.current_voyage)
                    else:
                        # Arrived at or before 19:00 — proceed directly, no hold
                        depart_bia = arrival
                    v.status = "SAILING_AB_LEG2"
                    v.next_event_time = depart_bia + SAIL_HOURS_FWY_TO_B

                elif v.status == "SAILING_AB_LEG2":
                    arrival = t
                    v.arrival_at_b = arrival

                    # Ensure berthing_start is within the daylight berthing window
                    # regardless of arrival time (night, early morning, etc.)
                    berthing_start = self.next_berthing_window(arrival, point="B")
                    if berthing_start > arrival + 0.01:
                        self.log_event(arrival, v.name, "WAITING_NIGHT",
                                       f"Arrived at {self.hours_to_dt(arrival).strftime('%H:%M')} outside berthing window — "
                                       f"waiting until {self.hours_to_dt(berthing_start).strftime('%Y-%m-%d %H:%M')}",
                                       voyage_num=v.current_voyage)

                    candidates = []
                    for mother_name in MOTHER_NAMES:
                        if self.mother_bbl[mother_name] + v.cargo_bbl > MOTHER_CAPACITY_BBL:
                            continue
                        earliest = max(berthing_start, self.mother_available_at[mother_name])
                        berth_t = self.next_berthing_window(earliest, point="B")
                        start = max(
                            berth_t,
                            self.mother_berth_free_at[mother_name],
                            self.mother_available_at[mother_name],
                            self.next_mother_berthing_start_at,
                        )
                        # Always enforce daylight window as the final step —
                        # any of the gate values above may be outside berthing hours
                        start = self.next_berthing_window(start, point="B")
                        candidates.append((start, berth_t, mother_name))

                    if not candidates:
                        mother_levels = ", ".join(
                            f"{name}: {self.mother_bbl[name]:,.0f}/{MOTHER_CAPACITY_BBL:,} bbl"
                            for name in MOTHER_NAMES
                        )
                        self.log_event(arrival, v.name, "WAITING_MOTHER_CAPACITY",
                                       f"Insufficient capacity on Point B mothers ({mother_levels})",
                                       voyage_num=v.current_voyage)
                        v.next_event_time = arrival + 6
                    else:
                        day_key = int(arrival // 24)
                        candidate_by_mother = {
                            mother_name: (start, berth_t, mother_name)
                            for start, berth_t, mother_name in candidates
                        }

                        # Day 1 exception: disable Point B auto-prioritization and require
                        # a manual nomination for each arriving daughter vessel.
                        if STARTUP_DAY_DISABLE_POINT_B_PRIORITY and day_key == 0:
                            nominated_mother = STARTUP_DAY_POINT_B_MANUAL_NOMINATIONS.get(v.name)
                            if not nominated_mother:
                                v.next_event_time = arrival + 0.5
                                self.log_event(
                                    arrival,
                                    v.name,
                                    "WAITING_MOTHER_CAPACITY",
                                    "Startup-day manual nomination required at Point B; no auto-priority assignment applied",
                                    voyage_num=v.current_voyage,
                                )
                                continue
                            if nominated_mother not in candidate_by_mother:
                                eligible_names = ", ".join(name for name in MOTHER_NAMES if name in candidate_by_mother)
                                v.next_event_time = arrival + 0.5
                                self.log_event(
                                    arrival,
                                    v.name,
                                    "WAITING_MOTHER_CAPACITY",
                                    f"Startup-day manual nomination '{nominated_mother}' is not currently eligible; eligible now: {eligible_names}",
                                    voyage_num=v.current_voyage,
                                )
                                continue
                            selected_mother = nominated_mother
                            selected = candidate_by_mother[selected_mother]
                        else:
                            # Standard strict Point B prioritization (Day 2+):
                            # 1) First daughter arrival on a day takes first eligible mother.
                            # 2) Next arrival(s) that day must take a different eligible mother.
                            # If only already-used mothers are eligible, the vessel waits.
                            assigned_today = self.point_b_day_assigned_mothers.setdefault(day_key, set())
                            available_distinct = [
                                name for name in MOTHER_NAMES
                                if name in candidate_by_mother and name not in assigned_today
                            ]
                            if not available_distinct:
                                v.next_event_time = arrival + 0.5
                                self.log_event(
                                    arrival,
                                    v.name,
                                    "WAITING_MOTHER_CAPACITY",
                                    f"Same-day spread constraint active: no different eligible mother available on day {day_key + 1}; waiting",
                                    voyage_num=v.current_voyage,
                                )
                                continue
                            selected_mother = available_distinct[0]
                            selected = candidate_by_mother[selected_mother]

                        start, berth_t, selected_mother = selected
                        if not (STARTUP_DAY_DISABLE_POINT_B_PRIORITY and day_key == 0):
                            self.point_b_day_assigned_mothers.setdefault(day_key, set()).add(selected_mother)
                        v.assigned_mother = selected_mother
                        v.status = "BERTHING_B"
                        _discharge_end = (
                            start + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + DISCHARGE_HOURS
                        )
                        self.mother_berth_free_at[selected_mother] = _discharge_end
                        # Enforce serial discharge: the global gate is pushed to the
                        # end of the full discharge operation so no other daughter can
                        # begin berthing at ANY mother until this one finishes.
                        self.next_mother_berthing_start_at = _discharge_end
                        v.next_event_time = start + BERTHING_DELAY_HOURS
                        if berth_t > berthing_start + 0.1:
                            self.log_event(berthing_start, v.name, "WAITING_BERTH_B",
                                           f"Waiting for berthing window at {selected_mother} | "
                                           f"Available at {self.hours_to_dt(berth_t).strftime('%Y-%m-%d %H:%M')}",
                                           voyage_num=v.current_voyage)
                        if STARTUP_DAY_DISABLE_POINT_B_PRIORITY and day_key == 0:
                            self.log_event(
                                arrival,
                                v.name,
                                "MOTHER_PRIORITY_ASSIGNMENT",
                                f"Day 1 startup manual nomination applied: {selected_mother} (auto-prioritization disabled)",
                                voyage_num=v.current_voyage,
                            )
                        else:
                            assigned_today = self.point_b_day_assigned_mothers.get(day_key, set())
                            self.log_event(
                                arrival,
                                v.name,
                                "MOTHER_PRIORITY_ASSIGNMENT",
                                f"Day {day_key + 1} same-day spread assignment: "
                                f"{selected_mother} selected as first eligible unused mother | "
                                f"already assigned today: {', '.join(sorted(assigned_today))}",
                                voyage_num=v.current_voyage,
                            )
                        self.log_event(start, v.name, "BERTHING_START_B",
                                       f"Berthing at {selected_mother} (30 min procedure)",
                                       voyage_num=v.current_voyage)

                elif v.status == "BERTHING_B":
                    if v.assigned_mother not in MOTHER_NAMES:
                        self.log_event(t, v.name, "WAITING_MOTHER_CAPACITY",
                                       "Blocked: no explicit mother assignment at Point B (fallback disabled)",
                                       voyage_num=v.current_voyage)
                        v.next_event_time = t + 0.5
                        continue
                    v.status = "HOSE_CONNECT_B"
                    v.next_event_time = t + HOSE_CONNECTION_HOURS
                    selected_mother = v.assigned_mother
                    self.log_event(t, v.name, "HOSE_CONNECTION_START_B",
                                   f"Hose connection initiated at {selected_mother} (2 hours)",
                                   voyage_num=v.current_voyage)

                elif v.status == "HOSE_CONNECT_B":
                    if v.assigned_mother not in MOTHER_NAMES:
                        self.log_event(t, v.name, "WAITING_MOTHER_CAPACITY",
                                       "Blocked: no explicit mother assignment at Point B (fallback disabled)",
                                       voyage_num=v.current_voyage)
                        v.next_event_time = t + 0.5
                        continue
                    selected_mother = v.assigned_mother
                    if self.mother_bbl[selected_mother] + v.cargo_bbl > MOTHER_CAPACITY_BBL:
                        self.log_event(t, v.name, "WAITING_MOTHER_CAPACITY",
                                       f"Cannot start discharge - {selected_mother} lacks space",
                                       voyage_num=v.current_voyage)
                        v.next_event_time = t + 6
                    else:
                        # Blend vessel cargo API into mother vessel.
                        # Vessel carries the storage point's API exactly — no blending at load point.
                        _vessel_api_val = self.vessel_api.get(v.name, 0.0)
                        self.mother_api[selected_mother] = self.blend_api(
                            self.mother_bbl[selected_mother], self.mother_api.get(selected_mother, 0.0),
                            v.cargo_bbl, _vessel_api_val)
                        self.mother_bbl[selected_mother] += v.cargo_bbl
                        v.status = "DISCHARGING"
                        self.mother_berth_free_at[selected_mother] = max(
                            self.mother_berth_free_at[selected_mother], t + DISCHARGE_HOURS
                        )
                        v.next_event_time = t + DISCHARGE_HOURS
                        self.log_event(t, v.name, "DISCHARGE_START",
                                       f"Discharging {v.cargo_bbl:,} bbl @ {_vessel_api_val:.2f}° API | "
                                       f"{selected_mother}: {self.mother_bbl[selected_mother]:,.0f} bbl "
                                       f"(blended {self.mother_api[selected_mother]:.2f}° API)",
                                       voyage_num=v.current_voyage,
                                       mother=selected_mother)

                elif v.status == "DISCHARGING":
                    if v.assigned_mother not in MOTHER_NAMES:
                        self.log_event(t, v.name, "WAITING_MOTHER_CAPACITY",
                                       "Blocked: no explicit mother assignment at Point B (fallback disabled)",
                                       voyage_num=v.current_voyage)
                        v.next_event_time = t + 0.5
                        continue
                    selected_mother = v.assigned_mother
                    v.cargo_bbl = 0
                    self.vessel_api[v.name] = 0.0
                    # Enforce daylight-only cast-off at BIA
                    cast_off_b_t = self.next_cast_off_window(t)
                    wait_co_b = cast_off_b_t - t
                    v.status = "CAST_OFF_B"
                    v.next_event_time = cast_off_b_t + CAST_OFF_HOURS
                    self.log_event(t, v.name, "DISCHARGE_COMPLETE",
                                   f"{selected_mother}: {self.mother_bbl[selected_mother]:,.0f} bbl | "
                                   f"Cast-off scheduled {self.hours_to_dt(cast_off_b_t).strftime('%H:%M')} (wait {wait_co_b:.1f}h)",
                                   voyage_num=v.current_voyage)
                    if wait_co_b > 0:
                        self.log_event(t, v.name, "WAITING_CAST_OFF",
                                       f"Night restriction — cast-off from {selected_mother} at "
                                       f"{self.hours_to_dt(cast_off_b_t).strftime('%Y-%m-%d %H:%M')}",
                                       voyage_num=v.current_voyage)
                    self.log_event(cast_off_b_t, v.name, "CAST_OFF_START_B",
                                   f"Cast-off from {selected_mother} ({CAST_OFF_HOURS}h)",
                                   voyage_num=v.current_voyage)

                elif v.status == "CAST_OFF_B":
                    if v.assigned_mother not in MOTHER_NAMES:
                        self.log_event(t, v.name, "WAITING_MOTHER_CAPACITY",
                                       "Blocked: no explicit mother assignment at Point B (fallback disabled)",
                                       voyage_num=v.current_voyage)
                        v.next_event_time = t + 0.5
                        continue
                    selected_mother = v.assigned_mother
                    if not self.export_ready[selected_mother]:
                        self.export_ready_since[selected_mother] = t
                    self.export_ready[selected_mother] = True
                    self.log_event(t, v.name, "CAST_OFF_COMPLETE_B",
                                   "Cast-off from mother complete; returning to storage",
                                   voyage_num=v.current_voyage)
                    v.status = "WAITING_RETURN_STOCK"
                    v.next_event_time = t

                elif v.status == "WAITING_RETURN_STOCK":
                    selected_mother = v.assigned_mother if v.assigned_mother in MOTHER_NAMES else "UNASSIGNED"
                    # If this is a point_f vessel not currently assigned to Ibom,
                    # force it back to SanBarth (Point A) for its SanBarth cycle.
                    # If swap is pending for this vessel it will sail to Ibom
                    # directly — skip storage allocation entirely for that case.
                    # Otherwise, if in SanBarth support mode restrict to Point A.
                    if self.point_f_swap_pending_for == v.name:
                        # Will be intercepted below — just need a dummy allocation
                        # to satisfy the flow; use Chapel as placeholder.
                        target_storage    = "Chapel"
                        required_stock    = 0
                        threshold_by_storage = {}
                    else:
                        _pf_sanbarth_mode = (
                            v.name in self.point_f_vessels
                            and self.point_f_active_loader != v.name
                        )
                        if _pf_sanbarth_mode:
                            v.target_point = "A"
                        _pt_restrict = "A" if _pf_sanbarth_mode else None
                        target_storage, required_stock, threshold_by_storage = self.return_allocation_candidate(v.cargo_capacity, v.name, point_restrict=_pt_restrict)
                    if target_storage is None:
                        if not threshold_by_storage:
                            self.log_event(
                                t,
                                v.name,
                                "WAITING_RETURN_STOCK",
                                "Waiting at Point B for permitted return storage allocation",
                                voyage_num=v.current_voyage,
                            )
                            v.next_event_time = t + 0.5
                            continue
                        storage_levels = ", ".join(
                            f"{name}: {self.storage_bbl[name]:,.0f} bbl "
                            f"(need {threshold_by_storage[name]:,.0f})"
                            for name in threshold_by_storage
                        )
                        self.log_event(
                            t,
                            v.name,
                            "WAITING_RETURN_STOCK",
                            f"Waiting at Point B for return allocation stock "
                            f"(storage-specific loading thresholds): {storage_levels}",
                            voyage_num=v.current_voyage,
                        )
                        v.next_event_time = t + 0.5
                        continue

                    # Point F swap pending: vessel must sail BIA → Ibom directly
                    if self.point_f_swap_pending_for == v.name:
                        sail_t = self.next_daylight_sail(t)
                        wait   = sail_t - t
                        v.target_point     = "F"
                        v.status           = "SAILING_B_TO_F"
                        v.next_event_time  = sail_t + SAIL_HOURS_B_TO_F
                        self.log_event(t, v.name, "SAILING_B_TO_F_START",
                                       f"Ibom swap ordered — sailing BIA → Ibom "
                                       f"({SAIL_HOURS_B_TO_F}h, depart "
                                       f"{self.hours_to_dt(sail_t).strftime('%H:%M')})",
                                       voyage_num=v.current_voyage)
                        if wait > 0:
                            self.log_event(t, v.name, "WAITING_DAYLIGHT",
                                           f"Daylight window opens at "
                                           f"{self.hours_to_dt(sail_t).strftime('%Y-%m-%d %H:%M')}",
                                           voyage_num=v.current_voyage)
                        continue

                    v.target_point = STORAGE_POINT.get(target_storage, "A")
                    sail_t = self.next_tidal_sail(t)
                    wait   = sail_t - t
                    self.log_event(t, v.name, "RETURN_POINT_ALLOCATED",
                                   f"Allocated to Point {v.target_point} on departure from {selected_mother} | "
                                   f"Immediate-load eligible storage: {target_storage} "
                                   f"({self.storage_bbl[target_storage]:,.0f} bbl, "
                                   f"loading-start threshold {required_stock:,.0f} bbl, "
                                   f"critical {STORAGE_CRITICAL_THRESHOLD_BY_NAME[target_storage]:,.0f} bbl)",
                                   voyage_num=v.current_voyage)
                    if wait > 0:
                        self.log_event(t, v.name, "WAITING_TIDAL",
                                       f"Daylight/tide window opens at "
                                       f"{self.hours_to_dt(sail_t).strftime('%Y-%m-%d %H:%M')} "
                                       f"({self.tidal_period_label(sail_t)}; available today: "
                                       f"{self.tidal_periods_available_for_day(sail_t)})",
                                       voyage_num=v.current_voyage)
                    if v.target_point == "D":
                        # 4-leg return: BIA → BW (1.5h) → cross BW (0.5h, tidal) →
                        #               CH (1h, tidal) → Point D (3h, tidal)
                        v.status = "SAILING_B_TO_BW_IN"
                        v.next_event_time = sail_t + SAIL_HOURS_B_TO_BW
                    elif v.target_point == "E":
                        # Starturn (Dawes) — short direct return 3h, no breakwater
                        v.status = "SAILING_BA"
                        v.next_event_time = sail_t + 3
                    else:
                        # A/C return: BIA → FWY (2h) → BW (2h) → cross BW (0.5h, tidal) → A/C (1.5h)
                        v.status = "SAILING_B_TO_FWY"
                        v.next_event_time = sail_t + SAIL_HOURS_B_TO_FWY

                elif v.status == "SAILING_B_TO_FWY":
                    # BIA → Fairway Buoy (2h, A/C return leg 1)
                    arrival = t
                    self.log_event(arrival, v.name, "ARRIVED_FAIRWAY_RETURN",
                                   "Reached Fairway Buoy returning (2h from BIA)",
                                   voyage_num=v.current_voyage)
                    # Leg 2: Fairway Buoy → Breakwater (2h, daylight)
                    depart_fwy = self.next_daylight_sail(arrival)
                    wait_fwy   = depart_fwy - arrival
                    if wait_fwy > 0:
                        self.log_event(arrival, v.name, "WAITING_FAIRWAY",
                                       f"Holding at Fairway Buoy (return) until daylight at "
                                       f"{self.hours_to_dt(depart_fwy).strftime('%Y-%m-%d %H:%M')}",
                                       voyage_num=v.current_voyage)
                    v.status = "SAILING_FWY_TO_BW"
                    v.next_event_time = depart_fwy + SAIL_HOURS_FWY_TO_BW

                elif v.status == "SAILING_FWY_TO_BW":
                    # Arrived at Breakwater inbound (A/C return leg 2) — tidal gate to cross
                    arrival = t
                    self.log_event(arrival, v.name, "ARRIVED_BREAKWATER_AC_IN",
                                   "Reached breakwater inbound (2h from Fairway Buoy)",
                                   voyage_num=v.current_voyage)
                    depart_bw = self.next_tidal_sail(arrival)
                    wait_bw   = depart_bw - arrival
                    if wait_bw > 0:
                        self.log_event(arrival, v.name, "WAITING_TIDAL",
                                       f"Breakwater inbound: waiting for daylight/tide at "
                                       f"{self.hours_to_dt(depart_bw).strftime('%Y-%m-%d %H:%M')} "
                                       f"({self.tidal_period_label(depart_bw)})",
                                       voyage_num=v.current_voyage)
                    v.status = "SAILING_CROSS_BW_IN_AC"
                    v.next_event_time = depart_bw + SAIL_HOURS_CROSS_BW_AC

                elif v.status == "SAILING_CROSS_BW_IN_AC":
                    # Crossed breakwater inbound — final run to Point A/C (1.5h, no gate)
                    arrival = t
                    self.log_event(arrival, v.name, "CROSSED_BREAKWATER_AC_IN",
                                   "Crossed breakwater inbound (0.5h) — running to Point A/C (1.5h)",
                                   voyage_num=v.current_voyage)
                    v.status = "SAILING_BA"
                    v.next_event_time = arrival + SAIL_HOURS_BW_TO_A

                elif v.status == "SAILING_B_TO_BW_IN":
                    # Arrived at clear breakwater (inbound from BIA, 1.5h)
                    arrival = t
                    self.log_event(arrival, v.name, "ARRIVED_BREAKWATER_IN",
                                   "Reached clear breakwater inbound (1.5h from BIA)",
                                   voyage_num=v.current_voyage)
                    depart_bw = self.next_tidal_sail(arrival)
                    wait_bw   = depart_bw - arrival
                    if wait_bw > 0:
                        self.log_event(arrival, v.name, "WAITING_TIDAL",
                                       f"Breakwater inbound: waiting for daylight/tide at "
                                       f"{self.hours_to_dt(depart_bw).strftime('%Y-%m-%d %H:%M')} "
                                       f"({self.tidal_period_label(depart_bw)})",
                                       voyage_num=v.current_voyage)
                    v.status = "SAILING_CROSS_BW_IN"
                    v.next_event_time = depart_bw + SAIL_HOURS_CROSS_BW

                elif v.status == "SAILING_CROSS_BW_IN":
                    # Crossed breakwater inbound — next leg to Cawthorne Channel (tidal)
                    arrival = t
                    self.log_event(arrival, v.name, "CROSSED_BREAKWATER_IN",
                                   "Crossed breakwater inbound (0.5h) — heading to Cawthorne Channel",
                                   voyage_num=v.current_voyage)
                    depart_bw_ch = self.next_tidal_sail(arrival)
                    wait_bw_ch   = depart_bw_ch - arrival
                    if wait_bw_ch > 0:
                        self.log_event(arrival, v.name, "WAITING_TIDAL",
                                       f"Post-breakwater: waiting for daylight/tide at "
                                       f"{self.hours_to_dt(depart_bw_ch).strftime('%Y-%m-%d %H:%M')} "
                                       f"({self.tidal_period_label(depart_bw_ch)})",
                                       voyage_num=v.current_voyage)
                    v.status = "SAILING_BW_TO_CH_IN"
                    v.next_event_time = depart_bw_ch + SAIL_HOURS_BW_TO_CH_IN

                elif v.status == "SAILING_BW_TO_CH_IN":
                    # Arrived Cawthorne Channel inbound — final leg to Point D (tidal)
                    arrival = t
                    self.log_event(arrival, v.name, "ARRIVED_CAWTHORNE_CHANNEL_IN",
                                   "Reached Cawthorne Channel inbound (1h from breakwater)",
                                   voyage_num=v.current_voyage)
                    depart_ch_d = self.next_tidal_sail(arrival)
                    wait_ch_d   = depart_ch_d - arrival
                    if wait_ch_d > 0:
                        self.log_event(arrival, v.name, "WAITING_TIDAL",
                                       f"Cawthorne Channel: waiting for daylight/tide at "
                                       f"{self.hours_to_dt(depart_ch_d).strftime('%Y-%m-%d %H:%M')} "
                                       f"({self.tidal_period_label(depart_ch_d)})",
                                       voyage_num=v.current_voyage)
                    v.status = "SAILING_CH_TO_D"
                    v.next_event_time = depart_ch_d + SAIL_HOURS_CH_TO_D

                elif v.status == "SAILING_CH_TO_D":
                    # Arrived Point D — reset for next loading cycle
                    v.status = "IDLE_A"
                    v.target_point = "D"          # always stay on Duke
                    v.assigned_storage = None
                    v.assigned_load_hours = None
                    v.assigned_mother = None
                    v._voyage_assigned = False
                    self.log_event(t, v.name, "ARRIVED_LOADING_POINT",
                                   f"Arrived Point D (Awoba) — ready for next cycle",
                                   voyage_num=v.current_voyage)
                    v.next_event_time = t

                elif v.status == "SAILING_B_TO_F":
                    # Arrived at Ibom — execute swap immediately
                    v.status          = "IDLE_A"
                    v.target_point    = "F"
                    v._voyage_assigned = False
                    self.log_event(t, v.name, "ARRIVED_IBOM",
                                   "Arrived at Ibom (Point F) for swap takeover",
                                   voyage_num=v.current_voyage)
                    v.next_event_time = t

                elif v.status == "SAILING_BA":
                    v.status = "IDLE_A"
                    v.assigned_storage = None
                    v.assigned_load_hours = None
                    v.assigned_mother = None
                    v._voyage_assigned = False  # allow next cycle to get a new voyage number
                    # Point F vessels that are not the active Ibom loader must be
                    # directed to SanBarth (Point A) — reset target_point here so
                    # IDLE_A dispatch immediately sees Chapel/JasmineS as eligible.
                    if (v.name in self.point_f_vessels
                            and self.point_f_active_loader != v.name
                            and self.point_f_swap_pending_for != v.name):
                        v.target_point = "A"
                    self.log_event(t, v.name, "ARRIVED_LOADING_POINT",
                                   f"Arrived Point {v.target_point} storage area — ready for next cycle",
                                   voyage_num=v.current_voyage)
                    v.next_event_time = t

            # 3. Advance mother export state machines independently
            active_export_mother = next(
                (name for name in MOTHER_NAMES if self.export_state[name] is not None),
                None,
            )
            if active_export_mother is None and t >= self.next_export_allowed_at:
                ready_candidates = []
                for mother_name in MOTHER_NAMES:
                    if (
                        self.export_state[mother_name] is None
                        and self.export_ready[mother_name]
                        and self.mother_bbl[mother_name] >= MOTHER_EXPORT_TRIGGER
                        and t >= self.mother_available_at[mother_name]
                    ):
                        discharging_here = any(
                            vv.status == "DISCHARGING" and (vv.assigned_mother == mother_name)
                            for vv in self.vessels
                        )
                        if discharging_here:
                            self.log_event(t, mother_name, "EXPORT_WAIT_DISCHARGE",
                                           "Export ready but waiting for daughter discharge to complete")
                            continue
                        ready_since = self.export_ready_since[mother_name]
                        if ready_since is None:
                            ready_since = t
                        ready_candidates.append((ready_since, mother_name))

                if ready_candidates:
                    ready_candidates.sort(key=lambda x: (x[0], x[1]))
                    selected_export_mother = ready_candidates[0][1]
                    wall_h = (t + SIM_HOUR_OFFSET) % 24
                    if DAYLIGHT_START <= wall_h < DAYLIGHT_END:
                        self.export_state[selected_export_mother] = "DOC"
                        self.export_ready[selected_export_mother] = False
                        self.export_ready_since[selected_export_mother] = None
                        self.export_end_time[selected_export_mother] = t + EXPORT_DOC_HOURS
                        self.log_event(t, selected_export_mother, "EXPORT_DOC_START",
                                       f"Export documentation ({EXPORT_DOC_HOURS}h)")
                    else:
                        next_light = self.next_daylight_sail(t)
                        if next_light > t:
                            self.log_event(t, selected_export_mother, "EXPORT_WAIT_DAYLIGHT",
                                           f"Export ready but waiting for daylight at "
                                           f"{self.hours_to_dt(next_light).strftime('%Y-%m-%d %H:%M')}")

            for mother_name in MOTHER_NAMES:

                state = self.export_state[mother_name]
                if state == "DOC":
                    if t >= self.export_end_time[mother_name]:
                        sail_start = self.next_export_sail_start(t)
                        if sail_start > t:
                            self.log_event(t, mother_name, "EXPORT_WAIT_SAIL_WINDOW",
                                           f"Export docs complete; waiting to start sail at "
                                           f"{self.hours_to_dt(sail_start).strftime('%Y-%m-%d %H:%M')}")
                        self.export_state[mother_name] = "SAILING"
                        self.export_start_time[mother_name] = sail_start
                        self.export_end_time[mother_name] = sail_start + EXPORT_SAIL_HOURS
                        self.log_event(sail_start, mother_name, "EXPORT_SAIL_START",
                                       f"Sailing to export terminal ({EXPORT_SAIL_HOURS}h)")

                elif state == "SAILING":
                    if t >= self.export_end_time[mother_name]:
                        self.export_state[mother_name] = "HOSE"
                        self.export_start_time[mother_name] = t
                        self.export_end_time[mother_name] = t + EXPORT_HOSE_HOURS
                        self.log_event(t, mother_name, "EXPORT_ARRIVED",
                                       f"Arrived at export terminal; initiating hose connection ({EXPORT_HOSE_HOURS}h)")
                        self.log_event(t, mother_name, "EXPORT_HOSE_START",
                                       f"Hose connection ({EXPORT_HOSE_HOURS}h)")

                elif state == "HOSE":
                    if t >= self.export_end_time[mother_name]:
                        self.export_state[mother_name] = "IN_PORT"
                        self.export_start_time[mother_name] = t
                        self.log_event(t, mother_name, "EXPORT_HOSE_COMPLETE",
                                       "Hose connection complete; ready to export")

                elif state == "IN_PORT":
                    amount = min(self.mother_bbl[mother_name], EXPORT_RATE_BPH * TIME_STEP_HOURS)
                    if amount > 0:
                        self.total_exported_api_bbl += amount * self.mother_api.get(mother_name, 0.0)
                        self.mother_bbl[mother_name] -= amount
                        self.total_exported += amount
                        self.log_event(t, mother_name, "EXPORT_PROGRESS",
                                       f"Exported {amount:,} bbl in port; Remaining: {self.mother_bbl[mother_name]:,.0f} bbl")
                    if self.mother_bbl[mother_name] <= 0:
                        self.export_state[mother_name] = None
                        self.export_start_time[mother_name] = None
                        self.export_end_time[mother_name] = None
                        self.log_event(t, mother_name, "EXPORT_COMPLETE",
                                       f"Export complete; Remaining on board: {self.mother_bbl[mother_name]:,.0f} bbl")
                        return_depart = self.next_daylight_sail(t)
                        if return_depart > t:
                            self.log_event(t, mother_name, "EXPORT_WAIT_DAYLIGHT_RETURN",
                                           f"Waiting for daylight to depart export terminal at "
                                           f"{self.hours_to_dt(return_depart).strftime('%Y-%m-%d %H:%M')}")
                        return_arrival = return_depart + EXPORT_SAIL_HOURS
                        self.mother_available_at[mother_name] = return_arrival + 2
                        self.log_event(return_depart, mother_name, "EXPORT_RETURN_START",
                                       f"Departing export terminal ({EXPORT_SAIL_HOURS}h transit)")
                        self.log_event(return_arrival, mother_name, "EXPORT_RETURN_ARRIVE",
                                       f"Arrived at {mother_name}; beginning 2h fendering")
                        self.log_event(self.mother_available_at[mother_name], mother_name, "EXPORT_FENDERING_COMPLETE",
                                       "Fendering complete; ready to receive daughters")
                        self.next_export_allowed_at = (
                            self.mother_available_at[mother_name] + EXPORT_SERIES_BUFFER_HOURS
                        )
                        self.last_export_mother = mother_name
                        self.log_event(
                            self.next_export_allowed_at,
                            mother_name,
                            "EXPORT_SERIES_BUFFER_COMPLETE",
                            f"Serial export buffer complete ({EXPORT_SERIES_BUFFER_HOURS}h) — next mother export may begin",
                        )

            # 4. Debit overflow accumulation and credit stock when space is available
            for storage_name in STORAGE_NAMES:
                overflow_backlog = self.storage_overflow_bbl[storage_name]
                if overflow_backlog <= 0:
                    continue
                cap = STORAGE_CAPACITY_BY_NAME[storage_name]
                space_available = max(0.0, cap - self.storage_bbl[storage_name])
                if space_available <= 0:
                    continue
                credit_amount = min(space_available, overflow_backlog)
                self.storage_bbl[storage_name] += credit_amount
                self.storage_overflow_bbl[storage_name] -= credit_amount

            # 5. Check storage critical thresholds (entry/exit)
            for storage_name in STORAGE_NAMES:
                threshold = STORAGE_CRITICAL_THRESHOLD_BY_NAME[storage_name]
                is_critical_now = self.storage_bbl[storage_name] > threshold
                if is_critical_now and not self.storage_critical_active[storage_name]:
                    self.storage_critical_active[storage_name] = True
                    self.log_event(
                        t,
                        storage_name,
                        "STORAGE_CRITICAL_ENTER",
                        f"Critical stock reached: {self.storage_bbl[storage_name]:,.0f} bbl > {threshold:,.0f} bbl",
                    )
                elif (not is_critical_now) and self.storage_critical_active[storage_name]:
                    self.storage_critical_active[storage_name] = False
                    self.log_event(
                        t,
                        storage_name,
                        "STORAGE_CRITICAL_EXIT",
                        f"Critical stock cleared: {self.storage_bbl[storage_name]:,.0f} bbl <= {threshold:,.0f} bbl",
                    )

            # 6. Snapshot for timeline
            vessel_statuses = {}
            for v in self.vessels:
                vessel_statuses[v.name]                  = v.status
                vessel_statuses[f"{v.name}_cargo_bbl"]   = round(v.cargo_bbl)
                vessel_statuses[f"{v.name}_api"]         = round(self.vessel_api.get(v.name, 0.0), 2)
            self.timeline.append({
                "Time"       : self.hours_to_dt(t),
                "Day"        : int(t // 24) + 1,
                "Storage_bbl": round(self.total_storage_bbl()),
                "Chapel_bbl": round(self.storage_bbl[STORAGE_PRIMARY_NAME]),
                "JasmineS_bbl": round(self.storage_bbl[STORAGE_SECONDARY_NAME]),
                "Westmore_bbl": round(self.storage_bbl[STORAGE_TERTIARY_NAME]),
                "Duke_bbl": round(self.storage_bbl[STORAGE_QUATERNARY_NAME]),
                "Starturn_bbl": round(self.storage_bbl[STORAGE_QUINARY_NAME]),
                "Storage_Overflow_Accum_bbl": round(sum(self.storage_overflow_bbl.values())),
                "Chapel_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_PRIMARY_NAME]),
                "JasmineS_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_SECONDARY_NAME]),
                "Westmore_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_TERTIARY_NAME]),
                "Duke_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_QUATERNARY_NAME]),
                "Starturn_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_QUINARY_NAME]),
                "PointF_Overflow_Accum_bbl": round(self.point_f_overflow_accum_bbl),
                "PointF_Active_Loading_bbl": round(self.point_f_active_loading_bbl()),
                "Mother_bbl" : round(self.total_mother_bbl()),
                "Bryanston_bbl": round(self.mother_bbl[MOTHER_PRIMARY_NAME]),
                "Alkebulan_bbl": round(self.mother_bbl[MOTHER_SECONDARY_NAME]),
                "GreenEagle_bbl": round(self.mother_bbl[MOTHER_TERTIARY_NAME]),
                "Total_Exported": self.total_exported,
                "Chapel_api"   : round(self.storage_api.get(STORAGE_PRIMARY_NAME,   0.0), 2),
                "JasmineS_api" : round(self.storage_api.get(STORAGE_SECONDARY_NAME, 0.0), 2),
                "Westmore_api" : round(self.storage_api.get(STORAGE_TERTIARY_NAME,  0.0), 2),
                "Duke_api"     : round(self.storage_api.get(STORAGE_QUATERNARY_NAME,0.0), 2),
                "Starturn_api" : round(self.storage_api.get(STORAGE_QUINARY_NAME,   0.0), 2),
                "Bryanston_api": round(self.mother_api.get(MOTHER_PRIMARY_NAME,   0.0), 2),
                "Alkebulan_api": round(self.mother_api.get(MOTHER_SECONDARY_NAME, 0.0), 2),
                "GreenEagle_api": round(self.mother_api.get(MOTHER_TERTIARY_NAME, 0.0), 2),
                **vessel_statuses
            })

            t = round(t + TIME_STEP_HOURS, 2)

        self.final_storage_api = dict(self.storage_api)
        self.final_vessel_api  = dict(self.vessel_api)
        self.final_mother_api  = dict(self.mother_api)
        self.avg_exported_api  = (
            self.total_exported_api_bbl / self.total_exported
            if self.total_exported > 0 else 0.0
        )
        return pd.DataFrame(self.log), pd.DataFrame(self.timeline)


# -----------------------------------------------------------------
# RUN SIMULATION
# -----------------------------------------------------------------
print("=" * 65)
print("  OIL TANKER DAUGHTER VESSEL OPERATION SIMULATION  (v5)")
print("=" * 65)

if POINT_B_DISTRIBUTION_TEST_MODE:
    SIMULATION_DAYS = POINT_B_DISTRIBUTION_TEST_DAYS
    print("[INFO] Point B distribution test mode enabled")
    print(f"[INFO] Simulation days overridden to {SIMULATION_DAYS}")

sim = Simulation()
log_df, timeline_df = sim.run()

# Print summary table
print(f"\n{'-'*65}")
print("DETAILED EVENT LOG (first 80 events)")
print(f"{'-'*65}")
display_cols = ["Time", "Vessel", "Voyage", "Event", "Detail", "Storage_bbl", "Mother_bbl"]
print(log_df[display_cols].head(80).to_string(index=False))

print(f"\n{'-'*65}")
print("SIMULATION SUMMARY")
print(f"{'-'*65}")
total_loads     = len(log_df[log_df["Event"] == "LOADING_START"])
total_discharge = len(log_df[log_df["Event"] == "DISCHARGE_START"])
total_exports   = len(log_df[log_df["Event"] == "EXPORT_COMPLETE"])
print(f"  Simulation Period    : {SIMULATION_DAYS} days")
print(f"  Total Loadings       : {total_loads}")
print(f"  Total Discharges     : {total_discharge}")
print(f"  Total Volume Loaded  : {sim.total_loaded:,} bbl")
print(f"  Mother Export Voyages: {total_exports}")
print(f"  Total Volume Exported: {sim.total_exported:,} bbl")
print(f"  Total Volume Produced: {sim.total_produced:,.0f} bbl")
print(f"  Produced Spill/Overflow: {sim.total_spilled:,.0f} bbl")
print(f"  Final Storage Level (Total Point A+C+D+E): {sim.total_storage_bbl():,.0f} bbl")
print(f"    - {STORAGE_PRIMARY_NAME:<8}: {sim.storage_bbl[STORAGE_PRIMARY_NAME]:,.0f} bbl")
print(f"    - {STORAGE_SECONDARY_NAME:<8}: {sim.storage_bbl[STORAGE_SECONDARY_NAME]:,.0f} bbl")
print(f"    - {STORAGE_TERTIARY_NAME:<8}: {sim.storage_bbl[STORAGE_TERTIARY_NAME]:,.0f} bbl")
print(f"    - {STORAGE_QUATERNARY_NAME:<8}: {sim.storage_bbl[STORAGE_QUATERNARY_NAME]:,.0f} bbl")
print(f"    - {STORAGE_QUINARY_NAME:<8}: {sim.storage_bbl[STORAGE_QUINARY_NAME]:,.0f} bbl")
print(f"  Final Mother Level (Total Point B): {sim.total_mother_bbl():,.0f} bbl")
print(f"    - {MOTHER_PRIMARY_NAME:<9}: {sim.mother_bbl[MOTHER_PRIMARY_NAME]:,.0f} bbl")
print(f"    - {MOTHER_SECONDARY_NAME:<9}: {sim.mother_bbl[MOTHER_SECONDARY_NAME]:,.0f} bbl")
print(f"    - {MOTHER_TERTIARY_NAME:<9}: {sim.mother_bbl[MOTHER_TERTIARY_NAME]:,.0f} bbl")
print(f"  Storage Overflow     : {sim.storage_overflow_events} events")

print(f"\n{'-'*65}")
print("BERTHING ORDER AT MOTHER VESSELS (all voyages)")
print(f"{'-'*65}")
berth_mask = log_df["Event"] == "BERTHING_START_B"
print(log_df[berth_mask][display_cols].to_string(index=False))

# -----------------------------------------------------------------
# CHARTS
# -----------------------------------------------------------------

# ── Unique base colours per daughter vessel ──────────────────────
VESSEL_COLORS = {
    "Sherlock"  : "#e74c3c",   # red family
    "Laphroaig" : "#2ecc71",   # green family
    "Rathbone"  : "#9b59b6",   # purple family
    "Bedford"   : "#f39c12",   # amber family
    "Balham"    : "#1abc9c",   # teal family
    "Woodstock" : "#e91e63",   # pink family
    "Bagshot"   : "#00bcd4",   # cyan family
    "Watson"    : "#95a5a6",   # slate/gray family
}

# Each vessel gets a palette of shades derived from its base colour.
# Ordered from light (idle/waiting) → vivid (active ops) → dark (return)
import colorsys

def hex_to_rgb(h):
    h = h.lstrip('#')
    return tuple(int(h[i:i+2], 16)/255 for i in (0, 2, 4))

def shade(hex_color, lightness_factor):
    """Return a lighter/darker shade of hex_color by scaling lightness."""
    r, g, b = hex_to_rgb(hex_color)
    h, l, s = colorsys.rgb_to_hls(r, g, b)
    l2 = max(0.0, min(1.0, l * lightness_factor))
    r2, g2, b2 = colorsys.hls_to_rgb(h, l2, s)
    return "#{:02x}{:02x}{:02x}".format(int(r2*255), int(g2*255), int(b2*255))

# Map every status to a lightness factor for each vessel's palette
STATUS_LIGHTNESS = {
    "IDLE_A"                  : 2.0,    # lightest — at rest at storage
    "WAITING_STOCK"           : 1.8,
    "WAITING_BERTH_A"         : 1.7,
    "WAITING_DEAD_STOCK"      : 1.6,    # berthed but stock too low
    "BERTHING_A"              : 1.3,
    "HOSE_CONNECT_A"          : 1.1,
    "LOADING"                 : 1.0,    # base colour — active loading
    "DOCUMENTING"             : 0.9,
    "WAITING_CAST_OFF"        : 0.85,
    "CAST_OFF"                : 0.8,
    "SAILING_AB"              : 0.7,
    "SAILING_AB_LEG2"         : 0.65,
    "WAITING_FAIRWAY"         : 0.6,
    "WAITING_BERTH_B"         : 0.6,
    "WAITING_MOTHER_RETURN"   : 0.55,
    "WAITING_MOTHER_CAPACITY" : 0.5,
    "BERTHING_B"              : 0.5,
    "HOSE_CONNECT_B"          : 0.45,
    "DISCHARGING"             : 0.4,    # darkest active — discharging
    "CAST_OFF_B"              : 0.38,
    "SAILING_BA"              : 0.5,
    "IDLE_B"                  : 0.55,
    "WAITING_DAYLIGHT"        : 1.5,
}

def vessel_status_color(vessel_name, status):
    base = VESSEL_COLORS.get(vessel_name, "#95a5a6")
    factor = STATUS_LIGHTNESS.get(status, 1.0)
    return shade(base, factor)

fig, axes = plt.subplots(3, 1, figsize=(18, 16))
fig.patch.set_facecolor("#1a1a2e")
for ax in axes:
    ax.set_facecolor("#16213e")
    ax.tick_params(colors="white")
    ax.xaxis.label.set_color("white")
    ax.yaxis.label.set_color("white")
    ax.title.set_color("white")
    for spine in ax.spines.values():
        spine.set_edgecolor("#444")

fig.suptitle("Oil Tanker Daughter Vessel Operation — 30-Day Simulation (v5)",
             fontsize=15, fontweight="bold", y=0.99, color="white")

# ── Chart 1: Storage vessel volume ───────────────────────────────
ax1 = axes[0]
ax1.fill_between(timeline_df["Time"], timeline_df["Storage_bbl"],
                 alpha=0.25, color="#e67e22")
ax1.plot(timeline_df["Time"], timeline_df["Storage_bbl"],
            color="#e67e22", linewidth=2, label="Point A/C/D/E Total Storage Volume")
ax1.plot(timeline_df["Time"], timeline_df["Chapel_bbl"],
            color="#f1c40f", linewidth=1.4, alpha=0.9, label=f"{STORAGE_PRIMARY_NAME} Volume")
ax1.plot(timeline_df["Time"], timeline_df["JasmineS_bbl"],
            color="#8e44ad", linewidth=1.4, alpha=0.9, label=f"{STORAGE_SECONDARY_NAME} Volume")
ax1.plot(timeline_df["Time"], timeline_df["Westmore_bbl"],
            color="#27ae60", linewidth=1.4, alpha=0.9, label=f"{STORAGE_TERTIARY_NAME} Volume")
ax1.plot(timeline_df["Time"], timeline_df["Duke_bbl"],
            color="#3498db", linewidth=1.4, alpha=0.9, label=f"{STORAGE_QUATERNARY_NAME} Volume")
ax1.plot(timeline_df["Time"], timeline_df["Starturn_bbl"],
            color="#d35400", linewidth=1.4, alpha=0.9, label=f"{STORAGE_QUINARY_NAME} Volume")
ax1.axhline(STORAGE_CAPACITY_BBL, color="#e74c3c", linestyle="--", alpha=0.7,
                label=f"Std Storage Capacity ({STORAGE_CAPACITY_BBL:,} bbl)")
ax1.axhline(DUKE_STORAGE_CAPACITY_BBL, color="#3498db", linestyle="--", alpha=0.7,
                label=f"Duke Capacity ({DUKE_STORAGE_CAPACITY_BBL:,} bbl)")
ax1.axhline(STARTURN_STORAGE_CAPACITY_BBL, color="#d35400", linestyle="--", alpha=0.7,
                label=f"Starturn Capacity ({STARTURN_STORAGE_CAPACITY_BBL:,} bbl)")

# Dead-stock lines per vessel (175% of each cargo)
ds_colors = {"Sherlock": "#e74c3c", "Laphroaig": "#2ecc71",
             "Rathbone": "#9b59b6", "Bedford": "#f39c12",
         "Balham": "#1abc9c", "Woodstock": "#e91e63", "Bagshot": "#00bcd4", "Watson": "#95a5a6"}
for vname, vcap in [("Sherlock", DAUGHTER_CARGO_BBL),
                     ("Laphroaig", DAUGHTER_CARGO_BBL),
                     ("Rathbone", VESSEL_CAPACITIES.get("Rathbone", DAUGHTER_CARGO_BBL)),
                     ("Bedford",  VESSEL_CAPACITIES.get("Bedford",  DAUGHTER_CARGO_BBL)),
                     ("Balham",   VESSEL_CAPACITIES.get("Balham",   DAUGHTER_CARGO_BBL)),
                     ("Woodstock", VESSEL_CAPACITIES.get("Woodstock", DAUGHTER_CARGO_BBL)),
             ("Bagshot",  VESSEL_CAPACITIES.get("Bagshot",  DAUGHTER_CARGO_BBL)),
             ("Watson",   VESSEL_CAPACITIES.get("Watson",   DAUGHTER_CARGO_BBL))]:
    ds = DEAD_STOCK_FACTOR * vcap
    ax1.axhline(ds, color=ds_colors[vname], linestyle=":",
                alpha=0.8, linewidth=1.2,
                label=f"{vname} dead-stock ({ds:,.0f} bbl)")

ax1.set_ylabel("Volume (bbls)", fontsize=10, color="white")
ax1.set_title(
    f"Point A/C/D/E Storage — Prod std {PRODUCTION_RATE_BPH:,}, Duke {DUKE_PRODUCTION_RATE_BPH:,}, Starturn {STARTURN_PRODUCTION_RATE_BPH:,} bbl/hr",
    fontsize=11,
)
ax1.legend(loc="upper right", fontsize=7, facecolor="#0f3460", labelcolor="white", ncol=2)
ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
ax1.grid(True, alpha=0.2, color="#444")

# ── Chart 2: Mother vessel volume ────────────────────────────────
ax2 = axes[1]
ax2.fill_between(timeline_df["Time"], timeline_df["Mother_bbl"],
                 alpha=0.25, color="#2980b9")
ax2.plot(timeline_df["Time"], timeline_df["Mother_bbl"],
            color="#2980b9", linewidth=2, label="Point B Total Mother Volume")
ax2.plot(timeline_df["Time"], timeline_df["Bryanston_bbl"],
            color="#16a085", linewidth=1.4, alpha=0.9, label=f"{MOTHER_PRIMARY_NAME} Volume")
ax2.plot(timeline_df["Time"], timeline_df["Alkebulan_bbl"],
            color="#c0392b", linewidth=1.4, alpha=0.9, label=f"{MOTHER_SECONDARY_NAME} Volume")
ax2.plot(timeline_df["Time"], timeline_df["GreenEagle_bbl"],
            color="#8e44ad", linewidth=1.4, alpha=0.9, label=f"{MOTHER_TERTIARY_NAME} Volume")
ax2.axhline(MOTHER_EXPORT_TRIGGER, color="#e74c3c", linestyle="--", alpha=0.7,
                label=f"Per-Mother Export Trigger ({MOTHER_EXPORT_TRIGGER:,} bbl)")
ax2.axhline(MOTHER_CAPACITY_BBL, color="#922b21", linestyle="-.", alpha=0.5,
                label=f"Per-Mother Max Capacity ({MOTHER_CAPACITY_BBL:,} bbl)")
ax2.set_ylabel("Volume (bbls)", fontsize=10, color="white")
ax2.set_title(
    f"Point B Mothers ({MOTHER_PRIMARY_NAME} + {MOTHER_SECONDARY_NAME} + {MOTHER_TERTIARY_NAME}) — Volume Level",
    fontsize=11,
)
ax2.legend(loc="upper right", fontsize=8, facecolor="#0f3460", labelcolor="white")
ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
ax2.grid(True, alpha=0.2, color="#444")

# ── Chart 3: Gantt — vessel-colour-coded status bars ─────────────
ax3 = axes[2]
vessel_names = [v.name for v in sim.vessels]
y_pos = {name: i for i, name in enumerate(vessel_names)}

for _, row in timeline_df.iterrows():
    for vn in vessel_names:
        if vn in row and pd.notna(row[vn]):
            color = vessel_status_color(vn, row[vn])
            ax3.barh(y_pos[vn], TIME_STEP_HOURS / 24,
                     left=row["Day"] - 1 + (row["Time"].hour + row["Time"].minute/60) / 24,
                     color=color, edgecolor="none", height=0.65)

ax3.set_yticks(list(y_pos.values()))
ax3.set_yticklabels(list(y_pos.keys()), color="white", fontsize=11, fontweight="bold")
for label, vn in zip(ax3.get_yticklabels(), vessel_names):
    label.set_color(VESSEL_COLORS.get(vn, "white"))

ax3.set_xlabel("Simulation Day", fontsize=10, color="white")
ax3.set_title("Daughter Vessel Status Timeline — colour = vessel, shade = activity", fontsize=11)
ax3.set_xlim(0, SIMULATION_DAYS)
ax3.grid(True, alpha=0.15, color="#444", axis="x")

# Build legend: vessel colour swatches + key status shades
legend_items = []
for vn in vessel_names:
    base = VESSEL_COLORS.get(vn, "#95a5a6")
    legend_items.append(mpatches.Patch(color=base, label=f"── {vn} ──"))
    for status, label in [
        ("IDLE_A",              "Idle at storage (light)"),
        ("LOADING",             "Loading (base colour)"),
        ("WAITING_DEAD_STOCK",  "Waiting dead-stock"),
        ("SAILING_AB",          "Sailing A→B"),
        ("DISCHARGING",         "Discharging (dark)"),
        ("SAILING_BA",          "Returning B→(A/C/D/E)"),
    ]:
        legend_items.append(
            mpatches.Patch(color=vessel_status_color(vn, status),
                           label=f"  {label}")
        )

ax3.legend(handles=legend_items, loc="lower right", fontsize=6.5,
           facecolor="#0f3460", labelcolor="white", ncol=4,
           handlelength=1.5, handleheight=1.2)

plt.tight_layout(rect=[0, 0, 1, 0.97])

import os
# Prefer a writable output directory next to this script.
# If unavailable, fall back to the user's home directory.
script_dir = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(script_dir, "outputs")
try:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
except PermissionError:
    OUTPUT_DIR = os.path.join(os.path.expanduser("~"), "tanker_outputs")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
chart_path = os.path.join(OUTPUT_DIR, "tanker_simulation_charts_v5.png")
plt.savefig(chart_path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"\n[OK] Charts saved to {chart_path}")

def safe_csv_write(df, base_filename):
    path = os.path.join(OUTPUT_DIR, base_filename)
    try:
        df.to_csv(path, index=False)
        return path
    except PermissionError:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        stem, ext = os.path.splitext(base_filename)
        fallback_name = f"{stem}_{stamp}{ext}"
        fallback_path = os.path.join(OUTPUT_DIR, fallback_name)
        df.to_csv(fallback_path, index=False)
        print(f"[WARN] {base_filename} is locked. Saved fallback file: {fallback_path}")
        return fallback_path

if POINT_B_DISTRIBUTION_TEST_MODE:
    event_log_path = safe_csv_write(log_df, "tanker_event_log_point_b_3day_test.csv")
    timeline_path = safe_csv_write(timeline_df, "tanker_timeline_point_b_3day_test.csv")
else:
    event_log_path = safe_csv_write(log_df, "tanker_event_log_v5.csv")
    timeline_path = safe_csv_write(timeline_df, "tanker_timeline_v5.csv")
print("[OK] CSVs saved.")

