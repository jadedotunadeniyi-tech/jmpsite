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
from dataclasses import dataclass, field as _dc_field
import random

# Version identifier — read by tanker_app.py to auto-clear Streamlit cache
# on deployment. Bump this string whenever the sim logic changes in a way
# that would invalidate cached run_sim() results.
SIM_VERSION = "5.9"

# -----------------------------------------------------------------
# SANJULIAN — Intermediate / Transient Floating Storage at Point B
# -----------------------------------------------------------------
# SanJulian is NOT a mother vessel and NEVER participates in export
# voyages.  It acts as a secondary, overflow buffer at Point B:
#   - Receives daughter cargo ONLY when all primary mothers are
#     unable to receive on that day (last-resort secondary receiver).
#   - Stores cargo up to SANJULIAN_CAPACITY_BBL (450,000 bbl).
#   - Transloads to primary mothers when any of four triggers fire.
#   - Excluded from MOTHER_NAMES so the entire export state-machine
#     never touches it.
SANJULIAN_NAME               = "SanJulian"
SANJULIAN_CAPACITY_BBL       = 450_000
SANJULIAN_DISCHARGE_RATE_BPH = 11_000   # transload rate to primary mothers (bph)

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
TIDE_MIN_CROSSING_M = 1.6

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
DUKE_STARTURN_DEAD_STOCK_BBL = 5_000
DEAD_STOCK_FACTOR      = 1.75         # vessel must wait until 175% of its cargo is available
# Maximum hours a vessel may wait in HOSE_CONNECT_A for dead-stock before
# aborting and reassessing to a better-stocked storage.  At Starturn (83 bbl/hr)
# filling 47k from near-zero takes ~566h — the escape valve fires well before that.
DEAD_STOCK_MAX_WAIT_HOURS = 12.0      # if no progress after 12h, try a different storage
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
# Woodstock, Bagshot and Rathbone load at a reduced rate from Chapel
CHAPEL_LOAD_RATE_SLOW_BPH = 5_000
CHAPEL_SLOW_LOADERS    = {"Woodstock", "Bagshot", "Rathbone"}
JASMINES_LOAD_RATE_BPH = 7_083   # 85,000 bbl / 12h
WESTMORE_LOAD_RATE_BPH = 2_000
DUKE_LOAD_RATE_BPH     = 3_500
STARTURN_LOAD_RATE_BPH = 2_500
POINT_F_LOAD_RATE_BPH  = 165
POINT_F_SWAP_HOURS     = 2
POINT_F_MIN_TRIGGER_BBL = 65_000
STARTURN_PRE_TANK_TOP_TRIGGER_RATIO = 0.90
DUKE_PRE_TANK_TOP_TRIGGER_RATIO = 0.90
PRE_TANK_TOP_TRIGGER_RATIO_DEFAULT = 0.90
DUKE_MIN_REMAINING_BBL = 5_000
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
MIN_INCOMING_TRANSFER_BBL = min(VESSEL_CAPACITIES.values(), default=DAUGHTER_CARGO_BBL)
TIME_STEP_HOURS        = 0.5
EXPORT_RATE_BPH        = 20_000
EXPORT_DOC_HOURS       = 2
EXPORT_SAIL_HOURS      = 6
EXPORT_SAIL_WINDOW_START = 6
EXPORT_SAIL_WINDOW_END   = 15
EXPORT_HOSE_HOURS       = 4
EXPORT_SERIES_BUFFER_HOURS = 48

STORAGE_PRIMARY_NAME = "Chapel"
STORAGE_SECONDARY_NAME = "JasmineS"
STORAGE_TERTIARY_NAME = "Westmore"
STORAGE_QUATERNARY_NAME = "Duke"
STORAGE_QUINARY_NAME = "Starturn"
WESTMORE_PERMITTED_VESSELS = {"Sherlock", "Laphroaig", "Bagshot", "Rathbone", "Watson"}  # Woodstock not permitted at Point C
DUKE_PERMITTED_VESSELS = {"Woodstock", "Bagshot", "Rathbone"}
STARTURN_PERMITTED_VESSELS = {"Woodstock", "Rathbone"}
POINT_A_ONLY_VESSELS = {"Woodstock"}  # not permitted away from Point A
JASMINES_LOAD_CAP_MULTIPLIER = 1.08
WESTMORE_LOAD_CAP_MULTIPLIER = 0.82

# Bedford and Balham are 85k vessels but may only load 63k bbl at Point A
# (Chapel / JasmineS).  Their full capacity is used at all other points.
POINT_A_LOAD_CAP_VESSELS = {"Bedford", "Balham"}
POINT_A_LOAD_CAP_BBL     = 63_000


def storage_adjusted_load_cap(base_cap, storage_name, vessel_name=None):
    """Return effective cargo loaded from a storage for a vessel.

    Storage-specific multipliers apply before any explicit operational caps.
    JasmineS loads 8% above the vessel's normal capacity.
    Westmore loads 18% below the vessel's normal capacity.
    Explicit Point A caps for Bedford/Balham still take precedence.
    """
    cap = int(round(base_cap))
    if storage_name == STORAGE_SECONDARY_NAME:
        cap = int(round(base_cap * JASMINES_LOAD_CAP_MULTIPLIER))
    elif storage_name == STORAGE_TERTIARY_NAME:
        cap = int(round(base_cap * WESTMORE_LOAD_CAP_MULTIPLIER))
    if (vessel_name in POINT_A_LOAD_CAP_VESSELS
            and storage_name in {STORAGE_PRIMARY_NAME, STORAGE_SECONDARY_NAME}):
        cap = min(cap, POINT_A_LOAD_CAP_BBL)
    return max(0, cap)


def _default_vessel_base_capacity(vessel_name):
    return VESSEL_CAPACITIES.get(vessel_name, DAUGHTER_CARGO_BBL)


def _allowed_default_storages(vessel_name):
    if vessel_name in {"Bedford", "Balham"}:
        return [STORAGE_PRIMARY_NAME, STORAGE_SECONDARY_NAME, "Ibom"]
    if vessel_name == "Watson":
        return [STORAGE_PRIMARY_NAME, STORAGE_SECONDARY_NAME, STORAGE_TERTIARY_NAME]
    if vessel_name in POINT_A_ONLY_VESSELS:
        return [STORAGE_PRIMARY_NAME, STORAGE_SECONDARY_NAME]
    return [
        STORAGE_PRIMARY_NAME,
        STORAGE_SECONDARY_NAME,
        *([STORAGE_TERTIARY_NAME] if vessel_name in WESTMORE_PERMITTED_VESSELS else []),
        *([STORAGE_QUATERNARY_NAME] if vessel_name in DUKE_PERMITTED_VESSELS else []),
        *([STORAGE_QUINARY_NAME] if vessel_name in STARTURN_PERMITTED_VESSELS else []),
    ]


_DEFAULT_EFFECTIVE_LOAD_CAPS = []
for _vname in VESSEL_NAMES:
    _base_cap = _default_vessel_base_capacity(_vname)
    for _stor in _allowed_default_storages(_vname):
        if _stor == "Ibom":
            _DEFAULT_EFFECTIVE_LOAD_CAPS.append(_base_cap)
        else:
            _DEFAULT_EFFECTIVE_LOAD_CAPS.append(
                storage_adjusted_load_cap(_base_cap, _stor, _vname)
            )

if _DEFAULT_EFFECTIVE_LOAD_CAPS:
    MAX_DAUGHTER_CARGO = max(_DEFAULT_EFFECTIVE_LOAD_CAPS)
    MIN_INCOMING_TRANSFER_BBL = min(_DEFAULT_EFFECTIVE_LOAD_CAPS)
    MOTHER_EXPORT_TRIGGER = MOTHER_CAPACITY_BBL - MAX_DAUGHTER_CARGO

# -----------------------------------------------------------------
# CUSTOM VESSEL INJECTION
# -----------------------------------------------------------------
# Register mid-sim daughter vessels via add_custom_vessel() before
# instantiating Simulation().  Each vessel joins the fleet on the
# specified calendar date and participates fully in the dispatch cycle.
#
# Quick-start example:
#   add_custom_vessel(
#       name="Aldgate",
#       join_date="2025-02-10",        # or datetime.date(2025, 2, 10)
#       cargo_capacity=60_000,
#       permitted_storages=["Chapel", "JasmineS", "Duke"],
#   )

@dataclass
class CustomVesselSpec:
    """Specification for a daughter vessel that joins mid-simulation."""
    name:               str
    join_date:          object          # date | datetime | "YYYY-MM-DD"
    cargo_capacity:     int             # barrels per voyage
    permitted_storages: list = _dc_field(default_factory=list)
    # Resolved at Simulation.__init__ time — not set by caller
    _join_hour: float  = _dc_field(default=None, init=False, repr=False)


# Module-level registry — populated by add_custom_vessel(); cleared by
# run_sim() before restoring to let Streamlit reruns stay isolated.
_CUSTOM_VESSELS: list = []


def add_custom_vessel(name, join_date, cargo_capacity, permitted_storages=None):
    """Register a new daughter vessel to enter the fleet on *join_date*.

    Parameters
    ----------
    name : str
        Unique vessel name.  Must not clash with any name in VESSEL_NAMES.
    join_date : datetime.date | datetime.datetime | str "YYYY-MM-DD"
        Calendar date the vessel becomes active.  Activates at 08:00 on
        that day, matching the simulation's daily start anchor.
    cargo_capacity : int
        Maximum cargo per voyage in barrels (e.g. 60_000).
    permitted_storages : list[str] | None
        Storage names the vessel may load from.  Valid values:
            "Chapel"    (Point A)
            "JasmineS"  (Point A)
            "Westmore"  (Point C)
            "Duke"      (Point D)
            "Starturn"  (Point E)
        Pass None or [] to allow Chapel and JasmineS only (safe default).
    """
    if permitted_storages is None:
        permitted_storages = []
    if isinstance(join_date, str):
        join_date = _date.fromisoformat(join_date)
    if name in VESSEL_NAMES:
        raise ValueError(
            f"add_custom_vessel: '{name}' already exists in VESSEL_NAMES."
        )
    _valid = {STORAGE_PRIMARY_NAME, STORAGE_SECONDARY_NAME,
              STORAGE_TERTIARY_NAME, STORAGE_QUATERNARY_NAME,
              STORAGE_QUINARY_NAME}
    _bad = [s for s in permitted_storages if s not in _valid]
    if _bad:
        raise ValueError(
            f"add_custom_vessel: unknown storage name(s) {_bad}. "
            f"Valid: {sorted(_valid)}"
        )
    _CUSTOM_VESSELS.append(CustomVesselSpec(
        name=name,
        join_date=join_date,
        cargo_capacity=int(cargo_capacity),
        permitted_storages=list(permitted_storages),
    ))

# ── Vessel resumption dates ────────────────────────────────────────────────
# Maps existing daughter vessel name → {date, storage}.  Set via
# set_vessel_resumption() before creating a Simulation().
# The vessel sleeps (held in IDLE_A) until 08:00 on the resumption date,
# then wakes with absolute loading priority at the designated storage,
# bypassing the serial berthing gap — but still respecting storage_berth_free_at
# to avoid physical conflicts.  All resumption state is cleared after the
# priority berth completes so the vessel returns to normal operation.
_VESSEL_RESUMPTION_DATES: dict = {}


def set_vessel_resumption(name: str, date_val, storage: str) -> None:
    """Register a resumption hold for an existing daughter vessel.

    Args:
        name:     Vessel name (must exist in VESSEL_NAMES).
        date_val: Resumption date — a date/datetime object or "YYYY-MM-DD" string.
                  The vessel will be held idle until 08:00 on this date.
        storage:  Storage to lock to on wake (one of the five storage names).
    """
    _valid_storages = {
        "Chapel", "JasmineS", "Westmore", "Duke", "Starturn",
    }
    if name not in VESSEL_NAMES:
        raise ValueError(
            f"set_vessel_resumption: '{name}' is not a known vessel. "
            f"Known vessels: {VESSEL_NAMES}"
        )
    if storage not in _valid_storages:
        raise ValueError(
            f"set_vessel_resumption: unknown storage '{storage}'. "
            f"Valid storages: {sorted(_valid_storages)}"
        )
    if isinstance(date_val, str):
        from datetime import datetime as _dt
        date_val = _dt.fromisoformat(date_val).date()
    _VESSEL_RESUMPTION_DATES[name] = {"date": date_val, "storage": storage}


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

# -----------------------------------------------------------------
# DISPATCH BIAS — production-rate preference & position-aware spread
# -----------------------------------------------------------------
# High-production storages (Chapel/JasmineS/Westmore) get a small apparent
# gap tightening so they are sorted as more urgent than low-production peers
# (Duke/Starturn) at similar real risk levels.  Only active within
# DISPATCH_BIAS_FORECAST_BBL of critical so it never overrides a genuine
# emergency at Duke/Starturn.
DISPATCH_BIAS_FORECAST_BBL  = 150_000  # window inside which bias activates
DISPATCH_BIAS_MAX_FACTOR    = 0.22     # max apparent-gap compression (22 %)

# Route-area travel-time matrix used by position-aware spread forecasting.
# Keys are (from_area, to_area) as single-char strings matching STORAGE_POINT.
# Values are minimum travel hours (conservative lower bound, no waiting).
# A vessel *already at* the same area has 0h travel.
_ROUTE_TRAVEL_HOURS = {
    ("A", "A"): 0.0,
    ("A", "C"): 0.0,   # Points A and C share the same breakwater approach
    ("C", "A"): 0.0,
    ("C", "C"): 0.0,
    ("A", "B"): 7.0,   # A/C → BIA  (1.5 + 0.5 + 2 + 2, tidal delays ignored)
    ("C", "B"): 7.0,
    ("B", "A"): 8.0,   # BIA → A/C  (2 + 2 + 0.5 + 1.5)
    ("B", "C"): 8.0,
    ("D", "B"): 6.0,   # D → BIA    (3 + 1 + 0.5 + 1.5)
    ("B", "D"): 6.0,   # BIA → D    (1.5 + 0.5 + 1 + 3)
    ("E", "B"): 3.0,   # Starturn direct
    ("B", "E"): 3.0,
    ("D", "A"): 14.0,  # D → BIA + BIA → A  (conservative, through BIA)
    ("D", "C"): 14.0,
    ("A", "D"): 14.0,
    ("C", "D"): 14.0,
    ("D", "E"): 9.0,   # D → BIA → E  (conservative)
    ("E", "D"): 9.0,
    ("E", "A"): 11.0,
    ("E", "C"): 11.0,
    ("A", "E"): 11.0,
    ("C", "E"): 11.0,
}

# Spread to D/E is suppressed unless the projected stock at D/E will be below
# critical within SPREAD_DE_URGENCY_HORIZON hours of the vessel's ETA there.
SPREAD_DE_URGENCY_HORIZON = 12.0   # hours ahead to check — tighter gate suppresses more D/E spreads

# Optional: if A/C high-production storage is itself approaching critical
# within this window, hold A/C vessels back even if D/E is also in need.
SPREAD_AC_HOLD_HORIZON = 36.0

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
    "WAITING_RETURN_STOCK"  : "Waiting at Point B for return destination assignment",
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
        # Track when dead-stock waiting began so we can escape if too long
        self.dead_stock_wait_start = None
        # ── Resumption hold fields ────────────────────────────────────────
        # Set by Simulation.__init__ when _VESSEL_RESUMPTION_DATES is populated.
        self.resumption_hour        = None   # sim-hour of 08:00 on resumption date
        self.resumption_storage     = None   # storage locked to on wake
        self.resumption_priority    = False  # True while priority berth is pending
        self.resumption_hold_logged = False  # suppresses repeated RESUMPTION_HOLD spam

    def __repr__(self):
        return f"{self.name}[{self.status}|cargo={self.cargo_bbl:,}bbl]"


class Simulation:
    def __init__(self):
        self.storage_bbl = {
            name: min(STORAGE_INIT_BBL, STORAGE_CAPACITY_BY_NAME[name])
            for name in STORAGE_NAMES
        }
        self.mother_bbl = {name: MOTHER_INIT_BBL for name in MOTHER_NAMES}

        # ── SanJulian — intermediate floating storage (NOT a mother vessel) ──
        self.sanjulian_bbl               = 0.0   # current inventory (bbl)
        self.sanjulian_api               = 0.0   # blended API of stored crude
        # Separate berth timeline — never shared with mother_berth_free_at
        self.sanjulian_berth_free_at     = 0.0
        # Active transload: None | {"mother": str, "end_t": float, "vol": float}
        self.sanjulian_transload_state   = None
        # Cumulative stats
        self.sanjulian_total_received    = 0.0
        self.sanjulian_total_transloaded = 0.0

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
        # Post-breakwater A/C reassessment gate: remains inactive until a
        # daughter crosses inbound breakwater heading to Point A/C.
        self.ac_post_bw_reassess_active = False
        self.ac_post_bw_next_reassess_at = None
        self.daily_preops_last_day_key = -1

        # ── Custom vessel injection ───────────────────────────────────────────
        # Maps vessel_name → frozenset of permitted storage names.
        # An empty set means "Chapel + JasmineS only" (safe default).
        # Populated at join time; consulted by storage_allowed_for_vessel().
        self._custom_vessel_storage_permissions: dict = {}
        # Resolve each registered spec's join hour relative to _SIM_EPOCH.
        # Vessels whose join date precedes t=0 are clamped to t=0 so they
        # still enter the fleet rather than being silently skipped.
        self._pending_custom_vessels: list = []
        for _spec in _CUSTOM_VESSELS:
            _jd = _spec.join_date
            if isinstance(_jd, datetime):
                _join_dt = _jd.replace(hour=8, minute=0, second=0, microsecond=0)
            else:
                _join_dt = datetime(_jd.year, _jd.month, _jd.day, 8, 0)
            _spec._join_hour = max(
                0.0,
                (_join_dt - _SIM_EPOCH).total_seconds() / 3600.0,
            )
            self._pending_custom_vessels.append(_spec)

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

        # ── Vessel resumption dates ───────────────────────────────────────
        # Resolve each entry in _VESSEL_RESUMPTION_DATES to a sim-hour and
        # stamp the corresponding DaughterVessel object.
        # MUST run AFTER self.vessels is fully populated so lookups succeed.
        # Dates before t=0 are clamped to 0.0 — vessel wakes immediately but
        # still holds the priority storage lock for its first load.
        for _vname, _rentry in _VESSEL_RESUMPTION_DATES.items():
            _rv = next((vv for vv in self.vessels if vv.name == _vname), None)
            if _rv is None:
                continue
            _rdate = _rentry["date"]
            if isinstance(_rdate, datetime):
                _rdt = _rdate.replace(hour=8, minute=0, second=0, microsecond=0)
            else:
                _rdt = datetime(_rdate.year, _rdate.month, _rdate.day, 8, 0)
            _rhour = max(0.0, (_rdt - _SIM_EPOCH).total_seconds() / 3600.0)
            _rv.resumption_hour    = _rhour
            _rv.resumption_storage = _rentry["storage"]
            # Push the vessel's first event time forward so it sleeps silently
            # until the resumption tick fires.
            if _rhour > 0.0:
                _rv.next_event_time = _rhour

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

    def storage_load_hours(self, storage_name, cargo_bbl, vessel_name=None):
        """Return loading duration in hours for cargo_bbl loaded at storage_name.
        Woodstock, Bagshot and Rathbone load at CHAPEL_LOAD_RATE_SLOW_BPH (5,000 bph)
        when loading from Chapel; all other vessel/storage combinations use the
        standard rate map."""
        _RATE_MAP = {
            STORAGE_PRIMARY_NAME:   CHAPEL_LOAD_RATE_BPH,
            STORAGE_SECONDARY_NAME: JASMINES_LOAD_RATE_BPH,
            STORAGE_TERTIARY_NAME:  WESTMORE_LOAD_RATE_BPH,
            STORAGE_QUATERNARY_NAME: DUKE_LOAD_RATE_BPH,
            STORAGE_QUINARY_NAME:   STARTURN_LOAD_RATE_BPH,
        }
        rate = _RATE_MAP.get(storage_name)
        if rate:
            if storage_name == STORAGE_PRIMARY_NAME and vessel_name in CHAPEL_SLOW_LOADERS:
                rate = CHAPEL_LOAD_RATE_SLOW_BPH
            return cargo_bbl / rate
        return LOAD_HOURS  # fallback for unknown storages

    def effective_load_cap(self, vessel_name, storage_name):
        """Return the loading volume cap for vessel at storage_name.
        JasmineS loads 8% above normal capacity and Westmore loads 18%
        below normal capacity. Explicit Point A caps for Bedford/Balham
        still override those adjusted capacities.
        Pass storage_name="__any__" to get full capacity (non-Point-A probe).
        """
        vessel = next((v for v in self.vessels if v.name == vessel_name), None)
        full_cap = vessel.cargo_capacity if vessel else DAUGHTER_CARGO_BBL
        if storage_name == "__any__":
            return full_cap
        return storage_adjusted_load_cap(full_cap, storage_name, vessel_name)

    def loading_start_threshold(self, storage_name, cargo_bbl):
        if storage_name in (STORAGE_QUATERNARY_NAME, STORAGE_QUINARY_NAME):
            # Duke/Starturn rule: load can commence once stock is at least
            # nominated cargo plus fixed 5,000 bbl dead-stock buffer.
            # Examples: Woodstock@Duke 42k -> 47k threshold;
            # Bagshot@Duke 43k -> 48k threshold.
            required = cargo_bbl + DUKE_STARTURN_DEAD_STOCK_BBL
            return min(required, STORAGE_CAPACITY_BY_NAME[storage_name])
        required = DEAD_STOCK_FACTOR * cargo_bbl
        return min(required, STORAGE_CAPACITY_BY_NAME[storage_name])

    def storage_allowed_for_vessel(self, storage_name, vessel_name):
        # Custom vessels carry an explicit permitted-storage list.  When that
        # entry exists, use it exclusively — skip all standard permission sets.
        if vessel_name in self._custom_vessel_storage_permissions:
            allowed = self._custom_vessel_storage_permissions[vessel_name]
            # Empty set means "Chapel + JasmineS only" (safe Point-A default)
            if not allowed:
                return storage_name in (STORAGE_PRIMARY_NAME,
                                        STORAGE_SECONDARY_NAME)
            return storage_name in allowed
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
            # Proactive positioning fallback: still nominate a return storage so
            # the vessel can sail/berth and wait at hose connection for stock build.
            if not allowed_storages:
                return None, None, threshold_by_storage
            def _fallback_key(name):
                stock   = self.storage_bbl[name]
                raw_gap = abs(stock - STORAGE_CRITICAL_THRESHOLD_BY_NAME[name])
                if raw_gap <= DISPATCH_BIAS_FORECAST_BBL:
                    effective_gap = raw_gap * (1.0 - self.production_rate_bias_factor(name))
                else:
                    effective_gap = raw_gap
                return (stock, -effective_gap, name)
            fallback = max(allowed_storages, key=_fallback_key)
            return fallback, threshold_by_storage[fallback], threshold_by_storage

        def rank_key(storage_name):
            stock    = self.storage_bbl[storage_name]
            critical = STORAGE_CRITICAL_THRESHOLD_BY_NAME[storage_name]
            above_critical = 0 if stock >= critical else 1
            raw_gap  = abs(stock - critical)
            # Apply a small production-rate bias: compress the apparent gap for
            # high-production storages so they are preferred over low-production
            # peers at similar risk levels (gentle nudge, not an override).
            if raw_gap <= DISPATCH_BIAS_FORECAST_BBL:
                bias = self.production_rate_bias_factor(storage_name)
                effective_gap = raw_gap * (1.0 - bias)
            else:
                effective_gap = raw_gap
            return (above_critical, effective_gap, -stock, storage_name)

        selected = min(eligible, key=rank_key)
        return selected, threshold_by_storage[selected], threshold_by_storage

    def assign_ac_point_post_breakwater(self, v, t):
        """Determine and assign Point A/C target immediately after inbound
        A/C breakwater crossing. This guarantees each crossing vessel receives
        a post-breakwater point allocation, even if berthing must wait."""
        ac_allowed = [
            name for name in STORAGE_NAMES
            if STORAGE_POINT.get(name) in ("A", "C")
            and self.storage_allowed_for_vessel(name, v.name)
        ]
        if not ac_allowed:
            v.target_point = "A"
            self.log_event(
                t,
                v.name,
                "RETURN_POINT_ALLOCATED",
                "Post-breakwater reassessment: no explicit A/C storage permissions found; defaulting to Point A",
                voyage_num=v.current_voyage,
            )
            return

        # Prefer storages already meeting loading-start threshold; otherwise pick
        # highest-stock candidate so point assignment always exists.
        ready = []
        fallback = []
        for name in ac_allowed:
            cap = self.effective_load_cap(v.name, name)
            thr = self.loading_start_threshold(name, cap)
            stock = self.storage_bbl[name]
            rec = (stock, name, thr)
            fallback.append(rec)
            if stock >= thr:
                ready.append(rec)
        selected_stock, selected_storage, selected_thr = max(ready or fallback, key=lambda x: (x[0], x[1]))
        v.target_point = STORAGE_POINT.get(selected_storage, "A")
        self.log_event(
            t,
            v.name,
            "RETURN_POINT_ALLOCATED",
            f"Post-breakwater reassessment assigned Point {v.target_point} via {selected_storage} "
            f"({selected_stock:,.0f} bbl, loading-start threshold {selected_thr:,.0f} bbl)",
            voyage_num=v.current_voyage,
        )

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
        """True if tidal height is above minimum crossing level (or no table loaded)."""
        if _TIDE_TABLE is None:
            return True
        h = self.tide_height_at(hour)
        return h is not None and h > TIDE_MIN_CROSSING_M

    def tide_high_ok_at(self, hour):
        """Backward-compatible alias used by prior logic; no local-peak requirement."""
        return self.tide_ok_at(hour)

    def tidal_period_label(self, hour):
        wall_h = (hour + SIM_HOUR_OFFSET) % 24
        if DAYLIGHT_START <= wall_h < DAYLIGHT_END and self.tide_ok_at(hour):
            return "daylight tide >1.6m"
        return "outside daylight/tidal window"

    def tidal_periods_available_for_day(self, hour):
        """Return daylight tide availability summary for the calendar day of `hour`."""
        if _TIDE_TABLE is None:
            return "daylight operations (no tidal file)"
        day_key = int(hour // 24)
        day_start = day_key * 24
        valid_slots = 0
        for slot in [day_start + 0.5 * i for i in range(48)]:
            wall_h = (slot + SIM_HOUR_OFFSET) % 24
            if not (DAYLIGHT_START <= wall_h < DAYLIGHT_END):
                continue
            if self.tide_ok_at(slot):
                valid_slots += 1
        return (f"{valid_slots} daylight tide slot(s) >{TIDE_MIN_CROSSING_M:.1f}m"
                if valid_slots else f"no daylight tide >{TIDE_MIN_CROSSING_M:.1f}m")

    def next_tidal_sail(self, current_hour):
        """
        Return the earliest hour >= current_hour that satisfies BOTH:
          - daylight (DAYLIGHT_START <= (h+SIM_HOUR_OFFSET)%24 < DAYLIGHT_END)
                    - tide height > TIDE_MIN_CROSSING_M (skipped if no table)
        Scans forward in 0.5 h steps for up to 7 days.
        """
        # Fast path: no tidal table — fall back to pure daylight check
        if _TIDE_TABLE is None:
            return self.next_daylight_sail(current_hour)
        t = self.next_daylight_sail(current_hour)
        for _ in range(336):   # max 7 days * 48 half-hour steps
            wall_h = (t + SIM_HOUR_OFFSET) % 24
            if DAYLIGHT_START <= wall_h < DAYLIGHT_END and self.tide_ok_at(t):
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

    def next_daylight_hourly_berth_check(self, current_hour, point=None):
        """Return next hourly berthing recheck time in daylight window.
        If currently in daylight, checks again in 1 hour; otherwise at next
        daylight berthing window start."""
        wall_h = (current_hour + SIM_HOUR_OFFSET) % 24
        if BERTHING_START <= wall_h < BERTHING_END:
            nxt = round(current_hour + 1.0, 2)
            wall_next = (nxt + SIM_HOUR_OFFSET) % 24
            if BERTHING_START <= wall_next < BERTHING_END:
                return nxt
        days_elapsed = int(current_hour // 24)
        sim_bs_today = days_elapsed * 24 + (BERTHING_START - SIM_HOUR_OFFSET)
        if current_hour <= sim_bs_today:
            return sim_bs_today
        return sim_bs_today + 24

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

    def storage_locked_by_active_berth(self, storage_name, requesting_vessel=None):
        """True when another vessel is already berthed/connecting/loading at storage.
        This prevents reassignment override until that vessel completes loading."""
        lock_statuses = {"BERTHING_A", "HOSE_CONNECT_A", "LOADING"}
        for vv in self.vessels:
            if vv.name == requesting_vessel:
                continue
            if vv.assigned_storage != storage_name:
                continue
            if vv.status in lock_statuses:
                return True
        return False

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

    def point_b_candidate_slots(self, v, at_time):
        """Build feasible Point B receiver slots for vessel v at decision time.
        Primary mothers are always tried first.  SanJulian is appended as a
        secondary fallback ONLY when every primary mother is unavailable or full."""
        berthing_start = self.next_berthing_window(at_time, point="B")
        candidates = []
        for mother_name in MOTHER_NAMES:
            if not self.mother_is_at_point_b(mother_name, at_time):
                continue
            if self.mother_bbl[mother_name] + v.cargo_bbl > MOTHER_CAPACITY_BBL:
                continue
            earliest = max(berthing_start, self.mother_available_at[mother_name])
            berth_t = self.next_berthing_window(earliest, point="B")
            start = max(
                berth_t,
                self.mother_berth_free_at[mother_name],
                self.mother_available_at[mother_name],
            )
            start = self.next_berthing_window(start, point="B")
            candidates.append((start, berth_t, mother_name))

        # SanJulian secondary fallback — only when no primary mother is feasible
        if not candidates:
            sj_slot = self._sanjulian_candidate_slot(v, at_time)
            if sj_slot is not None:
                candidates.append((sj_slot, sj_slot, SANJULIAN_NAME))

        return berthing_start, candidates

    def mother_is_at_point_b(self, mother_name, t):
        """True when the mother is physically available at Point B."""
        if t < self.mother_available_at.get(mother_name, 0.0):
            return False
        return self.export_state.get(mother_name) not in {"SAILING", "HOSE", "IN_PORT"}

    def mother_export_departure_eligible(self, mother_name):
        """Export may depart if target reached OR mother cannot take another transfer."""
        stock = self.mother_bbl[mother_name]
        reached_target = stock >= MOTHER_EXPORT_VOLUME
        remaining_capacity = max(0.0, MOTHER_CAPACITY_BBL - stock)
        cannot_accommodate_next = remaining_capacity < MIN_INCOMING_TRANSFER_BBL
        return reached_target or cannot_accommodate_next

    def next_wall_clock_hour(self, current_hour, wall_clock_hour):
        """Return next sim-hour aligned to a wall-clock hour (0-23)."""
        day_key = int(current_hour // 24)
        sim_target_today = day_key * 24 + (wall_clock_hour - SIM_HOUR_OFFSET)
        if current_hour <= sim_target_today:
            return sim_target_today
        return sim_target_today + 24

    def projected_mother_stock(self, mother_name, horizon, exclude_vessel=None):
        """Projected mother stock by horizon based on currently committed BIA work."""
        projected = float(self.mother_bbl[mother_name])
        for vv in self.vessels:
            if vv.name == exclude_vessel:
                continue
            if vv.assigned_mother != mother_name or vv.cargo_bbl <= 0:
                continue
            add_at = None
            if vv.status == "HOSE_CONNECT_B":
                add_at = vv.next_event_time
            elif vv.status == "BERTHING_B":
                add_at = vv.next_event_time + HOSE_CONNECTION_HOURS
            elif vv.status == "WAITING_BERTH_B":
                add_at = vv.next_event_time + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS
            if add_at is not None and add_at <= horizon + 1e-6:
                projected += vv.cargo_bbl
        return projected

    def select_point_b_mother(self, v, decision_time, day_key, candidates):
        """Pick the best Point B receiver for faster turnaround and export readiness.
        SanJulian is always ranked last — primary mothers have strict priority."""
        assigned_today = self.point_b_day_assigned_mothers.setdefault(day_key, set())
        horizon_8 = self.next_wall_clock_hour(decision_time, 8)
        ranked = []
        for start, berth_t, mother_name in candidates:
            if mother_name == SANJULIAN_NAME:
                projected_8 = self.sanjulian_bbl + v.cargo_bbl
            else:
                add_at = start + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS
                projected_8 = self.projected_mother_stock(
                    mother_name, horizon_8, exclude_vessel=v.name,
                )
                if add_at <= horizon_8 + 1e-6:
                    projected_8 += v.cargo_bbl
            ranked.append({
                "start"        : start,
                "berth_t"      : berth_t,
                "mother"       : mother_name,
                "immediate"    : start <= decision_time + 0.01,
                "unused_today" : mother_name not in assigned_today,
                "projected_8"  : projected_8,
                "is_sanjulian" : mother_name == SANJULIAN_NAME,
            })

        immediate = [r for r in ranked if r["immediate"]]
        pool = immediate if immediate else ranked
        pool.sort(
            key=lambda r: (
                1 if r["is_sanjulian"] else 0,   # SanJulian always last
                -r["projected_8"],
                r["start"],
                0 if r["unused_today"] else 1,
                r["mother"],
            )
        )
        selected = pool[0]
        assigned_today.add(selected["mother"])
        return selected, horizon_8

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
            "SanJulian_bbl": round(self.sanjulian_bbl),
            "Total_Exported_bbl": self.total_exported,
        })

    def is_daylight_at(self, hour):
        """True if `hour` falls inside daylight operating window."""
        wall_h = (hour + SIM_HOUR_OFFSET) % 24
        return DAYLIGHT_START <= wall_h < DAYLIGHT_END

    # -----------------------------------------------------------------
    # Dispatch-bias helpers
    # -----------------------------------------------------------------

    def projected_stock_at(self, storage_name, horizon_h):
        """Project stock at *storage_name* `horizon_h` hours from now.
        Uses the current net production rate minus an estimate of scheduled
        loading draws.  Conservative: only subtracts vessels actively loading
        or already berthed/connecting at that storage.
        """
        stock  = self.storage_bbl[storage_name]
        rate   = self.production_rate_bph_at(storage_name, 0)  # t=0 is close enough
        cap    = STORAGE_CAPACITY_BY_NAME[storage_name]
        # Subtract expected draws from vessels already committed to this storage
        for vv in self.vessels:
            if vv.assigned_storage != storage_name:
                continue
            if vv.status in {"LOADING", "HOSE_CONNECT_A", "BERTHING_A"}:
                draw = self.effective_load_cap(vv.name, storage_name)
                stock = max(0.0, stock - draw)
        projected = stock + rate * horizon_h
        return min(projected, cap)

    def area_travel_hours(self, from_area, to_area):
        """Return the conservative lower-bound travel time in hours
        between two area codes (single-char strings: A/B/C/D/E).
        Falls back to 14h if the pair is not in the table.
        """
        if from_area == to_area:
            return 0.0
        return _ROUTE_TRAVEL_HOURS.get((from_area, to_area),
               _ROUTE_TRAVEL_HOURS.get((to_area, from_area), 14.0))

    def production_rate_bias_factor(self, storage_name):
        """Return a small bias multiplier [0, DISPATCH_BIAS_MAX_FACTOR] that
        shrinks the apparent critical-gap for high-production storages.
        The bias is proportional to the normalised production rate and is
        only non-zero for Chapel, JasmineS and Westmore (high-rate storages).
        """
        rate = STORAGE_PRODUCTION_RATE_BY_NAME.get(storage_name, 0.0)
        max_rate = max(STORAGE_PRODUCTION_RATE_BY_NAME.values()) or 1.0
        rate_norm = rate / max_rate           # 0..1 (1 = Chapel/JasmineS)
        return DISPATCH_BIAS_MAX_FACTOR * rate_norm

    def choose_hourly_storage_option(self, v, t):
        """Choose hourly reassessment storage option with risk-first priority.

        Two enhancements over the baseline:

        1. PRODUCTION-RATE BIAS
           High-production storages (Chapel / JasmineS / Westmore) receive a
           small apparent-gap compression of up to DISPATCH_BIAS_MAX_FACTOR
           (12 %) when within DISPATCH_BIAS_FORECAST_BBL of critical.  This
           means a high-production storage at, say, 35 k bbl above critical
           sorts as though it were 31 k above critical, nudging it ahead of
           a low-production peer at the same real gap.  The effect is gentle
           and never overrides a genuine Duke/Starturn emergency.

        2. POSITION-AWARE SPREAD WITH FORECASTING
           Spreading to Duke (D) or Starturn (E) is only offered to a vessel
           when:
             a) The vessel is permitted for that storage, AND
             b) No other vessel is already serving / en-route to it, AND
             c) The projected stock at D/E will be below (or within
                SPREAD_DE_URGENCY_HORIZON hours of reaching) critical by the
                time the vessel's ETA arrives there.
             d) No A/C high-production storage will itself enter an acute
                shortage within SPREAD_AC_HOLD_HORIZON hours that this vessel
                could otherwise cover.
           If none of the D/E candidates pass the urgency gate, the vessel
           stays on the best A/C option and the low-production storage is
           allowed to stretch — it will fill slowly under its own production.
        """
        if v.name == self.point_f_active_loader or v.target_point == "F":
            return None

        candidates = [
            s for s in STORAGE_NAMES
            if self.storage_allowed_for_vessel(s, v.name)
            and not self.storage_locked_by_active_berth(s, requesting_vessel=v.name)
        ]
        if not candidates:
            return None

        waiting_pool = [
            vv for vv in self.vessels
            if vv.status in {"WAITING_BERTH_A", "IDLE_A", "WAITING_STOCK"}
            and vv.target_point in ("A", "C", "D", "E")
            and vv.name != self.point_f_active_loader
        ]

        # ── Production-rate bias ──────────────────────────────────────────────
        # Compress the apparent gap for high-production storages when they are
        # close to critical, so they sort ahead of low-production peers.
        def biased_gap(storage_name):
            stock = self.storage_bbl[storage_name]
            crit  = STORAGE_CRITICAL_THRESHOLD_BY_NAME[storage_name]
            raw_gap = abs(stock - crit)
            if raw_gap <= DISPATCH_BIAS_FORECAST_BBL:
                bias = self.production_rate_bias_factor(storage_name)
                return raw_gap * (1.0 - bias)
            return raw_gap

        def risk_rank(storage_name):
            stock = self.storage_bbl[storage_name]
            crit  = STORAGE_CRITICAL_THRESHOLD_BY_NAME[storage_name]
            unsafe = 0 if stock >= crit else 1
            return (unsafe, biased_gap(storage_name), -stock, storage_name)

        ordered = sorted(candidates, key=risk_rank)

        # ── Position-aware spread to D/E ─────────────────────────────────────
        if len(waiting_pool) >= 2:
            v_area = STORAGE_POINT.get(v.assigned_storage, "A") if v.assigned_storage else "A"

            spread_storages_raw = [
                s for s in (STORAGE_QUATERNARY_NAME, STORAGE_QUINARY_NAME)
                if s in candidates
            ]

            # Sort spread candidates by urgency (most urgent first).
            spread_storages_raw = sorted(spread_storages_raw, key=risk_rank)

            for spread_storage in spread_storages_raw:
                de_area = STORAGE_POINT[spread_storage]   # "D" or "E"

                # (a) Skip if another vessel already committed to this storage.
                active_or_reserved = any(
                    vv.assigned_storage == spread_storage
                    and vv.name != v.name
                    and vv.status in {"WAITING_BERTH_A", "BERTHING_A",
                                      "HOSE_CONNECT_A", "LOADING",
                                      "SAILING_D_CHANNEL", "SAILING_CH_TO_BW_OUT",
                                      "SAILING_BW_TO_CH_IN", "SAILING_CH_TO_D",
                                      "SAILING_BA"}
                    for vv in self.vessels
                )
                if active_or_reserved:
                    continue

                # (b) Permission check (already in candidates, but be explicit).
                if not self.storage_allowed_for_vessel(spread_storage, v.name):
                    continue

                # (c) Position-aware urgency gate:
                #     Would this vessel actually arrive in time to help?
                #     Only spread if D/E stock will be at/below critical
                #     within SPREAD_DE_URGENCY_HORIZON hours of the vessel ETA.
                eta_to_de = self.area_travel_hours(v_area, de_area)
                proj_de   = self.projected_stock_at(spread_storage, eta_to_de)
                crit_de   = STORAGE_CRITICAL_THRESHOLD_BY_NAME[spread_storage]
                de_urgent = proj_de <= crit_de + (
                    STORAGE_PRODUCTION_RATE_BY_NAME.get(spread_storage, 0.0)
                    * SPREAD_DE_URGENCY_HORIZON
                )
                if not de_urgent:
                    # D/E is not in genuine need — skip this spread candidate.
                    continue

                # (d) A/C hold check: don't pull a vessel away from A/C if an
                #     A/C high-production storage will itself hit critical soon.
                ac_acute = False
                for ac_stor in (STORAGE_PRIMARY_NAME, STORAGE_SECONDARY_NAME,
                                STORAGE_TERTIARY_NAME):
                    if not self.storage_allowed_for_vessel(ac_stor, v.name):
                        continue
                    crit_ac   = STORAGE_CRITICAL_THRESHOLD_BY_NAME[ac_stor]
                    proj_ac   = self.projected_stock_at(ac_stor, SPREAD_AC_HOLD_HORIZON)
                    if proj_ac < crit_ac:
                        ac_acute = True
                        break
                if ac_acute and v_area in ("A", "C"):
                    # Hold this A/C vessel — A/C needs it more urgently.
                    continue

                # All gates passed — offer this vessel the spread assignment
                # if it is first in the eligible queue for this storage.
                queued = sorted(
                    [vv for vv in waiting_pool
                     if self.storage_allowed_for_vessel(spread_storage, vv.name)],
                    key=lambda x: (self.effective_load_cap(x.name, spread_storage), x.name),
                )
                if queued and queued[0].name == v.name:
                    return spread_storage

        # ── Default: pick best risk-priority candidate, prefer berth-now ─────
        def rank(storage_name):
            p = STORAGE_POINT.get(storage_name, "A")
            berth_now = (
                self.is_valid_berthing_time(t, point=p)
                and t >= self.storage_berth_free_at[storage_name]
                and t >= self.next_storage_berthing_start_at[p]
            )
            ord_idx = ordered.index(storage_name) if storage_name in ordered else 99
            return (0 if berth_now else 1, ord_idx)

        return min(candidates, key=rank)

    def trigger_ac_post_breakwater_reassessment(self, t, trigger_vessel=None):
        """Activate and run immediate A/C allocation reassessment after inbound
        breakwater crossing, then schedule hourly daylight reassessment pulses."""
        self.ac_post_bw_reassess_active = True
        self.run_ac_post_breakwater_reassessment(t, reason="breakwater-cross")
        self.ac_post_bw_next_reassess_at = round(t + 1.0, 2)

    def run_ac_post_breakwater_reassessment(self, t, reason="hourly"):
        """Wake idle A/C daughters so existing IDLE_A allocation rules can reassess
        and auto-assign berthing/loading where eligible."""
        for vv in self.vessels:
            if vv.status not in {"IDLE_A", "WAITING_BERTH_A", "WAITING_STOCK"}:
                continue
            if vv.target_point not in ("A", "C", "D", "E"):
                continue
            # Don't disturb a sleeping or priority-locked vessel
            if vv.resumption_priority or (vv.resumption_hour is not None and t < vv.resumption_hour):
                continue
            _new_storage = self.choose_hourly_storage_option(vv, t)
            if _new_storage and vv.assigned_storage != _new_storage:
                vv.assigned_storage = _new_storage
                vv.target_point = STORAGE_POINT.get(_new_storage, "A")
                self.log_event(
                    t,
                    vv.name,
                    "ALLOCATION_REASSESS",
                    f"Post-breakwater {reason} reassessment rerouted to {_new_storage}",
                    voyage_num=vv.current_voyage,
                )
            vv.status = "IDLE_A"
            vv.next_event_time = t
            self.log_event(
                t,
                vv.name,
                "ALLOCATION_REASSESS",
                f"Post-breakwater {reason} reassessment pulse at Point {vv.target_point}",
                voyage_num=vv.current_voyage,
            )

    def maybe_run_ac_post_breakwater_reassessment(self, t):
        """Run hourly reassessment pulses in daylight after activation trigger."""
        if not self.ac_post_bw_reassess_active:
            return
        if self.ac_post_bw_next_reassess_at is None:
            self.ac_post_bw_next_reassess_at = round(t + 1.0, 2)
            return
        while t >= self.ac_post_bw_next_reassess_at - 1e-9:
            pulse_t = self.ac_post_bw_next_reassess_at
            if self.is_daylight_at(pulse_t):
                self.run_ac_post_breakwater_reassessment(pulse_t, reason="hourly")
            self.ac_post_bw_next_reassess_at = round(self.ac_post_bw_next_reassess_at + 1.0, 2)

    def run_daily_preops_storage_reassessment(self, t, day_key):
        """Daily 05:00 Day2+ allocation checkpoint for storage-side daughters.
        Re-evaluates capacity-priority storage assignment without disabling any
        other allocation/reassessment mechanisms."""
        reassessed = 0
        for vv in self.vessels:
            if vv.status not in {"IDLE_A", "WAITING_STOCK", "WAITING_BERTH_A"}:
                continue
            if vv.name == self.point_f_active_loader or vv.target_point == "F":
                continue
            # Don't disturb a sleeping or priority-locked vessel
            if vv.resumption_priority or (vv.resumption_hour is not None and t < vv.resumption_hour):
                continue
            target_storage, required_stock, _ = self.return_allocation_candidate(vv.cargo_capacity, vv.name)
            if target_storage is None:
                continue
            new_point = STORAGE_POINT.get(target_storage, "A")
            changed = (
                vv.assigned_storage != target_storage
                or vv.target_point != new_point
                or vv.status != "IDLE_A"
            )
            vv.assigned_storage = target_storage
            vv.target_point = new_point
            vv.status = "IDLE_A"
            vv.next_event_time = t
            if changed:
                reassessed += 1
                self.log_event(
                    t,
                    vv.name,
                    "ALLOCATION_REASSESS",
                    f"Daily 05:00 Day {day_key + 1} storage reassessment: reassigned to Point {new_point} "
                    f"via {target_storage} (threshold {required_stock:,.0f} bbl)",
                    voyage_num=vv.current_voyage,
                )
        if reassessed == 0:
            self.log_event(
                t,
                "SYSTEM",
                "ALLOCATION_REASSESS",
                f"Daily 05:00 Day {day_key + 1} storage reassessment: no changes required",
            )

    def maybe_run_daily_preops_storage_reassessment(self, t):
        """Trigger daily storage reassessment at 05:00 from Day 2 onward."""
        wall_hour = round((t + SIM_HOUR_OFFSET) % 24, 2)
        day_key = int((t + SIM_HOUR_OFFSET) // 24)
        if day_key < 1:
            return
        if wall_hour != 5.0:
            return
        if self.daily_preops_last_day_key == day_key:
            return
        self.daily_preops_last_day_key = day_key
        self.run_daily_preops_storage_reassessment(t, day_key)

    # -----------------------------------------------------------------
    # SanJulian — intermediate floating storage helpers
    # -----------------------------------------------------------------

    def _sanjulian_primary_mothers_available(self, cargo_bbl, at_time):
        """True if at least one primary mother can currently receive cargo_bbl."""
        for mn in MOTHER_NAMES:
            if not self.mother_is_at_point_b(mn, at_time):
                continue
            if self.mother_bbl[mn] + cargo_bbl <= MOTHER_CAPACITY_BBL:
                return True
        return False

    def _sanjulian_candidate_slot(self, v, at_time):
        """Return earliest berthing time for v to discharge to SanJulian, or None.
        SanJulian is offered ONLY when no primary mother can receive the cargo."""
        if self._sanjulian_primary_mothers_available(v.cargo_bbl, at_time):
            return None
        if self.sanjulian_bbl + v.cargo_bbl > SANJULIAN_CAPACITY_BBL:
            return None
        start = max(
            self.next_berthing_window(at_time, point="B"),
            self.sanjulian_berth_free_at,
        )
        return self.next_berthing_window(start, point="B")

    def _run_sanjulian_transload(self, t):
        """Called every timestep.  Advances active transload and, when idle,
        evaluates four trigger conditions to start a new one.

        T1  SanJulian >= 90 % of capacity.
        T2  SanJulian holds enough to fill a mother to its export target.
        T3  A primary mother is idle at Point B and no daughter arrives today.
        T4  Optimisation: SanJulian > 25 % full and a mother has >= 50,000 bbl space.

        Transload rate: SANJULIAN_DISCHARGE_RATE_BPH (11,000 bph).
        Only one transload runs concurrently.
        """
        # ── Step A: advance an active transload ──────────────────────────────
        if self.sanjulian_transload_state is not None:
            state = self.sanjulian_transload_state
            if t >= state["end_t"]:
                mother  = state["mother"]
                vol_req = state["vol"]
                actual  = min(
                    vol_req,
                    self.sanjulian_bbl,
                    max(0.0, MOTHER_CAPACITY_BBL - self.mother_bbl[mother]),
                )
                if actual > 0:
                    self.mother_api[mother] = self.blend_api(
                        self.mother_bbl[mother], self.mother_api.get(mother, 0.0),
                        actual, self.sanjulian_api,
                    )
                    self.mother_bbl[mother]          += actual
                    self.sanjulian_bbl               -= actual
                    self.sanjulian_total_transloaded += actual
                    self.log_event(
                        t, SANJULIAN_NAME, "SJ_TRANSLOAD_COMPLETE",
                        f"Transloaded {actual:,.0f} bbl @ {self.sanjulian_api:.2f}° API "
                        f"-> {mother} | {mother} now {self.mother_bbl[mother]:,.0f} bbl | "
                        f"SanJulian now {self.sanjulian_bbl:,.0f} bbl",
                    )
                self.sanjulian_transload_state = None
            else:
                return   # transload in progress

        if self.sanjulian_bbl <= 0:
            return

        # ── Step B: evaluate trigger conditions ──────────────────────────────
        sj      = self.sanjulian_bbl
        sj_pct  = sj / SANJULIAN_CAPACITY_BBL
        day_key = int(t // 24)

        _outbound = {
            "SAILING_AB_LEG2", "SAILING_BW_TO_FWY", "SAILING_CROSS_BW_AC",
            "SAILING_AB", "SAILING_CROSS_BW_OUT", "SAILING_CH_TO_BW_OUT",
            "SAILING_D_CHANNEL",
        }
        daughters_arriving_today = any(
            vv.status in _outbound and int(vv.next_event_time // 24) == day_key
            for vv in self.vessels
        )

        for mother_name in MOTHER_NAMES:
            if not self.mother_is_at_point_b(mother_name, t):
                continue
            stock = self.mother_bbl[mother_name]
            space = max(0.0, MOTHER_CAPACITY_BBL - stock)
            if space < 1.0:
                continue

            trigger = False
            reason  = ""

            # T1: SanJulian at >= 90 % capacity
            if sj_pct >= 0.90:
                trigger = True
                reason  = f"T1 — SanJulian at {sj_pct*100:.0f}% capacity"

            # T2: enough to top up mother to export volume
            elif (stock < MOTHER_EXPORT_VOLUME
                  and sj >= MOTHER_EXPORT_VOLUME - stock
                  and space >= MOTHER_EXPORT_VOLUME - stock):
                trigger = True
                reason  = (f"T2 — can complete {mother_name}'s export target "
                           f"(need {MOTHER_EXPORT_VOLUME - stock:,.0f} bbl)")

            # T3: mother idle and no daughter arriving today
            elif (not daughters_arriving_today
                  and not any(
                      vv.assigned_mother == mother_name
                      and vv.status in {"BERTHING_B", "HOSE_CONNECT_B",
                                        "DISCHARGING", "WAITING_BERTH_B"}
                      for vv in self.vessels)):
                trigger = True
                reason  = f"T3 — {mother_name} idle, no daughter arrivals today"

            # T4: optimisation — meaningful inventory, mother has room
            elif sj_pct >= 0.25 and space >= 50_000:
                trigger = True
                reason  = (f"T4 — optimisation: SanJulian {sj_pct*100:.0f}% full, "
                           f"{mother_name} has {space:,.0f} bbl space")

            if not trigger:
                continue

            transfer_vol = min(sj, space)
            if transfer_vol <= 0:
                continue

            duration = max(TIME_STEP_HOURS, transfer_vol / SANJULIAN_DISCHARGE_RATE_BPH)
            self.sanjulian_transload_state = {
                "mother": mother_name,
                "end_t" : t + duration,
                "vol"   : transfer_vol,
            }
            self.log_event(
                t, SANJULIAN_NAME, "SJ_TRANSLOAD_START",
                f"Transload -> {mother_name}: {transfer_vol:,.0f} bbl "
                f"@ {SANJULIAN_DISCHARGE_RATE_BPH:,} bph ({duration:.1f}h) — {reason}",
            )
            break   # one concurrent transload at a time

    # -- Main simulation loop ---------------------------------------------
    def run(self):
        total_hours = SIMULATION_DAYS * 24
        t = 0.0

        while t <= total_hours:
            self.maybe_run_daily_preops_storage_reassessment(t)
            self.maybe_run_ac_post_breakwater_reassessment(t)
            self._run_sanjulian_transload(t)

            # ── Custom vessel join ────────────────────────────────────────────
            # At each timestep check whether any registered custom vessel is due
            # to join the fleet.  A vessel joins exactly once: it is appended to
            # self.vessels, its per-vessel storage permissions are recorded, and
            # a VESSEL_JOINED event is written to the event log so it appears in
            # the dashboard and CSV exports.
            if self._pending_custom_vessels:
                _still_pending = []
                for _spec in self._pending_custom_vessels:
                    if t < _spec._join_hour:
                        _still_pending.append(_spec)
                        continue
                    # Register storage permissions for this vessel
                    self._custom_vessel_storage_permissions[_spec.name] = set(
                        _spec.permitted_storages
                    )
                    # Instantiate and configure — starts IDLE_A, ready immediately
                    _nv = DaughterVessel(
                        _spec.name,
                        start_offset_hours=t,
                        cargo_capacity=_spec.cargo_capacity,
                    )
                    _nv.status             = "IDLE_A"
                    _nv.target_point       = "A"
                    _nv.next_event_time    = t
                    _nv._voyage_assigned   = False
                    self.vessels.append(_nv)
                    self.vessel_api[_spec.name] = 0.0
                    _perm_str = (
                        ", ".join(sorted(_spec.permitted_storages))
                        if _spec.permitted_storages
                        else "Chapel, JasmineS (default)"
                    )
                    self.log_event(
                        t, _spec.name, "VESSEL_JOINED",
                        f"Custom vessel joined fleet — capacity {_spec.cargo_capacity:,} bbl, "
                        f"permitted storages: {_perm_str}",
                    )
                self._pending_custom_vessels = _still_pending
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
                    # ── Resumption hold ───────────────────────────────────────
                    # If this vessel has a future resumption hour, hold it here
                    # until t >= resumption_hour.  Log once then sleep silently.
                    if v.resumption_hour is not None and t < v.resumption_hour:
                        if not v.resumption_hold_logged:
                            v.resumption_hold_logged = True
                            self.log_event(
                                t, v.name, "RESUMPTION_HOLD",
                                f"Vessel held idle — resumption scheduled "
                                f"{self.hours_to_dt(v.resumption_hour).strftime('%Y-%m-%d %H:%M')} "
                                f"with priority load at {v.resumption_storage}",
                                voyage_num=v.current_voyage,
                            )
                        v.next_event_time = v.resumption_hour
                        continue

                    # ── Priority resumption wake ──────────────────────────────
                    # On the first tick at or after resumption_hour: lock to the
                    # designated storage, assign voyage, go straight to berthing,
                    # bypassing the serial start-gap (but honouring
                    # storage_berth_free_at to prevent physical collision).
                    if v.resumption_hour is not None and not v.resumption_priority:
                        v.resumption_priority = True

                    if v.resumption_priority:
                        _rs = v.resumption_storage
                        if not hasattr(v, '_voyage_assigned') or not v._voyage_assigned:
                            self.voyage_counter += 1
                            v.current_voyage = self.voyage_counter
                            v._voyage_assigned = True
                        _rpoint = STORAGE_POINT.get(_rs, "A")
                        _rcap   = self.effective_load_cap(v.name, _rs)
                        _rload  = self.storage_load_hours(_rs, _rcap, vessel_name=v.name)
                        v.assigned_storage    = _rs
                        v.assigned_load_hours = _rload
                        v.target_point        = _rpoint
                        # Honour physical berth availability — bypass only the
                        # serial start-gap (next_storage_berthing_start_at).
                        if not self.is_valid_berthing_time(t, point=_rpoint) \
                                or t < self.storage_berth_free_at[_rs]:
                            _next_chk = self.next_daylight_hourly_berth_check(t, point=_rpoint)
                            v.next_event_time = _next_chk
                            continue
                        v.status = "BERTHING_A"
                        self.storage_berth_free_at[_rs] = (
                            t + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + _rload
                        )
                        # Do NOT advance next_storage_berthing_start_at — priority vessel
                        # bypasses the inter-berth serial gap entirely.
                        v.next_event_time = t + BERTHING_DELAY_HOURS
                        _rslot = (VESSEL_NAMES.index(v.name) + 1) if v.name in VESSEL_NAMES else "C"
                        self.log_event(
                            t, v.name, "RESUMPTION_BERTHING",
                            f"Priority resumption berthing at {_rs} "
                            f"(resumed {self.hours_to_dt(v.resumption_hour).strftime('%Y-%m-%d')}, "
                            f"bypassing queue) [rotation slot {_rslot} of {NUM_DAUGHTERS}]",
                            voyage_num=v.current_voyage,
                        )
                        # Clear all resumption state — vessel runs normally from here
                        v.resumption_hour        = None
                        v.resumption_storage     = None
                        v.resumption_priority    = False
                        v.resumption_hold_logged = False
                        continue

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

                    # If a vessel was manually seeded at a specific storage with
                    # IDLE_A status, honour that assignment immediately: skip the
                    # stock-gate and go straight to berthing.  The dead-stock check
                    # in HOSE_CONNECT_A will hold loading until the threshold is met.
                    if v.assigned_storage and self.storage_allowed_for_vessel(v.assigned_storage, v.name):
                        _pre_assigned = v.assigned_storage
                        cap           = self.effective_load_cap(v.name, _pre_assigned)
                        _pre_stock    = self.storage_bbl[_pre_assigned]
                        _pre_point    = STORAGE_POINT.get(_pre_assigned, "A")
                        _pre_thresh   = self.loading_start_threshold(_pre_assigned, cap)
                        _berth_now_ok = (
                            self.is_valid_berthing_time(t, point=_pre_point)
                            and t >= self.storage_berth_free_at[_pre_assigned]
                            and t >= self.next_storage_berthing_start_at[_pre_point]
                        )
                        if not _berth_now_ok:
                            _next_chk = self.next_daylight_hourly_berth_check(t, point=_pre_point)
                            v.status = "WAITING_BERTH_A"
                            v.target_point = _pre_point
                            v.next_event_time = _next_chk
                            self.log_event(
                                t,
                                v.name,
                                "WAITING_BERTH_A",
                                f"Arrived/idle for {_pre_assigned}; berth unavailable now — hourly daylight recheck at "
                                f"{self.hours_to_dt(_next_chk).strftime('%Y-%m-%d %H:%M')}",
                                voyage_num=v.current_voyage,
                            )
                            continue
                        _pre_start = t
                        load_hours = self.storage_load_hours(_pre_assigned, cap, vessel_name=v.name)
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
                        slot = (VESSEL_NAMES.index(v.name) + 1) if v.name in VESSEL_NAMES else "C"
                        self.log_event(_pre_start, v.name, "BERTHING_START_A",
                                       f"Berthing at {_pre_assigned} (pre-assigned, 30 min procedure) "
                                       f"[rotation slot {slot} of {NUM_DAUGHTERS}]",
                                       voyage_num=v.current_voyage)
                        continue

                    # Use global permitted candidate pool so A/C defaults do not
                    # starve Duke/Starturn when they are approaching unsafe levels.
                    eligible_storage_names = [
                        name for name in STORAGE_NAMES
                        if self.storage_allowed_for_vessel(name, v.name)
                    ]

                    candidate_storages = []
                    for storage_name in eligible_storage_names:
                        if not self.storage_allowed_for_vessel(storage_name, v.name):
                            continue
                        if self.storage_locked_by_active_berth(storage_name, requesting_vessel=v.name):
                            continue
                        cap = self.effective_load_cap(v.name, storage_name)
                        stock = self.storage_bbl[storage_name]
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
                        crit = STORAGE_CRITICAL_THRESHOLD_BY_NAME.get(storage_name, STORAGE_CAPACITY_BY_NAME.get(storage_name, 1.0))
                        risk_gap = stock - crit
                        candidate_storages.append((
                            storage_name,
                            stock,
                            berth_t,
                            start,
                            threshold_required,
                            crit,
                            risk_gap,
                        ))

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
                                # Unsafe/borderline first to suppress local deterioration.
                                0 if x[6] >= 0 else 1,
                                -x[6],
                                # Keep stock-feasible candidates advantaged for immediate throughput.
                                0 if x[1] >= x[4] else 1,
                                x[3],
                                -x[1],
                                x[0],
                            )
                        )
                        selected_storage, selected_stock, berth_t, start, threshold_required, _crit, _risk_gap = candidate_storages[0]
                        selected_point = STORAGE_POINT.get(selected_storage, "A")
                        berth_now_ok = (
                            self.is_valid_berthing_time(t, point=selected_point)
                            and t >= self.storage_berth_free_at[selected_storage]
                            and t >= self.next_storage_berthing_start_at[selected_point]
                        )
                        v.assigned_storage = selected_storage
                        load_hours = self.storage_load_hours(selected_storage, cap, vessel_name=v.name)
                        v.assigned_load_hours = load_hours

                        if not berth_now_ok:
                            v.status = "WAITING_BERTH_A"
                            v.target_point = selected_point
                            next_check = self.next_daylight_hourly_berth_check(t, point=selected_point)
                            v.next_event_time = next_check
                            self.log_event(
                                t,
                                v.name,
                                "WAITING_BERTH_A",
                                f"Berth unavailable at {selected_storage}; hourly daylight recheck at "
                                f"{self.hours_to_dt(next_check).strftime('%Y-%m-%d %H:%M')}",
                                voyage_num=v.current_voyage,
                            )
                            continue

                        # The berth is reserved. We do NOT pre-commit stock
                        # here because the dead-stock rule may delay the
                        # actual loading start — stock is committed only once
                        # the 175% threshold is confirmed in HOSE_CONNECT_A.
                        v.status = "BERTHING_A"
                        start = t
                        self.storage_berth_free_at[selected_storage] = (
                            start + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + load_hours
                        )
                        self.next_storage_berthing_start_at[selected_point] = (
                            start + BERTHING_DELAY_HOURS + POST_BERTHING_START_GAP_HOURS
                        )
                        v.next_event_time = start + BERTHING_DELAY_HOURS

                        slot = (VESSEL_NAMES.index(v.name) + 1) if v.name in VESSEL_NAMES else "C"
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
                                       f"No eligible storage assignment currently available at Point {v.target_point} "
                                       f"(active berth locks and/or stock constraints; {storage_levels}; "
                                       f"threshold guide {min_threshold:,.0f} bbl) — waiting for hourly reassessment/reroute",
                                       voyage_num=v.current_voyage)

                elif v.status == "BERTHING_A":
                    v.status = "HOSE_CONNECT_A"
                    v.next_event_time = t + HOSE_CONNECTION_HOURS
                    berth_storage = v.assigned_storage or STORAGE_PRIMARY_NAME
                    self.log_event(t, v.name, "HOSE_CONNECTION_START_A",
                                   f"Hose connection initiated at {berth_storage} (2 hours)",
                                   voyage_num=v.current_voyage)

                elif v.status == "WAITING_BERTH_A":
                    selected_storage = v.assigned_storage
                    if not selected_storage or not self.storage_allowed_for_vessel(selected_storage, v.name):
                        v.status = "IDLE_A"
                        v.next_event_time = t
                        continue

                    # Don't override the locked storage for a priority vessel
                    if not v.resumption_priority:
                        _alt_storage = self.choose_hourly_storage_option(v, t)
                        if _alt_storage and _alt_storage != selected_storage:
                            selected_storage = _alt_storage
                            v.assigned_storage = selected_storage
                            v.target_point = STORAGE_POINT.get(selected_storage, "A")
                            self.log_event(
                                t,
                                v.name,
                                "ALLOCATION_REASSESS",
                                f"Hourly reassessment switched waiting berth target to {selected_storage}",
                                voyage_num=v.current_voyage,
                            )

                    selected_point = STORAGE_POINT.get(selected_storage, "A")

                    # ── Stock-aware berth reassignment ────────────────────────────────
                    # If the currently assigned storage has far too little stock to
                    # be worth waiting for (e.g. Starturn at <50% of threshold
                    # while another storage is already above threshold), proactively
                    # switch — prevents vessels idling indefinitely outside an empty tank.
                    _assigned_cap = self.effective_load_cap(v.name, selected_storage)
                    _assigned_thr = self.loading_start_threshold(selected_storage, _assigned_cap)
                    _assigned_stk = self.storage_bbl[selected_storage]
                    if not v.resumption_priority and _assigned_stk < _assigned_thr * 0.5:
                        for _alt in STORAGE_NAMES:
                            if _alt == selected_storage:
                                continue
                            if not self.storage_allowed_for_vessel(_alt, v.name):
                                continue
                            if self.storage_locked_by_active_berth(_alt, requesting_vessel=v.name):
                                continue
                            _alt_cap = self.effective_load_cap(v.name, _alt)
                            _alt_thr = self.loading_start_threshold(_alt, _alt_cap)
                            if self.storage_bbl[_alt] >= _alt_thr:
                                self.log_event(
                                    t, v.name, "ALLOCATION_REASSESS",
                                    f"Stock-aware reassignment: {selected_storage} only "
                                    f"{_assigned_stk:,.0f}/{_assigned_thr:,.0f} bbl (<50% threshold); "
                                    f"switching to {_alt} ({self.storage_bbl[_alt]:,.0f} bbl ready)",
                                    voyage_num=v.current_voyage,
                                )
                                selected_storage = _alt
                                v.assigned_storage = _alt
                                v.target_point = STORAGE_POINT.get(_alt, "A")
                                selected_point = v.target_point
                                break

                    # Priority vessels bypass the inter-berth serial gap but still
                    # respect storage_berth_free_at (physical availability).
                    if v.resumption_priority:
                        berth_now_ok = (
                            self.is_valid_berthing_time(t, point=selected_point)
                            and t >= self.storage_berth_free_at[selected_storage]
                        )
                    else:
                        berth_now_ok = (
                            self.is_valid_berthing_time(t, point=selected_point)
                            and t >= self.storage_berth_free_at[selected_storage]
                            and t >= self.next_storage_berthing_start_at[selected_point]
                        )
                    if not berth_now_ok:
                        next_check = self.next_daylight_hourly_berth_check(t, point=selected_point)
                        v.next_event_time = next_check
                        self.log_event(
                            t,
                            v.name,
                            "WAITING_BERTH_A",
                            f"Berth still unavailable at {selected_storage}; hourly daylight recheck at "
                            f"{self.hours_to_dt(next_check).strftime('%Y-%m-%d %H:%M')}",
                            voyage_num=v.current_voyage,
                        )
                        continue

                    cap = self.effective_load_cap(v.name, selected_storage)
                    load_hours = self.storage_load_hours(selected_storage, cap, vessel_name=v.name)
                    v.assigned_load_hours = load_hours
                    v.status = "BERTHING_A"
                    self.storage_berth_free_at[selected_storage] = (
                        t + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + load_hours
                    )
                    # Priority vessels skip the serial start-gap advancement
                    if not v.resumption_priority:
                        self.next_storage_berthing_start_at[selected_point] = (
                            t + BERTHING_DELAY_HOURS + POST_BERTHING_START_GAP_HOURS
                        )
                    v.next_event_time = t + BERTHING_DELAY_HOURS
                    slot = (VESSEL_NAMES.index(v.name) + 1) if v.name in VESSEL_NAMES else "C"
                    if v.resumption_priority:
                        _rh_disp = v.resumption_hour
                        self.log_event(
                            t, v.name, "RESUMPTION_BERTHING",
                            f"Priority resumption berthing at {selected_storage} "
                            f"(resumed {self.hours_to_dt(_rh_disp).strftime('%Y-%m-%d') if _rh_disp else 'pending'}, "
                            f"bypassing queue) [rotation slot {slot} of {NUM_DAUGHTERS}]",
                            voyage_num=v.current_voyage,
                        )
                        # Clear all resumption state — vessel runs normally from here
                        v.resumption_hour        = None
                        v.resumption_storage     = None
                        v.resumption_priority    = False
                        v.resumption_hold_logged = False
                    else:
                        self.log_event(
                            t,
                            v.name,
                            "BERTHING_START_A",
                            f"Berthing at {selected_storage} after standby (30 min procedure) "
                            f"[rotation slot {slot} of {NUM_DAUGHTERS}]",
                            voyage_num=v.current_voyage,
                        )

                elif v.status == "HOSE_CONNECT_A":
                    # ── Dead-stock rule enforced here ───────────────────
                    # Loading can only commence when storage holds at least
                    # the storage-specific loading-start threshold.
                    # For Duke/Starturn this is nominated cargo + 5,000 bbl;
                    # other storages use 175% dead-stock. The cargo was
                    # NOT pre-committed in IDLE_A for the berth reservation;
                    # it is committed here once the threshold is satisfied.
                    selected_storage = v.assigned_storage or STORAGE_PRIMARY_NAME
                    cap = self.effective_load_cap(v.name, selected_storage)
                    threshold_required = self.loading_start_threshold(selected_storage, cap)
                    load_hours = v.assigned_load_hours if v.assigned_load_hours is not None else LOAD_HOURS
                    # Recompute load_hours based on effective cap (handles Point A cap)
                    load_hours = self.storage_load_hours(selected_storage, cap, vessel_name=v.name)
                    if self.storage_bbl[selected_storage] < threshold_required:
                        # ── Dead-stock escape valve ────────────────────────────────────
                        # Track when the wait started.  If a vessel has been stuck here
                        # longer than DEAD_STOCK_MAX_WAIT_HOURS (default 12h) without
                        # loading commencing, check if another storage this vessel is
                        # permitted to use already has enough stock.  If so, abort the
                        # berth, release the lock, and let the vessel reassign — this
                        # prevents Starturn/Duke (83/250 bbl/hr) from trapping fast
                        # vessels for days while Chapel/JasmineS/Westmore are full.
                        if v.dead_stock_wait_start is None:
                            v.dead_stock_wait_start = t
                        wait_so_far = t - v.dead_stock_wait_start
                        if wait_so_far >= DEAD_STOCK_MAX_WAIT_HOURS:
                            # Find an alternative storage that is ready NOW
                            alt_storage = None
                            for alt in STORAGE_NAMES:
                                if alt == selected_storage:
                                    continue
                                if not self.storage_allowed_for_vessel(alt, v.name):
                                    continue
                                if self.storage_locked_by_active_berth(alt, requesting_vessel=v.name):
                                    continue
                                alt_cap   = self.effective_load_cap(v.name, alt)
                                alt_thr   = self.loading_start_threshold(alt, alt_cap)
                                if self.storage_bbl[alt] >= alt_thr:
                                    alt_storage = alt
                                    break
                            if alt_storage:
                                # Release the current berth lock and reassign
                                self.storage_berth_free_at[selected_storage] = t
                                v.assigned_storage = alt_storage
                                v.target_point = STORAGE_POINT.get(alt_storage, "A")
                                v.status = "IDLE_A"
                                v.next_event_time = t
                                v.dead_stock_wait_start = None
                                self.log_event(
                                    t, v.name, "ALLOCATION_REASSESS",
                                    f"Dead-stock escape: waited {wait_so_far:.1f}h at {selected_storage} "
                                    f"({self.storage_bbl[selected_storage]:,.0f}/{threshold_required:,.0f} bbl); "
                                    f"reassigned to {alt_storage} ({self.storage_bbl[alt_storage]:,.0f} bbl available)",
                                    voyage_num=v.current_voyage,
                                )
                                continue
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
                    v.dead_stock_wait_start = None  # reset escape timer
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
                    berthing_start, candidates = self.point_b_candidate_slots(v, arrival)
                    if berthing_start > arrival + 0.01:
                        self.log_event(arrival, v.name, "WAITING_NIGHT",
                                       f"Arrived at {self.hours_to_dt(arrival).strftime('%H:%M')} outside berthing window — "
                                       f"waiting until {self.hours_to_dt(berthing_start).strftime('%Y-%m-%d %H:%M')}",
                                       voyage_num=v.current_voyage)

                    if not candidates:
                        mother_levels = ", ".join(
                            f"{name}: {self.mother_bbl[name]:,.0f}/{MOTHER_CAPACITY_BBL:,} bbl"
                            for name in MOTHER_NAMES
                        )
                        next_recheck = self.next_daylight_hourly_berth_check(arrival, point="B")
                        wait_h = max(0.0, next_recheck - arrival)
                        self.log_event(arrival, v.name, "WAITING_MOTHER_CAPACITY",
                                       f"No berth/capacity slot currently available on Point B mothers ({mother_levels}); "
                                       f"hourly daylight reassessment at "
                                       f"{self.hours_to_dt(next_recheck).strftime('%Y-%m-%d %H:%M')} "
                                       f"(wait {wait_h:.1f}h)",
                                       voyage_num=v.current_voyage)
                        v.next_event_time = next_recheck
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
                            selected_meta, _ = self.select_point_b_mother(
                                v,
                                arrival,
                                day_key,
                                candidates,
                            )
                            selected_mother = selected_meta["mother"]
                            selected = (
                                selected_meta["start"],
                                selected_meta["berth_t"],
                                selected_meta["mother"],
                            )

                        start, berth_t, selected_mother = selected
                        v.assigned_mother = selected_mother
                        if start > arrival + 0.01:
                            v.status = "WAITING_BERTH_B"
                            v.next_event_time = self.next_daylight_hourly_berth_check(arrival, point="B")
                            self.log_event(
                                arrival,
                                v.name,
                                "WAITING_BERTH_B",
                                f"Assigned to {selected_mother}; hourly reassessment until berth opens "
                                f"(earliest {self.hours_to_dt(start).strftime('%Y-%m-%d %H:%M')})",
                                voyage_num=v.current_voyage,
                                mother=selected_mother,
                            )
                        else:
                            v.status = "BERTHING_B"
                            _discharge_end = (
                                start + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + DISCHARGE_HOURS
                            )
                            if selected_mother == SANJULIAN_NAME:
                                self.sanjulian_berth_free_at = max(
                                    self.sanjulian_berth_free_at, _discharge_end
                                )
                            else:
                                self.mother_berth_free_at[selected_mother] = max(
                                    self.mother_berth_free_at[selected_mother], _discharge_end
                                )
                            v.next_event_time = start + BERTHING_DELAY_HOURS
                            self.log_event(start, v.name, "BERTHING_START_B",
                                           f"Berthing at {selected_mother} (30 min procedure)",
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
                                f"Day {day_key + 1} optimization assignment: "
                                f"{selected_mother} selected using 08:00 projected stock + earliest berth | "
                                f"already assigned today: {', '.join(sorted(assigned_today))}",
                                voyage_num=v.current_voyage,
                            )

                elif v.status == "WAITING_BERTH_B":
                    decision_t = t
                    _, candidates = self.point_b_candidate_slots(v, decision_t)
                    if not candidates:
                        next_recheck = self.next_daylight_hourly_berth_check(decision_t, point="B")
                        v.next_event_time = next_recheck
                        self.log_event(
                            decision_t,
                            v.name,
                            "WAITING_MOTHER_CAPACITY",
                            f"No Point B mother currently feasible; hourly reassessment at "
                            f"{self.hours_to_dt(next_recheck).strftime('%Y-%m-%d %H:%M')}",
                            voyage_num=v.current_voyage,
                        )
                        continue

                    day_key = int(decision_t // 24)
                    if STARTUP_DAY_DISABLE_POINT_B_PRIORITY and day_key == 0:
                        nominated_mother = STARTUP_DAY_POINT_B_MANUAL_NOMINATIONS.get(v.name)
                        if nominated_mother not in {m for _, _, m in candidates}:
                            next_recheck = self.next_daylight_hourly_berth_check(decision_t, point="B")
                            v.next_event_time = next_recheck
                            self.log_event(
                                decision_t,
                                v.name,
                                "WAITING_MOTHER_CAPACITY",
                                f"Startup-day manual nomination '{nominated_mother}' not feasible yet; reassess at "
                                f"{self.hours_to_dt(next_recheck).strftime('%Y-%m-%d %H:%M')}",
                                voyage_num=v.current_voyage,
                            )
                            continue
                        selected_mother = nominated_mother
                        selected = next((x for x in candidates if x[2] == selected_mother), None)
                    else:
                        selected_meta, _ = self.select_point_b_mother(
                            v,
                            decision_t,
                            day_key,
                            candidates,
                        )
                        selected_mother = selected_meta["mother"]
                        selected = (
                            selected_meta["start"],
                            selected_meta["berth_t"],
                            selected_meta["mother"],
                        )

                    start, berth_t, selected_mother = selected
                    if selected_mother != v.assigned_mother and v.assigned_mother in MOTHER_NAMES:
                        self.log_event(
                            decision_t,
                            v.name,
                            "MOTHER_PRIORITY_ASSIGNMENT",
                            f"Hourly Point B reassessment reallocated mother {v.assigned_mother} -> {selected_mother}",
                            voyage_num=v.current_voyage,
                            mother=selected_mother,
                        )
                    v.assigned_mother = selected_mother

                    if start > decision_t + 0.01:
                        next_recheck = self.next_daylight_hourly_berth_check(decision_t, point="B")
                        v.next_event_time = next_recheck
                        self.log_event(
                            decision_t,
                            v.name,
                            "WAITING_BERTH_B",
                            f"Awaiting berth at {selected_mother}; earliest {self.hours_to_dt(start).strftime('%Y-%m-%d %H:%M')}, "
                            f"next reassessment {self.hours_to_dt(next_recheck).strftime('%Y-%m-%d %H:%M')}",
                            voyage_num=v.current_voyage,
                            mother=selected_mother,
                        )
                        continue

                    v.status = "BERTHING_B"
                    _discharge_end = (
                        start + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + DISCHARGE_HOURS
                    )
                    if selected_mother == SANJULIAN_NAME:
                        self.sanjulian_berth_free_at = max(
                            self.sanjulian_berth_free_at, _discharge_end
                        )
                    else:
                        self.mother_berth_free_at[selected_mother] = max(
                            self.mother_berth_free_at[selected_mother], _discharge_end
                        )
                    v.next_event_time = start + BERTHING_DELAY_HOURS
                    self.log_event(
                        start,
                        v.name,
                        "BERTHING_START_B",
                        f"Berthing at {selected_mother} (30 min procedure)",
                        voyage_num=v.current_voyage,
                        mother=selected_mother,
                    )

                elif v.status == "BERTHING_B":
                    if v.assigned_mother not in MOTHER_NAMES and v.assigned_mother != SANJULIAN_NAME:
                        self.log_event(t, v.name, "WAITING_MOTHER_CAPACITY",
                                       "Blocked: no explicit mother assignment at Point B (fallback disabled)",
                                       voyage_num=v.current_voyage)
                        v.next_event_time = t + 0.5
                        continue
                    # SanJulian is always physically at Point B — it never leaves
                    if v.assigned_mother != SANJULIAN_NAME and not self.mother_is_at_point_b(v.assigned_mother, t):
                        _next_chk = self.next_daylight_hourly_berth_check(t, point="B")
                        v.status = "WAITING_BERTH_B"
                        v.next_event_time = _next_chk
                        self.log_event(
                            t,
                            v.name,
                            "WAITING_MOTHER_RETURN",
                            f"{v.assigned_mother} not at Point B; reassessing at {self.hours_to_dt(_next_chk).strftime('%Y-%m-%d %H:%M')}",
                            voyage_num=v.current_voyage,
                            mother=v.assigned_mother,
                        )
                        continue
                    v.status = "HOSE_CONNECT_B"
                    v.next_event_time = t + HOSE_CONNECTION_HOURS
                    selected_mother = v.assigned_mother
                    self.log_event(t, v.name, "HOSE_CONNECTION_START_B",
                                   f"Hose connection initiated at {selected_mother} (2 hours)",
                                   voyage_num=v.current_voyage)

                elif v.status == "HOSE_CONNECT_B":
                    if v.assigned_mother not in MOTHER_NAMES and v.assigned_mother != SANJULIAN_NAME:
                        self.log_event(t, v.name, "WAITING_MOTHER_CAPACITY",
                                       "Blocked: no explicit mother assignment at Point B (fallback disabled)",
                                       voyage_num=v.current_voyage)
                        v.next_event_time = t + 0.5
                        continue
                    selected_mother = v.assigned_mother
                    # SanJulian never goes on export — skip mother_is_at_point_b check
                    if selected_mother != SANJULIAN_NAME and not self.mother_is_at_point_b(selected_mother, t):
                        next_recheck = self.next_daylight_hourly_berth_check(t, point="B")
                        v.status = "WAITING_BERTH_B"
                        v.next_event_time = next_recheck
                        self.log_event(
                            t,
                            v.name,
                            "WAITING_MOTHER_RETURN",
                            f"{selected_mother} not at Point B; reassessing at {self.hours_to_dt(next_recheck).strftime('%Y-%m-%d %H:%M')}",
                            voyage_num=v.current_voyage,
                            mother=selected_mother,
                        )
                        continue

                    # Capacity check — uses SanJulian's own capacity when applicable
                    if selected_mother == SANJULIAN_NAME:
                        _recv_cap   = SANJULIAN_CAPACITY_BBL
                        _recv_stock = self.sanjulian_bbl
                    else:
                        _recv_cap   = MOTHER_CAPACITY_BBL
                        _recv_stock = self.mother_bbl[selected_mother]

                    if _recv_stock + v.cargo_bbl > _recv_cap:
                        self.log_event(t, v.name, "WAITING_MOTHER_CAPACITY",
                                       f"Cannot start discharge - {selected_mother} lacks space "
                                       f"({_recv_stock:,.0f}/{_recv_cap:,.0f} bbl)",
                                       voyage_num=v.current_voyage)
                        v.next_event_time = t + 6
                    else:
                        _vessel_api_val = self.vessel_api.get(v.name, 0.0)
                        if selected_mother == SANJULIAN_NAME:
                            # Credit cargo to SanJulian's intermediate inventory
                            self.sanjulian_api = self.blend_api(
                                self.sanjulian_bbl, self.sanjulian_api,
                                v.cargo_bbl, _vessel_api_val,
                            )
                            self.sanjulian_bbl           += v.cargo_bbl
                            self.sanjulian_total_received += v.cargo_bbl
                            self.sanjulian_berth_free_at  = max(
                                self.sanjulian_berth_free_at, t + DISCHARGE_HOURS
                            )
                            self.log_event(
                                t, v.name, "DISCHARGE_START",
                                f"Discharging {v.cargo_bbl:,} bbl @ {_vessel_api_val:.2f}° API "
                                f"-> SanJulian (intermediate storage): "
                                f"{self.sanjulian_bbl:,.0f}/{SANJULIAN_CAPACITY_BBL:,} bbl "
                                f"(blended {self.sanjulian_api:.2f}° API) — "
                                f"no primary mother available",
                                voyage_num=v.current_voyage,
                                mother=SANJULIAN_NAME,
                            )
                        else:
                            # Credit cargo to a primary mother vessel
                            self.mother_api[selected_mother] = self.blend_api(
                                self.mother_bbl[selected_mother], self.mother_api.get(selected_mother, 0.0),
                                v.cargo_bbl, _vessel_api_val,
                            )
                            self.mother_bbl[selected_mother] += v.cargo_bbl
                            self.mother_berth_free_at[selected_mother] = max(
                                self.mother_berth_free_at[selected_mother], t + DISCHARGE_HOURS
                            )
                            self.log_event(
                                t, v.name, "DISCHARGE_START",
                                f"Discharging {v.cargo_bbl:,} bbl @ {_vessel_api_val:.2f}° API | "
                                f"{selected_mother}: {self.mother_bbl[selected_mother]:,.0f} bbl "
                                f"(blended {self.mother_api[selected_mother]:.2f}° API)",
                                voyage_num=v.current_voyage,
                                mother=selected_mother,
                            )
                        v.status = "DISCHARGING"
                        v.next_event_time = t + DISCHARGE_HOURS

                elif v.status == "DISCHARGING":
                    if v.assigned_mother not in MOTHER_NAMES and v.assigned_mother != SANJULIAN_NAME:
                        self.log_event(t, v.name, "WAITING_MOTHER_CAPACITY",
                                       "Blocked: no explicit mother assignment at Point B (fallback disabled)",
                                       voyage_num=v.current_voyage)
                        v.next_event_time = t + 0.5
                        continue
                    selected_mother = v.assigned_mother
                    v.cargo_bbl = 0
                    self.vessel_api[v.name] = 0.0
                    # Correct receiver stock for the log message
                    _recv_stock_now = (self.sanjulian_bbl if selected_mother == SANJULIAN_NAME
                                       else self.mother_bbl[selected_mother])
                    # Enforce daylight-only cast-off at BIA
                    cast_off_b_t = self.next_cast_off_window(t)
                    wait_co_b = cast_off_b_t - t
                    v.status = "CAST_OFF_B"
                    v.next_event_time = cast_off_b_t + CAST_OFF_HOURS
                    self.log_event(t, v.name, "DISCHARGE_COMPLETE",
                                   f"{selected_mother}: {_recv_stock_now:,.0f} bbl | "
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
                    if v.assigned_mother not in MOTHER_NAMES and v.assigned_mother != SANJULIAN_NAME:
                        self.log_event(t, v.name, "WAITING_MOTHER_CAPACITY",
                                       "Blocked: no explicit mother assignment at Point B (fallback disabled)",
                                       voyage_num=v.current_voyage)
                        v.next_event_time = t + 0.5
                        continue
                    selected_mother = v.assigned_mother
                    # Only primary mothers trigger export readiness — SanJulian never exports
                    if selected_mother in MOTHER_NAMES:
                        if not self.export_ready[selected_mother]:
                            self.export_ready_since[selected_mother] = t
                        self.export_ready[selected_mother] = True
                    self.log_event(t, v.name, "CAST_OFF_COMPLETE_B",
                                   "Cast-off from mother complete; returning to storage",
                                   voyage_num=v.current_voyage)
                    v.status = "WAITING_RETURN_STOCK"
                    v.next_event_time = t

                elif v.status == "WAITING_RETURN_STOCK":
                    # SanJulian is a valid prior receiver; treat same as a mother for departure
                    selected_mother = (v.assigned_mother
                                       if v.assigned_mother in MOTHER_NAMES
                                          or v.assigned_mother == SANJULIAN_NAME
                                       else "UNASSIGNED")
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

                    # ── Position-aware D/E override for vessels departing BIA ─
                    # A vessel returning from Point B is always at area "B".
                    # If return_allocation_candidate picked D or E, verify that
                    # the storage will actually be in genuine need by ETA; if
                    # not, redirect to the best A/C storage instead so the vessel
                    # promotes back-to-back loading at high-production points.
                    _tgt_area = STORAGE_POINT.get(target_storage, "A")
                    _pf_san = "_pf_sanbarth_mode" in dir() and _pf_sanbarth_mode
                    if _tgt_area in ("D", "E") and not _pf_san:
                        _eta_de  = self.area_travel_hours("B", _tgt_area)
                        _proj_de = self.projected_stock_at(target_storage, _eta_de)
                        _crit_de = STORAGE_CRITICAL_THRESHOLD_BY_NAME[target_storage]
                        _de_ok   = _proj_de <= _crit_de + (
                            STORAGE_PRODUCTION_RATE_BY_NAME.get(target_storage, 0.0)
                            * SPREAD_DE_URGENCY_HORIZON
                        )
                        if not _de_ok:
                            # D/E not urgent — redirect to best A/C storage
                            _ac_cands = [
                                nm for nm in STORAGE_NAMES
                                if STORAGE_POINT.get(nm) in ("A", "C")
                                and self.storage_allowed_for_vessel(nm, v.name)
                            ]
                            if _ac_cands:
                                _ac_eligible = [
                                    nm for nm in _ac_cands
                                    if self.storage_bbl[nm] >= self.loading_start_threshold(
                                        nm, self.effective_load_cap(v.name, nm))
                                ]
                                _ac_pool = _ac_eligible if _ac_eligible else _ac_cands
                                def _ac_rank(nm):
                                    stk = self.storage_bbl[nm]
                                    crit = STORAGE_CRITICAL_THRESHOLD_BY_NAME[nm]
                                    unsafe = 0 if stk >= crit else 1
                                    raw_g = abs(stk - crit)
                                    eff_g = raw_g * (1.0 - self.production_rate_bias_factor(nm)) if raw_g <= DISPATCH_BIAS_FORECAST_BBL else raw_g
                                    return (unsafe, eff_g, -stk, nm)
                                _ac_best = min(_ac_pool, key=_ac_rank)
                                self.log_event(
                                    t, v.name, "RETURN_POINT_ALLOCATED",
                                    f"D/E spread suppressed (no urgency at {target_storage} by ETA "
                                    f"{_eta_de:.0f}h, proj {_proj_de:,.0f} bbl): "
                                    f"redirecting to {_ac_best} for back-to-back A/C loading",
                                    voyage_num=v.current_voyage,
                                )
                                target_storage = _ac_best
                                required_stock = self.loading_start_threshold(
                                    _ac_best, self.effective_load_cap(v.name, _ac_best))

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
                    v.assigned_storage = target_storage
                    # BIA -> Fairway Buoy (A/C return leg 1) is daylight-only,
                    # while other return routes keep tidal gating.
                    if v.target_point in ("A", "C"):
                        sail_t = self.next_daylight_sail(t)
                    else:
                        sail_t = self.next_tidal_sail(t)
                    wait   = sail_t - t
                    self.log_event(t, v.name, "RETURN_POINT_ALLOCATED",
                                   f"Allocated to Point {v.target_point} on departure from {selected_mother} | "
                                   f"Designated return storage: {target_storage} "
                                   f"({self.storage_bbl[target_storage]:,.0f} bbl, "
                                   f"loading-start threshold {required_stock:,.0f} bbl, "
                                   f"critical {STORAGE_CRITICAL_THRESHOLD_BY_NAME[target_storage]:,.0f} bbl)",
                                   voyage_num=v.current_voyage)
                    if wait > 0:
                        if v.target_point in ("A", "C"):
                            self.log_event(t, v.name, "WAITING_DAYLIGHT",
                                           f"Daylight window opens at "
                                           f"{self.hours_to_dt(sail_t).strftime('%Y-%m-%d %H:%M')}",
                                           voyage_num=v.current_voyage)
                        else:
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
                    self.assign_ac_point_post_breakwater(v, arrival)
                    self.trigger_ac_post_breakwater_reassessment(arrival, trigger_vessel=v.name)
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
                        and self.mother_export_departure_eligible(mother_name)
                        and t >= self.mother_available_at[mother_name]
                    ):
                        daughter_active_here = any(
                            vv.assigned_mother == mother_name
                            and vv.status in {"BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING"}
                            for vv in self.vessels
                        )
                        if daughter_active_here:
                            self.log_event(t, mother_name, "EXPORT_WAIT_DISCHARGE",
                                           "Export ready but waiting for active daughter berthing/discharge operations")
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
                        daughter_active_here = any(
                            vv.assigned_mother == mother_name
                            and vv.status in {"BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING"}
                            for vv in self.vessels
                        )
                        if daughter_active_here:
                            # Hold export departure while daughter berth/discharge
                            # operations are active on this mother at Point B.
                            self.export_end_time[mother_name] = t + TIME_STEP_HOURS
                            self.log_event(
                                t,
                                mother_name,
                                "EXPORT_WAIT_DISCHARGE",
                                "Export docs complete but waiting for active daughter berthing/discharge operations",
                            )
                            continue
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
                        export_complete_t = t
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
                        self.next_export_allowed_at = max(
                            self.next_export_allowed_at,
                            export_complete_t + EXPORT_SERIES_BUFFER_HOURS,
                        )
                        self.last_export_mother = mother_name
                        self.log_event(
                            self.next_export_allowed_at,
                            mother_name,
                            "EXPORT_SERIES_BUFFER_COMPLETE",
                            f"Mandatory post-export buffer complete ({EXPORT_SERIES_BUFFER_HOURS}h from export discharge completion) — next export sailing may begin",
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
                "SanJulian_bbl": round(self.sanjulian_bbl),
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
        self.final_sanjulian_bbl = self.sanjulian_bbl
        self.final_sanjulian_api = self.sanjulian_api
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

