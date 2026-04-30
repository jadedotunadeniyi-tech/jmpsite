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
SIM_VERSION = "5.18"

# -----------------------------------------------------------------
# VOYAGE CODE SYSTEM
# -----------------------------------------------------------------
# Each daughter vessel loading is assigned a short, unique reference code
# so operators can unambiguously identify and reassign a specific voyage's
# discharge target (e.g. "lock BGT-007 to Alkebulan").
#
# Format:  <3-LETTER-PREFIX>-<ZERO-PADDED-3-DIGIT-VOYAGE>
# Examples: SHK-001  LAP-003  BGT-007  WDK-002  STA-004
#
# The prefix is derived from the vessel name (first 3 chars, uppercased,
# with short aliases for longer names).
_VESSEL_CODE_PREFIX = {
    "Sherlock":    "SHK",
    "Laphroaig":  "LAP",
    "Rathbone":   "RTH",
    "SantaMonica":"STM",
    "Bedford":    "BDF",
    "Balham":     "BLH",
    "Woodstock":  "WDK",
    "Bagshot":    "BGT",
    "Watson":     "WTS",
    "Amyla":    "AMY",
    # ZeeZee (third-party)
    "ZeeZee":     "ZZE",
}

def make_voyage_code(vessel_name: str, voyage_num: int) -> str:
    """Return a short, unique voyage reference code for a loading event.

    Format: <PREFIX>-<NNN>  e.g. ``SHK-001``, ``BGT-007``.
    Custom vessels not in _VESSEL_CODE_PREFIX use the first three
    characters of their name, uppercased.
    """
    prefix = _VESSEL_CODE_PREFIX.get(
        vessel_name,
        vessel_name[:3].upper() if vessel_name else "UNK",
    )
    return f"{prefix}-{int(voyage_num):03d}"

# -----------------------------------------------------------------
# PRODUCTION API GRAVITY (degrees API per source)
# -----------------------------------------------------------------
STORAGE_API = {
    "Chapel"  : 29.00,
    "JasmineS": 43.36,
    "Westmore" : 31.10,
    "Duke"    : 41.20,
    "Starturn" : 39.54,
    "PGM"     : 36.00,   # Point G
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
            # Parse time — handle both 'H:MM' and 'HH:MM' formats
            t_parts = ts.split(":")
            hh, mm = int(t_parts[0]), int(t_parts[1][:2])
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

# =============================================================================
# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║                    OPERATIONS CONFIG TABLE                               ║
# ║  Edit values in this section only. The simulation below reads these      ║
# ║  constants by name — nothing else needs changing when you update them.   ║
# ║                                                                          ║
# ║  HOW TO ADD A NEW DAUGHTER VESSEL:                                       ║
# ║    1. Add her name to VESSEL_NAMES (sets dispatch order)                 ║
# ║    2. Add her cargo capacity to VESSEL_CAPACITIES (bbl)                  ║
# ║    3. Add her name to the relevant *_PERMITTED_VESSELS sets below        ║
# ║    4. If Point A only: add to POINT_A_ONLY_VESSELS                       ║
# ║    5. If Chapel slow-loader: add to CHAPEL_SLOW_LOADERS                  ║
# ║    6. If Point A load cap applies: add to POINT_A_LOAD_CAP_VESSELS       ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
# =============================================================================

SIMULATION_DAYS = 14   # How many days to simulate

# ── SECTION A: DAUGHTER VESSELS ──────────────────────────────────────────────
#  Dispatch order = top to bottom. Add a new vessel by inserting a new row.
#  Column 1: Name (str)   Column 2: Cargo capacity (bbl)
# ─────────────────────────────────────────────────────────────────────────────
#  Name            Capacity (bbl)   Notes
# ─────────────────────────────────────────────────────────────────────────────
_DAUGHTER_ROWS = [
    # name           capacity
    ( "Sherlock",    85_000 ),   # Point A — loads 1st in cycle
    ( "Laphroaig",   85_000 ),   # Point A — loads 2nd
    ( "Rathbone",    44_000 ),   # Point A/D/E — loads 3rd
    ( "SantaMonica", 28_000 ),   # Point A/D/E — loads 4th
    ( "Bedford",     85_000 ),   # Point A/B — loads 4th (Ibom)
    ( "Balham",      85_000 ),   # Point A/B — loads 5th (Ibom)
    ( "Woodstock",   42_000 ),   # Point A/E — loads 6th
    ( "Bagshot",     43_000 ),   # Point A/C/D — loads 7th
    ( "Watson",      85_000 ),   # Point A/C — loads 8th
    ( "Amyla",     63_000 ),   # Point A/C/F — loads 9th
]
# ─────────────────────────────────────────────────────────────────────────────
# Derived — do not edit these two lines
VESSEL_NAMES      = [row[0] for row in _DAUGHTER_ROWS]
VESSEL_CAPACITIES = {row[0]: row[1] for row in _DAUGHTER_ROWS}

DAUGHTER_CARGO_BBL = 85_000   # Default cargo for vessels not in VESSEL_CAPACITIES

# ── SECTION B: VESSEL LOADING PERMISSIONS ────────────────────────────────────
#  Add / remove vessel names from each set to control where they can load.
#  A vessel absent from a set is BLOCKED at that storage.
# ─────────────────────────────────────────────────────────────────────────────
WESTMORE_PERMITTED_VESSELS  = {"Sherlock", "Bagshot", "Rathbone", "Watson", "Laphroaig", "Amyla"}
DUKE_PERMITTED_VESSELS      = {"Woodstock", "Bagshot", "Rathbone", "SantaMonica"}
STARTURN_PERMITTED_VESSELS  = {"Woodstock", "Rathbone", "SantaMonica", "Bagshot"}
POINT_A_ONLY_VESSELS        = set()   # Amyla now permitted at Westmore (C) and Ibom (F)
CHAPEL_SLOW_LOADERS         = {"Woodstock", "Bagshot", "Rathbone", "SantaMonica"}  # reduced Chapel rate
POINT_A_LOAD_CAP_VESSELS    = {"Bedford", "Balham"}      # capped at POINT_A_LOAD_CAP_BBL at Point A

# Special per-vessel storage allowlists (overrides all set-based checks above)
# SantaMonica: Chapel (A), Duke (D), Starturn (E) only
STORAGE_PRIMARY_NAME   = "Chapel"
STORAGE_SECONDARY_NAME = "JasmineS"
STORAGE_TERTIARY_NAME  = "Westmore"
STORAGE_QUATERNARY_NAME = "Duke"
STORAGE_QUINARY_NAME   = "Starturn"
STORAGE_SENARY_NAME    = "PGM"          # Point G — SantaMonica only
PGM_PERMITTED_VESSELS  = {"SantaMonica"}  # only vessel allowed at Point G
SANTAMONICA_PERMITTED_STORAGES = (
    STORAGE_PRIMARY_NAME,       # Chapel   — Point A
    STORAGE_QUATERNARY_NAME,    # Duke     — Point D
    STORAGE_QUINARY_NAME,       # Starturn — Point E
    STORAGE_SENARY_NAME,        # PGM      — Point G
)
# Watson: Chapel (A), JasmineS (A), Westmore (C)
WATSON_PERMITTED_STORAGES = (
    STORAGE_PRIMARY_NAME,       # Chapel   — Point A
    STORAGE_SECONDARY_NAME,     # JasmineS — Point A
    STORAGE_TERTIARY_NAME,      # Westmore — Point C
)
# Laphroaig: JasmineS (A), Westmore (C)
LAPHROAIG_PERMITTED_STORAGES = (
    STORAGE_SECONDARY_NAME,     # JasmineS — Point A
    STORAGE_TERTIARY_NAME,      # Westmore — Point C
)
# Amyla: Chapel (A), JasmineS (A), Westmore (C), Ibom (F offshore buoy)
AMYLA_PERMITTED_STORAGES = (
    STORAGE_PRIMARY_NAME,       # Chapel   — Point A
    STORAGE_SECONDARY_NAME,     # JasmineS — Point A
    STORAGE_TERTIARY_NAME,      # Westmore — Point C
    "Ibom",                     # Point F  — offshore buoy
)

# ── SECTION C: PRODUCTION RATES (bbl/hr) ─────────────────────────────────────
#  Storage          Rate (bph)   Notes
# ─────────────────────────────────────────────────────────────────────────────
PRODUCTION_RATE_BPH          = 1_600   # Chapel & JasmineS (Point A)
WESTMORE_PRODUCTION_RATE_BPH =   960   # Westmore (Point C)
DUKE_PRODUCTION_RATE_BPH     =   250   # Duke (Point D)
STARTURN_PRODUCTION_RATE_BPH =    83   # Starturn (Point E)
PGM_PRODUCTION_RATE_BPH      =    40   # PGM (Point G) — SantaMonica only
POINT_F_LOAD_RATE_BPH        =   165   # Ibom offshore buoy (Point F) — also production rate

# ── SECTION D: STORAGE CAPACITIES (bbl) ──────────────────────────────────────
#  Storage          Capacity (bbl)   Notes
# ─────────────────────────────────────────────────────────────────────────────
STORAGE_CAPACITY_BBL          = 270_000   # Chapel & JasmineS (default)
DUKE_STORAGE_CAPACITY_BBL     =  90_000   # Duke
STARTURN_STORAGE_CAPACITY_BBL =  70_000   # Starturn
PGM_STORAGE_CAPACITY_BBL      =  40_000   # PGM (Point G) — SantaMonica only
# Westmore uses STORAGE_CAPACITY_BBL (270,000) — override here if needed:
# WESTMORE_STORAGE_CAPACITY_BBL = 270_000

# ── SECTION E: MOTHER VESSEL CAPACITIES (bbl) ────────────────────────────────
#  Mother           Capacity (bbl)   Notes
# ─────────────────────────────────────────────────────────────────────────────
MOTHER_CAPACITY_BBL  = 550_000   # Bryanston (default for primary mothers)
#                                  # Alkebulan and GreenEagle capacities set below
SANJULIAN_CAPACITY_BBL = 450_000  # SanJulian (intermediate floating storage)

# ── SECTION F: LOADING RATES (bbl/hr at each storage) ────────────────────────
#  Storage          Rate (bph)   Notes
# ─────────────────────────────────────────────────────────────────────────────
CHAPEL_LOAD_RATE_BPH      = 6_538   # Standard Chapel rate (85,000 bbl / 13 h)
CHAPEL_LOAD_RATE_SLOW_BPH = 5_000   # Reduced rate for CHAPEL_SLOW_LOADERS
JASMINES_LOAD_RATE_BPH    = 4_000   # JasmineS (85,000 bbl / 21.25 h)
WESTMORE_LOAD_RATE_BPH    = 2_000   # Westmore
DUKE_LOAD_RATE_BPH        = 3_500   # Duke
STARTURN_LOAD_RATE_BPH    = 2_500   # Starturn
PGM_LOAD_RATE_BPH         = 440   # PGM (Point G) — SantaMonica loads at 60% reduction (440 bph)

# ── SECTION G: DISCHARGE / EXPORT RATES (bbl/hr) ─────────────────────────────
#  Operation         Rate (bph)   Notes
# ─────────────────────────────────────────────────────────────────────────────
EXPORT_RATE_BPH            = 20_000   # Mother vessel export pump rate
SANJULIAN_TRANSLOAD_RATE_BPH = 10_000 # SanJulian → primary mother transfer rate

# Per-vessel discharge rates (bbl/hr) at Point B.
# Vessels absent from this dict use DISCHARGE_HOURS (fixed 12-hour default).
# When present, discharge duration = cargo_bbl / rate (dynamic).
VESSEL_DISCHARGE_RATE_BPH: dict = {
    "SantaMonica": 2_500,   # 2,500 bph → 28,000 bbl discharges in ~11.2 h
}

# ── SECTION H: POINT A LOAD CAP ──────────────────────────────────────────────
POINT_A_LOAD_CAP_BBL = 63_000   # Max load at Point A for POINT_A_LOAD_CAP_VESSELS

# =============================================================================
# ║  END OF CONFIG TABLE — do not edit below this line unless you know what  ║
# ║  you are doing. The simulation reads the constants above directly.        ║
# =============================================================================

# ── Internal: validation scenario flags (leave False unless testing) ──────────
POINT_B_DISTRIBUTION_TEST_MODE = False
POINT_B_DISTRIBUTION_TEST_DAYS = 3

# ── MULTIPLE TRANSIENT OPERATION (MTO) ───────────────────────────────────────
# When True: if >=2 shuttle vessels are stuck at Point B waiting (WAITING_BERTH_B
# or WAITING_MOTHER_CAPACITY) and cannot berth today (hard blockage or all berths
# occupied past daylight end), a mid-day (12:00) nomination fires once per day.
#
# The vessel with the most headroom in the MTO capacity table is nominated as
# transient storage. The smallest waiting shuttle transfers its full cargo into
# the transient (clamped to available headroom so the cap is never exceeded).
# The discharger is then freed to return and reload immediately.
#
# The transient vessel carries the accumulated cargo and discharges to a primary
# mother opportunistically — it checks for an available berth every hourly tick
# and takes the first window that opens, regardless of what day it is.
# It does NOT wait to reach its capacity limit before offloading.
#
# MTO_MAX_PARCELS_BEFORE_OFFLOAD controls only how many additional shuttle
# top-ups the transient may accept on subsequent congested days WHILE it is
# still waiting for a berth.  The capacity ceiling prevents overfilling.
#
# Set at runtime by run_sim() from the app toggle; default True so MTO is
# active from the first tick unless explicitly disabled by the operator.
MULTIPLE_TRANSIENT_OPERATION = True

# ── MTO TRANSIENT STORAGE CAPACITIES (bbl) ───────────────────────────────────
# Maximum volume a vessel may hold when acting as temporary storage at BIA.
# A discharger's transfer is clamped so the transient never exceeds this cap.
# Vessels absent from this dict use their normal cargo_capacity as the cap.
MTO_TRANSIENT_CAPACITY_BBL: dict = {
    "Balham":     125_000,
    "Bedford":    125_000,
    "Amyla":    125_000,
    "Bagshot":    125_000,
    "Laphroaig":  230_000,
    "Sherlock":   230_000,
    "Watson":     230_000,
    "Rathbone":    78_000,
    "SantaMonica": 35_000,
    "Woodstock":   95_000,
}

# ── MTO MULTI-PARCEL ACCUMULATION ────────────────────────────────────────────
# Controls how many additional shuttle cargoes the transient vessel may accept
# on subsequent congested days while it is still waiting for a mother berth.
# This is NOT a "fill before offload" target — the transient discharges
# opportunistically as soon as any mother berth opens, whatever its volume.
# Setting this higher lets the transient absorb more stranded cargoes on
# prolonged congested periods (e.g. mother away at export for 2+ days).
# The optimizer sweeps this parameter when MTO is enabled.
MTO_MAX_PARCELS_BEFORE_OFFLOAD: int = 1

# ── Internal: derived values (auto-computed from config table above) ──────────
NUM_DAUGHTERS             = len(VESSEL_NAMES)
MAX_DAUGHTER_CARGO        = max(VESSEL_CAPACITIES.values(), default=DAUGHTER_CARGO_BBL)
MIN_INCOMING_TRANSFER_BBL = min(VESSEL_CAPACITIES.values(), default=DAUGHTER_CARGO_BBL)

# ── Internal: initialisation defaults (overridden by app at runtime) ─────────
# Storage defaults: 80% of standard 270k Chapel/JasmineS capacity.
# Per-tank 80% values are applied in run_sim() using each tank's actual capacity.
STORAGE_INIT_BBL = 216_000   # 80% of 270,000 bbl (Chapel/JasmineS default)
# Mother defaults: Bryanston 450k, GreenEagle 300k, SanJulian 450k — set by run_sim()
MOTHER_INIT_BBL  = 0   # set individually by run_sim(); do not change here

# ── Internal: dead-stock and dispatch tuning ──────────────────────────────────
DEAD_STOCK_FACTOR         = 1.75   # vessel waits until 175% of cargo is available
DEAD_STOCK_MAX_WAIT_HOURS = 12.0   # abort dead-stock wait after this many hours
DUKE_STARTURN_DEAD_STOCK_BBL = 5_000
DUKE_MIN_REMAINING_BBL    = 5_000
STARTURN_MIN_REMAINING_BBL = 5_000
PGM_MIN_REMAINING_BBL      = 2_000   # PGM dead-stock reserve (small tank)

# ── Internal: load-cap multipliers (JasmineS oversizes, Westmore undersizes) ─
JASMINES_LOAD_CAP_MULTIPLIER  = 1.08
WESTMORE_LOAD_CAP_MULTIPLIER  = 0.82

# ── Internal: Point F (Ibom) tuning ──────────────────────────────────────────
POINT_F_SWAP_HOURS          = 2
POINT_F_MIN_TRIGGER_BBL     = 65_000
STARTURN_PRE_TANK_TOP_TRIGGER_RATIO = 0.90
DUKE_PRE_TANK_TOP_TRIGGER_RATIO     = 0.90
PRE_TANK_TOP_TRIGGER_RATIO_DEFAULT  = 0.90

# ── Internal: operational timing (hours) ─────────────────────────────────────
HOSE_CONNECTION_HOURS  = 2.0
LOAD_HOURS             = 12
DISCHARGE_HOURS        = 12
CAST_OFF_HOURS         = 0.2
BERTHING_DELAY_HOURS   = 0.5
POST_BERTHING_START_GAP_HOURS         = 0.5
POST_MOTHER_BERTHING_START_GAP_HOURS  = 1.0

# ── Internal: daylight / berthing windows ────────────────────────────────────
CAST_OFF_START   = 6
CAST_OFF_END     = 17.5
BERTHING_START   = 6
BERTHING_END     = 18
DAYLIGHT_START   = 6
DAYLIGHT_END     = 18

# ── Internal: export operation timing ────────────────────────────────────────
EXPORT_DOC_HOURS            = 2
EXPORT_SAIL_HOURS           = 6
EXPORT_SAIL_WINDOW_START    = 6
EXPORT_SAIL_WINDOW_END      = 15
EXPORT_HOSE_HOURS           = 4
EXPORT_SERIES_BUFFER_HOURS  = 48
MOTHER_EXPORT_VOLUME        = 400_000

# ── Internal: SanJulian optimisation parameters ───────────────────────────────
# SanJulian sits permanently at BIA. It receives daughter discharges like a
# primary mother but never sails to the export terminal. Instead it transloads
# to primary mothers via SANJULIAN_TRANSLOAD_RATE_BPH. Transloads trigger when:
#   T1  SanJulian reaches SANJULIAN_CAPACITY_BBL (force-drain)
#   T2  SanJulian >= a primary mother's remaining export-volume requirement
#   T3  A primary mother is idle at BIA and no daughter is arriving same day
#   T4  SanJulian > SANJULIAN_OPTIM_THRESHOLD_FRAC AND mother has headroom
SANJULIAN_OPTIM_THRESHOLD_FRAC  = 0.25    # T4: trigger above 25% of capacity
SANJULIAN_OPTIM_MIN_SPACE_BBL   = 50_000  # T4: mother must have this space free
SANJULIAN_FENDER_PREP_HOURS     = 1.5
SANJULIAN_DELAY_THRESHOLD_HOURS = 4.5
# Dynamic SanJulian daughter accumulation: when ≥ this many daughters are
# inbound/waiting at BIA, raise sanjulian_daughters_min_threshold by 1 per
# extra daughter above the base so SanJulian loads more before draining.
SANJULIAN_DYNAMIC_THRESHOLD_INBOUND = 3   # escalate when ≥3 daughters at BIA
# Export departure look-ahead: if ≥ this many daughters are inbound/waiting
# at BIA in the next EXPORT_LOOKFORWARD_HOURS, defer departure unless the
# mother is physically full (cannot accept another cargo).
EXPORT_DEFER_INBOUND_THRESHOLD  = 3       # defer if ≥3 daughters inbound
EXPORT_LOOKFORWARD_HOURS        = 36      # look 36 h ahead for inbound daughters

# ── Internal: route leg durations (hours) ────────────────────────────────────
# Point A/C ↔ BIA
SAIL_HOURS_A_TO_BW      = 1.5   # Point A/C → Breakwater
SAIL_HOURS_CROSS_BW_AC  = 0.5   # Cross Breakwater (daylight/tidal)
SAIL_HOURS_BW_TO_FWY    = 2.0   # Breakwater → Fairway Buoy
SAIL_HOURS_FWY_TO_B     = 2.0   # Fairway Buoy → BIA
SAIL_HOURS_B_TO_FWY     = 2.0   # BIA → Fairway Buoy
SAIL_HOURS_FWY_TO_BW    = 2.0   # Fairway Buoy → Breakwater
SAIL_HOURS_BW_TO_A      = 1.5   # Breakwater → Point A/C
SAIL_HOURS_A_TO_B = SAIL_HOURS_A_TO_BW + SAIL_HOURS_CROSS_BW_AC + SAIL_HOURS_BW_TO_FWY + SAIL_HOURS_FWY_TO_B
SAIL_HOURS_B_TO_A = SAIL_HOURS_B_TO_FWY + SAIL_HOURS_FWY_TO_BW + SAIL_HOURS_CROSS_BW_AC + SAIL_HOURS_BW_TO_A
SAIL_HOURS_B_TO_F       = 3     # BIA → Ibom
# Point D ↔ BIA
SAIL_HOURS_D_TO_CH      = 3.0   # Point D → Cawthorne Channel
SAIL_HOURS_CH_TO_BW_OUT = 1.0   # Cawthorne Channel → Breakwater
SAIL_HOURS_CROSS_BW     = 0.5   # Cross Breakwater
SAIL_HOURS_BW_TO_B      = 1.5   # Breakwater → BIA
SAIL_HOURS_B_TO_BW      = 1.5   # BIA → Breakwater
SAIL_HOURS_BW_TO_CH_IN  = 1.0   # Breakwater → Cawthorne Channel
SAIL_HOURS_CH_TO_D      = 3.0   # Cawthorne Channel → Point D
SAIL_HOURS_D_TO_CHANNEL = SAIL_HOURS_D_TO_CH
SAIL_HOURS_CHANNEL_TO_B = SAIL_HOURS_CH_TO_BW_OUT + SAIL_HOURS_CROSS_BW + SAIL_HOURS_BW_TO_B

# ── SECTION I: MOTHER VESSELS ────────────────────────────────────────────────
#  Each row: (Name, Capacity bbl).  SanJulian capacity set in Section E above.
#  Add a new mother by adding a row and updating MOTHER_CAPACITY_BY_NAME below.
# ─────────────────────────────────────────────────────────────────────────────
#  Name             Capacity (bbl)   Notes
# ─────────────────────────────────────────────────────────────────────────────
MOTHER_PRIMARY_NAME    = "Bryanston"
MOTHER_SECONDARY_NAME  = "GreenEagle"
MOTHER_TERTIARY_NAME   = "GreenEagle"   # kept for legacy references — same vessel
MOTHER_QUATERNARY_NAME = "SanJulian"   # intermediate — does not export

GREENEAGLE_CAPACITY_BBL      = 750_000
GREENEAGLE_EXPORT_TRIGGER_BBL = 680_000
# Bryanston uses MOTHER_CAPACITY_BBL (550,000) defined in Section E above.
# SanJulian uses SANJULIAN_CAPACITY_BBL (450,000) defined in Section E above.

# ── Internal: simulation time step ───────────────────────────────────────────
TIME_STEP_HOURS = 0.5


# ── SECTION J: THIRD-PARTY VESSEL — ZEEZEE ───────────────────────────────────
#  ZeeZee is an external tanker that discharges to a primary mother vessel at
#  Point B.  Her schedule is set by the Discharge Override Panel in the app.
#  The sim reads ZEEZEE_SCHEDULE at runtime (populated by run_sim each call).
#  Leave defaults here; the panel controls live values.
# ─────────────────────────────────────────────────────────────────────────────
#  ZEEZEE_SCHEDULE is a list of dicts, each representing one recurring visit:
#    {"day_of_month": int,   # 1-28 calendar day-of-month for arrival
#     "volume_bbl":   float, # cargo volume for that visit
#     "api":          float} # API gravity of cargo
#  Multiple entries allow different months to have different volumes/days.
#  Populated by the app via run_sim; never edited here directly.
ZEEZEE_SCHEDULE: list = []          # [{day_of_month, volume_bbl, api}, ...]
ZEEZEE_MAX_DAUGHTER_WAIT_HOURS = 48.0   # max delay caused by daughter queue

# Forced export departure schedule.
# Structure: {mother_name: [sim_hour_of_departure, ...]}
# Populated by run_sim from the operator's Force Export panel.
# When the run loop reaches a scheduled hour the named mother is forced
# into DOC state immediately (bypassing export_ready and eligibility tests).
# The mother sails, exports, and returns empty — exactly like a normal export.
EXPORT_FORCE_SCHEDULE: dict = {}   # {mother_name: [sim_hour, ...]}

def storage_adjusted_load_cap(base_cap, storage_name, vessel_name=None):
    """Return effective cargo loaded from a storage for a vessel.

    Storage-specific multipliers apply before any explicit operational caps.
    JasmineS loads 8% above the vessel's normal capacity.
    Westmore loads 18% below the vessel's normal capacity.
    PGM loads 60% below the vessel's normal capacity (SantaMonica only).
    Explicit Point A caps for Bedford/Balham still take precedence.
    """
    cap = int(round(base_cap))
    if storage_name == STORAGE_SECONDARY_NAME:
        cap = int(round(base_cap * JASMINES_LOAD_CAP_MULTIPLIER))
    elif storage_name == STORAGE_TERTIARY_NAME:
        cap = int(round(base_cap * WESTMORE_LOAD_CAP_MULTIPLIER))
    elif storage_name == STORAGE_SENARY_NAME:
        cap = int(round(base_cap * 0.40))   # 60% less than normal capacity
    if (vessel_name in POINT_A_LOAD_CAP_VESSELS
            and storage_name in {STORAGE_PRIMARY_NAME, STORAGE_SECONDARY_NAME}):
        cap = min(cap, POINT_A_LOAD_CAP_BBL)
    return max(0, cap)


def _default_vessel_base_capacity(vessel_name):
    return VESSEL_CAPACITIES.get(vessel_name, DAUGHTER_CARGO_BBL)


def _allowed_default_storages(vessel_name):
    if vessel_name in {"Bedford", "Balham"}:
        return [STORAGE_PRIMARY_NAME, STORAGE_SECONDARY_NAME, "Ibom"]
    if vessel_name == "Amyla":
        return list(AMYLA_PERMITTED_STORAGES)
    if vessel_name == "SantaMonica":
        return list(SANTAMONICA_PERMITTED_STORAGES)
    if vessel_name == "Watson":
        return list(WATSON_PERMITTED_STORAGES)
    if vessel_name == "Laphroaig":
        return list(LAPHROAIG_PERMITTED_STORAGES)
    if vessel_name in POINT_A_ONLY_VESSELS:
        return [STORAGE_PRIMARY_NAME, STORAGE_SECONDARY_NAME]
    return [
        STORAGE_PRIMARY_NAME,
        STORAGE_SECONDARY_NAME,
        *([STORAGE_TERTIARY_NAME] if vessel_name in WESTMORE_PERMITTED_VESSELS else []),
        *([STORAGE_QUATERNARY_NAME] if vessel_name in DUKE_PERMITTED_VESSELS else []),
        *([STORAGE_QUINARY_NAME] if vessel_name in STARTURN_PERMITTED_VESSELS else []),
        *([STORAGE_SENARY_NAME] if vessel_name in PGM_PERMITTED_VESSELS else []),
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
    STORAGE_SENARY_NAME,
]
STORAGE_POINT = {
    STORAGE_PRIMARY_NAME: "A",
    STORAGE_SECONDARY_NAME: "A",
    STORAGE_TERTIARY_NAME: "C",
    STORAGE_QUATERNARY_NAME: "D",
    STORAGE_QUINARY_NAME: "E",
    STORAGE_SENARY_NAME: "G",
}
# Per-vessel-day loading point overrides set by the JMP Override Panel in the app.
# Structure: {vessel_name: {day_number (int): storage_name (str)}}
# Applied before normal dispatch scoring so the forced assignment wins.
# Cleared and repopulated by run_sim on every call; never persists across runs.
STORAGE_DISPATCH_OVERRIDES: dict = {}

# Per-voyage discharge point overrides for daughter vessels.
#
# Structure (voyage-code keyed — preferred):
#   {voyage_code (str): {"vessel": str,
#                        "mother": str,
#                        "discharge_date": "YYYY-MM-DD"}}
#
# Legacy structure (vessel/day keyed — still supported):
#   {vessel_name (str): {day_key_0based (int): mother_name (str)}}
#
# Behaviour for voyage-code keyed entries:
#  - The override is matched against a vessel's current voyage_code.
#  - If the vessel arrives at BIA BEFORE discharge_date, she waits
#    (WAITING_BERTH_B) until that exact calendar date before berthing.
#  - When the date arrives the override takes priority and the mother
#    berth lock is reset, displacing any incumbent vessel to WAITING_BERTH_B.
#  - If no discharge_date is set the vessel berths at the earliest
#    opportunity on the first day she arrives at BIA.
#
# ZeeZee is unaffected -- controlled via ZEEZEE_SCHEDULE separately.
# Cleared and repopulated by run_sim on every call; never persists.
DAUGHTER_DISCHARGE_OVERRIDES: dict = {}

STORAGE_CAPACITY_BY_NAME = {name: STORAGE_CAPACITY_BBL for name in STORAGE_NAMES}
STORAGE_CAPACITY_BY_NAME[STORAGE_SECONDARY_NAME] = 290_000
STORAGE_CAPACITY_BY_NAME[STORAGE_TERTIARY_NAME] = 270_000
STORAGE_CAPACITY_BY_NAME[STORAGE_QUATERNARY_NAME] = DUKE_STORAGE_CAPACITY_BBL
STORAGE_CAPACITY_BY_NAME[STORAGE_QUINARY_NAME] = STARTURN_STORAGE_CAPACITY_BBL
STORAGE_CAPACITY_BY_NAME[STORAGE_SENARY_NAME] = PGM_STORAGE_CAPACITY_BBL
STORAGE_PRODUCTION_RATE_BY_NAME = {name: PRODUCTION_RATE_BPH for name in STORAGE_NAMES}
STORAGE_PRODUCTION_RATE_BY_NAME[STORAGE_TERTIARY_NAME] = WESTMORE_PRODUCTION_RATE_BPH
STORAGE_PRODUCTION_RATE_BY_NAME[STORAGE_QUATERNARY_NAME] = DUKE_PRODUCTION_RATE_BPH
STORAGE_PRODUCTION_RATE_BY_NAME[STORAGE_QUINARY_NAME] = STARTURN_PRODUCTION_RATE_BPH
STORAGE_PRODUCTION_RATE_BY_NAME[STORAGE_SENARY_NAME] = PGM_PRODUCTION_RATE_BPH

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
    ("G", "B"): 3.0,   # PGM direct (same corridor as Starturn)
    ("B", "G"): 3.0,
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
    ("G", "A"): 11.0,  # PGM → BIA → A (conservative)
    ("A", "G"): 11.0,
    ("G", "C"): 11.0,
    ("C", "G"): 11.0,
    ("G", "D"): 9.0,
    ("D", "G"): 9.0,
    ("G", "E"): 6.0,
    ("E", "G"): 6.0,
    ("G", "G"): 0.0,
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
    STORAGE_TERTIARY_NAME: 175_000,   # Unsafe when >175k (reduced from 225k)
    STORAGE_QUATERNARY_NAME: 90_000,
    STORAGE_QUINARY_NAME: 70_000,
    STORAGE_SENARY_NAME: 40_000,      # PGM — full capacity is the trigger (small tank)
}
MOTHER_NAMES = [MOTHER_PRIMARY_NAME, MOTHER_SECONDARY_NAME, MOTHER_QUATERNARY_NAME]
MOTHER_CAPACITY_BY_NAME = {
    MOTHER_PRIMARY_NAME:    MOTHER_CAPACITY_BBL,       # Bryanston  — Section E
    MOTHER_SECONDARY_NAME:  GREENEAGLE_CAPACITY_BBL,   # GreenEagle — Section I
    MOTHER_QUATERNARY_NAME: SANJULIAN_CAPACITY_BBL,    # SanJulian  — Section E
}
MOTHER_EXPORT_TRIGGER_BY_NAME = {
    MOTHER_PRIMARY_NAME:   MOTHER_EXPORT_TRIGGER,
    MOTHER_SECONDARY_NAME: GREENEAGLE_EXPORT_TRIGGER_BBL,
}

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
    "SAILING_BA"        : "Returning B -> selected loading point (A/C/D/E/G)",
    "WAITING_DAYLIGHT"  : "Waiting for Daylight Window",
    "WAITING_FAIRWAY"   : "Waiting at Fairway Buoy",
    "WAITING_MOTHER_CAPACITY" : "Waiting for space on mother vessel",
    "WAITING_MOTHER_RETURN" : "Waiting for mother to return from export",
    "WAITING_DEAD_STOCK"    : "Berthed — waiting for dead-stock threshold",
    "WAITING_RETURN_STOCK"  : "Waiting at Point B for return destination assignment",
    "SAILING_B_TO_F"        : "Sailing BIA -> Ibom (swap takeover)",
    "PF_LOADING"            : "Loading at Point F",
    "PF_SWAP"               : "Point F swap/takeover in progress",
    # SanJulian transload events — SanJulian behaves like a daughter vessel
    # when pumping to a primary mother (BERTHING_B → HOSE_CONNECT_B →
    # DISCHARGING → CAST_OFF_B), with matching log events on both sides.
    "SJ_TRANSLOAD_START"    : "SanJulian: starting transload cycle to primary mother",
    "SJ_TRANSLOAD_COMPLETE" : "SanJulian: transload cycle to primary mother complete",
    "SJ_TRANSLOAD_ABORT"    : "SanJulian: transload aborted — target left Point B",
    "SJ_FENDER_PREP"        : "SanJulian: fender preparation in progress",    # Mid-simulation mother unavailability window
    "MOTHER_UNAVAILABLE_START" : "Mother vessel entered scheduled unavailability window",
    "MOTHER_UNAVAILABLE_END"   : "Mother vessel exited scheduled unavailability window — resuming operations",
    # Multiple Transient Operation events
    "MTO_TRANSIENT_NOMINATED"      : "MTO: vessel nominated as temporary storage at Point B",
    "MTO_DISCHARGE_TO_TRANSIENT"   : "MTO: vessel discharging cargo to transient storage vessel",
    "MTO_TRANSFER_COMPLETE"        : "MTO: vessel-to-vessel cargo transfer complete",
    "MTO_TRANSIENT_PRIORITY_BERTH" : "MTO: transient storage vessel claiming priority berth at mother",
    "MTO_PARCEL_LIMIT_REACHED"     : "MTO: transient vessel reached max parcel count — forcing offload",
    "MTO_TRANSIENT_CAP_REACHED"    : "MTO: transient vessel at storage capacity — forcing offload",
    "MTO_ABORT_INSUFFICIENT_SPACE" : "MTO: regulatory abort — mother lacks space for full cargo; re-anchoring",
    "MTO_REANCHOR"                 : "MTO: transient vessel re-anchoring at BIA — awaiting qualifying mother",
}



class ThirdPartyVessel:
    """ZeeZee — third-party tanker arriving at Point B fully loaded once a month.

    Arrives at BIA on ZEEZEE_SCHEDULE day_of_month each calendar month.
    Discharges to the earliest available PRIMARY mother (never SanJulian).
    Priority rules:
      - If daughter-vessel queue is blocking all primary berths, ZeeZee waits
        up to ZEEZEE_MAX_DAUGHTER_WAIT_HOURS (48 h = 2 days) then forces a
        berth regardless.
      - If mothers are operationally absent (on export / offline / at capacity)
        ZeeZee waits without consuming her daughter-congestion clock.

    Status lifecycle:
      WAITING_B → BERTHING_B → HOSE_CONNECT_B → DISCHARGING → CAST_OFF_B → None
    """

    DISCHARGE_RATE_BPH = 20_000   # pump rate while discharging to mother

    def __init__(self, volume_bbl: float, api: float, arrival_t: float):
        self.name            = "ZeeZee"
        self.cargo_bbl       = float(volume_bbl)
        self.cargo_capacity  = float(volume_bbl)
        self.api             = float(api)
        self.status          = "WAITING_B"
        self.arrival_t       = arrival_t
        self.next_event_time = arrival_t
        self.assigned_mother = None
        self.current_voyage  = 1
        # Daughter-congestion clock — started when a mother exists but all
        # berths are held by daughters; reset when a genuine operational
        # constraint (no mother available) is responsible for the delay.
        self.daughter_block_since: float | None = None

    def __repr__(self):
        return f"ZeeZee[{self.status}|{self.cargo_bbl:,.0f}bbl]"


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
        # Per-vessel trip counter — incremented only when this vessel starts a
        # genuinely new loading voyage.  Replaces the old global voyage_counter
        # so that each vessel's codes are sequential (STM-001, STM-002, …)
        # regardless of what other vessels are doing simultaneously.
        self._vessel_voyage_counter: int = 0
        self.queue_position = None
        self.assigned_storage = None
        self.assigned_load_hours = None
        self.assigned_mother = None
        self.target_point = "A"
        # Short voyage reference code stamped at LOADING_START (e.g. "SHK-001")
        self.voyage_code: str = ""
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
        # ── JMP override lock ─────────────────────────────────────────────
        # Set True when a JMP loading-point override takes effect.
        # While True: daily preops and hourly reassessment are suppressed
        # so the override cannot be silently undone by the scoring engine.
        # Cleared when loading completes (LOADING_COMPLETE event).
        self._jmp_override_locked   = False
        # Date-shift: sim-hour on or after which loading may begin.
        # None = no date restriction; vessel loads as soon as berth is free.
        self._jmp_load_after_hour   = None
        # ── Multiple Transient Operation state ────────────────────────────
        # Set to the day_key (int) on which this vessel was nominated as
        # transient storage.  None when not an active MTO transient.
        # Cleared in the WAITING_BERTH_B handler after the priority berth
        # is claimed.
        self._mto_transient_since_day = None
        # Number of parcels (discharger transfers) received so far while
        # acting as transient storage.  Used with MTO_MAX_PARCELS_BEFORE_OFFLOAD
        # to decide when to stop accumulating and seek an offloading window.
        self._mto_parcels_received: int = 0
        # Flag set when this vessel transitions from transient storage to
        # actively offloading to a primary mother. Causes DISCHARGE_START
        # to stamp VoyageCode with an "A" suffix (e.g. AMY-000A) so the
        # MTO discharge is distinguishable from a normal cargo delivery.
        self._is_mto_offload: bool = False
        # Tracks the last WAITING_BERTH_B log state to suppress duplicate entries
        # when half-step scanning produces no change in assignment or slot time.
        self._wb_last_logged_start:  object = None
        self._wb_last_logged_mother: object = None
        # Mid-sim dormancy: vessel operates normally until this sim-hour,
        # then becomes dormant (IDLE_A) until resumption_hour.
        self.dormancy_start_hour: object = None

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
        # Startup seed: mothers away at export at t=0.  Keyed by mother name,
        # value is the sim-hour they are available again.  Consulted by
        # mother_is_at_point_b so the full export state machine is never
        # touched by the seed — export_state stays None and is set only by
        # the sim's own DOC→SAILING→HOSE→IN_PORT machinery.
        self.mother_seeded_away_until = {name: 0.0 for name in MOTHER_NAMES}
        # Mid-simulation unavailability windows — keyed by mother name, value is
        # a list of (start_h, end_h) sim-hour tuples.  mother_is_at_point_b()
        # returns False for any t inside a window.  Daughters and SanJulian
        # transloads are automatically rerouted to available mothers.
        self.mother_unavailability_windows: dict = {name: [] for name in MOTHER_NAMES}
        self.point_b_day_assigned_mothers = {}

        # ── SanJulian — intermediate floating storage ─────────────────────────
        # SanJulian NEVER joins the export_state / export_ready / export_sail
        # machinery.  When a transload trigger fires it behaves exactly like a
        # daughter vessel: it steps through BERTHING_B → HOSE_CONNECT_B →
        # DISCHARGING → CAST_OFF_B against the target primary mother, reserves
        # the mother's berth slot, and emits exactly the same log events so the
        # mother vessel indicator reflects the incoming transload.
        #
        # sanjulian_status : None | "BERTHING_B" | "HOSE_CONNECT_B" |
        #                    "DISCHARGING" | "CAST_OFF_B"
        # sanjulian_target : name of the primary mother being loaded
        # sanjulian_amount : bbl committed to this transload cycle
        # sanjulian_next_t : sim-hour when the current phase completes
        self.sanjulian_status           = None
        self.sanjulian_target           = None
        self.sanjulian_amount           = 0.0
        self.sanjulian_next_t           = None
        # sanjulian_fender_ready_t: sim-hour when fender preparation completes.
        # Set to t + SANJULIAN_FENDER_PREP_HOURS after every cast-off from a
        # primary mother.  Until this time: daughters may not berth SanJulian,
        # and SanJulian may not start a new transload cycle (fender prep before
        # berthing the next mother also uses this gate).
        self.sanjulian_fender_ready_t   = 0.0
        # sanjulian_daughters_loaded: number of daughter vessels that have
        # completed a full discharge to SanJulian since her last offload to a
        # primary mother.  SanJulian may not offload (T2/T3/T4) until this
        # reaches sanjulian_daughters_min_threshold.
        # Resets to 0 after each completed transload cycle.
        # T1 (near-capacity force-drain) bypasses this gate to prevent overflow.
        self.sanjulian_daughters_loaded        = 0
        # sanjulian_daughters_min_threshold: dynamically adjusted after startup
        # seeding.  Any vessels already in BERTHING_B/HOSE_CONNECT_B/DISCHARGING
        # at SanJulian from t=0 complete early and are counted by the normal
        # DISCHARGING handler — we raise the threshold by that count so those
        # pre-existing completions do NOT satisfy the requirement.  Only fresh
        # arrivals that occur during the sim run count toward the 2-daughter gate.
        self.sanjulian_daughters_min_threshold = 2
        # Legacy aliases kept for backward compat with app summary reads
        self.sanjulian_transload_state  = None   # set to dict when active
        self.sanjulian_transload_end_t  = None
        self.sanjulian_total_transloaded = 0.0
        # Remove SanJulian from export machinery — it will never export
        _sj = MOTHER_QUATERNARY_NAME
        self.export_ready[_sj]        = False
        self.export_ready_since[_sj]  = None
        self.export_state[_sj]        = None   # locked — never changes
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
        # ── Multiple Transient Operation tracking ────────────────────────────
        # Set of calendar day-keys (int(t//24)) on which MTO has already fired.
        # Ensures exactly one transient nomination per calendar day.
        self._mto_days_fired: dict = {}   # {day_key: fire_count} — max 2 per day

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

        # ── ZeeZee — third-party monthly visitor ─────────────────────────────
        # self.zeezee: None when absent; ThirdPartyVessel instance while active.
        # self.zeezee_months_visited: set of (year, month) already triggered so
        #   the monthly check fires exactly once per calendar month.
        self.zeezee: "ThirdPartyVessel | None" = None
        self.zeezee_months_visited: set = set()

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
        # ── Vessel startup positions ──────────────────────────────────────────
        # Default startup scenario:
        #   Sherlock, Laphroaig, Rathbone, SantaMonica, Bagshot, Watson, Berners
        #     → cargo_bbl = 0, status = SAILING_BA (Leg 1 — returning to SanBarth)
        #   Bedford  → PF_LOADING at Ibom (active loader)
        #   Balham   → BERTHING_B at GreenEagle (just arrived from Ibom)
        # Overridden at runtime when vessel_states_json or POINT_B_DISTRIBUTION_TEST_MODE
        # supplies explicit positions.

        _RETURNING_LEG1 = {
            "Sherlock", "Laphroaig", "Rathbone", "SantaMonica",
            "Bagshot",  "Watson",    "Amyla",
        }
        _seeded_startups = set()
        if POINT_B_DISTRIBUTION_TEST_MODE:
            _seeded_startups = set(POINT_B_TEST_STARTUP_FULL_LOAD_NOMINATIONS.keys())

        for vv in self.vessels:
            if vv.name == "Bedford":
                vv.status          = "PF_LOADING"
                vv.target_point    = "F"
                vv.cargo_bbl       = 30_000
                vv.next_event_time = 0.0
                vv._voyage_assigned = True
                vv.current_voyage   = 1
                vv._vessel_voyage_counter = 1
                vv.voyage_code      = make_voyage_code(vv.name, 1)
                self.vessel_api[vv.name] = IBOM_API

            elif vv.name == "Balham":
                vv.status           = "BERTHING_B"
                vv.target_point     = "B"
                vv.cargo_bbl        = 85_000
                vv.assigned_mother  = MOTHER_SECONDARY_NAME   # GreenEagle
                vv.next_event_time  = BERTHING_DELAY_HOURS
                vv._voyage_assigned = True
                vv.current_voyage   = 1
                vv._vessel_voyage_counter = 1
                vv.voyage_code      = make_voyage_code(vv.name, 1)
                self.vessel_api[vv.name] = IBOM_API

            elif vv.name in _RETURNING_LEG1 and vv.name not in _seeded_startups:
                # Returning to SanBarth — empty, on Leg 1 of the return voyage.
                # next_event_time set so the vessel arrives at Point A at
                # t = SAIL_HOURS_B_TO_A (6h), spread slightly to avoid a
                # simultaneous thundering-herd at the storage berths.
                _spread = list(sorted(_RETURNING_LEG1)).index(vv.name) * 0.5
                vv.status           = "SAILING_BA"
                vv.target_point     = "A"
                vv.cargo_bbl        = 0
                vv.next_event_time  = SAIL_HOURS_B_TO_A + _spread
                vv._voyage_assigned = False   # fresh voyage assigned on IDLE_A
                vv.current_voyage   = 0
                vv._vessel_voyage_counter = 0
                vv.voyage_code      = ""
                self.vessel_api[vv.name] = 0.0

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
                vv._vessel_voyage_counter = vv.current_voyage
                vv.voyage_code    = make_voyage_code(vv.name, vv.current_voyage)
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
                _disch_rate_init = VESSEL_DISCHARGE_RATE_BPH.get(vv.name)
                _disch_hrs_init = (vv.cargo_bbl / _disch_rate_init) if _disch_rate_init else DISCHARGE_HOURS
                _end = BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + _disch_hrs_init
            elif vv.status == "HOSE_CONNECT_B":
                _disch_rate_init = VESSEL_DISCHARGE_RATE_BPH.get(vv.name)
                _disch_hrs_init = (vv.cargo_bbl / _disch_rate_init) if _disch_rate_init else DISCHARGE_HOURS
                _end = HOSE_CONNECTION_HOURS + _disch_hrs_init
            elif vv.status == "DISCHARGING":
                _disch_rate_init = VESSEL_DISCHARGE_RATE_BPH.get(vv.name)
                _end = (vv.cargo_bbl / _disch_rate_init) if _disch_rate_init else DISCHARGE_HOURS
            else:
                continue
            self.mother_berth_free_at[mother_name] = max(self.mother_berth_free_at[mother_name], _end)
            initial_gate_end = max(initial_gate_end, _end)
        self.next_mother_berthing_start_at = initial_gate_end
        # ── Adjust SanJulian daughter-minimum threshold for pre-seeded vessels ──
        # Count vessels already in BERTHING_B / HOSE_CONNECT_B / DISCHARGING at
        # SanJulian from t=0.  These will complete early in the run and increment
        # sanjulian_daughters_loaded via the normal DISCHARGING handler.  We raise
        # the threshold by that count so that only freshly arriving daughters (those
        # that dock during this sim run) count toward the 2-vessel minimum.
        _sj_preseeded = sum(
            1 for vv in self.vessels
            if vv.assigned_mother == MOTHER_QUATERNARY_NAME
            and vv.status in {"BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING"}
        )
        self.sanjulian_daughters_min_threshold = 2 + _sj_preseeded
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

    def _resolve_discharge_override(self, vessel_name: str, voyage_code: str, t: float):
        """Return (mother, discharge_date_iso) for the active override, or (None, None).

        Lookup order:
        1. Voyage-code keyed entries in DAUGHTER_DISCHARGE_OVERRIDES
           (e.g. {"SHK-001": {"vessel": "Sherlock", "mother": "Bryanston",
                               "discharge_date": "2025-04-16"}})
        2. Legacy vessel/day entries (plain string or {"mother": ..., "date": ...})

        discharge_date_iso is the calendar date (YYYY-MM-DD) the vessel should
        berth at the mother.  None means "berth as soon as feasible".
        """
        ddo = DAUGHTER_DISCHARGE_OVERRIDES
        if not ddo:
            return None, None

        # 1. Voyage-code keyed (preferred)
        if voyage_code and voyage_code in ddo:
            entry = ddo[voyage_code]
            if isinstance(entry, dict):
                return entry.get("mother"), entry.get("discharge_date")

        # 2. Legacy vessel/day keyed
        vessel_overrides = ddo.get(vessel_name, {})
        if vessel_overrides:
            day_key = int(t // 24)
            current_cal_date = self.hours_to_dt(t).date().isoformat()
            for _dk, _entry in vessel_overrides.items():
                if isinstance(_entry, dict):
                    _date   = _entry.get("date")
                    _mother = _entry.get("mother")
                    if _date and _mother and _date == current_cal_date:
                        return _mother, _date
                else:
                    if int(_dk) == day_key:
                        return str(_entry), None

        return None, None

    def _discharge_override_date_reached(self, discharge_date_iso: str, t: float) -> bool:
        """True when the simulation clock is on or past the target discharge date."""
        if not discharge_date_iso:
            return True
        current_date = self.hours_to_dt(t).date().isoformat()
        return current_date >= discharge_date_iso

    def _displace_incumbent_at_mother(self, mother_name: str, t: float):
        """Force any vessel currently berthing/discharging at mother_name to
        WAITING_BERTH_B so the override vessel can take the slot immediately."""
        for vv in self.vessels:
            if vv.assigned_mother != mother_name:
                continue
            if vv.status in {"BERTHING_B", "HOSE_CONNECT_B"}:
                # Vessel has not yet started pumping — safe to displace
                vv.status = "WAITING_BERTH_B"
                vv.next_event_time = self.next_daylight_hourly_berth_check(t, point="B")
                self.log_event(
                    t, vv.name, "WAITING_BERTH_B",
                    f"Displaced from {mother_name} berth by override priority vessel; "
                    f"reassessing at {self.hours_to_dt(vv.next_event_time).strftime('%Y-%m-%d %H:%M')}",
                    voyage_num=vv.current_voyage, mother=mother_name,
                )
        # Reset the berth lock so the override vessel can claim it now
        self.mother_berth_free_at[mother_name] = t

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

    def mother_capacity_bbl(self, mother_name):
        return MOTHER_CAPACITY_BY_NAME.get(mother_name, MOTHER_CAPACITY_BBL)

    def mother_export_trigger_bbl(self, mother_name):
        return MOTHER_EXPORT_TRIGGER_BY_NAME.get(mother_name, MOTHER_EXPORT_TRIGGER)

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
            STORAGE_SENARY_NAME:    PGM_LOAD_RATE_BPH,
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
        if storage_name in (STORAGE_QUATERNARY_NAME, STORAGE_QUINARY_NAME, STORAGE_SENARY_NAME):
            # Duke/Starturn/PGM rule: load can commence once stock is at least
            # nominated cargo plus fixed 5,000 bbl dead-stock buffer.
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
        if vessel_name == "SantaMonica":
            return storage_name in SANTAMONICA_PERMITTED_STORAGES
        if vessel_name == "Watson":
            return storage_name in WATSON_PERMITTED_STORAGES
        if vessel_name == "Laphroaig":
            return storage_name in LAPHROAIG_PERMITTED_STORAGES
        if vessel_name == "Amyla":
            return storage_name in AMYLA_PERMITTED_STORAGES
        if vessel_name in POINT_A_ONLY_VESSELS and STORAGE_POINT.get(storage_name) != "A":
            return False
        if storage_name == STORAGE_TERTIARY_NAME and vessel_name not in WESTMORE_PERMITTED_VESSELS:
            return False
        if storage_name == STORAGE_QUATERNARY_NAME and vessel_name not in DUKE_PERMITTED_VESSELS:
            return False
        if storage_name == STORAGE_QUINARY_NAME and vessel_name not in STARTURN_PERMITTED_VESSELS:
            return False
        if storage_name == STORAGE_SENARY_NAME and vessel_name not in PGM_PERMITTED_VESSELS:
            return False
        return True

    def storage_min_remaining_after_load(self, storage_name):
        if storage_name == STORAGE_QUATERNARY_NAME:
            return DUKE_MIN_REMAINING_BBL
        if storage_name == STORAGE_QUINARY_NAME:
            return STARTURN_MIN_REMAINING_BBL
        if storage_name == STORAGE_SENARY_NAME:
            return PGM_MIN_REMAINING_BBL
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
            STORAGE_SENARY_NAME: PRE_TANK_TOP_TRIGGER_RATIO_DEFAULT,
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

        # Select the most urgent A/C storage for this vessel using the same
        # risk-rank scoring as return_allocation_candidate: storage closest to
        # its overflow critical threshold (production-rate biased) sorts first.
        # Using max(stock) here was wrong — it always sent vessels to whichever
        # storage happened to have more barrels rather than the one most at risk.
        def _ac_risk_rank(name):
            stock    = self.storage_bbl[name]
            crit     = STORAGE_CRITICAL_THRESHOLD_BY_NAME[name]
            unsafe   = 0 if stock >= crit else 1
            raw_gap  = abs(stock - crit)
            if raw_gap <= DISPATCH_BIAS_FORECAST_BBL:
                bias         = self.production_rate_bias_factor(name)
                effective_gap = raw_gap * (1.0 - bias)
            else:
                effective_gap = raw_gap
            return (unsafe, effective_gap, -stock, name)

        ready    = []
        fallback = []
        for name in ac_allowed:
            cap   = self.effective_load_cap(v.name, name)
            thr   = self.loading_start_threshold(name, cap)
            stock = self.storage_bbl[name]
            fallback.append((name, thr, stock))
            if stock >= thr:
                ready.append((name, thr, stock))

        pool = ready if ready else fallback
        selected_storage, selected_thr, selected_stock = min(pool, key=lambda x: _ac_risk_rank(x[0]))
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
        # Align to calendar midnight, not sim-epoch boundary.
        # Without this, hours 06:00-07:30 (sim hours -2 to -0.5 on day 0)
        # get floor-divided to day_key=-1 and are never scanned.
        cal_day  = int((hour + SIM_HOUR_OFFSET) // 24)
        day_start = cal_day * 24 - SIM_HOUR_OFFSET   # sim-hour at 00:00 of that calendar day
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
        """Return next berthing recheck time during daylight window.

        Vessels waiting for a berth at Point B scan every TIME_STEP_HOURS
        (0.5h) during daylight — the same cadence used by vessels scanning
        for available berths at loading points.  This ensures that the moment
        a mother's berth frees up (after a discharging vessel casts off), the
        waiting daughter is immediately reallocated to it rather than waiting
        up to an hour for the next check.
        """
        wall_h = (current_hour + SIM_HOUR_OFFSET) % 24
        if BERTHING_START <= wall_h < BERTHING_END:
            # Scan every half-step during daylight
            nxt = round(current_hour + TIME_STEP_HOURS, 2)
            wall_next = (nxt + SIM_HOUR_OFFSET) % 24
            if BERTHING_START <= wall_next < BERTHING_END:
                return nxt
        # Outside daylight — jump to next daylight window start
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
        """True when another vessel is physically occupying the berth at storage.
        Blocks a new vessel from berthing until the incumbent has completed loading,
        finished documentation AND physically cast off. A vessel in DOCUMENTING or
        CAST_OFF status is still alongside — the berth is not free until SAILING_AB
        (or SAILING_D_CHANNEL for Point D) begins."""
        lock_statuses = {
            "BERTHING_A",      # arriving and securing
            "HOSE_CONNECT_A",  # connecting cargo hoses
            "LOADING",         # actively loading cargo
            "DOCUMENTING",     # cargo complete, paperwork in progress — still alongside
            "CAST_OFF",        # casting off lines — still physically at berth
        }
        for vv in self.vessels:
            if vv.name == requesting_vessel:
                continue
            if vv.assigned_storage != storage_name:
                continue
            if vv.status in lock_statuses:
                return True
        return False

    def next_berthing_window(self, current_hour, point=None):
        """Return earliest sim-hour >= current_hour within the daylight berthing window.

        No cast-off conflict check.  Cast-offs at one berth do not block berthing
        at a different mother (they are independent physical locations).  Approach
        overlap is handled by BERTHING_DELAY_HOURS.  The previous cast-off loop
        added up to 14 x 24h whenever ANY vessel was in CAST_OFF_B anywhere at
        Point B, stacking all future berth calculations by weeks and freezing the
        simulation completely.  That code is removed.
        """
        wall_h = (current_hour + SIM_HOUR_OFFSET) % 24
        if BERTHING_START <= wall_h < BERTHING_END:
            return current_hour
        days_elapsed = int(current_hour // 24)
        sim_bs_today = days_elapsed * 24 + (BERTHING_START - SIM_HOUR_OFFSET)
        if current_hour <= sim_bs_today:
            return sim_bs_today
        return sim_bs_today + 24

    def point_b_candidate_slots(self, v, at_time):
        """Build feasible Point B mother slots for vessel v at decision time.

        Priority rule:
          - Primary mothers (Bryanston, GreenEagle) are preferred when they can
            berth within SANJULIAN_DELAY_THRESHOLD_HOURS of SanJulian.
          - SanJulian is included as a candidate when she can berth the vessel
            strictly earlier than the best available primary mother slot.
          - EXCEPTION: MTO transient vessels (_is_mto_offload=True) must NEVER
            be assigned SanJulian. All consolidated MTO cargo must discharge to
            a primary mother (Bryanston or GreenEagle) only.
        """
        berthing_start = self.next_berthing_window(at_time, point="B")
        sj_candidate = None
        primary_candidates = []
        # MTO transient vessels must only discharge to primary mothers
        _vessel_is_mto = getattr(v, "_is_mto_offload", False)
        # Look-ahead: also include primary mothers returning from export within 24h
        # so vessels waiting at BIA reassign from SanJulian to an incoming primary.
        _lookahead_t = at_time + 24.0

        for mother_name in MOTHER_NAMES:
            # MTO transient vessels: SanJulian is never a valid target
            if _vessel_is_mto and mother_name == MOTHER_QUATERNARY_NAME:
                continue
            _at_bia_now  = self.mother_is_at_point_b(mother_name, at_time)
            # For primary mothers only, include those that will return within 24h
            _at_bia_soon = (
                not _at_bia_now
                and mother_name != MOTHER_QUATERNARY_NAME
                and self.mother_is_at_point_b(mother_name, _lookahead_t)
            )
            if not _at_bia_now and not _at_bia_soon:
                continue
            _cap = self.mother_capacity_bbl(mother_name)
            if self.mother_bbl[mother_name] + v.cargo_bbl > _cap:
                continue
            earliest = max(berthing_start, self.mother_available_at.get(mother_name, 0.0))
            berth_t = self.next_berthing_window(earliest, point="B")
            # Clamp berth_free_at to at_time — a past timestamp means "free now"
            _effective_free = max(self.mother_berth_free_at[mother_name], 0.0)
            start = max(
                berth_t,
                _effective_free if _effective_free > at_time else 0.0,
                self.mother_available_at.get(mother_name, 0.0),
            )
            start = self.next_berthing_window(start, point="B")
            entry = (start, berth_t, mother_name)
            if mother_name == MOTHER_QUATERNARY_NAME:
                sj_candidate = entry
            else:
                primary_candidates.append(entry)

        # Determine the earliest a primary mother can berth this vessel
        if primary_candidates:
            earliest_primary_start = min(s for s, _, _ in primary_candidates)
        else:
            earliest_primary_start = None

        # Include SanJulian only when she can berth no later than the best primary,
        # i.e. when all primaries are delayed and SanJulian is free sooner.
        # Also include her when no primary is available at all (last-resort fallback).
        # PRESSURE OVERRIDE: when ≥ SANJULIAN_DYNAMIC_THRESHOLD_INBOUND daughters
        # are already inbound/waiting, also offer SanJulian even if a primary could
        # take the vessel — this distributes load and reduces queue time.
        # Hard gate: SanJulian cannot receive a daughter while she is still
        # berthed at a primary mother (BERTHING_B / HOSE_CONNECT_B / DISCHARGING
        # / CAST_OFF_B). She must have fully cast off first.
        sj_occupied = self.sanjulian_status in {
            "BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING", "CAST_OFF_B"
        }
        # Also block daughters during post-cast-off fender preparation
        sj_fender_busy = at_time < self.sanjulian_fender_ready_t
        candidates = list(primary_candidates)
        if sj_candidate is not None and not sj_occupied and not sj_fender_busy:
            sj_start = sj_candidate[0]
            _pressure_high = (self._daughters_inbound_to_bia()
                              >= SANJULIAN_DYNAMIC_THRESHOLD_INBOUND)
            if (earliest_primary_start is None
                    or sj_start < earliest_primary_start - 1e-6
                    or _pressure_high):
                candidates.append(sj_candidate)

        return berthing_start, candidates

    def mother_is_at_point_b(self, mother_name, t):
        """True when the mother is physically available at Point B.
        SanJulian does not go on export voyages, but she CAN be marked
        unavailable via a scheduled maintenance / dry-dock window.
        """
        # Startup seed: mother was seeded as away at export until a specific hour
        if t < self.mother_seeded_away_until.get(mother_name, 0.0):
            return False
        # Mid-simulation scheduled unavailability windows (applies to all mothers,
        # including SanJulian for dry-dock / maintenance periods)
        for _start_h, _end_h in self.mother_unavailability_windows.get(mother_name, []):
            if _start_h <= t < _end_h:
                return False
        if mother_name == MOTHER_QUATERNARY_NAME:
            # SanJulian has no export state machine — just return True once
            # any scheduled unavailability window has been checked above.
            return True
        # Check if mother is currently unavailable (at export or in transit)
        # If mother_available_at is in the future, the mother is either at export
        # or returning from export. We need to distinguish between these cases.
        _available_at = self.mother_available_at.get(mother_name, 0.0)
        if t < _available_at:
            # Mother is not yet available - check if it's because she's at export
            # or because she's returning from export
            _export_state = self.export_state.get(mother_name)
            if _export_state in {"SAILING", "HOSE", "IN_PORT"}:
                # Mother is at export terminal - not available
                return False
            # Mother is in return transit (export complete but not yet arrived at BIA)
            # Consider her "at Point B" for candidate selection so daughters can
            # be assigned to her immediately upon arrival. The berth lock will
            # prevent actual berthing until fendering is complete.
            return True
        # Mother is available at Point B
        return True

    def mother_export_departure_eligible(self, mother_name):
        """Export may depart when the trigger is reached, unless daughter traffic
        at BIA is high enough that staying to absorb more cargo is beneficial.

        Departure rules (in priority order):
          1. MUST sail: mother is physically full (cannot take another daughter cargo)
          2. MUST sail: mother has been export_ready for ≥ EXPORT_SERIES_BUFFER_HOURS
             (prevents indefinite deferral)
          3. DEFER: ≥ EXPORT_DEFER_INBOUND_THRESHOLD daughters are inbound/waiting
             at BIA AND mother still has meaningful capacity remaining — stay and load
             more cargo before sailing (better utilisation of the voyage)
          4. SAIL: trigger reached and daughter pressure is low — depart normally
        """
        stock = self.mother_bbl[mother_name]
        cap   = self.mother_capacity_bbl(mother_name)
        remaining_capacity = max(0.0, cap - stock)

        # Rule 1 — physically full: must sail immediately
        cannot_accommodate_next = remaining_capacity < MIN_INCOMING_TRANSFER_BBL
        if cannot_accommodate_next:
            return True

        # Must have reached the export trigger to be a departure candidate at all
        reached_target = stock >= self.mother_export_trigger_bbl(mother_name)
        if not reached_target:
            return False

        # Rule 2 — safety valve: if export_ready has been set for too long, sail
        # regardless of daughter traffic (prevents indefinite deferral).
        ready_since = self.export_ready_since.get(mother_name)
        if ready_since is not None:
            time_ready = getattr(self, '_current_t', 0) - ready_since
            if time_ready >= EXPORT_SERIES_BUFFER_HOURS * 0.75:
                return True

        # Rule 3 — daughter look-ahead deferral
        # If ≥ EXPORT_DEFER_INBOUND_THRESHOLD daughters are inbound/at BIA AND
        # this mother can still absorb at least one more daughter cargo, defer
        # departure so she loads up further before sailing.
        _n_inbound = self._daughters_inbound_to_bia()
        if (_n_inbound >= EXPORT_DEFER_INBOUND_THRESHOLD
                and remaining_capacity >= MIN_INCOMING_TRANSFER_BBL):
            return False

        # Rule 4 — normal departure: trigger reached, daughter pressure is low
        return True

    # ── SanJulian transload machinery ─────────────────────────────────────────

    def _sj_primary_mothers(self):
        """Return list of primary mother names (everything except SanJulian)."""
        return [n for n in MOTHER_NAMES if n != MOTHER_QUATERNARY_NAME]

    def _sj_best_transload_target(self, t, require_full_space=True):
        """Return (mother_name, amount) for the best transload target.

        When require_full_space=True (default, regulatory compliance):
          Only considers mothers that have space >= the ENTIRE SanJulian volume.
          This enforces the single-berthing-operation rule — no split discharges.
          SanJulian will wait at anchor until a mother can receive her full contents.

        When require_full_space=False:
          Falls back to partial-fit selection (used only for internal calculations,
          never for initiating an actual transload cycle).
        """
        _sj = MOTHER_QUATERNARY_NAME
        sj_vol = self.mother_bbl[_sj]
        if sj_vol <= 0:
            return None, 0

        candidates = []
        for mn in self._sj_primary_mothers():
            if not self.mother_is_at_point_b(mn, t):
                continue
            if self.export_state.get(mn) == "DOC":
                continue
            space = self.mother_capacity_bbl(mn) - self.mother_bbl[mn]
            if space <= 0:
                continue
            if require_full_space and space < sj_vol:
                continue   # regulatory: must fit entire volume in one operation
            # How far from export trigger (negative = already exceeded, very close = urgent)
            to_trigger = max(0.0, MOTHER_EXPORT_VOLUME - self.mother_bbl[mn])
            # Prefer idle mothers with no arrival expected
            idle_no_arrival = not any(
                vv.assigned_mother == mn and
                vv.status in {"BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING",
                              "SAILING_AB_LEG2", "WAITING_FAIRWAY",
                              "WAITING_BERTH_B", "WAITING_MOTHER_RETURN"}
                for vv in self.vessels
            )
            amount = min(sj_vol, space)
            candidates.append({
                "mother": mn, "amount": amount, "space": space,
                "to_trigger": to_trigger, "idle_no_arrival": idle_no_arrival,
            })

        if not candidates:
            return None, 0

        candidates.sort(key=lambda c: (
            0 if c["idle_no_arrival"] else 1,
            c["to_trigger"],
            -c["space"],
        ))
        best = candidates[0]
        return best["mother"], best["amount"]

    def _daughters_inbound_to_bia(self):
        """Count daughter vessels currently at BIA or inbound within the next
        EXPORT_LOOKFORWARD_HOURS hours (waiting, sailing final legs, or berthing).
        Used for SanJulian dynamic threshold and export departure look-ahead.
        """
        _at_or_inbound = {
            "SAILING_AB_LEG2", "WAITING_FAIRWAY", "SAILING_BW_TO_FWY",
            "SAILING_CROSS_BW_AC", "SAILING_AB",
            "WAITING_BERTH_B", "BERTHING_B", "HOSE_CONNECT_B",
            "DISCHARGING", "CAST_OFF_B", "WAITING_CAST_OFF",
            "WAITING_MOTHER_CAPACITY", "WAITING_MOTHER_RETURN",
            "WAITING_RETURN_STOCK",
        }
        return sum(1 for vv in self.vessels if vv.status in _at_or_inbound)

    def _sj_transload_trigger_check(self, t):
        """Return (trigger_reason_str, target_mother, amount) if a transload should
        start, otherwise return (None, None, 0).

        Priority rules:
          T1  SanJulian at ≥90% capacity — force drain immediately
          T2  SanJulian holds enough to complete a primary mother's export gap
          T3  A primary mother is idle (no daughter active or arriving) AND
              SanJulian holds any volume — trigger regardless of stock size.
              This fires even if SanJulian only holds a small amount.
          T4  Optimisation: SanJulian above 25% and a mother has ≥50k free space

        Hard gate: SanJulian cannot start a transload while any daughter vessel
        is still berthing, connecting, discharging, or casting off at SanJulian.
        She must wait until her own berth is fully clear first.
        """
        sj_vol = self.mother_bbl[MOTHER_QUATERNARY_NAME]
        sj_cap = SANJULIAN_CAPACITY_BBL

        # Never transload if SanJulian is empty
        if sj_vol <= 0:
            return None, None, 0

        # Never start a new cycle while one is active
        if self.sanjulian_status is not None:
            return None, None, 0

        # Never start a new cycle during post-cast-off fender preparation
        if t < self.sanjulian_fender_ready_t:
            return None, None, 0

        # ── Hard gate: wait until no daughter is berthing/discharging to SanJulian ──
        daughter_on_sanjulian = any(
            vv.assigned_mother == MOTHER_QUATERNARY_NAME and
            vv.status in {"BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING", "CAST_OFF_B"}
            for vv in self.vessels
        )
        if daughter_on_sanjulian:
            return None, None, 0

        # ── Dynamic daughter-threshold escalation ────────────────────────────
        # When ≥ SANJULIAN_DYNAMIC_THRESHOLD_INBOUND daughters are inbound or
        # waiting at BIA, raise the minimum-load threshold by 1 per additional
        # daughter beyond the base (capped at sanjulian_daughters_min_threshold + 3)
        # so SanJulian accumulates more cargo before draining.  This reduces the
        # frequency of SJ→primary transload cycles during high-traffic periods,
        # keeping primary mother berths free for daughter discharges.
        # The threshold is NEVER reduced below the hard 2-daughter minimum.
        _n_inbound = self._daughters_inbound_to_bia()
        _dyn_extra = max(0, _n_inbound - SANJULIAN_DYNAMIC_THRESHOLD_INBOUND)
        _dyn_extra = min(_dyn_extra, 3)   # cap escalation at +3
        _effective_min_threshold = self.sanjulian_daughters_min_threshold + _dyn_extra

        _min_daughters_met = (
            self.sanjulian_daughters_loaded >= _effective_min_threshold
        )

        # T1 — SanJulian at ≥75% capacity: force drain immediately (no gate).
        # Only starts if a target has space for the ENTIRE SanJulian volume
        # (single-berth rule).  If no mother qualifies, SanJulian waits at anchor.
        if sj_vol >= sj_cap * 0.75:
            target, amount = self._sj_best_transload_target(t, require_full_space=True)
            if target:
                return "T1-NEAR_CAPACITY", target, amount

        # T1b — SanJulian holding ≥50% but gate not yet met AND daughters still
        # inbound: drain now so SJ remains available as a buffer for new arrivals.
        # Requires full space on target (no partial discharge).
        if sj_vol >= sj_cap * 0.50 and not _min_daughters_met:
            for mn in self._sj_primary_mothers():
                if not self.mother_is_at_point_b(mn, t):
                    continue
                if self.export_state.get(mn) == "DOC":
                    continue
                space = self.mother_capacity_bbl(mn) - self.mother_bbl[mn]
                if space >= sj_vol:   # must fit entire SJ volume
                    return "T1b-HALF_CAPACITY_DRAIN", mn, sj_vol

        # ── T6 here (BEFORE daughter gate) ───────────────────────────────────
        # T6 — Post-export-return priority drain: a primary mother just returned
        # from export with very large free space (≥ 60% capacity).  This is the
        # highest-urgency optimisation opportunity: drain SanJulian into the
        # freshly emptied mother immediately, regardless of daughter count.
        # Must fire BEFORE the daughter-gate check so it cannot be blocked when
        # SanJulian is holding meaningful cargo and a primary is near-empty.
        for mn in self._sj_primary_mothers():
            if not self.mother_is_at_point_b(mn, t):
                continue
            if self.export_state.get(mn) in {"DOC", "SAILING"}:
                continue
            cap_mn = self.mother_capacity_bbl(mn)
            space  = cap_mn - self.mother_bbl[mn]
            if space >= cap_mn * 0.60 and space >= sj_vol:
                return "T6-POST_EXPORT_RETURN", mn, sj_vol

        # T2/T3/T4/T5 require minimum daughters discharged (dynamic threshold)
        if not _min_daughters_met:
            return None, None, 0

        # ── Eligibility helpers ───────────────────────────────────────────────
        # Strict: used by T2/T3 — skips mothers flagged export_ready or DOC
        # (don't top up a mother that is committed to sailing imminently)
        def _sj_eligible_strict(mn):
            if not self.mother_is_at_point_b(mn, t):
                return False   # away at export or seeded as away
            if self.export_state.get(mn) == "DOC":
                return False   # export documentation in progress — about to sail
            if self.export_ready.get(mn):
                return False   # hit export trigger — departure imminent
            return True

        # Relaxed: used by T4/T5 — accepts export_ready mothers.
        # A mother with export_ready=True is STILL physically at BIA and can
        # receive more cargo before she departs; those barrels ship on the
        # next export voyage.  Blocking T4/T5 on export_ready was the primary
        # cause of SanJulian holding volume for many days (all primaries cycling
        # through export_ready simultaneously leaves no eligible target).
        def _sj_eligible_relaxed(mn):
            if not self.mother_is_at_point_b(mn, t):
                return False   # physically away — cannot receive
            if self.export_state.get(mn) == "DOC":
                return False   # export docs started — sailing imminent (hours)
            return True

        # T2 — SanJulian holds enough to complete a primary mother's export gap
        # Requires the target has space for the entire SanJulian volume (no partial).
        for mn in self._sj_primary_mothers():
            if not _sj_eligible_strict(mn):
                continue
            space = self.mother_capacity_bbl(mn) - self.mother_bbl[mn]
            if space < sj_vol:
                continue   # cannot receive entire SJ volume — skip
            export_gap = max(0.0, MOTHER_EXPORT_VOLUME - self.mother_bbl[mn])
            if export_gap > 0 and sj_vol >= export_gap:
                return "T2-EXPORT_COMPLETION", mn, sj_vol

        # T3 — A primary mother is idle with no daughter active or en-route to it.
        #       SanJulian transloads regardless of stock volume (even small amounts).
        for mn in self._sj_primary_mothers():
            if not _sj_eligible_strict(mn):
                continue
            space = self.mother_capacity_bbl(mn) - self.mother_bbl[mn]
            if space < sj_vol:
                continue   # cannot receive entire SJ volume — no partial discharge
            # Mother is idle: no daughter currently berthing/discharging
            daughter_active = any(
                vv.assigned_mother == mn and
                vv.status in {"BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING"}
                for vv in self.vessels
            )
            if daughter_active:
                continue
            # No daughter is en-route or waiting to berth at this mother
            daughter_arriving = any(
                vv.assigned_mother == mn and
                vv.status in {
                    "SAILING_AB_LEG2", "WAITING_FAIRWAY", "SAILING_BW_TO_FWY",
                    "WAITING_BERTH_B", "WAITING_MOTHER_RETURN", "WAITING_MOTHER_CAPACITY",
                }
                for vv in self.vessels
            )
            if not daughter_arriving:
                # Mother is fully idle and no daughter is coming — transload entire volume
                return "T3-IDLE_NO_ARRIVAL", mn, sj_vol

        # T4 — Optimisation: any primary mother has space ≥ full SanJulian volume.
        # Uses relaxed eligibility (export_ready mothers included).
        best_mother, best_space = None, 0
        for mn in self._sj_primary_mothers():
            if not _sj_eligible_relaxed(mn):
                continue
            space = self.mother_capacity_bbl(mn) - self.mother_bbl[mn]
            if space < sj_vol:
                continue   # must fit entire SJ volume
            if space > best_space:
                best_space = space
                best_mother = mn
        if best_mother and best_space >= SANJULIAN_OPTIM_MIN_SPACE_BBL:
            return "T4-OPTIMISATION", best_mother, sj_vol

        # T5 — Eager drain: gate met and SJ holds meaningful volume.
        # Uses relaxed eligibility. Still requires full-space on target.
        if sj_vol >= SANJULIAN_OPTIM_MIN_SPACE_BBL:
            best_t5_mother, best_t5_space = None, 0
            for mn in self._sj_primary_mothers():
                if not _sj_eligible_relaxed(mn):
                    continue
                space = self.mother_capacity_bbl(mn) - self.mother_bbl[mn]
                if space < sj_vol:
                    continue   # must fit entire SJ volume
                if space > best_t5_space:
                    best_t5_space  = space
                    best_t5_mother = mn
            if best_t5_mother:
                return "T5-EAGER_DRAIN", best_t5_mother, sj_vol

        return None, None, 0

    # ------------------------------------------------------------------
    # MULTIPLE TRANSIENT OPERATION (MTO)
    # ------------------------------------------------------------------
    def _maybe_run_multiple_transient_op(self, t):
        """Fire at 08:00 AND at the first tick >=12:00 each day (max once/day).

        Why two windows:
          08:00 — matches the morning position report. If >=2 vessels are
                  already confirmed idle at BIA (hard-wait), act immediately
                  so the discharger can cast off and catch morning tide.
          12:00 — midday fallback for situations not yet clear at 08:00
                  (e.g. vessels that arrived at BIA mid-morning).

        Gate 3 (cannot berth today) explicitly accounts for SanJulian's active
        transload, which ties up a primary mother berth for 40+ hours at
        10,000 bph — mother_berth_free_at alone understates the blockage.

        Transient capacity:
          MTO_TRANSIENT_CAPACITY_BBL is a ceiling, not a loading target.
          Transfer is clamped to headroom; transient discharges opportunistically
          every hourly tick as soon as any mother berth window opens.

        MTO_MAX_PARCELS_BEFORE_OFFLOAD:
          Gates only further top-ups while the transient awaits a berth.
          Never delays the transient's offload.
        """
        if not MULTIPLE_TRANSIENT_OPERATION:
            return

        # ── Gate 1: 08:00 or first tick >=12:00, up to 2 per calendar day ───────
        wall_hour = (t + SIM_HOUR_OFFSET) % 24
        day_key   = int((t + SIM_HOUR_OFFSET) // 24)
        _is_morning       = abs(wall_hour - SIM_HOUR_OFFSET) < TIME_STEP_HOURS * 0.6
        _is_noon_or_later = wall_hour >= 12.0
        if not (_is_morning or _is_noon_or_later):
            return
        # Allow up to 2 MTO nominations per calendar day
        _day_fires = self._mto_days_fired.get(day_key, 0)
        if _day_fires >= 2:
            return
        # Morning window: fire only if not already fired today
        if _is_morning and not _is_noon_or_later and _day_fires >= 1:
            return

        # ── Gate 2: >=1 shuttle vessel stranded at Point B ───────────────────
        # Also check SanJulian — if SanJulian is available AND has space, vessels
        # should be routed to her instead of triggering MTO.
        _hard_wait = {"WAITING_BERTH_B", "WAITING_MOTHER_CAPACITY"}
        _soft_wait = {"WAITING_FAIRWAY", "SAILING_AB_LEG2"}
        _all_wait  = _hard_wait | _soft_wait
        waiters = [
            vv for vv in self.vessels
            if vv.status in _all_wait
            and vv.cargo_bbl > 0
            and vv.name not in {"Bedford", "Balham"}
        ]
        if len(waiters) < 1:
            return
        # At 08:00 only fire if >=1 is already confirmed idle (hard-wait)
        _hard_waiters = [vv for vv in waiters if vv.status in _hard_wait]
        if _is_morning and not _is_noon_or_later and len(_hard_waiters) < 1:
            return

        # ── Gate 3: vessels genuinely cannot berth today ──────────────────────
        # Check ALL mothers including SanJulian.  If SanJulian has space AND
        # her berth is free before daylight end, vessels should be routed to
        # her — MTO should not fire while SanJulian remains an idle option.
        # Also account for SanJulian's active transload tying up a primary berth.
        _min_cargo = min(vv.cargo_bbl for vv in waiters)

        _sj_blocks_mother = None
        if self.sanjulian_status in {"BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING"}:
            _sj_blocks_mother = self.sanjulian_target

        # Check all mothers including SanJulian for space
        receiver_available = False
        for mn in MOTHER_NAMES:
            if not self.mother_is_at_point_b(mn, t):
                continue
            if self.mother_capacity_bbl(mn) - self.mother_bbl[mn] >= _min_cargo:
                receiver_available = True
                break

        _daylight_end_t = (int((t + SIM_HOUR_OFFSET) // 24) * 24
                           + DAYLIGHT_END - SIM_HOUR_OFFSET)
        berth_free_before_daylight = False
        if receiver_available:
            for mn in MOTHER_NAMES:
                if not self.mother_is_at_point_b(mn, t):
                    continue
                if self.mother_capacity_bbl(mn) - self.mother_bbl[mn] < _min_cargo:
                    continue
                berth_free_at = self.mother_berth_free_at.get(mn, 0.0)
                # SanJulian's active transload blocks the target primary mother
                if mn == _sj_blocks_mother and self.sanjulian_next_t is not None:
                    berth_free_at = max(berth_free_at, self.sanjulian_next_t)
                # SanJulian's OWN berth is blocked while she is actively pumping
                if mn == MOTHER_QUATERNARY_NAME and self.sanjulian_status in {
                    "BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING"
                }:
                    if self.sanjulian_next_t is not None:
                        berth_free_at = max(berth_free_at, self.sanjulian_next_t)
                if berth_free_at <= _daylight_end_t:
                    berth_free_before_daylight = True
                    break

        if receiver_available and berth_free_before_daylight:
            return

        # ── All gates passed — record this fire ──────────────────────────────
        self._mto_days_fired[day_key] = self._mto_days_fired.get(day_key, 0) + 1

        # ── Check for an existing active transient to top up ──────────────────
        existing_transient = next(
            (vv for vv in waiters
             if getattr(vv, "_mto_transient_since_day", None) is not None),
            None
        )

        if existing_transient is not None:
            _parcels_so_far = getattr(existing_transient, "_mto_parcels_received", 0)
            _trn_cap  = MTO_TRANSIENT_CAPACITY_BBL.get(
                existing_transient.name, existing_transient.cargo_capacity)
            _headroom = max(0.0, _trn_cap - existing_transient.cargo_bbl)

            if _parcels_so_far >= MTO_MAX_PARCELS_BEFORE_OFFLOAD or _headroom <= 0:
                self.log_event(
                    t, existing_transient.name, "MTO_PARCEL_LIMIT_REACHED",
                    f"[MTO Day {day_key+1}] No further top-ups — "
                    f"{'parcel limit reached' if _parcels_so_far >= MTO_MAX_PARCELS_BEFORE_OFFLOAD else 'at capacity'} "
                    f"({existing_transient.cargo_bbl:,.0f}/{_trn_cap:,.0f} bbl) | "
                    f"awaiting opportunistic mother berth",
                    voyage_num=existing_transient.current_voyage,
                )
                return

            remaining    = [vv for vv in waiters if vv is not existing_transient]
            if not remaining:
                return
            discharger_v = min(remaining, key=lambda vv: vv.cargo_bbl)
            transient_v  = existing_transient
            transfer_bbl = min(discharger_v.cargo_bbl, _headroom)
        else:
            # ── Nominate a new transient ──────────────────────────────────────
            # Score: strongly prefer hard-wait (confirmed idle at BIA) over
            # soft-wait (still en-route); among equals pick most MTO headroom.
            def _nom_score(vv):
                cap    = MTO_TRANSIENT_CAPACITY_BBL.get(vv.name, vv.cargo_capacity)
                hdroom = max(0.0, cap - vv.cargo_bbl)
                hard_bonus = 1_000_000 if vv.status in _hard_wait else 0
                return hdroom + hard_bonus

            waiters_scored = sorted(waiters, key=_nom_score, reverse=True)
            transient_v   = waiters_scored[0]
            _trn_cap      = MTO_TRANSIENT_CAPACITY_BBL.get(
                                transient_v.name, transient_v.cargo_capacity)
            _headroom     = max(0.0, _trn_cap - transient_v.cargo_bbl)

            if _headroom <= 0:
                return

            # Discharger: prefer vessels whose full cargo fits in headroom
            # (clean transfer — discharger leaves completely empty).
            remaining = [vv for vv in waiters if vv is not transient_v]
            if not remaining:
                # Only one waiter — cannot do a vessel-to-vessel transfer;
                # MTO requires at least two vessels (one transient, one discharger).
                return
            _fits     = [vv for vv in remaining if vv.cargo_bbl <= _headroom]
            discharger_v = min(
                _fits if _fits else remaining,
                key=lambda vv: vv.cargo_bbl,
            )
            transfer_bbl = min(discharger_v.cargo_bbl, _headroom)

            transient_v._mto_transient_since_day = day_key
            transient_v._mto_parcels_received    = 0

        if transfer_bbl <= 0:
            return

        # ── Execute vessel-to-vessel transfer ─────────────────────────────────
        _disch_rate    = VESSEL_DISCHARGE_RATE_BPH.get(discharger_v.name)
        transfer_hours = (transfer_bbl / _disch_rate) if _disch_rate else DISCHARGE_HOURS
        transfer_end_t = t + transfer_hours

        # Blend API from discharger into transient
        _dis_api = self.vessel_api.get(discharger_v.name, 0.0)
        _trn_api = self.vessel_api.get(transient_v.name, 0.0)
        _trn_vol = transient_v.cargo_bbl
        _new_trn = _trn_vol + transfer_bbl
        if _new_trn > 0:
            self.vessel_api[transient_v.name] = (
                (_trn_vol * _trn_api + transfer_bbl * _dis_api) / _new_trn
            )
        transient_v.cargo_bbl = _new_trn
        transient_v._mto_parcels_received = getattr(
            transient_v, "_mto_parcels_received", 0) + 1

        # Discharger: empty fully and return to load
        discharger_v.cargo_bbl = 0
        self.vessel_api[discharger_v.name] = 0.0
        cast_off_t = self.next_cast_off_window(transfer_end_t)
        discharger_v.status          = "CAST_OFF_B"
        discharger_v.next_event_time = cast_off_t + CAST_OFF_HOURS

        # Transient: remains WAITING_BERTH_B — priority offload fires on every
        # hourly tick via the WAITING_BERTH_B handler the moment any mother
        # berth with sufficient space opens.
        transient_v.status          = "WAITING_BERTH_B"
        transient_v.next_event_time = self.next_daylight_hourly_berth_check(t, point="B")

        # ── Log ───────────────────────────────────────────────────────────────
        _parcel_num = transient_v._mto_parcels_received
        _cap_label  = MTO_TRANSIENT_CAPACITY_BBL.get(
                          transient_v.name, transient_v.cargo_capacity)
        _hdroom_left = max(0.0, _cap_label - transient_v.cargo_bbl)
        self.log_event(
            t, transient_v.name, "MTO_TRANSIENT_NOMINATED",
            f"[MTO Day {day_key+1} — Parcel {_parcel_num}/{MTO_MAX_PARCELS_BEFORE_OFFLOAD}] "
            f"Received {transfer_bbl:,.0f} bbl from {discharger_v.name} "
            f"@ {_dis_api:.2f}° API | on-board: {transient_v.cargo_bbl:,.0f} bbl "
            f"(cap {_cap_label:,.0f} bbl, {_hdroom_left:,.0f} bbl headroom remaining) | "
            f"discharging opportunistically when mother berth opens",
            voyage_num=transient_v.current_voyage,
        )
        self.log_event(
            t, discharger_v.name, "MTO_DISCHARGE_TO_TRANSIENT",
            f"[MTO Day {day_key+1}] Transferred {transfer_bbl:,.0f} bbl to "
            f"{transient_v.name} ({transfer_hours:.1f}h) | "
            f"freed — returning to reload | cast-off "
            f"{self.hours_to_dt(cast_off_t).strftime('%Y-%m-%d %H:%M')}",
            voyage_num=discharger_v.current_voyage,
        )
        if transfer_hours > 0:
            self.log_event(
                transfer_end_t, discharger_v.name, "MTO_TRANSFER_COMPLETE",
                f"Transfer complete | {transient_v.name}: {transient_v.cargo_bbl:,.0f} bbl on board",
                voyage_num=discharger_v.current_voyage,
            )
    def _run_zeezee(self, t):
        """Monthly arrival trigger + full discharge state machine for ZeeZee.

        Called every timestep from run() BEFORE the daughter vessel loop so
        ZeeZee gets priority in the same tick she is processed.

        Two-clock priority model
        ────────────────────────
        Operational constraint  — mothers away / at capacity / offline.
          ZeeZee waits indefinitely; daughter_block_since is NOT advanced.

        Daughter congestion  — a feasible mother exists but her berth is held
          by a queued daughter.  daughter_block_since starts (or continues).
          After ZEEZEE_MAX_DAUGHTER_WAIT_HOURS the berth is forcibly cleared
          and ZeeZee proceeds immediately.
        """
        # ── Step A: monthly arrival trigger ───────────────────────────────────
        if ZEEZEE_SCHEDULE and self.zeezee is None:
            _zz_wall = (t + SIM_HOUR_OFFSET) % 24
            # Only trigger at the 08:00 wall-clock tick.
            # SIM_HOUR_OFFSET=8 means t=0 is 08:00, so _zz_wall==8.0 at every 08:00.
            if abs(_zz_wall - SIM_HOUR_OFFSET) < TIME_STEP_HOURS * 0.5:
                _cal = self.hours_to_dt(t)
                _ym  = (_cal.year, _cal.month)
                if _ym not in self.zeezee_months_visited:
                    # Find the schedule entry whose day_of_month matches today
                    for _entry in ZEEZEE_SCHEDULE:
                        if _cal.day == _entry.get("day_of_month", 0):
                            self.zeezee_months_visited.add(_ym)
                            _vol = float(_entry.get("volume_bbl", 200_000))
                            _api = float(_entry.get("api", 32.0))
                            self.zeezee = ThirdPartyVessel(
                                volume_bbl=_vol, api=_api, arrival_t=t)
                            self.log_event(
                                t, "ZeeZee", "VESSEL_JOINED",
                                f"ZeeZee arrived at Point B — {_vol:,.0f} bbl "
                                f"@ {_api}° API — awaiting discharge berth",
                            )
                            break

        # ── Step B: state machine ─────────────────────────────────────────────
        _zz = self.zeezee
        if _zz is None or t < _zz.next_event_time:
            return

        if _zz.status == "WAITING_B":
            # Find earliest-available PRIMARY mother (never SanJulian)
            _best_start  = None
            _best_mother = None
            _bwin        = self.next_berthing_window(t, point="B")
            for _mn in MOTHER_NAMES:
                if _mn == MOTHER_QUATERNARY_NAME:
                    continue                       # skip SanJulian
                if not self.mother_is_at_point_b(_mn, t):
                    continue                       # operationally absent
                _mcap = self.mother_capacity_bbl(_mn)
                if self.mother_bbl[_mn] + _zz.cargo_bbl > _mcap:
                    continue                       # no space
                _earliest = max(_bwin,
                                self.mother_berth_free_at[_mn],
                                self.mother_available_at[_mn])
                _slot = self.next_berthing_window(_earliest, point="B")
                if _best_start is None or _slot < _best_start:
                    _best_start  = _slot
                    _best_mother = _mn

            if _best_mother is None:
                # ── No primary operationally available ────────────────────────
                # True constraint (not daughters).  Reset congestion clock.
                _zz.daughter_block_since = None
                _next = self.next_daylight_hourly_berth_check(t, point="B")
                _zz.next_event_time = _next
                self.log_event(t, "ZeeZee", "WAITING_MOTHER_CAPACITY",
                               "No primary mother available (operational constraint); "
                               f"reassessing at "
                               f"{self.hours_to_dt(_next).strftime('%Y-%m-%d %H:%M')}")
                return

            # A feasible mother exists — check if daughters are blocking her
            _berth_blocked_by_daughter = any(
                v.assigned_mother == _best_mother
                and v.status in {"BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING"}
                for v in self.vessels
            )

            if _best_start > t + TIME_STEP_HOURS * 0.5 and _berth_blocked_by_daughter:
                # ── Daughter-congestion wait ──────────────────────────────────
                if _zz.daughter_block_since is None:
                    _zz.daughter_block_since = t
                    self.log_event(
                        t, "ZeeZee", "WAITING_BERTH_B",
                        f"Berth at {_best_mother} held by daughter vessel; "
                        f"2-day deadline starts — "
                        f"force-berth at "
                        f"{self.hours_to_dt(t + ZEEZEE_MAX_DAUGHTER_WAIT_HOURS).strftime('%Y-%m-%d %H:%M')}",
                    )
                _waited = t - _zz.daughter_block_since
                if _waited >= ZEEZEE_MAX_DAUGHTER_WAIT_HOURS:
                    # ── 2-day deadline exceeded: force berth ──────────────────
                    self.mother_berth_free_at[_best_mother] = t
                    _best_start = self.next_berthing_window(t, point="B")
                    self.log_event(
                        t, "ZeeZee", "ZEEZEE_DEADLINE_OVERRIDE",
                        f"2-day daughter queue exceeded ({_waited:.1f} h); "
                        f"forcing berth at {_best_mother}",
                    )
                    _zz.daughter_block_since = None
                    # Fall through to BERTHING_B below
                else:
                    _next = self.next_daylight_hourly_berth_check(t, point="B")
                    _zz.next_event_time = _next
                    return

            # ── Berth secured: proceed to BERTHING_B ─────────────────────────
            _zz.daughter_block_since = None
            _zz.assigned_mother = _best_mother
            _discharge_hrs = _zz.cargo_bbl / ThirdPartyVessel.DISCHARGE_RATE_BPH
            _discharge_end = (_best_start + BERTHING_DELAY_HOURS
                              + HOSE_CONNECTION_HOURS + _discharge_hrs)
            self.mother_berth_free_at[_best_mother] = max(
                self.mother_berth_free_at[_best_mother], _discharge_end)
            _zz.status = "BERTHING_B"
            _zz.next_event_time = _best_start + BERTHING_DELAY_HOURS
            self.log_event(
                _best_start, "ZeeZee", "BERTHING_START_B",
                f"ZeeZee berthing at {_best_mother} "
                f"(priority discharge — {BERTHING_DELAY_HOURS*60:.0f} min procedure)",
                mother=_best_mother,
            )

        elif _zz.status == "BERTHING_B":
            _mn = _zz.assigned_mother
            if not self.mother_is_at_point_b(_mn, t):
                # Mother departed — requeue
                _zz.status = "WAITING_B"
                _zz.assigned_mother = None
                _zz.next_event_time = self.next_daylight_hourly_berth_check(t, point="B")
                self.log_event(t, "ZeeZee", "WAITING_MOTHER_RETURN",
                               f"{_mn} departed during ZeeZee berthing; requeueing")
                return
            _zz.status = "HOSE_CONNECT_B"
            _zz.next_event_time = t + HOSE_CONNECTION_HOURS
            self.log_event(t, "ZeeZee", "HOSE_CONNECTION_START_B",
                           f"Hose connection at {_mn} ({HOSE_CONNECTION_HOURS:.0f} h)",
                           mother=_mn)

        elif _zz.status == "HOSE_CONNECT_B":
            _mn = _zz.assigned_mother
            _mcap = self.mother_capacity_bbl(_mn)
            if self.mother_bbl[_mn] + _zz.cargo_bbl > _mcap:
                # Capacity issue — wait 6 h and retry
                _zz.next_event_time = t + 6
                self.log_event(t, "ZeeZee", "WAITING_MOTHER_CAPACITY",
                               f"{_mn} lacks space; rechecking in 6 h")
                return
            # Blend API and credit mother
            self.mother_api[_mn] = self.blend_api(
                self.mother_bbl[_mn], self.mother_api.get(_mn, 0.0),
                _zz.cargo_bbl, _zz.api,
            )
            self.mother_bbl[_mn] += _zz.cargo_bbl
            self.total_loaded    += _zz.cargo_bbl
            _discharge_hrs = _zz.cargo_bbl / ThirdPartyVessel.DISCHARGE_RATE_BPH
            _zz.status = "DISCHARGING"
            self.mother_berth_free_at[_mn] = max(
                self.mother_berth_free_at[_mn], t + _discharge_hrs)
            _zz.next_event_time = t + _discharge_hrs
            self.log_event(
                t, "ZeeZee", "DISCHARGE_START",
                f"Discharging {_zz.cargo_bbl:,.0f} bbl "
                f"@ {_zz.api:.2f}° API | "
                f"{_mn}: {self.mother_bbl[_mn]:,.0f} bbl "
                f"(blended {self.mother_api.get(_mn, 0.0):.2f}° API)",
                mother=_mn,
            )

        elif _zz.status == "DISCHARGING":
            _mn = _zz.assigned_mother
            _zz.status = "CAST_OFF_B"
            _zz.next_event_time = t + CAST_OFF_HOURS
            self.log_event(t, "ZeeZee", "DISCHARGE_COMPLETE",
                           f"{_mn}: {self.mother_bbl[_mn]:,.0f} bbl | "
                           f"ZeeZee departing in {CAST_OFF_HOURS*60:.0f} min",
                           mother=_mn)

        elif _zz.status == "CAST_OFF_B":
            self.log_event(t, "ZeeZee", "VESSEL_DEPARTED",
                           "ZeeZee cast off and departed — next visit next month")
            self.zeezee = None   # visit complete; reset for next month trigger

    def _run_sanjulian_transload(self, t):
        """Advance SanJulian's transload state machine each time-step.

        SanJulian behaves exactly like a daughter vessel when transloading:
          BERTHING_B    → hose connection starts after BERTHING_DELAY_HOURS
          HOSE_CONNECT_B→ discharge starts after HOSE_CONNECTION_HOURS;
                           cargo credited to target mother at this point
          DISCHARGING   → cast-off window opens after pump completes
          CAST_OFF_B    → transload cycle complete; export_ready set on target

        All events are logged with Vessel = "SanJulian" so the mother vessel
        receiving the transload shows it identically to a daughter discharge.
        The target mother's berth slot is reserved for the full cycle duration.
        """
        _sj = MOTHER_QUATERNARY_NAME

        # ── Phase advance: step the active status forward ─────────────────────
        if self.sanjulian_status is not None and self.sanjulian_next_t is not None:
            if t < self.sanjulian_next_t - 1e-6:
                return   # phase not yet complete — nothing to do this step

            target = self.sanjulian_target
            amount = self.sanjulian_amount

            # ── BERTHING_B → HOSE_CONNECT_B ──────────────────────────────────
            if self.sanjulian_status == "BERTHING_B":
                if not self.mother_is_at_point_b(target, t):
                    self.log_event(t, _sj, "SJ_TRANSLOAD_ABORT",
                                   f"Transload to {target} aborted during berthing "
                                   f"— {target} left Point B")
                    self._sj_reset()
                    return
                self.sanjulian_status = "HOSE_CONNECT_B"
                self.sanjulian_next_t = t + HOSE_CONNECTION_HOURS
                self.sanjulian_transload_state = {
                    "target": target, "amount": amount,
                    "end_t": self.sanjulian_next_t, "phase": "HOSE_CONNECT_B",
                }
                self.log_event(t, _sj, "HOSE_CONNECTION_START_B",
                               f"Hose connection initiated at {target} "
                               f"({HOSE_CONNECTION_HOURS}h) — transloading "
                               f"{amount:,.0f} bbl from SanJulian",
                               mother=target)
                return   # ← explicit return: do NOT fall through to next elif

            # ── HOSE_CONNECT_B → DISCHARGING: credit cargo to mother now ─────
            elif self.sanjulian_status == "HOSE_CONNECT_B":
                if not self.mother_is_at_point_b(target, t):
                    self.log_event(t, _sj, "SJ_TRANSLOAD_ABORT",
                                   f"Transload to {target} aborted during hose connection "
                                   f"— {target} left Point B")
                    self._sj_reset()
                    return
                sj_vol  = self.mother_bbl[_sj]
                space   = max(0.0, self.mother_capacity_bbl(target) - self.mother_bbl[target])

                # ── REGULATORY GATE: full-volume single-berth rule ───────────
                # SanJulian must discharge ALL contents to a single mother in
                # one uninterrupted berthing operation.  If the target no longer
                # has space for the entire volume (e.g. another vessel discharged
                # to it between trigger and hose-connect), abort and re-anchor.
                # The trigger-check will re-select a suitable target next cycle.
                if space < sj_vol:
                    self.log_event(t, _sj, "SJ_TRANSLOAD_ABORT",
                                   f"Regulatory abort: {target} has only {space:,.0f} bbl "
                                   f"space but SanJulian holds {sj_vol:,.0f} bbl — "
                                   f"full-volume single-berth rule requires "
                                   f"{sj_vol:,.0f} bbl space. Re-anchoring to await "
                                   f"a vessel with sufficient capacity.",
                                   mother=target)
                    self._sj_reset()
                    return

                if sj_vol <= 0:
                    self.log_event(t, _sj, "SJ_TRANSLOAD_ABORT",
                                   f"Transload to {target} aborted — SanJulian is empty")
                    self._sj_reset()
                    return

                actual = sj_vol   # transfer ENTIRE volume — no partial allowed
                # Blend API and credit barrels to target mother
                sj_api  = self.mother_api.get(_sj, 0.0)
                tgt_api = self.mother_api.get(target, 0.0)
                tgt_vol = self.mother_bbl[target]
                new_tgt = tgt_vol + actual
                if new_tgt > 0:
                    self.mother_api[target] = (
                        (tgt_vol * tgt_api + actual * sj_api) / new_tgt
                    )
                self.mother_bbl[target] = new_tgt
                self.mother_bbl[_sj]   -= actual
                if self.mother_bbl[_sj] <= 0:
                    self.mother_api[_sj] = 0.0
                self.sanjulian_amount = actual  # store actual for DISCHARGE_COMPLETE log

                pump_hours = actual / SANJULIAN_TRANSLOAD_RATE_BPH
                self.sanjulian_status = "DISCHARGING"
                self.sanjulian_next_t = t + pump_hours
                self.mother_berth_free_at[target] = max(
                    self.mother_berth_free_at.get(target, 0.0),
                    t + pump_hours + CAST_OFF_HOURS,
                )
                self.sanjulian_transload_state = {
                    "target": target, "amount": actual,
                    "end_t": self.sanjulian_next_t, "phase": "DISCHARGING",
                }
                self.log_event(t, _sj, "DISCHARGE_START",
                               f"Discharging {actual:,.0f} bbl @ {sj_api:.2f}° API | "
                               f"{target}: {self.mother_bbl[target]:,.0f} bbl "
                               f"(blended {self.mother_api[target]:.2f}° API)",
                               mother=target)
                return   # ← explicit return: do NOT fall through

            # ── DISCHARGING → CAST_OFF_B ─────────────────────────────────────
            # SanJulian must empty ALL her volumes to the SAME target vessel in
            # a single uninterrupted operation.  Residual barrels that arrive
            # during the pump cycle are pumped to the same target if space
            # permits.  If the target fills before SanJulian empties, the
            # operation aborts — SanJulian re-anchors and waits for a mother
            # with sufficient space before starting a new cycle.
            # PROHIBITED: redirecting residual to a different mother vessel.
            elif self.sanjulian_status == "DISCHARGING":
                sj_remaining = self.mother_bbl[_sj]

                if sj_remaining > 0:
                    # Extend pump to the SAME target if it still has space
                    cur_space = max(0.0, self.mother_capacity_bbl(target) - self.mother_bbl[target])
                    if cur_space > 0 and self.mother_is_at_point_b(target, t):
                        residual = min(sj_remaining, cur_space)
                        sj_api   = self.mother_api.get(_sj, 0.0)
                        tgt_api  = self.mother_api.get(target, 0.0)
                        tgt_vol  = self.mother_bbl[target]
                        new_tgt  = tgt_vol + residual
                        if new_tgt > 0:
                            self.mother_api[target] = (
                                (tgt_vol * tgt_api + residual * sj_api) / new_tgt
                            )
                        self.mother_bbl[target] = new_tgt
                        self.mother_bbl[_sj]   -= residual
                        if self.mother_bbl[_sj] <= 0:
                            self.mother_api[_sj] = 0.0
                        self.sanjulian_amount += residual
                        extra_pump_hours = residual / SANJULIAN_TRANSLOAD_RATE_BPH
                        self.sanjulian_next_t = t + extra_pump_hours
                        self.mother_berth_free_at[target] = max(
                            self.mother_berth_free_at.get(target, 0.0),
                            t + extra_pump_hours + CAST_OFF_HOURS,
                        )
                        self.sanjulian_transload_state = {
                            "target": target, "amount": self.sanjulian_amount,
                            "end_t": self.sanjulian_next_t, "phase": "DISCHARGING",
                        }
                        self.log_event(t, _sj, "DISCHARGE_START",
                                       f"Pumping residual {residual:,.0f} bbl to {target} "
                                       f"(same-vessel continuation) @ {sj_api:.2f}° API | "
                                       f"{target}: {self.mother_bbl[target]:,.0f} bbl",
                                       mother=target)
                        return
                    else:
                        # Target is full or left BIA — regulatory violation to redirect.
                        # Abort, cast off, re-anchor; a new cycle will start when a
                        # mother with sufficient capacity is available.
                        self.log_event(t, _sj, "SJ_TRANSLOAD_ABORT",
                                       f"Regulatory abort: {target} {'full' if cur_space <= 0 else 'left BIA'} "
                                       f"— {sj_remaining:,.0f} bbl residual cannot be redirected "
                                       f"to another vessel (single-berth rule). "
                                       f"SanJulian re-anchoring with {sj_remaining:,.0f} bbl remaining.",
                                       mother=target)
                        actual = self.sanjulian_amount
                        self.sanjulian_total_transloaded += actual
                        cast_off_t = self.next_cast_off_window(t)
                        wait_co    = cast_off_t - t
                        self.sanjulian_status = "CAST_OFF_B"
                        self.sanjulian_next_t = cast_off_t + CAST_OFF_HOURS
                        self.sanjulian_transload_state = {
                            "target": target, "amount": actual,
                            "end_t": self.sanjulian_next_t, "phase": "CAST_OFF_B",
                        }
                        if wait_co > 0:
                            self.log_event(t, _sj, "WAITING_CAST_OFF",
                                           f"Night restriction — cast-off from {target} at "
                                           f"{self.hours_to_dt(cast_off_t).strftime('%Y-%m-%d %H:%M')}",
                                           mother=target)
                        self.log_event(cast_off_t, _sj, "CAST_OFF_START_B",
                                       f"Cast-off from {target} ({CAST_OFF_HOURS}h)",
                                       mother=target)
                        return

                actual     = self.sanjulian_amount
                self.sanjulian_total_transloaded += actual
                cast_off_t = self.next_cast_off_window(t)
                wait_co    = cast_off_t - t
                self.sanjulian_status = "CAST_OFF_B"
                self.sanjulian_next_t = cast_off_t + CAST_OFF_HOURS
                self.sanjulian_transload_state = {
                    "target": target, "amount": actual,
                    "end_t": self.sanjulian_next_t, "phase": "CAST_OFF_B",
                }
                self.log_event(t, _sj, "DISCHARGE_COMPLETE",
                               f"{target}: {self.mother_bbl[target]:,.0f} bbl | "
                               f"SanJulian emptied — {actual:,.0f} bbl total | "
                               f"Cast-off scheduled "
                               f"{self.hours_to_dt(cast_off_t).strftime('%H:%M')} "
                               f"(wait {wait_co:.1f}h)",
                               mother=target)
                if wait_co > 0:
                    self.log_event(t, _sj, "WAITING_CAST_OFF",
                                   f"Night restriction — cast-off from {target} at "
                                   f"{self.hours_to_dt(cast_off_t).strftime('%Y-%m-%d %H:%M')}",
                                   mother=target)
                self.log_event(cast_off_t, _sj, "CAST_OFF_START_B",
                               f"Cast-off from {target} ({CAST_OFF_HOURS}h)",
                               mother=target)
                return   # ← explicit return: do NOT fall through

            # ── CAST_OFF_B → cycle complete ──────────────────────────────────
            elif self.sanjulian_status == "CAST_OFF_B":
                # Before completing, check if more barrels arrived on SanJulian
                # during the cast-off wait (daughter finished discharging while
                # we waited for daylight).  Only pump to the current target —
                # residual cannot be redirected to another vessel.
                # If current target has no space, cast off and let the next
                # cycle (T1/T4) handle the remainder.
                sj_remaining = self.mother_bbl[_sj]

                if sj_remaining > 0:
                    cur_space = max(0.0, self.mother_capacity_bbl(target) - self.mother_bbl[target])
                    if cur_space > 0 and self.mother_is_at_point_b(target, t):
                        residual = min(sj_remaining, cur_space)
                        sj_api   = self.mother_api.get(_sj, 0.0)
                        tgt_api  = self.mother_api.get(target, 0.0)
                        tgt_vol  = self.mother_bbl[target]
                        new_tgt  = tgt_vol + residual
                        if new_tgt > 0:
                            self.mother_api[target] = (
                                (tgt_vol * tgt_api + residual * sj_api) / new_tgt
                            )
                        self.mother_bbl[target] = new_tgt
                        self.mother_bbl[_sj]   -= residual
                        if self.mother_bbl[_sj] <= 0:
                            self.mother_api[_sj] = 0.0
                        self.sanjulian_amount += residual
                        extra_pump_hours = residual / SANJULIAN_TRANSLOAD_RATE_BPH
                        self.sanjulian_status = "DISCHARGING"
                        self.sanjulian_next_t = t + extra_pump_hours
                        self.mother_berth_free_at[target] = max(
                            self.mother_berth_free_at.get(target, 0.0),
                            t + extra_pump_hours + CAST_OFF_HOURS,
                        )
                        self.sanjulian_transload_state = {
                            "target": target, "amount": self.sanjulian_amount,
                            "end_t": self.sanjulian_next_t, "phase": "DISCHARGING",
                        }
                        self.log_event(t, _sj, "DISCHARGE_START",
                                       f"Pumping residual {residual:,.0f} bbl arrived during "
                                       f"cast-off wait @ {sj_api:.2f}° API | "
                                       f"{target}: {self.mother_bbl[target]:,.0f} bbl",
                                       mother=target)
                        return   # back to DISCHARGING — cast-off deferred
                    # else: target full / unavailable — cast off now with residual
                    # remaining; a new cycle will start once SanJulian is free

                actual = self.sanjulian_amount
                # Mark target export-ready if it crossed the trigger
                if (self.mother_bbl[target] >= self.mother_export_trigger_bbl(target)
                        and not self.export_ready[target]):
                    self.export_ready[target]       = True
                    self.export_ready_since[target] = t
                self.log_event(t, _sj, "SJ_TRANSLOAD_COMPLETE",
                               f"Transload cycle complete → {target}: "
                               f"{actual:,.0f} bbl | "
                               f"SanJulian now {self.mother_bbl[_sj]:,.0f} bbl | "
                               f"{target} now {self.mother_bbl[target]:,.0f} bbl",
                               mother=target)
                # ── Fender preparation after cast-off ───────────────────────
                # SanJulian requires SANJULIAN_FENDER_PREP_HOURS of fender
                # preparation after every cast-off before she can:
                #   (a) receive a daughter vessel, or
                #   (b) start a new transload cycle (berth next primary mother)
                fender_ready = t + SANJULIAN_FENDER_PREP_HOURS
                self.sanjulian_fender_ready_t = fender_ready
                self.log_event(t, _sj, "SJ_FENDER_PREP",
                               f"Post-cast-off fender preparation "
                               f"({SANJULIAN_FENDER_PREP_HOURS}h) — "
                               f"ready at {self.hours_to_dt(fender_ready).strftime('%H:%M')}",
                               mother=target)
                self.sanjulian_transload_end_t = t
                self._sj_reset()
                return

        # ── Check whether a new transload cycle should start ──────────────────
        if self.sanjulian_status is None:
            reason, target, amount = self._sj_transload_trigger_check(t)
            if not (reason and target and amount > 0):
                return

            # Compute berth start — wait for berthing window and berth availability.
            # For T3 (idle mother, no daughter active or arriving), mother_berth_free_at
            # may hold a stale timestamp from a previous completed discharge.  The berth
            # is genuinely free now, so clamp berth_free_at to current time t.
            # Also enforce that SanJulian's fender preparation is complete before she
            # can berth: add SANJULIAN_FENDER_PREP_HOURS to earliest possible start.
            _fender_earliest = max(t, self.sanjulian_fender_ready_t)
            _is_t3 = reason.startswith("T3")
            if _is_t3:
                # Mother is confirmed idle — only respect the daylight berthing window
                # and fender prep completion, not a potentially stale berth-free timestamp.
                berth_start = self.next_berthing_window(_fender_earliest, point="B")
                berth_start = max(berth_start, self.mother_available_at.get(target, 0.0))
                berth_start = self.next_berthing_window(berth_start, point="B")
            else:
                berth_start = self.next_berthing_window(_fender_earliest, point="B")
                berth_start = max(berth_start,
                                  self.mother_berth_free_at.get(target, 0.0),
                                  self.mother_available_at.get(target, 0.0))
                berth_start = self.next_berthing_window(berth_start, point="B")
            # Don't gate on mother_is_at_point_b here — SanJulian is always at BIA.
            # The phase machine will abort if target leaves before hose connects.

            # Log fender prep before berthing if it delays the start
            if self.sanjulian_fender_ready_t > t + 1e-6:
                self.log_event(t, _sj, "SJ_FENDER_PREP",
                               f"Pre-berthing fender preparation "
                               f"({SANJULIAN_FENDER_PREP_HOURS}h) before berthing {target} — "
                               f"ready at {self.hours_to_dt(self.sanjulian_fender_ready_t).strftime('%H:%M')}",
                               mother=target)

            # Reserve berth for full cycle: berthing + hose + pump + cast-off
            # (fender prep is already baked into berth_start via _fender_earliest)
            pump_hours  = amount / SANJULIAN_TRANSLOAD_RATE_BPH
            _full_cycle = (BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS
                           + pump_hours + CAST_OFF_HOURS)
            self.mother_berth_free_at[target] = max(
                self.mother_berth_free_at.get(target, 0.0),
                berth_start + _full_cycle,
            )
            self.sanjulian_status = "BERTHING_B"
            self.sanjulian_target = target
            self.sanjulian_amount = amount
            self.sanjulian_next_t = berth_start + BERTHING_DELAY_HOURS
            self.sanjulian_transload_state = {
                "target": target, "amount": amount,
                "end_t": self.sanjulian_next_t, "phase": "BERTHING_B",
            }
            self.sanjulian_transload_end_t = berth_start + _full_cycle
            self.log_event(t, _sj, "SJ_TRANSLOAD_START",
                           f"[{reason}] Transload cycle started → {target}: "
                           f"{amount:,.0f} bbl @ {SANJULIAN_TRANSLOAD_RATE_BPH:,} bph | "
                           f"SanJulian: {self.mother_bbl[_sj]:,.0f} bbl | "
                           f"{target}: {self.mother_bbl[target]:,.0f} bbl",
                           mother=target)
            self.log_event(berth_start, _sj, "BERTHING_START_B",
                           f"Berthing at {target} ({BERTHING_DELAY_HOURS}h procedure) "
                           f"— SanJulian transload",
                           mother=target)

    def _sj_reset(self):
        """Clear all SanJulian transload state after a cycle ends or aborts."""
        self.sanjulian_status          = None
        self.sanjulian_target          = None
        self.sanjulian_amount          = 0.0
        self.sanjulian_next_t          = None
        self.sanjulian_transload_state = None
        # Reset daughter count. After the first cycle completes, no pre-seeded
        # vessels remain, so threshold drops back to the base value of 2.
        self.sanjulian_daughters_loaded        = 0
        self.sanjulian_daughters_min_threshold = 2
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
        """Pick the best Point B mother for faster turnaround and export readiness.

        Primary mothers are generally preferred over SanJulian.  However, when a
        same-day primary is delayed by more than SANJULIAN_DELAY_THRESHOLD_HOURS
        beyond SanJulian's earliest available start, that primary is demoted so that
        SanJulian absorbs the incoming daughter immediately rather than leaving her
        idle for many hours at a congested BIA.  This is especially important when
        a primary mother (e.g. Alkebulan) is offline — the remaining two primaries
        can become fully occupied, and SanJulian must act as the active buffer.

        Grouping:
          group 0 — same-day primary available within SANJULIAN_DELAY_THRESHOLD_HOURS
                    of SanJulian's start (or SanJulian not in candidates)
          group 1 — SanJulian (beats delayed primaries; loses to prompt primaries)
          group 2 — same-day primary delayed beyond threshold, OR next-day primary
        """
        assigned_today = self.point_b_day_assigned_mothers.setdefault(day_key, set())
        horizon_8 = self.next_wall_clock_hour(decision_time, 8)
        # Next midnight in sim-hours — used to identify same-day candidates
        day_end = (int(decision_time // 24) + 1) * 24

        ranked = []
        for start, berth_t, mother_name in candidates:
            add_at = start + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS
            projected_8 = self.projected_mother_stock(
                mother_name,
                horizon_8,
                exclude_vessel=v.name,
            )
            if add_at <= horizon_8 + 1e-6:
                projected_8 += v.cargo_bbl
            ranked.append({
                "start": start,
                "berth_t": berth_t,
                "mother": mother_name,
                "immediate": start <= decision_time + 0.01,
                "same_day": start < day_end,
                "unused_today": mother_name not in assigned_today,
                "projected_8": projected_8,
                "is_sanjulian": 1 if mother_name == MOTHER_QUATERNARY_NAME else 0,
            })

        # SanJulian's earliest start (None when she is not in candidates)
        sj_entry = next((r for r in ranked if r["is_sanjulian"] == 1), None)
        sj_start_ref = sj_entry["start"] if sj_entry else None

        # Sort all candidates together.
        # Key priority:
        #   1. Primary prompt (same_day, start ≤ sj_start + threshold) → group 0
        #      Within group 0: prefer the primary NOT yet used today (load balancing)
        #      Then: earlier start; then lower projected stock; then name
        #   2. SanJulian                                                → group 1
        #   3. Primary delayed (same_day but start > threshold) or next-day → group 2
        #
        # Load-balancing threshold: two primaries are "equally prompt" when their
        # start times are within LOAD_BALANCE_WINDOW_HOURS of each other.  Within
        # that window, the unused-today flag is the first tiebreaker so traffic
        # alternates between Bryanston and GreenEagle rather than funnelling to
        # whichever has the lower current stock (which always favours the one that
        # just returned from export).
        LOAD_BALANCE_WINDOW_HOURS = 4.0

        # Earliest group-0 primary start — used to detect "equally prompt" candidates
        _g0_starts = [
            r["start"] for r in ranked
            if r["is_sanjulian"] == 0 and r["same_day"]
            and (sj_start_ref is None
                 or r["start"] <= sj_start_ref + SANJULIAN_DELAY_THRESHOLD_HOURS)
        ]
        _earliest_g0 = min(_g0_starts) if _g0_starts else None

        def _sort_key(r):
            if r["is_sanjulian"] == 0 and r["same_day"]:
                if (sj_start_ref is not None
                        and r["start"] > sj_start_ref + SANJULIAN_DELAY_THRESHOLD_HOURS):
                    group = 2
                else:
                    group = 0
            elif r["is_sanjulian"] == 1:
                group = 1
            else:
                group = 2

            if group == 0 and _earliest_g0 is not None:
                # Within group 0, candidates within LOAD_BALANCE_WINDOW_HOURS of
                # the earliest are treated as equally prompt.  Among those, prefer
                # the one unused today (load balancing), then earlier start, then
                # lower projected stock.  Candidates outside the window sort by
                # start time as before.
                _equally_prompt = (r["start"] <= _earliest_g0 + LOAD_BALANCE_WINDOW_HOURS)
                if _equally_prompt:
                    return (
                        group,
                        0 if r["unused_today"] else 1,   # unused-today first
                        r["start"],
                        -r["projected_8"],
                        r["mother"],
                    )

            return (
                group,
                r["start"],
                -r["projected_8"],
                0 if r["unused_today"] else 1,
                r["mother"],
            )

        ranked.sort(key=_sort_key)
        selected = ranked[0]
        assigned_today.add(selected["mother"])
        return selected, horizon_8

    def log_event(self, t, vessel_name, event, detail="", voyage_num=None, mother=None):
        # O(1) vessel lookup via index dict (built lazily, invalidated on join).
        if not hasattr(self, "_vessel_index") or len(self._vessel_index) != len(self.vessels):
            self._vessel_index = {vv.name: vv for vv in self.vessels}
        _v = self._vessel_index.get(vessel_name)
        # Resolve mother: explicit arg → vessel's current assigned_mother → None
        if mother is None and _v is not None:
            mother = _v.assigned_mother
        # Snapshot the vessel's current cargo API for this log row
        _vessel_api_snap = round(self.vessel_api.get(vessel_name, 0.0), 2) if _v is not None else 0.0
        # Resolve voyage code: explicit on vessel, else derive from current voyage_num
        _vcode = ""
        if _v is not None:
            _vcode = getattr(_v, "voyage_code", "") or ""
            if not _vcode and voyage_num:
                _vcode = make_voyage_code(vessel_name, voyage_num)
        self.log.append({
            "Time"       : self.hours_to_dt(t).strftime("%Y-%m-%d %H:%M"),
            "Day"        : int(t // 24) + 1,
            "Hour"       : f"{int(t % 24):02d}:{int((t % 1)*60):02d}",
            "Vessel"     : vessel_name,
            "Voyage"     : voyage_num,
            "VoyageCode" : _vcode,
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
            "PGM_bbl": round(self.storage_bbl[STORAGE_SENARY_NAME]),
            "Storage_Overflow_Accum_bbl": round(sum(self.storage_overflow_bbl.values())),
            "Chapel_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_PRIMARY_NAME]),
            "JasmineS_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_SECONDARY_NAME]),
            "Westmore_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_TERTIARY_NAME]),
            "Duke_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_QUATERNARY_NAME]),
            "Starturn_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_QUINARY_NAME]),
            "PGM_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_SENARY_NAME]),
            "PointF_Overflow_Accum_bbl": round(self.point_f_overflow_accum_bbl),
            "PointF_Active_Loading_bbl": round(self.point_f_active_loading_bbl()),
            "Mother_bbl" : round(self.total_mother_bbl()),
            "Bryanston_bbl":  round(self.mother_bbl[MOTHER_PRIMARY_NAME]),
            "GreenEagle_bbl": round(self.mother_bbl[MOTHER_SECONDARY_NAME]),
            "SanJulian_bbl":  round(self.mother_bbl[MOTHER_QUATERNARY_NAME]),
            "SanJulian": self.sanjulian_status or "IDLE_B",
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
        Uses the current net production rate minus expected draws from all
        vessels committed to this storage — including those waiting for a
        berth (WAITING_BERTH_A, WAITING_STOCK) which are en route but not
        yet loading.  Including them gives a more accurate end-of-day
        forecast and prevents the dispatch engine from treating an already-
        committed storage as under-served.
        """
        stock  = self.storage_bbl[storage_name]
        rate   = self.production_rate_bph_at(storage_name, 0)
        cap    = STORAGE_CAPACITY_BY_NAME[storage_name]
        # Subtract draws from all vessels committed to this storage,
        # whether actively loading or waiting for the berth to open.
        committed_statuses = {
            "LOADING",          # actively pumping cargo
            "HOSE_CONNECT_A",   # hoses connected, loading imminent
            "BERTHING_A",       # securing alongside, hose connection next
            "WAITING_BERTH_A",  # committed, waiting for berth to open
            "WAITING_STOCK",    # committed, waiting for stock threshold
        }
        for vv in self.vessels:
            if vv.assigned_storage != storage_name:
                continue
            if vv.status in committed_statuses:
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

    def storage_dispatch_rank(self, storage_name):
        """Return the risk-first dispatch rank tuple for a storage.

        Lower tuples are more urgent. High-production storages receive the
        same small bias compression used elsewhere in the dispatcher.
        """
        stock = self.storage_bbl[storage_name]
        crit  = STORAGE_CRITICAL_THRESHOLD_BY_NAME[storage_name]
        raw_gap = abs(stock - crit)
        if raw_gap <= DISPATCH_BIAS_FORECAST_BBL:
            bias = self.production_rate_bias_factor(storage_name)
            effective_gap = raw_gap * (1.0 - bias)
        else:
            effective_gap = raw_gap
        unsafe = 0 if stock >= crit else 1
        return (unsafe, effective_gap, -stock, storage_name)

    def plan_ac_waiting_assignments(self, vessels, t):
        """Greedy A/C matching for idle or waiting vessels.

        This prevents a smaller flexible vessel from reserving the most urgent
        Point A/C storage when a larger permitted vessel in the same waiting
        pool can drain materially more stock from that location.
        """
        ac_storages = [
            STORAGE_PRIMARY_NAME,
            STORAGE_SECONDARY_NAME,
            STORAGE_TERTIARY_NAME,
        ]
        pairings = []
        for vv in vessels:
            for storage_name in ac_storages:
                if not self.storage_allowed_for_vessel(storage_name, vv.name):
                    continue
                if self.storage_locked_by_active_berth(storage_name, requesting_vessel=vv.name):
                    continue
                current_bonus = 0 if vv.assigned_storage == storage_name else 1
                pairings.append((
                    self.storage_dispatch_rank(storage_name),
                    -self.effective_load_cap(vv.name, storage_name),
                    current_bonus,
                    vv.name,
                    storage_name,
                ))

        assignments = {}
        claimed_vessels = set()
        claimed_storages = set()
        for _, _, _, vessel_name, storage_name in sorted(pairings):
            if vessel_name in claimed_vessels or storage_name in claimed_storages:
                continue
            assignments[vessel_name] = storage_name
            claimed_vessels.add(vessel_name)
            claimed_storages.add(storage_name)
        return assignments

    def choose_hourly_storage_option(self, v, t, excluded_storages=None):
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
        # Skip reassessment only when the vessel is physically committed to Point F.
        # Bedford may be the active Ibom loader but have returned to Point A to load
        # a full cargo — in that case target_point != "F" and it must be reassessed.
        if v.target_point == "F":
            return None

        candidates_all = [
            s for s in STORAGE_NAMES
            if self.storage_allowed_for_vessel(s, v.name)
            and not self.storage_locked_by_active_berth(s, requesting_vessel=v.name)
        ]
        if not candidates_all:
            return None

        excluded = set(excluded_storages or ())
        candidates = candidates_all
        if excluded:
            non_excluded = [s for s in candidates_all if s not in excluded]
            if non_excluded:
                candidates = non_excluded

        waiting_pool = [
            vv for vv in self.vessels
            if vv.status in {"WAITING_BERTH_A", "IDLE_A", "WAITING_STOCK"}
            and vv.target_point in ("A", "C", "D", "E")
            and vv.target_point != "F"
        ]

        # ── Production-rate bias ──────────────────────────────────────────────
        # Compress the apparent gap for high-production storages when they are
        # close to critical, so they sort ahead of low-production peers.
        def risk_rank(storage_name):
            return self.storage_dispatch_rank(storage_name)

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

        # ── Default: pick best risk-priority candidate ───────────────────────
        # Berth availability provides a fractional tie-break bonus (0.5 rank
        # positions) but can never override a genuine urgency advantage.
        # Old code: (berth_now_penalty, urgency_idx) — berth_now completely
        # dominated urgency, routing vessels to the wrong storage whenever a
        # more urgent berth was busy.
        def rank(storage_name):
            p = STORAGE_POINT.get(storage_name, "A")
            berth_now = (
                self.is_valid_berthing_time(t, point=p)
                and t >= self.storage_berth_free_at[storage_name]
                and t >= self.next_storage_berthing_start_at[p]
            )
            ord_idx = ordered.index(storage_name) if storage_name in ordered else 99
            # Berth-available bonus: 0.5 rank improvement (fractional, never
            # enough to leapfrog a storage that is 1+ full urgency ranks ahead)
            effective_idx = ord_idx - (0.5 if berth_now else 0.0)
            return effective_idx

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
        reassess_vessels = []
        for vv in self.vessels:
            if vv.status not in {"IDLE_A", "WAITING_BERTH_A", "WAITING_STOCK"}:
                continue
            if vv.target_point not in ("A", "C", "D", "E"):
                continue
            # Don't disturb a sleeping, priority-locked, or JMP-locked vessel
            if vv.resumption_priority or (vv.resumption_hour is not None and t < vv.resumption_hour):
                continue
            if getattr(vv, "_jmp_override_locked", False):
                continue   # JMP override active — do not reassign
            reassess_vessels.append(vv)

        assigned_ac = self.plan_ac_waiting_assignments(reassess_vessels, t)
        reserved_ac = set(assigned_ac.values())

        for vv in reassess_vessels:
            _new_storage = assigned_ac.get(vv.name)
            if _new_storage is None:
                _new_storage = self.choose_hourly_storage_option(vv, t, excluded_storages=reserved_ac)
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
            # Skip only if the vessel is physically committed to Point F:
            # either it is the active Ibom loader AND its target_point is still "F"
            # (i.e. it hasn't returned to Point A yet), or it is en-route to F.
            if (vv.name == self.point_f_active_loader and vv.target_point == "F") \
                    or vv.target_point == "F":
                continue
            # Don't disturb a sleeping, priority-locked, or JMP-locked vessel.
            # A JMP date-shift override sets _jmp_override_locked=True while the
            # vessel waits to load on a specific future date.  Without this guard,
            # the 05:00 preops reassessment silently overwrites the locked storage
            # (e.g. JasmineS → Chapel) before the target date is reached, causing
            # the vessel to load from the wrong storage when it wakes.
            if vv.resumption_priority or (vv.resumption_hour is not None and t < vv.resumption_hour):
                continue
            if getattr(vv, "_jmp_override_locked", False):
                continue   # JMP date-shift active — preserve locked storage assignment
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

    # -- Main simulation loop ---------------------------------------------
    def run(self):
        total_hours = SIMULATION_DAYS * 24
        t = 0.0

        while t <= total_hours:
            self._current_t = t   # make current time available to helper methods
            self.maybe_run_daily_preops_storage_reassessment(t)
            self.maybe_run_ac_post_breakwater_reassessment(t)
            self._maybe_run_multiple_transient_op(t)

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
                    # Invalidate vessel index so log_event rebuilds it.
                    if hasattr(self, "_vessel_index"):
                        del self._vessel_index
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
            # ── Capacity-aware IDLE_A dispatch ordering ───────────────────
            # When multiple vessels are simultaneously ready for IDLE_A
            # dispatch, the vessel with the highest load capacity for the
            # most urgent available storage must be processed first.  Without
            # this sort, vessels are processed in VESSEL_NAMES order, meaning
            # a smaller vessel (e.g. Woodstock 42k, index 6) grabs the most
            # urgent storage (Chapel at overflow) before a larger vessel
            # (Watson 85k, index 8) arrives — wasting 49k of drain potential.
            #
            # We split the vessel list into two groups at each t:
            #   Group A: vessels NOT ready for IDLE_A dispatch — processed
            #            first in their natural order (no behaviour change).
            #   Group B: vessels simultaneously at IDLE_A and due to fire now
            #            — sorted by urgency(best_storage) × load_cap DESC.
            _group_a = []   # non-IDLE_A or not yet due
            _group_b = []   # IDLE_A vessels due to fire this tick
            for _v in self.vessels:
                if t < _v.next_event_time or _v.status != "IDLE_A":
                    _group_a.append(_v)
                else:
                    _group_b.append(_v)

            # Score each Group B vessel by the urgency of its best available
            # unlocked storage and its load capacity at that storage.
            # urgency = overflow_risk = (stock / critical_threshold); higher
            # ratio → closer to overflow → more urgent to drain.
            def _dispatch_priority(vv):
                best_urgency = 0.0
                best_cap     = 0
                for sn in STORAGE_NAMES:
                    if not self.storage_allowed_for_vessel(sn, vv.name):
                        continue
                    if self.storage_locked_by_active_berth(sn, requesting_vessel=vv.name):
                        continue
                    crit = STORAGE_CRITICAL_THRESHOLD_BY_NAME.get(sn, 1)
                    if crit <= 0:
                        continue
                    urgency = self.storage_bbl[sn] / crit   # >1 → above critical
                    cap     = self.effective_load_cap(vv.name, sn)
                    if urgency > best_urgency or (urgency == best_urgency and cap > best_cap):
                        best_urgency = urgency
                        best_cap     = cap
                # Primary sort: urgency DESC (most overflow-risk first)
                # Secondary sort: capacity DESC (largest drain first)
                # Negate both so min() / sort() gives descending order
                return (-best_urgency, -best_cap)

            _group_b.sort(key=_dispatch_priority)
            _ordered_vessels = _group_a + _group_b

            for v in _ordered_vessels:
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
                    # ── Mid-sim dormancy trigger ──────────────────────────────
                    # If the vessel has a dormancy_start_hour set (from a date-range
                    # dormancy window), enforce it once t crosses that threshold:
                    # the vessel goes idle and sleeps until resumption_hour.
                    _dorm_h = getattr(v, "dormancy_start_hour", None)
                    if (_dorm_h is not None
                            and t >= _dorm_h
                            and v.resumption_hour is None):
                        # Activate dormancy now
                        v.resumption_hour      = v.resumption_hour  # already set by sim init
                        v.dormancy_start_hour  = None               # one-shot trigger
                        # resumption_hour was set by set_vessel_resumption in __init__
                        # but not yet enforced; now it takes effect.

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
                            v._vessel_voyage_counter = getattr(v, '_vessel_voyage_counter', 0) + 1
                            v.current_voyage = v._vessel_voyage_counter
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

                    if v.name == self.point_f_active_loader and v.target_point == "F":
                        # This vessel is the designated Ibom loader AND is physically
                        # at Point F — route to PF_LOADING.
                        # IMPORTANT: if target_point != "F" the vessel has returned to
                        # Point A/C after delivering a partial Ibom cargo and must be
                        # allowed to load a full Point A cargo before returning to Ibom.
                        # Do NOT redirect to PF_LOADING in that case.
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

                    # Bedford/Balham: if not the active Ibom loader (or is the active
                    # loader but has physically returned to Point A after delivering a
                    # partial Ibom cargo), dispatch to SanBarth (Point A).
                    if (v.name in self.point_f_vessels
                            and (self.point_f_active_loader != v.name
                                 or v.target_point != "F")
                            and v.target_point != "F"):
                        v.target_point = "A"  # load Chapel/JasmineS

                    # Only assign a new voyage number on a fresh cycle start.
                    if not hasattr(v, '_voyage_assigned') or not v._voyage_assigned:
                        v._vessel_voyage_counter = getattr(v, '_vessel_voyage_counter', 0) + 1
                        v.current_voyage = v._vessel_voyage_counter
                        v._voyage_assigned = True
                    cap = v.cargo_capacity   # default; overridden per-storage below

                    # ── JMP manual override check (must run BEFORE pre-assigned) ──
                    # Checked first so a forced assignment always wins over any
                    # stale v.assigned_storage from a previous voyage.
                    # Use calendar-day formula (epoch-aligned) so Day 4 always means
                    # the 4th calendar day regardless of when within it the vessel is idle.
                    _dispatch_day = int((t + SIM_HOUR_OFFSET) // 24) + 1
                    _ovr_entry    = STORAGE_DISPATCH_OVERRIDES.get(v.name, {}).get(_dispatch_day)
                    # Support both plain storage string and dict with load_after_hour
                    if isinstance(_ovr_entry, dict):
                        _forced_stor       = _ovr_entry.get("storage")
                        _forced_load_after = _ovr_entry.get("load_after_hour")  # sim-hour
                    else:
                        _forced_stor       = _ovr_entry
                        _forced_load_after = None
                    # Also honour a date-shift already encoded on the vessel from a prior tick
                    if _forced_stor is None and getattr(v, "_jmp_override_locked", False):
                        # Vessel is locked from a previous JMP trigger; keep waiting
                        _forced_stor       = v.assigned_storage
                        _forced_load_after = getattr(v, "_jmp_load_after_hour", None)
                    if (_forced_stor
                            and _forced_stor in STORAGE_NAMES
                            and self.storage_allowed_for_vessel(_forced_stor, v.name)):
                        # ── Date-shift hold: vessel must wait until _forced_load_after ──
                        if (_forced_load_after is not None and t < _forced_load_after):
                            # Lock the vessel and hold it idle until the target hour
                            v._jmp_override_locked  = True
                            v._jmp_load_after_hour  = _forced_load_after
                            v.assigned_storage      = _forced_stor
                            v.target_point          = STORAGE_POINT.get(_forced_stor, "A")
                            v.next_event_time       = _forced_load_after
                            self.log_event(
                                t, v.name, "WAITING_BERTH_A",
                                f"JMP override: holding until "
                                f"{self.hours_to_dt(_forced_load_after).strftime('%Y-%m-%d %H:%M')}"
                                f" then loading from {_forced_stor}",
                                voyage_num=v.current_voyage,
                            )
                            continue
                        # ── Normal override (or date-shift hour reached) ───────────
                        if not self.storage_locked_by_active_berth(
                                _forced_stor, requesting_vessel=v.name):
                            _f_cap   = self.effective_load_cap(v.name, _forced_stor)
                            _f_point = STORAGE_POINT.get(_forced_stor, "A")
                            _f_berth = max(
                                self.next_berthing_window(t, point=_f_point),
                                self.storage_berth_free_at[_forced_stor],
                                self.next_storage_berthing_start_at[_f_point],
                            )
                            _f_berth = self.next_berthing_window(_f_berth, point=_f_point)
                            # Lock: prevent preops and hourly reassessment from undoing this
                            v._jmp_override_locked  = True
                            v._jmp_load_after_hour  = None   # date-shift consumed
                            v.assigned_storage    = _forced_stor
                            v.assigned_load_hours = self.storage_load_hours(
                                _forced_stor, _f_cap, vessel_name=v.name)
                            v.target_point        = _f_point
                            v.status              = "WAITING_BERTH_A"
                            v.next_event_time     = _f_berth
                            self.log_event(
                                t, v.name, "WAITING_BERTH_A",
                                f"JMP override → {_forced_stor} (Day {_dispatch_day}); "
                                f"berthing at "
                                f"{self.hours_to_dt(_f_berth).strftime('%Y-%m-%d %H:%M')}"
                                f" [override locked — immune to reassessment]",
                                voyage_num=v.current_voyage,
                            )
                            self.next_storage_berthing_start_at[_f_point] = (
                                _f_berth + BERTHING_DELAY_HOURS
                            )
                            continue

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
                        # Hard active-berth lock: if another vessel is physically
                        # berthed/connecting/loading here, block regardless of timing.
                        _pre_active_lock = self.storage_locked_by_active_berth(
                            _pre_assigned, requesting_vessel=v.name)
                        _berth_now_ok = (
                            not _pre_active_lock
                            and self.is_valid_berthing_time(t, point=_pre_point)
                            and t >= self.storage_berth_free_at[_pre_assigned]
                            and t >= self.next_storage_berthing_start_at[_pre_point]
                        )
                        if not _berth_now_ok:
                            _next_chk = self.next_daylight_hourly_berth_check(t, point=_pre_point)
                            v.status = "WAITING_BERTH_A"
                            v.target_point = _pre_point
                            v.next_event_time = _next_chk
                            _lock_reason = " (berth physically occupied)" if _pre_active_lock else ""
                            self.log_event(
                                t,
                                v.name,
                                "WAITING_BERTH_A",
                                f"Arrived/idle for {_pre_assigned}; berth unavailable now{_lock_reason} — hourly daylight recheck at "
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
                        _cand_active_lock = self.storage_locked_by_active_berth(
                            selected_storage, requesting_vessel=v.name)
                        berth_now_ok = (
                            not _cand_active_lock
                            and self.is_valid_berthing_time(t, point=selected_point)
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
                            _lock_reason = " (berth physically occupied)" if _cand_active_lock else ""
                            self.log_event(
                                t,
                                v.name,
                                "WAITING_BERTH_A",
                                f"Berth unavailable at {selected_storage}{_lock_reason}; hourly daylight recheck at "
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
                    # ── JMP override intercept ────────────────────────────────
                    # Redirect if a JMP override targets a different storage today.
                    # Calendar-day formula matches what the JMP displays to the user.
                    _wb_day    = int((t + SIM_HOUR_OFFSET) // 24) + 1
                    _wb_forced = STORAGE_DISPATCH_OVERRIDES.get(v.name, {}).get(_wb_day)
                    if (_wb_forced
                            and _wb_forced != v.assigned_storage
                            and _wb_forced in STORAGE_NAMES
                            and self.storage_allowed_for_vessel(_wb_forced, v.name)
                            and not self.storage_locked_by_active_berth(
                                _wb_forced, requesting_vessel=v.name)):
                        _wb_cap   = self.effective_load_cap(v.name, _wb_forced)
                        _wb_point = STORAGE_POINT.get(_wb_forced, "A")
                        _wb_berth = max(
                            self.next_berthing_window(t, point=_wb_point),
                            self.storage_berth_free_at[_wb_forced],
                            self.next_storage_berthing_start_at[_wb_point],
                        )
                        _wb_berth = self.next_berthing_window(_wb_berth, point=_wb_point)
                        v.assigned_storage    = _wb_forced
                        v.assigned_load_hours = self.storage_load_hours(
                            _wb_forced, _wb_cap, vessel_name=v.name)
                        v.target_point        = _wb_point
                        v.next_event_time     = _wb_berth
                        self.log_event(
                            t, v.name, "ALLOCATION_REASSESS",
                            f"JMP override redirected waiting berth to {_wb_forced} "
                            f"(Day {_wb_day}); berth at "
                            f"{self.hours_to_dt(_wb_berth).strftime('%Y-%m-%d %H:%M')}",
                            voyage_num=v.current_voyage,
                        )
                        self.next_storage_berthing_start_at[_wb_point] = (
                            _wb_berth + BERTHING_DELAY_HOURS
                        )
                        continue

                    selected_storage = v.assigned_storage
                    if not selected_storage or not self.storage_allowed_for_vessel(selected_storage, v.name):
                        v.status = "IDLE_A"
                        v.next_event_time = t
                        continue

                    # Don't override the locked storage for a priority or JMP-locked vessel
                    if not v.resumption_priority and not getattr(v, "_jmp_override_locked", False):
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
                    if (not v.resumption_priority
                            and not getattr(v, "_jmp_override_locked", False)
                            and _assigned_stk < _assigned_thr * 0.5):
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

                    # Hard active-berth lock: block immediately if another vessel
                    # is physically berthed, connecting hoses, or actively loading —
                    # regardless of what storage_berth_free_at says.
                    # This is the primary guard against double-berthing at a storage.
                    _wb_active_lock = self.storage_locked_by_active_berth(
                        selected_storage, requesting_vessel=v.name)
                    # Priority vessels bypass the inter-berth serial gap but still
                    # respect the active-berth lock and storage_berth_free_at.
                    if v.resumption_priority:
                        berth_now_ok = (
                            not _wb_active_lock
                            and self.is_valid_berthing_time(t, point=selected_point)
                            and t >= self.storage_berth_free_at[selected_storage]
                        )
                    else:
                        berth_now_ok = (
                            not _wb_active_lock
                            and self.is_valid_berthing_time(t, point=selected_point)
                            and t >= self.storage_berth_free_at[selected_storage]
                            and t >= self.next_storage_berthing_start_at[selected_point]
                        )
                    if not berth_now_ok:
                        next_check = self.next_daylight_hourly_berth_check(t, point=selected_point)
                        v.next_event_time = next_check
                        _lock_reason = " (berth physically occupied)" if _wb_active_lock else ""
                        self.log_event(
                            t,
                            v.name,
                            "WAITING_BERTH_A",
                            f"Berth still unavailable at {selected_storage}{_lock_reason}; hourly daylight recheck at "
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
                    # Stamp the voyage code on the vessel for referencing discharge assignment
                    v.voyage_code = make_voyage_code(v.name, v.current_voyage)
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
                    # Clear JMP lock — override has been honoured
                    v._jmp_override_locked = False
                    v._jmp_load_after_hour = None
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
                    elif v.target_point == "F":
                        # Casting off from Ibom (Point F) with partial cargo —
                        # sail directly to BIA (same leg distance as B→F = 3h).
                        # Reuse SAILING_AB_LEG2 which arrives at BIA and triggers
                        # the normal Point B berthing/discharge flow.
                        v.status = "SAILING_AB_LEG2"
                        v.next_event_time = sail_t + SAIL_HOURS_B_TO_F
                        # Clear Point F target so return allocation sends to Point A
                        v.target_point = "B"
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
                            f"{name}: {self.mother_bbl[name]:,.0f}/{self.mother_capacity_bbl(name):,} bbl"
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
                            # ── Daughter discharge point override ──────────────
                            # DAUGHTER_DISCHARGE_OVERRIDES: {voyage_code: {"vessel", "mother",
                            #   "discharge_date"}} or legacy {vessel: {day: mother}}.
                            # ZeeZee is unaffected (handled by _run_zeezee separately).
                            _ddo_mother, _ddo_disc_date = self._resolve_discharge_override(
                                v.name, getattr(v, "voyage_code", ""), arrival)
                            _candidate_mothers = {m for _, _, m in candidates}
                            if _ddo_mother:
                                # ── Date-hold: vessel arrived early — wait at BIA ──
                                if not self._discharge_override_date_reached(_ddo_disc_date, arrival):
                                    # Calculate sim-hour of 08:00 on the discharge date
                                    from datetime import datetime as _ddt
                                    _disc_dt = _ddt.fromisoformat(_ddo_disc_date).replace(hour=8, minute=0)
                                    _hold_until = (_disc_dt - _SIM_EPOCH).total_seconds() / 3600.0
                                    _hold_until = max(_hold_until, arrival)
                                    v.status = "WAITING_BERTH_B"
                                    v.assigned_mother = _ddo_mother
                                    v.next_event_time = _hold_until
                                    self.log_event(
                                        arrival, v.name, "WAITING_BERTH_B",
                                        f"Discharge override [{v.voyage_code}]: holding at BIA until "
                                        f"{_ddo_disc_date} to discharge to {_ddo_mother} — "
                                        f"vessel arrived early; wait until "
                                        f"{self.hours_to_dt(_hold_until).strftime('%Y-%m-%d %H:%M')}",
                                        voyage_num=v.current_voyage, mother=_ddo_mother,
                                    )
                                    continue
                                # ── Date reached: displace incumbent and berth ──
                                if _ddo_mother in _candidate_mothers:
                                    self._displace_incumbent_at_mother(_ddo_mother, arrival)
                                    selected_mother = _ddo_mother
                                    # Recompute candidates after displacement
                                    _, candidates = self.point_b_candidate_slots(v, arrival)
                                    selected = next(
                                        (x for x in candidates if x[2] == _ddo_mother),
                                        None,
                                    )
                                    if selected is None:
                                        selected = (arrival, arrival, _ddo_mother)
                                    self.log_event(
                                        arrival, v.name, "MOTHER_PRIORITY_ASSIGNMENT",
                                        f"Discharge override [{v.voyage_code}]: forced to "
                                        f"{_ddo_mother} on {_ddo_disc_date} — incumbent displaced",
                                        voyage_num=v.current_voyage, mother=_ddo_mother,
                                    )
                                else:
                                    # Mother not at BIA (e.g. at export) — wait for 30-min rescan
                                    v.next_event_time = self.next_daylight_hourly_berth_check(arrival, point="B")
                                    self.log_event(
                                        arrival, v.name, "WAITING_BERTH_B",
                                        f"Discharge override [{v.voyage_code}]: target {_ddo_mother} "
                                        f"not available on {self.hours_to_dt(arrival).strftime('%Y-%m-%d')}; "
                                        f"rescan in 30 min at "
                                        f"{self.hours_to_dt(v.next_event_time).strftime('%Y-%m-%d %H:%M')}",
                                        voyage_num=v.current_voyage, mother=_ddo_mother,
                                    )
                                    v.status = "WAITING_BERTH_B"
                                    v.assigned_mother = _ddo_mother
                                    continue
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
                                f"Assigned to {selected_mother}; rescan every 30 min until berth opens "
                                f"(earliest {self.hours_to_dt(start).strftime('%Y-%m-%d %H:%M')})",
                                voyage_num=v.current_voyage,
                                mother=selected_mother,
                            )
                        else:
                            v.status = "BERTHING_B"
                            _disch_rate_3 = VESSEL_DISCHARGE_RATE_BPH.get(v.name)
                            _disch_hrs_3 = (v.cargo_bbl / _disch_rate_3) if _disch_rate_3 else DISCHARGE_HOURS
                            _discharge_end = (
                                start + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + _disch_hrs_3
                            )
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

                    # ── MTO transient vessel: opportunistic offload priority ───
                    # A transient vessel tries to berth at a primary mother on
                    # every hourly check — as soon as a window opens it takes it.
                    # It does NOT wait for "day+1"; the capacity ceiling and parcel
                    # count control only when MORE shuttles can top it up, never
                    # when it may discharge.  Priority is absolute: it displaces
                    # any incumbent at the best available berth.
                    _mto_since = getattr(v, "_mto_transient_since_day", None)
                    if _mto_since is not None:
                        # Find the best primary mother — any mother with ANY space.
                        # The old guard (space < v.cargo_bbl) was wrong: it froze
                        # vessels indefinitely when no mother had full-cargo space.
                        # The transient discharges WHATEVER FITS, not necessarily all
                        # at once. Transfer is clamped at HOSE_CONNECT_B to available
                        # space so the mother never overflows.
                        _mto_best_mother = None
                        _mto_best_start  = None
                        for _mn in MOTHER_NAMES:
                            if _mn == MOTHER_QUATERNARY_NAME:
                                continue
                            if not self.mother_is_at_point_b(_mn, decision_t):
                                continue
                            # Skip mothers preparing to sail — they will be
                            # absent before the transient can complete discharge
                            if self.export_state.get(_mn) in {"DOC", "SAILING"}:
                                continue
                            _space = self.mother_capacity_bbl(_mn) - self.mother_bbl[_mn]
                            # MTO single-operation rule: mother must have space
                            # for the transient's FULL on-board cargo volume.
                            # Partial discharge is prohibited by regulatory policy.
                            if _space < v.cargo_bbl:
                                continue
                            _slot = self.next_berthing_window(
                                max(decision_t,
                                    self.mother_berth_free_at[_mn],
                                    self.mother_available_at[_mn]),
                                point="B",
                            )
                            if _mto_best_start is None or _slot < _mto_best_start:
                                _mto_best_start  = _slot
                                _mto_best_mother = _mn
                        if _mto_best_mother is not None:
                            # Berth is available — displace incumbent and claim it
                            self._displace_incumbent_at_mother(_mto_best_mother, decision_t)
                            v.assigned_mother = _mto_best_mother
                            _disch_rate_mto = VESSEL_DISCHARGE_RATE_BPH.get(v.name)
                            _disch_hrs_mto  = (v.cargo_bbl / _disch_rate_mto) if _disch_rate_mto else DISCHARGE_HOURS
                            _discharge_end_mto = (
                                _mto_best_start + BERTHING_DELAY_HOURS
                                + HOSE_CONNECTION_HOURS + _disch_hrs_mto
                            )
                            self.mother_berth_free_at[_mto_best_mother] = max(
                                self.mother_berth_free_at[_mto_best_mother],
                                _discharge_end_mto,
                            )
                            v.status = "BERTHING_B"
                            v.next_event_time = _mto_best_start + BERTHING_DELAY_HOURS
                            v._mto_transient_since_day  = None   # clear transient flag
                            v._mto_parcels_received     = 0      # reset parcel counter
                            v._is_mto_offload           = True   # mark for voyage code suffix
                            _cur_day_key = int((decision_t + SIM_HOUR_OFFSET) // 24)
                            self.log_event(
                                decision_t, v.name, "MTO_TRANSIENT_PRIORITY_BERTH",
                                f"[MTO] Transient offloading at {_mto_best_mother} "
                                f"(Day {_cur_day_key+1}) — {v.cargo_bbl:,.0f} bbl on board | "
                                f"berth at {self.hours_to_dt(_mto_best_start).strftime('%H:%M')}",
                                voyage_num=v.current_voyage, mother=_mto_best_mother,
                            )
                            continue
                        else:
                            # No qualifying mother yet — recheck every TIME_STEP_HOURS
                            # during daylight so we catch returning mothers immediately
                            # rather than waiting up to an hour.
                            _wall_now = (decision_t + SIM_HOUR_OFFSET) % 24
                            if DAYLIGHT_START <= _wall_now < DAYLIGHT_END:
                                _next_mto = decision_t + TIME_STEP_HOURS
                            else:
                                _next_mto = self.next_daylight_hourly_berth_check(decision_t, point="B")
                            v.next_event_time = _next_mto
                            continue

                    _, candidates = self.point_b_candidate_slots(v, decision_t)
                    if not candidates:
                        next_recheck = self.next_daylight_hourly_berth_check(decision_t, point="B")
                        v.next_event_time = next_recheck
                        self.log_event(
                            decision_t,
                            v.name,
                            "WAITING_MOTHER_CAPACITY",
                            f"No Point B mother currently feasible; rescan in 30 min at "
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
                        # ── Daughter discharge point override (hourly reassessment) ──
                        # ZeeZee is unaffected — she never enters WAITING_BERTH_B
                        # via this path (handled entirely by _run_zeezee).
                        _ddo_mother, _ddo_disc_date = self._resolve_discharge_override(
                            v.name, getattr(v, "voyage_code", ""), decision_t)
                        _candidate_mothers = {m for _, _, m in candidates}
                        if _ddo_mother:
                            # ── Still holding for target date? ─────────────────
                            if not self._discharge_override_date_reached(_ddo_disc_date, decision_t):
                                from datetime import datetime as _ddt
                                _disc_dt = _ddt.fromisoformat(_ddo_disc_date).replace(hour=8, minute=0)
                                _hold_until = (_disc_dt - _SIM_EPOCH).total_seconds() / 3600.0
                                _hold_until = max(_hold_until, decision_t)
                                v.next_event_time = _hold_until
                                self.log_event(
                                    decision_t, v.name, "WAITING_BERTH_B",
                                    f"Discharge override [{v.voyage_code}]: holding at BIA until "
                                    f"{_ddo_disc_date} to discharge to {_ddo_mother}",
                                    voyage_num=v.current_voyage, mother=_ddo_mother,
                                )
                                continue
                            # ── Date reached — displace incumbent and berth ───
                            if _ddo_mother in _candidate_mothers:
                                self._displace_incumbent_at_mother(_ddo_mother, decision_t)
                                # Recompute candidates after displacement
                                _, candidates = self.point_b_candidate_slots(v, decision_t)
                                _candidate_mothers = {m for _, _, m in candidates}
                                if _ddo_mother not in _candidate_mothers:
                                    # Mother not at BIA (export/unavailable) — keep waiting
                                    next_recheck = self.next_daylight_hourly_berth_check(decision_t, point="B")
                                    v.next_event_time = next_recheck
                                    self.log_event(
                                        decision_t, v.name, "WAITING_BERTH_B",
                                        f"Discharge override [{v.voyage_code}]: {_ddo_mother} not at BIA; "
                                        f"reassessing at {self.hours_to_dt(next_recheck).strftime('%Y-%m-%d %H:%M')}",
                                        voyage_num=v.current_voyage, mother=_ddo_mother,
                                    )
                                    continue
                                selected_mother = _ddo_mother
                                selected = next(
                                    (x for x in candidates if x[2] == _ddo_mother),
                                    (decision_t, decision_t, _ddo_mother),
                                )
                                if _ddo_mother != v.assigned_mother:
                                    self.log_event(
                                        decision_t, v.name, "MOTHER_PRIORITY_ASSIGNMENT",
                                        f"Discharge override [{v.voyage_code}]: forced to "
                                        f"{_ddo_mother} on {_ddo_disc_date} — incumbent displaced",
                                        voyage_num=v.current_voyage, mother=_ddo_mother,
                                    )
                            else:
                                # Mother not available yet — keep waiting
                                next_recheck = self.next_daylight_hourly_berth_check(decision_t, point="B")
                                v.next_event_time = next_recheck
                                self.log_event(
                                    decision_t, v.name, "WAITING_BERTH_B",
                                    f"Discharge override [{v.voyage_code}]: awaiting {_ddo_mother} — "
                                    f"not currently feasible; next rescan "
                                    f"{self.hours_to_dt(next_recheck).strftime('%Y-%m-%d %H:%M')}",
                                    voyage_num=v.current_voyage, mother=_ddo_mother,
                                )
                                continue
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
                    _prev_mother = v.assigned_mother
                    _mother_changed = selected_mother != _prev_mother and _prev_mother in MOTHER_NAMES
                    if _mother_changed:
                        self.log_event(
                            decision_t,
                            v.name,
                            "MOTHER_PRIORITY_ASSIGNMENT",
                            f"Point B rescan reallocated: {_prev_mother} → {selected_mother} "
                            f"(berth freed — earlier slot available)",
                            voyage_num=v.current_voyage,
                            mother=selected_mother,
                        )
                    v.assigned_mother = selected_mother

                    if start > decision_t + 0.01:
                        next_recheck = self.next_daylight_hourly_berth_check(decision_t, point="B")
                        v.next_event_time = next_recheck
                        # Only log when mother changed or this is the first check
                        # (half-step scanning generates too many identical log entries)
                        _last_log_start = getattr(v, "_wb_last_logged_start", None)
                        _last_log_mother = getattr(v, "_wb_last_logged_mother", None)
                        if _mother_changed or _last_log_start != start or _last_log_mother != selected_mother:
                            self.log_event(
                                decision_t,
                                v.name,
                                "WAITING_BERTH_B",
                                f"Awaiting berth at {selected_mother}; earliest {self.hours_to_dt(start).strftime('%Y-%m-%d %H:%M')}, "
                                f"next rescan {self.hours_to_dt(next_recheck).strftime('%Y-%m-%d %H:%M')}",
                                voyage_num=v.current_voyage,
                                mother=selected_mother,
                            )
                            v._wb_last_logged_start  = start
                            v._wb_last_logged_mother = selected_mother
                        continue

                    v.status = "BERTHING_B"
                    _disch_rate_4 = VESSEL_DISCHARGE_RATE_BPH.get(v.name)
                    _disch_hrs_4 = (v.cargo_bbl / _disch_rate_4) if _disch_rate_4 else DISCHARGE_HOURS
                    _discharge_end = (
                        start + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + _disch_hrs_4
                    )
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
                    if v.assigned_mother not in MOTHER_NAMES:
                        self.log_event(t, v.name, "WAITING_MOTHER_CAPACITY",
                                       "Blocked: no explicit mother assignment at Point B (fallback disabled)",
                                       voyage_num=v.current_voyage)
                        v.next_event_time = t + 0.5
                        continue
                    if not self.mother_is_at_point_b(v.assigned_mother, t):
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
                    if v.assigned_mother not in MOTHER_NAMES:
                        # No valid mother assignment — reset to WAITING_BERTH_B
                        # so the candidate-selection logic can assign one.
                        v.status = "WAITING_BERTH_B"
                        v.next_event_time = self.next_daylight_hourly_berth_check(t, point="B")
                        self.log_event(t, v.name, "WAITING_BERTH_B",
                                       "No mother assignment at HOSE_CONNECT_B — "
                                       "returning to WAITING_BERTH_B for reassignment",
                                       voyage_num=v.current_voyage)
                        continue
                    selected_mother = v.assigned_mother
                    if not self.mother_is_at_point_b(selected_mother, t):
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
                    _mother_cap = self.mother_capacity_bbl(selected_mother)
                    _mother_space = max(0.0, _mother_cap - self.mother_bbl[selected_mother])
                    _is_mto_vessel = getattr(v, "_is_mto_offload", False)

                    if _mother_space <= 0:
                        # Mother is completely full — wait and retry
                        self.log_event(t, v.name, "WAITING_MOTHER_CAPACITY",
                                       f"Cannot start discharge - {selected_mother} lacks space",
                                       voyage_num=v.current_voyage)
                        v.next_event_time = t + 6
                        continue

                    if _is_mto_vessel and _mother_space < v.cargo_bbl:
                        # ── REGULATORY GATE: MTO single-operation rule ────────────
                        # All consolidated MTO cargo must be offloaded to a single
                        # mother in one uninterrupted berthing operation.
                        # Partial discharge is strictly prohibited.
                        # Cast off, restore MTO transient flag, and re-anchor to
                        # await a mother vessel with sufficient space.
                        cast_off_t = self.next_cast_off_window(t)
                        v.status = "CAST_OFF_B"
                        v.next_event_time = cast_off_t + CAST_OFF_HOURS
                        # Restore MTO transient state so the priority berth
                        # handler keeps seeking a qualifying mother each tick
                        v._is_mto_offload = True
                        _day_key = int((t + SIM_HOUR_OFFSET) // 24)
                        v._mto_transient_since_day = _day_key
                        self.log_event(
                            t, v.name, "MTO_ABORT_INSUFFICIENT_SPACE",
                            f"[MTO] Regulatory abort at {selected_mother} — "
                            f"space available {_mother_space:,.0f} bbl < "
                            f"full cargo {v.cargo_bbl:,.0f} bbl. "
                            f"Partial discharge prohibited. Re-anchoring to await "
                            f"a mother with sufficient capacity.",
                            voyage_num=v.current_voyage, mother=selected_mother,
                        )
                        continue

                    _actual_discharge = v.cargo_bbl   # always discharge full volume
                    # Blend vessel cargo API into mother vessel.
                    _vessel_api_val = self.vessel_api.get(v.name, 0.0)
                    self.mother_api[selected_mother] = self.blend_api(
                        self.mother_bbl[selected_mother], self.mother_api.get(selected_mother, 0.0),
                        _actual_discharge, _vessel_api_val)
                    self.mother_bbl[selected_mother] += _actual_discharge
                    v.cargo_bbl -= _actual_discharge
                    if v.cargo_bbl <= 0:
                        v.cargo_bbl = 0
                        self.vessel_api[v.name] = 0.0
                    v.status = "DISCHARGING"
                    _disch_rate = VESSEL_DISCHARGE_RATE_BPH.get(v.name)
                    _disch_hrs = (_actual_discharge / _disch_rate) if _disch_rate else DISCHARGE_HOURS
                    self.mother_berth_free_at[selected_mother] = max(
                        self.mother_berth_free_at[selected_mother], t + _disch_hrs
                    )
                    v.next_event_time = t + _disch_hrs
                    # MTO transient offload: stamp "A" suffix on VoyageCode
                    # (e.g. AMY-000 → AMY-000A) so JMP can distinguish
                    # transient offloads from normal cargo deliveries.
                    _is_mto = getattr(v, "_is_mto_offload", False)
                    _log_vcode = v.current_voyage
                    if _is_mto:
                        _base_vcode = make_voyage_code(v.name, v.current_voyage)
                        v.voyage_code = _base_vcode + "A"
                    self.log_event(t, v.name, "DISCHARGE_START",
                                   f"{'[MTO offload] ' if _is_mto else ''}"
                                   f"Discharging {_actual_discharge:,} bbl @ {_vessel_api_val:.2f}° API | "
                                   f"{selected_mother}: {self.mother_bbl[selected_mother]:,.0f} bbl "
                                   f"(blended {self.mother_api[selected_mother]:.2f}° API)"
                                   + (f" | {v.cargo_bbl:,.0f} bbl residual remaining on vessel" if v.cargo_bbl > 0 else ""),
                                   voyage_num=_log_vcode,
                                   mother=selected_mother)

                elif v.status == "DISCHARGING":
                    if v.assigned_mother not in MOTHER_NAMES:
                        # No valid mother assignment — cast off and return to load
                        cast_off_t = self.next_cast_off_window(t)
                        v.status = "CAST_OFF_B"
                        v.next_event_time = cast_off_t + CAST_OFF_HOURS
                        self.log_event(t, v.name, "CAST_OFF_START_B",
                                       "No mother assignment at DISCHARGING — casting off",
                                       voyage_num=v.current_voyage)
                        continue
                    selected_mother = v.assigned_mother

                    # If vessel still has residual cargo after discharge cycle,
                    # determine correct action based on whether this is MTO.
                    if v.cargo_bbl > 0:
                        if getattr(v, "_is_mto_offload", False):
                            # MTO transient: regulatory prohibition on partial offload.
                            # This path should not be reached (HOSE_CONNECT_B aborts
                            # before a partial pump starts), but as a safety net:
                            # cast off, restore MTO transient flag, re-anchor.
                            cast_off_t = self.next_cast_off_window(t)
                            v.status = "CAST_OFF_B"
                            v.next_event_time = cast_off_t + CAST_OFF_HOURS
                            _day_key = int((t + SIM_HOUR_OFFSET) // 24)
                            v._mto_transient_since_day = _day_key
                            self.log_event(
                                t, v.name, "MTO_ABORT_INSUFFICIENT_SPACE",
                                f"[MTO] Safety net: partial discharge detected with "
                                f"{v.cargo_bbl:,.0f} bbl residual — re-anchoring. "
                                f"Partial discharge prohibited per regulatory policy.",
                                voyage_num=v.current_voyage,
                            )
                            continue
                        else:
                            # Non-MTO vessel with residual (edge case): cast off normally.
                            # This can occur when a mother fills up mid-pump on a
                            # normal cargo delivery. The vessel returns and the
                            # residual stays on board for the next BIA trip.
                            cast_off_t = self.next_cast_off_window(t)
                            v.status = "CAST_OFF_B"
                            v.next_event_time = cast_off_t + CAST_OFF_HOURS
                            self.log_event(
                                t, v.name, "DISCHARGE_PARTIAL_COMPLETE",
                                f"Mother filled: {v.cargo_bbl:,.0f} bbl residual retained on board",
                                voyage_num=v.current_voyage,
                            )
                            continue

                    v.cargo_bbl = 0
                    self.vessel_api[v.name] = 0.0
                    # Clear MTO offload flag and restore normal voyage code
                    if getattr(v, "_is_mto_offload", False):
                        v._is_mto_offload = False
                        v.voyage_code = make_voyage_code(v.name, v.current_voyage)
                    # Track how many daughters have fully discharged to SanJulian
                    # since her last transload cycle — she must receive at least
                    # two before she may offload (T2/T3/T4).
                    if selected_mother == MOTHER_QUATERNARY_NAME:
                        self.sanjulian_daughters_loaded += 1
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
                    # MTO discharger: vessel cast off from anchor after transferring
                    # its cargo to the transient — it was never physically berthed,
                    # so assigned_mother may be None. Skip the mother check and go
                    # straight to WAITING_RETURN_STOCK.
                    if v.assigned_mother not in MOTHER_NAMES:
                        if v.cargo_bbl == 0:
                            # Casting off from anchor after MTO transfer — no mother
                            self.log_event(t, v.name, "CAST_OFF_COMPLETE_B",
                                           "Cast-off complete (MTO discharger — no berth occupied)",
                                           voyage_num=v.current_voyage)
                            v.status = "WAITING_RETURN_STOCK"
                            v.next_event_time = t
                            continue
                        # No mother but still has cargo — send to WAITING_BERTH_B for reassignment
                        v.status = "WAITING_BERTH_B"
                        v.next_event_time = self.next_daylight_hourly_berth_check(t, point="B")
                        self.log_event(t, v.name, "WAITING_BERTH_B",
                                       "No mother assignment at CAST_OFF_B — returning to WAITING_BERTH_B",
                                       voyage_num=v.current_voyage)
                        continue
                    selected_mother = v.assigned_mother
                    # SanJulian never triggers the export state machine — it
                    # transloads to primary mothers instead (see _run_sanjulian_transload)
                    # Only mark export_ready when the mother has reached its
                    # export trigger.  Setting it unconditionally after every
                    # cast-off was causing Bryanston to start an export cycle
                    # after receiving just one daughter cargo (85k), far below
                    # the 465k trigger.
                    if selected_mother != MOTHER_QUATERNARY_NAME:
                        _trigger = self.mother_export_trigger_bbl(selected_mother)
                        if self.mother_bbl[selected_mother] >= _trigger:
                            if not self.export_ready[selected_mother]:
                                self.export_ready_since[selected_mother] = t
                            self.export_ready[selected_mother] = True
                    self.log_event(t, v.name, "CAST_OFF_COMPLETE_B",
                                   "Cast-off from mother complete; returning to storage",
                                   voyage_num=v.current_voyage)

                    # MTO transient re-anchor: if this cast-off was triggered by
                    # an abort (insufficient mother space), the vessel still holds
                    # its consolidated cargo and must return to WAITING_BERTH_B to
                    # seek a qualifying mother — not sail back to storage.
                    if getattr(v, "_mto_transient_since_day", None) is not None:
                        v.status = "WAITING_BERTH_B"
                        v.next_event_time = self.next_daylight_hourly_berth_check(t, point="B")
                        self.log_event(
                            t, v.name, "MTO_REANCHOR",
                            f"[MTO] Re-anchoring at BIA with {v.cargo_bbl:,.0f} bbl on board — "
                            f"awaiting a primary mother with sufficient space for full cargo",
                            voyage_num=v.current_voyage,
                        )
                        continue

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
                            and (self.point_f_active_loader != v.name
                                 or v.target_point != "F")
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
                    elif v.target_point in ("E", "G"):
                        # Starturn (E) / PGM (G) — short direct return 3h, no breakwater
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
                    # Also redirect if this vessel IS the active Ibom loader but has
                    # just returned from delivering a partial Ibom cargo (target_point
                    # was set to "B" when casting off from Point F — it is no longer
                    # "F", meaning it needs a full Point A cycle before returning).
                    if (v.name in self.point_f_vessels
                            and self.point_f_swap_pending_for != v.name
                            and (self.point_f_active_loader != v.name
                                 or v.target_point != "F")):
                        v.target_point = "A"
                    self.log_event(t, v.name, "ARRIVED_LOADING_POINT",
                                   f"Arrived Point {v.target_point} storage area — ready for next cycle",
                                   voyage_num=v.current_voyage)
                    v.next_event_time = t

            # 2b. Check mother unavailability windows — log transitions and
            #     reserve berth when a window ends so daughters don't try to
            #     berth before the mother is fully settled back.
            for _uname, _windows in self.mother_unavailability_windows.items():
                for _ws, _we in _windows:
                    # Log entry into window (once, at the first step inside)
                    if abs(t - _ws) < TIME_STEP_HOURS * 0.5:
                        self.log_event(t, _uname, "MOTHER_UNAVAILABLE_START",
                                       f"{_uname} entering scheduled unavailability window — "
                                       f"unavailable until "
                                       f"{self.hours_to_dt(_we).strftime('%Y-%m-%d %H:%M')}")
                        # Reserve berth through the whole window so no daughter
                        # is assigned until the window ends
                        self.mother_berth_free_at[_uname] = max(
                            self.mother_berth_free_at.get(_uname, 0.0), _we
                        )
                        self.mother_available_at[_uname] = max(
                            self.mother_available_at.get(_uname, 0.0), _we
                        )
                    # Log exit from window (once, at the first step after)
                    if abs(t - _we) < TIME_STEP_HOURS * 0.5:
                        self.log_event(t, _uname, "MOTHER_UNAVAILABLE_END",
                                       f"{_uname} resuming normal operations after scheduled "
                                       f"unavailability window")

            # 3. Advance mother export state machines independently
            active_export_mother = next(
                (name for name in MOTHER_NAMES if self.export_state[name] is not None),
                None,
            )

            # ── Forced export override ─────────────────────────────────────────
            # Operator can schedule a specific mother to sail on a particular day
            # via EXPORT_FORCE_SCHEDULE.  When the sim clock crosses a forced
            # departure hour the mother is put into DOC state immediately,
            # bypassing export_ready and departure-eligibility tests.
            # Constraints honoured: no active daughter discharge, daylight window,
            # no other export already active (one export at a time).
            if active_export_mother is None:
                for _fm, _fhours in EXPORT_FORCE_SCHEDULE.items():
                    if _fm == MOTHER_QUATERNARY_NAME:
                        continue   # SanJulian never exports
                    if self.export_state[_fm] is not None:
                        continue   # already in an export cycle
                    if not self.mother_is_at_point_b(_fm, t):
                        continue   # mother is away
                    for _fh in sorted(_fhours):
                        # Fire when the sim clock has just passed the target hour
                        # (within one timestep tolerance)
                        if t - TIME_STEP_HOURS < _fh <= t:
                            _fdaughter_active = any(
                                vv.assigned_mother == _fm and
                                vv.status in {"BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING"}
                                for vv in self.vessels
                            )
                            if _fdaughter_active:
                                self.log_event(t, _fm, "EXPORT_FORCE_WAIT_DISCHARGE",
                                               f"Forced export override: waiting for active daughter "
                                               f"discharge to complete before starting DOC")
                                # Reschedule by one timestep so the loop retries
                                EXPORT_FORCE_SCHEDULE[_fm] = [
                                    h if h != _fh else t + TIME_STEP_HOURS
                                    for h in _fhours
                                ]
                                break
                            wall_h = (t + SIM_HOUR_OFFSET) % 24
                            if not (DAYLIGHT_START <= wall_h < DAYLIGHT_END):
                                # Reschedule to next daylight tick
                                next_light = self.next_daylight_sail(t)
                                EXPORT_FORCE_SCHEDULE[_fm] = [
                                    h if h != _fh else next_light
                                    for h in _fhours
                                ]
                                self.log_event(t, _fm, "EXPORT_FORCE_WAIT_DAYLIGHT",
                                               f"Forced export override: waiting for daylight at "
                                               f"{self.hours_to_dt(next_light).strftime('%Y-%m-%d %H:%M')}")
                                break
                            # All clear — force DOC now
                            self.export_state[_fm]       = "DOC"
                            self.export_ready[_fm]       = False
                            self.export_ready_since[_fm] = None
                            self.export_end_time[_fm]    = t + EXPORT_DOC_HOURS
                            # ── Block the berth immediately ───────────────────
                            # Set mother_berth_free_at to a far-future value so
                            # no new daughter can claim this berth while the
                            # mother is in DOC / SAILING / HOSE / IN_PORT.
                            # Daughters already berthed/discharging finish
                            # normally (they hold their own next_event_time).
                            # The berth is released when the mother returns
                            # (mother_available_at is set by the return logic).
                            _lock_until = t + (EXPORT_DOC_HOURS
                                               + EXPORT_SAIL_HOURS
                                               + EXPORT_HOSE_HOURS
                                               + (self.mother_bbl[_fm]
                                                  / max(1, EXPORT_RATE_BPH))
                                               + EXPORT_SAIL_HOURS + 2.0)
                            self.mother_berth_free_at[_fm] = max(
                                self.mother_berth_free_at.get(_fm, 0.0),
                                _lock_until,
                            )
                            self.log_event(t, _fm, "EXPORT_DOC_START",
                                           f"FORCED export departure override — "
                                           f"documentation ({EXPORT_DOC_HOURS}h) | "
                                           f"Stock: {self.mother_bbl[_fm]:,.0f} bbl | "
                                           f"Berth locked until return")
                            active_export_mother = _fm   # block normal selection below
                            break
                    if active_export_mother is not None:
                        break
            if active_export_mother is None and t >= self.next_export_allowed_at:
                # ── Proactive trigger check ───────────────────────────────────
                # In case a mother reached her export trigger via SanJulian
                # transload or partial discharge without going through CAST_OFF_B
                # (which is the normal path), ensure export_ready is set here.
                for _mn in MOTHER_NAMES:
                    if _mn == MOTHER_QUATERNARY_NAME:
                        continue
                    if (self.export_state[_mn] is None
                            and not self.export_ready[_mn]
                            and self.mother_bbl[_mn] >= self.mother_export_trigger_bbl(_mn)):
                        self.export_ready[_mn] = True
                        if not self.export_ready_since[_mn]:
                            self.export_ready_since[_mn] = t

                ready_candidates = []
                for mother_name in MOTHER_NAMES:
                    # SanJulian never exports — it transloads to primary mothers instead
                    if mother_name == MOTHER_QUATERNARY_NAME:
                        continue
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
                        # Lock the berth for the full export round-trip so that
                        # no new daughter can berth while she is away.  Daughters
                        # already mid-discharge finish normally; no new ones start.
                        _nat_lock = t + (EXPORT_DOC_HOURS
                                         + EXPORT_SAIL_HOURS
                                         + EXPORT_HOSE_HOURS
                                         + (self.mother_bbl[selected_export_mother]
                                            / max(1, EXPORT_RATE_BPH))
                                         + EXPORT_SAIL_HOURS + 2.0)
                        self.mother_berth_free_at[selected_export_mother] = max(
                            self.mother_berth_free_at.get(selected_export_mother, 0.0),
                            _nat_lock,
                        )
                        self.log_event(t, selected_export_mother, "EXPORT_DOC_START",
                                       f"Export documentation ({EXPORT_DOC_HOURS}h) | "
                                       f"Berth locked until return")
                    else:
                        next_light = self.next_daylight_sail(t)
                        if next_light > t:
                            self.log_event(t, selected_export_mother, "EXPORT_WAIT_DAYLIGHT",
                                           f"Export ready but waiting for daylight at "
                                           f"{self.hours_to_dt(next_light).strftime('%Y-%m-%d %H:%M')}")

            for mother_name in MOTHER_NAMES:
                # SanJulian has no export state — handled by transload machinery
                if mother_name == MOTHER_QUATERNARY_NAME:
                    continue

                state = self.export_state[mother_name]
                if state == "DOC":
                    if t >= self.export_end_time[mother_name]:
                        daughter_active_here = any(
                            vv.assigned_mother == mother_name
                            and vv.status in {"BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING"}
                            for vv in self.vessels
                        )
                        # Hard timeout: never wait more than 24h after DOC completes.
                        # Without this, a continuous stream of daughters keeps the
                        # mother in DOC forever and the export never sails.
                        _doc_complete_t = self.export_start_time.get(mother_name) or t
                        _waited = t - (_doc_complete_t + EXPORT_DOC_HOURS)
                        if daughter_active_here and _waited < 24.0:
                            self.export_end_time[mother_name] = t + TIME_STEP_HOURS
                            self.log_event(
                                t, mother_name, "EXPORT_WAIT_DISCHARGE",
                                "Export docs complete but waiting for active daughter discharge operations",
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
                        _fender_done = return_arrival + 2
                        # Release the berth lock so daughters can berth again
                        # once fendering is complete.  Use direct assignment —
                        # the old min() kept the far-future DOC-phase lock value,
                        # which prevented any new daughters from ever berthing.
                        self.mother_berth_free_at[mother_name] = _fender_done
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

            # 3b. Advance SanJulian transload state machine
            # SanJulian never exports — instead it pumps inventory to primary
            # mothers whenever one of the four trigger conditions is satisfied.
            self._run_sanjulian_transload(t)

            # 3c. Advance ZeeZee third-party discharge state machine
            # Runs after SanJulian so both compete for the same berth slots
            # with ZeeZee given deadline-enforced priority.
            self._run_zeezee(t)

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
            # Pre-cache repeated lookups to avoid redundant function calls
            # and dict traversals per step (~4,320 steps per 90-day run).
            _t_dt   = self.hours_to_dt(t)
            _t_day  = int(t // 24) + 1
            _s_bbl  = self.storage_bbl
            _s_ovf  = self.storage_overflow_bbl
            _m_bbl  = self.mother_bbl
            _s_api  = self.storage_api
            _m_api  = self.mother_api
            _v_api  = self.vessel_api
            _ovf_total = round(_s_ovf[STORAGE_PRIMARY_NAME] + _s_ovf[STORAGE_SECONDARY_NAME]
                               + _s_ovf[STORAGE_TERTIARY_NAME] + _s_ovf[STORAGE_QUATERNARY_NAME]
                               + _s_ovf[STORAGE_QUINARY_NAME])
            vessel_statuses = {}
            for v in self.vessels:
                _vn = v.name
                vessel_statuses[_vn]                = v.status
                vessel_statuses[f"{_vn}_cargo_bbl"] = round(v.cargo_bbl)
                vessel_statuses[f"{_vn}_api"]       = round(_v_api.get(_vn, 0.0), 2)
            self.timeline.append({
                "Time"       : _t_dt,
                "Day"        : _t_day,
                "Storage_bbl": round(sum(self.storage_bbl.values())),
                "Chapel_bbl": round(_s_bbl[STORAGE_PRIMARY_NAME]),
                "JasmineS_bbl": round(_s_bbl[STORAGE_SECONDARY_NAME]),
                "Westmore_bbl": round(_s_bbl[STORAGE_TERTIARY_NAME]),
                "Duke_bbl": round(_s_bbl[STORAGE_QUATERNARY_NAME]),
                "Starturn_bbl": round(_s_bbl[STORAGE_QUINARY_NAME]),
                "PGM_bbl": round(_s_bbl[STORAGE_SENARY_NAME]),
                "Storage_Overflow_Accum_bbl": _ovf_total,
                "Chapel_Overflow_Accum_bbl": round(_s_ovf[STORAGE_PRIMARY_NAME]),
                "JasmineS_Overflow_Accum_bbl": round(_s_ovf[STORAGE_SECONDARY_NAME]),
                "Westmore_Overflow_Accum_bbl": round(_s_ovf[STORAGE_TERTIARY_NAME]),
                "Duke_Overflow_Accum_bbl": round(_s_ovf[STORAGE_QUATERNARY_NAME]),
                "Starturn_Overflow_Accum_bbl": round(_s_ovf[STORAGE_QUINARY_NAME]),
                "PGM_Overflow_Accum_bbl": round(_s_ovf[STORAGE_SENARY_NAME]),
                "PointF_Overflow_Accum_bbl": round(self.point_f_overflow_accum_bbl),
                "PointF_Active_Loading_bbl": round(self.point_f_active_loading_bbl()),
                "Mother_bbl" : round(sum(self.mother_bbl.values())),
                "Bryanston_bbl":  round(_m_bbl[MOTHER_PRIMARY_NAME]),
                "GreenEagle_bbl": round(_m_bbl[MOTHER_SECONDARY_NAME]),
                "SanJulian_bbl":  round(_m_bbl[MOTHER_QUATERNARY_NAME]),
                "SanJulian": self.sanjulian_status or "IDLE_B",
                "Total_Exported": self.total_exported,
                "Chapel_api"   : round(_s_api.get(STORAGE_PRIMARY_NAME,   0.0), 2),
                "JasmineS_api" : round(_s_api.get(STORAGE_SECONDARY_NAME, 0.0), 2),
                "Westmore_api" : round(_s_api.get(STORAGE_TERTIARY_NAME,  0.0), 2),
                "Duke_api"     : round(_s_api.get(STORAGE_QUATERNARY_NAME,0.0), 2),
                "Starturn_api" : round(_s_api.get(STORAGE_QUINARY_NAME,   0.0), 2),
                "PGM_api"      : round(_s_api.get(STORAGE_SENARY_NAME,    0.0), 2),
                "Bryanston_api":  round(_m_api.get(MOTHER_PRIMARY_NAME,    0.0), 2),
                "GreenEagle_api": round(_m_api.get(MOTHER_SECONDARY_NAME,  0.0), 2),
                "SanJulian_api":  round(_m_api.get(MOTHER_QUATERNARY_NAME, 0.0), 2),
                **vessel_statuses,
                # ZeeZee snapshot — only present when she is visiting
                **({  "ZeeZee": self.zeezee.status,
                      "ZeeZee_cargo_bbl": round(self.zeezee.cargo_bbl),
                      "ZeeZee_api": round(self.zeezee.api, 2)}
                   if self.zeezee is not None else {}),
            })

            t = round(t + TIME_STEP_HOURS, 2)

        self.final_storage_api = dict(self.storage_api)
        self.final_vessel_api  = dict(self.vessel_api)
        self.final_mother_api  = dict(self.mother_api)
        self.avg_exported_api  = (
            self.total_exported_api_bbl / self.total_exported
            if self.total_exported > 0 else 0.0
        )
        # SanJulian final state — exposed so the app can surface them in KPIs
        self.final_sanjulian_bbl        = self.mother_bbl[MOTHER_QUATERNARY_NAME]
        self.final_sanjulian_api        = self.mother_api.get(MOTHER_QUATERNARY_NAME, 0.0)
        # sanjulian_total_transloaded already accumulated during the run
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
print(f"    - {MOTHER_QUATERNARY_NAME:<9}: {sim.mother_bbl[MOTHER_QUATERNARY_NAME]:,.0f} bbl")
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
    "SantaMonica": "#6c5ce7",  # indigo family
    "Bedford"   : "#f39c12",   # amber family
    "Balham"    : "#1abc9c",   # teal family
    "Woodstock" : "#e91e63",   # pink family
    "Bagshot"   : "#00bcd4",   # cyan family
    "Watson"    : "#95a5a6",   # slate/gray family
    "Amyla"   : "#7f8c8d",   # steel gray family
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
             "Rathbone": "#9b59b6", "SantaMonica": "#6c5ce7", "Bedford": "#f39c12",
         "Balham": "#1abc9c", "Woodstock": "#e91e63", "Bagshot": "#00bcd4", "Watson": "#95a5a6", "Amyla": "#7f8c8d"}
for vname, vcap in [("Sherlock", DAUGHTER_CARGO_BBL),
                     ("Laphroaig", DAUGHTER_CARGO_BBL),
                     ("Rathbone", VESSEL_CAPACITIES.get("Rathbone", DAUGHTER_CARGO_BBL)),
                     ("SantaMonica", VESSEL_CAPACITIES.get("SantaMonica", DAUGHTER_CARGO_BBL)),
                     ("Bedford",  VESSEL_CAPACITIES.get("Bedford",  DAUGHTER_CARGO_BBL)),
                     ("Balham",   VESSEL_CAPACITIES.get("Balham",   DAUGHTER_CARGO_BBL)),
                     ("Woodstock", VESSEL_CAPACITIES.get("Woodstock", DAUGHTER_CARGO_BBL)),
             ("Bagshot",  VESSEL_CAPACITIES.get("Bagshot",  DAUGHTER_CARGO_BBL)),
             ("Watson",   VESSEL_CAPACITIES.get("Watson",   DAUGHTER_CARGO_BBL)),
             ("Amyla",  VESSEL_CAPACITIES.get("Amyla",  DAUGHTER_CARGO_BBL))]:
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
ax2.plot(timeline_df["Time"], timeline_df["GreenEagle_bbl"],
            color="#8e44ad", linewidth=1.4, alpha=0.9, label=f"{MOTHER_SECONDARY_NAME} Volume")
ax2.plot(timeline_df["Time"], timeline_df["SanJulian_bbl"],
            color="#000000", linewidth=2.0, alpha=1.0, label=f"{MOTHER_QUATERNARY_NAME} Volume")
ax2.axhline(MOTHER_EXPORT_TRIGGER_BY_NAME[MOTHER_PRIMARY_NAME], color="#e74c3c", linestyle="--", alpha=0.7,
          label=(f"{MOTHER_PRIMARY_NAME} Export Trigger "
              f"({MOTHER_EXPORT_TRIGGER_BY_NAME[MOTHER_PRIMARY_NAME]:,} bbl)"))
ax2.axhline(MOTHER_EXPORT_TRIGGER_BY_NAME[MOTHER_SECONDARY_NAME], color="#8e44ad", linestyle="--", alpha=0.7,
          label=(f"{MOTHER_SECONDARY_NAME} Export Trigger "
              f"({MOTHER_EXPORT_TRIGGER_BY_NAME[MOTHER_SECONDARY_NAME]:,} bbl)"))
ax2.axhline(MOTHER_CAPACITY_BY_NAME[MOTHER_PRIMARY_NAME], color="#922b21", linestyle="-.", alpha=0.5,
          label=f"{MOTHER_PRIMARY_NAME} Max Capacity ({MOTHER_CAPACITY_BY_NAME[MOTHER_PRIMARY_NAME]:,} bbl)")
ax2.axhline(MOTHER_CAPACITY_BY_NAME[MOTHER_SECONDARY_NAME], color="#7f8c8d", linestyle="-.", alpha=0.5,
          label=(f"{MOTHER_SECONDARY_NAME} Max Capacity "
              f"({MOTHER_CAPACITY_BY_NAME[MOTHER_SECONDARY_NAME]:,} bbl)"))
ax2.set_ylabel("Volume (bbls)", fontsize=10, color="white")
ax2.set_title(
    f"Point B Mothers ({MOTHER_PRIMARY_NAME} + {MOTHER_SECONDARY_NAME} + {MOTHER_QUATERNARY_NAME}) — Volume Level",
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

