"""
=============================================================
  OIL TANKER DAUGHTER VESSEL OPERATION — STREAMLIT DASHBOARD
=============================================================
  Run locally:
      streamlit run tanker_app.py

  Deploy to Streamlit Cloud:
      1. Push this file + requirements.txt to a GitHub repo
      2. Go to share.streamlit.io → New app → point to this file
      3. Share the public URL

  Google Sheets live sync (optional):
      - Fill in GOOGLE_SHEET_ID in the sidebar
      - Upload your service-account credentials JSON in sidebar
      - The app re-reads the sheet on every refresh cycle
=============================================================
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import colorsys
import time
import io

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Tanker Operations Dashboard",
    page_icon="🛢️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #0e1117; }
    .stMetric { background-color: #1e2130; border-radius: 8px; padding: 12px; }
    .stMetric label { color: #a0aec0 !important; font-size: 13px !important; }
    .stMetric [data-testid="stMetricValue"] { color: #e2e8f0 !important; font-size: 26px !important; }
    .stMetric [data-testid="stMetricDelta"] { font-size: 13px !important; }
    .block-container { padding-top: 1.5rem; padding-bottom: 1rem; }
    div[data-testid="stSidebarContent"] { background-color: #161b27; }
    .section-header {
        background: linear-gradient(90deg, #1a2035, #0e1117);
        border-left: 3px solid #4a9eff;
        padding: 6px 12px;
        border-radius: 4px;
        margin: 12px 0 8px 0;
        color: #e2e8f0;
        font-weight: 600;
        font-size: 15px;
    }
    .status-badge {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 12px;
        font-size: 12px;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# ── SIMULATION ENGINE  (imported inline — no separate file needed) ─────────
# =============================================================================

# ── Configuration (mirrored from simulation; editable via sidebar) ────────────
DEFAULT_CONFIG = dict(
    SIMULATION_DAYS        = 30,
    DAUGHTER_CARGO_BBL     = 85_000,
    VESSEL_NAMES           = ["Sherlock", "Laphroaig", "Rathbone", "Bedford"],
    VESSEL_CAPACITIES      = {"Rathbone": 44_000, "Bedford": 63_000},
    STORAGE_CAPACITY_BBL   = 800_000,
    MOTHER_CAPACITY_BBL    = 550_000,
    STORAGE_INIT_BBL       = 400_000,
    MOTHER_INIT_BBL        = 0,
    PRODUCTION_RATE_BPH    = 1_700,
    DEAD_STOCK_FACTOR      = 1.75,
    SAIL_HOURS_A_TO_B      = 6,
    SAIL_HOURS_B_TO_A      = 6,
    BERTHING_DELAY_HOURS   = 0.5,
    HOSE_CONNECTION_HOURS  = 2.0,
    LOAD_HOURS             = 12,
    DISCHARGE_HOURS        = 12,
    CAST_OFF_HOURS         = 0.2,
    CAST_OFF_START         = 6,
    CAST_OFF_END           = 17.5,
    BERTHING_START         = 6,
    BERTHING_END           = 18,
    DAYLIGHT_START         = 6,
    DAYLIGHT_END           = 18,
    TIME_STEP_HOURS        = 0.5,
    EXPORT_RATE_BPH        = 16_000,
    EXPORT_DOC_HOURS       = 2,
    EXPORT_SAIL_HOURS      = 6,
    EXPORT_SAIL_WINDOW_START = 6,
    EXPORT_SAIL_WINDOW_END   = 15,
    EXPORT_HOSE_HOURS       = 4,
    STORAGE_NAME           = "Chapel",
    MOTHER_NAME            = "Bryanston",
)


class DaughterVessel:
    def __init__(self, name, cargo_capacity):
        self.name           = name
        self.cargo_capacity = cargo_capacity
        self.cargo_bbl      = 0
        self.status         = "IDLE_A"
        self.next_event_time = 0.0
        self.current_voyage  = 0
        self.queue_position  = None
        self.arrival_at_b    = None
        self._voyage_assigned = False

    def __repr__(self):
        return f"{self.name}[{self.status}]"


class TankerSimulation:
    """Full discrete-event simulation, parameterised by a config dict."""

    def __init__(self, cfg: dict):
        self.cfg = cfg
        C = cfg  # shorthand

        self.storage_bbl   = C["STORAGE_INIT_BBL"]
        self.mother_bbl    = C["MOTHER_INIT_BBL"]
        self.total_exported = 0
        self.total_loaded   = 0
        self.storage_overflow_events = 0
        self.log      = []
        self.timeline = []

        self.voyage_counter       = 0
        self.loading_queue_counter = 0
        self.storage_berth_free_at = 0.0
        self.mother_berth_free_at  = 0.0
        self.mother_available_at   = 0.0
        self._mother_prioritized_at = None

        self.waiting_mother_return_queue = []
        self.loading_priority_queue      = []
        self.loading_rotation_queue      = []

        self.exporting        = False
        self.export_ready     = False
        self.export_state     = None
        self.export_start_time = None
        self.export_end_time   = None

        self.MOTHER_EXPORT_TRIGGER = (
            C["MOTHER_CAPACITY_BBL"]
            - max(C["VESSEL_CAPACITIES"].values(), default=C["DAUGHTER_CARGO_BBL"])
        )

        self.vessels = []
        for name in C["VESSEL_NAMES"]:
            cap = C["VESSEL_CAPACITIES"].get(name, C["DAUGHTER_CARGO_BBL"])
            self.vessels.append(DaughterVessel(name, cap))

    # ── helpers ──────────────────────────────────────────────────────────────
    def _dt(self, h):
        return datetime(2025, 1, 1) + timedelta(hours=h)

    def _next_daylight(self, h):
        C = self.cfg
        dh = h % 24
        if C["DAYLIGHT_START"] <= dh < C["DAYLIGHT_END"]:
            return h
        d = int(h // 24)
        return d * 24 + C["DAYLIGHT_START"] if dh < C["DAYLIGHT_START"] else (d + 1) * 24 + C["DAYLIGHT_START"]

    def _next_export_sail(self, h):
        C = self.cfg
        dh = h % 24
        if C["EXPORT_SAIL_WINDOW_START"] <= dh < C["EXPORT_SAIL_WINDOW_END"]:
            return h
        d = int(h // 24)
        return (d * 24 + C["EXPORT_SAIL_WINDOW_START"] if dh < C["EXPORT_SAIL_WINDOW_START"]
                else (d + 1) * 24 + C["EXPORT_SAIL_WINDOW_START"])

    def _next_cast_off(self, h):
        C = self.cfg
        dh = h % 24
        if C["CAST_OFF_START"] <= dh < C["CAST_OFF_END"]:
            return h
        d = int(h // 24)
        return d * 24 + C["CAST_OFF_START"] if dh < C["CAST_OFF_START"] else (d + 1) * 24 + C["CAST_OFF_START"]

    def _casting_off(self):
        return any(v.status in ("WAITING_CAST_OFF", "CAST_OFF", "CAST_OFF_B") for v in self.vessels)

    def _discharging(self):
        return any(v.status == "DISCHARGING" for v in self.vessels)

    def _valid_berth(self, h):
        C = self.cfg
        dh = h % 24
        return C["BERTHING_START"] <= dh < C["BERTHING_END"] and not self._casting_off()

    def _next_berth_window(self, h):
        if self._valid_berth(h):
            return h
        for step in range(48):
            h2 = h + step
            if self._valid_berth(h2):
                return h2
        C = self.cfg
        return (int(h // 24) + 1) * 24 + C["BERTHING_START"]

    def _log(self, t, vessel, event, detail="", voyage_num=None):
        self.log.append({
            "Time"       : self._dt(t).strftime("%Y-%m-%d %H:%M"),
            "Day"        : int(t // 24) + 1,
            "Hour"       : f"{int(t % 24):02d}:{int((t % 1) * 60):02d}",
            "Vessel"     : vessel,
            "Voyage"     : voyage_num,
            "Event"      : event,
            "Detail"     : detail,
            "Storage_bbl": round(self.storage_bbl),
            "Mother_bbl" : round(self.mother_bbl),
            "Total_Exported_bbl": self.total_exported,
        })

    # ── main loop ─────────────────────────────────────────────────────────────
    def run(self):
        C   = self.cfg
        SN  = C["STORAGE_NAME"]
        MN  = C["MOTHER_NAME"]
        total_hours = C["SIMULATION_DAYS"] * 24
        t = 0.0

        while t <= total_hours:
            # 1. Production
            prod = C["PRODUCTION_RATE_BPH"] * C["TIME_STEP_HOURS"]
            self.storage_bbl = min(self.storage_bbl + prod, C["STORAGE_CAPACITY_BBL"])
            if self.storage_bbl >= C["STORAGE_CAPACITY_BBL"]:
                self.storage_overflow_events += 1

            # 2. Prioritise mother-return queue
            if (self.mother_available_at
                    and t >= self.mother_available_at
                    and self._mother_prioritized_at != self.mother_available_at):

                if self.loading_priority_queue:
                    for v in self.loading_priority_queue:
                        v.queue_position = None
                    self.loading_priority_queue.clear()

                self.waiting_mother_return_queue.sort(
                    key=lambda v: v.arrival_at_b if v.arrival_at_b is not None else float("inf")
                )
                if self.waiting_mother_return_queue:
                    self.vessels = (
                        self.waiting_mother_return_queue
                        + [v for v in self.vessels if v not in self.waiting_mother_return_queue]
                    )
                self._mother_prioritized_at = self.mother_available_at
                try:
                    order_str = ", ".join(
                        f"{v.name}(arrived {self._dt(v.arrival_at_b).strftime('%d/%m %H:%M')})"
                        for v in self.waiting_mother_return_queue if v.arrival_at_b is not None
                    )
                    self._log(t, MN, "MOTHER_AVAILABLE_PRIORITIZE",
                              f"Mother ready; FIFO berthing order: {order_str}")
                except Exception:
                    pass

            # 3. Vessel state machines
            for v in self.vessels:
                if t < v.next_event_time:
                    continue

                # ── IDLE_A ────────────────────────────────────────────────
                if v.status == "IDLE_A":
                    if v not in self.loading_rotation_queue:
                        self.loading_rotation_queue.append(v)
                    if self.loading_rotation_queue[0] is not v:
                        v.next_event_time = t + 0.5
                        continue
                    if not v._voyage_assigned:
                        self.voyage_counter += 1
                        v.current_voyage = self.voyage_counter
                        v._voyage_assigned = True
                    cap = v.cargo_capacity

                    if self.storage_bbl >= cap:
                        dead_thresh = C["DEAD_STOCK_FACTOR"] * cap
                        if self.exporting or t < self.mother_available_at:
                            if v not in self.loading_priority_queue:
                                self.loading_queue_counter += 1
                                v.queue_position = self.loading_queue_counter
                                self.loading_priority_queue.append(v)
                                reason = "currently exporting" if self.exporting else "completing return/fendering"
                                self._log(t, v.name, "WAITING_MOTHER_UNAVAILABLE",
                                          f"Mother unavailable ({reason}); queued at position {v.queue_position}",
                                          voyage_num=v.current_voyage)
                            v.next_event_time = t + 1
                            continue

                        berth_t = self._next_berth_window(t)
                        start   = max(berth_t, self.storage_berth_free_at)
                        if not self._valid_berth(start):
                            start = self._next_berth_window(start)

                        if v in self.loading_priority_queue:
                            self.loading_priority_queue.remove(v)
                            v.queue_position = None

                        v.status = "BERTHING_A"
                        self.storage_berth_free_at = start + C["BERTHING_DELAY_HOURS"] + C["HOSE_CONNECTION_HOURS"] + C["LOAD_HOURS"]
                        v.next_event_time = start + C["BERTHING_DELAY_HOURS"]

                        slot = C["VESSEL_NAMES"].index(v.name) + 1
                        if berth_t - t > 0.1:
                            self._log(t, v.name, "WAITING_BERTH_A",
                                      f"Waiting for berthing window | Available at {self._dt(berth_t).strftime('%Y-%m-%d %H:%M')}",
                                      voyage_num=v.current_voyage)
                        self._log(start, v.name, "BERTHING_START_A",
                                  f"Berthing at {SN} [rotation slot {slot} of {len(C['VESSEL_NAMES'])}]",
                                  voyage_num=v.current_voyage)
                    else:
                        v.next_event_time = t + 0.5
                        self._log(t, v.name, "WAITING_STOCK",
                                  f"Stock {self.storage_bbl:,.0f} bbl — waiting for {cap:,} bbl",
                                  voyage_num=v.current_voyage)

                # ── BERTHING_A ────────────────────────────────────────────
                elif v.status == "BERTHING_A":
                    v.status = "HOSE_CONNECT_A"
                    v.next_event_time = t + C["HOSE_CONNECTION_HOURS"]
                    self._log(t, v.name, "HOSE_CONNECTION_START_A",
                              f"Hose connection at {SN} (2h)", voyage_num=v.current_voyage)

                # ── HOSE_CONNECT_A (dead-stock gate) ─────────────────────
                elif v.status == "HOSE_CONNECT_A":
                    cap = v.cargo_capacity
                    dead_thresh = C["DEAD_STOCK_FACTOR"] * cap
                    if self.storage_bbl < dead_thresh:
                        v.next_event_time = t + 0.5
                        self._log(t, v.name, "WAITING_DEAD_STOCK",
                                  f"Need {dead_thresh:,.0f} bbl; have {self.storage_bbl:,.0f} bbl",
                                  voyage_num=v.current_voyage)
                        continue
                    self.storage_bbl -= cap
                    v.cargo_bbl = cap
                    self.total_loaded += cap
                    v.status = "LOADING"
                    self.storage_berth_free_at = max(self.storage_berth_free_at, t + C["LOAD_HOURS"])
                    v.next_event_time = t + C["LOAD_HOURS"]
                    self._log(t, v.name, "LOADING_START",
                              f"Loading {cap:,} bbl | {SN}: {self.storage_bbl:,.0f} bbl",
                              voyage_num=v.current_voyage)

                # ── LOADING ───────────────────────────────────────────────
                elif v.status == "LOADING":
                    v.status = "DOCUMENTING"
                    v.next_event_time = t + 4
                    self._log(t, v.name, "LOADING_COMPLETE",
                              f"Cargo: {v.cargo_bbl:,} bbl | Begin 4h documentation",
                              voyage_num=v.current_voyage)

                # ── DOCUMENTING ───────────────────────────────────────────
                elif v.status == "DOCUMENTING":
                    cast_t = self._next_cast_off(t)
                    wait   = cast_t - t
                    v.status = "CAST_OFF"
                    v.next_event_time = cast_t + C["CAST_OFF_HOURS"]
                    self._log(t, v.name, "DOCUMENTATION_COMPLETE",
                              f"Ready for cast-off at {self._dt(cast_t).strftime('%H:%M')} (wait {wait:.1f}h)",
                              voyage_num=v.current_voyage)
                    if wait > 0:
                        self._log(t, v.name, "WAITING_CAST_OFF",
                                  f"Cast-off window opens at {self._dt(cast_t).strftime('%Y-%m-%d %H:%M')}",
                                  voyage_num=v.current_voyage)

                # ── CAST_OFF ──────────────────────────────────────────────
                elif v.status == "CAST_OFF":
                    sail_t = self._next_daylight(t)
                    wait   = sail_t - t
                    v.status = "SAILING_AB"
                    v.next_event_time = sail_t + C["SAIL_HOURS_A_TO_B"]
                    self._log(t, v.name, "CAST_OFF_COMPLETE",
                              f"Departure {self._dt(sail_t).strftime('%H:%M')} (wait {wait:.1f}h)",
                              voyage_num=v.current_voyage)
                    if wait > 0:
                        self._log(t, v.name, "WAITING_DAYLIGHT",
                                  f"Daylight opens at {self._dt(sail_t).strftime('%Y-%m-%d %H:%M')}",
                                  voyage_num=v.current_voyage)
                    # advance rotation queue
                    if self.loading_rotation_queue and self.loading_rotation_queue[0] is v:
                        self.loading_rotation_queue.pop(0)
                        self.loading_rotation_queue.append(v)
                        nxt = self.loading_rotation_queue[0]
                        self._log(t, SN, "ROTATION_ADVANCE",
                                  f"{v.name} departed — next in rotation: {nxt.name}")

                # ── SAILING_AB ────────────────────────────────────────────
                elif v.status == "SAILING_AB":
                    self._log(t, v.name, "ARRIVED_FAIRWAY",
                              "Reached fairway buoy (2h from Point B)", voyage_num=v.current_voyage)
                    hour = t % 24
                    if hour >= 19:
                        cont = (int(t // 24) + 1) * 24 + 6
                        self._log(t, v.name, "WAITING_FAIRWAY",
                                  f"Holding until {self._dt(cont).strftime('%Y-%m-%d %H:%M')}",
                                  voyage_num=v.current_voyage)
                    else:
                        cont = t
                    v.status = "SAILING_AB_LEG2"
                    v.next_event_time = cont + 2

                # ── SAILING_AB_LEG2 ───────────────────────────────────────
                elif v.status == "SAILING_AB_LEG2":
                    arrival = t
                    v.arrival_at_b = arrival
                    hour = arrival % 24
                    if hour >= 18:
                        berthing_start = (int(arrival // 24) + 1) * 24 + 7
                        self._log(arrival, v.name, "WAITING_NIGHT",
                                  f"Arrived after 18:00; waiting until {self._dt(berthing_start).strftime('%Y-%m-%d %H:%M')}",
                                  voyage_num=v.current_voyage)
                    else:
                        berthing_start = arrival

                    mother_unavail    = self.exporting or arrival < self.mother_available_at
                    queue_has_vessels = len(self.waiting_mother_return_queue) > 0

                    if mother_unavail or queue_has_vessels:
                        v.status = "WAITING_MOTHER_RETURN"
                        if v not in self.waiting_mother_return_queue:
                            self.waiting_mother_return_queue.append(v)
                        if self.exporting:
                            detail = "Mother exporting — waiting"
                        elif arrival < self.mother_available_at:
                            detail = f"Mother due back at {self._dt(self.mother_available_at).strftime('%Y-%m-%d %H:%M')}"
                        else:
                            detail = f"Joining FIFO queue behind {self.waiting_mother_return_queue[0].name}"
                        self._log(arrival, v.name, "WAITING_MOTHER_RETURN", detail,
                                  voyage_num=v.current_voyage)
                        v.next_event_time = (self.mother_available_at
                                             if self.mother_available_at > arrival
                                             else arrival + C["TIME_STEP_HOURS"])
                        continue

                    berth_t = self._next_berth_window(berthing_start)
                    start   = max(berth_t, self.mother_berth_free_at, self.mother_available_at)
                    if not self._valid_berth(start):
                        start = self._next_berth_window(start)

                    if self.mother_bbl + v.cargo_bbl > C["MOTHER_CAPACITY_BBL"]:
                        self._log(arrival, v.name, "WAITING_MOTHER_CAPACITY",
                                  "Insufficient capacity on mother", voyage_num=v.current_voyage)
                        v.next_event_time = arrival + 6
                    else:
                        v.status = "BERTHING_B"
                        self.mother_berth_free_at = start + C["BERTHING_DELAY_HOURS"] + C["HOSE_CONNECTION_HOURS"] + C["DISCHARGE_HOURS"]
                        v.next_event_time = start + C["BERTHING_DELAY_HOURS"]
                        if berth_t > berthing_start + 0.1:
                            self._log(berthing_start, v.name, "WAITING_BERTH_B",
                                      f"Waiting for berthing window at {self._dt(berth_t).strftime('%Y-%m-%d %H:%M')}",
                                      voyage_num=v.current_voyage)
                        self._log(start, v.name, "BERTHING_START_B",
                                  f"Berthing at {MN} (30 min procedure)", voyage_num=v.current_voyage)

                # ── BERTHING_B ────────────────────────────────────────────
                elif v.status == "BERTHING_B":
                    v.status = "HOSE_CONNECT_B"
                    v.next_event_time = t + C["HOSE_CONNECTION_HOURS"]
                    self._log(t, v.name, "HOSE_CONNECTION_START_B",
                              f"Hose connection at {MN} (2h)", voyage_num=v.current_voyage)

                # ── HOSE_CONNECT_B ────────────────────────────────────────
                elif v.status == "HOSE_CONNECT_B":
                    if self.mother_bbl + v.cargo_bbl > C["MOTHER_CAPACITY_BBL"]:
                        self._log(t, v.name, "WAITING_MOTHER_CAPACITY",
                                  "Cannot discharge — mother lacks space", voyage_num=v.current_voyage)
                        v.next_event_time = t + 6
                    else:
                        self.mother_bbl += v.cargo_bbl
                        v.status = "DISCHARGING"
                        self.mother_berth_free_at = max(self.mother_berth_free_at, t + C["DISCHARGE_HOURS"])
                        v.next_event_time = t + C["DISCHARGE_HOURS"]
                        self._log(t, v.name, "DISCHARGE_START",
                                  f"Discharging {v.cargo_bbl:,} bbl | {MN}: {self.mother_bbl:,.0f} bbl",
                                  voyage_num=v.current_voyage)

                # ── DISCHARGING ───────────────────────────────────────────
                elif v.status == "DISCHARGING":
                    v.cargo_bbl = 0
                    v.status = "CAST_OFF_B"
                    v.next_event_time = t + C["CAST_OFF_HOURS"]
                    self._log(t, v.name, "DISCHARGE_COMPLETE",
                              f"{MN}: {self.mother_bbl:,.0f} bbl | Begin cast-off",
                              voyage_num=v.current_voyage)

                # ── CAST_OFF_B ────────────────────────────────────────────
                elif v.status == "CAST_OFF_B":
                    sail_t = self._next_daylight(t)
                    wait   = sail_t - t
                    v.status = "SAILING_BA"
                    v.next_event_time = sail_t + C["SAIL_HOURS_B_TO_A"]
                    self.export_ready = True
                    self._log(t, v.name, "CAST_OFF_COMPLETE_B",
                              "Cast-off from mother; returning to storage", voyage_num=v.current_voyage)
                    if wait > 0:
                        self._log(t, v.name, "WAITING_DAYLIGHT",
                                  f"Daylight opens at {self._dt(sail_t).strftime('%Y-%m-%d %H:%M')}",
                                  voyage_num=v.current_voyage)

                # ── SAILING_BA ────────────────────────────────────────────
                elif v.status == "SAILING_BA":
                    v.status = "IDLE_A"
                    v._voyage_assigned = False
                    v.next_event_time = t
                    self._log(t, v.name, "ARRIVED_POINT_A",
                              "Back at storage — ready for next cycle", voyage_num=v.current_voyage)

                # ── WAITING_MOTHER_RETURN ─────────────────────────────────
                elif v.status == "WAITING_MOTHER_RETURN":
                    if self.exporting:
                        v.next_event_time = t + C["TIME_STEP_HOURS"]
                        self._log(t, v.name, "WAITING_MOTHER_RETURN",
                                  "Still waiting — mother exporting", voyage_num=v.current_voyage)
                        continue
                    if t < self.mother_available_at:
                        v.next_event_time = self.mother_available_at
                        continue
                    if not self._valid_berth(t):
                        berth_t = self._next_berth_window(t)
                        v.next_event_time = berth_t
                        self._log(t, v.name, "WAITING_BERTH_B",
                                  f"Waiting for berthing window at {self._dt(berth_t).strftime('%Y-%m-%d %H:%M')}",
                                  voyage_num=v.current_voyage)
                        continue
                    start = max(t, self.mother_berth_free_at, self.mother_available_at)
                    if not self._valid_berth(start):
                        start = self._next_berth_window(start)
                        v.next_event_time = start
                        self._log(t, v.name, "WAITING_BERTH_B",
                                  f"Waiting until {self._dt(start).strftime('%Y-%m-%d %H:%M')}",
                                  voyage_num=v.current_voyage)
                        continue
                    if self.mother_bbl + v.cargo_bbl > C["MOTHER_CAPACITY_BBL"]:
                        self._log(t, v.name, "WAITING_MOTHER_CAPACITY",
                                  "Insufficient capacity", voyage_num=v.current_voyage)
                        v.next_event_time = t + 6
                    else:
                        if v in self.waiting_mother_return_queue:
                            self.waiting_mother_return_queue.remove(v)
                        v.status = "BERTHING_B"
                        self.mother_berth_free_at = start + C["BERTHING_DELAY_HOURS"] + C["HOSE_CONNECTION_HOURS"] + C["DISCHARGE_HOURS"]
                        v.next_event_time = start + C["BERTHING_DELAY_HOURS"]
                        self._log(start, v.name, "BERTHING_START_B",
                                  f"Berthing at {MN} [priority FIFO]", voyage_num=v.current_voyage)

            # 4. Export state machine
            if not self.exporting and self.export_ready and self.mother_bbl >= self.MOTHER_EXPORT_TRIGGER:
                if not self._discharging():
                    dh = t % 24
                    if C["DAYLIGHT_START"] <= dh < C["DAYLIGHT_END"]:
                        self.exporting = True
                        self.export_ready = False
                        self.export_state = "DOC"
                        self.export_end_time = t + C["EXPORT_DOC_HOURS"]
                        self._log(t, MN, "EXPORT_DOC_START", f"Export documentation ({C['EXPORT_DOC_HOURS']}h)")

            if self.exporting:
                if self.export_state == "DOC" and t >= self.export_end_time:
                    sail_start = self._next_export_sail(t)
                    self.export_state = "SAILING"
                    self.export_end_time = sail_start + C["EXPORT_SAIL_HOURS"]
                    self._log(sail_start, MN, "EXPORT_SAIL_START", f"Sailing to export terminal")
                elif self.export_state == "SAILING" and t >= self.export_end_time:
                    self.export_state = "HOSE"
                    self.export_end_time = t + C["EXPORT_HOSE_HOURS"]
                    self._log(t, MN, "EXPORT_HOSE_START", f"Hose connection ({C['EXPORT_HOSE_HOURS']}h)")
                elif self.export_state == "HOSE" and t >= self.export_end_time:
                    self.export_state = "IN_PORT"
                    self._log(t, MN, "EXPORT_HOSE_COMPLETE", "Ready to export")
                elif self.export_state == "IN_PORT":
                    amount = min(self.mother_bbl, C["EXPORT_RATE_BPH"] * C["TIME_STEP_HOURS"])
                    if amount > 0:
                        self.mother_bbl   -= amount
                        self.total_exported += amount
                    if self.mother_bbl <= 0:
                        self.exporting     = False
                        self.export_state  = None
                        self._log(t, MN, "EXPORT_COMPLETE",
                                  f"Export complete; {self.total_exported:,.0f} bbl total exported")
                        return_depart  = self._next_daylight(t)
                        return_arrival = return_depart + C["EXPORT_SAIL_HOURS"]
                        self.mother_available_at = return_arrival + 2
                        self._log(return_depart,  MN, "EXPORT_RETURN_START",   f"Departing export terminal")
                        self._log(return_arrival, MN, "EXPORT_RETURN_ARRIVE",  f"Arrived at {MN}; beginning 2h fendering")
                        self._log(self.mother_available_at, MN, "EXPORT_FENDERING_COMPLETE", "Fendering complete")

            # 5. Timeline snapshot
            self.timeline.append({
                "Time"       : self._dt(t),
                "Day"        : int(t // 24) + 1,
                "Storage_bbl": round(self.storage_bbl),
                "Mother_bbl" : round(self.mother_bbl),
                "Total_Exported": self.total_exported,
                **{v.name: v.status for v in self.vessels},
            })
            t = round(t + C["TIME_STEP_HOURS"], 2)

        return pd.DataFrame(self.log), pd.DataFrame(self.timeline)


# =============================================================================
# ── COLOUR UTILITIES ──────────────────────────────────────────────────────────
# =============================================================================

VESSEL_COLORS = {
    "Sherlock"  : "#e74c3c",
    "Laphroaig" : "#2ecc71",
    "Rathbone"  : "#9b59b6",
    "Bedford"   : "#f39c12",
}

STATUS_LIGHTNESS = {
    "IDLE_A": 2.0, "WAITING_STOCK": 1.8, "WAITING_BERTH_A": 1.7,
    "WAITING_DEAD_STOCK": 1.6, "BERTHING_A": 1.3, "HOSE_CONNECT_A": 1.1,
    "LOADING": 1.0, "DOCUMENTING": 0.9, "WAITING_CAST_OFF": 0.85,
    "CAST_OFF": 0.8, "SAILING_AB": 0.7, "SAILING_AB_LEG2": 0.65,
    "WAITING_FAIRWAY": 0.6, "WAITING_BERTH_B": 0.6,
    "WAITING_MOTHER_RETURN": 0.55, "WAITING_MOTHER_CAPACITY": 0.5,
    "BERTHING_B": 0.5, "HOSE_CONNECT_B": 0.45, "DISCHARGING": 0.4,
    "CAST_OFF_B": 0.38, "SAILING_BA": 0.5, "IDLE_B": 0.55, "WAITING_DAYLIGHT": 1.5,
}

STATUS_LABELS = {
    "IDLE_A": "Idle at Storage", "WAITING_STOCK": "Waiting — Low Stock",
    "WAITING_DEAD_STOCK": "Waiting — Dead Stock", "LOADING": "Loading",
    "SAILING_AB": "Sailing → Bryanston", "SAILING_AB_LEG2": "Approaching Bryanston",
    "DISCHARGING": "Discharging", "SAILING_BA": "Returning → Chapel",
    "WAITING_MOTHER_RETURN": "Waiting — Mother Exporting",
    "BERTHING_A": "Berthing at Chapel", "BERTHING_B": "Berthing at Bryanston",
    "HOSE_CONNECT_A": "Hose Connection (Chapel)", "HOSE_CONNECT_B": "Hose Connection (Bryanston)",
    "DOCUMENTING": "Documentation", "CAST_OFF": "Cast Off (Chapel)",
    "CAST_OFF_B": "Cast Off (Bryanston)", "WAITING_CAST_OFF": "Waiting — Cast Off Window",
    "WAITING_BERTH_B": "Waiting — Berth at Bryanston", "WAITING_DAYLIGHT": "Waiting — Daylight",
    "WAITING_FAIRWAY": "Holding at Fairway",
}

def _shade(hex_color, factor):
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i:i+2], 16) / 255 for i in (0, 2, 4))
    hh, l, s = colorsys.rgb_to_hls(r, g, b)
    l2 = max(0.0, min(1.0, l * factor))
    r2, g2, b2 = colorsys.hls_to_rgb(hh, l2, s)
    return "#{:02x}{:02x}{:02x}".format(int(r2 * 255), int(g2 * 255), int(b2 * 255))

def vessel_color(name, status):
    base   = VESSEL_COLORS.get(name, "#95a5a6")
    factor = STATUS_LIGHTNESS.get(status, 1.0)
    return _shade(base, factor)


# =============================================================================
# ── GOOGLE SHEETS LOADER (optional) ──────────────────────────────────────────
# =============================================================================

def load_from_gsheets(sheet_id: str, creds_json: str) -> dict | None:
    """
    Pull the latest row from a Google Sheet and return overrides for
    STORAGE_INIT_BBL and MOTHER_INIT_BBL.

    Expected sheet columns (row 1 = headers):
        timestamp | storage_bbl | mother_bbl | sim_days (optional)

    Returns None if gspread is not installed or credentials are invalid.
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        import json, tempfile, os

        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds_dict = json.loads(creds_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc    = gspread.authorize(creds)
        ws    = gc.open_by_key(sheet_id).sheet1
        rows  = ws.get_all_records()
        if not rows:
            return None
        latest = rows[-1]   # most recent entry
        overrides = {}
        if "storage_bbl" in latest and latest["storage_bbl"]:
            overrides["STORAGE_INIT_BBL"] = int(latest["storage_bbl"])
        if "mother_bbl" in latest and latest["mother_bbl"]:
            overrides["MOTHER_INIT_BBL"] = int(latest["mother_bbl"])
        if "sim_days" in latest and latest["sim_days"]:
            overrides["SIMULATION_DAYS"] = int(latest["sim_days"])
        return overrides
    except ImportError:
        st.sidebar.warning("gspread not installed. Run: pip install gspread google-auth")
        return None
    except Exception as e:
        st.sidebar.error(f"Google Sheets error: {e}")
        return None


# =============================================================================
# ── RUN / CACHE SIMULATION ───────────────────────────────────────────────────
# =============================================================================

@st.cache_data(ttl=300, show_spinner="Running simulation…")
def run_simulation(cfg_tuple):
    """Cached — re-runs only when config changes or TTL (5 min) expires."""
    cfg = dict(cfg_tuple)
    cfg["VESSEL_NAMES"]      = list(cfg["VESSEL_NAMES"])
    cfg["VESSEL_CAPACITIES"] = dict(cfg["VESSEL_CAPACITIES"])
    sim = TankerSimulation(cfg)
    log_df, tl_df = sim.run()
    summary = dict(
        total_loadings    = int(len(log_df[log_df["Event"] == "LOADING_START"])),
        total_discharges  = int(len(log_df[log_df["Event"] == "DISCHARGE_START"])),
        total_loaded      = int(sim.total_loaded),
        total_exported    = float(sim.total_exported),
        export_voyages    = int(len(log_df[log_df["Event"] == "EXPORT_COMPLETE"])),
        final_storage     = float(sim.storage_bbl),
        final_mother      = float(sim.mother_bbl),
        overflow_events   = int(sim.storage_overflow_events),
    )
    return log_df, tl_df, summary


def make_cfg_tuple(overrides: dict) -> tuple:
    """Convert config dict to a hashable tuple for st.cache_data."""
    cfg = {**DEFAULT_CONFIG, **overrides}
    return tuple(sorted(
        (k, tuple(v) if isinstance(v, list) else
             tuple(sorted(v.items())) if isinstance(v, dict) else v)
        for k, v in cfg.items()
    ))


# =============================================================================
# ── CHARTS ───────────────────────────────────────────────────────────────────
# =============================================================================

def build_volume_chart(tl_df: pd.DataFrame, cfg: dict) -> go.Figure:
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        subplot_titles=(
            f"📦 {cfg['STORAGE_NAME']} (Chapel) — Volume",
            f"🛢️ {cfg['MOTHER_NAME']} (Bryanston) — Volume",
        ),
        vertical_spacing=0.08,
    )
    # Storage
    fig.add_trace(go.Scatter(
        x=tl_df["Time"], y=tl_df["Storage_bbl"],
        fill="tozeroy", fillcolor="rgba(230,126,34,0.15)",
        line=dict(color="#e67e22", width=2), name="Chapel Volume",
    ), row=1, col=1)
    fig.add_hline(y=cfg["STORAGE_CAPACITY_BBL"], line=dict(color="#e74c3c", dash="dash", width=1.5),
                  annotation_text="Max Capacity", row=1, col=1)

    export_trigger = cfg["MOTHER_CAPACITY_BBL"] - max(cfg["VESSEL_CAPACITIES"].values(), default=cfg["DAUGHTER_CARGO_BBL"])

    # Mother
    fig.add_trace(go.Scatter(
        x=tl_df["Time"], y=tl_df["Mother_bbl"],
        fill="tozeroy", fillcolor="rgba(41,128,185,0.15)",
        line=dict(color="#2980b9", width=2), name="Bryanston Volume",
    ), row=2, col=1)
    fig.add_hline(y=export_trigger, line=dict(color="#e74c3c", dash="dash", width=1.5),
                  annotation_text="Export Trigger", row=2, col=1)
    fig.add_hline(y=cfg["MOTHER_CAPACITY_BBL"], line=dict(color="#922b21", dash="dot", width=1),
                  annotation_text="Max Capacity", row=2, col=1)

    fig.update_layout(
        height=480,
        paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
        font=dict(color="#e2e8f0"),
        legend=dict(bgcolor="#1e2130", bordercolor="#444"),
        margin=dict(l=60, r=30, t=50, b=30),
    )
    fig.update_yaxes(tickformat=",", gridcolor="#1e2130", title_text="Volume (bbl)")
    fig.update_xaxes(gridcolor="#1e2130")
    return fig


def build_gantt(tl_df: pd.DataFrame, cfg: dict) -> go.Figure:
    vessel_names = cfg["VESSEL_NAMES"]
    y_pos = {name: i for i, name in enumerate(vessel_names)}

    bars_x, bars_y, bars_color, bars_text = [], [], [], []
    dt_step = timedelta(hours=cfg["TIME_STEP_HOURS"])

    for _, row in tl_df.iterrows():
        for vn in vessel_names:
            status = row.get(vn)
            if pd.notna(status):
                color  = vessel_color(vn, status)
                label  = STATUS_LABELS.get(status, status)
                bars_x.append(row["Time"])
                bars_y.append(y_pos[vn])
                bars_color.append(color)
                bars_text.append(f"<b>{vn}</b><br>{label}<br>{row['Time'].strftime('%d %b %H:%M')}")

    fig = go.Figure()
    for vn in vessel_names:
        base  = VESSEL_COLORS.get(vn, "#aaa")
        idxs  = [i for i, y in enumerate(bars_y) if y == y_pos[vn]]
        fig.add_trace(go.Bar(
            x=[bars_x[i] for i in idxs],
            y=[cfg["TIME_STEP_HOURS"] / 24] * len(idxs),
            base=[y_pos[vn] - 0.3] * len(idxs),
            orientation="h",
            marker_color=[bars_color[i] for i in idxs],
            hovertext=[bars_text[i] for i in idxs],
            hoverinfo="text",
            name=vn,
            showlegend=True,
            marker_line_width=0,
        ))

    fig.update_layout(
        height=280,
        barmode="overlay",
        paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
        font=dict(color="#e2e8f0"),
        bargap=0, bargroupgap=0,
        margin=dict(l=90, r=30, t=10, b=40),
        yaxis=dict(
            tickvals=list(y_pos.values()),
            ticktext=[f"<b>{n}</b>" for n in vessel_names],
            range=[-0.5, len(vessel_names) - 0.5],
            gridcolor="#1e2130",
        ),
        xaxis=dict(title="Date", gridcolor="#1e2130"),
        legend=dict(bgcolor="#1e2130", bordercolor="#444", orientation="h",
                    yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def build_event_timeline(log_df: pd.DataFrame) -> go.Figure:
    key_events = ["LOADING_START", "DISCHARGE_START", "EXPORT_COMPLETE",
                  "ARRIVED_POINT_A", "EXPORT_RETURN_ARRIVE"]
    df = log_df[log_df["Event"].isin(key_events)].copy()
    if df.empty:
        return go.Figure()
    df["Time_dt"] = pd.to_datetime(df["Time"])
    color_map = {v: VESSEL_COLORS.get(v, "#aaa") for v in df["Vessel"].unique()}

    fig = px.scatter(
        df, x="Time_dt", y="Vessel", color="Vessel",
        color_discrete_map=color_map,
        symbol="Event", hover_data=["Event", "Detail", "Storage_bbl", "Mother_bbl"],
        title="Key Events Timeline",
    )
    fig.update_traces(marker=dict(size=10, line=dict(width=1, color="#0e1117")))
    fig.update_layout(
        height=280,
        paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
        font=dict(color="#e2e8f0"),
        legend=dict(bgcolor="#1e2130", bordercolor="#444"),
        margin=dict(l=90, r=30, t=40, b=40),
        xaxis=dict(gridcolor="#1e2130"),
        yaxis=dict(gridcolor="#1e2130"),
    )
    return fig


# =============================================================================
# ── MAIN APP ─────────────────────────────────────────────────────────────────
# =============================================================================

def main():
    # ── Header ───────────────────────────────────────────────────────────────
    col_logo, col_title = st.columns([1, 8])
    with col_logo:
        st.markdown("# 🛢️")
    with col_title:
        st.markdown("## Oil Tanker Daughter Vessel Operation Dashboard")
        st.caption("Discrete-event simulation · Chapel (Point A) → Bryanston (Point B)")

    st.divider()

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### ⚙️ Simulation Settings")

        sim_days = st.slider("Simulation Days", 7, 60, 30)
        prod_rate = st.number_input(
            "Production Rate (bbl/hr)", 500, 5000,
            DEFAULT_CONFIG["PRODUCTION_RATE_BPH"], step=100,
        )
        storage_init = st.number_input(
            "Starting Storage Level (bbl)", 0, 800_000,
            DEFAULT_CONFIG["STORAGE_INIT_BBL"], step=10_000,
        )
        mother_init = st.number_input(
            "Starting Bryanston Level (bbl)", 0, 550_000,
            DEFAULT_CONFIG["MOTHER_INIT_BBL"], step=10_000,
        )

        st.markdown("---")
        st.markdown("### 🔄 Auto-Refresh")
        auto_refresh = st.toggle("Enable auto-refresh", value=False)
        refresh_secs = st.slider("Refresh interval (seconds)", 30, 600, 300,
                                  disabled=not auto_refresh)

        st.markdown("---")
        st.markdown("### 📊 Google Sheets Live Data")
        st.caption("Connect a Google Sheet to seed the simulation with real-time vessel data.")
        use_gsheets  = st.toggle("Enable Google Sheets sync", value=False)
        gsheet_id    = st.text_input("Sheet ID", placeholder="Paste your Google Sheet ID here",
                                      disabled=not use_gsheets)
        creds_file   = st.file_uploader("Service Account JSON", type=["json"],
                                         disabled=not use_gsheets)

        st.markdown("---")
        st.markdown("### 📋 Google Sheets Setup Guide")
        with st.expander("How to connect Google Sheets"):
            st.markdown("""
**Step 1 — Create a Google Sheet**
Add a sheet with these column headers in row 1:
```
timestamp | storage_bbl | mother_bbl | sim_days
```
Each time your team updates data, add a new row.

**Step 2 — Create a Service Account**
1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Create a project → Enable **Google Sheets API**
3. Go to *Credentials* → *Create credentials* → *Service account*
4. Download the JSON key file

**Step 3 — Share the Sheet**
Share your Google Sheet with the service account email
(e.g. `my-app@project.iam.gserviceaccount.com`) as **Viewer**.

**Step 4 — Connect here**
- Paste your **Sheet ID** (the long string in the URL)
- Upload the **JSON key file**
- Toggle sync ON — dashboard auto-refreshes every 5 minutes
""")

        st.markdown("---")
        st.markdown("### 🚀 Deploy as Public Link")
        with st.expander("How to share a live link"):
            st.markdown("""
**Streamlit Community Cloud (Free)**

1. Save this file as `tanker_app.py`
2. Create a `requirements.txt` containing:
```
streamlit
pandas
plotly
gspread
google-auth
```
3. Push both files to a **GitHub repository**
4. Go to [share.streamlit.io](https://share.streamlit.io)
5. Click *New app* → select your repo → select `tanker_app.py`
6. Click **Deploy** — you get a permanent public URL instantly

Anyone with the link can view the live dashboard.
Your Google Sheets data updates automatically every 5 minutes.
""")

    # ── Resolve config overrides ──────────────────────────────────────────────
    overrides = dict(
        SIMULATION_DAYS      = sim_days,
        PRODUCTION_RATE_BPH  = prod_rate,
        STORAGE_INIT_BBL     = storage_init,
        MOTHER_INIT_BBL      = mother_init,
    )

    if use_gsheets and gsheet_id and creds_file:
        creds_json = creds_file.read().decode("utf-8")
        gs_overrides = load_from_gsheets(gsheet_id, creds_json)
        if gs_overrides:
            overrides.update(gs_overrides)
            st.sidebar.success(f"✅ Google Sheets synced: "
                               f"Storage={gs_overrides.get('STORAGE_INIT_BBL','—'):,} bbl, "
                               f"Bryanston={gs_overrides.get('MOTHER_INIT_BBL','—'):,} bbl")

    # ── Run simulation ────────────────────────────────────────────────────────
    cfg_tuple = make_cfg_tuple(overrides)
    log_df, tl_df, summary = run_simulation(cfg_tuple)
    cfg = {**DEFAULT_CONFIG, **overrides}

    # ── KPI metrics row ───────────────────────────────────────────────────────
    st.markdown('<div class="section-header">📈 Summary — {} Day Simulation</div>'.format(sim_days),
                unsafe_allow_html=True)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Loadings",    summary["total_loadings"])
    c2.metric("Total Discharges",  summary["total_discharges"])
    c3.metric("Volume Loaded",     f"{summary['total_loaded']:,} bbl")
    c4.metric("Volume Exported",   f"{summary['total_exported']:,.0f} bbl")
    c5.metric("Final Storage",     f"{summary['final_storage']:,.0f} bbl",
              delta=f"{'▲' if summary['final_storage'] > storage_init else '▼'} vs start")
    c6.metric("Final Bryanston",   f"{summary['final_mother']:,.0f} bbl")

    if summary["overflow_events"] > 0:
        st.warning(f"⚠️  Storage overflow detected: {summary['overflow_events']} events — "
                   "consider increasing lifting frequency or reducing production rate.")

    # ── Volume charts ─────────────────────────────────────────────────────────
    st.markdown('<div class="section-header">📦 Volume Levels Over Time</div>', unsafe_allow_html=True)
    st.plotly_chart(build_volume_chart(tl_df, cfg), use_container_width=True)

    # ── Gantt chart ───────────────────────────────────────────────────────────
    st.markdown('<div class="section-header">⛴️ Vessel Activity Timeline (Gantt)</div>',
                unsafe_allow_html=True)
    st.plotly_chart(build_gantt(tl_df, cfg), use_container_width=True)

    # ── Colour legend ─────────────────────────────────────────────────────────
    with st.expander("🎨 Gantt colour key — shade = activity phase"):
        legend_cols = st.columns(4)
        for i, vn in enumerate(cfg["VESSEL_NAMES"]):
            with legend_cols[i]:
                st.markdown(f"**{vn}**")
                for status, label in [
                    ("IDLE_A",      "Idle at storage"),
                    ("LOADING",     "Loading"),
                    ("SAILING_AB",  "Sailing → Bryanston"),
                    ("DISCHARGING", "Discharging"),
                    ("SAILING_BA",  "Returning → Chapel"),
                ]:
                    color = vessel_color(vn, status)
                    st.markdown(
                        f'<span style="background:{color};padding:2px 10px;'
                        f'border-radius:4px;font-size:12px;">&nbsp;</span> {label}',
                        unsafe_allow_html=True,
                    )

    # ── Key events scatter ────────────────────────────────────────────────────
    st.markdown('<div class="section-header">🔵 Key Events Timeline</div>', unsafe_allow_html=True)
    st.plotly_chart(build_event_timeline(log_df), use_container_width=True)

    # ── Vessel status tabs ────────────────────────────────────────────────────
    st.markdown('<div class="section-header">🚢 Vessel-by-Vessel Breakdown</div>',
                unsafe_allow_html=True)
    tabs = st.tabs(cfg["VESSEL_NAMES"])
    for tab, vn in zip(tabs, cfg["VESSEL_NAMES"]):
        with tab:
            vlog = log_df[log_df["Vessel"] == vn].copy()
            c_left, c_right = st.columns([2, 3])
            with c_left:
                loads      = vlog[vlog["Event"] == "LOADING_START"]
                discharges = vlog[vlog["Event"] == "DISCHARGE_START"]
                cap        = cfg["VESSEL_CAPACITIES"].get(vn, cfg["DAUGHTER_CARGO_BBL"])
                st.metric("Voyages Completed", len(loads))
                st.metric("Total Loaded",   f"{len(loads) * cap:,} bbl")
                st.metric("Total Discharged", f"{len(discharges) * cap:,} bbl")
                st.metric("Cargo Capacity", f"{cap:,} bbl")
                st.markdown(
                    f'<span style="background:{VESSEL_COLORS.get(vn,"#aaa")};'
                    f'padding:4px 14px;border-radius:8px;color:#fff;font-weight:700;">'
                    f'{vn}</span>',
                    unsafe_allow_html=True,
                )
            with c_right:
                show_cols = ["Time", "Day", "Voyage", "Event", "Detail", "Storage_bbl", "Mother_bbl"]
                st.dataframe(
                    vlog[show_cols].rename(columns={
                        "Storage_bbl": "Chapel (bbl)", "Mother_bbl": "Bryanston (bbl)"
                    }),
                    use_container_width=True,
                    height=320,
                )

    # ── Full event log ────────────────────────────────────────────────────────
    st.markdown('<div class="section-header">📋 Full Event Log</div>', unsafe_allow_html=True)
    col_filter1, col_filter2, col_filter3 = st.columns(3)
    with col_filter1:
        vessel_filter = st.multiselect(
            "Filter by Vessel",
            options=cfg["VESSEL_NAMES"] + [cfg["STORAGE_NAME"], cfg["MOTHER_NAME"]],
            default=[],
        )
    with col_filter2:
        event_types = sorted(log_df["Event"].dropna().unique())
        event_filter = st.multiselect("Filter by Event Type", options=event_types, default=[])
    with col_filter3:
        day_range = st.slider("Filter by Day", 1, sim_days, (1, sim_days))

    filtered = log_df[log_df["Day"].between(day_range[0], day_range[1])].copy()
    if vessel_filter:
        filtered = filtered[filtered["Vessel"].isin(vessel_filter)]
    if event_filter:
        filtered = filtered[filtered["Event"].isin(event_filter)]

    show_cols = ["Time", "Day", "Vessel", "Voyage", "Event", "Detail", "Storage_bbl", "Mother_bbl"]
    st.dataframe(
        filtered[show_cols].rename(columns={
            "Storage_bbl": "Chapel (bbl)", "Mother_bbl": "Bryanston (bbl)"
        }),
        use_container_width=True,
        height=420,
    )
    st.caption(f"Showing {len(filtered):,} of {len(log_df):,} events")

    # ── Downloads ─────────────────────────────────────────────────────────────
    st.markdown('<div class="section-header">⬇️ Download</div>', unsafe_allow_html=True)
    dl1, dl2 = st.columns(2)
    with dl1:
        csv_log = log_df.to_csv(index=False).encode("utf-8")
        st.download_button("📥 Download Full Event Log (CSV)", csv_log,
                           "tanker_event_log.csv", "text/csv")
    with dl2:
        csv_tl = tl_df.to_csv(index=False).encode("utf-8")
        st.download_button("📥 Download Timeline Snapshots (CSV)", csv_tl,
                           "tanker_timeline.csv", "text/csv")

    # ── Auto-refresh countdown ────────────────────────────────────────────────
    if auto_refresh:
        placeholder = st.empty()
        for remaining in range(refresh_secs, 0, -1):
            placeholder.caption(f"🔄 Auto-refreshing in {remaining}s…")
            time.sleep(1)
        placeholder.caption("🔄 Refreshing now…")
        st.cache_data.clear()
        st.rerun()

    # ── Footer ────────────────────────────────────────────────────────────────
    st.divider()
    st.caption(
        "Tanker Operations Simulation v4 · "
        f"Last run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} · "
        "Rotation order: Sherlock → Laphroaig → Rathbone → Bedford"
    )


if __name__ == "__main__":
    main()
