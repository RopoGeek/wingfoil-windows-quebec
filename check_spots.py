# check_spots.py — Wind + Tides (Open-Meteo + DFO SPINE) with local-time CHS offsets
# - Wind: Open-Meteo (auto model selection for Québec)
# - Tides: SPINE baseline near Baie de Beauport; other spots = time-shifted estimates
# - Rules: gusts >= 10 kn + spot-specific direction + tide
# - Output: forecast.json consumed by index.html

import json, datetime as dt, urllib.request, urllib.parse, sys, time, bisect
from zoneinfo import ZoneInfo
from collections import Counter

# ----- CONFIG -----
TZ = ZoneInfo("America/Toronto")
NOW = dt.datetime.now(TZ).replace(minute=0, second=0, microsecond=0)
HOURS = 96  # forecast horizon (hours)

SPOTS = {
    "beauport":     {"name": "Baie de Beauport",             "lat": 46.8598,    "lon": -71.2006},
    "ste_anne":     {"name": "Quai Ste-Anne-de-Beaupré",     "lat": 47.0153,    "lon": -70.9280},
    "st_jean":      {"name": "Quai St-Jean, Île d’Orléans",  "lat": 46.8577,    "lon": -70.8169},
    "ange_gardien": {"name": "Ange-Gardien",                  "lat": 46.907944,  "lon": -71.090028},
}

# Gust thresholds (kn)
THRESHOLD_GUST = {k: 10.0 for k in SPOTS.keys()}

# Direction sectors (deg, FROM)
DIR_SW = (200, 250)  # Ste-Anne / Ange-Gardien
DIR_NE = (30, 70)    # St-Jean
def _in_sector(deg, lo, hi): return (lo <= deg <= hi) if lo <= hi else (deg >= lo or deg <= hi)
def in_SW(deg): return _in_sector(deg, *DIR_SW)
def in_NE(deg): return _in_sector(deg, *DIR_NE)

# SPINE water level API (DFO)
SPINE_BASE = "https://api-spine.azure.cloud-nuage.dfo-mpo.gc.ca/rest/v1/waterLevel"

# Candidate points around Beauport (pick the one returning most data)
BASELINE_CANDIDATES = [
    (46.8609, -71.1835),
    (46.8420, -71.2100),
    (46.8750, -71.1600),
    (46.8350, -71.2450),
]

# Tide classification tuning
EPS_TIDE = 0.02           # meters; delta to call rising/falling vs slack
MAX_T_MATCH_MIN = 75      # tolerate ±75 min difference for matching SPINE instants

# CHS-style phase offsets (minutes): rising uses LLW offset; falling uses HHW offset
TIDE_PHASE_OFFSETS = {
    "ste_anne":     {"rising": 23, "falling": 10},
    "st_jean":      {"rising": 17, "falling":  8},
    "ange_gardien": {"rising": 23, "falling": 10},  # same as Ste-Anne
}

# ----- HTTP helper -----
def http_get_json(url, timeout=45):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)

# ----- Wind (Open-Meteo) -----
def fetch_open_meteo_wind(lat, lon, start_dt, end_dt):
    """
    Uses Open-Meteo /v1/forecast WITHOUT a models= parameter.
    OM will pick the best available model(s) for the coordinates & window.
    """
    base = "https://api.open-meteo.com/v1/forecast"
    qs = urllib.parse.urlencode({
        "latitude": lat,
        "longitude": lon,
        "hourly": "windspeed_10m,windgusts_10m,winddirection_10m",
        "wind_speed_unit": "kn",
        "timezone": "America/Toronto",
        "start_date": start_dt.date().isoformat(),
        "end_date": end_dt.date().isoformat(),
    })
    return http_get_json(f"{base}?{qs}")

# ----- SPINE water levels (batched) -----
def spine_levels_batch(lat, lon, utc_list, chunk_size=36, pause=0.2, max_retries=2):
    """
    Fetch many instants via query ?lat=..&lon=..&t=..&t=.. (batched).
    Returns map { instant_isoZ: waterLevel } from items with status=OK.
    """
    out, n = {}, len(utc_list)
    for i in range(0, n, chunk_size):
        chunk = utc_list[i:i+chunk_size]
        q = []
        for t in chunk:
            q += [("lat", f"{lat}"), ("lon", f"{lon}"), ("t", t)]
        url = f"{SPINE_BASE}?{urllib.parse.urlencode(q)}"
        tries = 0
        while True:
            try:
                data = http_get_json(url)
                items = data.get("responseItems", [])
                ok = other = 0
                for it in items:
                    if it.get("status") == "OK":
                        inst = it.get("instant"); wl = it.get("waterLevel")
                        if inst is not None and wl is not None:
                            out[inst] = wl; ok += 1
                    else:
                        other += 1
                print(f"[INFO] SPINE chunk {i//chunk_size+1}: {ok}/{len(chunk)} OK (+{other} non-OK) @({lat},{lon})", file=sys.stderr)
                break
            except Exception as e:
                tries += 1
                if tries > max_retries:
                    print(f"[WARN] SPINE chunk failed after retries: {e}", file=sys.stderr)
                    break
                time.sleep(pause)
    return out

def build_sorted_series(spine_map):
    times, vals = [], []
    for ts, v in spine_map.items():
        try:
            t = dt.datetime.fromisoformat(ts.replace("Z","+00:00")).astimezone(dt.timezone.utc)
            times.append(t); vals.append(float(v))
        except Exception:
            continue
    order = sorted(range(len(times)), key=lambda i: times[i])
    return [times[i] for i in order], [vals[i] for i in order]

def nearest_value(times, vals, target_dt_utc, max_minutes=MAX_T_MATCH_MIN):
    if not times: return None
    i = bisect.bisect_left(times, target_dt_utc)
    cand = []
    if i < len(times): cand.append(i)
    if i > 0: cand.append(i-1)
    best = None; best_dt = None
    for idx in cand:
        dt_i = times[idx]
        if abs((dt_i - target_dt_utc).total_seconds()) <= max_minutes*60:
            if best is None or abs((dt_i - target_dt_utc).total_seconds()) < abs((best_dt - target_dt_utc).total_seconds()):
                best = vals[idx]; best_dt = dt_i
    return best

def classify_trend_from_series(times, vals, t0_utc, t1_utc, eps=EPS_TIDE):
    v0 = nearest_value(times, vals, t0_utc)
    v1 = nearest_value(times, vals, t1_utc)
    if v0 is None or v1 is None: return "unknown"
    dv = v1 - v0
    if dv > eps: return "rising"
    if dv < -eps: return "falling"
    return "slack"

# ----- Helpers for local-time handling -----
def parse_local_iso(s: str) -> dt.datetime | None:
    # Open-Meteo returns "YYYY-MM-DDTHH:00" in the requested timezone (no offset).
    try:
        # Attach TZ to interpret as local time
        return dt.datetime.fromisoformat(s).replace(tzinfo=TZ)
    except Exception:
        return None

def round_to_local_hour(t: dt.datetime) -> dt.datetime:
    # round to nearest hour in local time
    if t.minute >= 30:
        t = t + dt.timedelta(hours=1)
    return t.replace(minute=0, second=0, microsecond=0, tzinfo=TZ)

# ----- Apply CHS offsets in LOCAL time (fixes UTC/local key mismatch) -----
def apply_spot_tide_offsets(out_data: dict):
    """
    Use Beauport tide phase as baseline in LOCAL time and synthesize tide phase for
    Sainte-Anne, St-Jean, Ange-Gardien by applying CHS-style time offsets (minutes).
    """
    hours = out_data.get("hours", [])
    if not hours: return

    # Build local-time map: 'YYYY-MM-DDTHH:00' (as emitted in row['time']) -> Beauport tide
    base_local = {}
    for row in hours:
        t_str = row.get("time")
        if not t_str: continue
        tide = (row.get("beauport") or {}).get("tide", "unknown")
        base_local[t_str] = tide

    def probe_local(local_dt: dt.datetime) -> str:
        key = round_to_local_hour(local_dt).replace(tzinfo=None).isoformat(timespec="minutes")
        # Ensure format matches Open-Meteo's "YYYY-MM-DDTHH:MM"
        if len(key) == 16:  # e.g., 2025-08-15T13:00
            return base_local.get(key, "unknown")
        # Fallback: try constructing manually
        key2 = f"{local_dt.year:04d}-{local_dt.month:02d}-{local_dt.day:02d}T{local_dt.hour:02d}:00"
        return base_local.get(key2, "unknown")

    def shifted_state_local(t_local: dt.datetime, off_rise_min: int, off_fall_min: int) -> str:
        tr = probe_local(t_local - dt.timedelta(minutes=off_rise_min))
        tf = probe_local(t_local - dt.timedelta(minutes=off_fall_min))
        if tr == "rising":  return "rising"
        if tf == "falling": return "falling"
        if "slack" in (tr, tf): return "slack"
        return tr if tr != "unknown" else tf

    for row in hours:
        t_local = parse_local_iso(row.get("time",""))
        if not t_local: continue
        for spot_key, offs in TIDE_PHASE_OFFSETS.items():
            spot = (row.get(spot_key) or {})
            spot["tide"] = shifted_state_local(t_local, offs["rising"], offs["falling"])
            row[spot_key] = spot

# ----- MAIN -----
def main():
    start_local = NOW
    end_local   = NOW + dt.timedelta(hours=HOURS)
    result = {"generated_at": NOW.isoformat(), "hours": []}

    # 1) Wind per spot
    wind = {}
    for key, spot in SPOTS.items():
        try:
            wj = fetch_open_meteo_wind(spot["lat"], spot["lon"], start_local, end_local)
            wind[key] = {
                "time": wj["hourly"]["time"],                # local ISO (no tz offset)
                "avg":  wj["hourly"]["windspeed_10m"],       # kn
                "gust": wj["hourly"]["windgusts_10m"],       # kn
                "dir":  wj["hourly"]["winddirection_10m"],   # deg
            }
        except Exception as e:
            print(f"[WARN] Wind fetch failed for {spot['name']}: {e}", file=sys.stderr)
            wind[key] = {"time": [], "avg": [], "gust": [], "dir": []}

    # 2) Master timeline = Beauport wind hours
    timeline_local = wind.get("beauport", {}).get("time", [])
    if not timeline_local:
        with open("forecast.json", "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        return

    # Build UTC hour instants and hour pairs for tide classification (SPINE is UTC)
    utc_hours, utc_pairs = [], []
    for tloc in timeline_local:
        t0 = parse_local_iso(tloc).astimezone(dt.timezone.utc)
        a  = t0.isoformat().replace("+00:00","Z")
        b  = (t0 + dt.timedelta(hours=1)).isoformat().replace("+00:00","Z")
        utc_hours.append(a)
        utc_pairs.append((a,b))

    # 3) Pick the BEST SPINE proxy by trial coverage, then fetch full horizon
    flat_times = [ts for p in utc_pairs for ts in p]
    trial = flat_times[:48] if len(flat_times) >= 48 else flat_times

    best = None  # (ok_count, (lat,lon), test_map)
    for (plat, plon) in BASELINE_CANDIDATES:
        test_map = spine_levels_batch(plat, plon, trial, chunk_size=24)
        ok_test  = sum(1 for ts in trial if ts in test_map)
        print(f"[INFO] Baseline test @({plat},{plon}): {ok_test} OK trial points", file=sys.stderr)
        if best is None or ok_test > best[0]:
            best = (ok_test, (plat, plon), test_map)

    baseline_map = {}
    baseline_latlon = None
    if best and best[0] >= max(6, len(trial)//6):  # relaxed: ≥6 or ≥~16% of trial
        plat, plon = best[1]
        print(f"[INFO] Baseline SELECTED @({plat},{plon}) — fetching full horizon", file=sys.stderr)
        baseline_map = spine_levels_batch(plat, plon, flat_times, chunk_size=36)
        baseline_latlon = (plat, plon)
    else:
        print("[INFO] No suitable baseline from SPINE trials; tides will be 'unknown'", file=sys.stderr)

    # 4) Build baseline (Beauport) tide trend per hour (keyed by UTC hour string)
    baseline_trend = {}
    if baseline_map:
        times, vals = build_sorted_series(baseline_map)
        for (a,b) in utc_pairs:
            t0 = dt.datetime.fromisoformat(a.replace("Z","+00:00")).astimezone(dt.timezone.utc)
            t1 = dt.datetime.fromisoformat(b.replace("Z","+00:00")).astimezone(dt.timezone.utc)
            baseline_trend[a] = classify_trend_from_series(times, vals, t0, t1)
    else:
        print("[INFO] Baseline map empty; tides will be 'unknown'", file=sys.stderr)

    # 5) Compose hour rows (fill Beauport tide here)
    for i, t_loc_iso in enumerate(timeline_local):
        row = {"time": t_loc_iso}
        utc_iso = utc_hours[i]

        for key, spot in SPOTS.items():
            gust = avg = d = None
            try:
                idx = wind[key]["time"].index(t_loc_iso)
                gust = float(wind[key]["gust"][idx]) if wind[key]["gust"][idx] is not None else None
                avg  = float(wind[key]["avg"][idx])  if wind[key]["avg"][idx]  is not None else None
                d    = float(wind[key]["dir"][idx])  if wind[key]["dir"][idx]  is not None else None
            except Exception:
                pass

            tide_status = "unknown"
            if baseline_trend and key == "beauport":
                tide_status = baseline_trend.get(utc_iso, "unknown")

            # Go / No-go rules (gust-based)
            thr = THRESHOLD_GUST[key]
            if key == "beauport":
                go_flag = (gust is not None and gust >= thr)
            elif key == "ste_anne":
                go_flag = (gust is not None and gust >= thr and d is not None and in_SW(d) and tide_status == "rising")
            elif key == "st_jean":
                go_flag = (gust is not None and gust >= thr and d is not None and in_NE(d) and tide_status == "falling")
            elif key == "ange_gardien":
                go_flag = (gust is not None and gust >= thr and d is not None and in_SW(d) and tide_status == "rising")
            else:
                go_flag = False

            row[key] = {
                "wind_kn": round(gust, 1) if gust is not None else None,
                "wind_avg_kn": round(avg, 1) if avg is not None else None,
                "dir_deg": round(d) if d is not None else None,
                "tide": tide_status,
                "go": {
                    "beauport":     go_flag if key=="beauport"     else None,
                    "ste_anne":     go_flag if key=="ste_anne"     else None,
                    "st_jean":      go_flag if key=="st_jean"      else None,
                    "ange_gardien": go_flag if key=="ange_gardien" else None,
                }
            }

        result["hours"].append(row)

    # 6) Apply CHS-style time offsets in LOCAL time to synthesize tides for the other spots
    apply_spot_tide_offsets(result)

    # 7) Debug + metadata
    def count_trend(rows, key):
        return dict(Counter(r.get(key, {}).get("tide", "unknown") for r in rows))
    result["tide_baseline"] = {
        "lat": (baseline_latlon[0] if baseline_latlon else None),
        "lon": (baseline_latlon[1] if baseline_latlon else None),
        "max_match_minutes": MAX_T_MATCH_MIN,
        "note": "Non-Baseline tides are time-shifted from Beauport using local-time offsets.",
    }
    result["debug_counts"] = {k: count_trend(result["hours"], k) for k in SPOTS.keys()}
    result["wind_models"] = "Open-Meteo auto (no models= param)"

    with open("forecast.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
