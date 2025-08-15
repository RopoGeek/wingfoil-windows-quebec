# check_spots.py — Fast + robust tides via a single SPINE baseline + per-spot time offsets
# Wind: Open-Meteo (gust/mean/dir) — no key
# Tide: DFO SPINE water-level forecast at one reliable "baseline" location (batched to avoid 414)
#       Other spots' tide states are estimated by time-shifting the baseline (separate offsets for rising/falling).
#
# Edit these OFFSETS to tune timing per spot. Units: minutes.
# Negative = that spot's tide happens EARLIER than baseline. Positive = LATER than baseline.

import json, datetime as dt, urllib.request, urllib.parse, sys, time
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/Toronto")
NOW = dt.datetime.now(TZ).replace(minute=0, second=0, microsecond=0)
HOURS = 96  # 4 days

# ----------------------- CONFIG -----------------------
# Display/wind coordinates (unchanged)
SPOTS = {
    "beauport": {"name": "Baie de Beauport", "lat": 46.8598, "lon": -71.2006},
    "ste_anne": {"name": "Quai Ste-Anne-de-Beaupré", "lat": 47.0153, "lon": -70.9280},
    "st_jean":  {"name": "Quai St-Jean, Île d’Orléans", "lat": 46.8577, "lon": -70.8169},
}

# Per-spot gust thresholds (kn)
THRESHOLD_GUST = {"beauport": 10.0, "ste_anne": 10.0, "st_jean": 10.0}

# Direction sectors (deg, coming FROM)
DIR_SW = (200, 250)  # Ste-Anne
DIR_NE = (30, 70)    # St-Jean

# Baseline tide candidates (mid-channel points around Québec City)
# We'll try these in order; first that returns OK points becomes the baseline.
BASELINE_CANDIDATES = [
    (46.8609, -71.1835),  # your earlier Beauport seed that once worked
    (46.8420, -71.2100),  # mid-channel S of Beauport
    (46.8750, -71.1600),  # mid-channel NE of Beauport
    (46.8350, -71.2450),  # mid-channel SW
]

# Offsets (minutes) relative to the baseline for each spot.
# We allow different values for rising vs falling because rising tends to propagate faster.
# Tune these numbers after a few days of observation. Signs: negative = earlier than baseline.
TIDE_OFFSETS_MIN = {
    "beauport": {"rising": 0,   "falling": 0},    # the baseline itself
    "ste_anne": {"rising": -25, "falling": -15},  # Ste-Anne generally downriver → a bit earlier
    "st_jean":  {"rising": -15, "falling": -10},  # Île d’Orléans (St-Jean) slightly earlier than QC
}

# How strict to be when deciding rising/falling between consecutive hours (meters)
EPS_TIDE = 0.02

# ----------------------- HELPERS -----------------------
def is_dir_in_sector(deg, lo, hi):
    return (lo <= deg <= hi) if lo <= hi else (deg >= lo or deg <= hi)

def in_SW(deg): return is_dir_in_sector(deg, *DIR_SW)
def in_NE(deg): return is_dir_in_sector(deg, *DIR_NE)

def http_get_json(url, timeout=45):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)

# ----------------------- WIND (Open-Meteo) -----------------------
def fetch_open_meteo_wind(lat, lon, start_dt, end_dt):
    base = "https://api.open-meteo.com/v1/forecast"
    qs = urllib.parse.urlencode({
        "latitude": lat, "longitude": lon,
        "hourly": "windspeed_10m,windgusts_10m,winddirection_10m",
        "wind_speed_unit": "kn",
        "timezone": "America/Toronto",
        "start_date": start_dt.date().isoformat(),
        "end_date": end_dt.date().isoformat(),
    })
    return http_get_json(f"{base}?{qs}")

# ----------------------- TIDE (SPINE baseline) -----------------------
SPINE_BASE = "https://api-spine.azure.cloud-nuage.dfo-mpo.gc.ca/rest/v1/waterLevel"

def spine_levels_batch(lat, lon, utc_list, chunk_size=36, pause=0.2, max_retries=2):
    """Fetch SPINE water levels for many times, in chunks (avoid 414). Return dict[utc_iso]->level (meters)."""
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
                    st = it.get("status")
                    if st == "OK":
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

def classify_trend_from_map(spine_map, t_utc_iso, t1_utc_iso, eps=EPS_TIDE):
    v0 = spine_map.get(t_utc_iso)
    v1 = spine_map.get(t1_utc_iso)
    if v0 is None or v1 is None:
        return "unknown"
    try:
        dv = float(v1) - float(v0)
    except Exception:
        return "unknown"
    if dv > eps: return "rising"
    if dv < -eps: return "falling"
    return "slack"

# ----------------------- MAIN -----------------------
def main():
    start_local = NOW
    end_local   = NOW + dt.timedelta(hours=HOURS)
    result = {"generated_at": NOW.isoformat(), "hours": []}

    # 1) Wind
    wind = {}
    for key, spot in SPOTS.items():
        try:
            wj = fetch_open_meteo_wind(spot["lat"], spot["lon"], start_local, end_local)
            wind[key] = {
                "time": wj["hourly"]["time"],                # local ISO
                "avg":  wj["hourly"]["windspeed_10m"],       # kn
                "gust": wj["hourly"]["windgusts_10m"],       # kn
                "dir":  wj["hourly"]["winddirection_10m"],   # deg
            }
        except Exception as e:
            print(f"[WARN] Wind fetch failed for {spot['name']}: {e}", file=sys.stderr)
            wind[key] = {"time": [], "avg": [], "gust": [], "dir": []}

    # 2) Master timeline = Beauport wind hours (prevents grey)
    timeline_local = wind.get("beauport", {}).get("time", [])
    if not timeline_local:
        with open("forecast.json", "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        return

    # Build UTC hours + hour pairs
    utc_hours, utc_pairs = [], []
    for tloc in timeline_local:
        t0 = dt.datetime.fromisoformat(tloc).replace(tzinfo=TZ).astimezone(dt.timezone.utc)
        a  = t0.isoformat().replace("+00:00","Z")
        b  = (t0 + dt.timedelta(hours=1)).isoformat().replace("+00:00","Z")
        utc_hours.append(a)
        utc_pairs.append((a,b))

    # 3) Find a baseline tide series: try candidates in order, pick the first that returns enough points
    flat_times = [ts for p in utc_pairs for ts in p]  # t and t+1 for each hour
    baseline_map = {}
    baseline_latlon = None
    for (plat, plon) in BASELINE_CANDIDATES:
        test_map = spine_levels_batch(plat, plon, flat_times[:48], chunk_size=24)  # small test (~24 hours worth)
        ok_test  = sum(1 for ts in flat_times[:48] if ts in test_map)
        print(f"[INFO] Baseline test @({plat},{plon}): {ok_test} OK trial points", file=sys.stderr)
        if ok_test >= 12:  # need at least half of the small test points
            print(f"[INFO] Baseline SELECTED @({plat},{plon}) — fetching full horizon", file=sys.stderr)
            baseline_map = spine_levels_batch(plat, plon, flat_times, chunk_size=36)
            baseline_latlon = (plat, plon)
            break

    # Build baseline trend per hour (if we have baseline_map)
    baseline_trend = {}  # dict[utc_hour] -> "rising"/"falling"/"slack"/"unknown"
    for (a,b) in utc_pairs:
        baseline_trend[a] = classify_trend_from_map(baseline_map, a, b)

    # Helper to get trend at a *shifted* time:
    # shift_minutes >0 means look LATER on the baseline; <0 look EARLIER.
    def get_baseline_trend_shifted(utc_hour_iso, shift_minutes):
        try:
            t = dt.datetime.fromisoformat(utc_hour_iso.replace("Z","+00:00"))
        except Exception:
            return "unknown"
        t_shift = t + dt.timedelta(minutes=shift_minutes)
        # snap to nearest hour in utc_hours
        # (timeline is hourly; pick nearest index)
        diffs = [(abs((t_shift - dt.datetime.fromisoformat(u.replace("Z","+00:00"))).total_seconds()), idx) for idx,u in enumerate(utc_hours)]
        if not diffs:
            return "unknown"
        _, idx = min(diffs, key=lambda x: x[0])
        return baseline_trend.get(utc_hours[idx], "unknown")

    # 4) Compose grid rows
    for i, t_loc_iso in enumerate(timeline_local):
        row = {"time": t_loc_iso}
        utc_hour = utc_hours[i]

        for key, spot in SPOTS.items():
            gust = avg = d = None
            try:
                idx = wind[key]["time"].index(t_loc_iso)
                gust = float(wind[key]["gust"][idx]) if wind[key]["gust"][idx] is not None else None
                avg  = float(wind[key]["avg"][idx])  if wind[key]["avg"][idx]  is not None else None
                d    = float(wind[key]["dir"][idx])  if wind[key]["dir"][idx]  is not None else None
            except Exception:
                pass

            # Tide trend:
            if baseline_latlon is None:
                tide_status = "unknown"
            else:
                # Determine which offset to use (based on *baseline* phase at this hour)
                phase_now = baseline_trend.get(utc_hour, "unknown")
                offs = TIDE_OFFSETS_MIN[key]
                shift = offs["rising"] if phase_now == "rising" else offs["falling"] if phase_now == "falling" else 0
                tide_status = get_baseline_trend_shifted(utc_hour, shift)

            # Go / No-go rules (gust-based)
            thr = THRESHOLD_GUST[key]
            if key == "beauport":
                go_flag = (gust is not None and gust >= thr)
            elif key == "ste_anne":
                go_flag = (gust is not None and gust >= thr and d is not None and in_SW(d) and tide_status == "rising")
            elif key == "st_jean":
                go_flag = (gust is not None and gust >= thr and d is not None and in_NE(d) and tide_status == "falling")
            else:
                go_flag = False

            row[key] = {
                "wind_kn": round(gust, 1) if gust is not None else None,       # gusts
                "wind_avg_kn": round(avg, 1) if avg is not None else None,     # mean wind (info only)
                "dir_deg": round(d) if d is not None else None,
                "tide": tide_status,
                "go": {
                    "beauport": go_flag if key=="beauport" else None,
                    "ste_anne": go_flag if key=="ste_anne" else None,
                    "st_jean":  go_flag if key=="st_jean" else None
                }
            }

        result["hours"].append(row)

    # Add a little metadata so you can see which baseline was used in Actions logs (and if none)
    result["tide_baseline"] = {
        "lat": baseline_latlon[0] if baseline_latlon else None,
        "lon": baseline_latlon[1] if baseline_latlon else None,
        "note": "Tide for Ste-Anne & St-Jean is time-shifted from the baseline using TIDE_OFFSETS_MIN."
    }

    with open("forecast.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
