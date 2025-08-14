# check_spots.py — gust-based rules + robust tide "finder" around each spot
# - Uses Open-Meteo (no API key) for hourly: wind gusts, mean wind, direction, and tide height
# - "Go" is evaluated on GUST speed (kn)
# - Tide: if the exact coordinate has no tide data, probe nearby points and use the first valid series

import json, datetime as dt, urllib.request, urllib.parse, sys, math
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/Toronto")
NOW = dt.datetime.now(TZ).replace(minute=0, second=0, microsecond=0)
HOURS = 96  # look ahead

SPOTS = {
    "beauport": {"name": "Baie de Beauport", "lat": 46.8598, "lon": -71.2006},
    "ste_anne": {"name": "Quai Ste-Anne-de-Beaupré", "lat": 47.0153, "lon": -70.9280},
    "st_jean":  {"name": "Quai St-Jean, Île d’Orléans", "lat": 46.8577, "lon": -70.8169},
}

# Gust thresholds (kn)
THRESHOLD_GUST = {
    "beauport": 10.0,
    "ste_anne": 10.0,  # was 12.0
    "st_jean":  10.0,  # was 12.0
}

# Direction sectors (degrees, "from"):
DIR_SW = (200, 250)  # Ste-Anne
DIR_NE = (30, 70)    # St-Jean

def is_dir_in_sector(deg, lo, hi):
    return (lo <= deg <= hi) if lo <= hi else (deg >= lo or deg <= hi)

def in_SW(deg): return is_dir_in_sector(deg, *DIR_SW)
def in_NE(deg): return is_dir_in_sector(deg, *DIR_NE)

def om_get(url):
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.load(r)

def fetch_open_meteo_wind(lat, lon, start_dt, end_dt):
    base = "https://api.open-meteo.com/v1/forecast"
    qs = urllib.parse.urlencode({
        "latitude": lat, "longitude": lon,
        "hourly": "windspeed_10m,windgusts_10m,winddirection_10m",
        "wind_speed_unit": "kn",
        "timezone": "America/Toronto",
        "start_date": start_dt.date().isoformat(),
        "end_date": end_dt.date().isoformat()
    })
    return om_get(f"{base}?{qs}")

def fetch_open_meteo_tide(lat, lon, start_dt, end_dt):
    base = "https://marine-api.open-meteo.com/v1/marine"
    qs = urllib.parse.urlencode({
        "latitude": lat, "longitude": lon,
        "hourly": "tide_height",
        "timezone": "America/Toronto",
        "start_date": start_dt.date().isoformat(),
        "end_date": end_dt.date().isoformat()
    })
    return om_get(f"{base}?{qs}")

def tide_trend(series, idx):
    try:
        prev_h = float(series[idx-1]) if idx-1 >= 0 and series[idx-1] is not None else None
        cur_h  = float(series[idx])   if series[idx] is not None else None
        next_h = float(series[idx+1]) if idx+1 < len(series) and series[idx+1] is not None else None
    except (ValueError, TypeError):
        return "unknown"
    if prev_h is None or next_h is None or cur_h is None:
        return "unknown"
    if next_h > cur_h > prev_h: return "rising"
    if next_h < cur_h < prev_h: return "falling"
    if abs(next_h-cur_h) < 0.02 and abs(cur_h-prev_h) < 0.02: return "slack"
    return "rising" if next_h > prev_h else "falling" if next_h < prev_h else "unknown"

def has_meaningful_tide(vals):
    # At least some non-None values and not all identical
    clean = [v for v in vals if v is not None]
    if len(clean) < 3:
        return False
    return not all(abs(clean[i] - clean[0]) < 1e-6 for i in range(1, len(clean)))

def probe_coords(lat, lon):
    """
    Generate nearby coordinates to try for tide data.
    Start with the exact point, then a small ring around (~0.05°), then a bit wider (~0.1°).
    """
    deltas = [0.0, 0.05, -0.05, 0.05, -0.05, 0.0, 0.0, 0.1, -0.1, 0.1, -0.1]
    pairs = []
    # exact
    pairs.append((lat, lon))
    # small ring (N,S,E,W)
    pairs += [(lat+deltas[1], lon),
              (lat+deltas[2], lon),
              (lat, lon+deltas[1]),
              (lat, lon+deltas[2])]
    # diagonals small
    pairs += [(lat+deltas[1], lon+deltas[1]),
              (lat+deltas[1], lon+deltas[2]),
              (lat+deltas[2], lon+deltas[1]),
              (lat+deltas[2], lon+deltas[2])]
    # slightly wider ring
    pairs += [(lat+0.1, lon), (lat-0.1, lon), (lat, lon+0.1), (lat, lon-0.1)]
    return pairs

def find_working_tide_series(lat, lon, start_dt, end_dt):
    """
    Try multiple nearby points; return (times, values, used_lat, used_lon) for the first that has meaningful tide.
    If none work, return ([], [], None, None).
    """
    for (tlat, tlon) in probe_coords(lat, lon):
        try:
            tj = fetch_open_meteo_tide(tlat, tlon, start_dt, end_dt)
            times = tj.get("hourly", {}).get("time", [])
            vals  = tj.get("hourly", {}).get("tide_height", [])
            if times and vals and has_meaningful_tide(vals):
                return times, vals, tlat, tlon
        except Exception as e:
            print(f"[WARN] Tide fetch failed @ ({tlat:.4f},{tlon:.4f}): {e}", file=sys.stderr)
    return [], [], None, None

def main():
    start = NOW
    end = NOW + dt.timedelta(hours=HOURS)
    result = {"generated_at": NOW.isoformat(), "hours": []}

    spot_data = {}
    for key, spot in SPOTS.items():
        # Wind (gust + mean + direction)
        try:
            wjson = fetch_open_meteo_wind(spot["lat"], spot["lon"], start, end)
            hours = wjson["hourly"]["time"]
            wind_avg = wjson["hourly"]["windspeed_10m"]      # kn
            wind_gust = wjson["hourly"]["windgusts_10m"]     # kn
            dirs  = wjson["hourly"]["winddirection_10m"]     # deg
        except Exception as e:
            print(f"[WARN] Wind fetch failed for {spot['name']}: {e}", file=sys.stderr)
            hours, wind_avg, wind_gust, dirs = [], [], [], []

        # Tide (robust: try nearby points)
        tide_times, tide_vals, used_lat, used_lon = find_working_tide_series(
            spot["lat"], spot["lon"], start, end
        )
        if not tide_times:
            print(f"[INFO] No tide found near {spot['name']} ({spot['lat']},{spot['lon']})", file=sys.stderr)
        else:
            print(f"[INFO] Tide source for {spot['name']}: ({used_lat:.4f},{used_lon:.4f})", file=sys.stderr)

        spot_data[key] = {
            "hours": hours, "avg": wind_avg, "gust": wind_gust, "dirs": dirs,
            "tide_times": tide_times, "tide_vals": tide_vals
        }

    # Use Beauport timeline as backbone
    timeline = spot_data["beauport"]["hours"] or []
    for t in timeline:
        row = {"time": t}
        for key in SPOTS.keys():
            gust = avg = d = None
            tide_status = "unknown"

            # align wind arrays at timestamp t
            try:
                idx = spot_data[key]["hours"].index(t)
                gust = float(spot_data[key]["gust"][idx]) if spot_data[key]["gust"][idx] is not None else None
                avg  = float(spot_data[key]["avg"][idx])  if spot_data[key]["avg"][idx]  is not None else None
                d    = float(spot_data[key]["dirs"][idx]) if spot_data[key]["dirs"][idx] is not None else None
            except Exception:
                pass

            # align tide by timestamp t (if any)
            try:
                tidx = spot_data[key]["tide_times"].index(t)
                tide_status = tide_trend(spot_data[key]["tide_vals"], tidx)
            except Exception:
                pass

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
                "wind_avg_kn": round(avg, 1) if avg is not None else None,     # mean
                "dir_deg": round(d) if d is not None else None,
                "tide": tide_status,
                "go": {
                    "beauport": go_flag if key=="beauport" else None,
                    "ste_anne": go_flag if key=="ste_anne" else None,
                    "st_jean":  go_flag if key=="st_jean" else None
                }
            }
        result["hours"].append(row)

    with open("forecast.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
