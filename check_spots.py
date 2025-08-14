# check_spots.py
# Fetches hourly wind (kn, dir) + tide height (m) from Open-Meteo for 72h,
# applies your rules, and writes forecast.json consumed by index.html.
#
# How to run locally (optional):
#   python3 check_spots.py
#
# No extra Python packages needed.

import json, math, datetime as dt, urllib.request, urllib.parse, sys
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/Toronto")
NOW = dt.datetime.now(TZ).replace(minute=0, second=0, microsecond=0)
HOURS = 96  # look ahead

SPOTS = {
    "beauport": {"name": "Baie de Beauport", "lat": 46.8598, "lon": -71.2006},
    "ste_anne": {"name": "Quai Ste-Anne-de-Beaupré", "lat": 47.0153, "lon": -70.9280},
    "st_jean": {"name": "Quai St-Jean, Île d’Orléans", "lat": 46.8577, "lon": -70.8169},
}

def is_dir_in_sector(deg, lo, hi):
    if lo <= hi:
        return lo <= deg <= hi
    return deg >= lo or deg <= hi

def in_SW(deg): return is_dir_in_sector(deg, 200, 250)
def in_NE(deg): return is_dir_in_sector(deg, 30, 70)

def fetch_open_meteo_wind(lat, lon, start_dt, end_dt):
    base = "https://api.open-meteo.com/v1/forecast"
    qs = urllib.parse.urlencode({
        "latitude": lat, "longitude": lon,
        "hourly": "windspeed_10m,winddirection_10m",
        "wind_speed_unit": "kn",
        "timezone": "America/Toronto",
        "start_date": start_dt.date().isoformat(),
        "end_date": end_dt.date().isoformat()
    })
    with urllib.request.urlopen(f"{base}?{qs}") as r:
        return json.load(r)

def fetch_open_meteo_tide(lat, lon, start_dt, end_dt):
    base = "https://marine-api.open-meteo.com/v1/marine"
    qs = urllib.parse.urlencode({
        "latitude": lat, "longitude": lon,
        "hourly": "tide_height",
        "timezone": "America/Toronto",
        "start_date": start_dt.date().isoformat(),
        "end_date": end_dt.date().isoformat()
    })
    with urllib.request.urlopen(f"{base}?{qs}") as r:
        return json.load(r)

def tide_trend(series, idx):
    # Determine rising/falling around hour idx using surrounding values
    # Return "rising", "falling", "slack", or "unknown"
    try:
        prev_h = float(series[idx-1]) if idx-1 >= 0 and series[idx-1] is not None else None
        cur_h  = float(series[idx])   if series[idx] is not None else None
        next_h = float(series[idx+1]) if idx+1 < len(series) and series[idx+1] is not None else None
    except (ValueError, TypeError):
        return "unknown"
    if prev_h is None or next_h is None or cur_h is None:
        return "unknown"
    if next_h > cur_h > prev_h:
        return "rising"
    if next_h < cur_h < prev_h:
        return "falling"
    if abs(next_h - cur_h) < 0.02 and abs(cur_h - prev_h) < 0.02:
        return "slack"
    if next_h > prev_h:
        return "rising"
    if next_h < prev_h:
        return "falling"
    return "unknown"

def main():
    start = NOW
    end = NOW + dt.timedelta(hours=HOURS)
    result = {"generated_at": NOW.isoformat(), "hours": []}

    spot_data = {}
    for key, spot in SPOTS.items():
        # Wind
        try:
            wjson = fetch_open_meteo_wind(spot["lat"], spot["lon"], start, end)
            hours = wjson["hourly"]["time"]
            winds = wjson["hourly"]["windspeed_10m"]
            dirs  = wjson["hourly"]["winddirection_10m"]
        except Exception as e:
            print(f"[WARN] Wind fetch failed for {spot['name']}: {e}", file=sys.stderr)
            hours, winds, dirs = [], [], []

        # Tide
        tide_times, tide_vals = [], []
        try:
            tjson = fetch_open_meteo_tide(spot["lat"], spot["lon"], start, end)
            tide_times = tjson.get("hourly", {}).get("time", [])
            tide_vals  = tjson.get("hourly", {}).get("tide_height", [])
        except Exception as e:
            print(f"[WARN] Tide fetch failed for {spot['name']}: {e}", file=sys.stderr)

        spot_data[key] = {"hours": hours, "winds": winds, "dirs": dirs,
                          "tide_times": tide_times, "tide_vals": tide_vals}

    # Use Beauport's wind timeline as the backbone
    timeline = spot_data["beauport"]["hours"] or []
    for i, t in enumerate(timeline):
        row = {"time": t}
        for key in SPOTS.keys():
            w = d = None
            tide_status = "unknown"
            # align wind by timestamp
            try:
                idx = spot_data[key]["hours"].index(t)
                w = float(spot_data[key]["winds"][idx])
                d = float(spot_data[key]["dirs"][idx])
            except Exception:
                pass
            # align tide by timestamp
            try:
                tidx = spot_data[key]["tide_times"].index(t)
                tide_status = tide_trend(spot_data[key]["tide_vals"], tidx)
            except Exception:
                pass

            go_flag = False
            if key == "beauport":
                go_flag = (w is not None and w >= 10.0)
            elif key == "ste_anne":
                go_flag = (w is not None and w >= 12.0 and d is not None and in_SW(d) and tide_status == "rising")
            elif key == "st_jean":
                go_flag = (w is not None and w >= 12.0 and d is not None and in_NE(d) and tide_status == "falling")

            row[key] = {
                "wind_kn": round(w, 1) if w is not None else None,
                "dir_deg": round(d) if d is not None else None,
                "tide": tide_status,
                "go": {
                    "beauport": True  if key=="beauport" and go_flag else False if key=="beauport" else None,
                    "ste_anne": True  if key=="ste_anne"  and go_flag else False if key=="ste_anne"  else None,
                    "st_jean":  True  if key=="st_jean"   and go_flag else False if key=="st_jean"   else None
                }
            }
        result["hours"].append(row)

    with open("forecast.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
