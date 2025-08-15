# check_spots.py — robust wind-first timeline + graceful tide handling
# - Uses Open-Meteo for hourly gust/mean/dir (no key)
# - Uses DFO SPINE for water level when available (no key)
# - Timeline is taken from Beauport's wind hours so wind data is always present
# - If tide is missing/unknown, tide-dependent spots stay RED (not grey)

import json, datetime as dt, urllib.request, urllib.parse, sys
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
THRESHOLD_GUST = {"beauport": 10.0, "ste_anne": 10.0, "st_jean": 10.0}

# Direction sectors (deg, "from")
DIR_SW = (200, 250)  # Ste-Anne
DIR_NE = (30, 70)    # St-Jean

def is_dir_in_sector(deg, lo, hi):
    return (lo <= deg <= hi) if lo <= hi else (deg >= lo or deg <= hi)
def in_SW(deg): return is_dir_in_sector(deg, *DIR_SW)
def in_NE(deg): return is_dir_in_sector(deg, *DIR_NE)

def http_get_json(url):
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.load(r)

# ---------- WIND (Open-Meteo) ----------
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
    return http_get_json(f"{base}?{qs}")

# ---------- WATER LEVEL (DFO SPINE) ----------
def fetch_spine_levels(lat, lon, times_utc_iso):
    base = "https://api-spine.azure.cloud-nuage.dfo-mpo.gc.ca/rest/v1/waterLevel"
    q = []
    for t in times_utc_iso:
        q.append(("lat", f"{lat}"))
        q.append(("lon", f"{lon}"))
        q.append(("t", t))
    qs = urllib.parse.urlencode(q)
    data = http_get_json(f"{base}?{qs}")
    out = {}
    for it in data.get("responseItems", []):
        if it.get("status") == "OK" and "waterLevel" in it and "instant" in it:
            out[it["instant"]] = it["waterLevel"]
    return out  # dict[utc_iso] -> level (m)

def tide_trend(levels, idx):
    try:
        prev_h = float(levels[idx-1]) if idx-1 >= 0 and levels[idx-1] is not None else None
        cur_h  = float(levels[idx])   if levels[idx]   is not None else None
        next_h = float(levels[idx+1]) if idx+1 < len(levels) and levels[idx+1] is not None else None
    except (ValueError, TypeError):
        return "unknown"
    if prev_h is None or next_h is None or cur_h is None:
        return "unknown"
    if next_h > cur_h > prev_h: return "rising"
    if next_h < cur_h < prev_h: return "falling"
    if abs(next_h-cur_h) < 0.02 and abs(cur_h-prev_h) < 0.02: return "slack"
    return "rising" if next_h > prev_h else "falling" if next_h < prev_h else "unknown"

def main():
    start_local = NOW
    end_local = NOW + dt.timedelta(hours=HOURS)
    result = {"generated_at": NOW.isoformat(), "hours": []}

    # 1) Fetch wind for each spot
    wind = {}
    for key, spot in SPOTS.items():
        try:
            wjson = fetch_open_meteo_wind(spot["lat"], spot["lon"], start_local, end_local)
            wind[key] = {
                "time": wjson["hourly"]["time"],                # local ISO strings
                "avg":  wjson["hourly"]["windspeed_10m"],       # kn
                "gust": wjson["hourly"]["windgusts_10m"],       # kn
                "dir":  wjson["hourly"]["winddirection_10m"],   # deg
            }
        except Exception as e:
            print(f"[WARN] Wind fetch failed for {spot['name']}: {e}", file=sys.stderr)
            wind[key] = {"time": [], "avg": [], "gust": [], "dir": []}

    # 2) Use BEAUPORT's wind times as the master timeline (prevents grey "missing data")
    timeline_local = wind["beauport"]["time"]
    if not timeline_local:
        # If wind failed entirely, produce an empty scaffold to avoid crashes
        with open("forecast.json", "w", encoding="utf-8") as f:
            json.dump({"generated_at": NOW.isoformat(), "hours": []}, f, ensure_ascii=False, indent=2)
        return

    # Pre-build UTC times for SPINE aligned to the same hours
    timeline_utc_iso = []
    for tloc in timeline_local:
        # parse local time string and convert to UTC Z ISO
        t = dt.datetime.fromisoformat(tloc).replace(tzinfo=TZ)
        utc_iso = t.astimezone(dt.timezone.utc).isoformat().replace("+00:00","Z")
        timeline_utc_iso.append(utc_iso)

    # 3) Fetch SPINE water levels series per spot (aligned to our UTC timeline)
    tide_levels = {}
    for key, spot in SPOTS.items():
        try:
            lvl_by_instant = fetch_spine_levels(spot["lat"], spot["lon"], timeline_utc_iso)
            series = [lvl_by_instant.get(utc) for utc in timeline_utc_iso]
        except Exception as e:
            print(f"[WARN] SPINE fetch failed for {spot['name']}: {e}", file=sys.stderr)
            series = [None] * len(timeline_local)
        tide_levels[key] = series

    # 4) Compose rows using the master timeline strings
    for i, t_loc_iso in enumerate(timeline_local):
        row = {"time": t_loc_iso}
        for key in SPOTS.keys():
            # Align wind by string match (same provider & timezone, so exact strings)
            gust = avg = d = None
            try:
                w_times = wind[key]["time"]
                idx = w_times.index(t_loc_iso)
                gust = float(wind[key]["gust"][idx]) if wind[key]["gust"][idx] is not None else None
                avg  = float(wind[key]["avg"][idx])  if wind[key]["avg"][idx]  is not None else None
                d    = float(wind[key]["dir"][idx])  if wind[key]["dir"][idx]  is not None else None
            except Exception:
                # Even if this spot had a hole, BEAUPORT timeline keeps the row; leave gust=None -> UI will show neutral or red as appropriate
                pass

            # Tide status (graceful): compute if we have SPINE series; else "unknown"
            tide_status = tide_trend(tide_levels.get(key, []), i) if tide_levels.get(key) else "unknown"

            # Evaluate rules on gust
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
