# check_spots.py — gust-based rules + SPINE tide using channel-proxy coordinates (robust)
# Wind: Open-Meteo (gust/mean/dir) — no key
# Tide: DFO SPINE water-level forecast at nearby channel points — no key

import json, datetime as dt, urllib.request, urllib.parse, sys
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/Toronto")
NOW = dt.datetime.now(TZ).replace(minute=0, second=0, microsecond=0)
HOURS = 96  # look ahead

SPOTS = {
    "beauport": {
        "name": "Baie de Beauport",
        "lat": 46.8598, "lon": -71.2006,           # display/ wind point
        "spine_lat": 46.8609, "spine_lon": -71.1835  # channel proxy
    },
    "ste_anne": {
        "name": "Quai Ste-Anne-de-Beaupré",
        "lat": 47.0153, "lon": -70.9280,
        "spine_lat": 47.0088, "spine_lon": -70.9250
    },
    "st_jean": {
        "name": "Quai St-Jean, Île d’Orléans",
        "lat": 46.8577, "lon": -70.8169,
        "spine_lat": 46.8740, "spine_lon": -70.8140
    },
}

# Gust thresholds (kn) — you asked for 10 kn at all 3 spots
THRESHOLD_GUST = {"beauport": 10.0, "ste_anne": 10.0, "st_jean": 10.0}

# Direction sectors (deg "from")
DIR_SW = (200, 250)  # Ste-Anne
DIR_NE = (30, 70)    # St-Jean

def is_dir_in_sector(deg, lo, hi): return (lo <= deg <= hi) if lo <= hi else (deg >= lo or deg <= hi)
def in_SW(deg): return is_dir_in_sector(deg, *DIR_SW)
def in_NE(deg): return is_dir_in_sector(deg, *DIR_NE)

def http_get_json(url, timeout=45):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)

# -------- WIND (Open-Meteo) ----------
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

# -------- TIDE (SPINE) ----------
SPINE_BASE = "https://api-spine.azure.cloud-nuage.dfo-mpo.gc.ca/rest/v1/waterLevel"

def spine_levels_for_hours(lat, lon, utc_list):
    # Batch request: repeat lat, lon, t for all hours in list
    q = []
    for t in utc_list:
        q += [("lat", f"{lat}"), ("lon", f"{lon}"), ("t", t)]
    url = f"{SPINE_BASE}?{urllib.parse.urlencode(q)}"
    try:
        data = http_get_json(url)
        items = data.get("responseItems", [])
        out = {it.get("instant"): it.get("waterLevel") for it in items if it.get("status") == "OK"}
        return out
    except Exception as e:
        print(f"[WARN] SPINE fetch failed @({lat},{lon}): {e}", file=sys.stderr)
        return {}

def classify_trend(spine_map, t_utc_iso, t1_utc_iso):
    v0 = spine_map.get(t_utc_iso)
    v1 = spine_map.get(t1_utc_iso)
    if v0 is None or v1 is None: return "unknown"
    try:
        dv = float(v1) - float(v0)
    except Exception:
        return "unknown"
    if dv > 0.02: return "rising"
    if dv < -0.02: return "falling"
    return "slack"

def main():
    start_local = NOW
    end_local = NOW + dt.timedelta(hours=HOURS)
    result = {"generated_at": NOW.isoformat(), "hours": []}

    # 1) Wind for each spot
    wind = {}
    for key, spot in SPOTS.items():
        try:
            wj = fetch_open_meteo_wind(spot["lat"], spot["lon"], start_local, end_local)
            wind[key] = {
                "time": wj["hourly"]["time"],                # local ISO strings
                "avg":  wj["hourly"]["windspeed_10m"],       # kn
                "gust": wj["hourly"]["windgusts_10m"],       # kn
                "dir":  wj["hourly"]["winddirection_10m"],   # deg
            }
        except Exception as e:
            print(f"[WARN] Wind fetch failed for {spot['name']}: {e}", file=sys.stderr)
            wind[key] = {"time": [], "avg": [], "gust": [], "dir": []}

    # 2) Master timeline = Beauport wind times (ensures we always render)
    timeline_local = wind.get("beauport", {}).get("time", [])
    if not timeline_local:
        with open("forecast.json", "w", encoding="utf-8") as f:
            json.dump({"generated_at": NOW.isoformat(), "hours": []}, f, ensure_ascii=False, indent=2)
        return

    # Build paired UTC times [t, t+1h] for SPINE
    utc_pairs = []
    for tloc in timeline_local:
        t0 = dt.datetime.fromisoformat(tloc).replace(tzinfo=TZ).astimezone(dt.timezone.utc)
        t1 = t0 + dt.timedelta(hours=1)
        utc_pairs.append( (t0.isoformat().replace("+00:00","Z"), t1.isoformat().replace("+00:00","Z")) )

    # 3) Fetch SPINE for each spot at the proxy channel location
    spine_maps = {}
    # We’ll request all t and all t+1h in a single batch per spot
    for key, spot in SPOTS.items():
        t_list = []
        for a,b in utc_pairs:
            t_list.append(a); t_list.append(b)
        m = spine_levels_for_hours(spot["spine_lat"], spot["spine_lon"], t_list)
        if not m:
            print(f"[INFO] SPINE returned empty map near {spot['name']} (proxy {spot['spine_lat']},{spot['spine_lon']})", file=sys.stderr)
        spine_maps[key] = m

    # 4) Compose rows
    for i, t_loc_iso in enumerate(timeline_local):
        row = {"time": t_loc_iso}
        t0_utc, t1_utc = utc_pairs[i]

        for key, spot in SPOTS.items():
            gust = avg = d = None
            try:
                w_times = wind[key]["time"]
                idx = w_times.index(t_loc_iso)
                gust = float(wind[key]["gust"][idx]) if wind[key]["gust"][idx] is not None else None
                avg  = float(wind[key]["avg"][idx])  if wind[key]["avg"][idx]  is not None else None
                d    = float(wind[key]["dir"][idx])  if wind[key]["dir"][idx]  is not None else None
            except Exception:
                pass

            tide_status = classify_trend(spine_maps.get(key, {}), t0_utc, t1_utc)

            # Evaluate rules (gust-based)
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
                "wind_kn": round(gust, 1) if gust is not None else None,
                "wind_avg_kn": round(avg, 1) if avg is not None else None,
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
