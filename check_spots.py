# check_spots.py — gust-based rules + CHS IWLS tide predictions (official)
# - Wind (gust/mean/dir): Open-Meteo (no key)
# - Tide: CHS IWLS API (official) predicted water levels (no key), nearest station to each spot
# - "Go" uses gusts (kn). Tide trend computed as rising/falling/slack from consecutive predictions.

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
THRESHOLD_GUST = {"beauport": 10.0, "ste_anne": 10.0, "st_jean": 10.0}

# Direction sectors (deg, "from"):
DIR_SW = (200, 250)  # Ste-Anne
DIR_NE = (30, 70)    # St-Jean

# ----------------- helpers -----------------
def is_dir_in_sector(deg, lo, hi):
    return (lo <= deg <= hi) if lo <= hi else (deg >= lo or deg <= hi)
def in_SW(deg): return is_dir_in_sector(deg, *DIR_SW)
def in_NE(deg): return is_dir_in_sector(deg, *DIR_NE)

def http_get_json(url, timeout=45):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)

# ----------------- WIND: Open-Meteo -----------------
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

# ----------------- TIDE: CHS IWLS (official) -----------------
# Docs/notes:
# - CHS says former tides.gc.ca web services moved to the IWLS API. :contentReference[oaicite:1]{index=1}
# - CHS “Web services offered” page lists IWLS endpoints incl. stations, tide tables, data/time series. :contentReference[oaicite:2]{index=2}
# - Swagger: https://api-iwls.dfo-mpo.gc.ca (and related v3 api-docs). :contentReference[oaicite:3]{index=3}
#
# We’ll:
#   1) find nearest station with predictions via /stations near lat/lon
#   2) pull predicted heights via the predictions/data endpoint for begin/end (UTC)
#   3) align them to our hourly timeline & compute rising/falling/slack

IWLS_BASE = "https://api-iwls.dfo-mpo.gc.ca"

def iwls_nearest_station(lat, lon, radius_km=30):
    """
    Find the nearest prediction-capable station to (lat, lon).
    We query a small bounding box + filter for hasTideTable or hasPredictions in metadata.
    """
    # Build a rough bbox (~radius_km). 1 deg lat ~ 111 km; 1 deg lon ~ 75 km near Quebec City.
    dlat = radius_km / 111.0
    dlon = radius_km / 75.0
    minLat, maxLat = lat - dlat, lat + dlat
    minLon, maxLon = lon - dlon, lon + dlon
    bbox = f"{minLon:.6f},{minLat:.6f},{maxLon:.6f},{maxLat:.6f}"
    # Try v3 stations search with bbox
    urls = [
        f"{IWLS_BASE}/v3/stations?bbox={bbox}",
        f"{IWLS_BASE}/stations?bbox={bbox}",  # legacy path fallback
    ]
    for url in urls:
        try:
            data = http_get_json(url)
            items = data.get("items") or data.get("stations") or data
            if not isinstance(items, list):
                continue
            # Choose the closest station that advertises predictions/tables if such flags exist
            best = None
            def dist_km(a,b):  # simple haversine-lite
                from math import radians, cos, sin, asin, sqrt
                rlat1, rlon1, rlat2, rlon2 = map(radians, [a[0],a[1],b[0],b[1]])
                dlon = rlon2-rlon1; dlat = rlat2-rlat1
                x = sin(dlat/2)**2 + cos(rlat1)*cos(rlat2)*sin(dlon/2)**2
                return 6371.0*2*asin(sqrt(x))
            for st in items:
                slat = st.get("latitude") or st.get("lat")
                slon = st.get("longitude") or st.get("lon")
                if slat is None or slon is None: continue
                flags = json.dumps(st).lower()
                # Prefer stations that clearly have predictions/tide tables
                prefers = ("tide" in flags or "prediction" in flags or "table" in flags)
                d = dist_km((lat,lon), (float(slat), float(slon)))
                score = (0 if prefers else 1, d)  # prefer predictors, then nearest
                if best is None or score < best[0]:
                    best = (score, {"id": st.get("id") or st.get("stationId") or st.get("code"), "lat": float(slat), "lon": float(slon)})
            if best:
                return best[1]
        except Exception as e:
            print(f"[WARN] IWLS stations search failed: {e}", file=sys.stderr)
    return None

def iwls_predicted_levels(station_id, begin_utc_iso, end_utc_iso):
    """
    Fetch predicted water levels for a station between begin/end (UTC ISO).
    IWLS provides multiple endpoints; we try common ones for compatibility.
    Returns dict[utc_iso] -> level (meters).
    """
    candidates = [
        f"{IWLS_BASE}/v3/stations/{station_id}/predictions?begin={urllib.parse.quote(begin_utc_iso)}&end={urllib.parse.quote(end_utc_iso)}",
        f"{IWLS_BASE}/stations/{station_id}/predictions?begin={urllib.parse.quote(begin_utc_iso)}&end={urllib.parse.quote(end_utc_iso)}",
        f"{IWLS_BASE}/v3/stations/{station_id}/data?datatype=predictions&begin={urllib.parse.quote(begin_utc_iso)}&end={urllib.parse.quote(end_utc_iso)}",
    ]
    for url in candidates:
        try:
            data = http_get_json(url)
            # Common shapes we might see:
            # {"predictions":[{"t":"2025-08-15T12:00:00Z","v":2.34}, ...]}
            # or {"items":[{"time":"...Z","value":2.34}, ...]}
            out = {}
            if "predictions" in data and isinstance(data["predictions"], list):
                for it in data["predictions"]:
                    t = it.get("t") or it.get("time") or it.get("instant")
                    v = it.get("v") or it.get("value") or it.get("waterLevel")
                    if t is not None and v is not None:
                        out[str(t)] = float(v)
            elif "items" in data and isinstance(data["items"], list):
                for it in data["items"]:
                    t = it.get("t") or it.get("time") or it.get("instant")
                    v = it.get("v") or it.get("value") or it.get("waterLevel")
                    if t is not None and v is not None:
                        out[str(t)] = float(v)
            # some older/alternate formats
            elif isinstance(data, list):
                for it in data:
                    t = it.get("t") or it.get("time") or it.get("instant")
                    v = it.get("v") or it.get("value") or it.get("waterLevel")
                    if t is not None and v is not None:
                        out[str(t)] = float(v)
            if out:
                return out
        except Exception as e:
            print(f"[WARN] IWLS predictions fetch failed for {station_id}: {e}", file=sys.stderr)
    return {}

def tide_trend(levels, idx):
    # levels: list of floats (may include None) aligned hourly
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

# ----------------- main -----------------
def main():
    start_local = NOW
    end_local = NOW + dt.timedelta(hours=HOURS)
    result = {"generated_at": NOW.isoformat(), "hours": []}

    # WIND (all spots)
    wind = {}
    for key, spot in SPOTS.items():
        try:
            wjson = fetch_open_meteo_wind(spot["lat"], spot["lon"], start_local, end_local)
            wind[key] = {
                "time": wjson["hourly"]["time"],                # local ISO
                "avg":  wjson["hourly"]["windspeed_10m"],       # kn
                "gust": wjson["hourly"]["windgusts_10m"],       # kn
                "dir":  wjson["hourly"]["winddirection_10m"],   # deg
            }
        except Exception as e:
            print(f"[WARN] Wind fetch failed for {spot['name']}: {e}", file=sys.stderr)
            wind[key] = {"time": [], "avg": [], "gust": [], "dir": []}

    # Master timeline = Beauport wind times
    timeline_local = wind["beauport"]["time"]
    if not timeline_local:
        with open("forecast.json", "w", encoding="utf-8") as f:
            json.dump({"generated_at": NOW.isoformat(), "hours": []}, f, ensure_ascii=False, indent=2)
        return

    # Build UTC times for IWLS (Z strings)
    timeline_utc_iso = []
    for tloc in timeline_local:
        t = dt.datetime.fromisoformat(tloc).replace(tzinfo=TZ)
        timeline_utc_iso.append(t.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z"))

    # TIDE PREDICTIONS (per spot via nearest station)
    tide_levels = {}
    for key, spot in SPOTS.items():
        series = [None] * len(timeline_local)
        try:
            st = iwls_nearest_station(spot["lat"], spot["lon"], radius_km=40)
            if st and st.get("id"):
                pred = iwls_predicted_levels(st["id"], timeline_utc_iso[0], timeline_utc_iso[-1])
                if pred:
                    # align predictions to our hourly UTC timeline
                    for i, utc in enumerate(timeline_utc_iso):
                        if utc in pred:
                            series[i] = pred[utc]
                print(f"[INFO] IWLS tide station for {spot['name']}: {st['id']} at ({st['lat']:.4f},{st['lon']:.4f})", file=sys.stderr)
            else:
                print(f"[INFO] No IWLS station found near {spot['name']}", file=sys.stderr)
        except Exception as e:
            print(f"[WARN] IWLS tide retrieval failed for {spot['name']}: {e}", file=sys.stderr)
        tide_levels[key] = series

    # Compose hourly rows
    for i, t_loc_iso in enumerate(timeline_local):
        row = {"time": t_loc_iso}
        for key in SPOTS.keys():
            gust = avg = d = None
            try:
                w_times = wind[key]["time"]
                idx = w_times.index(t_loc_iso)
                gust = float(wind[key]["gust"][idx]) if wind[key]["gust"][idx] is not None else None
                avg  = float(wind[key]["avg"][idx])  if wind[key]["avg"][idx]  is not None else None
                d    = float(wind[key]["dir"][idx])  if wind[key]["dir"][idx]  is not None else None
            except Exception:
                pass

            tide_status = tide_trend(tide_levels.get(key, []), i) if tide_levels.get(key) else "unknown"

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
