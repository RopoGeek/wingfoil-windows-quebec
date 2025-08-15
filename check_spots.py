# check_spots.py — gust rules + IWLS tide predictions by station UUID (fast & reliable)
# Wind: Open-Meteo (gust/mean/dir) — no key
# Tide: CHS Integrated Water Level System (IWLS) predictions — no key
#
# How it works:
# 1) Resolve CHS station codes -> UUIDs via /v3/stations?code=XXXX
# 2) Fetch predicted water levels for [now, now+96h] via /v3/stations/{UUID}/predictions
# 3) For each hour, classify tide as rising/falling/slack using nearest prediction before/after
#
# Stations:
#   - Baie de Beauport:   Vieux-Québec 03248
#   - Ste-Anne-de-Beaupré: 03087
#   - St-Jean, Île d’Orléans: 03105

import json, datetime as dt, urllib.request, urllib.parse, sys
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/Toronto")
NOW = dt.datetime.now(TZ).replace(minute=0, second=0, microsecond=0)
HOURS = 96  # look ahead

SPOTS = {
    "beauport": {"name": "Baie de Beauport", "lat": 46.8598, "lon": -71.2006, "iwls_code": "03248"},
    "ste_anne": {"name": "Quai Ste-Anne-de-Beaupré", "lat": 47.0153, "lon": -70.9280, "iwls_code": "03087"},
    "st_jean":  {"name": "Quai St-Jean, Île d’Orléans", "lat": 46.8577, "lon": -70.8169, "iwls_code": "03105"},
}

# Gust thresholds (kn)
THRESHOLD_GUST = {"beauport": 10.0, "ste_anne": 10.0, "st_jean": 10.0}

# Direction sectors (deg, "from")
DIR_SW = (200, 250)  # Ste-Anne
DIR_NE = (30, 70)    # St-Jean

def is_dir_in_sector(deg, lo, hi): return (lo <= deg <= hi) if lo <= hi else (deg >= lo or deg <= hi)
def in_SW(deg): return is_dir_in_sector(deg, *DIR_SW)
def in_NE(deg): return is_dir_in_sector(deg, *DIR_NE)

# ---------------- HTTP helper ----------------
def http_get_json(url, timeout=30):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)

# ---------------- WIND (Open-Meteo) ----------------
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

# ---------------- TIDE (IWLS) ----------------
IWLS_BASE = "https://api-iwls.dfo-mpo.gc.ca"

def iwls_station_uuid_from_code(code):
    # Try v3 and legacy paths; return first matching UUID string
    urls = [
        f"{IWLS_BASE}/v3/stations?code={urllib.parse.quote(code)}",
        f"{IWLS_BASE}/stations?code={urllib.parse.quote(code)}",
    ]
    for url in urls:
        try:
            data = http_get_json(url)
            items = data.get("items") or data.get("stations") or (data if isinstance(data, list) else [])
            if isinstance(items, list) and items:
                st = items[0]
                # UUID may be "id" or "uuid"
                uuid = st.get("id") or st.get("uuid")
                if uuid:
                    return str(uuid)
        except Exception as e:
            print(f"[WARN] IWLS station lookup failed for code {code}: {e}", file=sys.stderr)
    return None

def iwls_predictions(uuid, begin_utc_iso, end_utc_iso):
    # Fetch predicted levels for [begin,end] (UTC ISO). Try a couple of shapes.
    enc = urllib.parse.quote
    urls = [
        f"{IWLS_BASE}/v3/stations/{uuid}/predictions?begin={enc(begin_utc_iso)}&end={enc(end_utc_iso)}",
        f"{IWLS_BASE}/v3/stations/{uuid}/data?datatype=predictions&begin={enc(begin_utc_iso)}&end={enc(end_utc_iso)}",
        f"{IWLS_BASE}/stations/{uuid}/predictions?begin={enc(begin_utc_iso)}&end={enc(end_utc_iso)}",
    ]
    for url in urls:
        try:
            data = http_get_json(url)
            out = {}
            seqs = []
            if isinstance(data, dict):
                if "predictions" in data and isinstance(data["predictions"], list):
                    seqs = data["predictions"]
                elif "items" in data and isinstance(data["items"], list):
                    seqs = data["items"]
            elif isinstance(data, list):
                seqs = data
            for it in seqs:
                t = it.get("t") or it.get("time") or it.get("instant")
                v = it.get("v") or it.get("value") or it.get("waterLevel")
                if t is not None and v is not None:
                    out[str(t)] = float(v)
            if out:
                return out
        except Exception as e:
            print(f"[WARN] IWLS predictions fetch failed for {uuid}: {e}", file=sys.stderr)
    return {}

def classify_trend_at_hour(pred_map, utc_hour_iso, eps=0.02):
    """Use nearest prediction before and after the hour (within ±3h) to decide rising/falling/slack."""
    if not pred_map:
        return "unknown"
    try:
        t0 = dt.datetime.fromisoformat(utc_hour_iso.replace("Z","+00:00"))
    except Exception:
        return "unknown"

    items = []
    for ts, val in pred_map.items():
        try:
            t = dt.datetime.fromisoformat(ts.replace("Z","+00:00"))
            items.append((t, float(val)))
        except Exception:
            continue
    if not items:
        return "unknown"
    items.sort(key=lambda x: x[0])

    before = None
    after = None
    for (t, v) in items:
        if t <= t0:
            before = (t, v)
        if t >= t0 and after is None:
            after = (t, v)
            break
    # widen within ±3h if needed
    def within(a,b,h): return abs((a-b).total_seconds()) <= h*3600
    if before is None:
        cand = [iv for iv in items if iv[0] < t0 and within(iv[0], t0, 3)]
        if cand: before = max(cand, key=lambda x: x[0])
    if after is None:
        cand = [iv for iv in items if iv[0] > t0 and within(iv[0], t0, 3)]
        if cand: after = min(cand, key=lambda x: x[0])

    if not before or not after:
        return "unknown"
    vb, va = before[1], after[1]
    if va > vb + eps:  return "rising"
    if va < vb - eps:  return "falling"
    return "slack"

# ---------------- Main ----------------
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
                "time": wj["hourly"]["time"],                # local ISO
                "avg":  wj["hourly"]["windspeed_10m"],       # kn
                "gust": wj["hourly"]["windgusts_10m"],       # kn
                "dir":  wj["hourly"]["winddirection_10m"],   # deg
            }
        except Exception as e:
            print(f"[WARN] Wind fetch failed for {spot['name']}: {e}", file=sys.stderr)
            wind[key] = {"time": [], "avg": [], "gust": [], "dir": []}

    # 2) Master timeline = Beauport wind times (prevents grey)
    timeline_local = wind.get("beauport", {}).get("time", [])
    if not timeline_local:
        with open("forecast.json", "w", encoding="utf-8") as f:
            json.dump({"generated_at": NOW.isoformat(), "hours": []}, f, ensure_ascii=False, indent=2)
        return

    # Build UTC hour stamps for the whole window
    utc_hours = []
    for tloc in timeline_local:
        t0 = dt.datetime.fromisoformat(tloc).replace(tzinfo=TZ).astimezone(dt.timezone.utc)
        utc_hours.append(t0.isoformat().replace("+00:00","Z"))
    begin_iso = utc_hours[0]
    end_iso   = dt.datetime.fromisoformat(utc_hours[-1].replace("Z","+00:00")).isoformat().replace("+00:00","Z")

    # 3) IWLS: resolve UUIDs and pull predictions per spot
    tide_pred_maps = {}
    for key, spot in SPOTS.items():
        uuid = iwls_station_uuid_from_code(spot["iwls_code"])
        if not uuid:
            print(f"[WARN] No IWLS UUID for {spot['name']} (code {spot['iwls_code']})", file=sys.stderr)
            tide_pred_maps[key] = {}
            continue
        preds = iwls_predictions(uuid, begin_iso, end_iso)
        if preds:
            print(f"[INFO] IWLS predictions loaded for {spot['name']} (code {spot['iwls_code']}, uuid {uuid[:8]}…): {len(preds)} points", file=sys.stderr)
        else:
            print(f"[INFO] IWLS predictions EMPTY for {spot['name']} (code {spot['iwls_code']}, uuid {uuid[:8]}…)", file=sys.stderr)
        tide_pred_maps[key] = preds

    # 4) Compose rows (one per hour)
    for i, t_loc_iso in enumerate(timeline_local):
        row = {"time": t_loc_iso}
        utc_iso = utc_hours[i]

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

            tide_status = classify_trend_at_hour(tide_pred_maps.get(key, {}), utc_iso)

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
