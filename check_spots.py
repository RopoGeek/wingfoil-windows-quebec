# check_spots.py — gust-based rules + IWLS predictions using fixed CHS station IDs
# Wind (gust/mean/dir): Open-Meteo (no key)
# Tide: Canadian Hydrographic Service IWLS predictions (no key) for stations:
#   - Vieux-Québec (03248) for Baie de Beauport
#   - Sainte-Anne-de-Beaupré (03087) for Ste-Anne
#   - Saint-Jean I.O. (03105) for St-Jean, Île d’Orléans
#
# "Go" uses gusts (kn). Tide trend computed from nearest predictions around each hour.

import json, datetime as dt, urllib.request, urllib.parse, sys, math
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/Toronto")
NOW = dt.datetime.now(TZ).replace(minute=0, second=0, microsecond=0)
HOURS = 96  # look ahead

SPOTS = {
    "beauport": {"name": "Baie de Beauport", "lat": 46.8598, "lon": -71.2006, "iwls_id": "03248"},  # Vieux-Québec
    "ste_anne": {"name": "Quai Ste-Anne-de-Beaupré", "lat": 47.0153, "lon": -70.9280, "iwls_id": "03087"},
    "st_jean":  {"name": "Quai St-Jean, Île d’Orléans", "lat": 46.8577, "lon": -70.8169, "iwls_id": "03105"},
}

# Gust thresholds (kn)
THRESHOLD_GUST = {"beauport": 10.0, "ste_anne": 10.0, "st_jean": 10.0}

# Direction sectors (deg, "from"):
DIR_SW = (200, 250)  # Ste-Anne
DIR_NE = (30, 70)    # St-Jean

def is_dir_in_sector(deg, lo, hi):
    return (lo <= deg <= hi) if lo <= hi else (deg >= lo or deg <= hi)
def in_SW(deg): return is_dir_in_sector(deg, *DIR_SW)
def in_NE(deg): return is_dir_in_sector(deg, *DIR_NE)

def http_get_json(url, timeout=45):
    with urllib.request.urlopen(url, timeout=timeout) as r:
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
        "end_date": end_dt.date().isoformat(),
    })
    return http_get_json(f"{base}?{qs}")

# ---------- TIDE (IWLS predictions, fixed station IDs) ----------
IWLS_BASE = "https://api-iwls.dfo-mpo.gc.ca"

def iwls_predictions(station_id, begin_utc_iso, end_utc_iso):
    """
    Return dict[utc_iso] -> predicted level (m) for station_id between begin & end UTC.
    Try a few known endpoint shapes for compatibility.
    """
    enc = urllib.parse.quote
    candidates = [
        f"{IWLS_BASE}/v3/stations/{station_id}/predictions?begin={enc(begin_utc_iso)}&end={enc(end_utc_iso)}",
        f"{IWLS_BASE}/stations/{station_id}/predictions?begin={enc(begin_utc_iso)}&end={enc(end_utc_iso)}",
        f"{IWLS_BASE}/v3/stations/{station_id}/data?datatype=predictions&begin={enc(begin_utc_iso)}&end={enc(end_utc_iso)}",
    ]
    for url in candidates:
        try:
            data = http_get_json(url)
            out = {}
            seqs = []
            if isinstance(data, dict):
                if "predictions" in data and isinstance(data["predictions"], list):
                    seqs = data["predictions"]
                elif "items" in data and isinstance(data["items"], list):
                    seqs = data["items"]
            if isinstance(data, list):
                seqs = data
            for it in seqs:
                t = it.get("t") or it.get("time") or it.get("instant")
                v = it.get("v") or it.get("value") or it.get("waterLevel")
                if t is not None and v is not None:
                    out[str(t)] = float(v)
            if out:
                return out
        except Exception as e:
            print(f"[WARN] IWLS predictions fetch failed for {station_id}: {e}", file=sys.stderr)
    return {}

def classify_trend_at_hour(pred_map, utc_hour_iso):
    """
    Given a dict of predictions (time->level) and an hour timestamp (UTC),
    find the nearest prediction before and after that hour within ±3h and classify.
    """
    if not pred_map:
        return "unknown"
    try:
        t0 = dt.datetime.fromisoformat(utc_hour_iso.replace("Z","+00:00"))
    except Exception:
        return "unknown"

    # Build sorted list of (time, level)
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

    # Find neighbors around the hour
    before = None
    after = None
    for (t, v) in items:
        if t <= t0:
            before = (t, v)
        if t >= t0 and after is None:
            after = (t, v)
            break

    # Expand window if both missing
    def within_hrs(a, b, hrs): return abs((a-b).total_seconds()) <= hrs*3600

    if before is None:
        # take closest earlier within 3h
        cand = [iv for iv in items if iv[0] < t0 and within_hrs(iv[0], t0, 3)]
        if cand:
            before = max(cand, key=lambda x: x[0])
    if after is None:
        cand = [iv for iv in items if iv[0] > t0 and within_hrs(iv[0], t0, 3)]
        if cand:
            after = min(cand, key=lambda x: x[0])

    if not before or not after:
        return "unknown"

    vb, va = before[1], after[1]
    if va > vb + 0.02:  # small epsilon to avoid jitter
        return "rising"
    if va < vb - 0.02:
        return "falling"
    return "slack"

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

    # Build per-spot IWLS prediction maps once
    tide_pred_maps = {}
    begin_utc = dt.datetime.fromisoformat(timeline_local[0]).replace(tzinfo=TZ).astimezone(dt.timezone.utc)
    end_utc   = dt.datetime.fromisoformat(timeline_local[-1]).replace(tzinfo=TZ).astimezone(dt.timezone.utc)
    begin_iso = begin_utc.isoformat().replace("+00:00","Z")
    end_iso   = end_utc.isoformat().replace("+00:00","Z")

    for key, spot in SPOTS.items():
        stid = spot["iwls_id"]
        m = {}
        try:
            m = iwls_predictions(stid, begin_iso, end_iso)
            if not m:
                print(f"[INFO] IWLS returned empty predictions for station {stid} ({spot['name']})", file=sys.stderr)
            else:
                print(f"[INFO] IWLS predictions loaded for station {stid} ({spot['name']})", file=sys.stderr)
        except Exception as e:
            print(f"[WARN] IWLS retrieval failed for station {stid}: {e}", file=sys.stderr)
        tide_pred_maps[key] = m

    # Compose hourly rows
    for t_loc_iso in timeline_local:
        row = {"time": t_loc_iso}
        utc_iso = dt.datetime.fromisoformat(t_loc_iso).replace(tzinfo=TZ).astimezone(dt.timezone.utc).isoformat().replace("+00:00","Z")

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

            # Classify tide trend using nearest predictions (graceful)
            tide_status = classify_trend_at_hour(tide_pred_maps.get(key, {}), utc_iso)

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
