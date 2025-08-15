# check_spots.py — fast + reliable:
# - Wind (gust/mean/dir): Open-Meteo (no key)
# - Tide: SPINE (DFO) only for Baie de Beauport using a known-good seed coordinate
# - Ste-Anne & St-Jean tide temporarily disabled to keep runs quick (wind still shows)
# - "Go" uses GUSTS (kn). Ste-Anne needs SW & rising tide; St-Jean needs NE & falling tide.

import json, datetime as dt, urllib.request, urllib.parse, sys, time
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/Toronto")
NOW = dt.datetime.now(TZ).replace(minute=0, second=0, microsecond=0)
HOURS = 96  # look ahead (4 days)

# -------- Switch tide per spot (quick + safe) --------
ENABLE_TIDE_FOR = {
    "beauport": True,   # SPINE enabled (seed works)
    "ste_anne": False,  # keep False until we wire a stable source/point
    "st_jean":  False,  # keep False until we wire a stable source/point
}

# -------- Spots (display/wind coords) --------
SPOTS = {
    "beauport": {"name": "Baie de Beauport", "lat": 46.8598, "lon": -71.2006},
    "ste_anne": {"name": "Quai Ste-Anne-de-Beaupré", "lat": 47.0153, "lon": -70.9280},
    "st_jean":  {"name": "Quai St-Jean, Île d’Orléans", "lat": 46.8577, "lon": -70.8169},
}

# -------- Gust thresholds (kn) --------
THRESHOLD_GUST = {"beauport": 10.0, "ste_anne": 10.0, "st_jean": 10.0}

# -------- Direction sectors (deg, "from") --------
DIR_SW = (200, 250)  # Ste-Anne needs SW
DIR_NE = (30, 70)    # St-Jean needs NE

def is_dir_in_sector(deg, lo, hi): return (lo <= deg <= hi) if lo <= hi else (deg >= lo or deg <= hi)
def in_SW(deg): return is_dir_in_sector(deg, *DIR_SW)
def in_NE(deg): return is_dir_in_sector(deg, *DIR_NE)

def http_get_json(url, timeout=45):
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

# ---------------- TIDE (SPINE, batched) ----------------
SPINE_BASE = "https://api-spine.azure.cloud-nuage.dfo-mpo.gc.ca/rest/v1/waterLevel"

# Known-good SPINE mid-channel seed for Baie de Beauport (confirmed in your logs)
SPINE_SEED_BEAUPORT = (46.8609, -71.1835)

def spine_levels_batch(lat, lon, utc_list, chunk_size=36, pause=0.2, max_retries=2):
    """Fetch SPINE water levels for many times, in chunks (avoid 414). Returns dict[utc_iso]->level."""
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

def classify_trend(spine_map, t_utc_iso, t1_utc_iso, eps=0.02):
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

# ---------------- Main ----------------
def main():
    start_local = NOW
    end_local = NOW + dt.timedelta(hours=HOURS)
    result = {"generated_at": NOW.isoformat(), "hours": []}

    # 1) Fetch wind for each spot
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

    # 2) Master timeline = Beauport wind times (prevents grey "missing data")
    timeline_local = wind.get("beauport", {}).get("time", [])
    if not timeline_local:
        with open("forecast.json", "w", encoding="utf-8") as f:
            json.dump({"generated_at": NOW.isoformat(), "hours": []}, f, ensure_ascii=False, indent=2)
        return

    # Build paired UTC times [t, t+1h] + flat list for SPINE
    utc_pairs, flat_times = [], []
    for tloc in timeline_local:
        t0 = dt.datetime.fromisoformat(tloc).replace(tzinfo=TZ).astimezone(dt.timezone.utc)
        t1 = t0 + dt.timedelta(hours=1)
        a = t0.isoformat().replace("+00:00","Z")
        b = t1.isoformat().replace("+00:00","Z")
        utc_pairs.append((a,b))
        flat_times += [a,b]

    # 3) Tide maps (only for enabled spots)
    spine_maps = {}
    for key, spot in SPOTS.items():
        if not ENABLE_TIDE_FOR.get(key, False):
            print(f"[INFO] Tide disabled for {spot['name']} (skipping SPINE)", file=sys.stderr)
            spine_maps[key] = {}
            continue

        # For Beauport: use the known-good seed directly (fast & reliable)
        seed_lat, seed_lon = SPINE_SEED_BEAUPORT
        trial_flat = [t for ab in utc_pairs[:2] for t in ab]  # tiny trial
        seed_trial = spine_levels_batch(seed_lat, seed_lon, trial_flat, chunk_size=24)
        ok_seed = sum(1 for t in trial_flat if t in seed_trial)
        if ok_seed > 0:
            print(f"[INFO] Using SPINE seed for {spot['name']}: ({seed_lat},{seed_lon}) with {ok_seed} OK trials", file=sys.stderr)
            full_map = spine_levels_batch(seed_lat, seed_lon, flat_times, chunk_size=36)
            spine_maps[key] = full_map
        else:
            print(f"[INFO] SPINE seed returned no data for {spot['name']} — leaving tide unknown", file=sys.stderr)
            spine_maps[key] = {}

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

            tide_status = classify_trend(spine_maps.get(key, {}), t0_utc, t1_utc) if ENABLE_TIDE_FOR.get(key, False) else "unknown"

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
