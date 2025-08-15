# check_spots.py — gust rules + SPINE tide with AUTO-DISCOVERY of channel proxy per spot
# Wind: Open-Meteo (gust/mean/dir) — no key
# Tide: DFO SPINE water-level forecast — batched, auto-finds a valid mid-channel point per spot
#
# Notes:
# - SPINE only returns values for positions tied to the navigation channel inside its footprint.
#   We scan a small lat/lon grid around each spot to find a coordinate that returns "OK" values,
#   then fetch the full 96h horizon there. (See CHS SPINE docs.) 
#   https://tides.gc.ca/en/web-services-offered-canadian-hydrographic-service  (SPINE) 

import json, datetime as dt, urllib.request, urllib.parse, sys, time
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/Toronto")
NOW = dt.datetime.now(TZ).replace(minute=0, second=0, microsecond=0)
HOURS = 96  # look ahead

# Display coordinates (for wind)
SPOTS = {
    "beauport": {"name": "Baie de Beauport", "lat": 46.8598, "lon": -71.2006},
    "ste_anne": {"name": "Quai Ste-Anne-de-Beaupré", "lat": 47.0153, "lon": -70.9280},
    "st_jean":  {"name": "Quai St-Jean, Île d’Orléans", "lat": 46.8577, "lon": -70.8169},
}

# Gust thresholds (kn) — your rule
THRESHOLD_GUST = {"beauport": 10.0, "ste_anne": 10.0, "st_jean": 10.0}

# Direction sectors (deg, "from")
DIR_SW = (200, 250)  # Ste-Anne
DIR_NE = (30, 70)    # St-Jean

def is_dir_in_sector(deg, lo, hi): return (lo <= deg <= hi) if lo <= hi else (deg >= lo or deg <= hi)
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

# ---------- TIDE (SPINE, batched) ----------
SPINE_BASE = "https://api-spine.azure.cloud-nuage.dfo-mpo.gc.ca/rest/v1/waterLevel"

def spine_levels_batch(lat, lon, utc_list, chunk_size=36, pause=0.2, max_retries=2):
    """Fetch SPINE water levels for many times, in chunks to avoid 414."""
    out = {}
    n = len(utc_list)
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
                ok = 0; other = 0
                for it in items:
                    st = it.get("status")
                    if st == "OK":
                        inst = it.get("instant"); wl = it.get("waterLevel")
                        if inst is not None and wl is not None:
                            out[inst] = wl; ok += 1
                    else:
                        other += 1
                print(f"[INFO] SPINE chunk {i//chunk_size+1}: got {ok}/{len(chunk)} OK (+{other} non-OK) @({lat},{lon})", file=sys.stderr)
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

# --- Auto-discover a valid SPINE channel point near (lat,lon) ---
def discover_spine_proxy(name, lat, lon, trial_times_utc):
    """
    Scan rings around (lat,lon) to find a coordinate that yields OK values.
    Returns (best_map, best_lat, best_lon). If none, ({}, None, None)
    """
    # rings in degrees (~1 deg lat ≈ 111 km; 0.01 ≈ 1.1 km)
    rings = [
        0.00,  # exact (sometimes works)
        0.01,  # ~1.1 km
        0.02,  # ~2.2 km
        0.03,  # ~3.3 km
        0.04,  # ~4.4 km
    ]
    # 8-direction offsets per ring
    dirs = [(0,0),(1,0),(-1,0),(0,1),(0,-1),(1,1),(1,-1),(-1,1),(-1,-1)]

    # Build a flat trial list for batching (t and t+1h only is enough to test)
    trial_flat = []
    for a,b in trial_times_utc:
        trial_flat.append(a); trial_flat.append(b)

    best = (0, None, None, {})  # (ok_count, lat, lon, map)
    for r in rings:
        for dx,dy in dirs:
            plat = lat + r*dy      # note: dy -> latitude
            plon = lon + r*dx      # dx -> longitude
            m = spine_levels_batch(plat, plon, trial_flat, chunk_size=24)
            ok_count = sum(1 for k in trial_flat if k in m)
            if ok_count > 0:
                print(f"[INFO] SPINE trial @({plat:.5f},{plon:.5f}) for {name}: {ok_count} OK points", file=sys.stderr)
            if ok_count > best[0]:
                best = (ok_count, plat, plon, m)
        # if we already have a decent hit (>= half of requested), stop early
        if best[0] >= len(trial_flat) // 2:
            break

    if best[0] > 0:
        print(f"[INFO] SPINE auto-selected proxy for {name}: ({best[1]:.5f},{best[2]:.5f}) with {best[0]} OK trial points", file=sys.stderr)
        return best[3], best[1], best[2]
    else:
        print(f"[INFO] SPINE auto-discovery FAILED for {name}", file=sys.stderr)
        return {}, None, None

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

    # 2) Master timeline = Beauport wind times
    timeline_local = wind.get("beauport", {}).get("time", [])
    if not timeline_local:
        with open("forecast.json", "w", encoding="utf-8") as f:
            json.dump({"generated_at": NOW.isoformat(), "hours": []}, f, ensure_ascii=False, indent=2)
        return

    # Build paired UTC times [t, t+1h]
    utc_pairs = []
    flat_times = []
    for tloc in timeline_local:
        t0 = dt.datetime.fromisoformat(tloc).replace(tzinfo=TZ).astimezone(dt.timezone.utc)
        t1 = t0 + dt.timedelta(hours=1)
        a = t0.isoformat().replace("+00:00","Z")
        b = t1.isoformat().replace("+00:00","Z")
        utc_pairs.append((a,b))
        flat_times += [a,b]

    # 3) For each spot, discover a working SPINE proxy and then fetch full horizon
    spine_maps = {}
    for key, spot in SPOTS.items():
        # Quick 4-hour window near the start for discovery
        trial_pairs = utc_pairs[0:4] if len(utc_pairs) >= 4 else utc_pairs
        trial_map, plat, plon = discover_spine_proxy(spot["name"], spot["lat"], spot["lon"], trial_pairs)
        if plat is not None:
            # Now fetch ALL hours at the chosen point
            full_map = spine_levels_batch(plat, plon, flat_times, chunk_size=36)
            if not full_map:
                print(f"[INFO] SPINE full-fetch empty for {spot['name']} @({plat},{plon})", file=sys.stderr)
            spine_maps[key] = full_map
        else:
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
