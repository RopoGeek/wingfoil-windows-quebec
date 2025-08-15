# check_spots.py — gust rules + SPINE tide with seeds + fine grid auto-discovery
# Wind: Open-Meteo (gust/mean/dir) — no key
# Tide: DFO SPINE water-level forecast — batched; tries seed points first, then fine grid search
#
# Notes:
# - SPINE returns values only on its navigation-channel cells. We first try hand-picked seeds
#   and then search a fine grid around each spot along the river axis.
# - Keeps gust-based rules (10 kn for all 3).

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

# Gust thresholds (kn)
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

# ---------- TIDE (SPINE) ----------
SPINE_BASE = "https://api-spine.azure.cloud-nuage.dfo-mpo.gc.ca/rest/v1/waterLevel"

def spine_levels_batch(lat, lon, utc_list, chunk_size=36, pause=0.2, max_retries=2):
    """Fetch SPINE water levels for many times, in chunks to avoid 414. Returns dict[utc_iso]->level."""
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
                sample_other = []
                for it in items:
                    st = it.get("status")
                    if st == "OK":
                        inst = it.get("instant"); wl = it.get("waterLevel")
                        if inst is not None and wl is not None:
                            out[inst] = wl; ok += 1
                    else:
                        other += 1
                        if len(sample_other) < 3:
                            sample_other.append(st)
                print(f"[INFO] SPINE chunk {i//chunk_size+1}: {ok}/{len(chunk)} OK (+{other} non-OK: {sample_other}) @({lat},{lon})", file=sys.stderr)
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

# ---- Seeds (mid-channel guesses), then fine search if needed ----
SPINE_SEEDS = {
    "beauport": [(46.8609, -71.1835)],  # previously confirmed working
    "ste_anne": [
        # along-channel seeds (a few km window)
        (47.0200, -70.9360),
        (47.0100, -70.9220),
        (47.0250, -70.9150),
        (47.0000, -70.9300),
    ],
    "st_jean": [
        (46.8700, -70.8000),
        (46.8800, -70.8300),
        (46.8650, -70.8200),
        (46.8750, -70.8050),
    ],
}

def discover_spine_proxy(name, lat, lon, trial_pairs):
    """
    Try seeds first; if none OK, scan a fine grid: lat in [lat±0.06] step 0.005, lon in [lon±0.12] step 0.005
    Return (best_map, best_lat, best_lon). If none OK, ({}, None, None)
    """
    # Flatten trial times (t and t+1h per row)
    trial_flat = []
    for a,b in trial_pairs:
        trial_flat += [a,b]

    # 1) Try seeds
    seeds = SPINE_SEEDS.get(name_key(name), [])
    for (plat, plon) in seeds:
        m = spine_levels_batch(plat, plon, trial_flat, chunk_size=24)
        ok_count = sum(1 for k in trial_flat if k in m)
        print(f"[INFO] SPINE seed @{(plat,plon)} for {name}: {ok_count} OK", file=sys.stderr)
        if ok_count > 0:
            print(f"[INFO] SPINE seed SELECTED for {name}: ({plat},{plon})", file=sys.stderr)
            return m, plat, plon

    # 2) Fine grid search (prioritize east-west along river)
    lat_min, lat_max = lat - 0.06, lat + 0.06
    lon_min, lon_max = lon - 0.12, lon + 0.12
    step = 0.005  # ~550m
    best = (0, None, None, {})
    # Sweep lon first (east-west), then a couple of lat bands around the spot
    lat_list = [lat + i*step for i in range(-12, 13)]  # 25 bands
    lon_list = [lon + j*step for j in range(-24, 25)]  # 49 steps
    tested = 0
    for li in [0, -1, 1, -2, 2, -3, 3] + list(range(-12,13)):  # center-out order
        lat_cur = lat + li*step
        if lat_cur < lat_min or lat_cur > lat_max: continue
        for lj in list(range(-24, 25)):
            lon_cur = lon + lj*step
            if lon_cur < lon_min or lon_cur > lon_max: continue
            tested += 1
            m = spine_levels_batch(lat_cur, lon_cur, trial_flat, chunk_size=24)
            ok_count = sum(1 for k in trial_flat if k in m)
            if ok_count > 0:
                print(f"[INFO] SPINE trial @{(lat_cur,lon_cur)} for {name}: {ok_count} OK", file=sys.stderr)
            if ok_count > best[0]:
                best = (ok_count, lat_cur, lon_cur, m)
            # stop early if we already have ≥ half the requested points OK
            if best[0] >= len(trial_flat) // 2:
                break
        if best[0] >= len(trial_flat) // 2:
            break

    if best[0] > 0:
        print(f"[INFO] SPINE auto-selected proxy for {name}: ({best[1]:.5f},{best[2]:.5f}) with {best[0]} OK trial points (tested {tested} locs)", file=sys.stderr)
        return best[3], best[1], best[2]
    else:
        print(f"[INFO] SPINE auto-discovery FAILED for {name} (tested {tested} locs)", file=sys.stderr)
        return {}, None, None

def name_key(human_name):
    # map human name back to our spot keys for seed lookup
    s = human_name.lower()
    if "beauport" in s: return "beauport"
    if "anne" in s: return "ste_anne"
    if "st-jean" in s or "saint-jean" in s or "î" in s: return "st_jean"
    return "beauport"

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

    # Paired UTC list [t, t+1h] and a flat list for full fetches
    utc_pairs, flat_times = [], []
    for tloc in timeline_local:
        t0 = dt.datetime.fromisoformat(tloc).replace(tzinfo=TZ).astimezone(dt.timezone.utc)
        t1 = t0 + dt.timedelta(hours=1)
        a = t0.isoformat().replace("+00:00","Z")
        b = t1.isoformat().replace("+00:00","Z")
        utc_pairs.append((a,b))
        flat_times += [a,b]

    # 3) Discover/seed a working SPINE point per spot, then fetch ALL hours there
    spine_maps = {}
    for key, spot in SPOTS.items():
        # Use 4-hour discovery window
        trial_pairs = utc_pairs[:4] if len(utc_pairs) >= 4 else utc_pairs
        trial_map, plat, plon = discover_spine_proxy(spot["name"], spot["lat"], spot["lon"], trial_pairs)
        if plat is not None:
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
