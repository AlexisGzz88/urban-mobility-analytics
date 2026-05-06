#!/usr/bin/env python3
"""
Urban Mobility Analytics — Data Generator
seed=42 para reproducibilidad
"""

import random, json, csv, os, math
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

random.seed(42)

BASE = os.path.join(os.path.dirname(__file__), "raw")
os.makedirs(BASE, exist_ok=True)

# ── Constantes ─────────────────────────────────────────────────────────────────
START   = datetime(2024, 1, 1)
END     = datetime(2025, 6, 30)
DAYS    = (END - START).days

CITY_CLEAN   = ["CDMX", "Monterrey", "Guadalajara"]
CITY_W       = [0.50, 0.30, 0.20]
CITY_VARIANTS = {
    "CDMX":        ["CDMX", "cdmx", "Ciudad de Mexico"],
    "Monterrey":   ["Monterrey", "MTY"],
    "Guadalajara": ["Guadalajara", "GDL"],
}
CITY_COORDS = {
    "CDMX":        (19.4326, -99.1332),
    "Monterrey":   (25.6866, -100.3161),
    "Guadalajara": (20.6597, -103.3496),
}

N_USERS = 500; N_VEH = 200; N_TRIPS = 5000; N_MAINT = 1500

user_ids    = [f"USR{i:04d}"  for i in range(1, N_USERS + 1)]
scooter_ids = [f"SC{i:04d}"   for i in range(1, N_VEH + 1)]
trip_ids    = [f"TRIP{i:05d}" for i in range(1, N_TRIPS + 1)]

# ── Temporal helpers ───────────────────────────────────────────────────────────
HOUR_W = {0:0.3,1:0.2,2:0.1,3:0.1,4:0.2,5:0.5,6:1.5,7:3.0,8:3.5,9:2.5,
           10:1.5,11:1.5,12:1.8,13:1.5,14:1.5,15:1.5,16:2.0,17:3.0,
           18:3.5,19:2.8,20:1.5,21:1.0,22:0.7,23:0.4}
DOW_W  = [1.5,1.5,1.5,1.5,1.5,0.9,0.7]
MON_W  = {1:0.8,2:0.8,3:1.3,4:1.4,5:1.3,6:1.0,7:1.0,8:1.0,9:1.2,10:1.3,11:1.2,12:0.9}

def rand_ts(start=START, end=END):
    total = (end - start).days
    for _ in range(200):
        d = start + timedelta(days=random.randint(0, total))
        if random.random() < (MON_W[d.month] * DOW_W[d.weekday()]) / (1.4 * 1.5):
            break
    hrs = list(range(24)); wts = [HOUR_W[h] for h in hrs]
    s = sum(wts); r = random.random() * s; cum = 0
    h = 0
    for hour, w in zip(hrs, wts):
        cum += w
        if r <= cum: h = hour; break
    return d.replace(hour=h, minute=random.randint(0,59), second=random.randint(0,59))

def normalize_city(v):
    for c, variants in CITY_VARIANTS.items():
        if v in variants or v == c: return c
    return "CDMX"

# ══════════════════════════════════════════════════════════════════════════════
# 1. trips_sample.csv
# ══════════════════════════════════════════════════════════════════════════════
print("Generating trips_sample.csv ...")

PAYMENTS = ["credit_card","debit_card","cash","wallet","qr_code"]
ghost_sc  = [f"SC{i:04d}" for i in range(N_VEH+1, N_VEH+51)]

scooter_city = {}
city_sc_map  = {c:[] for c in CITY_CLEAN}
for sc in scooter_ids:
    c = random.choices(CITY_CLEAN, weights=CITY_W)[0]
    scooter_city[sc] = c
    city_sc_map[c].append(sc)

dup_idx = set(random.sample(range(N_TRIPS), int(N_TRIPS*0.01)))
inc_trips = dict(null_user=0,end_before_start=0,fare_string=0,
                 city_variant=0,duplicate_trip=0,zero_dist_with_fare=0)

trips_data = []
for i, tid in enumerate(trip_ids):
    city = random.choices(CITY_CLEAN, weights=CITY_W)[0]
    city_v = random.choice(CITY_VARIANTS[city])
    if city_v != city: inc_trips["city_variant"] += 1

    uid = None if random.random() < 0.03 else random.choice(user_ids)
    if uid is None: inc_trips["null_user"] += 1

    sc = random.choice(ghost_sc) if random.random() < 0.02 \
         else random.choice(city_sc_map[city] or scooter_ids)

    st = rand_ts()
    dur = random.randint(5, 90)
    et  = st + timedelta(minutes=dur)
    if random.random() < 0.02:
        et = st - timedelta(minutes=random.randint(1,30))
        inc_trips["end_before_start"] += 1

    if random.random() < 0.01:
        dist = 0.0; fare_val = round(random.uniform(20,100), 2)
        inc_trips["zero_dist_with_fare"] += 1
    else:
        dist = round(random.uniform(0.5,15.0), 2)
        fare_val = round(dist * random.uniform(8,15) + random.uniform(5,20), 2)

    if random.random() < 0.10:
        fare = f"${fare_val:.2f}"; inc_trips["fare_string"] += 1
    else:
        fare = fare_val

    row = {"trip_id":tid,"user_id":uid,"scooter_id":sc,"city":city_v,
           "start_time":st.isoformat(),"end_time":et.isoformat(),
           "distance_km":dist,"fare_mxn":fare,"payment_method":random.choice(PAYMENTS)}
    trips_data.append(row)
    if i in dup_idx:
        trips_data.append(row.copy()); inc_trips["duplicate_trip"] += 1

random.shuffle(trips_data)

with open(f"{BASE}/trips_sample.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(trips_data[0].keys()))
    w.writeheader(); w.writerows(trips_data)

dates_trips = [r["start_time"] for r in trips_data]
print(f"  ✓ {len(trips_data)} rows | range: {min(dates_trips)[:10]} → {max(dates_trips)[:10]}")
print(f"  Inconsistencies: {inc_trips}\n")

# ══════════════════════════════════════════════════════════════════════════════
# 2. users_sample.json
# ══════════════════════════════════════════════════════════════════════════════
print("Generating users_sample.json ...")

FIRST = ["Carlos","Maria","Juan","Ana","Luis","Sofia","Miguel","Isabella",
         "Diego","Valeria","Alejandro","Fernanda","Ricardo","Camila","Jorge",
         "Daniela","Eduardo","Paola","Roberto","Andrea"]
LAST  = ["Garcia","Rodriguez","Martinez","Lopez","Gonzalez","Perez","Sanchez",
         "Ramirez","Torres","Flores","Rivera","Gomez","Diaz","Reyes","Morales"]
BAD_CITIES = ["Tijuana","Puebla","Cancun","Merida","Leon"]

inc_users = dict(missing_email=0,plan_variant=0,bad_city=0,old_date_fmt=0)
users_data = []

for uid in user_ids:
    name = f"{random.choice(FIRST)} {random.choice(LAST)}"
    email = None if random.random() < 0.10 else \
            f"{name.lower().replace(' ','.')}{random.randint(1,999)}@email.com"
    if email is None: inc_users["missing_email"] += 1

    sig_dt = rand_ts(START - timedelta(days=365), END)
    if random.random() < 0.30:
        signup = sig_dt.strftime("%d/%m/%Y"); inc_users["old_date_fmt"] += 1
    else:
        signup = sig_dt.date().isoformat()

    base_plan = random.choice(["premium","basic"])
    if base_plan == "premium":
        plan = random.choice(["premium","Premium","PREMIUM"])
    else:
        plan = random.choice(["basic","Basic"])
    if plan not in ["premium","basic"]: inc_users["plan_variant"] += 1

    if random.random() < 0.05:
        city = random.choice(BAD_CITIES); inc_users["bad_city"] += 1
    else:
        city = random.choices(CITY_CLEAN, weights=CITY_W)[0]

    users_data.append({"user_id":uid,"name":name,"email":email,
                        "signup_date":signup,"plan_type":plan,"city":city})

with open(f"{BASE}/users_sample.json","w") as f:
    json.dump(users_data, f, indent=2, ensure_ascii=False, default=str)

print(f"  ✓ {len(users_data)} records")
print(f"  Inconsistencies: {inc_users}\n")

# ══════════════════════════════════════════════════════════════════════════════
# 3. vehicles_sample.parquet
# ══════════════════════════════════════════════════════════════════════════════
print("Generating vehicles_sample.parquet ...")

MODELS = ["Xiaomi Pro 2","Segway Ninebot G30","Unagi Model One","Razor E300",
          "Hiboy S2","GoTrax GXL V2","Apollo City","Kaabo Mantis"]
STATUS_OPTS = ["active","Active","INACTIVE","maintenance",None]

used_sc = set(r["scooter_id"] for r in trips_data if r["scooter_id"] in set(scooter_ids))
inc_veh = dict(orphan=0,status_variant=0,null_status=0)
veh_rows = []

for sc in scooter_ids:
    city = scooter_city.get(sc, random.choices(CITY_CLEAN, weights=CITY_W)[0])
    lm   = (START - timedelta(days=random.randint(1,365))).date().isoformat()
    bat  = random.choice([186,187,374,500,551])

    r = random.random()
    if r < 0.70:
        status = random.choice(["active","Active"])
        if status != "active": inc_veh["status_variant"] += 1
    elif r < 0.85:
        status = "INACTIVE"; inc_veh["status_variant"] += 1
    elif r < 0.95:
        status = "maintenance"
    else:
        status = None; inc_veh["null_status"] += 1

    if sc not in used_sc: inc_veh["orphan"] += 1

    veh_rows.append({"scooter_id":sc,"model":random.choice(MODELS),"city":city,
                     "last_maintenance_date":lm,"battery_capacity_wh":bat,"status":status})

df_v = pd.DataFrame(veh_rows)
pq.write_table(pa.Table.from_pandas(df_v), f"{BASE}/vehicles_sample.parquet")

print(f"  ✓ {len(veh_rows)} records")
print(f"  Inconsistencies: {inc_veh}\n")

# ══════════════════════════════════════════════════════════════════════════════
# 4. maintenance_logs_sample.csv
# ══════════════════════════════════════════════════════════════════════════════
print("Generating maintenance_logs_sample.csv ...")

ISSUES = ["battery_replacement","brake_repair","tire_puncture","motor_issue",
          "display_fault","throttle_repair","frame_damage","software_update","general_service"]
ghost_sc2 = [f"SC{i:04d}" for i in range(N_VEH+51, N_VEH+100)]
inc_maint  = dict(negative_cost=0,future_date=0,ghost_scooter=0)
maint_rows = []

for k in range(1, N_MAINT+1):
    if random.random() < 0.05:
        sc = random.choice(ghost_sc2); inc_maint["ghost_scooter"] += 1
    else:
        sc = random.choice(scooter_ids)

    if random.random() < 0.03:
        md = (END + timedelta(days=random.randint(1,180))).isoformat()
        inc_maint["future_date"] += 1
    else:
        md = rand_ts().isoformat()

    cost = round(random.uniform(50,800), 2)
    if random.random() < 0.05:
        cost = -cost; inc_maint["negative_cost"] += 1

    maint_rows.append({"log_id":f"LOG{k:05d}","scooter_id":sc,"maintenance_date":md,
                        "issue_type":random.choice(ISSUES),"cost_usd":cost,
                        "technician_id":f"TECH{random.randint(1,20):02d}"})

with open(f"{BASE}/maintenance_logs_sample.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(maint_rows[0].keys()))
    w.writeheader(); w.writerows(maint_rows)

print(f"  ✓ {len(maint_rows)} rows")
print(f"  Inconsistencies: {inc_maint}\n")

# ══════════════════════════════════════════════════════════════════════════════
# 5. ride_events_sample.json (JSON Lines)
# ══════════════════════════════════════════════════════════════════════════════
print("Generating ride_events_sample.json ...")

ghost_trips = [f"TRIP{i:05d}" for i in range(N_TRIPS+1, N_TRIPS+201)]

# Build trip lookup (first occurrence only — deduped)
trip_lookup = {}
for r in trips_data:
    tid = r["trip_id"]
    if tid not in trip_lookup:
        st = datetime.fromisoformat(r["start_time"])
        et_raw = datetime.fromisoformat(r["end_time"])
        et = et_raw if et_raw > st else st + timedelta(minutes=20)
        trip_lookup[tid] = {"city": normalize_city(r["city"]),
                             "start": st, "end": et,
                             "scooter_id": r["scooter_id"]}

unique_trips = list(trip_lookup.keys())

# Pre-assign events per trip to hit ~50,000 total
events_per_trip = {tid: random.randint(8,12) for tid in unique_trips}
raw_total = sum(events_per_trip.values())
scale = 50000 / raw_total

inc_evt = dict(neg_battery=0,battery_over100=0,null_speed=0,
               speed_over120=0,dup_event=0,ghost_trip=0)

evt_counter = 1
written = 0

with open(f"{BASE}/ride_events_sample.json","w") as f:

    # Real trip events
    for tid in unique_trips:
        info = trip_lookup[tid]
        city = info["city"]
        base_lat, base_lon = CITY_COORDS[city]
        st, et = info["start"], info["end"]
        sc = info["scooter_id"]
        n  = max(8, min(15, int(events_per_trip[tid] * scale)))
        dur = (et - st).total_seconds()
        if dur <= 0: dur = 600

        bat = random.uniform(40, 100)
        lat = base_lat + random.uniform(-0.05, 0.05)
        lon = base_lon + random.uniform(-0.05, 0.05)

        for j in range(n):
            prog = j / max(n-1, 1)
            ts   = st + timedelta(seconds=dur * prog)

            if j == 0:             etype = "trip_start"
            elif j == n-1:         etype = "trip_end"
            elif random.random() < 0.05: etype = random.choice(["pause","resume"])
            else:                  etype = "in_progress"

            bat_val = bat - bat*0.3*prog + random.uniform(-2,2)
            r_bat = random.random()
            if r_bat < 0.01:
                bat_val = round(random.uniform(-20,-1),1); inc_evt["neg_battery"] += 1
            elif r_bat < 0.02:
                bat_val = round(random.uniform(101,150),1); inc_evt["battery_over100"] += 1
            else:
                bat_val = round(max(0,min(100, bat_val)),1)

            if etype in ("trip_start","trip_end"):
                spd = 0.0
            else:
                spd = round(random.uniform(5,25)*math.sin(math.pi*prog), 1)

            r_spd = random.random()
            if r_spd < 0.05:
                spd = None; inc_evt["null_speed"] += 1
            elif r_spd < 0.055:
                spd = round(random.uniform(121,150),1); inc_evt["speed_over120"] += 1

            lat += random.uniform(-0.001,0.001)
            lon += random.uniform(-0.001,0.001)

            f.write(json.dumps({
                "event_id":   f"EVT{evt_counter:07d}",
                "trip_id":    tid,
                "scooter_id": sc,
                "timestamp":  ts.isoformat(),
                "latitude":   round(lat,6),
                "longitude":  round(lon,6),
                "battery_pct": bat_val,
                "speed_kmh":  spd,
                "event_type": etype,
            }, default=str) + "\n")
            evt_counter += 1; written += 1

    # ~1% duplicate event_ids
    n_dup = int(written * 0.01)
    for _ in range(n_dup):
        dup_eid = f"EVT{random.randint(1, evt_counter-1):07d}"
        city = random.choice(CITY_CLEAN)
        bl, bn = CITY_COORDS[city]
        f.write(json.dumps({
            "event_id":   dup_eid,
            "trip_id":    random.choice(unique_trips[:200]),
            "scooter_id": random.choice(scooter_ids),
            "timestamp":  rand_ts().isoformat(),
            "latitude":   round(bl + random.uniform(-0.05,0.05),6),
            "longitude":  round(bn + random.uniform(-0.05,0.05),6),
            "battery_pct": round(random.uniform(20,80),1),
            "speed_kmh":  round(random.uniform(5,25),1),
            "event_type": "in_progress",
        }, default=str) + "\n")
        written += 1; inc_evt["dup_event"] += 1

    # ~2% ghost trip_ids
    n_ghost = int(written * 0.02)
    for _ in range(n_ghost):
        city = random.choice(CITY_CLEAN)
        bl, bn = CITY_COORDS[city]
        f.write(json.dumps({
            "event_id":   f"EVT{evt_counter:07d}",
            "trip_id":    random.choice(ghost_trips),
            "scooter_id": random.choice(scooter_ids),
            "timestamp":  rand_ts().isoformat(),
            "latitude":   round(bl + random.uniform(-0.05,0.05),6),
            "longitude":  round(bn + random.uniform(-0.05,0.05),6),
            "battery_pct": round(random.uniform(20,80),1),
            "speed_kmh":  round(random.uniform(5,25),1),
            "event_type": "in_progress",
        }, default=str) + "\n")
        evt_counter += 1; written += 1; inc_evt["ghost_trip"] += 1

print(f"  ✓ {written} records")
print(f"  Inconsistencies: {inc_evt}\n")
print("✅ All files generated successfully!")
