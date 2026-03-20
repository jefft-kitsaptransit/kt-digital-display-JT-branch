

from flask import Flask, render_template, jsonify
from google.transit import gtfs_realtime_pb2
import requests
import time
import datetime
import csv
import os
from zoneinfo import ZoneInfo

app = Flask(__name__)

# --- CONFIGURATION ---
TRIPS_URL   = "https://kttracker.com/gtfsrt/trips"
ALERTS_URL  = "https://cdn.simplifytransit.com/alerts/service-alerts.pb"
OWM_API_KEY = "OWM_API_KEY"   # OPENWEATHERMAP API KEY  
LAT, LON    = "47.5673", "-122.6329"
LA_TZ       = ZoneInfo("America/Los_Angeles")

# --- SPORTS CONFIG ---
# ESPN public API  
# Each entry: display name, ESPN sport path, ESPN team slug (None = show all, e.g. World Cup)
SPORTS_TEAMS = [
    {"name": "World Cup",  "sport": "soccer/fifa.world",      "team": None,       "color": "#8B0000"},
    {"name": "Sounders",   "sport": "soccer/usa.1",           "team": "seattle-sounders-fc", "color": "#5D9741"},
    {"name": "Seahawks",   "sport": "football/nfl",           "team": "sea",      "color": "#002244"},
    {"name": "Mariners",   "sport": "baseball/mlb",           "team": "sea",      "color": "#0C2C56"},
    {"name": "Kraken",     "sport": "hockey/nhl",             "team": "sea",      "color": "#001628"},
    {"name": "Storm",      "sport": "basketball/wnba",        "team": "sea",      "color": "#2C5234"},
    {"name": "OL Reign",   "sport": "soccer/usa.nwsl",        "team": "seattle-reign-fc", "color": "#010101"},
]

# --- STATIC DATA LOADING ---
STATIC_DIR = os.path.join(os.getcwd(), "static")
ROUTES, TRIPS, CALENDAR, CALENDAR_DATES = {}, {}, {}, {}
BUS_SCHEDULE, FERRY_SCHEDULE = [], []

print("Loading Static GTFS Data...")
try:
    with open(os.path.join(STATIC_DIR, "routes.txt"), "r", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f): ROUTES[row["route_id"]] = row

    with open(os.path.join(STATIC_DIR, "calendar.txt"), "r", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f): CALENDAR[row["service_id"]] = row

    try:
        with open(os.path.join(STATIC_DIR, "calendar_dates.txt"), "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                d = row["date"]
                if d not in CALENDAR_DATES: CALENDAR_DATES[d] = {}
                CALENDAR_DATES[d][row["service_id"]] = row["exception_type"]
    except: print("⚠️ calendar_dates.txt not found - ferry schedules may be incomplete.")

    with open(os.path.join(STATIC_DIR, "trips.txt"), "r", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f): TRIPS[row["trip_id"]] = row

    filtered_file = os.path.join(STATIC_DIR, "stop_times_filtered.txt")
    raw_file      = os.path.join(STATIC_DIR, "stop_times.txt")

    if os.path.exists(filtered_file):
        print("✅ Found filtered stop_times. Loading directly...")
        with open(filtered_file, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                try:
                    h, m, s = map(int, row["departure_time"].split(':'))
                    t_sec = (h * 3600) + (m * 60) + s
                    t_id  = row["trip_id"]
                    if row["stop_id"] == "1":           BUS_SCHEDULE.append({"trip_id": t_id, "time_sec": t_sec})
                    elif row["stop_id"] in ["82","230"]: FERRY_SCHEDULE.append({"trip_id": t_id, "stop_id": row["stop_id"], "time_sec": t_sec})
                except: pass
    elif os.path.exists(raw_file):
        print("⚠️ Found raw stop_times. Calculating arrivals dynamically...")
        max_sequences = {}
        with open(raw_file, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                t_id, seq = row["trip_id"], int(row["stop_sequence"])
                if t_id not in max_sequences or seq > max_sequences[t_id]: max_sequences[t_id] = seq
        with open(raw_file, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                t_id, seq = row["trip_id"], int(row["stop_sequence"])
                if seq == max_sequences.get(t_id): continue
                try:
                    h, m, s = map(int, row["departure_time"].split(':'))
                    t_sec = (h * 3600) + (m * 60) + s
                    if row["stop_id"] == "1":           BUS_SCHEDULE.append({"trip_id": t_id, "time_sec": t_sec})
                    elif row["stop_id"] in ["82","230"]: FERRY_SCHEDULE.append({"trip_id": t_id, "stop_id": row["stop_id"], "time_sec": t_sec})
                except: pass

    BUS_SCHEDULE.sort(key=lambda x: x["time_sec"])
    FERRY_SCHEDULE.sort(key=lambda x: x["time_sec"])
    print(f"✅ Loaded {len(BUS_SCHEDULE)} buses and {len(FERRY_SCHEDULE)} ferries.")
except Exception as e: print(f"❌ Static Load Error: {e}")

# --- HELPERS ---
def get_active_services(target_date=None):
    if not target_date: target_date = datetime.datetime.now(LA_TZ)
    date_str = target_date.strftime("%Y%m%d")
    day_name = target_date.strftime("%A").lower()
    active = {s_id for s_id, r in CALENDAR.items() if r[day_name] == '1' and r['start_date'] <= date_str <= r['end_date']}
    if date_str in CALENDAR_DATES:
        for s_id, ex_type in CALENDAR_DATES[date_str].items():
            if ex_type == '1': active.add(s_id)
            elif ex_type == '2': active.discard(s_id)
    return active

# --- WEATHER CACHE ---
weather_cache = {"data": {"current": {"temp": "--", "desc": "Loading..."}, "forecast": [], "hourly": []}, "last_fetched": 0}

def get_weather():
    now = time.time()
    if now - weather_cache["last_fetched"] > 900 and OWM_API_KEY != "OWM_API_KEY":
        try:
            cur  = requests.get(f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={OWM_API_KEY}&units=imperial", timeout=5).json()
            fore = requests.get(f"https://api.openweathermap.org/data/2.5/forecast?lat={LAT}&lon={LON}&appid={OWM_API_KEY}&units=imperial", timeout=5).json()
            if "main" in cur:
                weather_cache["data"]["current"] = {
                    "temp": f"{int(cur['main']['temp'])}\u00b0F",
                    "desc": cur["weather"][0]["main"],
                    "icon": f"https://openweathermap.org/img/wn/{cur['weather'][0]['icon']}@2x.png"
                }
                # 5-day forecast (daily highs/lows)
                daily = {}
                for item in fore["list"]:
                    day = datetime.datetime.fromtimestamp(item["dt"], tz=LA_TZ).strftime("%A")[:3]
                    if day == datetime.datetime.now(LA_TZ).strftime("%A")[:3]: continue
                    if day not in daily: daily[day] = {"day": day, "high": -999, "low": 999, "icon": item["weather"][0]["icon"].replace('n','d')}
                    if item["main"]["temp"] > daily[day]["high"]: daily[day]["high"] = item["main"]["temp"]
                    if item["main"]["temp"] < daily[day]["low"]:  daily[day]["low"]  = item["main"]["temp"]
                weather_cache["data"]["forecast"] = [
                    {"day": d["day"].upper(), "high": f"{int(d['high'])}\u00b0", "low": f"{int(d['low'])}\u00b0",
                     "icon": f"https://openweathermap.org/img/wn/{d['icon']}@2x.png"}
                    for d in list(daily.values())[:5]
                ]
                # Hourly — next 5 slots (3hr intervals from OWM free tier)
                weather_cache["data"]["hourly"] = [
                    {"time": datetime.datetime.fromtimestamp(item["dt"], tz=LA_TZ).strftime("%I%p").lstrip("0").lower(),
                     "temp": f"{int(item['main']['temp'])}\u00b0",
                     "desc": item["weather"][0]["main"],
                     "icon": f"https://openweathermap.org/img/wn/{item['weather'][0]['icon']}@2x.png",
                     "pop":  f"{int(item.get('pop', 0) * 100)}%"}
                    for item in fore["list"][:5]
                ]
                weather_cache["last_fetched"] = now
        except Exception as e:
            print(f"⚠️ Weather fetch error: {e}")
    return weather_cache["data"]

# --- SPORTS CACHE ---
sports_cache = {"data": [], "last_fetched": 0}

def get_sports():
    now = time.time()
    if now - sports_cache["last_fetched"] < 60:
        return sports_cache["data"]

    results = []
    today     = datetime.datetime.now(LA_TZ).date()
    tomorrow  = today + datetime.timedelta(days=1)
    yesterday = today - datetime.timedelta(days=1)

    for team_cfg in SPORTS_TEAMS:
        sport = team_cfg["sport"]
        slug  = team_cfg["team"]
        name  = team_cfg["name"]
        color = team_cfg["color"]

        try:
            url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/scoreboard"
            resp = requests.get(url, timeout=5).json()
            events = resp.get("events", [])

            # For World Cup show ALL games; for Seattle teams filter to their games
            team_games = []
            for ev in events:
                if slug is None:
                    # World Cup — include all
                    team_games.append(ev)
                else:
                    for comp in ev.get("competitions", []):
                        for comp_team in comp.get("competitors", []):
                            if slug in comp_team.get("team", {}).get("slug", "").lower() or \
                               slug in comp_team.get("team", {}).get("abbreviation", "").lower():
                                team_games.append(ev)

            if not team_games:
                # Nothing today — find last result and next upcoming
                last_result  = fetch_last_result(sport, slug)
                next_game    = fetch_next_game(sport, slug)
                if last_result or next_game:
                    results.append({
                        "name":       name,
                        "color":      color,
                        "mode":       "idle",
                        "last":       last_result,
                        "next":       next_game,
                        "games":      []
                    })
                continue

            # Parse today's games
            parsed_games = []
            for ev in team_games:
                comp  = ev["competitions"][0]
                teams = comp["competitors"]
                status_type = ev["status"]["type"]
                state       = status_type.get("state", "")    # pre / in / post
                detail      = status_type.get("shortDetail", "")
                display_clock = ev["status"].get("displayClock", "")
                period        = ev["status"].get("period", 0)

                home = next((t for t in teams if t["homeAway"] == "home"), teams[0])
                away = next((t for t in teams if t["homeAway"] == "away"), teams[1])

                game = {
                    "home_name":  home["team"]["shortDisplayName"],
                    "away_name":  away["team"]["shortDisplayName"],
                    "home_score": home.get("score", "-"),
                    "away_score": away.get("score", "-"),
                    "home_logo":  home["team"].get("logo", ""),
                    "away_logo":  away["team"].get("logo", ""),
                    "state":      state,
                    "detail":     detail,
                    "clock":      display_clock,
                    "period":     period,
                    "date":       ev["date"],
                }
                parsed_games.append(game)

            mode = "live" if any(g["state"] == "in" for g in parsed_games) else \
                   "final" if all(g["state"] == "post" for g in parsed_games) else "scheduled"

            results.append({
                "name":  name,
                "color": color,
                "mode":  mode,
                "games": parsed_games,
                "last":  None,
                "next":  None
            })

        except Exception as e:
            print(f"⚠️ Sports fetch error ({name}): {e}")

    sports_cache["data"]         = results
    sports_cache["last_fetched"] = now
    return results

def fetch_last_result(sport, slug):
    """Fetch most recent completed game for a team."""
    try:
        url  = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/scoreboard?limit=10"
        resp = requests.get(url, timeout=5).json()
        for ev in reversed(resp.get("events", [])):
            state = ev["status"]["type"].get("state", "")
            if state != "post": continue
            comp  = ev["competitions"][0]
            teams = comp["competitors"]
            if slug and not any(slug in t.get("team", {}).get("slug","").lower() or
                                slug in t.get("team", {}).get("abbreviation","").lower()
                                for t in teams):
                continue
            home  = next((t for t in teams if t["homeAway"] == "home"), teams[0])
            away  = next((t for t in teams if t["homeAway"] == "away"), teams[1])
            date  = datetime.datetime.fromisoformat(ev["date"].replace("Z","+00:00")).astimezone(LA_TZ)
            return {
                "home_name": home["team"]["shortDisplayName"], "away_name": away["team"]["shortDisplayName"],
                "home_score": home.get("score","-"), "away_score": away.get("score","-"),
                "date_str": date.strftime("%b %-d")
            }
    except Exception as e:
        print(f"⚠️ Last result fetch error: {e}")
    return None

def fetch_next_game(sport, slug):
    """Fetch next scheduled game for a team."""
    try:
        url  = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/scoreboard?limit=10"
        resp = requests.get(url, timeout=5).json()
        for ev in resp.get("events", []):
            state = ev["status"]["type"].get("state","")
            if state != "pre": continue
            comp  = ev["competitions"][0]
            teams = comp["competitors"]
            if slug and not any(slug in t.get("team",{}).get("slug","").lower() or
                                slug in t.get("team",{}).get("abbreviation","").lower()
                                for t in teams):
                continue
            home = next((t for t in teams if t["homeAway"] == "home"), teams[0])
            away = next((t for t in teams if t["homeAway"] == "away"), teams[1])
            date = datetime.datetime.fromisoformat(ev["date"].replace("Z","+00:00")).astimezone(LA_TZ)
            return {
                "home_name": home["team"]["shortDisplayName"], "away_name": away["team"]["shortDisplayName"],
                "date_str":  date.strftime("%b %-d"),
                "time_str":  date.strftime("%-I:%M %p")
            }
    except Exception as e:
        print(f"⚠️ Next game fetch error: {e}")
    return None

# --- API ROUTES ---
@app.route('/api/data')
def get_board_data():
    now = datetime.datetime.now(LA_TZ)
    curr_posix = int(now.timestamp())
    curr_sec   = (now.hour * 3600) + (now.minute * 60) + now.second
    if now.hour < 3: curr_sec += 86400

    buses, ferries, alerts = [], [], []
    displayed_route_ids = {"400", "500", "501"}
    rt_trips, raw_alerts = {}, []

    try:
        a_f = gtfs_realtime_pb2.FeedMessage()
        a_f.ParseFromString(requests.get(ALERTS_URL, timeout=5).content)
        raw_alerts = a_f.entity
        t_f = gtfs_realtime_pb2.FeedMessage()
        t_f.ParseFromString(requests.get(TRIPS_URL, timeout=5).content)
        for e in t_f.entity:
            if e.HasField('trip_update'): rt_trips[e.trip_update.trip.trip_id] = e
    except Exception as e:
        print(f"⚠️ RT feed error: {e}")

    active_svcs = get_active_services()

    def process_bus_list(service_list, time_offset, is_tomorrow=False):
        results, seen, candidates = [], set(), []
        for s in BUS_SCHEDULE:
            t_id = s["trip_id"]
            if TRIPS.get(t_id, {}).get("service_id") in service_list and s["time_sec"] > time_offset:
                rid    = TRIPS[t_id]["route_id"]
                rt_time = None
                if t_id in rt_trips:
                    for stu in rt_trips[t_id].trip_update.stop_time_update:
                        if stu.stop_id == "1": rt_time = stu.departure.time
                target = rt_time if rt_time else (s["time_sec"] + curr_posix - curr_sec + (86400 if is_tomorrow else 0))
                candidates.append({"rid": rid, "t_id": t_id, "target": target, "eta_s": target - curr_posix})

        candidates.sort(key=lambda x: x["target"])
        for c in candidates:
            if c["rid"] in seen: continue
            r_info   = ROUTES.get(c["rid"], {})
            headsign = TRIPS[c["t_id"]].get("trip_headsign", "Local")
            dt       = datetime.datetime.fromtimestamp(c["target"], tz=LA_TZ)
            eta_txt  = "BOARDING" if c["eta_s"] <= 90 else (f"{int(c['eta_s']/60)} Min" if c['eta_s'] <= 300 else f"{dt.strftime('%I:%M %p').lstrip('0')}")
            if is_tomorrow: eta_txt = f"Tomorrow {dt.strftime('%I:%M %p').lstrip('0')}"
            results.append({
                "route":      r_info.get("route_short_name", c["rid"]),
                "color":      f"#{r_info.get('route_color','000')}",
                "text_color": f"#{r_info.get('route_text_color','fff')}",
                "destination": headsign.split(" via ")[0],
                "via":        f"via {headsign.split(' via ')[1]}" if " via " in headsign else "",
                "eta":        eta_txt,
                "eta_seconds": c["eta_s"]
            })
            seen.add(c["rid"]); displayed_route_ids.add(c["rid"])
        return results

    buses = process_bus_list(active_svcs, curr_sec - 60)
    if not buses:
        tomorrow_svcs = get_active_services(now + datetime.timedelta(days=1))
        buses = process_bus_list(tomorrow_svcs, 0, True)[:6]

    # FERRY LOGIC
    f_targets = {
        "400": {"name": "Seattle",      "stop": "230", "found": False},
        "500": {"name": "Port Orchard", "stop": "82",  "found": False},
        "501": {"name": "Annapolis",    "stop": "82",  "found": False}
    }
    for f in FERRY_SCHEDULE:
        t_id = f["trip_id"]
        if TRIPS.get(t_id, {}).get("service_id") in active_svcs and f["time_sec"] > curr_sec - 120:
            rid = TRIPS[t_id]["route_id"]
            if rid in f_targets and f["stop_id"] == f_targets[rid]["stop"] and not f_targets[rid]["found"]:
                f_targets[rid]["found"] = True
                rt_time = None
                if t_id in rt_trips:
                    for stu in rt_trips[t_id].trip_update.stop_time_update:
                        if stu.stop_id == f["stop_id"]: rt_time = stu.departure.time if stu.departure.time > 0 else stu.arrival.time
                target = rt_time if rt_time else (f["time_sec"] + curr_posix - curr_sec)
                dt = datetime.datetime.fromtimestamp(target, tz=LA_TZ)
                ferries.append({
                    "route": rid, "color": f"#{ROUTES[rid].get('route_color','000000')}",
                    "text_color": f"#{ROUTES[rid].get('route_text_color','ffffff')}",
                    "destination": f_targets[rid]["name"], "status": "ON TIME",
                    "time_str": dt.strftime("%I:%M %p").lstrip("0"), "eta_seconds": target - curr_posix
                })

    for rid, t in f_targets.items():
        if not t["found"]:
            ferries.append({"route": rid, "color": "#95a5a6", "text_color": "#ffffff",
                            "destination": t["name"], "status": "COMPLETED",
                            "time_str": "NO MORE SAILINGS", "eta_seconds": 99999})

    # ALERT LOGIC
    for e in raw_alerts:
        if e.HasField('alert'):
            try:
                msg = e.alert.header_text.translation[0].text.replace('\n',' ').strip()
                if not msg: continue
                is_system_wide = False
                matches_displayed_route = False
                if not e.alert.informed_entity:
                    is_system_wide = True
                else:
                    for ie in e.alert.informed_entity:
                        if not ie.HasField('route_id'): is_system_wide = True
                        elif ie.route_id in displayed_route_ids: matches_displayed_route = True
                if (is_system_wide or matches_displayed_route) and msg not in alerts:
                    alerts.append(msg)
            except: pass

    return jsonify({
        "buses":   sorted(buses,   key=lambda x: x["eta_seconds"]),
        "ferries": sorted(ferries, key=lambda x: x["eta_seconds"]),
        "alerts":  alerts,
        "weather": get_weather(),
        "sports":  get_sports()
    })

@app.route('/')
def index(): return render_template('index.html')

if __name__ == '__main__': app.run(host='0.0.0.0', port=5000, debug=True)


