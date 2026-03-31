from flask import Flask, render_template, jsonify
from google.transit import gtfs_realtime_pb2
import requests
import time
import datetime
import csv
import json
import os
import logging
import threading
from logging.handlers import RotatingFileHandler
from collections import deque
from zoneinfo import ZoneInfo

app = Flask(__name__)

# --- LOGGING ---
LOG_FILE = os.path.join(os.getcwd(), "app.log")
file_handler = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=3)
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
file_handler.setLevel(logging.WARNING)

logger = logging.getLogger("ktdisplay")
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)

# Also log to stdout for PM2
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
stream_handler.setLevel(logging.INFO)
logger.addHandler(stream_handler)

# Ring buffer of recent errors for /health
error_log = deque(maxlen=30)
def log_error(msg):
    ts = datetime.datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d %H:%M:%S")
    error_log.append({"time": ts, "error": msg})
    logger.error(msg)

# --- CONFIGURATION ---
TRIPS_URL   = "https://kttracker.com/gtfsrt/trips"
ALERTS_URL  = "https://cdn.simplifytransit.com/kitsap-transit/alerts/service-alerts.pb"
LAT, LON    = "47.5673", "-122.6329"
LA_TZ       = ZoneInfo("America/Los_Angeles")
APP_START   = time.time()

# --- SPORTS CONFIG ---
SPORTS_TEAMS = [
    {"name": "World Cup",  "sport": "soccer/fifa.world",      "team": None,                  "color": "#8B0000"},
    {"name": "Sounders",   "sport": "soccer/usa.1",           "team": "seattle-sounders-fc", "color": "#5D9741"},
    {"name": "Seahawks",   "sport": "football/nfl",           "team": "sea",                 "color": "#002244"},
    {"name": "Mariners",   "sport": "baseball/mlb",           "team": "sea",                 "color": "#0C2C56"},
    {"name": "Kraken",     "sport": "hockey/nhl",             "team": "sea",                 "color": "#001628"},
    {"name": "Storm",      "sport": "basketball/wnba",        "team": "sea",                 "color": "#2C5234"},
    {"name": "OL Reign",   "sport": "soccer/usa.nwsl",        "team": "seattle-reign-fc",    "color": "#010101"},
]

# --- STATIC DATA LOADING ---
STATIC_DIR = os.path.join(os.getcwd(), "static")
ROUTES, TRIPS, CALENDAR, CALENDAR_DATES = {}, {}, {}, {}
BUS_SCHEDULE, FERRY_SCHEDULE = [], []
BAY_LOOKUP = {}

logger.info("Loading Static GTFS Data...")
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
    except: logger.warning("calendar_dates.txt not found - ferry schedules may be incomplete.")

    with open(os.path.join(STATIC_DIR, "trips.txt"), "r", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f): TRIPS[row["trip_id"]] = row

    filtered_file = os.path.join(STATIC_DIR, "stop_times_filtered.txt")
    raw_file      = os.path.join(STATIC_DIR, "stop_times.txt")

    if os.path.exists(filtered_file):
        logger.info("Found filtered stop_times. Loading directly...")
        with open(filtered_file, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                try:
                    h, m, s = map(int, row["departure_time"].split(':'))
                    t_sec = (h * 3600) + (m * 60) + s
                    t_id  = row["trip_id"]
                    if row["stop_id"] == "1":            BUS_SCHEDULE.append({"trip_id": t_id, "time_sec": t_sec})
                    elif row["stop_id"] in ["82", "230"]: FERRY_SCHEDULE.append({"trip_id": t_id, "stop_id": row["stop_id"], "time_sec": t_sec})
                except: pass
    elif os.path.exists(raw_file):
        logger.warning("Found raw stop_times. Calculating arrivals dynamically...")
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
                    if row["stop_id"] == "1":            BUS_SCHEDULE.append({"trip_id": t_id, "time_sec": t_sec})
                    elif row["stop_id"] in ["82", "230"]: FERRY_SCHEDULE.append({"trip_id": t_id, "stop_id": row["stop_id"], "time_sec": t_sec})
                except: pass

    BUS_SCHEDULE.sort(key=lambda x: x["time_sec"])
    FERRY_SCHEDULE.sort(key=lambda x: x["time_sec"])
    logger.info(f"Loaded {len(BUS_SCHEDULE)} buses and {len(FERRY_SCHEDULE)} ferries.")
except Exception as e:
    log_error(f"Static Load Error: {e}")

# --- BAY DATA LOADING ---
BAY_FILE = os.path.join(os.getcwd(), "bay_data.json")
try:
    with open(BAY_FILE, "r") as f:
        bay_entries = json.load(f)
    for entry in bay_entries:
        BAY_LOOKUP[str(entry["trip_id"])] = entry["bay"]
    logger.info(f"Loaded {len(BAY_LOOKUP)} bay assignments.")
except Exception as e:
    log_error(f"Bay data not loaded: {e}")

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

def split_headsign(headsign):
    """Split a headsign into (destination, via) using 'via', 'and', or '/' as fallback delimiters."""
    if " via " in headsign:
        parts = headsign.split(" via ", 1)
        return parts[0], f"via {parts[1]}"
    if len(headsign) > 22:
        if " and " in headsign:
            parts = headsign.split(" and ", 1)
            return parts[0], f"& {parts[1]}"
        if "/" in headsign:
            parts = headsign.split("/", 1)
            return parts[0], f"/ {parts[1]}"
    return headsign, ""

# --- RT FEED CACHE (survives up to 5 min of endpoint failures) ---
RT_CACHE_MAX_AGE = 300  # 5 minutes
rt_cache = {
    "rt_trips": {},
    "raw_alerts": [],
    "canceled_trips": set(),
    "last_fetched": 0,
    "last_success": 0,
}

def fetch_rt_feeds():
    """Fetch GTFS-RT trip updates and alerts. Cache result; on failure use stale cache up to 5 min."""
    now = time.time()
    rt_trips, canceled_trips = {}, set()
    raw_alerts = []
    try:
        a_f = gtfs_realtime_pb2.FeedMessage()
        a_f.ParseFromString(requests.get(ALERTS_URL, timeout=5).content)
        raw_alerts = list(a_f.entity)
        t_f = gtfs_realtime_pb2.FeedMessage()
        t_f.ParseFromString(requests.get(TRIPS_URL, timeout=5).content)
        for e in t_f.entity:
            if e.HasField('trip_update'):
                trip = e.trip_update.trip
                if trip.schedule_relationship == gtfs_realtime_pb2.TripDescriptor.CANCELED:
                    canceled_trips.add(trip.trip_id)
                else:
                    rt_trips[trip.trip_id] = e
        rt_cache["rt_trips"] = rt_trips
        rt_cache["raw_alerts"] = raw_alerts
        rt_cache["canceled_trips"] = canceled_trips
        rt_cache["last_success"] = now
        rt_cache["last_fetched"] = now
        data_freshness["rt"] = now
    except Exception as e:
        rt_cache["last_fetched"] = now
        age = now - rt_cache["last_success"] if rt_cache["last_success"] else 999
        if age < RT_CACHE_MAX_AGE:
            log_error(f"RT feed error (using {int(age)}s old cache): {e}")
        else:
            log_error(f"RT feed error (cache expired): {e}")
            rt_cache["rt_trips"] = {}
            rt_cache["raw_alerts"] = []
            rt_cache["canceled_trips"] = set()

# --- WEATHER CACHE ---
WMO_CODES = {
    0:  ("Clear",         "wi-day-sunny"),
    1:  ("Mostly Clear",  "wi-day-cloudy"),
    2:  ("Partly Cloudy", "wi-day-cloudy-high"),
    3:  ("Overcast",      "wi-cloudy"),
    45: ("Fog",           "wi-fog"),
    48: ("Fog",           "wi-fog"),
    51: ("Drizzle",       "wi-sprinkle"),
    53: ("Drizzle",       "wi-sprinkle"),
    55: ("Drizzle",       "wi-sprinkle"),
    61: ("Rain",          "wi-rain"),
    63: ("Rain",          "wi-rain"),
    65: ("Heavy Rain",    "wi-rain-wind"),
    71: ("Snow",          "wi-snow"),
    73: ("Snow",          "wi-snow"),
    75: ("Heavy Snow",    "wi-snow-wind"),
    80: ("Showers",       "wi-showers"),
    81: ("Showers",       "wi-showers"),
    82: ("Heavy Showers", "wi-storm-showers"),
    95: ("Thunderstorm",  "wi-thunderstorm"),
    96: ("Thunderstorm",  "wi-thunderstorm"),
    99: ("Thunderstorm",  "wi-thunderstorm"),
}

def wmo_icon(code):
    desc, css_class = WMO_CODES.get(code, ("Cloudy", "wi-cloudy"))
    return desc, css_class

weather_cache = {"data": {"current": {"temp": "--", "desc": "Loading..."}, "forecast": [], "hourly": []}, "last_fetched": 0}

def fetch_weather():
    now = time.time()
    if now - weather_cache["last_fetched"] > 900:
        try:
            url = (
                "https://api.open-meteo.com/v1/forecast"
                "?latitude=47.5673&longitude=-122.6329"
                "&current=temperature_2m,weathercode"
                "&hourly=temperature_2m,precipitation_probability,weathercode"
                "&daily=weathercode,temperature_2m_max,temperature_2m_min"
                "&temperature_unit=fahrenheit"
                "&timezone=America%2FLos_Angeles"
                "&forecast_days=6"
            )
            d = requests.get(url, timeout=5).json()

            cur_temp = int(d["current"]["temperature_2m"])
            cur_code = d["current"]["weathercode"]
            cur_desc, cur_icon = wmo_icon(cur_code)
            weather_cache["data"]["current"] = {
                "temp":       f"{cur_temp}°F",
                "desc":       cur_desc,
                "icon_class": cur_icon
            }

            daily = d["daily"]
            forecast = []
            for i in range(0, 5):
                date = datetime.datetime.strptime(daily["time"][i], "%Y-%m-%d")
                desc, icon = wmo_icon(daily["weathercode"][i])
                forecast.append({
                    "day":        date.strftime("%a").upper(),
                    "high":       f"{int(daily['temperature_2m_max'][i])}°",
                    "low":        f"{int(daily['temperature_2m_min'][i])}°",
                    "icon_class": icon
                })
            weather_cache["data"]["forecast"] = forecast

            hourly = d["hourly"]
            now_dt = datetime.datetime.now(LA_TZ)
            current_hour = now_dt.hour
            start = current_hour + (2 - current_hour % 2)
            indices = [start + i * 2 for i in range(5)]
            slots = []
            for i in indices:
                t = datetime.datetime.strptime(hourly["time"][i], "%Y-%m-%dT%H:%M")
                desc, icon = wmo_icon(hourly["weathercode"][i])
                slots.append({
                    "time":       t.strftime("%I%p").lstrip("0"),
                    "temp":       f"{int(hourly['temperature_2m'][i])}°",
                    "desc":       desc,
                    "icon_class": icon,
                    "pop":        f"{hourly['precipitation_probability'][i]}%"
                })
            weather_cache["data"]["hourly"] = slots
            weather_cache["last_fetched"] = now
            data_freshness["weather"] = now

        except Exception as e:
            log_error(f"Weather fetch error: {e}")

# --- SPORTS CACHE ---
sports_cache = {"data": [], "last_fetched": 0}

def fetch_sports():
    now = time.time()
    if now - sports_cache["last_fetched"] < 60:
        return

    results = []

    for team_cfg in SPORTS_TEAMS:
        sport = team_cfg["sport"]
        slug  = team_cfg["team"]
        name  = team_cfg["name"]
        color = team_cfg["color"]

        try:
            yesterday_str  = (datetime.datetime.now(LA_TZ) - datetime.timedelta(days=1)).strftime("%Y%m%d")
            resp_yesterday = requests.get(
                f"https://site.api.espn.com/apis/site/v2/sports/{sport}/scoreboard?dates={yesterday_str}",
                timeout=5
            ).json()
            resp_today = requests.get(
                f"https://site.api.espn.com/apis/site/v2/sports/{sport}/scoreboard",
                timeout=5
            ).json()
            events = resp_today.get("events", []) + resp_yesterday.get("events", [])

            team_games = []
            seen_ids = set()
            for ev in events:
                ev_id = ev.get("id")
                if ev_id in seen_ids: continue
                if slug is None:
                    team_games.append(ev)
                    seen_ids.add(ev_id)
                else:
                    for comp in ev.get("competitions", []):
                        for comp_team in comp.get("competitors", []):
                            if slug in comp_team.get("team", {}).get("slug", "").lower() or \
                               slug in comp_team.get("team", {}).get("abbreviation", "").lower():
                                team_games.append(ev)
                                seen_ids.add(ev_id)

            if not team_games:
                last_result = fetch_last_result(sport, slug)
                next_game   = fetch_next_game(sport, slug)
                if last_result or next_game:
                    results.append({
                        "name":  name,
                        "color": color,
                        "mode":  "idle",
                        "last":  last_result,
                        "next":  next_game,
                        "games": []
                    })
                continue

            parsed_games = []
            for ev in team_games:
                comp        = ev["competitions"][0]
                teams       = comp["competitors"]
                status_type = ev["status"]["type"]
                state       = status_type.get("state", "")
                detail      = status_type.get("shortDetail", "")
                home = next((t for t in teams if t["homeAway"] == "home"), teams[0])
                away = next((t for t in teams if t["homeAway"] == "away"), teams[1])
                parsed_games.append({
                    "home_name":  home["team"]["shortDisplayName"],
                    "away_name":  away["team"]["shortDisplayName"],
                    "home_score": home.get("score", "-"),
                    "away_score": away.get("score", "-"),
                    "home_logo":  home["team"].get("logo", ""),
                    "away_logo":  away["team"].get("logo", ""),
                    "state":      state,
                    "detail":     detail,
                    "clock":      ev["status"].get("displayClock", ""),
                    "period":     ev["status"].get("period", 0),
                    "date":       ev["date"],
                })

            mode = "live"  if any(g["state"] == "in"  for g in parsed_games) else \
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
            log_error(f"Sports fetch error ({name}): {e}")

    sports_cache["data"]         = results
    sports_cache["last_fetched"] = now
    data_freshness["sports"] = now

def fetch_last_result(sport, slug):
    """Fetch most recent completed game within 3 days."""
    try:
        resp   = requests.get(
            f"https://site.api.espn.com/apis/site/v2/sports/{sport}/scoreboard?limit=10",
            timeout=5
        ).json()
        cutoff = datetime.datetime.now(LA_TZ) - datetime.timedelta(days=3)
        for ev in reversed(resp.get("events", [])):
            if ev["status"]["type"].get("state", "") != "post": continue
            date = datetime.datetime.fromisoformat(ev["date"].replace("Z", "+00:00")).astimezone(LA_TZ)
            if date < cutoff: continue
            teams = ev["competitions"][0]["competitors"]
            if slug and not any(
                slug in t.get("team", {}).get("slug", "").lower() or
                slug in t.get("team", {}).get("abbreviation", "").lower()
                for t in teams
            ): continue
            home = next((t for t in teams if t["homeAway"] == "home"), teams[0])
            away = next((t for t in teams if t["homeAway"] == "away"), teams[1])
            return {
                "home_name":  home["team"]["shortDisplayName"],
                "away_name":  away["team"]["shortDisplayName"],
                "home_score": home.get("score", "-"),
                "away_score": away.get("score", "-"),
                "home_logo":  home["team"].get("logo", ""),
                "away_logo":  away["team"].get("logo", ""),
                "date":       ev["date"],
                "date_str":   date.strftime("%b %d")
            }
    except Exception as e:
        log_error(f"Last result fetch error: {e}")
    return None

def fetch_next_game(sport, slug):
    """Fetch next scheduled game for a team."""
    try:
        resp = requests.get(
            f"https://site.api.espn.com/apis/site/v2/sports/{sport}/scoreboard?limit=10",
            timeout=5
        ).json()
        for ev in resp.get("events", []):
            if ev["status"]["type"].get("state", "") != "pre": continue
            teams = ev["competitions"][0]["competitors"]
            if slug and not any(
                slug in t.get("team", {}).get("slug", "").lower() or
                slug in t.get("team", {}).get("abbreviation", "").lower()
                for t in teams
            ): continue
            home = next((t for t in teams if t["homeAway"] == "home"), teams[0])
            away = next((t for t in teams if t["homeAway"] == "away"), teams[1])
            date = datetime.datetime.fromisoformat(ev["date"].replace("Z", "+00:00")).astimezone(LA_TZ)
            return {
                "home_name":  home["team"]["shortDisplayName"],
                "away_name":  away["team"]["shortDisplayName"],
                "home_logo":  home["team"].get("logo", ""),
                "away_logo":  away["team"].get("logo", ""),
                "date":       ev["date"],
                "date_str":   date.strftime("%b %d"),
                "time_str":   date.strftime("%I:%M %p").lstrip("0")
            }
    except Exception as e:
        log_error(f"Next game fetch error: {e}")
    return None

# --- BACKGROUND DATA REFRESH ---
data_freshness = {"rt": 0, "weather": 0, "sports": 0}
board_cache = {"data": None, "test_data": None}

def build_board_data(all_trips=False):
    """Build the board JSON from cached RT/weather/sports data."""
    now = datetime.datetime.now(LA_TZ)
    curr_posix = int(now.timestamp())
    curr_sec   = (now.hour * 3600) + (now.minute * 60) + now.second
    if now.hour < 3: curr_sec += 86400

    buses, ferries, alerts = [], [], []
    displayed_route_ids = {"400", "500", "501"}
    rt_trips = rt_cache["rt_trips"]
    raw_alerts = rt_cache["raw_alerts"]
    canceled_trips = rt_cache["canceled_trips"]

    active_svcs = get_active_services()

    if all_trips:
        # ALL buses — no one-per-route filter
        candidates = []
        for s in BUS_SCHEDULE:
            t_id = s["trip_id"]
            if TRIPS.get(t_id, {}).get("service_id") in active_svcs and s["time_sec"] > curr_sec - 60:
                rid     = TRIPS[t_id]["route_id"]
                rt_time = None
                if t_id in rt_trips:
                    for stu in rt_trips[t_id].trip_update.stop_time_update:
                        if stu.stop_id == "1": rt_time = stu.departure.time
                target = rt_time if rt_time else (s["time_sec"] + curr_posix - curr_sec)
                is_canceled = t_id in canceled_trips
                candidates.append({"rid": rid, "t_id": t_id, "target": target, "eta_s": target - curr_posix, "canceled": is_canceled})

        candidates.sort(key=lambda x: x["target"])
        for c in candidates:
            rid = c["rid"]
            r_info   = ROUTES.get(rid, {})
            headsign = TRIPS[c["t_id"]].get("trip_headsign", "Local")
            dest, via = split_headsign(headsign)
            bay      = BAY_LOOKUP.get(c["t_id"], "")
            displayed_route_ids.add(rid)
            if c["canceled"]:
                dt = datetime.datetime.fromtimestamp(c["target"], tz=LA_TZ)
                buses.append({
                    "route": r_info.get("route_short_name", rid), "color": f"#{r_info.get('route_color','000')}",
                    "text_color": f"#{r_info.get('route_text_color','fff')}", "destination": dest, "via": via,
                    "eta": "CANCELLED", "eta_seconds": c["eta_s"], "canceled": True, "bay": bay,
                    "time_str": dt.strftime('%I:%M %p').lstrip('0')
                })
            else:
                dt = datetime.datetime.fromtimestamp(c["target"], tz=LA_TZ)
                eta_txt = "BOARDING" if c["eta_s"] <= 90 else (f"{int(c['eta_s']/60)} Min" if c['eta_s'] <= 300 else f"{dt.strftime('%I:%M %p').lstrip('0')}")
                buses.append({
                    "route": r_info.get("route_short_name", rid), "color": f"#{r_info.get('route_color','000')}",
                    "text_color": f"#{r_info.get('route_text_color','fff')}", "destination": dest, "via": via,
                    "eta": eta_txt, "eta_seconds": c["eta_s"], "canceled": False, "bay": bay
                })

        # If no buses today, show tomorrow's
        if not buses:
            tomorrow = now + datetime.timedelta(days=1)
            tomorrow_svcs = get_active_services(tomorrow)
            for s in BUS_SCHEDULE:
                t_id = s["trip_id"]
                if TRIPS.get(t_id, {}).get("service_id") in tomorrow_svcs:
                    rid      = TRIPS[t_id]["route_id"]
                    r_info   = ROUTES.get(rid, {})
                    headsign = TRIPS[t_id].get("trip_headsign", "Local")
                    dest, via = split_headsign(headsign)
                    bay      = BAY_LOOKUP.get(t_id, "")
                    target   = s["time_sec"] + curr_posix - curr_sec + 86400
                    dt       = datetime.datetime.fromtimestamp(target, tz=LA_TZ)
                    buses.append({
                        "route": r_info.get("route_short_name", rid), "color": f"#{r_info.get('route_color','000')}",
                        "text_color": f"#{r_info.get('route_text_color','fff')}", "destination": dest, "via": via,
                        "eta": f"Tomorrow {dt.strftime('%I:%M %p').lstrip('0')}", "eta_seconds": target - curr_posix,
                        "canceled": False, "bay": bay
                    })
    else:
        # One per route (main dashboard)
        def process_bus_list(service_list, time_offset, is_tomorrow=False):
            results, seen, candidates = [], set(), []
            for s in BUS_SCHEDULE:
                t_id = s["trip_id"]
                if TRIPS.get(t_id, {}).get("service_id") in service_list and s["time_sec"] > time_offset:
                    rid     = TRIPS[t_id]["route_id"]
                    rt_time = None
                    if t_id in rt_trips:
                        for stu in rt_trips[t_id].trip_update.stop_time_update:
                            if stu.stop_id == "1": rt_time = stu.departure.time
                    target = rt_time if rt_time else (s["time_sec"] + curr_posix - curr_sec + (86400 if is_tomorrow else 0))
                    is_canceled = t_id in canceled_trips
                    candidates.append({"rid": rid, "t_id": t_id, "target": target, "eta_s": target - curr_posix, "canceled": is_canceled})

            candidates.sort(key=lambda x: x["target"])
            used_tids = set()
            for c in candidates:
                rid = c["rid"]
                if c["canceled"]:
                    r_info   = ROUTES.get(rid, {})
                    headsign = TRIPS[c["t_id"]].get("trip_headsign", "Local")
                    dest, via = split_headsign(headsign)
                    bay      = BAY_LOOKUP.get(c["t_id"], "")
                    dt       = datetime.datetime.fromtimestamp(c["target"], tz=LA_TZ)
                    results.append({
                        "route": r_info.get("route_short_name", rid), "color": f"#{r_info.get('route_color','000')}",
                        "text_color": f"#{r_info.get('route_text_color','fff')}", "destination": dest, "via": via,
                        "eta": "CANCELLED", "eta_seconds": c["eta_s"], "canceled": True, "bay": bay,
                        "time_str": dt.strftime('%I:%M %p').lstrip('0')
                    })
                    displayed_route_ids.add(rid)
                    used_tids.add(c["t_id"])
                    continue

                if rid in seen: continue
                r_info   = ROUTES.get(rid, {})
                headsign = TRIPS[c["t_id"]].get("trip_headsign", "Local")
                dest, via = split_headsign(headsign)
                dt       = datetime.datetime.fromtimestamp(c["target"], tz=LA_TZ)
                eta_txt  = "BOARDING" if c["eta_s"] <= 90 else (f"{int(c['eta_s']/60)} Min" if c['eta_s'] <= 300 else f"{dt.strftime('%I:%M %p').lstrip('0')}")
                if is_tomorrow: eta_txt = f"Tomorrow {dt.strftime('%I:%M %p').lstrip('0')}"
                bay      = BAY_LOOKUP.get(c["t_id"], "")
                results.append({
                    "route": r_info.get("route_short_name", rid), "color": f"#{r_info.get('route_color','000')}",
                    "text_color": f"#{r_info.get('route_text_color','fff')}", "destination": dest, "via": via,
                    "eta": eta_txt, "eta_seconds": c["eta_s"], "canceled": False, "bay": bay
                })
                seen.add(rid); displayed_route_ids.add(rid)
                used_tids.add(c["t_id"])

            # When few routes are running (e.g. Sunday), fill with additional departures
            if len(results) < 6:
                for c in candidates:
                    if c["t_id"] in used_tids or c["canceled"]: continue
                    rid      = c["rid"]
                    r_info   = ROUTES.get(rid, {})
                    headsign = TRIPS[c["t_id"]].get("trip_headsign", "Local")
                    dest, via = split_headsign(headsign)
                    dt       = datetime.datetime.fromtimestamp(c["target"], tz=LA_TZ)
                    eta_txt  = "BOARDING" if c["eta_s"] <= 90 else (f"{int(c['eta_s']/60)} Min" if c['eta_s'] <= 300 else f"{dt.strftime('%I:%M %p').lstrip('0')}")
                    if is_tomorrow: eta_txt = f"Tomorrow {dt.strftime('%I:%M %p').lstrip('0')}"
                    bay      = BAY_LOOKUP.get(c["t_id"], "")
                    results.append({
                        "route": r_info.get("route_short_name", rid), "color": f"#{r_info.get('route_color','000')}",
                        "text_color": f"#{r_info.get('route_text_color','fff')}", "destination": dest, "via": via,
                        "eta": eta_txt, "eta_seconds": c["eta_s"], "canceled": False, "bay": bay
                    })
                    used_tids.add(c["t_id"])
                    displayed_route_ids.add(rid)

            return results

        buses = process_bus_list(active_svcs, curr_sec - 60)
        if not buses:
            tomorrow_svcs = get_active_services(now + datetime.timedelta(days=1))
            buses = process_bus_list(tomorrow_svcs, 0, True)

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
                    "route":       rid,
                    "color":       f"#{ROUTES[rid].get('route_color','000000')}",
                    "text_color":  f"#{ROUTES[rid].get('route_text_color','ffffff')}",
                    "destination": f_targets[rid]["name"],
                    "status":      "ON TIME",
                    "time_str":    dt.strftime("%I:%M %p").lstrip("0"),
                    "eta_seconds": target - curr_posix
                })

    for rid, t in f_targets.items():
        if not t["found"]:
            ferries.append({
                "route": rid, "color": "#95a5a6", "text_color": "#ffffff",
                "destination": t["name"], "status": "COMPLETED",
                "time_str": "NO MORE SAILINGS", "eta_seconds": 99999
            })

    # ALERT LOGIC
    ALERT_EFFECTS_SHOW = {1, 3}
    for e in raw_alerts:
        if e.HasField('alert'):
            try:
                alert = e.alert
                if alert.effect not in ALERT_EFFECTS_SHOW: continue
                if alert.active_period:
                    still_active = False
                    for ap in alert.active_period:
                        if not ap.end or ap.end > curr_posix:
                            still_active = True
                            break
                    if not still_active: continue
                msg = alert.header_text.translation[0].text.replace('\n', ' ').strip()
                if not msg: continue
                is_system_wide = False
                matches_displayed_route = False
                if not alert.informed_entity: is_system_wide = True
                else:
                    for ie in alert.informed_entity:
                        if not ie.HasField('route_id'): is_system_wide = True
                        elif ie.route_id in displayed_route_ids: matches_displayed_route = True
                if (is_system_wide or matches_displayed_route) and msg not in alerts:
                    alerts.append(msg)
            except: pass

    return {
        "buses":   sorted(buses,   key=lambda x: x["eta_seconds"]),
        "ferries": sorted(ferries, key=lambda x: x["eta_seconds"]),
        "alerts":  alerts,
        "weather": weather_cache["data"],
        "sports":  sports_cache["data"]
    }

def background_refresh():
    """Runs in a background thread every 15 seconds to refresh all data."""
    while True:
        try:
            fetch_rt_feeds()
            fetch_weather()
            fetch_sports()
            board_cache["data"] = build_board_data(all_trips=False)
            board_cache["test_data"] = build_board_data(all_trips=True)
        except Exception as e:
            log_error(f"Background refresh error: {e}")
        time.sleep(15)

# Start background refresh thread
refresh_thread = threading.Thread(target=background_refresh, daemon=True)
refresh_thread.start()

# --- API ROUTES ---
@app.route('/api/data')
def get_board_data():
    if board_cache["data"]:
        return jsonify(board_cache["data"])
    # First request before background thread has run
    return jsonify(build_board_data(all_trips=False))

@app.route('/')
def index(): return render_template('index.html')

@app.route('/test')
def test_page():
    return render_template('test.html')

@app.route('/api/test-data')
def get_test_data():
    data = board_cache["test_data"] or build_board_data(all_trips=True)
    # Inject a fake cancelled trip so the test page always shows one
    buses = data.get("buses", [])
    has_canceled = any(b.get("canceled") for b in buses)
    if not has_canceled and len(buses) >= 2:
        # Mark the second bus as cancelled and keep the next trip visible after it
        orig = buses[1]
        dt = datetime.datetime.fromtimestamp(time.time() + orig["eta_seconds"], tz=LA_TZ)
        buses[1] = dict(orig, eta="CANCELLED", canceled=True, time_str=dt.strftime('%I:%M %p').lstrip('0'))
    return jsonify(data)

@app.route('/health')
def health():
    now = time.time()
    uptime_sec = int(now - APP_START)
    hours, remainder = divmod(uptime_sec, 3600)
    minutes, seconds = divmod(remainder, 60)

    def age_str(ts):
        if not ts: return "never"
        a = int(now - ts)
        if a < 60: return f"{a}s ago"
        if a < 3600: return f"{a//60}m ago"
        return f"{a//3600}h {(a%3600)//60}m ago"

    rt_age = now - rt_cache["last_success"] if rt_cache["last_success"] else None
    rt_status = "ok" if rt_age and rt_age < 60 else ("stale" if rt_age and rt_age < RT_CACHE_MAX_AGE else "down")

    return jsonify({
        "status": "ok" if rt_status != "down" else "degraded",
        "uptime": f"{hours}h {minutes}m {seconds}s",
        "data_sources": {
            "rt_feeds": {
                "status": rt_status,
                "last_success": age_str(rt_cache["last_success"]),
                "last_attempt": age_str(rt_cache["last_fetched"]),
                "cache_max_age": f"{RT_CACHE_MAX_AGE}s",
            },
            "weather": {
                "last_refresh": age_str(data_freshness.get("weather")),
            },
            "sports": {
                "last_refresh": age_str(data_freshness.get("sports")),
            },
        },
        "static_data": {
            "buses_loaded": len(BUS_SCHEDULE),
            "ferries_loaded": len(FERRY_SCHEDULE),
            "routes_loaded": len(ROUTES),
            "bay_assignments": len(BAY_LOOKUP),
        },
        "recent_errors": list(error_log),
    })

if __name__ == '__main__': app.run(host='0.0.0.0', port=5004, debug=False)
