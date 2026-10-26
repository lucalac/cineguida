#!/usr/bin/env python3
"""
CineGuida — Web Application for Movies on Italian TV (Rai & Mediaset Official JSON APIs + TMDB + Tvheadend PVR)
Runs on 192.168.1.237:8090 (systemd service `cineguida.service`).
"""

import argparse
import concurrent.futures
import datetime
import hashlib
import http.server
import json
import os
import re
import signal
import socketserver
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request

# ==============================================================================
# Configuration & Constants
# ==============================================================================

DEFAULT_PORT = 8090
DATA_DIR = os.path.expanduser("~/.cineguida")
DB_PATH = os.path.join(DATA_DIR, "cineguida.db")

TMDB_API_KEY = "a0e3411763fd679ab60c754f76c18dc4"
TVH_HOST = "127.0.0.1" if os.path.exists("/home/pi") else "192.168.1.237"
TVH_PORT = 9981
TVH_USER = "admin"
TVH_PASS = "admin"
TVH_DVR_CONFIG_UUID = "52a53abe244b7a50aa9b6927aad6f330"

USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 14; SM-F966B) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"
)

RAI_CHANNELS = [
    {"slug": "rai-movie",   "name": "Rai Movie",   "lcn": 24,  "group": "Rai", "is_cinema": True,  "color": "#9d174d"},
    {"slug": "rai-4",       "name": "Rai 4",       "lcn": 21,  "group": "Rai", "is_cinema": True,  "color": "#6d28d9"},
    {"slug": "rai-1",       "name": "Rai 1",       "lcn": 1,   "group": "Rai", "is_cinema": False, "color": "#1d4ed8"},
    {"slug": "rai-2",       "name": "Rai 2",       "lcn": 2,   "group": "Rai", "is_cinema": False, "color": "#b91c1c"},
    {"slug": "rai-3",       "name": "Rai 3",       "lcn": 3,   "group": "Rai", "is_cinema": False, "color": "#15803d"},
    {"slug": "rai-5",       "name": "Rai 5",       "lcn": 23,  "group": "Rai", "is_cinema": False, "color": "#0f766e"},
    {"slug": "rai-premium", "name": "Rai Premium", "lcn": 25,  "group": "Rai", "is_cinema": False, "color": "#b45309"},
    {"slug": "rai-storia",  "name": "Rai Storia",  "lcn": 54,  "group": "Rai", "is_cinema": False, "color": "#3f6212"},
    {"slug": "rai-gulp",    "name": "Rai Gulp",    "lcn": 42,  "group": "Rai", "is_cinema": False, "color": "#0369a1"},
    {"slug": "rai-4k",      "name": "Rai 4K",      "lcn": 101, "group": "Rai", "is_cinema": False, "color": "#4338ca"},
]

MEDIASET_CHANNELS = [
    {"callSign": "KI", "name": "Iris",            "lcn": 22, "group": "Mediaset", "is_cinema": True,  "color": "#7e22ce"},
    {"callSign": "B6", "name": "Cine34",          "lcn": 34, "group": "Mediaset", "is_cinema": True,  "color": "#c2410c"},
    {"callSign": "TS", "name": "27 Twentyseven",  "lcn": 27, "group": "Mediaset", "is_cinema": True,  "color": "#0e7490"},
    {"callSign": "LB", "name": "20 Mediaset",     "lcn": 20, "group": "Mediaset", "is_cinema": True,  "color": "#a16207"},
    {"callSign": "R4", "name": "Rete 4",          "lcn": 4,  "group": "Mediaset", "is_cinema": False, "color": "#d97706"},
    {"callSign": "C5", "name": "Canale 5",        "lcn": 5,  "group": "Mediaset", "is_cinema": False, "color": "#0284c7"},
    {"callSign": "I1", "name": "Italia 1",        "lcn": 6,  "group": "Mediaset", "is_cinema": False, "color": "#2563eb"},
    {"callSign": "KA", "name": "La5",             "lcn": 30, "group": "Mediaset", "is_cinema": False, "color": "#db2777"},
    {"callSign": "LT", "name": "TopCrime",        "lcn": 39, "group": "Mediaset", "is_cinema": False, "color": "#475569"},
    {"callSign": "I2", "name": "Italia 2",        "lcn": 49, "group": "Mediaset", "is_cinema": False, "color": "#dc2626"},
]

DAYS_AHEAD = 6  # Today + 5 future days

# ==============================================================================
# SQLite Storage & Enrichment Cache
# ==============================================================================

class MovieStore:
    def __init__(self, db_path: str):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self.lock = threading.Lock()
        self.sync_in_progress = False
        self.last_sync_ts = 0
        self.last_sync_added = 0
        self._init_db()

    def _conn(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self.lock, self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS movies (
                    uid TEXT PRIMARY KEY,
                    source TEXT,
                    channel TEXT,
                    channel_group TEXT,
                    channel_color TEXT,
                    lcn INTEGER,
                    is_cinema_ch INTEGER,
                    title TEXT,
                    clean_title TEXT,
                    start_ts INTEGER,
                    end_ts INTEGER,
                    date_str TEXT,
                    time_str TEXT,
                    duration_min INTEGER,
                    year TEXT,
                    country TEXT,
                    genre TEXT,
                    director TEXT,
                    cast_list TEXT,
                    description TEXT,
                    poster_url TEXT,
                    backdrop_url TEXT,
                    rating TEXT,
                    rai_program_path TEXT,
                    first_seen_ts INTEGER,
                    sync_batch INTEGER,
                    is_newly_added INTEGER DEFAULT 0,
                    recorded INTEGER DEFAULT 0
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS meta_cache (
                    cache_key TEXT PRIMARY KEY,
                    year TEXT,
                    country TEXT,
                    genre TEXT,
                    director TEXT,
                    cast_list TEXT,
                    description TEXT,
                    poster_url TEXT,
                    backdrop_url TEXT,
                    rating TEXT,
                    updated_ts INTEGER
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS app_state (
                    k TEXT PRIMARY KEY,
                    v TEXT
                )
            """)
            conn.commit()

    def get_state(self, key: str, default: str = "") -> str:
        with self._conn() as conn:
            row = conn.execute("SELECT v FROM app_state WHERE k = ?", (key,)).fetchone()
            return row["v"] if row else default

    def set_state(self, key: str, val: str):
        with self.lock, self._conn() as conn:
            conn.execute("INSERT OR REPLACE INTO app_state (k, v) VALUES (?, ?)", (key, str(val)))
            conn.commit()


# ==============================================================================
# Title & Description Parsers
# ==============================================================================

def clean_movie_title(raw_title: str) -> str:
    if not raw_title:
        return ""
    t = raw_title.strip()
    t = re.sub(r"^(?:Film\s*[-–:]\s*|FILM\s+)", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s*\((?:19|20)\d{2}\)\s*$", "", t)
    t = re.sub(r"\s*-\s*1\^?\s*TV.*$", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s*-\s*Prima\s+Visione.*$", "", t, flags=re.IGNORECASE)
    return t.strip()


def parse_credits_from_text(text: str):
    """Extract genre, director, cast, country, year, and clean plot from Italian TV EPG text."""
    if not text:
        return {}, ""
    info = {}

    m_gen = re.match(r"^\s*\[([^\]]+)\]\s*(?:-\s*)?", text)
    if m_gen:
        info["genre"] = m_gen.group(1).strip().title()

    m_dir = re.search(
        r"Regia(?:\s+di|:)\s+([^;\n]+?)(?=\.\s+(?:Con\b|VISIONE\b|QUESTO\b|[A-Z]{2,})|;\s*[A-Z]{2,3}\b|,\s*(?:ITA|USA|FRA|GBR|GER|ESP)\b|\n|$)",
        text,
        flags=re.IGNORECASE,
    )
    if m_dir:
        d = m_dir.group(1).strip(" .,;-")
        if len(d) < 80:
            info["director"] = d

    m_cast = re.search(r"(?:^|[.;\n-]\s*|\b)Con\s+([A-ZÀ-Ú][^;\n]+?)(?=\.\s*Regia|\;\s*[A-Z]{2,3}\s+\d{4}|\n|$)", text)
    if m_cast:
        c = m_cast.group(1).strip(" .;,-")
        if len(c) < 180 and "consigliata" not in c.lower():
            info["cast_list"] = c

    m_cy = re.search(r"\b([A-Z]{2,3}(?:/[A-Z]{2,3})?)\s+((?:19[2-9]\d|20[0-2]\d))\b", text)
    if m_cy:
        info["country"] = m_cy.group(1).strip()
        info["year"] = m_cy.group(2).strip()
    else:
        m_y = re.search(r"\b(19[3-9]\d|20[0-2]\d)\b", text)
        if m_y:
            info["year"] = m_y.group(1)

    lines = []
    for line in text.splitlines():
        l_str = line.strip()
        if not l_str:
            continue
        if any(skip in l_str.upper() for skip in (
            "VISIONE CONSIGLIATA", "VISIONE ADATTA", "QUESTO PROGRAMMA", "PUO' NUOCERE"
        )):
            continue
        lines.append(l_str)
    clean_desc = " ".join(lines)
    clean_desc = re.sub(r"^\s*\[[^\]]+\]\s*-\s*", "", clean_desc)
    return info, clean_desc.strip()


# ==============================================================================
# Rai & Mediaset Official JSON Fetchers
# ==============================================================================

def http_get_json(url: str, timeout: float = 7.0):
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def is_rai_event_movie(ev: dict, ch_meta: dict) -> bool:
    """Determine if a RaiPlay schedule event is a movie."""
    title = (ev.get("name") or "").strip()
    if not title:
        return False
    title_low = title.lower()
    if any(k in title_low for k in (
        "telegiornale", "tg1", "tg2", "tg3", "tgr", "meteo", "trailer",
        "che tempo fa", "intervallo", "sigla", "spazio", "rubrica",
        "mezzogiorno", "porta a porta", "domenica in", "vita in diretta",
        "chi l'ha visto", "report", "presadiretta", "ulisse", "quark",
        "passato e presente", "il cavallo e la torre", "blob", "agiskea",
        "elisir", "uno mattina", "unomattina", "linea verde", "linea blu",
        "linea bianca", "bellitalia", "tv talk", "reazione a catena", "l'eredità",
        "affari tuoi", "cinque minuti", "sottovoce", "applausi", "milleeunlibro"
    )):
        return False

    dfp = ev.get("dfp") or {}
    typology = (dfp.get("escaped_typology_name") or "").strip().lower()
    if typology in (
        "programmitv", "fiction", "serietv", "documentari", "informazione",
        "sport", "musica", "bambini", "teatro", "original"
    ):
        return False

    desc = (ev.get("description") or "")
    desc_low = desc.lower()

    dur_min = 0
    dur_str = ev.get("duration_in_minutes") or ""
    m_dur = re.search(r"(\d+)", dur_str)
    if m_dur:
        dur_min = int(m_dur.group(1))
    elif ev.get("duration"):
        parts = str(ev["duration"]).split(":")
        if len(parts) == 3 and all(p.isdigit() for p in parts):
            dur_min = int(parts[0]) * 60 + int(parts[1])

    if typology == "film" or "film" in typology:
        return dur_min == 0 or dur_min >= 45

    if ch_meta["slug"] == "rai-movie" and dur_min >= 55:
        return True

    if dur_min >= 70 and not typology:
        if any(k in desc_low for k in ("film ", "pellicola", "lungometraggio")):
            if not ev.get("episode") and "puntata" not in title_low and "stagione" not in desc_low:
                return True

    return False


def fetch_rai_day(ch_meta: dict, day_dt: datetime.date):
    """Fetch a single day's JSON schedule for a Rai channel."""
    ds = day_dt.strftime("%d-%m-%Y")
    url = f"https://www.raiplay.it/palinsesto/app/{ch_meta['slug']}/{ds}.json"
    results = []
    try:
        data = http_get_json(url, timeout=7.0)
    except Exception:
        return results

    for ev in data.get("events", []):
        if not is_rai_event_movie(ev, ch_meta):
            continue

        raw_title = (ev.get("name") or "").strip()
        c_title = clean_movie_title(raw_title)
        hour_str = (ev.get("hour") or "00:00").strip()
        try:
            hh, mm = [int(x) for x in hour_str.split(":")[:2]]
        except Exception:
            hh, mm = 0, 0

        start_dt = datetime.datetime(day_dt.year, day_dt.month, day_dt.day, hh, mm)
        start_ts = int(start_dt.timestamp())

        dur_min = 95
        m_dur = re.search(r"(\d+)", ev.get("duration_in_minutes") or "")
        if m_dur:
            dur_min = int(m_dur.group(1))
        elif ev.get("duration"):
            parts = str(ev["duration"]).split(":")
            if len(parts) == 3 and all(p.isdigit() for p in parts):
                dur_min = int(parts[0]) * 60 + int(parts[1])

        end_ts = start_ts + dur_min * 60
        end_dt = datetime.datetime.fromtimestamp(end_ts)
        time_str = f"{start_dt.strftime('%H:%M')} - {end_dt.strftime('%H:%M')}"

        desc_raw = (ev.get("description") or "").strip()
        parsed, clean_desc = parse_credits_from_text(desc_raw)

        dfp = ev.get("dfp") or {}
        genre = dfp.get("escaped_genre_name") or parsed.get("genre") or "Film"
        genre = re.sub(r"([a-z])([A-Z])", r"\1 / \2", genre)

        imgs = ev.get("images") or {}
        poster = ""
        backdrop = ""
        for k in ("portrait", "portrait43", "portrait_logo"):
            v = imgs.get(k) or ""
            if v and "placeholder" not in v:
                poster = "https://www.raiplay.it" + v if v.startswith("/") else v
                break
        raw_img = ev.get("image") or imgs.get("landscape") or ""
        if raw_img and "placeholder" not in raw_img:
            backdrop = "https://www.raiplay.it" + raw_img if raw_img.startswith("/") else raw_img
            if not poster:
                poster = backdrop

        prog_obj = ev.get("program") or {}
        rai_prog_path = prog_obj.get("path_id") or ""

        uid_raw = f"rai|{ch_meta['slug']}|{day_dt.isoformat()}|{hour_str}|{c_title.lower()}"
        uid = hashlib.md5(uid_raw.encode("utf-8")).hexdigest()[:16]

        results.append({
            "uid": uid,
            "source": "RaiPlay JSON",
            "channel": ch_meta["name"],
            "channel_group": "Rai",
            "channel_color": ch_meta["color"],
            "lcn": ch_meta["lcn"],
            "is_cinema_ch": 1 if ch_meta["is_cinema"] else 0,
            "title": c_title,
            "clean_title": c_title.lower(),
            "start_ts": start_ts,
            "end_ts": end_ts,
            "date_str": day_dt.isoformat(),
            "time_str": time_str,
            "duration_min": dur_min,
            "year": parsed.get("year", ""),
            "country": parsed.get("country", ""),
            "genre": genre,
            "director": parsed.get("director", ""),
            "cast_list": parsed.get("cast_list", ""),
            "description": clean_desc or desc_raw,
            "poster_url": poster,
            "backdrop_url": backdrop,
            "rating": "",
            "rai_program_path": rai_prog_path,
        })
    return results


def is_mediaset_listing_movie(listing: dict, ch_meta: dict) -> bool:
    prog = listing.get("program") or {}
    ptype = (prog.get("programType") or "").lower()
    subtype = (prog.get("mediasetprogram$subType") or "").lower()
    edtype = (prog.get("mediasetprogram$editorialType") or "").lower()
    subbrand = (prog.get("mediasetprogram$subBrandDescription") or "").lower()
    skygenre = (prog.get("mediasetprogram$skyGenre") or "").lower()

    if any(v == "movie" for v in (ptype, subtype, edtype)) or "film" in (subbrand, skygenre):
        return True

    for t in prog.get("tags") or []:
        if (t.get("scheme") in ("category", "typology")) and (t.get("title") or "").lower() == "film":
            return True

    return False


def fetch_mediaset_day(ch_meta: dict, day_dt: datetime.date):
    """Fetch a single day's JSON schedule for a Mediaset channel from Mediaset Infinity API."""
    start_day = datetime.datetime(day_dt.year, day_dt.month, day_dt.day, 0, 0, 0)
    t0 = int(start_day.timestamp() * 1000)
    t1 = t0 + 86400 * 1000
    url = (
        f"https://api-ott-prod-fe.mediaset.net/PROD/play/feed/allListingFeedEpg/v2.0"
        f"?byListingTime={t0}~{t1}&byCallSign={ch_meta['callSign']}"
    )
    results = []
    try:
        data = http_get_json(url, timeout=8.0)
    except Exception:
        return results

    entries = (data.get("response") or {}).get("entries") or []
    if not entries:
        return results

    for listing in entries[0].get("listings") or []:
        if not is_mediaset_listing_movie(listing, ch_meta):
            continue

        prog = listing.get("program") or {}
        raw_title = (
            listing.get("mediasetlisting$epgTitle")
            or prog.get("mediasetprogram$titleUI")
            or prog.get("title")
            or ""
        ).strip()
        if not raw_title:
            continue

        c_title = clean_movie_title(raw_title)
        start_ms = listing.get("startTime") or 0
        end_ms = listing.get("endTime") or 0
        if not start_ms:
            continue

        start_ts = int(start_ms // 1000)
        end_ts = int(end_ms // 1000) if end_ms else (start_ts + 6000)
        dur_min = max(1, (end_ts - start_ts) // 60)
        if dur_min < 45:
            continue

        start_dt = datetime.datetime.fromtimestamp(start_ts)
        end_dt = datetime.datetime.fromtimestamp(end_ts)
        date_str = start_dt.date().isoformat()
        time_str = f"{start_dt.strftime('%H:%M')} - {end_dt.strftime('%H:%M')}"

        desc_listing = (listing.get("description") or "").strip()
        long_desc = (
            prog.get("longDescription")
            or prog.get("mediasetprogram$longDescriptionUI")
            or prog.get("description")
            or desc_listing
        ).strip()

        parsed_l, clean_l = parse_credits_from_text(desc_listing)
        parsed_p, clean_p = parse_credits_from_text(long_desc)

        year = parsed_l.get("year") or parsed_p.get("year") or ""
        countries = prog.get("mediasetprogram$countryProduction") or []
        country = ", ".join(countries) if countries else (parsed_l.get("country") or parsed_p.get("country") or "")

        genres = prog.get("mediasetprogram$genres") or []
        genre = ", ".join(genres) if genres else (
            prog.get("mediasetprogram$skySubGenre") or parsed_l.get("genre") or "Film"
        )

        director = parsed_l.get("director") or parsed_p.get("director") or ""
        if not director:
            season_t = prog.get("mediasetprogram$tvLinearSeasonTitle") or ""
            m_di = re.search(r"\(di\s+([^)]+)\)", season_t, flags=re.IGNORECASE)
            if m_di:
                director = m_di.group(1).strip().title()

        cast_list = parsed_l.get("cast_list") or parsed_p.get("cast_list") or ""

        final_desc = clean_p if len(clean_p) >= len(clean_l) else clean_l
        final_desc = re.sub(r"^[A-Z]{2,3}(?:/[A-Z]{2,3})?\s+(?:19|20)\d{2}\s*-\s*", "", final_desc).strip()

        thumbs = prog.get("thumbnails") or {}
        poster = ""
        backdrop = ""
        for pk in (
            "img_s_master_boxart-1080x1440",
            "image_vertical-264x396",
            "image_vertical-192x288",
            "image_vertical-168x252",
        ):
            if pk in thumbs and thumbs[pk].get("url"):
                poster = thumbs[pk]["url"]
                break
        for bk in (
            "image_keyframe_poster-652x367",
            "image_keyframe_poster-1280x720",
            "img_s_master_16_9-1920x1080",
            "image_horizontal_cover-704x396",
            "brand_cover-768x340",
        ):
            if bk in thumbs and thumbs[bk].get("url"):
                backdrop = thumbs[bk]["url"]
                break
        if not poster and backdrop:
            poster = backdrop

        uid_raw = f"ms|{ch_meta['callSign']}|{start_ts}|{c_title.lower()}"
        uid = hashlib.md5(uid_raw.encode("utf-8")).hexdigest()[:16]

        results.append({
            "uid": uid,
            "source": "Mediaset JSON",
            "channel": ch_meta["name"],
            "channel_group": "Mediaset",
            "channel_color": ch_meta["color"],
            "lcn": ch_meta["lcn"],
            "is_cinema_ch": 1 if ch_meta["is_cinema"] else 0,
            "title": c_title,
            "clean_title": c_title.lower(),
            "start_ts": start_ts,
            "end_ts": end_ts,
            "date_str": date_str,
            "time_str": time_str,
            "duration_min": dur_min,
            "year": year,
            "country": country,
            "genre": genre,
            "director": director,
            "cast_list": cast_list,
            "description": final_desc or desc_listing,
            "poster_url": poster,
            "backdrop_url": backdrop,
            "rating": "",
            "rai_program_path": "",
        })
    return results


# ==============================================================================
# RaiPlay Program JSON + TMDB Enrichment
# ==============================================================================

def enrich_movie_metadata(movie: dict, store: MovieStore) -> dict:
    cache_key = re.sub(r"[^a-z0-9àèéìòù]+", "_", movie["clean_title"]).strip("_")
    if not cache_key:
        return movie

    with store._conn() as conn:
        cached = conn.execute("SELECT * FROM meta_cache WHERE cache_key = ?", (cache_key,)).fetchone()

    if cached:
        if not movie["year"] and cached["year"]:
            movie["year"] = cached["year"]
        if not movie["country"] and cached["country"]:
            movie["country"] = cached["country"]
        if (not movie["genre"] or movie["genre"] == "Film") and cached["genre"]:
            movie["genre"] = cached["genre"]
        if cached["director"] and (not movie["director"] or len(movie["director"]) < len(cached["director"])):
            movie["director"] = cached["director"]
        if (not movie["cast_list"] or len(movie["cast_list"]) < 25) and cached["cast_list"]:
            movie["cast_list"] = cached["cast_list"]
        if (not movie["description"] or len(movie["description"]) < 40) and cached["description"]:
            movie["description"] = cached["description"]
        if cached["poster_url"] and (not movie["poster_url"] or "1152" in movie["poster_url"]):
            movie["poster_url"] = cached["poster_url"]
        if not movie["backdrop_url"] and cached["backdrop_url"]:
            movie["backdrop_url"] = cached["backdrop_url"]
        if not movie["rating"] and cached["rating"]:
            movie["rating"] = cached["rating"]
        return movie

    rai_path = movie.get("rai_program_path") or ""
    if rai_path and rai_path.endswith(".json"):
        try:
            r_url = "https://www.raiplay.it" + rai_path if rai_path.startswith("/") else rai_path
            r_data = http_get_json(r_url, timeout=5.0)
            p_info = r_data.get("program_info") or {}
            if p_info.get("year") and not movie["year"]:
                movie["year"] = str(p_info["year"]).strip()
            if p_info.get("direction") and not movie["director"]:
                movie["director"] = str(p_info["direction"]).strip()
            if p_info.get("actors") and not movie["cast_list"]:
                movie["cast_list"] = str(p_info["actors"]).strip()
            if p_info.get("country") and not movie["country"]:
                movie["country"] = str(p_info["country"]).strip()
            if p_info.get("description") and len(p_info["description"]) > len(movie["description"]):
                movie["description"] = p_info["description"].strip()
            imgs = p_info.get("images") or r_data.get("images") or {}
            for pk in ("portrait", "portrait43", "portrait_logo"):
                if imgs.get(pk) and "placeholder" not in imgs[pk]:
                    v = imgs[pk]
                    movie["poster_url"] = "https://www.raiplay.it" + v if v.startswith("/") else v
                    break
        except Exception:
            pass

    if TMDB_API_KEY and (not movie["cast_list"] or not movie["year"] or not movie["director"] or not movie["rating"]):
        try:
            q = urllib.parse.quote(movie["title"])
            y_param = f"&year={movie['year']}" if (movie["year"] and movie["year"].isdigit()) else ""
            s_url = f"https://api.themoviedb.org/3/search/movie?api_key={TMDB_API_KEY}&language=it-IT&query={q}{y_param}"
            s_data = http_get_json(s_url, timeout=5.0)
            results = s_data.get("results") or []
            if not results and y_param:
                s_url2 = f"https://api.themoviedb.org/3/search/movie?api_key={TMDB_API_KEY}&language=it-IT&query={q}"
                results = (http_get_json(s_url2, timeout=5.0).get("results") or [])

            if results:
                best = results[0]
                tmdb_id = best.get("id")
                rel_date = best.get("release_date") or ""
                if rel_date[:4].isdigit() and not movie["year"]:
                    movie["year"] = rel_date[:4]
                vote = best.get("vote_average")
                if vote and float(vote) > 0:
                    movie["rating"] = f"{float(vote):.1f}"
                if best.get("overview") and len(best["overview"]) > len(movie["description"]):
                    movie["description"] = best["overview"].strip()
                if best.get("poster_path") and (not movie["poster_url"] or "2048x1152" in movie["poster_url"]):
                    movie["poster_url"] = f"https://image.tmdb.org/t/p/w500{best['poster_path']}"
                if best.get("backdrop_path") and not movie["backdrop_url"]:
                    movie["backdrop_url"] = f"https://image.tmdb.org/t/p/w780{best['backdrop_path']}"

                if tmdb_id and (not movie["cast_list"] or not movie["director"] or not movie["country"] or len(movie["director"]) < 5 or re.search(r"\b[A-Z]\.", movie["director"])):
                    c_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={TMDB_API_KEY}&language=it-IT&append_to_response=credits"
                    c_data = http_get_json(c_url, timeout=5.0)
                    credits = c_data.get("credits") or {}
                    cast_arr = [c.get("name") for c in (credits.get("cast") or [])[:6] if c.get("name")]
                    if cast_arr and (not movie["cast_list"] or len(movie["cast_list"]) < 25):
                        movie["cast_list"] = ", ".join(cast_arr)
                    crew_arr = credits.get("crew") or []
                    dirs = [c.get("name") for c in crew_arr if c.get("job") == "Director" and c.get("name")]
                    if dirs and (not movie["director"] or len(movie["director"]) < 5 or re.search(r"\b[A-Z]\.", movie["director"])):
                        movie["director"] = ", ".join(dirs[:2])
                    prod_c = [c.get("iso_3166_1") for c in (c_data.get("production_countries") or []) if c.get("iso_3166_1")]
                    if prod_c and not movie["country"]:
                        movie["country"] = "/".join(prod_c[:2])
                    genres_t = [g.get("name") for g in (c_data.get("genres") or []) if g.get("name")]
                    if genres_t and (not movie["genre"] or movie["genre"] == "Film"):
                        movie["genre"] = ", ".join(genres_t[:2])
        except Exception:
            pass

    with store.lock, store._conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO meta_cache (
                cache_key, year, country, genre, director, cast_list,
                description, poster_url, backdrop_url, rating, updated_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            cache_key,
            movie["year"],
            movie["country"],
            movie["genre"],
            movie["director"],
            movie["cast_list"],
            movie["description"],
            movie["poster_url"],
            movie["backdrop_url"],
            movie["rating"],
            int(time.time()),
        ))
        conn.commit()

    return movie


# ==============================================================================
# Sync Engine (Rai + Mediaset JSONs -> SQLite)
# ==============================================================================

def sync_all_schedules(store: MovieStore, enrich_limit: int = 80):
    if store.sync_in_progress:
        return
    store.sync_in_progress = True
    try:
        now_ts = int(time.time())
        today = datetime.date.today()
        days = [today + datetime.timedelta(days=i) for i in range(DAYS_AHEAD)]

        tasks = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=14) as pool:
            for d in days:
                for ch in RAI_CHANNELS:
                    tasks.append(pool.submit(fetch_rai_day, ch, d))
                for ch in MEDIASET_CHANNELS:
                    tasks.append(pool.submit(fetch_mediaset_day, ch, d))

            fetched_movies = []
            for fut in concurrent.futures.as_completed(tasks):
                try:
                    fetched_movies.extend(fut.result())
                except Exception:
                    pass

        dedup = {}
        for m in fetched_movies:
            key = (m["channel"], m["start_ts"])
            if key not in dedup:
                dedup[key] = m
        all_movies = sorted(dedup.values(), key=lambda x: x["start_ts"])

        with store._conn() as conn:
            existing_rows = conn.execute("SELECT uid, first_seen_ts, sync_batch, is_newly_added, recorded FROM movies").fetchall()
        existing_map = {r["uid"]: dict(r) for r in existing_rows}
        is_first_ever_sync = len(existing_map) == 0

        prev_batch = int(store.get_state("sync_batch", "0") or "0")
        curr_batch = prev_batch + 1
        new_count = 0

        to_enrich = []
        for m in all_movies:
            needs = (
                not m["cast_list"]
                or not m["year"]
                or not m["director"]
                or len(m["director"]) < 5
                or bool(re.search(r"\b[A-Z]\.", m["director"]))
                or not m["poster_url"]
            )
            if needs:
                to_enrich.append(m)

        today_str = today.isoformat()
        to_enrich.sort(key=lambda x: (
            0 if x["date_str"] == today_str else 1,
            0 if x["uid"] not in existing_map else 1,
            0 if x["is_cinema_ch"] else 1,
            x["start_ts"]
        ))

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            futures = [pool.submit(enrich_movie_metadata, m, store) for m in to_enrich[:enrich_limit]]
            concurrent.futures.wait(futures, timeout=25)

        cutoff_first_sync_date = (today + datetime.timedelta(days=DAYS_AHEAD - 2)).isoformat()

        with store.lock, store._conn() as conn:
            yesterday_ts = int((datetime.datetime.combine(today, datetime.time.min) - datetime.timedelta(hours=6)).timestamp())
            conn.execute("DELETE FROM movies WHERE end_ts < ?", (yesterday_ts,))

            newly_discovered_uids = [m["uid"] for m in all_movies if m["uid"] not in existing_map]
            if not is_first_ever_sync and len(newly_discovered_uids) > 0:
                conn.execute("UPDATE movies SET is_newly_added = 0 WHERE first_seen_ts < ?", (now_ts - 64800,))

            for m in all_movies:
                uid = m["uid"]
                if uid in existing_map:
                    first_seen = existing_map[uid]["first_seen_ts"]
                    batch_no = existing_map[uid].get("sync_batch", curr_batch)
                    is_new = existing_map[uid]["is_newly_added"]
                    rec = existing_map[uid]["recorded"]
                else:
                    first_seen = now_ts
                    batch_no = curr_batch
                    rec = 0
                    if is_first_ever_sync:
                        is_new = 1 if (m["date_str"] >= cutoff_first_sync_date) else 0
                    else:
                        is_new = 1
                    if is_new:
                        new_count += 1

                conn.execute("""
                    INSERT OR REPLACE INTO movies (
                        uid, source, channel, channel_group, channel_color, lcn, is_cinema_ch,
                        title, clean_title, start_ts, end_ts, date_str, time_str, duration_min,
                        year, country, genre, director, cast_list, description,
                        poster_url, backdrop_url, rating, rai_program_path,
                        first_seen_ts, sync_batch, is_newly_added, recorded
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    uid, m["source"], m["channel"], m["channel_group"], m["channel_color"],
                    m["lcn"], m["is_cinema_ch"], m["title"], m["clean_title"],
                    m["start_ts"], m["end_ts"], m["date_str"], m["time_str"], m["duration_min"],
                    m["year"], m["country"], m["genre"], m["director"], m["cast_list"],
                    m["description"], m["poster_url"], m["backdrop_url"], m["rating"],
                    m["rai_program_path"], first_seen, batch_no, is_new, rec
                ))
            conn.commit()

        store.set_state("sync_batch", str(curr_batch))
        store.set_state("last_sync_ts", str(now_ts))
        store.last_sync_ts = now_ts
        store.last_sync_added = new_count
    finally:
        store.sync_in_progress = False


def periodic_sync_worker(store: MovieStore):
    """Background loop that keeps schedules and TMDB metadata fresh every 2 hours."""
    while True:
        time.sleep(7200)
        try:
            sync_all_schedules(store, enrich_limit=150)
        except Exception:
            pass


# ==============================================================================
# Tvheadend PVR Recording Helper (127.0.0.1:9981 / 192.168.1.237:9981)
# ==============================================================================

def schedule_tvheadend_recording(movie: dict) -> tuple[bool, str]:
    pwd_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    pwd_mgr.add_password(None, f"http://{TVH_HOST}:{TVH_PORT}/", TVH_USER, TVH_PASS)
    auth_handler = urllib.request.HTTPDigestAuthHandler(pwd_mgr)
    opener = urllib.request.build_opener(auth_handler)

    ch_target = movie["channel"].lower().replace(" ", "")
    start_ts = int(movie["start_ts"])
    end_ts = int(movie["end_ts"])

    try:
        epg_url = f"http://{TVH_HOST}:{TVH_PORT}/api/epg/events/grid?limit=2000"
        with opener.open(epg_url, timeout=6) as r:
            epg_data = json.loads(r.read().decode("utf-8", "replace"))

        matched_event_id = None
        for ev in epg_data.get("entries", []):
            ev_ch = (ev.get("channelName") or "").lower().replace("hd", "").replace(" ", "")
            ev_start = int(ev.get("start") or 0)
            ev_title = (ev.get("title") or "").lower()
            if abs(ev_start - start_ts) <= 900 and (
                ch_target in ev_ch or ev_ch in ch_target or movie["clean_title"][:8] in ev_title
            ):
                matched_event_id = ev.get("eventId")
                break

        if matched_event_id:
            rec_url = (
                f"http://{TVH_HOST}:{TVH_PORT}/api/dvr/entry/create_by_event"
                f"?event_id={matched_event_id}&config_uuid={TVH_DVR_CONFIG_UUID}"
            )
            with opener.open(rec_url, timeout=6) as r:
                r.read()
            return True, f"Registrazione programmata su Tvheadend (Evento #{matched_event_id})"

        ch_url = f"http://{TVH_HOST}:{TVH_PORT}/api/channel/grid?limit=300"
        with opener.open(ch_url, timeout=6) as r:
            ch_data = json.loads(r.read().decode("utf-8", "replace"))

        ch_uuid = None
        for ch in ch_data.get("entries", []):
            cname = (ch.get("name") or "").lower().replace("hd", "").replace(" ", "")
            if cname == ch_target or ch_target in cname or cname in ch_target:
                ch_uuid = ch.get("uuid")
                break

        if not ch_uuid:
            return False, f"Canale '{movie['channel']}' non trovato su Tvheadend."

        conf = {
            "disp_title": movie["title"],
            "disp_extratext": f"{movie['genre']} ({movie['year']}) - Regia: {movie['director']} - Cast: {movie['cast_list']}",
            "channel": ch_uuid,
            "start": start_ts,
            "stop": end_ts,
            "start_extra": 2,
            "stop_extra": 10,
            "config_name": TVH_DVR_CONFIG_UUID,
            "comment": "Programmato da CineGuida WebApp",
        }
        params = urllib.parse.urlencode({"conf": json.dumps(conf)})
        create_url = f"http://{TVH_HOST}:{TVH_PORT}/api/dvr/entry/create?{params}"
        with opener.open(create_url, timeout=6) as r:
            r.read()
        return True, f"Timer di registrazione creato su Tvheadend per {movie['channel']} ({movie['time_str']})"

    except Exception as e:
        return False, f"Errore connessione Tvheadend ({TVH_HOST}): {e}"


# ==============================================================================
# Frontend HTML / CSS / JS Template (Raw String r"""...""")
# ==============================================================================

HTML_PAGE = r"""<!DOCTYPE html>
<html lang="it">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
<meta name="theme-color" content="#0b0f19">
<title>CineGuida TV · Film Rai & Mediaset</title>
<style>
  :root {
    --bg: #0b0f19;
    --bg-card: #131b2e;
    --bg-card-hover: #1a2540;
    --bg-glass: rgba(19, 27, 46, 0.88);
    --border: rgba(255, 255, 255, 0.09);
    --text: #f1f5f9;
    --text-muted: #94a3b8;
    --accent: #3b82f6;
    --accent-glow: rgba(59, 130, 246, 0.35);
    --gold: #f59e0b;
    --green: #10b981;
    --red: #ef4444;
    --purple: #8b5cf6;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; -webkit-tap-highlight-color: transparent; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    min-height: 100vh;
    padding-bottom: 48px;
  }

  .app-header {
    position: sticky;
    top: 0;
    z-index: 100;
    background: rgba(11, 15, 25, 0.94);
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
    border-bottom: 1px solid var(--border);
    padding: 12px 16px 10px;
  }
  .header-top {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    max-width: 1440px;
    margin: 0 auto 10px;
    flex-wrap: wrap;
  }
  .brand {
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .brand-icon {
    width: 38px;
    height: 38px;
    border-radius: 11px;
    background: linear-gradient(135deg, #2563eb, #7c3aed);
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 20px;
    box-shadow: 0 4px 14px rgba(37, 99, 235, 0.45);
  }
  .brand-title {
    font-size: 19px;
    font-weight: 800;
    letter-spacing: -0.3px;
    background: linear-gradient(90deg, #fff, #cbd5e1);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
  }
  .brand-sub {
    font-size: 11px;
    color: var(--text-muted);
    font-weight: 500;
  }

  .header-actions {
    display: flex;
    align-items: center;
    gap: 8px;
    flex: 1;
    justify-content: flex-end;
    min-width: 240px;
  }
  .search-wrap {
    position: relative;
    flex: 1;
    max-width: 360px;
  }
  .search-input {
    width: 100%;
    background: rgba(255, 255, 255, 0.07);
    border: 1px solid var(--border);
    color: #fff;
    padding: 8px 12px 8px 34px;
    border-radius: 20px;
    font-size: 13px;
    outline: none;
    transition: border-color 0.2s, background 0.2s;
  }
  .search-input:focus {
    border-color: var(--accent);
    background: rgba(255, 255, 255, 0.11);
  }
  .search-icon {
    position: absolute;
    left: 11px;
    top: 50%;
    transform: translateY(-50%);
    font-size: 13px;
    color: var(--text-muted);
    pointer-events: none;
  }
  .refresh-btn {
    background: rgba(59, 130, 246, 0.16);
    color: #93c5fd;
    border: 1px solid rgba(59, 130, 246, 0.35);
    padding: 8px 13px;
    border-radius: 20px;
    font-size: 12px;
    font-weight: 700;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    gap: 6px;
    white-space: nowrap;
    transition: all 0.15s;
  }
  .refresh-btn:active { transform: scale(0.95); }
  .refresh-btn.spinning .spin-icon {
    display: inline-block;
    animation: spin 1s linear infinite;
  }
  @keyframes spin { 100% { transform: rotate(360deg); } }

  /* Main Navigation Tabs (Sezioni Principali) */
  .section-tabs {
    display: flex;
    align-items: center;
    gap: 8px;
    max-width: 1440px;
    margin: 0 auto 8px;
    overflow-x: auto;
    scrollbar-width: none;
    padding-bottom: 2px;
  }
  .section-tabs::-webkit-scrollbar { display: none; }
  .sec-tab {
    background: rgba(255, 255, 255, 0.06);
    color: #cbd5e1;
    border: 1px solid var(--border);
    padding: 8px 15px;
    border-radius: 22px;
    font-size: 13px;
    font-weight: 700;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    gap: 7px;
    white-space: nowrap;
    transition: all 0.18s ease;
  }
  .sec-tab:hover { background: rgba(255, 255, 255, 0.1); }
  .sec-tab.active {
    background: linear-gradient(135deg, #2563eb, #1d4ed8);
    color: #fff;
    border-color: #60a5fa;
    box-shadow: 0 4px 14px var(--accent-glow);
  }
  .sec-tab.tab-new.active {
    background: linear-gradient(135deg, #d97706, #b45309);
    border-color: #fbbf24;
    box-shadow: 0 4px 14px rgba(245, 158, 11, 0.35);
  }
  .sec-tab.tab-today.active {
    background: linear-gradient(135deg, #059669, #047857);
    border-color: #34d399;
    box-shadow: 0 4px 14px rgba(16, 185, 129, 0.35);
  }
  .tab-count {
    background: rgba(0, 0, 0, 0.32);
    color: #e2e8f0;
    padding: 1px 7px;
    border-radius: 10px;
    font-size: 11px;
    font-weight: 800;
  }

  /* Sub-filters Bar */
  .filter-bar {
    display: flex;
    align-items: center;
    gap: 6px;
    max-width: 1440px;
    margin: 0 auto;
    overflow-x: auto;
    scrollbar-width: none;
    padding-top: 4px;
  }
  .filter-bar::-webkit-scrollbar { display: none; }
  .filter-chip {
    background: rgba(255, 255, 255, 0.04);
    color: var(--text-muted);
    border: 1px solid rgba(255, 255, 255, 0.08);
    padding: 5px 11px;
    border-radius: 14px;
    font-size: 11.5px;
    font-weight: 600;
    cursor: pointer;
    white-space: nowrap;
    transition: all 0.15s;
  }
  .filter-chip.active {
    background: rgba(59, 130, 246, 0.22);
    color: #93c5fd;
    border-color: #3b82f6;
  }
  .filter-sep {
    width: 1px;
    height: 18px;
    background: var(--border);
    flex-shrink: 0;
    margin: 0 3px;
  }

  .main-container {
    max-width: 1440px;
    margin: 0 auto;
    padding: 16px;
  }
  .section-banner {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 16px;
    padding: 14px 18px;
    border-radius: 16px;
    background: linear-gradient(135deg, rgba(30, 41, 59, 0.8), rgba(15, 23, 42, 0.9));
    border: 1px solid var(--border);
    flex-wrap: wrap;
  }
  .section-banner-title {
    font-size: 17px;
    font-weight: 800;
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .section-banner-desc {
    font-size: 12.5px;
    color: var(--text-muted);
    margin-top: 3px;
  }
  .stats-pill {
    font-size: 11.5px;
    color: #cbd5e1;
    background: rgba(255, 255, 255, 0.06);
    padding: 6px 12px;
    border-radius: 12px;
    border: 1px solid var(--border);
  }

  /* Movie Grid & Cards */
  .movies-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(330px, 1fr));
    gap: 16px;
  }
  .movie-card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 16px;
    overflow: hidden;
    display: flex;
    flex-direction: column;
    transition: transform 0.18s ease, border-color 0.18s ease, box-shadow 0.18s ease;
    cursor: pointer;
    position: relative;
  }
  .movie-card:hover {
    transform: translateY(-2px);
    border-color: rgba(96, 165, 250, 0.4);
    box-shadow: 0 10px 28px rgba(0, 0, 0, 0.55);
  }
  .movie-card.on-air {
    border-color: rgba(16, 185, 129, 0.65);
    box-shadow: 0 0 20px rgba(16, 185, 129, 0.18);
  }
  .movie-card.newly-added {
    border-color: rgba(245, 158, 11, 0.45);
  }

  .card-top {
    display: flex;
    gap: 14px;
    padding: 14px;
  }
  .poster-wrap {
    width: 106px;
    height: 156px;
    border-radius: 10px;
    overflow: hidden;
    flex-shrink: 0;
    background: #0f172a;
    position: relative;
    border: 1px solid rgba(255, 255, 255, 0.1);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.45);
  }
  .poster-img {
    width: 100%;
    height: 100%;
    object-fit: cover;
    display: block;
  }
  .poster-fallback {
    width: 100%;
    height: 100%;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 8px;
    text-align: center;
    background: linear-gradient(145deg, #1e293b, #0f172a);
    color: #94a3b8;
    font-size: 11px;
    font-weight: 700;
  }
  .rating-badge {
    position: absolute;
    bottom: 6px;
    right: 6px;
    background: rgba(0, 0, 0, 0.82);
    color: #fbbf24;
    font-size: 10.5px;
    font-weight: 800;
    padding: 2px 6px;
    border-radius: 6px;
    border: 1px solid rgba(251, 191, 36, 0.4);
    backdrop-filter: blur(4px);
  }
  .new-ribbon {
    position: absolute;
    top: 6px;
    left: 6px;
    background: linear-gradient(135deg, #f59e0b, #d97706);
    color: #fff;
    font-size: 9.5px;
    font-weight: 900;
    padding: 2px 6px;
    border-radius: 5px;
    letter-spacing: 0.4px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.5);
  }

  .card-info {
    flex: 1;
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 5px;
  }
  .card-badges {
    display: flex;
    align-items: center;
    gap: 6px;
    flex-wrap: wrap;
  }
  .ch-badge {
    font-size: 10.5px;
    font-weight: 800;
    padding: 2px 8px;
    border-radius: 6px;
    color: #fff;
    letter-spacing: 0.2px;
  }
  .time-badge {
    font-size: 11px;
    font-weight: 700;
    color: #e2e8f0;
    background: rgba(255, 255, 255, 0.08);
    padding: 2px 7px;
    border-radius: 6px;
  }
  .live-pill {
    background: rgba(16, 185, 129, 0.2);
    color: #34d399;
    border: 1px solid rgba(16, 185, 129, 0.5);
    font-size: 10px;
    font-weight: 800;
    padding: 1px 6px;
    border-radius: 6px;
    animation: pulse 1.8s infinite;
  }
  @keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.55; }
  }

  .movie-title {
    font-size: 15.5px;
    font-weight: 800;
    color: #fff;
    line-height: 1.25;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
  }
  .movie-meta-line {
    font-size: 11.5px;
    font-weight: 600;
    color: #60a5fa;
    display: flex;
    align-items: center;
    gap: 6px;
    flex-wrap: wrap;
  }
  .movie-credits {
    font-size: 11.5px;
    color: #cbd5e1;
    line-height: 1.38;
    margin-top: 2px;
  }
  .movie-credits strong {
    color: #94a3b8;
    font-weight: 600;
  }
  .cast-clamp {
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
  }

  .card-desc {
    padding: 0 14px 12px;
    font-size: 12px;
    color: var(--text-muted);
    line-height: 1.45;
    display: -webkit-box;
    -webkit-line-clamp: 3;
    -webkit-box-orient: vertical;
    overflow: hidden;
    flex: 1;
  }

  .card-footer {
    padding: 10px 14px;
    background: rgba(0, 0, 0, 0.22);
    border-top: 1px solid var(--border);
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
  }
  .source-tag {
    font-size: 10.5px;
    color: #64748b;
    font-weight: 600;
  }
  .rec-btn {
    background: rgba(239, 68, 68, 0.16);
    color: #fca5a5;
    border: 1px solid rgba(239, 68, 68, 0.4);
    padding: 5px 11px;
    border-radius: 14px;
    font-size: 11.5px;
    font-weight: 700;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    gap: 5px;
    transition: all 0.15s;
  }
  .rec-btn:hover {
    background: rgba(239, 68, 68, 0.3);
    color: #fff;
  }
  .rec-btn.recorded {
    background: rgba(16, 185, 129, 0.2);
    color: #6ee7b7;
    border-color: rgba(16, 185, 129, 0.5);
  }

  /* Modal Dettaglio Film */
  .modal-overlay {
    position: fixed;
    inset: 0;
    background: rgba(0, 0, 0, 0.82);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    z-index: 300;
    display: none;
    align-items: center;
    justify-content: center;
    padding: 16px;
  }
  .modal-overlay.open { display: flex; }
  .modal-card {
    background: #131b2e;
    border: 1px solid rgba(255, 255, 255, 0.16);
    border-radius: 20px;
    max-width: 680px;
    width: 100%;
    max-height: 90vh;
    overflow-y: auto;
    box-shadow: 0 24px 60px rgba(0, 0, 0, 0.8);
    position: relative;
  }
  .modal-hero {
    position: relative;
    min-height: 210px;
    background-size: cover;
    background-position: center;
    display: flex;
    align-items: flex-end;
    padding: 20px;
    border-bottom: 1px solid var(--border);
  }
  .modal-hero::before {
    content: "";
    position: absolute;
    inset: 0;
    background: linear-gradient(to top, #131b2e 8%, rgba(19, 27, 46, 0.55) 60%, rgba(19, 27, 46, 0.3) 100%);
  }
  .modal-close {
    position: absolute;
    top: 14px;
    right: 14px;
    width: 36px;
    height: 36px;
    border-radius: 50%;
    background: rgba(0, 0, 0, 0.65);
    color: #fff;
    border: 1px solid rgba(255, 255, 255, 0.25);
    font-size: 20px;
    cursor: pointer;
    z-index: 5;
    display: flex;
    align-items: center;
    justify-content: center;
  }
  .modal-header-content {
    position: relative;
    z-index: 2;
    display: flex;
    gap: 16px;
    align-items: flex-end;
    width: 100%;
  }
  .modal-poster {
    width: 120px;
    height: 176px;
    border-radius: 12px;
    object-fit: cover;
    border: 2px solid rgba(255, 255, 255, 0.2);
    box-shadow: 0 8px 24px rgba(0,0,0,0.65);
    flex-shrink: 0;
    background: #0f172a;
  }
  .modal-body {
    padding: 20px;
    display: flex;
    flex-direction: column;
    gap: 14px;
  }
  .modal-info-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 10px;
    background: rgba(0, 0, 0, 0.25);
    padding: 12px 14px;
    border-radius: 12px;
    border: 1px solid var(--border);
  }
  .info-item-label {
    font-size: 10.5px;
    text-transform: uppercase;
    color: var(--text-muted);
    font-weight: 700;
    letter-spacing: 0.5px;
  }
  .info-item-val {
    font-size: 13px;
    color: #fff;
    font-weight: 600;
    margin-top: 2px;
  }
  .modal-plot {
    font-size: 14px;
    line-height: 1.6;
    color: #e2e8f0;
  }

  .toast {
    position: fixed;
    bottom: 22px;
    left: 50%;
    transform: translateX(-50%) translateY(30px);
    background: rgba(15, 23, 42, 0.95);
    color: #fff;
    padding: 10px 20px;
    border-radius: 24px;
    font-size: 13px;
    font-weight: 700;
    border: 1px solid rgba(59, 130, 246, 0.5);
    box-shadow: 0 10px 30px rgba(0, 0, 0, 0.7);
    z-index: 500;
    opacity: 0;
    pointer-events: none;
    transition: all 0.25s ease;
  }
  .toast.show {
    opacity: 1;
    transform: translateX(-50%) translateY(0);
  }

  .empty-state {
    text-align: center;
    padding: 60px 20px;
    color: var(--text-muted);
    grid-column: 1 / -1;
  }
</style>
</head>
<body>

<header class="app-header">
  <div class="header-top">
    <div class="brand">
      <div class="brand-icon">🎬</div>
      <div>
        <div class="brand-title">CineGuida TV</div>
        <div class="brand-sub" id="syncStatusText">Palinsesti JSON ufficiali RaiPlay & Mediaset Infinity</div>
      </div>
    </div>
    <div class="header-actions">
      <div class="search-wrap">
        <span class="search-icon">🔍</span>
        <input type="text" class="search-input" id="searchInput" placeholder="Cerca titolo, attore, regista, anno..." oninput="onSearchInput()">
      </div>
      <button class="refresh-btn" id="refreshBtn" onclick="triggerRefresh()">
        <span class="spin-icon">🔄</span> Aggiorna JSON
      </button>
    </div>
  </div>

  <!-- Sezioni Principali: 1) Appena Aggiunti, 2) Oggi, 3+) Giorni Futuri -->
  <div class="section-tabs" id="sectionTabs"></div>

  <!-- Filtri Rapidi: Emittente / Fascia Oraria -->
  <div class="filter-bar" id="filterBar">
    <button class="filter-chip active" data-grp="all" onclick="setGroupFilter('all')">📺 Tutti i Canali</button>
    <button class="filter-chip" data-grp="cinema" onclick="setGroupFilter('cinema')">🍿 Solo Canali Cinema (Iris · Cine34 · Rai Movie · 27 · 20)</button>
    <button class="filter-chip" data-grp="Rai" onclick="setGroupFilter('Rai')">🔵 Canali Rai</button>
    <button class="filter-chip" data-grp="Mediaset" onclick="setGroupFilter('Mediaset')">🟠 Canali Mediaset</button>
    <div class="filter-sep"></div>
    <button class="filter-chip active" data-slot="all" onclick="setSlotFilter('all')">🕒 Tutte le ore</button>
    <button class="filter-chip" data-slot="primetime" onclick="setSlotFilter('primetime')">🌙 Prima Serata (20:30-22:30)</button>
    <button class="filter-chip" data-slot="late" onclick="setSlotFilter('late')">🦉 Seconda Serata / Notte</button>
    <button class="filter-chip" data-slot="daytime" onclick="setSlotFilter('daytime')">☀️ Mattina / Pomeriggio</button>
  </div>
</header>

<main class="main-container">
  <div class="section-banner" id="sectionBanner">
    <div>
      <div class="section-banner-title" id="bannerTitle">📅 Film in programmazione</div>
      <div class="section-banner-desc" id="bannerDesc">Caricamento dai JSON Rai e Mediaset in corso...</div>
    </div>
    <div class="stats-pill" id="statsPill">0 film</div>
  </div>

  <div class="movies-grid" id="moviesGrid"></div>
</main>

<!-- Modal Dettaglio Film -->
<div class="modal-overlay" id="movieModal" onclick="closeModal(event)">
  <div class="modal-card" onclick="event.stopPropagation()" id="modalContent"></div>
</div>

<div class="toast" id="toast">Notifica</div>

<script>
var allMovies = [];
var sectionsMeta = [];
var activeSection = 'today';
var activeGroup = 'all';
var activeSlot = 'all';
var searchQuery = '';
var nowTs = Math.floor(Date.now() / 1000);
var serverTodayStr = '';
var serverTomorrowStr = '';

function showToast(msg) {
  var t = document.getElementById('toast');
  t.innerText = msg;
  t.classList.add('show');
  clearTimeout(t._timer);
  t._timer = setTimeout(function() { t.classList.remove('show'); }, 3000);
}

function escapeHtml(s) {
  if (!s) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function handlePosterError(imgEl) {
  var ch = imgEl.getAttribute('data-ch') || 'TV';
  imgEl.style.display = 'none';
  var fb = document.createElement('div');
  fb.className = 'poster-fallback';
  fb.innerHTML = '🎬<br>' + escapeHtml(ch);
  if (imgEl.parentNode) {
    imgEl.parentNode.appendChild(fb);
  }
}

function formatDayLabel(dateStr, todayStr, tomorrowStr) {
  if (dateStr === todayStr) return 'Oggi';
  if (dateStr === tomorrowStr) return 'Domani';
  var parts = dateStr.split('-');
  var dt = new Date(parseInt(parts[0], 10), parseInt(parts[1], 10) - 1, parseInt(parts[2], 10));
  var days = ['Dom', 'Lun', 'Mar', 'Mer', 'Gio', 'Ven', 'Sab'];
  var months = ['Gen', 'Feb', 'Mar', 'Apr', 'Mag', 'Giu', 'Lug', 'Ago', 'Set', 'Ott', 'Nov', 'Dic'];
  return days[dt.getDay()] + ' ' + dt.getDate() + ' ' + months[dt.getMonth()];
}

function loadMovies() {
  fetch('/api/movies')
    .then(function(r) { return r.json(); })
    .then(function(data) {
      allMovies = data.movies || [];
      nowTs = data.now_ts || Math.floor(Date.now() / 1000);
      serverTodayStr = data.today_str || '';
      serverTomorrowStr = data.tomorrow_str || '';

      if (data.last_sync_str) {
        document.getElementById('syncStatusText').innerText =
          'JSON RaiPlay & Mediaset Infinity · Aggiornato alle ' + data.last_sync_str +
          (data.sync_in_progress ? ' (sincronizzazione in corso...)' : '');
      }

      buildSections(serverTodayStr, serverTomorrowStr);
      renderMovies();

      if (data.sync_in_progress) {
        setTimeout(loadMovies, 3500);
      }
    })
    .catch(function(e) {
      console.error(e);
    });
}

function buildSections(todayStr, tomorrowStr) {
  var newCount = allMovies.filter(function(m) { return m.is_newly_added === 1 && m.end_ts >= nowTs; }).length;
  var todayCount = allMovies.filter(function(m) { return m.date_str === todayStr && m.end_ts >= nowTs; }).length;
  var futureAllCount = allMovies.filter(function(m) { return m.date_str > todayStr; }).length;

  var dateSet = {};
  allMovies.forEach(function(m) {
    if (m.date_str > todayStr) {
      dateSet[m.date_str] = (dateSet[m.date_str] || 0) + 1;
    }
  });
  var futureDates = Object.keys(dateSet).sort();

  sectionsMeta = [
    {
      id: 'new',
      label: '🆕 Appena Aggiunti',
      count: newCount,
      cls: 'tab-new',
      title: '🆕 Film Appena Aggiunti alla Programmazione',
      desc: 'Gli ultimi film inseriti nei palinsesti JSON di Rai e Mediaset.'
    },
    {
      id: 'today',
      label: '📅 Oggi in TV',
      count: todayCount,
      cls: 'tab-today',
      title: '📅 Film di Oggi in TV',
      desc: 'Tutti i film in onda oggi sui canali Rai e Mediaset (in diretta e nelle prossime ore).'
    }
  ];

  futureDates.forEach(function(dStr) {
    var lbl = formatDayLabel(dStr, todayStr, tomorrowStr);
    sectionsMeta.push({
      id: 'date:' + dStr,
      label: '🗓️ ' + lbl,
      count: dateSet[dStr],
      cls: '',
      title: '🗓️ Programmazione Film · ' + lbl,
      desc: 'Tutti i film in programma per ' + lbl + ' dai palinsesti ufficiali Rai e Mediaset.'
    });
  });

  sectionsMeta.push({
    id: 'future_all',
    label: '🔮 Tutti i Futuri',
    count: futureAllCount,
    cls: '',
    title: '🔮 Tutta la Programmazione Futura (Prossimi Giorni)',
    desc: 'Panoramica completa di tutti i film nei prossimi giorni su Rai e Mediaset.'
  });

  var tabsEl = document.getElementById('sectionTabs');
  tabsEl.innerHTML = sectionsMeta.map(function(s) {
    var isAct = (s.id === activeSection) ? ' active' : '';
    return '<button class="sec-tab ' + s.cls + isAct + '" data-sec="' + escapeHtml(s.id) + '" onclick="selectSection(this.getAttribute(\'data-sec\'))">' +
      escapeHtml(s.label) + ' <span class="tab-count">' + s.count + '</span></button>';
  }).join('');
}

function selectSection(secId) {
  activeSection = secId;
  var tabs = document.querySelectorAll('.sec-tab');
  tabs.forEach(function(btn) {
    btn.classList.toggle('active', btn.getAttribute('data-sec') === activeSection);
  });
  renderMovies();
}

function setGroupFilter(grp) {
  activeGroup = grp;
  document.querySelectorAll('[data-grp]').forEach(function(b) {
    b.classList.toggle('active', b.getAttribute('data-grp') === grp);
  });
  renderMovies();
}

function setSlotFilter(slot) {
  activeSlot = slot;
  document.querySelectorAll('[data-slot]').forEach(function(b) {
    b.classList.toggle('active', b.getAttribute('data-slot') === slot);
  });
  renderMovies();
}

function onSearchInput() {
  searchQuery = (document.getElementById('searchInput').value || '').trim().toLowerCase();
  renderMovies();
}

function matchesSlot(m, slot) {
  if (slot === 'all') return true;
  var hh = parseInt((m.time_str || '00:00').slice(0, 2), 10);
  var mm = parseInt((m.time_str || '00:00').slice(3, 5), 10);
  var mins = hh * 60 + mm;
  if (slot === 'primetime') {
    return mins >= (20 * 60 + 20) && mins <= (22 * 60 + 40);
  }
  if (slot === 'late') {
    return mins > (22 * 60 + 40) || mins < (6 * 60);
  }
  if (slot === 'daytime') {
    return mins >= (6 * 60) && mins < (20 * 60 + 20);
  }
  return true;
}

function renderMovies() {
  var secObj = sectionsMeta.find(function(s) { return s.id === activeSection; }) || sectionsMeta[0];
  if (secObj) {
    document.getElementById('bannerTitle').innerText = secObj.title;
    document.getElementById('bannerDesc').innerText = secObj.desc;
  }

  var filtered = allMovies.filter(function(m) {
    if (m.end_ts < nowTs) return false;

    if (!searchQuery) {
      if (activeSection === 'new' && m.is_newly_added !== 1) return false;
      if (activeSection === 'today' && m.date_str !== serverTodayStr) return false;
      if (activeSection.indexOf('date:') === 0) {
        var targetDate = activeSection.slice(5);
        if (m.date_str !== targetDate) return false;
      } else if (activeSection === 'future_all' && m.date_str <= serverTodayStr) {
        return false;
      }
    }

    if (activeGroup === 'cinema' && !m.is_cinema_ch) return false;
    if (activeGroup === 'Rai' && m.channel_group !== 'Rai') return false;
    if (activeGroup === 'Mediaset' && m.channel_group !== 'Mediaset') return false;

    if (!matchesSlot(m, activeSlot)) return false;

    if (searchQuery) {
      var hay = (
        (m.title || '') + ' ' +
        (m.director || '') + ' ' +
        (m.cast_list || '') + ' ' +
        (m.year || '') + ' ' +
        (m.genre || '') + ' ' +
        (m.channel || '') + ' ' +
        (m.description || '')
      ).toLowerCase();
      if (hay.indexOf(searchQuery) === -1) return false;
    }
    return true;
  });

  document.getElementById('statsPill').innerText = filtered.length + ' film visualizzati';

  var grid = document.getElementById('moviesGrid');
  if (filtered.length === 0) {
    grid.innerHTML = '<div class="empty-state">Nessun film trovato con i filtri selezionati.</div>';
    return;
  }

  grid.innerHTML = filtered.map(function(m) {
    var isLive = (m.start_ts <= nowTs && m.end_ts > nowTs);
    var cardCls = 'movie-card' + (isLive ? ' on-air' : '') + (m.is_newly_added ? ' newly-added' : '');
    var posterHtml = m.poster_url
      ? '<img class="poster-img" loading="lazy" src="' + escapeHtml(m.poster_url) + '" data-ch="' + escapeHtml(m.channel) + '" alt="' + escapeHtml(m.title) + '" onerror="handlePosterError(this)">'
      : '<div class="poster-fallback">🎬<br>' + escapeHtml(m.channel) + '</div>';

    var ratingHtml = m.rating ? '<span class="rating-badge">⭐ ' + escapeHtml(m.rating) + '</span>' : '';
    var newRibbon = m.is_newly_added ? '<span class="new-ribbon">NUOVO</span>' : '';
    var liveBadge = isLive ? '<span class="live-pill">● IN ONDA</span>' : '';

    var metaParts = [];
    if (m.year) metaParts.push('📅 ' + escapeHtml(m.year));
    if (m.genre) metaParts.push('🎭 ' + escapeHtml(m.genre));
    if (m.duration_min) metaParts.push('⏱ ' + m.duration_min + ' min');
    if (m.country) metaParts.push('🌍 ' + escapeHtml(m.country));

    var dirHtml = m.director
      ? '<div><strong>Regia:</strong> ' + escapeHtml(m.director) + '</div>'
      : '';
    var castHtml = m.cast_list
      ? '<div class="cast-clamp"><strong>Cast:</strong> ' + escapeHtml(m.cast_list) + '</div>'
      : '';

    var recCls = m.recorded ? 'rec-btn recorded' : 'rec-btn';
    var recText = m.recorded ? '✅ In Registrazione' : '🔴 Registra';
    var dateShort = m.date_str.slice(8, 10) + '/' + m.date_str.slice(5, 7);

    return '<div class="' + cardCls + '" data-uid="' + escapeHtml(m.uid) + '" onclick="openMovieModal(this.getAttribute(\'data-uid\'))">' +
      '<div class="card-top">' +
        '<div class="poster-wrap">' + posterHtml + newRibbon + ratingHtml + '</div>' +
        '<div class="card-info">' +
          '<div class="card-badges">' +
            '<span class="ch-badge" style="background:' + escapeHtml(m.channel_color || '#2563eb') + '">' + escapeHtml(m.channel) + '</span>' +
            '<span class="time-badge">' + dateShort + ' · ' + escapeHtml(m.time_str) + '</span>' +
            liveBadge +
          '</div>' +
          '<div class="movie-title">' + escapeHtml(m.title) + '</div>' +
          '<div class="movie-meta-line">' + metaParts.join(' &middot; ') + '</div>' +
          '<div class="movie-credits">' + dirHtml + castHtml + '</div>' +
        '</div>' +
      '</div>' +
      '<div class="card-desc">' + escapeHtml(m.description || 'Nessuna descrizione disponibile.') + '</div>' +
      '<div class="card-footer">' +
        '<span class="source-tag">Fonte: ' + escapeHtml(m.source) + '</span>' +
        '<button class="' + recCls + '" data-uid="' + escapeHtml(m.uid) + '" onclick="recordMovie(this.getAttribute(\'data-uid\'), event)">' + recText + '</button>' +
      '</div>' +
    '</div>';
  }).join('');
}

function openMovieModal(uid) {
  var m = allMovies.find(function(x) { return x.uid === uid; });
  if (!m) return;
  var bgUrl = m.backdrop_url || m.poster_url || '';
  var posterUrl = m.poster_url || m.backdrop_url || '';
  var recCls = m.recorded ? 'rec-btn recorded' : 'rec-btn';
  var recText = m.recorded ? '✅ Già programmato su Tvheadend' : '🔴 Registra su Tvheadend (237)';

  var html =
    '<div class="modal-hero" style="background-image:url(\'' + escapeHtml(bgUrl) + '\')">' +
      '<button class="modal-close" onclick="closeModal(event)">&times;</button>' +
      '<div class="modal-header-content">' +
        (posterUrl ? '<img class="modal-poster" src="' + escapeHtml(posterUrl) + '">' : '') +
        '<div>' +
          '<div class="card-badges" style="margin-bottom:8px;">' +
            '<span class="ch-badge" style="background:' + escapeHtml(m.channel_color || '#2563eb') + '">LCN ' + m.lcn + ' · ' + escapeHtml(m.channel) + '</span>' +
            '<span class="time-badge">' + escapeHtml(m.date_str) + ' · ' + escapeHtml(m.time_str) + '</span>' +
            (m.rating ? '<span class="time-badge" style="color:#fbbf24">⭐ ' + escapeHtml(m.rating) + '/10</span>' : '') +
          '</div>' +
          '<h2 style="font-size:22px;font-weight:800;color:#fff;line-height:1.2;">' + escapeHtml(m.title) + '</h2>' +
        '</div>' +
      '</div>' +
    '</div>' +
    '<div class="modal-body">' +
      '<div class="modal-info-grid">' +
        '<div><div class="info-item-label">Anno & Paese</div><div class="info-item-val">' + escapeHtml((m.year || 'N/D') + (m.country ? ' (' + m.country + ')' : '')) + '</div></div>' +
        '<div><div class="info-item-label">Genere & Durata</div><div class="info-item-val">' + escapeHtml((m.genre || 'Film') + ' · ' + m.duration_min + ' min') + '</div></div>' +
        '<div><div class="info-item-label">Regia</div><div class="info-item-val">' + escapeHtml(m.director || 'Non specificata') + '</div></div>' +
        '<div><div class="info-item-label">Fonte Dati</div><div class="info-item-val">' + escapeHtml(m.source) + '</div></div>' +
      '</div>' +
      '<div>' +
        '<div class="info-item-label" style="margin-bottom:4px;">Cast / Interpreti</div>' +
        '<div style="font-size:13.5px;color:#cbd5e1;line-height:1.45;">' + escapeHtml(m.cast_list || 'Non disponibile nei metadati del programma.') + '</div>' +
      '</div>' +
      '<div>' +
        '<div class="info-item-label" style="margin-bottom:4px;">Trama / Descrizione</div>' +
        '<div class="modal-plot">' + escapeHtml(m.description || 'Nessuna descrizione disponibile.') + '</div>' +
      '</div>' +
      '<div style="display:flex;justify-content:flex-end;gap:10px;padding-top:8px;border-top:1px solid rgba(255,255,255,0.1);">' +
        '<button class="' + recCls + '" style="padding:9px 18px;font-size:13px;" data-uid="' + escapeHtml(m.uid) + '" onclick="recordMovie(this.getAttribute(\'data-uid\'), event)">' + recText + '</button>' +
      '</div>' +
    '</div>';

  document.getElementById('modalContent').innerHTML = html;
  document.getElementById('movieModal').classList.add('open');
}

function closeModal(e) {
  if (e) e.stopPropagation();
  document.getElementById('movieModal').classList.remove('open');
}

function recordMovie(uid, e) {
  if (e) e.stopPropagation();
  showToast('⏳ Invio richiesta di registrazione a Tvheadend (237)...');
  fetch('/api/record?uid=' + encodeURIComponent(uid), { method: 'POST' })
    .then(function(r) { return r.json(); })
    .then(function(res) {
      if (res.ok) {
        var m = allMovies.find(function(x) { return x.uid === uid; });
        if (m) m.recorded = 1;
        renderMovies();
        showToast('✅ ' + res.message);
      } else {
        showToast('⚠️ ' + res.message);
      }
    })
    .catch(function(err) {
      showToast('⚠️ Errore comunicazione server');
    });
}

function triggerRefresh() {
  var btn = document.getElementById('refreshBtn');
  btn.classList.add('spinning');
  showToast('🔄 Aggiornamento JSON da RaiPlay e Mediaset Infinity avviato...');
  fetch('/api/refresh', { method: 'POST' })
    .then(function(r) { return r.json(); })
    .then(function() {
      setTimeout(function() {
        btn.classList.remove('spinning');
        loadMovies();
      }, 3000);
    });
}

window.addEventListener('DOMContentLoaded', loadMovies);
</script>
</body>
</html>
"""


# ==============================================================================
# HTTP Server Handler
# ==============================================================================

class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class CineGuidaHandler(http.server.BaseHTTPRequestHandler):
    store: MovieStore = None

    def log_message(self, fmt, *args):
        pass

    def _send_json(self, payload: dict, status: int = 200):
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path

        if path in ("/", "/index.html"):
            raw = HTML_PAGE.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
            return

        if path == "/api/movies":
            now_ts = int(time.time())
            today = datetime.date.today()
            tomorrow = today + datetime.timedelta(days=1)
            with self.store._conn() as conn:
                rows = conn.execute(
                    "SELECT * FROM movies WHERE end_ts >= ? ORDER BY start_ts ASC",
                    (now_ts - 1800,)
                ).fetchall()
            movies = [dict(r) for r in rows]
            last_ts = int(self.store.get_state("last_sync_ts", "0") or "0")
            last_str = datetime.datetime.fromtimestamp(last_ts).strftime("%H:%M") if last_ts else ""
            self._send_json({
                "now_ts": now_ts,
                "today_str": today.isoformat(),
                "tomorrow_str": tomorrow.isoformat(),
                "last_sync_str": last_str,
                "sync_in_progress": self.store.sync_in_progress,
                "movies": movies,
            })
            return

        self.send_error(404, "Not Found")

    def do_POST(self):
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        if path == "/api/refresh":
            if not self.store.sync_in_progress:
                threading.Thread(target=sync_all_schedules, args=(self.store, 120), daemon=True).start()
            self._send_json({"ok": True, "status": "sync_started"})
            return

        if path == "/api/record":
            uid = (query.get("uid") or [""])[0]
            with self.store._conn() as conn:
                row = conn.execute("SELECT * FROM movies WHERE uid = ?", (uid,)).fetchone()
            if not row:
                self._send_json({"ok": False, "message": "Film non trovato nel database."}, 404)
                return
            movie = dict(row)
            ok, msg = schedule_tvheadend_recording(movie)
            if ok:
                with self.store.lock, self.store._conn() as conn:
                    conn.execute("UPDATE movies SET recorded = 1 WHERE uid = ?", (uid,))
                    conn.commit()
            self._send_json({"ok": ok, "message": msg})
            return

        self.send_error(404, "Not Found")


# ==============================================================================
# Main Entrypoint
# ==============================================================================

def main():
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))

    parser = argparse.ArgumentParser(description="CineGuida — Web App for Rai & Mediaset Movies")
    parser.add_argument("-p", "--port", type=int, default=DEFAULT_PORT, help="HTTP port (default: 8090)")
    parser.add_argument("--no-browser", action="store_true", help="Do not auto-open browser on startup")
    parser.add_argument("--local", action="store_true", help="Run server locally instead of opening 192.168.1.237")
    args = parser.parse_args()

    is_termux = os.path.exists("/data/data/com.termux/files/usr/bin/termux-open-url")
    if is_termux and not args.local:
        remote_url = f"http://192.168.1.237:{args.port}"
        try:
            urllib.request.urlopen(f"{remote_url}/api/movies", timeout=3).read(64)
        except Exception:
            subprocess.run(
                ["ssh", "pi@192.168.1.237", "sudo systemctl restart cineguida.service"],
                check=False,
            )
            time.sleep(1.5)
        print(f"● CineGuida è attiva sul server 192.168.1.237: {remote_url}")
        if not args.no_browser:
            subprocess.run(["termux-open-url", remote_url], check=False)
        return

    store = MovieStore(DB_PATH)
    CineGuidaHandler.store = store

    with store._conn() as conn:
        cnt = conn.execute("SELECT COUNT(*) AS c FROM movies WHERE end_ts >= ?", (int(time.time()),)).fetchone()["c"]

    if cnt == 0:
        print("[CineGuida] Scaricamento iniziale palinsesti JSON da RaiPlay e Mediaset Infinity...")
        sync_all_schedules(store, enrich_limit=50)
        threading.Thread(target=sync_all_schedules, args=(store, 180), daemon=True).start()
    else:
        threading.Thread(target=sync_all_schedules, args=(store, 150), daemon=True).start()

    threading.Thread(target=periodic_sync_worker, args=(store,), daemon=True).start()

    server = None
    port = args.port
    for p in range(port, port + 10):
        try:
            server = ThreadedHTTPServer(("0.0.0.0", p), CineGuidaHandler)
            port = p
            break
        except OSError:
            continue

    if not server:
        print("Errore: Impossibile aprire la porta per CineGuida.")
        sys.exit(1)

    url = f"http://127.0.0.1:{port}"
    print(f"● CineGuida WebApp attiva su: {url}")

    if not args.no_browser and os.path.exists("/data/data/com.termux/files/usr/bin/termux-open-url"):
        subprocess.run(["termux-open-url", url], check=False)

    server.serve_forever()


if __name__ == "__main__":
    main()
