#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, logging, re, random, signal, uuid, datetime as dt
from typing import Optional, Tuple, Dict, List
import requests
import psycopg
from psycopg.rows import dict_row
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

# =========================
# CONFIG
# =========================
DB_DSN        = os.getenv("PG_DSN", "postgresql://postgres:root@localhost:5432/track_donor_v4")
USER_AGENT    = os.getenv("NOMINATIM_USER_AGENT", "YourApp/1.0 (contact: tareqelbawab94@gmail.com)")
COUNTRY       = os.getenv("DEFAULT_COUNTRY", "US")
BATCH_SIZE    = int(os.getenv("BATCH_SIZE", "200"))
HTTP_RETRIES  = int(os.getenv("HTTP_RETRIES", "3"))
LOG_FILE      = os.getenv("LOG_FILE", "geocode_worker.log")
LOG_LEVEL     = os.getenv("LOG_LEVEL", "INFO").upper()
FORCE_FRESH   = os.getenv("FORCE_FRESH", "0") == "1"  # تجاهل الكاش إذا 1
DRY_RUN       = os.getenv("DRY_RUN", "0") == "1"

# Fallback to Google (اختياري)
USE_GOOGLE_FALLBACK = os.getenv("USE_GOOGLE_FALLBACK", "0") == "1"
GOOGLE_KEY          = os.getenv("GOOGLE_GEOCODE_KEY", "")
GOOGLE_REGION_BIAS  = os.getenv("GOOGLE_REGION_BIAS", "US")
GOOGLE_ENDPOINT     = "https://maps.googleapis.com/maps/api/geocode/json"

# Global QPS حسب سياسة Nominatim
QPS_GLOBAL    = float(os.getenv("QPS_GLOBAL", "1"))
MIN_GAP_SEC   = 1.0 / max(QPS_GLOBAL, 0.001)

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# =========================
# Logging
# =========================
RUN_ID = uuid.uuid4().hex[:8]
logger = logging.getLogger("geocode_worker")
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
_console = logging.StreamHandler()
_console.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
_console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [run=%(run_id)s] %(message)s"))
from logging.handlers import RotatingFileHandler
_file = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=5, encoding="utf-8")
_file.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
_file.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [run=%(run_id)s] %(name)s %(funcName)s:%(lineno)d - %(message)s"))

class RunIdFilter(logging.Filter):
    def filter(self, record): record.run_id = RUN_ID; return True
_console.addFilter(RunIdFilter()); _file.addFilter(RunIdFilter())
logger.handlers.clear(); logger.addHandler(_console); logger.addHandler(_file)

logger.info(f"Starting run_id={RUN_ID} QPS_GLOBAL={QPS_GLOBAL} FORCE_FRESH={FORCE_FRESH} DRY_RUN={DRY_RUN}")

# =========================
# Graceful stop
# =========================
_stop = False
def _handle_sigterm(signum, frame):
    global _stop
    logger.warning(f"Signal {signum} -> will stop after current batch.")
    _stop = True
signal.signal(signal.SIGINT, _handle_sigterm); signal.signal(signal.SIGTERM, _handle_sigterm)

# =========================
# DB helpers
# =========================
@contextmanager
def db_connect():
    conn = None
    try:
        conn = psycopg.connect(
            DB_DSN,
            row_factory=dict_row,
            autocommit=False,
            options="-c statement_timeout=600000 -c idle_in_transaction_session_timeout=0"
        )
        yield conn
    finally:
        if conn is not None:
            try: conn.close()
            except Exception as e: logger.warning(f"DB close error: {e}")

def get_contrib_columns(conn) -> Dict[str, bool]:
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='contributions'
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = {r["column_name"] for r in cur.fetchall()}
    return {
        "has_checked_at": "geocode_checked_at" in cols,
        "has_provider":   "geocode_provider" in cols,
    }

def cache_get_many(conn, keys: List[str]) -> Dict[str, Tuple[float,float,str]]:
    if not keys: return {}
    qmarks = ",".join(["%s"] * len(keys))
    sql = f"SELECT key, lat, lng, provider FROM geocode_cache WHERE key IN ({qmarks})"
    out: Dict[str, Tuple[float,float,str]] = {}
    with conn.cursor() as cur:
        cur.execute(sql, keys)
        for r in cur.fetchall():
            out[r["key"]] = (r["lat"], r["lng"], r.get("provider") or "nominatim")
    return out

def cache_put_many(conn, items: Dict[str, Tuple[float,float,str]]):
    if not items or DRY_RUN: return
    with conn.cursor() as cur:
        cur.execute("BEGIN;")
        for k,(lat,lng,prov) in items.items():
            cur.execute("""
                INSERT INTO geocode_cache(key,lat,lng,provider)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (key) DO UPDATE
                  SET lat=EXCLUDED.lat, lng=EXCLUDED.lng, provider=EXCLUDED.provider, updated_at=now()
            """, (k,lat,lng,prov))
        cur.execute("COMMIT;")

# =========================
# Global rate limiter (DB-backed)
# =========================
def pace_global(conn):
    with conn.cursor() as cur:
        cur.execute("BEGIN;")
        cur.execute("SELECT last_ts FROM nominatim_rl WHERE id=1 FOR UPDATE;")
        last_ts = cur.fetchone()["last_ts"]
        cur.execute("SELECT now() AS now_ts;")
        now_ts = cur.fetchone()["now_ts"]
        target_ts = last_ts + dt.timedelta(seconds=MIN_GAP_SEC)
        wait = (target_ts - now_ts).total_seconds()
        if wait > 0:
            time.sleep(wait)
        cur.execute("UPDATE nominatim_rl SET last_ts = now() WHERE id=1;")
        cur.execute("COMMIT;")

# =========================
# Address cleaning / normalization
# =========================
_clean_re = re.compile(r'\s+')
def norm(s: Optional[str]) -> str: return _clean_re.sub(' ', (s or '').strip().lower())
def as_str(x) -> str: return '' if x is None else str(x)

_unit_re = re.compile(r'(?:\s*(?:\#|apt\.?|apartment|suite|ste\.?|unit|bldg|building|fl\.?|floor|rm\.?|room|dept|department|po\s*box|p\.?o\.?\s*box|box)\s*[\w\-]+)', re.IGNORECASE)
def _space(s:str)->str: return re.sub(r'\s+',' ',(s or '').strip())

def clean_street(s:str)->str:
    s=(s or '').replace(',', ' ')
    s=_unit_re.sub('', s)
    return _space(s).title()

def clean_city(s:str)->str:
    return _space(s).title()

def clean_state(s:str)->str:
    s=_space(s).upper()
    return s[:2] if len(s)>=2 else s

def sanitize_zip(x) -> str:
    s = as_str(x)
    digits = re.sub(r'\D+', '', s)
    return digits[:5].zfill(5) if digits else ''

def mk_addr(street='', city='', state='', zip5='', country='USA') -> str:
    parts = []
    if street.strip(): parts.append(street.strip())
    if city.strip():   parts.append(city.strip())
    if state.strip():  parts.append(state.strip())
    if zip5.strip():   parts.append(zip5.strip()[:5])
    if country:        parts.append(country)
    return ', '.join(parts)

def cache_key(street, city, state, zip5)->str:
    return norm(mk_addr(street, city, state, zip5, "USA"))

# =========================
# Geocoders
# =========================
class Nominatim:
    def __init__(self, ua: str, country: str):
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": ua})
        self.country = country

    def one_structured(self, conn_rl, params: Dict[str, str]) -> Optional[Tuple[float,float]]:
        pace_global(conn_rl)
        base={"format":"jsonv2","limit":1,"addressdetails":0}
        final = {**base, **params}
        try:
            r=self.s.get(NOMINATIM_URL, params=final, timeout=(10,30))
            if r.status_code in (429,503): return None
            r.raise_for_status()
            data=r.json()
            if not data: return None
            return float(data[0]["lat"]), float(data[0]["lon"])
        except Exception:
            return None

    def with_retries(self, conn_rl, params: Dict[str,str])->Optional[Tuple[float,float]]:
        for attempt in range(HTTP_RETRIES):
            res=self.one_structured(conn_rl, params)
            if res: return res
            backoff=min(180,(2**attempt))+random.uniform(0,0.5)
            time.sleep(backoff)
        return None

class GoogleGeocoder:
    def __init__(self, api_key: str, region_bias: str = "US"):
        self.key = api_key
        self.session = requests.Session()
        self.region_bias = region_bias

    def geocode(self, address: str, zip5: str = "", country: str = "") -> Optional[Tuple[float,float]]:
        if not self.key:
            return None
        params = {"key": self.key, "address": address}
        comp = []
        if zip5: comp.append(f"postal_code:{zip5}")
        if country: comp.append(f"country:{country}")
        elif self.region_bias: comp.append(f"country:{self.region_bias}")
        if comp: params["components"] = "|".join(comp)
        try:
            r = self.session.get(GOOGLE_ENDPOINT, params=params, timeout=(10,30))
            r.raise_for_status()
            data = r.json()
            if data.get("status") != "OK": return None
            loc = data["results"][0]["geometry"]["location"]
            return float(loc["lat"]), float(loc["lng"])
        except Exception:
            return None

# =========================
# Batch reservation (SKIP LOCKED) — lat/lng فقط
# =========================
def reserve_batch(conn, has_checked_at: bool) -> List[dict]:
    mark_checked = "geocode_checked_at = COALESCE(geocode_checked_at, now())," if has_checked_at else ""
    sql = f"""
    WITH picked AS (
        SELECT ctid
        FROM public.contributions
        WHERE (lat IS NULL OR lng IS NULL)
        ORDER BY ctid
        LIMIT {BATCH_SIZE}
        FOR UPDATE SKIP LOCKED
    )
    UPDATE public.contributions t
       SET {mark_checked}
           -- no-op لتفعيل RETURNING
           contributor_zip = contributor_zip
      FROM picked
     WHERE t.ctid = picked.ctid
    RETURNING t.ctid,
              t.contributor_street_1 AS st1,
              t.contributor_city AS city,
              t.contributor_state AS state,
              t.contributor_zip AS zip;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    logger.info(f"[batch] reserved rows={len(rows)}")
    return rows

def build_update_sql(flags: Dict[str,bool]) -> str:
    set_parts = ["lat = %s", "lng = %s"]
    if flags.get("has_provider"): set_parts.append("geocode_provider = %s")
    if flags.get("has_checked_at"): set_parts.append("geocode_checked_at = now()")
    set_sql = ", ".join(set_parts)
    return f"""
        UPDATE public.contributions
           SET {set_sql}
         WHERE ctid = %s
           AND (lat IS NULL OR lng IS NULL)
    """

def save_updates(conn, updates: List[Tuple], update_sql: str):
    if not updates:
        logger.info("[batch] no updates to write"); return 0
    if DRY_RUN:
        logger.info(f"[batch] DRY_RUN -> would write updates={len(updates)} (skipped)"); return 0
    with conn.cursor() as cur:
        cur.execute("BEGIN;")
        cur.executemany(update_sql, updates)
        cur.execute("COMMIT;")
    logger.info(f"[batch] wrote updates={len(updates)}")
    return len(updates)

# =========================
# Processor (Cache → Nominatim → Google)
# =========================
def process_once(conn_main, conn_rl, geo: Nominatim, ggeo: Optional[GoogleGeocoder]) -> Tuple[int,int]:
    flags = get_contrib_columns(conn_main)
    rows = reserve_batch(conn_main, has_checked_at=flags["has_checked_at"])
    if not rows: return 0, 0

    # جهّز مفاتيح للكاش
    key_to_ctids: Dict[str, List[str]] = {}
    key_to_parts: Dict[str, Tuple[str,str,str,str]] = {}

    for r in rows:
        street = clean_street(as_str(r["st1"]))
        city   = clean_city(as_str(r["city"]))
        state  = clean_state(as_str(r["state"]))
        zip5   = sanitize_zip(r["zip"])
        k = cache_key(street, city, state, zip5)
        key_to_ctids.setdefault(k, []).append(r["ctid"])
        if k not in key_to_parts: key_to_parts[k] = (street, city, state, zip5)

    uniq_keys = list(key_to_ctids.keys())
    cached = {} if FORCE_FRESH else cache_get_many(conn_main, uniq_keys)
    to_resolve = [k for k in uniq_keys if k not in cached]

    newly_cached: Dict[str, Tuple[float,float,str]] = {}

    # حلّ العناوين غير الموجودة في الكاش
    for i, k in enumerate(to_resolve, 1):
        street, city, state, zip5 = key_to_parts[k]
        latlng: Optional[Tuple[float,float]] = None
        provider = None

        # 1) Nominatim (structured)
        params = {"street": street, "city": city, "state": state, "postalcode": zip5, "countrycodes": "us"}
        latlng = geo.with_retries(conn_rl, params)
        provider = "nominatim" if latlng else None

        # 2) Google fallback (اختياري)
        if not latlng and USE_GOOGLE_FALLBACK and GOOGLE_KEY:
            addr = mk_addr(street, city, state, zip5, COUNTRY or GOOGLE_REGION_BIAS)
            g = GoogleGeocoder(GOOGLE_KEY, GOOGLE_REGION_BIAS)
            latlng = g.geocode(addr, zip5, COUNTRY or GOOGLE_REGION_BIAS)
            provider = "google" if latlng else None

        if latlng:
            newly_cached[k] = (latlng[0], latlng[1], provider or "nominatim")

        if i % 100 == 0:
            logger.info(f"[progress] resolved_keys={len(newly_cached)} / {i}")

    # اكتب للكاش وادمج
    if newly_cached: cache_put_many(conn_main, newly_cached)
    hits: Dict[str, Tuple[float,float,str]] = {**cached, **newly_cached}

    # Build UPDATE sql مرة واحدة حسب الأعمدة المتاحة
    update_sql = build_update_sql(flags)

    # حضّر تحديثات DB
    updates: List[Tuple] = []
    for k, ctids in key_to_ctids.items():
        hit = hits.get(k)
        if not hit: continue
        lat, lng, prov = hit
        for ctid in ctids:
            params = [lat, lng]
            if flags["has_provider"]:
                params.append(prov)
            params.append(ctid)
            updates.append(tuple(params))

    n = save_updates(conn_main, updates, update_sql)
    logger.info(f"[summary] reserved={len(rows)} updated={n} cache_hit_keys={len(cached)} new_cache_keys={len(newly_cached)}")
    return n, len(rows)

# =========================
# Main loop
# =========================
def run_forever():
    geo = Nominatim(USER_AGENT, COUNTRY)

    while not _stop:
        try:
            with db_connect() as conn_main, db_connect() as conn_rl:
                try:
                    updated, rows_count = process_once(conn_main, conn_rl, geo, None if not USE_GOOGLE_FALLBACK or not GOOGLE_KEY else GoogleGeocoder(GOOGLE_KEY, GOOGLE_REGION_BIAS))
                    if rows_count == 0:
                        logger.info("No rows available. Sleeping 5 min...")
                        time.sleep(300)
                    elif updated == 0:
                        logger.info("Rows reserved but no updates. Sleeping 10s...")
                        time.sleep(10)
                except Exception:
                    logger.exception("Batch error"); time.sleep(3); continue
        except psycopg.OperationalError as e:
            logger.warning(f"DB reconnect after error: {e}"); time.sleep(3); continue
    logger.info("Graceful stop requested. Bye.")

if __name__ == "__main__":
    run_forever()