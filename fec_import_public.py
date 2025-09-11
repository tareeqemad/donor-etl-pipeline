# fec_import_public.py
import os, sys, argparse, datetime as dt
from time import monotonic, sleep
from typing import Dict, Any, Tuple, Optional
import random

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError
from urllib3.util.retry import Retry

from dotenv import load_dotenv
from psycopg import connect
from psycopg.rows import dict_row
from tqdm import tqdm

load_dotenv()

BASE_URL = "https://api.open.fec.gov/v1/schedules/schedule_a/"

# --------------------------- Rate limiting ---------------------------

class RateLimiter:
    def __init__(self, rpm: int = 15):
        rpm = max(1, int(rpm))
        self.interval = 60.0 / float(rpm)
        self._last = 0.0
        self.backoff = 1.0

    def before_request(self):
        now = monotonic()
        wait = (self._last + self.interval) - now
        if wait > 0:
            sleep(wait)

    def after_success(self):
        self._last = monotonic()
        self.backoff = max(1.0, self.backoff * 0.5)

    def after_rate_limit(self, retry_after: float | None = None):
        if retry_after is None or retry_after <= 0:
            retry_after = min(120.0, 2.0 * self.backoff)
        sleep(float(retry_after))
        self._last = monotonic()
        self.backoff = min(60.0, self.backoff * 2.0)

def parse_retry_after(resp):
    ra = resp.headers.get("Retry-After")
    try:
        return float(ra) if ra is not None else None
    except Exception:
        return None

def maybe_pace_by_headers(resp, debug=False, limiter: "RateLimiter | None" = None):
    if not limiter:
        return
    remaining = resp.headers.get("X-RateLimit-Remaining")
    reset = resp.headers.get("X-RateLimit-Reset")
    try:
        remaining_val = int(remaining) if remaining is not None else None
        reset_val = int(reset) if reset is not None else None
    except Exception:
        remaining_val = None
        reset_val = None

    if remaining_val is not None and remaining_val <= 0 and reset_val is not None:
        if debug:
            print(f"[RATE] Remaining=0; backing off for reset {reset_val}s")
        limiter.after_rate_limit(reset_val)

# --------------------------- Helpers ---------------------------

def env_or_die(name: str) -> str:
    v = os.getenv(name)
    if not v:
        print(f"[ERR] missing env {name}", file=sys.stderr)
        sys.exit(2)
    return v

# نتأكد من وجود الفهارس الداعمة للـ upsert (بدون تغيير الأعمدة)
def ensure_constraints(conn):
    with conn.cursor() as cur:
        # فهرس فريد على sub_id (لو عندك بالفعل قيد UNIQUE(sub_id) فهالإنشاء سيتخطّاه)
        cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE schemaname='public' AND indexname='ux_contributions_sub_id'
            ) THEN
                EXECUTE 'CREATE UNIQUE INDEX ux_contributions_sub_id ON public.contributions (sub_id)';
            END IF;
        END$$;
        """)
        # فهرس فريد على (transaction_id, contribution_receipt_date, contribution_receipt_amount)
        cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE schemaname='public' AND indexname='ux_contributions_txn_date_amount'
            ) THEN
                EXECUTE 'CREATE UNIQUE INDEX ux_contributions_txn_date_amount
                         ON public.contributions (transaction_id, contribution_receipt_date, contribution_receipt_amount)';
            END IF;
        END$$;
        """)
    conn.commit()

# --------------------------- HTTP session & fetch ---------------------------

def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5, connect=3, read=3, backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers["User-Agent"] = "fec-import/1.0"
    return s

def fetch_schedule_a(session: requests.Session, params: Dict[str, Any], limiter: RateLimiter, debug=False) -> Dict[str, Any]:
    retries = 0
    while True:
        limiter.before_request()
        if debug:
            print(f"[HTTP] GET {BASE_URL} params={params}")
        try:
            resp = session.get(BASE_URL, params=params, timeout=(10, 180))
        except (ReadTimeout, ConnectTimeout, ConnectionError) as e:
            wait = min(60.0, (2 ** retries)) + random.uniform(0, 0.5)
            if debug:
                print(f"[HTTP] timeout/conn error: {type(e).__name__}. sleeping {wait:.1f}s, retry={retries}")
            sleep(wait); retries += 1
            limiter.after_rate_limit(wait)
            continue

        status = resp.status_code

        if status == 429 or "OVER_RATE_LIMIT" in resp.text:
            ra = parse_retry_after(resp)
            if debug:
                print(f"[RATE] 429/OVER_RATE_LIMIT. Retry-After={ra}. Backoff={limiter.backoff}")
            limiter.after_rate_limit(ra)
            continue

        if status in (500, 502, 503, 504):
            wait = min(60.0, 2.0 * (2 ** retries))
            if debug:
                print(f"[HTTP] {status} server error. Sleeping {wait:.1f}s, retry={retries}")
            sleep(wait); retries += 1
            continue

        resp.raise_for_status()
        maybe_pace_by_headers(resp, debug=debug, limiter=limiter)
        limiter.after_success()
        return resp.json()

# --------------------------- Row builders & DB ops ---------------------------

# جدولك حسب الاستعلام:
# contrib_row_id, sub_id, transaction_id, two_year_transaction_period, committee_id,
# contributor_name, contributor_first_name, contributor_last_name, contributor_street_1, contributor_street_2,
# contributor_city, contributor_state, contributor_zip, contributor_employer, contributor_occupation,
# is_individual, contribution_receipt_date, contributor_year, contribution_receipt_amount,
# contribution_aggregate_ytd, is_refunded_or_reattributed, contribution_refunded_amount,
# line_number, line_number_label, lat, lng, geocode_provider, geocode_checked_at

def build_row(r: Dict[str, Any]) -> Dict[str, Any]:
    dstr = r.get("contribution_receipt_date")
    contributor_year = None
    if dstr:
        try:
            contributor_year = dt.date.fromisoformat(dstr).year
        except Exception:
            contributor_year = None

    return {
        "sub_id": r.get("sub_id"),
        "transaction_id": r.get("transaction_id"),
        "two_year_transaction_period": r.get("two_year_transaction_period"),
        "committee_id": r.get("committee_id"),
        "contributor_name": r.get("contributor_name"),
        "contributor_first_name": r.get("contributor_first_name"),
        "contributor_last_name": r.get("contributor_last_name"),
        "contributor_street_1": r.get("contributor_street_1"),
        "contributor_street_2": r.get("contributor_street_2"),
        "contributor_city": r.get("contributor_city"),
        "contributor_state": r.get("contributor_state"),
        "contributor_zip": r.get("contributor_zip"),
        "contributor_employer": r.get("contributor_employer"),
        "contributor_occupation": r.get("contributor_occupation"),
        "is_individual": r.get("is_individual"),
        "contribution_receipt_date": dstr,
        "contributor_year": contributor_year,
        "contribution_receipt_amount": r.get("contribution_receipt_amount") or 0,
        "contribution_aggregate_ytd": r.get("contribution_aggregate_ytd"),
        "is_refunded_or_reattributed": r.get("is_refunded_or_reattributed"),
        "contribution_refunded_amount": r.get("contribution_refunded_amount"),
        "line_number": r.get("line_number"),
        "line_number_label": r.get("line_number_label"),
        "lat": None,
        "lng": None,
        "geocode_provider": None,
        "geocode_checked_at": None,
    }

def upsert_by_sub(cur, row: Dict[str, Any]) -> Tuple[int, int]:
    sql = (
        "INSERT INTO public.contributions ("
        "  sub_id, transaction_id, two_year_transaction_period, committee_id,"
        "  contributor_name, contributor_first_name, contributor_last_name,"
        "  contributor_street_1, contributor_street_2, contributor_city, contributor_state, contributor_zip,"
        "  contributor_employer, contributor_occupation,"
        "  is_individual, contribution_receipt_date, contributor_year, contribution_receipt_amount, contribution_aggregate_ytd,"
        "  is_refunded_or_reattributed, contribution_refunded_amount,"
        "  line_number, line_number_label, lat, lng, geocode_provider, geocode_checked_at"
        ") VALUES ("
        "  %(sub_id)s, %(transaction_id)s, %(two_year_transaction_period)s, %(committee_id)s,"
        "  %(contributor_name)s, %(contributor_first_name)s, %(contributor_last_name)s,"
        "  %(contributor_street_1)s, %(contributor_street_2)s, %(contributor_city)s, %(contributor_state)s, %(contributor_zip)s,"
        "  %(contributor_employer)s, %(contributor_occupation)s,"
        "  %(is_individual)s, %(contribution_receipt_date)s, %(contributor_year)s, %(contribution_receipt_amount)s, %(contribution_aggregate_ytd)s,"
        "  %(is_refunded_or_reattributed)s, %(contribution_refunded_amount)s,"
        "  %(line_number)s, %(line_number_label)s, %(lat)s, %(lng)s, %(geocode_provider)s, %(geocode_checked_at)s"
        ") ON CONFLICT (sub_id) DO UPDATE SET "
        "  transaction_id = EXCLUDED.transaction_id,"
        "  two_year_transaction_period = EXCLUDED.two_year_transaction_period,"
        "  committee_id = EXCLUDED.committee_id,"
        "  contributor_name = EXCLUDED.contributor_name,"
        "  contributor_first_name = EXCLUDED.contributor_first_name,"
        "  contributor_last_name = EXCLUDED.contributor_last_name,"
        "  contributor_street_1 = EXCLUDED.contributor_street_1,"
        "  contributor_street_2 = EXCLUDED.contributor_street_2,"
        "  contributor_city = EXCLUDED.contributor_city,"
        "  contributor_state = EXCLUDED.contributor_state,"
        "  contributor_zip = EXCLUDED.contributor_zip,"
        "  contributor_employer = EXCLUDED.contributor_employer,"
        "  contributor_occupation = EXCLUDED.contributor_occupation,"
        "  is_individual = EXCLUDED.is_individual,"
        "  contribution_receipt_date = EXCLUDED.contribution_receipt_date,"
        "  contributor_year = EXCLUDED.contributor_year,"
        "  contribution_receipt_amount = EXCLUDED.contribution_receipt_amount,"
        "  contribution_aggregate_ytd = EXCLUDED.contribution_aggregate_ytd,"
        "  is_refunded_or_reattributed = EXCLUDED.is_refunded_or_reattributed,"
        "  contribution_refunded_amount = EXCLUDED.contribution_refunded_amount,"
        "  line_number = EXCLUDED.line_number,"
        "  line_number_label = EXCLUDED.line_number_label,"
        "  lat = COALESCE(EXCLUDED.lat, public.contributions.lat),"
        "  lng = COALESCE(EXCLUDED.lng, public.contributions.lng),"
        "  geocode_provider = COALESCE(EXCLUDED.geocode_provider, public.contributions.geocode_provider),"
        "  geocode_checked_at = COALESCE(EXCLUDED.geocode_checked_at, public.contributions.geocode_checked_at) "
        "RETURNING (xmax = 0) AS inserted;"
    )
    cur.execute(sql, row)
    ret = cur.fetchone()
    inserted = bool(ret and (ret.get('inserted') if isinstance(ret, dict) else ret[0]))
    return (1, 0) if inserted else (0, 1)

def insert_by_txn_or_ignore(cur, row: Dict[str, Any]) -> Tuple[int, int]:
    # fallback إذا ما فيه sub_id: منع التكرار على (transaction_id, date, amount)
    sql = (
        "INSERT INTO public.contributions ("
        "  sub_id, transaction_id, two_year_transaction_period, committee_id,"
        "  contributor_name, contributor_first_name, contributor_last_name,"
        "  contributor_street_1, contributor_street_2, contributor_city, contributor_state, contributor_zip,"
        "  contributor_employer, contributor_occupation,"
        "  is_individual, contribution_receipt_date, contributor_year, contribution_receipt_amount, contribution_aggregate_ytd,"
        "  is_refunded_or_reattributed, contribution_refunded_amount,"
        "  line_number, line_number_label, lat, lng, geocode_provider, geocode_checked_at"
        ") VALUES ("
        "  %(sub_id)s, %(transaction_id)s, %(two_year_transaction_period)s, %(committee_id)s,"
        "  %(contributor_name)s, %(contributor_first_name)s, %(contributor_last_name)s,"
        "  %(contributor_street_1)s, %(contributor_street_2)s, %(contributor_city)s, %(contributor_state)s, %(contributor_zip)s,"
        "  %(contributor_employer)s, %(contributor_occupation)s,"
        "  %(is_individual)s, %(contribution_receipt_date)s, %(contributor_year)s, %(contribution_receipt_amount)s, %(contribution_aggregate_ytd)s,"
        "  %(is_refunded_or_reattributed)s, %(contribution_refunded_amount)s,"
        "  %(line_number)s, %(line_number_label)s, %(lat)s, %(lng)s, %(geocode_provider)s, %(geocode_checked_at)s"
        ") ON CONFLICT (transaction_id, contribution_receipt_date, contribution_receipt_amount) DO NOTHING "
        "RETURNING 1;"
    )
    cur.execute(sql, row)
    return (1, 0) if cur.fetchone() else (0, 1)

def flush_batch(conn, batch, debug, counters):
    if not batch:
        return
    inserted = updated = ignored = 0
    with conn.cursor() as cur:
        for b in batch:
            if b["sub_id"] is not None:
                ins, upd = upsert_by_sub(cur, b)
                inserted += ins; updated += upd
            elif b["transaction_id"] and b["contribution_receipt_date"] and b["contribution_receipt_amount"] is not None:
                ins, ign = insert_by_txn_or_ignore(cur, b)
                inserted += ins; ignored += ign
            else:
                # إدخال عادي لو ما توفر أي مفتاح
                cur.execute(
                    "INSERT INTO public.contributions ("
                    "  sub_id, transaction_id, two_year_transaction_period, committee_id,"
                    "  contributor_name, contributor_first_name, contributor_last_name,"
                    "  contributor_street_1, contributor_street_2, contributor_city, contributor_state, contributor_zip,"
                    "  contributor_employer, contributor_occupation,"
                    "  is_individual, contribution_receipt_date, contributor_year, contribution_receipt_amount, contribution_aggregate_ytd,"
                    "  is_refunded_or_reattributed, contribution_refunded_amount,"
                    "  line_number, line_number_label, lat, lng, geocode_provider, geocode_checked_at"
                    ") VALUES ("
                    "  %(sub_id)s, %(transaction_id)s, %(two_year_transaction_period)s, %(committee_id)s,"
                    "  %(contributor_name)s, %(contributor_first_name)s, %(contributor_last_name)s,"
                    "  %(contributor_street_1)s, %(contributor_street_2)s, %(contributor_city)s, %(contributor_state)s, %(contributor_zip)s,"
                    "  %(contributor_employer)s, %(contributor_occupation)s,"
                    "  %(is_individual)s, %(contribution_receipt_date)s, %(contributor_year)s, %(contribution_receipt_amount)s, %(contribution_aggregate_ytd)s,"
                    "  %(is_refunded_or_reattributed)s, %(contribution_refunded_amount)s,"
                    "  %(line_number)s, %(line_number_label)s, %(lat)s, %(lng)s, %(geocode_provider)s, %(geocode_checked_at)s"
                    ");",
                    b
                )
                inserted += 1
        conn.commit()
    counters["inserted"] += inserted
    counters["updated"]  += updated
    counters["ignored"]  += ignored
    if debug:
        print(f"[COMMIT] +{len(batch)} rows => inserted={counters['inserted']}, updated={counters['updated']}, ignored={counters['ignored']}")

# --------------------------- Cursor state ---------------------------

def read_cursor(conn, committee_id: str, period: int):
    with conn.cursor() as cur:
        cur.execute("""
          SELECT watermark_date, last_page_date, last_index
          FROM public.sync_cursor
          WHERE committee_id=%s AND period=%s
        """, (committee_id, period))
        row = cur.fetchone()
        if not row:
            return None, None, None
        return row["watermark_date"], row["last_page_date"], row["last_index"]

def write_checkpoint(conn, committee_id: str, period: int,
                     max_date_seen: Optional[dt.date],
                     last_page_date: Optional[str],
                     last_index: Optional[str]):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO public.sync_cursor (committee_id, period, watermark_date, last_page_date, last_index, cursor_updated_at, last_run_at)
        VALUES (%s, %s, %s, %s, %s, now(), now())
        ON CONFLICT (committee_id, period) DO UPDATE
          SET watermark_date = GREATEST(public.sync_cursor.watermark_date, COALESCE(EXCLUDED.watermark_date, public.sync_cursor.watermark_date)),
              last_page_date = EXCLUDED.last_page_date,
              last_index     = EXCLUDED.last_index,
              cursor_updated_at = now(),
              last_run_at    = now()
        """, (committee_id, period, max_date_seen, last_page_date, last_index))
    conn.commit()

def set_watermark(conn, committee_id: str, period: int, new_date: dt.date):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO public.sync_cursor (committee_id, period, watermark_date, last_run_at)
        VALUES (%s, %s, %s, now())
        ON CONFLICT (committee_id, period)
        DO UPDATE SET watermark_date = GREATEST(public.sync_cursor.watermark_date, EXCLUDED.watermark_date),
                      last_run_at = now()
        """, (committee_id, period, new_date))
    conn.commit()

# --------------------------- Main runner ---------------------------

def run(committee_id: str, period: int, full: bool, window_days: int, debug: bool, rpm: int):
    dsn = env_or_die("PG_DSN")
    api_key = env_or_die("FEC_API_KEY")

    s = build_session()
    limiter = RateLimiter(rpm=rpm)

    counters = {"inserted": 0, "updated": 0, "ignored": 0}
    total_rows = 0

    with connect(dsn, row_factory=dict_row) as conn:
        # نتأكد من الفهارس فقط (لا ننشئ جداول جديدة)
        ensure_constraints(conn)

        wm, resume_date, resume_idx = read_cursor(conn, committee_id, period)
        if full:
            params = {
                "api_key": api_key,
                "committee_id": committee_id,
                "two_year_transaction_period": period,
                "sort": "-contribution_receipt_date",
                "per_page": 100
            }
            if debug: print(f"[MODE] FULL for {committee_id}/{period}")
        else:
            params = {
                "api_key": api_key,
                "committee_id": committee_id,
                "two_year_transaction_period": period,
                "sort": "-contribution_receipt_date",
                "per_page": 100
            }
            if resume_date and resume_idx:
                params["last_contribution_receipt_date"] = resume_date
                params["last_index"] = resume_idx
                if debug: print("[RESUME] Attempting from checkpoint ...")
            else:
                md = (wm or dt.date(1900,1,1)) - dt.timedelta(days=window_days)
                params["min_date"] = md.isoformat()
                if debug: print(f"[MODE] INCR from {md} (wm={wm}, window={window_days}d)")

        max_date_seen = dt.date(1900,1,1)
        batch = []
        page = 0
        total_pages = None
        pbar = None

        while True:
            data = fetch_schedule_a(s, params, limiter, debug=debug)

            if total_pages is None:
                count = (data.get("pagination") or {}).get("count")
                if count:
                    total_pages = (count // 100) + (1 if count % 100 else 0)
                    pbar = tqdm(total=total_pages, desc="Downloading pages", unit="page")

            results = data.get("results", [])
            page += 1
            if pbar: pbar.update(1)

            if debug: print(f"[PAGE {page}] rows={len(results)}")
            if not results:
                break

            for r in results:
                total_rows += 1
                row = build_row(r)
                batch.append(row)

                dstr = row.get("contribution_receipt_date")
                if dstr:
                    try:
                        d = dt.date.fromisoformat(dstr)
                        if d > max_date_seen: max_date_seen = d
                    except Exception:
                        pass

                if len(batch) >= 200:
                    flush_batch(conn, batch, debug, counters)
                    batch.clear()

            last_indexes = (data.get("pagination") or {}).get("last_indexes")
            if not last_indexes:
                break
            params = {
                "api_key": api_key,
                "committee_id": committee_id,
                "two_year_transaction_period": period,
                "sort": "-contribution_receipt_date",
                "per_page": 100,
                "last_contribution_receipt_date": last_indexes.get("last_contribution_receipt_date"),
                "last_index": last_indexes.get("last_index")
            }
            write_checkpoint(conn, committee_id, period,
                             max_date_seen,
                             last_indexes.get("last_contribution_receipt_date"),
                             last_indexes.get("last_index"))

        if batch:
            flush_batch(conn, batch, debug, counters)
            batch.clear()

        if pbar: pbar.close()

    if total_rows > 0:
        with connect(dsn, row_factory=dict_row) as conn2:
            set_watermark(conn2, committee_id, period, max_date_seen)
            if debug: print(f"[WM] -> {max_date_seen}")

    print(f"[SUMMARY] processed={total_rows}, inserted={counters['inserted']}, updated={counters['updated']}, ignored={counters['ignored']}")

def main():
    ap = argparse.ArgumentParser(description="Import FEC Schedule A into public.contributions (matching your schema) with resume + rate limiting.")
    ap.add_argument("--committee-id", required=True, help="FEC committee ID, e.g., C00797670")
    ap.add_argument("--period", type=int, required=True, help="Two-year transaction period (even year)")
    ap.add_argument("--full", action="store_true", help="Full backfill for the given period")
    ap.add_argument("--window-days", type=int, default=7, help="Incremental safety overlap in days")
    ap.add_argument("--rpm", type=int, default=int(os.getenv("FEC_RPM", "15")), help="Requests per minute limit (default 15)")
    ap.add_argument("--debug", action="store_true", help="Verbose HTTP/DB logs")
    args = ap.parse_args()
    run(args.committee_id, args.period, args.full, args.window_days, args.debug, args.rpm)

if __name__ == "__main__":
    main()