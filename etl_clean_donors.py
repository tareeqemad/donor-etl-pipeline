import re
import sys
import hashlib
import logging
import unicodedata
from typing import Optional, List, Dict, Any

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text, bindparam
from rapidfuzz import fuzz, process  # fuzzy matching

# ----------------------------
# Config
# ----------------------------
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'track_donor_v4'
DB_USER = 'postgres'
DB_PASSWORD = 'root'

from sqlalchemy import create_engine

ENGINE = create_engine(
    f'postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}',
    pool_pre_ping=True,
)

DONOR_KEY_LEN = 64
LOG_EVERY = 50000
CHUNK_SIZE = 10000

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("donor_etl")

# ----------------------------
# Normalization helpers
# ----------------------------
SUFFIX_MAP = {
    "AVENUE":"AVE","AVE":"AVE","AV":"AVE",
    "STREET":"ST","ST":"ST","STR":"ST",
    "ROAD":"RD","RD":"RD",
    "BOULEVARD":"BLVD","BD":"BLVD","BLVD":"BLVD",
    "LANE":"LN","LANES":"LN","LN":"LN",
    "DRIVE":"DR","DR":"DR",
    "COURT":"CT","CT":"CT",
    "TERRACE":"TER","TER":"TER",
    "PLACE":"PL","PL":"PL",
    "HIGHWAY":"HWY","HWY":"HWY",
    "CIRCLE":"CIR","CIR":"CIR",
    "WAY":"WAY","WY":"WAY",
    "PLAZA":"PLZ","PLZ":"PLZ",
    "ALLEY":"ALY","ALY":"ALY",
    "CRESCENT":"CRES","CRES":"CRES","CRESENT":"CRES",
    "CROSSING":"XING","XING":"XING",
}
UNIT_WORDS = {"APT","APARTMENT","UNIT","STE","SUITE","#","FL","FLOOR","BLDG","BUILDING","ROOM","RM"}
POBOX_RE = re.compile(r'\bP(?:\.|\s*)?O(?:\.|\s*)?(?:BOX)?\s*(\d+)\b', re.IGNORECASE)
DIR_WORDS = {"N","S","E","W","NE","NW","SE","SW"}

STATE_ABBR = {
    "ALABAMA":"AL","ALASKA":"AK","ARIZONA":"AZ","ARKANSAS":"AR","CALIFORNIA":"CA","COLORADO":"CO","CONNECTICUT":"CT",
    "DELAWARE":"DE","FLORIDA":"FL","GEORGIA":"GA","HAWAII":"HI","IDAHO":"ID","ILLINOIS":"IL","INDIANA":"IN","IOWA":"IA",
    "KANSAS":"KS","KENTUCKY":"KY","LOUISIANA":"LA","MAINE":"ME","MARYLAND":"MD","MASSACHUSETTS":"MA","MICHIGAN":"MI",
    "MINNESOTA":"MN","MISSISSIPPI":"MS","MISSOURI":"MO","MONTANA":"MT","NEBRASKA":"NE","NEVADA":"NV","NEW HAMPSHIRE":"NH",
    "NEW JERSEY":"NJ","NEW MEXICO":"NM","NEW YORK":"NY","NORTH CAROLINA":"NC","NORTH DAKOTA":"ND","OHIO":"OH","OKLAHOMA":"OK",
    "OREGON":"OR","PENNSYLVANIA":"PA","RHODE ISLAND":"RI","SOUTH CAROLINA":"SC","SOUTH DAKOTA":"SD","TENNESSEE":"TN",
    "TEXAS":"TX","UTAH":"UT","VERMONT":"VT","VIRGINIA":"VA","WASHINGTON":"WA","WEST VIRGINIA":"WV","WISCONSIN":"WI","WYOMING":"WY",
    "JORDAN":"JO"
}

def up(s: Optional[str]) -> str:
    return (s or "").strip().upper()

def strip_accents(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = str(s)
    return ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))

def standardize_name(name: Optional[str]) -> str:
    n = up(strip_accents(name))
    if not n:
        return "UNKNOWN"
    n = re.sub(r'^(MR|MRS|MS|DR|PROF|REV)\s+', '', n)
    n = re.sub(r'\s+', ' ', n)
    return n

def name_core(s: Optional[str]) -> str:
    toks = [t for t in up(strip_accents(s)).split() if t]
    suffixes = {"JR","SR","II","III","IV","V"}
    toks = [t for t in toks if t not in suffixes]
    if toks and len(toks[-1]) == 1:
        toks = toks[:-1]
    return " ".join(toks)

def soundex_en(s: str) -> str:
    s = up(strip_accents(s))
    s = re.sub(r'[^A-Z]', '', s)
    if not s:
        return "Z000"
    first = s[0]
    tail = s[1:]
    code = {'B':'1','F':'1','P':'1','V':'1','C':'2','G':'2','J':'2','K':'2','Q':'2','S':'2','X':'2','Z':'2',
            'D':'3','T':'3','L':'4','M':'5','N':'5','R':'6'}
    out, prev = [], code.get(first, '')
    for ch in tail:
        d = code.get(ch, '')
        if d != prev:
            out.append(d)
        if d:
            prev = d
    digits = ''.join([d for d in out if d])
    return (first + digits + "000")[:4]

def phonetic_phrase(s: str) -> str:
    toks = [t for t in up(strip_accents(s)).split() if t]
    return "-".join(soundex_en(t) for t in toks) if toks else ""

def state_std(s: Optional[str]) -> str:
    s = up(s)
    return STATE_ABBR.get(s, s) or "NA"

def zip5(z: Optional[str]) -> str:
    z = up(str(z or ""))
    m = re.match(r'(\d{5})', z)
    return m.group(1) if m else "00000"

def normalize_street(st1: Optional[str], st2: Optional[str]) -> Dict[str, str]:
    s1, s2 = up(st1), up(st2)
    m = POBOX_RE.search(s1) or POBOX_RE.search(s2)
    if m:
        box = m.group(1)
        return {"kind":"POBOX", "house_or_box":box, "core":"POBOX", "suf":"", "clean":f"PO BOX {box}"}
    if s2:
        s2 = " ".join(t for t in s2.split() if t not in UNIT_WORDS)
    s = s1
    house, rest = "", s
    m = re.match(r'^\s*(\d+)[\s\-]?(.*)$', s)
    if m:
        house, rest = m.group(1), m.group(2)
    toks = [t for t in rest.split() if t and t not in UNIT_WORDS]
    if toks and toks[-1] in DIR_WORDS:
        toks = toks[:-1]
    suf = ""
    if toks:
        last = toks[-1]
        if last in SUFFIX_MAP:
            suf = SUFFIX_MAP[last]
            toks = toks[:-1]
    core = re.sub(r'[^A-Z\s]', ' ', " ".join(toks))
    core = re.sub(r'\s+', ' ', core).strip()
    pretty_parts = [house] if house else []
    if core: pretty_parts.append(core.title())
    if suf:  pretty_parts.append(suf)
    return {"kind":"ADDR", "house_or_box":house, "core":core, "suf":suf, "clean":" ".join(pretty_parts)}

def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest().upper()

# ======= Donor keys (person-level) =======
def donor_key_person(row: Dict[str, Any]) -> str:
    fn = phonetic_phrase(name_core(row.get("contributor_first_name","")))
    ln = phonetic_phrase(name_core(row.get("contributor_last_name","")))
    city = phonetic_phrase(row.get("contributor_city",""))
    st   = state_std(row.get("contributor_state",""))
    z5   = zip5(row.get("contributor_zip",""))
    basis = "|".join(["PERSON", fn, ln, city, st, z5])
    return sha256_hex(basis)[:DONOR_KEY_LEN]

def donor_key_fuzzy_city_only(row: Dict[str, Any]) -> str:
    fn = phonetic_phrase(name_core(row.get("contributor_first_name","")))
    ln = phonetic_phrase(name_core(row.get("contributor_last_name","")))
    city = phonetic_phrase(row.get("contributor_city",""))
    st   = state_std(row.get("contributor_state",""))
    basis = "|".join(["PERSON", fn, ln, city, st])
    return sha256_hex(basis)[:DONOR_KEY_LEN]

# -------- Clean Occupation / Employer --------
def clean_occupation(occ: Optional[str]) -> str:
    if not occ or not isinstance(occ, str): return ""
    clean = up(strip_accents(occ)).strip()
    clean = re.sub(r'[^A-Z0-9\s&/.,-]', '', clean)
    clean = re.sub(r'\s+', ' ', clean)
    if len(clean) < 3 or re.match(r'^[\-.\d]+$', clean): return ""
    spelling_map = {'AATTORNEY':'ATTORNEY','ACCOYNTANT':'ACCOUNTANT','ADMINISTRAOR':'ADMINISTRATOR',
                    'ADVERTING':'ADVERTISING','ADVIDER':'ADVISER','ANESTGESIOLGIST':'ANESTHESIOLOGIST',
                    'AQUISITIONS':'ACQUISITIONS','ATORNEY':'ATTORNEY','ATTOENEY':'ATTORNEY','ATTORNWY':'ATTORNEY',
                    'ATTTORNEY':'ATTORNEY','CONSTRUCTIOM':'CONSTRUCTION','BUSINESS DEVELOPEMENT':'BUSINESS DEVELOPMENT',
                    'BUSINESS IWNER':'BUSINESS OWNER','BUSINESS OWNERR':'BUSINESS OWNER',
                    'CERIFIED PUBLIC ACCOUNTANT':'CERTIFIED PUBLIC ACCOUNTANT','CHARIMAN':'CHAIRMAN',
                    'CHIEF DEVELOPMENT OFFIER':'CHIEF DEVELOPMENT OFFICER','CLINICAL PSYCHOLGIST/CONSULTANT':'CLINICAL PSYCHOLOGIST/CONSULTANT',
                    'CLINICAL PSYCHOLOGIWST':'CLINICAL PSYCHOLOGIST','COMMERCAIL REAL ESTATE EXECUTIVE':'COMMERCIAL REAL ESTATE EXECUTIVE',
                    'COMSULTANT':'CONSULTANT','CONSULTENT':'CONSULTANT','CONSUTANT':'CONSULTANT','PHYCISIAN':'PHYSICIAN','DOCOTOR':'DOCTOR'}
    corrected = spelling_map.get(clean, clean)
    standard_occupations = [
        'ACCOUNTANT','ADMINISTRATOR','ADVISER','ATTORNEY','CEO','CONSULTANT','INVESTOR',
        'MANAGER','PRESIDENT','DIRECTOR','EXECUTIVE','PARTNER','PROJECT MANAGER','CHAIRMAN',
        'SOFTWARE ENGINEER','DEVELOPER','PROGRAMMER','IT PROFESSIONAL','ENGINEER','DATA SCIENTIST',
        'PHYSICIAN','DOCTOR','SURGEON','DENTIST','NURSE','CHIROPRACTOR','HYGIENIST',
        'LAWYER','LAW ENFORCEMENT OFFICER','FIREFIGHTER',
        'TEACHER','PROFESSOR','INSTRUCTOR','ACADEMIC','LIBRARIAN',
        'CONSTRUCTION WORKER','ELECTRICIAN','TRUCK DRIVER','FARMER','LABORER','WELDER','SHEET METAL WORKER',
        'REAL ESTATE AGENT','REAL ESTATE PROFESSIONAL','REALTOR','BROKER',
        'NON-PROFIT EMPLOYEE','INSURANCE PROFESSIONAL','ARTIST','ARCHITECT','AUTHOR','COACH',
        'PSYCHOLOGIST','SCIENTIST','ANALYST','SUPERVISOR','OPERATOR','EXECUTIVE ASSISTANT',
        'RETIRED','HOMEMAKER','STAY-AT-HOME MOM','VETERAN','DISABLED INDIVIDUAL','ENTREPRENEUR','DEVELOPER'
    ]
    if corrected:
        match = process.extractOne(corrected, standard_occupations, scorer=fuzz.WRatio)
        if match and match[1] > 90:
            corrected = match[0]
    return corrected.title() if corrected else ""

def clean_employer(emp: Optional[str]) -> str:
    if not emp or not isinstance(emp, str): return ""
    clean = up(strip_accents(emp)).strip()
    clean = re.sub(r'[^A-Z0-9\s&/.,-]', '', clean)
    clean = re.sub(r'\s+', ' ', clean)
    if len(clean) < 3 or re.match(r'^[\-.\d]+$', clean): return ""
    spelling_map = {
        'SELF':'SELF-EMPLOYED','SELF EMPLOYED':'SELF-EMPLOYED','SELF-EMPOYED':'SELF-EMPLOYED','SELF WMPLOYED':'SELF-EMPLOYED',
        'NOT EMPLOYED':'NOT EMPLOYED','NONE':'NOT EMPLOYED','NO':'NOT EMPLOYED','NA':'NOT EMPLOYED',
        'NOT SPECIFIED':'NOT EMPLOYED','NOT APPLICABLE':'NOT EMPLOYED','RETIRED MILITARY':'RETIRED',
        'COMMUNITY VOLUNTEER':'NOT EMPLOYED','HOMEMAKER':'HOMEMAKER','LIGHTSTONE':'LIGHTSTONE GROUP',
        'LIGHTSTONE GROUP LLC':'LIGHTSTONE GROUP',
    }
    corrected = spelling_map.get(clean, clean)
    standard_employers = [
        'SELF-EMPLOYED','NOT EMPLOYED','RETIRED','HOMEMAKER',
        'GOOGLE','META','MICROSOFT','AMAZON','APPLE','COMCAST','CISCO SYSTEMS',
        'MERCK','LOCKHEED MARTIN',
        'MORGAN STANLEY','GOLDMAN SACHS','JPMORGAN','UBS','PWC','EY','KPMG','DELOITTE',
        'UNIVERSITY OF CALIFORNIA','HARVARD UNIVERSITY','YESHIVA UNIVERSITY',
        'KIRKLAND & ELLIS','LATHAM & WATKINS','ROPES & GRAY','DAVIS POLK & WARDWELL',
        'GREENBERG TRAURIG','SKADDEN ARPS','COOLEY LLP','ARNOLD & PORTER',
        'AIPAC','LIGHTSTONE GROUP'
    ]
    if corrected and corrected not in standard_employers:
        match = process.extractOne(corrected, standard_employers, scorer=fuzz.WRatio)
        if match and match[1] > 90:
            corrected = match[0]
    return corrected.title() if corrected else ""

# -------- Canon mapping --------
_OCC_CANON_TABLE: Dict[str, str] = {
    "LAWYER":"LAWYER","ATTORNEY":"LAWYER","COUNSEL":"LAWYER","GENERAL COUNSEL":"LAWYER","SOLICITOR":"LAWYER",
    "ESQ":"LAWYER","ASSOCIATE ATTORNEY":"LAWYER","PARTNER (LAW)":"LAWYER",
    "PHYSICIAN":"MEDICAL_DOCTOR","DOCTOR":"MEDICAL_DOCTOR","MD":"MEDICAL_DOCTOR",
    "SURGEON":"MEDICAL_DOCTOR","ANESTHESIOLOGIST":"MEDICAL_DOCTOR","DENTIST":"DENTIST","ORTHODONTIST":"DENTIST",
    "SOFTWARE ENGINEER":"SOFTWARE_ENGINEERING","DEVELOPER":"SOFTWARE_ENGINEERING","PROGRAMMER":"SOFTWARE_ENGINEERING",
    "ENGINEER":"ENGINEERING","DATA SCIENTIST":"DATA_SCIENCE","IT PROFESSIONAL":"IT",
    "BANKER":"FINANCE","INVESTOR":"FINANCE","ANALYST":"FINANCE","BROKER":"FINANCE",
    "CEO":"EXECUTIVE","CFO":"EXECUTIVE","COO":"EXECUTIVE","CMO":"EXECUTIVE","PRESIDENT":"EXECUTIVE",
    "MANAGER":"MANAGEMENT","DIRECTOR":"MANAGEMENT","SUPERVISOR":"MANAGEMENT","PROJECT MANAGER":"MANAGEMENT",
    "CHAIRMAN":"EXECUTIVE","EXECUTIVE":"EXECUTIVE","PARTNER":"EXECUTIVE",
    "TEACHER":"EDUCATION","PROFESSOR":"EDUCATION","INSTRUCTOR":"EDUCATION","ACADEMIC":"EDUCATION","SCIENTIST":"RESEARCH","LIBRARIAN":"EDUCATION",
    "LAW ENFORCEMENT OFFICER":"PUBLIC_SAFETY","FIREFIGHTER":"PUBLIC_SAFETY",
    "CONSTRUCTION WORKER":"CONSTRUCTION","ELECTRICIAN":"TRADES","TRUCK DRIVER":"TRADES","FARMER":"AGRICULTURE",
    "LABORER":"TRADES","WELDER":"TRADES","SHEET METAL WORKER":"TRADES",
    "REAL ESTATE AGENT":"REAL_ESTATE","REAL ESTATE PROFESSIONAL":"REAL_ESTATE","REALTOR":"REAL_ESTATE",
    "PSYCHOLOGIST":"HEALTHCARE_OTHER","CHIROPRACTOR":"HEALTHCARE_OTHER","HYGIENIST":"HEALTHCARE_OTHER",
    "RETIRED":"RETIRED","HOMEMAKER":"HOMEMAKER","STAY-AT-HOME MOM":"HOMEMAKER","VETERAN":"VETERAN",
    "DISABLED INDIVIDUAL":"DISABLED","ENTREPRENEUR":"BUSINESS_OWNER","BUSINESS OWNER":"BUSINESS_OWNER",
    "ARTIST":"ARTS","ARCHITECT":"ARCHITECTURE","AUTHOR":"WRITER","WRITER":"WRITER","CONSULTANT":"CONSULTING","ADVISER":"CONSULTING","ADMINISTRATOR":"ADMIN"
}

def occupation_to_canon(occ_clean: str) -> Optional[str]:
    if not occ_clean: return None
    key = occ_clean.upper()
    if key in _OCC_CANON_TABLE:
        return _OCC_CANON_TABLE[key]
    candidates = list(_OCC_CANON_TABLE.keys())
    match = process.extractOne(key, candidates, scorer=fuzz.WRatio)
    if match and match[1] >= 92:
        return _OCC_CANON_TABLE[match[0]]
    if re.search(r'\b(MD|M\.D\.)\b', key): return "MEDICAL_DOCTOR"
    if "LAW" in key or "ATTORNEY" in key or "COUNSEL" in key or "SOLICITOR" in key or "ESQ" in key: return "LAWYER"
    if "SOFTWARE" in key or "DEVELOPER" in key or "PROGRAMMER" in key: return "SOFTWARE_ENGINEERING"
    return None

_EMP_CANON_TABLE: Dict[str, str] = {
    "SELF-EMPLOYED":"SELF_EMPLOYED","NOT EMPLOYED":"UNEMPLOYED","RETIRED":"RETIRED","HOMEMAKER":"HOMEMAKER",
    "KIRKLAND & ELLIS":"LAW_FIRM","LATHAM & WATKINS":"LAW_FIRM","ROPES & GRAY":"LAW_FIRM",
    "DAVIS POLK & WARDWELL":"LAW_FIRM","GREENBERG TRAURIG":"LAW_FIRM","SKADDEN ARPS":"LAW_FIRM",
    "COOLEY LLP":"LAW_FIRM","ARNOLD & PORTER":"LAW_FIRM",
    "GOOGLE":"TECH","META":"TECH","MICROSOFT":"TECH","AMAZON":"TECH","APPLE":"TECH","CISCO SYSTEMS":"TECH","COMCAST":"TECH",
    "MORGAN STANLEY":"FINANCE","GOLDMAN SACHS":"FINANCE","JPMORGAN":"FINANCE","UBS":"FINANCE",
    "PWC":"PROF_SERVICES","EY":"PROF_SERVICES","KPMG":"PROF_SERVICES","DELOITTE":"PROF_SERVICES",
    "UNIVERSITY OF CALIFORNIA":"EDUCATION","HARVARD UNIVERSITY":"EDUCATION","YESHIVA UNIVERSITY":"EDUCATION","AIPAC":"NGO",
    "LIGHTSTONE GROUP":"REAL_ESTATE",
}

def employer_to_canon(emp_clean: str) -> Optional[str]:
    if not emp_clean: return None
    key = emp_clean.upper()
    if key in _EMP_CANON_TABLE:
        return _EMP_CANON_TABLE[key]
    match = process.extractOne(key, list(_EMP_CANON_TABLE.keys()), scorer=fuzz.WRatio)
    if match and match[1] >= 93:
        return _EMP_CANON_TABLE[match[0]]
    if "LLP" in key or "LAW" in key or "ATTORNEY" in key or "LEGAL" in key: return "LAW_FIRM"
    if "UNIV" in key or "SCHOOL" in key or "COLLEGE" in key: return "EDUCATION"
    if "HOSPITAL" in key or "CLINIC" in key or "MEDICAL" in key or "HEALTH" in key: return "HEALTHCARE_ORG"
    if "INC" in key or "LLC" in key or "CORP" in key or "COMPANY" in key: return "PRIVATE_COMPANY"
    return None

# ----------------------------
# SQL templates (متوافقة مع DDL)
# ----------------------------
MASTER_MIN_SQL = text("""
INSERT INTO public.donors_master (
  donor_key, donor_key_fuzzy, contributor_name, contributor_first_name, contributor_last_name,
  contributor_street, contributor_city, contributor_state, contributor_zip,
  contributor_employer, contributor_employer_canon,
  contributor_occupation, contributor_occupation_canon,
  is_individual,
  lat, lng
) VALUES (
  :donor_key, :donor_key_fuzzy, :contributor_name, :contributor_first_name, :contributor_last_name,
  :contributor_street, :contributor_city, :contributor_state, :contributor_zip,
  :contributor_employer, :contributor_employer_canon,
  :contributor_occupation, :contributor_occupation_canon,
  :is_individual,
  :lat, :lng
)
ON CONFLICT (donor_key) DO NOTHING
""")

DETAIL_SQL_WITH_TXN = text("""
INSERT INTO public.donations_detail (
  transaction_id, donor_key,
  contribution_receipt_date, contribution_year, contribution_receipt_amount,
  committee_id, two_year_transaction_period, is_refund
) VALUES (
  :transaction_id, :donor_key,
  :contribution_receipt_date, :contribution_year, :contribution_receipt_amount,
  :committee_id, :two_year_transaction_period, :is_refund
)
ON CONFLICT ON CONSTRAINT donations_detail_txn_uniq DO NOTHING
""")

DETAIL_SQL_FALLBACK = text("""
INSERT INTO public.donations_detail (
  donor_key, contribution_receipt_date, contribution_year, contribution_receipt_amount,
  committee_id, two_year_transaction_period, is_refund
)
SELECT :donor_key, :contribution_receipt_date, :contribution_year, :contribution_receipt_amount,
       :committee_id, :two_year_transaction_period, :is_refund
WHERE NOT EXISTS (
  SELECT 1 FROM public.donations_detail d
  WHERE d.donor_key = :donor_key
    AND d.contribution_receipt_date = :contribution_receipt_date
    AND d.contribution_receipt_amount = :contribution_receipt_amount
    AND COALESCE(d.committee_id,'') = COALESCE(:committee_id,'')
    AND d.two_year_transaction_period = :two_year_transaction_period
)
""")

MASTER_SUMMARY_UPSERT = text("""
INSERT INTO public.donors_master (
  donor_key, donor_key_fuzzy, contributor_name, contributor_first_name, contributor_last_name,
  contributor_street, contributor_city, contributor_state, contributor_zip,
  contributor_employer, contributor_employer_canon,
  contributor_occupation, contributor_occupation_canon,
  is_individual,
  lat, lng,
  total_contributions, contribution_count, contribution_years,
  first_contribution_date, last_contribution_date
) VALUES (
  :donor_key, :donor_key_fuzzy, :contributor_name, :contributor_first_name, :contributor_last_name,
  :contributor_street, :contributor_city, :contributor_state, :contributor_zip,
  :contributor_employer, :contributor_employer_canon,
  :contributor_occupation, :contributor_occupation_canon,
  :is_individual,
  :lat, :lng,
  :total_contributions, :contribution_count, :contribution_years,
  :first_contribution_date, :last_contribution_date
)
ON CONFLICT (donor_key) DO UPDATE SET
  donor_key_fuzzy            = EXCLUDED.donor_key_fuzzy,
  contributor_name           = EXCLUDED.contributor_name,
  contributor_first_name     = EXCLUDED.contributor_first_name,
  contributor_last_name      = EXCLUDED.contributor_last_name,
  contributor_street         = EXCLUDED.contributor_street,
  contributor_city           = EXCLUDED.contributor_city,
  contributor_state          = EXCLUDED.contributor_state,
  contributor_zip            = EXCLUDED.contributor_zip,
  contributor_employer       = EXCLUDED.contributor_employer,
  contributor_employer_canon = EXCLUDED.contributor_employer_canon,
  contributor_occupation     = EXCLUDED.contributor_occupation,
  contributor_occupation_canon = EXCLUDED.contributor_occupation_canon,
  is_individual              = EXCLUDED.is_individual,
  lat                        = COALESCE(EXCLUDED.lat, donors_master.lat),
  lng                        = COALESCE(EXCLUDED.lng, donors_master.lng),
  total_contributions        = EXCLUDED.total_contributions,
  contribution_count         = EXCLUDED.contribution_count,
  contribution_years         = EXCLUDED.contribution_years,
  first_contribution_date    = COALESCE(
      LEAST(donors_master.first_contribution_date, EXCLUDED.first_contribution_date),
      COALESCE(donors_master.first_contribution_date, EXCLUDED.first_contribution_date)
  ),
  last_contribution_date     = COALESCE(
      GREATEST(donors_master.last_contribution_date, EXCLUDED.last_contribution_date),
      COALESCE(donors_master.last_contribution_date, EXCLUDED.last_contribution_date)
  )
""")

# ----------------------------
# ETL
# ----------------------------
def main():
    log.info("Loading source rows…")
    df = pd.read_sql_query("""
        SELECT
          transaction_id,
          contributor_name, contributor_first_name, contributor_last_name,
          contributor_street_1, contributor_street_2,
          contributor_city, contributor_state, contributor_zip,
          contributor_employer, contributor_occupation,
          is_individual,
          committee_id, committee_name,
          contribution_receipt_date, contribution_receipt_amount,
          two_year_transaction_period, lat, lng
        FROM public.contributions
        ORDER BY contribution_receipt_date, contributor_last_name, contributor_first_name
    """, ENGINE)

    # Names
    df['contributor_first_name'] = df['contributor_first_name'].apply(standardize_name)
    df['contributor_last_name']  = df['contributor_last_name'].apply(standardize_name)

    # Geo bits
    df['contributor_city']  = df['contributor_city'].fillna('').str.upper().str.strip()
    df['contributor_state'] = df['contributor_state'].fillna('').str.upper().str.strip()
    df['contributor_zip']   = df['contributor_zip'].apply(zip5)

    df['contributor_street_1'] = df['contributor_street_1'].fillna('').astype(str)
    df['contributor_street_2'] = df['contributor_street_2'].fillna('').astype(str)

    # Money/time
    df['contribution_receipt_amount'] = pd.to_numeric(df['contribution_receipt_amount'], errors='coerce')
    df['contribution_receipt_date']   = pd.to_datetime(df['contribution_receipt_date'], errors='coerce').dt.date
    df['two_year_transaction_period'] = pd.to_numeric(df['two_year_transaction_period'], errors='coerce').fillna(0).astype(int)

    before = len(df)
    df = df[df['contribution_receipt_date'].notna() & df['contribution_receipt_amount'].notna()]
    dropped = before - len(df)
    if dropped:
        log.warning("Dropped %d rows missing date/amount.", dropped)

    # Clean occupation/employer
    df['contributor_occupation'] = df['contributor_occupation'].apply(clean_occupation)
    df['contributor_employer']   = df['contributor_employer'].apply(clean_employer)

    # Canon (للأفراد فقط)
    df['contributor_occupation_canon'] = np.where(
        df['is_individual'].fillna(False),
        df['contributor_occupation'].apply(occupation_to_canon),
        None
    )
    df['contributor_employer_canon'] = np.where(
        df['is_individual'].fillna(False),
        df['contributor_employer'].apply(employer_to_canon),
        None
    )

    # Keys
    log.info("Computing donor keys…")
    df['donor_key']       = df.apply(donor_key_person, axis=1)
    df['donor_key_fuzzy'] = df.apply(donor_key_fuzzy_city_only, axis=1)

    # Pretty street (display only)
    norm_parts = df.apply(lambda r: normalize_street(r['contributor_street_1'], r['contributor_street_2']), axis=1)
    df['contributor_street_clean'] = [p['clean'] for p in norm_parts]

    # Display name
    composed_name = (df['contributor_first_name'].str.title() + ' ' + df['contributor_last_name'].str.title()).str.strip()
    df['contributor_name_final'] = np.where(
        df['contributor_name'].fillna('').str.strip() != '',
        df['contributor_name'],
        composed_name
    )

    # ---- Upsert minimal donors_master (FK-safe)
    log.info("Upserting minimal master rows…")
    masters_min = (
        df[['donor_key','donor_key_fuzzy','contributor_name_final','contributor_first_name','contributor_last_name',
            'contributor_street_clean','contributor_city','contributor_state','contributor_zip',
            'contributor_employer','contributor_employer_canon',
            'contributor_occupation','contributor_occupation_canon',
            'is_individual','lat','lng']]
        .rename(columns={
            'contributor_name_final':'contributor_name',
            'contributor_street_clean':'contributor_street'
        })
        .drop_duplicates(subset=['donor_key'])
        .replace({np.nan: None})
    )

    with ENGINE.begin() as conn:
        records = masters_min.to_dict('records')
        for i in range(0, len(records), CHUNK_SIZE):
            conn.execute(MASTER_MIN_SQL, records[i:i+CHUNK_SIZE])

    # ---- Insert donations_detail
    log.info("Inserting detail rows…")
    df['transaction_id'] = df.get('transaction_id').apply(
        lambda x: (str(x).strip() if x is not None and str(x).strip() != '' else None)
    )
    df['contribution_year'] = pd.to_datetime(df['contribution_receipt_date'], errors='coerce').astype('datetime64[ns]').dt.year.astype('Int64')
    df['is_refund'] = (df['contribution_receipt_amount'] < 0).fillna(False)

    detail_payload = (
        df[['transaction_id','donor_key','contribution_receipt_date','contribution_year','contribution_receipt_amount',
            'committee_id','two_year_transaction_period','is_refund']]
        .replace({np.nan: None})
        .to_dict('records')
    )

    rows_with_txn = [r for r in detail_payload if r['transaction_id'] is not None]
    rows_no_txn   = [r for r in detail_payload if r['transaction_id'] is None]

    with ENGINE.begin() as conn:
        if rows_with_txn:
            for i in range(0, len(rows_with_txn), CHUNK_SIZE):
                conn.execute(DETAIL_SQL_WITH_TXN, rows_with_txn[i:i+CHUNK_SIZE])
        if rows_no_txn:
            for i in range(0, len(rows_no_txn), CHUNK_SIZE):
                conn.execute(DETAIL_SQL_FALLBACK, rows_no_txn[i:i+CHUNK_SIZE])

    # ---- Aggregate -> master snapshot
    log.info("Aggregating and updating master…")
    donor_keys = masters_min['donor_key'].tolist()
    KEYS_PARAM = bindparam("keys", expanding=True)

    agg_sql = text("""
        SELECT
          d.donor_key,
          SUM(d.contribution_receipt_amount)                                     AS total_amt,
          COUNT(*)                                                               AS txn_count,
          COUNT(*) FILTER (WHERE NOT d.is_refund)                                AS donation_count,
          MIN(d.contribution_receipt_date)                                       AS dmin,
          MAX(d.contribution_receipt_date)                                       AS dmax,
          ARRAY(
            SELECT DISTINCT d2.contribution_year
            FROM public.donations_detail d2
            WHERE d2.donor_key = d.donor_key
            ORDER BY contribution_year
          )                                                                      AS years
        FROM public.donations_detail d
        WHERE d.donor_key IN :keys
        GROUP BY d.donor_key
    """).bindparams(KEYS_PARAM)

    with ENGINE.begin() as conn:
        res = conn.execute(agg_sql, {"keys": donor_keys})
        agg_rows = res.mappings().all()

    latest_df = (
        df.sort_values('contribution_receipt_date')
          .groupby('donor_key', as_index=False)
          .tail(1)
    )
    latest_map = {
        r['donor_key']: {
            "donor_key_fuzzy": r['donor_key_fuzzy'],
            "contributor_name": r['contributor_name_final'],
            "contributor_first_name": r['contributor_first_name'],
            "contributor_last_name": r['contributor_last_name'],
            "contributor_street": normalize_street(r['contributor_street_1'], r['contributor_street_2'])['clean'],
            "contributor_city": r['contributor_city'],
            "contributor_state": state_std(r['contributor_state']),
            "contributor_zip": zip5(r['contributor_zip']),
            "contributor_employer": r['contributor_employer'],
            "contributor_employer_canon": r['contributor_employer_canon'],
            "contributor_occupation": r['contributor_occupation'],
            "contributor_occupation_canon": r['contributor_occupation_canon'],
            "is_individual": bool(r['is_individual']),
            "lat": r['lat'],
            "lng": r['lng'],
        }
        for _, r in latest_df.iterrows()
    }

    upserts: List[Dict[str, Any]] = []
    for row in agg_rows:
        dk = row['donor_key']
        ident = latest_map.get(dk, {})
        upserts.append({
            "donor_key": dk,
            "donor_key_fuzzy": ident.get("donor_key_fuzzy"),
            "contributor_name": ident.get("contributor_name"),
            "contributor_first_name": ident.get("contributor_first_name"),
            "contributor_last_name": ident.get("contributor_last_name"),
            "contributor_street": ident.get("contributor_street"),
            "contributor_city": ident.get("contributor_city"),
            "contributor_state": ident.get("contributor_state"),
            "contributor_zip": ident.get("contributor_zip"),
            "contributor_employer": ident.get("contributor_employer"),
            "contributor_employer_canon": ident.get("contributor_employer_canon"),
            "contributor_occupation": ident.get("contributor_occupation"),
            "contributor_occupation_canon": ident.get("contributor_occupation_canon"),
            "is_individual": ident.get("is_individual"),
            "lat": ident.get("lat"),
            "lng": ident.get("lng"),
            "total_contributions": float(row['total_amt'] or 0),
            "contribution_count": int(row['donation_count'] or 0),
            "contribution_years": row['years'] if row['years'] is not None else [],
            "first_contribution_date": row['dmin'],
            "last_contribution_date": row['dmax'],
        })

    with ENGINE.begin() as conn:
        for i in range(0, len(upserts), CHUNK_SIZE):
            conn.execute(MASTER_SUMMARY_UPSERT, upserts[i:i+CHUNK_SIZE])

    log.info("Done. Master donors upserted: %d", len(upserts))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception("ETL failed: %s", e)
        sys.exit(1)