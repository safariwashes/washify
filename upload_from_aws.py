"""
pos_ingest_multi_tenant.py

Multi-tenant version of the kiosk Transaction log ingester.

What this script does
- Pulls the latest Transaction*.txt file(s) from S3 (or reads local INPUT_PATH for testing)
- Parses sessions in the file and extracts POS fields (bill, timestamps, plate, customer, package, add-ons, totals, etc.)
- Resolves tenant_id + location_id from database tables (no hard-coded FRA/NSH mapping)
- Resolves wash_type using DB-driven rules (no hard-coded wash-type map)
- Upserts into public.pos

Key DB tables used (created automatically if missing):
1) tenants (existing in your schema)
   - expected columns: tenant_id (uuid), tenant_slug (text) OR slug-like column
2) locations (existing in your schema)
   - expected columns: location_id (uuid), tenant_id (uuid), location_code (text) (or similar)
3) kiosk_file_rules (created by this script)
   - maps filename patterns -> location_id + lane_no (+ optional location_code label)
4) wash_type_rules (created by this script)
   - maps wash_package_name patterns -> normalized wash_type (Basic/Good/Better/Best/Super)

Environment
- DATABASE_URL (recommended) OR DB_NAME/DB_USER/DB_PASS/DB_HOST/DB_PORT/DB_SSLMODE
- TENANT_ID (uuid) OR TENANT_SLUG (text)  [TENANT_ID wins]
- AWS_REGION, S3_BUCKET, S3_PREFIX, FILE_MATCH
- INPUT_PATH (optional): local file or directory for testing
- DELETE_S3_AFTER_SUCCESS (default: "true")
"""

import os
import re
import json
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any, Optional

import psycopg2
import psycopg2.extras

# AWS
import boto3
from botocore.config import Config

# timezone (Python 3.9+ built-in)
from zoneinfo import ZoneInfo

# Optional .env support
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


# ===================== CONFIG =====================
AWS_REGION  = (os.getenv("AWS_REGION") or "us-east-2").strip()
S3_BUCKET   = (os.getenv("S3_BUCKET") or "").strip()
S3_PREFIX   = (os.getenv("S3_PREFIX") or "kiosks/").strip()
FILE_MATCH  = (os.getenv("FILE_MATCH") or "Transaction").strip()

# Local override for testing (file or directory)
INPUT_PATH = os.getenv("INPUT_PATH")

DELETE_S3_AFTER_SUCCESS = (os.getenv("DELETE_S3_AFTER_SUCCESS", "true").strip().lower() in ("1", "true", "yes", "y"))

# Tail-ingest safety buffer (lines before the last known bill)
BUFFER_LINES = int(os.getenv("BUFFER_LINES", "2000"))

# Byte-buffer when tail-reading from S3/local using last byte offset (keeps session context)
BUFFER_BYTES = int(os.getenv("BUFFER_BYTES", "200000"))  # ~200KB

# S3 folder layout support
# Two common layouts:
#   1) plain:        {S3_PREFIX}/.../Transaction*.txt
#   2) partitioned:  {S3_PREFIX}/tenant=<tenant_value>/address=<addr>/lane=<n>/Transaction*.txt
#
# Configure via:
#   S3_LAYOUT=plain|partitioned   (default: plain)
#   S3_TENANT_VALUE=<value>       (only used when S3_LAYOUT=partitioned; example: safariexpresswash)
S3_LAYOUT = (os.getenv("S3_LAYOUT") or "plain").strip().lower()
S3_TENANT_VALUE = (os.getenv("S3_TENANT_VALUE") or "").strip()
WORKER_MODE = (os.getenv("WORKER_MODE") or "multi").strip().lower()  # multi|single
TENANT_FILTER_RAW = (os.getenv("TENANT_FILTER") or "").strip()
TENANT_FILTER = set([t.strip().lower() for t in TENANT_FILTER_RAW.split(",") if t.strip()]) or None

# Multi-tenant identity
TENANT_ID_ENV = (os.getenv("TENANT_ID") or "").strip()
TENANT_SLUG   = (os.getenv("TENANT_SLUG") or "").strip().lower()

# DB connection
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()


# ===================== TIME HELPERS =====================
def now_cst() -> datetime:
    return datetime.now(ZoneInfo("America/Chicago"))

def now_cst_date():
    return now_cst().date()

def now_cst_time():
    return now_cst().time()


# ===================== DB HELPERS =====================
def get_conn():
    if DATABASE_URL:
        return psycopg2.connect(DATABASE_URL, sslmode=os.getenv("DB_SSLMODE", "require"))
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432"),
        sslmode=os.getenv("DB_SSLMODE", "require"),
    )

def db_fingerprint(conn) -> str:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_user, inet_server_addr()::text, inet_server_port()::text")
            db, user, addr, port = cur.fetchone()
            return f"db={db} user={user} host={addr}:{port}"
    except Exception as e:
        return f"(db fingerprint failed: {e})"

def location_exists(conn, location_id: str) -> bool:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM locations WHERE location_id = %s LIMIT 1", (location_id,))
            return cur.fetchone() is not None
    except Exception:
        return False



def ensure_rule_tables(conn) -> None:
    """
    Create rule tables used for multi-tenant mapping, if they don't exist.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS kiosk_file_rules (
      rule_id            BIGSERIAL PRIMARY KEY,
      tenant_id          UUID NOT NULL,
      location_id        UUID NOT NULL,
      filename_contains  TEXT NOT NULL,
      lane_no            INTEGER,
      location_code      TEXT,
      priority           INTEGER NOT NULL DEFAULT 100,
      enabled            BOOLEAN NOT NULL DEFAULT TRUE,
      created_on         DATE NOT NULL DEFAULT CURRENT_DATE,
      created_at         TIME NOT NULL DEFAULT CURRENT_TIME
    );
    CREATE INDEX IF NOT EXISTS idx_kiosk_file_rules_tenant_enabled_priority
      ON kiosk_file_rules (tenant_id, enabled, priority);

    CREATE TABLE IF NOT EXISTS wash_type_rules (
      rule_id       BIGSERIAL PRIMARY KEY,
      tenant_id     UUID NOT NULL,
      match_type    TEXT NOT NULL DEFAULT 'contains', -- contains|regex|exact
      pattern       TEXT NOT NULL,
      wash_type     TEXT NOT NULL, -- must match your pos_wash_type_check
      priority      INTEGER NOT NULL DEFAULT 100,
      enabled       BOOLEAN NOT NULL DEFAULT TRUE,
      created_on    DATE NOT NULL DEFAULT CURRENT_DATE,
      created_at    TIME NOT NULL DEFAULT CURRENT_TIME,
      CONSTRAINT wash_type_rules_match_type_chk CHECK (match_type IN ('contains','regex','exact')),
      CONSTRAINT wash_type_rules_wash_type_chk CHECK (wash_type IN ('Basic','Good','Better','Best','Super'))
    );
    CREATE INDEX IF NOT EXISTS idx_wash_type_rules_tenant_enabled_priority
      ON wash_type_rules (tenant_id, enabled, priority);

    CREATE TABLE IF NOT EXISTS pos_ingest_offsets (
      tenant_id      UUID NOT NULL,
      location_id    UUID NOT NULL,
      lane_no        INTEGER NOT NULL,
      source_file    TEXT NOT NULL,
      s3_key         TEXT,
      s3_etag        TEXT,
      last_modified  TIMESTAMPTZ,
      last_size      BIGINT,
      last_bill      BIGINT,
      last_byte_offset BIGINT NOT NULL DEFAULT 0,
      updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (tenant_id, location_id, lane_no, source_file)
    );


    CREATE TABLE IF NOT EXISTS tenant_s3_map (
      tenant_id        UUID PRIMARY KEY,
      s3_tenant_value  TEXT NOT NULL UNIQUE,
      enabled          BOOLEAN NOT NULL DEFAULT TRUE,
      created_on       DATE NOT NULL DEFAULT CURRENT_DATE,
      created_at       TIME NOT NULL DEFAULT CURRENT_TIME
    );
    CREATE INDEX IF NOT EXISTS idx_tenant_s3_map_enabled
      ON tenant_s3_map (enabled);
    CREATE INDEX IF NOT EXISTS idx_pos_ingest_offsets_updated
      ON pos_ingest_offsets (tenant_id, updated_at DESC);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def resolve_tenant_id(conn) -> str:
    """
    Resolve tenant_id from env.
    TENANT_ID takes precedence; otherwise look up by TENANT_SLUG.
    """
    if TENANT_ID_ENV:
        return TENANT_ID_ENV

    if not TENANT_SLUG:
        raise RuntimeError("Missing TENANT_ID or TENANT_SLUG environment variable.")

    # Try common slug column names
    with conn.cursor() as cur:
        # 1) tenant_slug
        cur.execute(
            """
            SELECT tenant_id
            FROM tenants
            WHERE LOWER(COALESCE(tenant_slug, '')) = %s
            LIMIT 1
            """,
            (TENANT_SLUG,),
        )
        row = cur.fetchone()
        if row:
            return str(row[0])

        # 2) slug
        cur.execute(
            """
            SELECT tenant_id
            FROM tenants
            WHERE LOWER(COALESCE(slug, '')) = %s
            LIMIT 1
            """,
            (TENANT_SLUG,),
        )
        row = cur.fetchone()
        if row:
            return str(row[0])

    raise RuntimeError(f"Could not resolve tenant_id for TENANT_SLUG='{TENANT_SLUG}'")

def resolve_tenant_slug(conn, tenant_id: str) -> Optional[str]:
    """Resolve tenant_slug (or slug) from tenants table for S3 folder use."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT NULLIF(TRIM(COALESCE(tenant_slug, slug, '')), '')
            FROM tenants
            WHERE tenant_id = %s
            LIMIT 1
            """,
            (tenant_id,),
        )
        row = cur.fetchone()
        return (row[0] or None) if row else None



def load_kiosk_file_rules(conn, tenant_id: str) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT rule_id, filename_contains, location_id, lane_no, location_code, priority
            FROM kiosk_file_rules
            WHERE tenant_id = %s AND enabled = TRUE
            ORDER BY priority ASC, rule_id ASC
            """,
            (tenant_id,),
        )
        return list(cur.fetchall())


def load_wash_type_rules(conn, tenant_id: str) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT rule_id, match_type, pattern, wash_type, priority
            FROM wash_type_rules
            WHERE tenant_id = %s AND enabled = TRUE
            ORDER BY priority ASC, rule_id ASC
            """,
            (tenant_id,),
        )
        rules = list(cur.fetchall())

    # Precompile regex rules
    for r in rules:
        if r["match_type"] == "regex":
            try:
                r["_re"] = re.compile(r["pattern"], re.IGNORECASE)
            except Exception:
                r["_re"] = None
    return rules

def load_ingest_offset(conn, tenant_id: str, location_id: str, lane_no: int, source_file: str) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT tenant_id, location_id, lane_no, source_file, s3_key, s3_etag, last_modified, last_size,
                   last_bill, last_byte_offset, updated_at
            FROM pos_ingest_offsets
            WHERE tenant_id=%s AND location_id=%s AND lane_no=%s AND source_file=%s
            """,
            (tenant_id, location_id, int(lane_no), source_file),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def save_ingest_offset(
    conn,
    tenant_id: str,
    location_id: str,
    lane_no: int,
    source_file: str,
    *,
    last_byte_offset: int,
    last_bill: Optional[int],
    s3_key: Optional[str] = None,
    s3_etag: Optional[str] = None,
    last_modified: Optional[datetime] = None,
    last_size: Optional[int] = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pos_ingest_offsets
              (tenant_id, location_id, lane_no, source_file, s3_key, s3_etag, last_modified, last_size, last_bill, last_byte_offset, updated_at)
            VALUES
              (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (tenant_id, location_id, lane_no, source_file) DO UPDATE SET
              s3_key = COALESCE(EXCLUDED.s3_key, pos_ingest_offsets.s3_key),
              s3_etag = COALESCE(EXCLUDED.s3_etag, pos_ingest_offsets.s3_etag),
              last_modified = COALESCE(EXCLUDED.last_modified, pos_ingest_offsets.last_modified),
              last_size = COALESCE(EXCLUDED.last_size, pos_ingest_offsets.last_size),
              last_bill = COALESCE(EXCLUDED.last_bill, pos_ingest_offsets.last_bill),
              last_byte_offset = GREATEST(pos_ingest_offsets.last_byte_offset, EXCLUDED.last_byte_offset),
              updated_at = NOW();
            """,
            (tenant_id, location_id, int(lane_no), source_file, s3_key, s3_etag, last_modified, last_size, last_bill, int(last_byte_offset)),
        )
    conn.commit()



def infer_location_and_lane_from_rules(key_hint: str, rules: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    """
    Returns (location_id, lane_no, location_code_label) by matching filename_contains against a key hint.

    key_hint can be either:
      - the basename filename (plain layout), OR
      - the full S3 key (partitioned layout like .../address=.../lane=.../Transaction_...txt)

    We match using simple case-insensitive substring search against key_hint.
    """
    fn = (key_hint or "").lower()
    for r in rules:
        if (r.get("filename_contains") or "").lower() in fn:
            return (str(r["location_id"]), r.get("lane_no"), r.get("location_code"))
    return (None, None, None)


def lookup_location_code(conn, location_id: str) -> Optional[str]:
    """
    Reads the human label/location code from locations table if present.
    """
    with conn.cursor() as cur:
        # Try common column names
        cur.execute(
            """
            SELECT
              COALESCE(location_code, code, location, name)::text
            FROM locations
            WHERE location_id = %s
            LIMIT 1
            """,
            (location_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None


# ===================== PARSING REGEX =====================
TS_RE = re.compile(r"^\s*(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s+[AP]M)\s*,\s*")

INVOICE_INLINE_PAY_RE = re.compile(r"InvoiceID\s+(\d+)\s+Payment Type\s+([A-Za-z]+)", re.IGNORECASE)
PROCEED_INVOICE_RE = re.compile(r"ProceedToCarWashViewModel.*?InvoiceID\s+(\d+)", re.IGNORECASE)
DO_TXN_RE = re.compile(r"DoTransactionAfterDispatcher\s+(\d+)", re.IGNORECASE)
INVOICE_ANY_RE = re.compile(r"InvoiceID\s+(\d+)", re.IGNORECASE)
INVOICE_FROM_AWS_RE = re.compile(r"InvoiceId\s+(\d+)", re.IGNORECASE)

INVOICE_SEARCH_RES = [
    INVOICE_INLINE_PAY_RE,
    DO_TXN_RE,
    PROCEED_INVOICE_RE,
    INVOICE_ANY_RE,
    INVOICE_FROM_AWS_RE,
]

WASH_PKG_RE = re.compile(r"Wash Package\s+(\d+)\s+with Name\s+(.+)$", re.IGNORECASE)
PAYMENT_TYPE_RE = re.compile(r"Payment Type\s+([A-Za-z]+)", re.IGNORECASE)
AWS_FILE_RE = re.compile(r"Aws File Name\s+(.+)$", re.IGNORECASE)
LICENSE_PLATE_RE = re.compile(r"(?:License Plate|LICENSE PLATE)\s+([A-Z0-9]+)", re.IGNORECASE)
CUSTOMER_NAME_RE = re.compile(r"Customer Name\s+([^,]+)", re.IGNORECASE)

UNLIMITED_NEW_RE   = re.compile(r"NEW CUSTOMER\s*->", re.IGNORECASE)
UNLIMITED_RECUR_RE = re.compile(r"RECURRING\s*->", re.IGNORECASE)

TIP_AMOUNT_RE = re.compile(r"\bTip\s*\$?\s*([0-9]+(?:\.[0-9]{1,2})?)\b", re.IGNORECASE)

DISCOUNT_BOTH_RE   = re.compile(r"Discount[:\s]+([A-Za-z0-9._-]+)\s+\$?([0-9]+(?:\.[0-9]{1,2})?)", re.IGNORECASE)
DISCOUNT_CODE_RE   = re.compile(r"Discount(?:\s+Code)?[:\s]+([A-Za-z][A-Za-z0-9._-]*)", re.IGNORECASE)
DISCOUNT_AMOUNT_RE = re.compile(r"Discount(?:\s+Amount)?[:\s]+\$?([0-9]+(?:\.[0-9]{1,2})?)", re.IGNORECASE)

TAX_RE   = re.compile(r"Tax[:\s]+\$?([0-9]+(?:\.[0-9]{1,2})?)\b", re.IGNORECASE)
TOTAL_RE = re.compile(r"Total[:\s]+\$?([0-9]+(?:\.[0-9]{1,2})?)\b", re.IGNORECASE)


def parse_ts(line: str) -> Tuple[Optional[datetime], str]:
    """
    Extracts the leading timestamp (if present) and returns (datetime, rest_of_line).
    """
    m = TS_RE.match(line)
    if not m:
        return None, line
    ts = datetime.strptime(m.group(1), "%m/%d/%Y %I:%M:%S %p")
    return ts, line[m.end():]


def find_last_bill_index(lines: List[str], last_bill: Optional[int]) -> Optional[int]:
    """Search backwards in the lines to find the last occurrence of last_bill. Returns index or None."""
    if last_bill is None:
        return None
    target = str(last_bill)
    for idx in range(len(lines) - 1, -1, -1):
        raw = lines[idx].strip()
        if not raw:
            continue
        _, content = parse_ts(raw)
        for regex in INVOICE_SEARCH_RES:
            mm = regex.search(content)
            if mm and mm.group(1) == target:
                return idx
    return None


# ===================== WASH TYPE RESOLUTION (DB RULES) =====================
def normalize_ws_name(s: str) -> str:
    return re.sub(r"\s{2,}", " ", (s or "").strip())


def map_wash_type_from_rules(pkg_name: Optional[str], rules: List[Dict[str, Any]]) -> Optional[str]:
    """
    Convert wash_package_name into normalized wash_type using DB rules.
    Rule evaluation order: priority ASC, rule_id ASC (already sorted by query).
    match_type:
      - contains: case-insensitive substring match
      - exact: case-insensitive full-string match
      - regex: pattern compiled with IGNORECASE
    """
    if not pkg_name:
        return None

    name = normalize_ws_name(pkg_name)
    low = name.lower()

    for r in rules:
        mtype = r["match_type"]
        pat = r["pattern"]
        if mtype == "contains":
            if pat.lower() in low:
                return r["wash_type"]
        elif mtype == "exact":
            if pat.lower() == low:
                return r["wash_type"]
        elif mtype == "regex":
            rx = r.get("_re")
            if rx and rx.search(name):
                return r["wash_type"]

    return None


# ===================== PARSER =====================
def parse_file(
    path: Path,
    wash_type_rules: List[Dict[str, Any]],
    start_index: int = 0,
    preloaded_lines: Optional[List[str]] = None,
    location_label: Optional[str] = None,
    lane_no: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Parse a single Transaction log file into POS rows.

    Key behaviors (important for SIGNUP flows):
      - We DO NOT end a session on ResetTransaction (kiosk can reset mid-flow and still continue the same signup).
      - We accept Wash Package lines from ANY screen (PaymentScreenViewModel / ProceedToCarWashViewModel / etc.).
      - If a later non-zero InvoiceID appears, we update to the latest invoice within the session.
      - Tips are detected from "Wash Package ... Tip $X" and added to tip_amount (and not treated as wash type).
    """
    sessions: List[Dict[str, Any]] = []
    sess: Optional[Dict[str, Any]] = None

    def new_session(ts: Optional[datetime]):
        return {
            "invoice": None,
            "first_ts": ts,
            "last_ts": ts,
            "customer_name": None,
            "license_plate": None,

            # main wash package (what becomes wash_type)
            "wash_package_id": None,
            "wash_package_name": None,

            # payment / receipt / image
            "payment_type": None,
            "payment_type_ts": None,
            "image_path": None,

            # unlimited classification
            "unlimited_type": None,
            "unlimited_ts": None,
            "saw_unlimited_signature": False,
            "saw_creditcard_unlimited": False,
            "saw_unlimited_pkg_name": False,

            # add-ons / tips / discounts / totals
            "addon_map": {},  # pkg_id -> {name, ts}
            "tip_amount": 0.0,
            "tip_ts": None,

            "discount_code": None,
            "discount_amount": None,
            "tax": None,
            "total": None,
        }

    def has_meaningful_data(s: Dict[str, Any]) -> bool:
        return bool(
            (s.get("invoice") and s["invoice"] != "0")
            or s.get("wash_package_name")
            or s.get("license_plate")
            or s.get("customer_name")
            or s.get("image_path")
            or s.get("payment_type")
            or s.get("saw_unlimited_signature")
            or s.get("saw_creditcard_unlimited")
        )

    def end_session(ts: Optional[datetime]):
        nonlocal sess
        if not sess:
            return
        if ts and (not sess["last_ts"] or ts > sess["last_ts"]):
            sess["last_ts"] = ts
        sessions.append(sess)
        sess = None

    # Load lines
    if preloaded_lines is not None:
        lines = preloaded_lines
    else:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()

    for raw in lines[start_index:]:
        line = raw.strip()
        if not line:
            continue

        ts, content = parse_ts(line)

        if sess is None:
            sess = new_session(ts)

        if ts:
            if not sess["first_ts"] or ts < sess["first_ts"]:
                sess["first_ts"] = ts
            if not sess["last_ts"] or ts > sess["last_ts"]:
                sess["last_ts"] = ts

        # ----- Explicit new-transaction start marker -----
        if "AdjustScreenViewModel" in content and "Adjust screen started" in content:
            if sess and has_meaningful_data(sess):
                end_session(ts)
            if sess is None:
                sess = new_session(ts)

        # ----- Invoice detection (keep the latest non-zero invoice) -----
        for regex in INVOICE_SEARCH_RES:
            m = regex.search(content)
            if m:
                inv = m.group(1)
                if inv and inv != "0":
                    sess["invoice"] = inv

        # ----- Unlimited markers -----
        if UNLIMITED_NEW_RE.search(content):
            if not sess["unlimited_type"]:
                sess["unlimited_type"] = "NEW"
            sess["unlimited_ts"] = ts or sess["unlimited_ts"]

        if UNLIMITED_RECUR_RE.search(content):
            sess["unlimited_type"] = "RECURRING"
            sess["unlimited_ts"] = ts or sess["unlimited_ts"]

        if "UnlimitedCustomerSignatureViewModel" in content:
            sess["saw_unlimited_signature"] = True
        if "CreditCardUnlimitedViewModel" in content:
            sess["saw_creditcard_unlimited"] = True

        # ----- Customer -----
        m = CUSTOMER_NAME_RE.search(content)
        if m and not sess["customer_name"]:
            sess["customer_name"] = normalize_ws_name(m.group(1))

        # ----- Plate -----
        m = LICENSE_PLATE_RE.search(content)
        if m and not sess["license_plate"]:
            sess["license_plate"] = m.group(1).strip().upper()

        # ----- Wash package / Add-ons / Tip handling -----
        m = WASH_PKG_RE.search(content)
        if m:
            pkg_id = m.group(1).strip()
            pkg_name = normalize_ws_name(m.group(2).rstrip("."))

            # Tip often shows up as "Wash Package <id> with Name Tip $10"
            tip_m = TIP_AMOUNT_RE.search(pkg_name) or TIP_AMOUNT_RE.search(content)
            is_tip = bool(tip_m) or pkg_name.lower().startswith("tip")
            if is_tip:
                try:
                    amt = float(tip_m.group(1)) if tip_m else 0.0
                    if amt > 0:
                        sess["tip_amount"] = float(sess["tip_amount"] or 0) + amt
                        sess["tip_ts"] = ts or sess["tip_ts"]
                except Exception:
                    pass
            else:
                mapped = map_wash_type_from_rules(pkg_name, wash_type_rules)
                is_main_candidate = bool(mapped) or ("unlimited" in pkg_name.lower())

                if "unlimited" in pkg_name.lower():
                    sess["saw_unlimited_pkg_name"] = True

                if is_main_candidate:
                    current_mapped = map_wash_type_from_rules(sess.get("wash_package_name"), wash_type_rules)
                    if (sess["wash_package_name"] is None) or (current_mapped is None and mapped is not None):
                        sess["wash_package_id"] = pkg_id
                        sess["wash_package_name"] = pkg_name
                else:
                    if "SelectOptionalServiceBlock" in content:
                        if pkg_id and pkg_id != str(sess.get("wash_package_id") or ""):
                            sess["addon_map"][pkg_id] = {"name": pkg_name, "ts": ts}

        # ----- Payment Type -----
        if "SaveTransactions" in content and "SaveTransaction" in content:
            m = PAYMENT_TYPE_RE.search(content)
            if m:
                sess["payment_type"] = m.group(1).strip()
                sess["payment_type_ts"] = ts

        # ----- Image Path -----
        m = AWS_FILE_RE.search(content)
        if m and not sess["image_path"]:
            sess["image_path"] = m.group(1).strip()

        # ----- Discount / Tax / Total -----
        m = DISCOUNT_BOTH_RE.search(content)
        if m:
            sess["discount_code"] = m.group(1)
            try:
                sess["discount_amount"] = float(m.group(2))
            except Exception:
                pass

        m = DISCOUNT_CODE_RE.search(content)
        if m:
            sess["discount_code"] = m.group(1)

        m = DISCOUNT_AMOUNT_RE.search(content)
        if m:
            try:
                sess["discount_amount"] = float(m.group(1))
            except Exception:
                pass

        m = TAX_RE.search(content)
        if m:
            try:
                sess["tax"] = float(m.group(1))
            except Exception:
                pass

        m = TOTAL_RE.search(content)
        if m:
            try:
                sess["total"] = float(m.group(1))
            except Exception:
                pass

        # ----- End-of-transaction marker -----
        if "ProceedToCarWashViewModel" in content and "ReturnToMainScreen" in content:
            end_session(ts)

    # flush trailing
    if sess and has_meaningful_data(sess):
        sessions.append(sess)

    # ----- Convert sessions → DB rows -----
    rows: List[Dict[str, Any]] = []
    for s in sessions:
        if not s.get("invoice") or s["invoice"] == "0":
            continue

        sorted_addons = sorted(s["addon_map"].values(), key=lambda x: (x["ts"] or datetime.min))
        addons_text = "; ".join([a["name"] for a in sorted_addons]) if sorted_addons else None

        strong_signup = bool(s["saw_unlimited_signature"] or s["saw_creditcard_unlimited"])
        strong_wash = bool(s["saw_unlimited_pkg_name"])

        if strong_signup:
            invoice_kind = "SIGNUP"
            is_unl = True
            if not s["unlimited_type"]:
                s["unlimited_type"] = "NEW"
        elif strong_wash:
            invoice_kind = "WASH"
            is_unl = True
        else:
            invoice_kind = "NORMAL"
            is_unl = False

        rows.append(
            {
                "bill": int(s["invoice"]),
                "wash_ts_first": s["first_ts"],
                "wash_ts_last": s["last_ts"],
                "license_plate": s["license_plate"],
                "customer_name": s["customer_name"],
                "wash_package_id": int(s["wash_package_id"]) if s["wash_package_id"] else None,
                "wash_package_name": s["wash_package_name"],
                "wash_type": map_wash_type_from_rules(s["wash_package_name"], wash_type_rules),
                "payment_type": s["payment_type"],
                "image_path": s["image_path"],
                "is_unlimited": is_unl,
                "unlimited_type": s["unlimited_type"] if is_unl else None,
                "addons": addons_text,
                "tip_amount": float(s["tip_amount"] or 0),
                "discount_code": s["discount_code"],
                "discount_amount": s["discount_amount"],
                "tax": s["tax"],
                "total": s["total"],
                "location": location_label,
                "lane_no": lane_no,
                "source_file": path.name,
                "created_on": now_cst_date(),
                "created_at": now_cst_time(),
                "invoice_kind": invoice_kind,
            }
        )

    return rows


# ===================== UPSERT INTO POS =====================
UPSERT_SQL = """
INSERT INTO pos (
  bill, wash_ts_first, wash_ts_last, license_plate, customer_name,
  wash_package_id, wash_package_name, wash_type, payment_type, image_path,
  is_unlimited, unlimited_type, addons, tip_amount,
  discount_code, discount_amount, tax, total,
  location, lane_no, source_file, created_on, created_at, invoice_kind,
  tenant_id, location_id
) VALUES (
  %(bill)s, %(wash_ts_first)s, %(wash_ts_last)s, %(license_plate)s, %(customer_name)s,
  %(wash_package_id)s, %(wash_package_name)s, %(wash_type)s, %(payment_type)s, %(image_path)s,
  %(is_unlimited)s, %(unlimited_type)s, %(addons)s, %(tip_amount)s,
  %(discount_code)s, %(discount_amount)s, %(tax)s, %(total)s,
  %(location)s, %(lane_no)s, %(source_file)s, %(created_on)s, %(created_at)s, %(invoice_kind)s,
  %(tenant_id)s, %(location_id)s
)
ON CONFLICT (tenant_id, location_id, bill, wash_date) DO UPDATE SET
  wash_ts_first = COALESCE(pos.wash_ts_first, EXCLUDED.wash_ts_first),
  wash_ts_last  = GREATEST(COALESCE(pos.wash_ts_last, EXCLUDED.wash_ts_last), EXCLUDED.wash_ts_last),

  license_plate     = COALESCE(pos.license_plate, EXCLUDED.license_plate),
  customer_name     = COALESCE(pos.customer_name, EXCLUDED.customer_name),
  wash_package_id   = COALESCE(pos.wash_package_id, EXCLUDED.wash_package_id),
  wash_package_name = COALESCE(pos.wash_package_name, EXCLUDED.wash_package_name),
  wash_type         = COALESCE(pos.wash_type, EXCLUDED.wash_type),
  payment_type      = COALESCE(pos.payment_type, EXCLUDED.payment_type),
  image_path        = COALESCE(pos.image_path, EXCLUDED.image_path),
  is_unlimited      = COALESCE(pos.is_unlimited, EXCLUDED.is_unlimited),
  unlimited_type    = COALESCE(pos.unlimited_type, EXCLUDED.unlimited_type),
  addons            = COALESCE(pos.addons, EXCLUDED.addons),

  tip_amount        = CASE
                        WHEN (pos.tip_amount IS NULL OR pos.tip_amount = 0) AND EXCLUDED.tip_amount > 0
                          THEN EXCLUDED.tip_amount
                        ELSE pos.tip_amount
                      END,

  discount_code     = COALESCE(pos.discount_code, EXCLUDED.discount_code),
  discount_amount   = COALESCE(pos.discount_amount, EXCLUDED.discount_amount),
  tax               = COALESCE(pos.tax, EXCLUDED.tax),
  total             = COALESCE(pos.total, EXCLUDED.total),

  location          = COALESCE(pos.location, EXCLUDED.location),
  lane_no           = COALESCE(pos.lane_no, EXCLUDED.lane_no),
  source_file       = COALESCE(pos.source_file, EXCLUDED.source_file),
  invoice_kind      = COALESCE(pos.invoice_kind, EXCLUDED.invoice_kind),

  -- tenant/location should not change once set; only fill if NULL (defensive)
  tenant_id         = COALESCE(pos.tenant_id, EXCLUDED.tenant_id),
  location_id       = COALESCE(pos.location_id, EXCLUDED.location_id);
"""


def batch_upsert(conn, rows: List[Dict[str, Any]], batch_size: int = 500) -> int:
    if not rows:
        return 0
    total = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]
            psycopg2.extras.execute_batch(cur, UPSERT_SQL, chunk, page_size=len(chunk))
            total += len(chunk)
    conn.commit()
    return total


# ===================== LAST BILL (TENANT + LOCATION + TODAY) =====================
def get_last_bill_for_today(conn, tenant_id: str, location_id: str) -> Optional[int]:
    """
    Find today's latest bill for a tenant + location based on wash_date (derived from wash_ts_first).
    """
    today = now_cst_date()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT bill
            FROM pos
            WHERE tenant_id = %s
              AND location_id = %s
              AND wash_date = %s
            ORDER BY wash_ts_last DESC NULLS LAST
            LIMIT 1
            """,
            (tenant_id, location_id, today),
        )
        row = cur.fetchone()
        return row[0] if row else None

def get_last_bill_for_today_lane(conn, tenant_id: str, location_id: str, lane_no: int) -> Optional[int]:
    """Find today's latest bill for a tenant + location + lane."""
    today = now_cst_date()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT bill
            FROM pos
            WHERE tenant_id=%s
              AND location_id=%s
              AND wash_date=%s
              AND lane_no=%s
            ORDER BY wash_ts_last DESC NULLS LAST
            LIMIT 1
            """,
            (tenant_id, location_id, today, int(lane_no)),
        )
        row = cur.fetchone()
        return row[0] if row else None



# ===================== S3 HELPERS =====================
def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION, config=Config(signature_version="s3v4"))


def parse_lane_from_key_hint(key_hint: Optional[str]) -> Optional[int]:
    """Extract lane number from either a partitioned S3 key (.../lane=1/...) or the legacy filename (_1_Transaction...)."""
    if not key_hint:
        return None
    s = str(key_hint)

    m = re.search(r"(?:^|/)lane=(\d+)(?:/|$)", s, flags=re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None

    m = re.search(r"_(\d+)_transaction", s, flags=re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None

    return None


def parse_tenant_value_from_key(key: str) -> Optional[str]:
    if not key:
        return None
    m = re.search(r"(?:^|/)tenant=([^/]+)(?:/|$)", key, flags=re.IGNORECASE)
    return m.group(1) if m else None


def resolve_tenant_id_from_s3_value(conn, s3_tenant_value: str) -> Optional[str]:
    if not s3_tenant_value:
        return None
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT tenant_id
            FROM tenant_s3_map
            WHERE enabled = TRUE AND LOWER(s3_tenant_value) = LOWER(%s)
            LIMIT 1
            """,
            (s3_tenant_value,),
        )
        row = cur.fetchone()
        return str(row[0]) if row else None


    m = re.search(r"_(\d+)_transaction", s, flags=re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None

    return None

def latest_s3_objects(prefix: str, file_match: str, limit: int = 5) -> List[dict]:
    """
    Returns up to `limit` newest objects under prefix that contain file_match in basename.
    """
    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET is required when not using INPUT_PATH.")
    s3 = get_s3_client()
    items = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if file_match in os.path.basename(key):
                items.append(obj)
    items.sort(key=lambda o: o["LastModified"], reverse=True)
    return items[:limit]

def download_s3_to_temp(key: str) -> Path:
    s3 = get_s3_client()
    basename = os.path.basename(key)
    local_path = Path(tempfile.gettempdir()) / basename
    s3.download_file(S3_BUCKET, key, str(local_path))
    return local_path

def head_s3_object(key: str) -> Dict[str, Any]:
    s3 = get_s3_client()
    r = s3.head_object(Bucket=S3_BUCKET, Key=key)
    return {
        "Key": key,
        "LastModified": r.get("LastModified"),
        "ETag": (r.get("ETag") or "").strip('"'),
        "ContentLength": int(r.get("ContentLength") or 0),
    }


def read_s3_range(key: str, start: int, end: Optional[int] = None) -> bytes:
    """Read bytes from S3 object using Range header."""
    s3 = get_s3_client()
    if start < 0:
        start = 0
    if end is not None and end >= start:
        rng = f"bytes={start}-{end}"
    else:
        rng = f"bytes={start}-"
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key, Range=rng)
    return obj["Body"].read()



def delete_s3_object(key: str):
    s3 = get_s3_client()
    s3.delete_object(Bucket=S3_BUCKET, Key=key)


# ===================== INPUT GATHERING =====================
def gather_input_files_local(input_path: str) -> List[Path]:
    p = Path(input_path)
    if p.is_file():
        return [p]
    if p.is_dir():
        return sorted(p.glob("*.txt"))
    raise FileNotFoundError(f"Input path not found: {input_path}")


# ===================== OPTIONAL SEEDING (DB DRIVEN, NOT HARDCODED IN LOGIC) =====================
def seed_rules_if_empty(conn, tenant_id: str):
    """
    If you want to ship sensible defaults without hardcoding logic, you can seed via env JSON.
    This function only seeds when the rule tables are empty for this tenant.

    Env JSON formats:
      KIOSK_FILE_RULES_JSON: [
        {"filename_contains":"1004 center point pl","location_code":"FRA","lane_no":1},
        ...
      ]
      WASH_TYPE_RULES_JSON: [
        {"match_type":"contains","pattern":"basic","wash_type":"Basic","priority":10},
        ...
      ]

    For kiosk file rules we still need location_id; so seeding expects location_code and it looks up location_id.
    """
    kiosk_json = (os.getenv("KIOSK_FILE_RULES_JSON") or "").strip()
    wash_json = (os.getenv("WASH_TYPE_RULES_JSON") or "").strip()

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM kiosk_file_rules WHERE tenant_id=%s", (tenant_id,))
        kiosk_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM wash_type_rules WHERE tenant_id=%s", (tenant_id,))
        wash_count = cur.fetchone()[0]

    if kiosk_count == 0 and kiosk_json:
        try:
            rules = json.loads(kiosk_json)
        except Exception:
            rules = []
        for r in rules:
            loc_code = (r.get("location_code") or "").strip()
            if not loc_code:
                continue
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT location_id
                    FROM locations
                    WHERE tenant_id=%s AND LOWER(COALESCE(location_code, code, location, name)::text)=LOWER(%s)
                    LIMIT 1
                    """,
                    (tenant_id, loc_code),
                )
                loc_row = cur.fetchone()
            if not loc_row:
                continue
            location_id = str(loc_row[0])
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO kiosk_file_rules (tenant_id, location_id, filename_contains, lane_no, location_code, priority, enabled)
                    VALUES (%s,%s,%s,%s,%s,%s,TRUE)
                    """,
                    (
                        tenant_id,
                        location_id,
                        r.get("filename_contains") or "",
                        r.get("lane_no"),
                        loc_code,
                        int(r.get("priority") or 100),
                    ),
                )
        conn.commit()

    if wash_count == 0 and wash_json:
        try:
            rules = json.loads(wash_json)
        except Exception:
            rules = []
        with conn.cursor() as cur:
            for r in rules:
                cur.execute(
                    """
                    INSERT INTO wash_type_rules (tenant_id, match_type, pattern, wash_type, priority, enabled)
                    VALUES (%s,%s,%s,%s,%s,TRUE)
                    """,
                    (
                        tenant_id,
                        (r.get("match_type") or "contains"),
                        (r.get("pattern") or ""),
                        (r.get("wash_type") or ""),
                        int(r.get("priority") or 100),
                    ),
                )
        conn.commit()


# ===================== MAIN =====================
def main():
    file_entries: List[Tuple[Optional[Path], str]] = []  # (local_path, source_hint)
    from_s3 = False

    if INPUT_PATH:
        files = gather_input_files_local(INPUT_PATH)
        for f in files:
            file_entries.append((f, str(f)))
    else:
        # S3 mode - list newest Transaction files (supports multi-tenant folder prefixes)
        if not S3_BUCKET:
            raise RuntimeError("S3_BUCKET is required when not using INPUT_PATH.")

        # Effective prefix (multi-tenant folder support)
        effective_prefix = S3_PREFIX
        if S3_LAYOUT == "partitioned":
            if WORKER_MODE == "single":
                tenant_val = (S3_TENANT_VALUE or TENANT_SLUG or "").strip()
                if not tenant_val:
                    raise RuntimeError("WORKER_MODE=single with S3_LAYOUT=partitioned requires S3_TENANT_VALUE (or TENANT_SLUG).")
                effective_prefix = f"{S3_PREFIX.rstrip('/')}/tenant={tenant_val}/"
            else:
                effective_prefix = f"{S3_PREFIX.rstrip('/')}/"
        objs = latest_s3_objects(effective_prefix, FILE_MATCH, limit=int(os.getenv("MAX_FILES", "50")))
        if not objs:
            print(f"No Transaction files in S3 under prefix '{effective_prefix}'.")
            return
        for obj in objs:
            key = obj["Key"]
            file_entries.append((None, key))
        from_s3 = True

    if not file_entries:
        print("No input files found.")
        return

    conn = get_conn()
    print('DB:', db_fingerprint(conn))
    print(f"WORKER_MODE={WORKER_MODE} TENANT_FILTER={'ALL' if not TENANT_FILTER else ','.join(sorted(TENANT_FILTER))}")
    try:
        tenant_id_single = resolve_tenant_id(conn) if WORKER_MODE == "single" else None
        ensure_rule_tables(conn)

        # optional seeding via env JSON (only if empty) - single tenant only
        if WORKER_MODE == "single" and tenant_id_single:
            seed_rules_if_empty(conn, tenant_id_single)

        rules_cache = {}  # tenant_id -> (kiosk_rules, wash_type_rules)


        all_rows: List[Dict[str, Any]] = []
        offsets_pending: List[Dict[str, Any]] = []

        for local_path, source_hint in file_entries:
            # source_hint is either a local path string (INPUT_PATH mode) or an S3 key (S3 mode)
            if local_path is None:
                key = source_hint
                filename = os.path.basename(key)
            else:
                key = None
                filename = local_path.name

            key_hint = key or filename

            # Resolve tenant_id for this file
            tenant_id = None
            if WORKER_MODE == "single":
                tenant_id = tenant_id_single
            else:
                if not key:
                    # Local mode with multi is ambiguous; use single mode for local testing
                    tenant_id = tenant_id_single
                else:
                    tval = parse_tenant_value_from_key(key)
                    if TENANT_FILTER and tval and tval.lower() not in TENANT_FILTER:
                        print(f"Skipping tenant '{tval}' due to TENANT_FILTER")
                        continue
                    if not tval:
                        print(f"Skipping key (no tenant= partition): {key}")
                        continue
                    tenant_id = resolve_tenant_id_from_s3_value(conn, tval)
                    if not tenant_id:
                        print(f"WARNING: No tenant_id mapping for s3 tenant value '{tval}'. Add it to tenant_s3_map. Skipping: {key}")
                        continue

            if not tenant_id:
                print(f"Skipping file (tenant_id unresolved): {filename}")
                continue

            # Load rules for this tenant (cached)
            if tenant_id not in rules_cache:
                kiosk_rules_t = load_kiosk_file_rules(conn, tenant_id)
                wash_rules_t = load_wash_type_rules(conn, tenant_id)
                rules_cache[tenant_id] = (kiosk_rules_t, wash_rules_t)
            kiosk_rules, wash_type_rules = rules_cache[tenant_id]

            location_id, lane_rule, location_code_label = infer_location_and_lane_from_rules(key_hint, kiosk_rules)
            if not location_id:
                print(f"Skipping file (no kiosk_file_rules match): {filename}")
                continue

            lane_no = lane_rule if lane_rule is not None else parse_lane_from_key_hint(key_hint)
            if lane_no is None:
                print(f"Skipping file (no lane_no resolved): {filename} (hint: {key_hint})")
                continue

            # Prefer label from rule; otherwise pull from locations table
            location_label = location_code_label or lookup_location_code(conn, location_id)

            # Load last-ingest offset (per tenant + location + lane + filename)
            offset_row = load_ingest_offset(conn, tenant_id, location_id, int(lane_no), filename)

            lines: List[str] = []
            object_size = None
            s3_meta = None

            if key:
                # S3 mode: read only the tail using the stored byte offset
                s3_meta = head_s3_object(key)
                object_size = int(s3_meta.get("ContentLength") or 0)

                last_byte = int((offset_row or {}).get("last_byte_offset") or 0)
                same_key = bool(offset_row and (offset_row.get("s3_key") == key))

                if same_key and last_byte > 0 and object_size > last_byte:
                    # Read from a little before last_byte to preserve session context
                    start_byte = max(last_byte - BUFFER_BYTES, 0)
                    b = read_s3_range(key, start_byte)
                    txt = b.decode("utf-8", errors="ignore")
                    lines = txt.splitlines()
                    if start_byte > 0 and lines:
                        # first line may be truncated mid-line
                        lines = lines[1:]
                    print(f"Tail-read s3://{S3_BUCKET}/{key} from byte {start_byte} (prev={last_byte}, size={object_size})")
                else:
                    # New file day or first run: download full file
                    print(f"Downloading full s3://{S3_BUCKET}/{key} (size={object_size}) ...")
                    local_path = download_s3_to_temp(key)
                    with local_path.open("r", encoding="utf-8", errors="ignore") as fh:
                        lines = fh.readlines()

            else:
                # Local mode
                assert local_path is not None
                object_size = local_path.stat().st_size
                last_byte = int((offset_row or {}).get("last_byte_offset") or 0)
                if last_byte > 0 and object_size > last_byte:
                    start_byte = max(last_byte - BUFFER_BYTES, 0)
                    with local_path.open("rb") as fh:
                        fh.seek(start_byte)
                        b = fh.read()
                    txt = b.decode("utf-8", errors="ignore")
                    lines = txt.splitlines()
                    if start_byte > 0 and lines:
                        lines = lines[1:]
                    print(f"Tail-read local {local_path} from byte {start_byte} (prev={last_byte}, size={object_size})")
                else:
                    with local_path.open("r", encoding="utf-8", errors="ignore") as fh:
                        lines = fh.readlines()

            print(f"File: {filename} → tenant_id={tenant_id}, location_id={location_id}, lane={lane_no}, label={location_label}")

            # Determine last bill for safe start point
            last_bill = (offset_row or {}).get("last_bill")
            if last_bill is None:
                last_bill = get_last_bill_for_today_lane(conn, tenant_id, location_id, int(lane_no))

            if last_bill is not None:
                found_idx = find_last_bill_index(lines, int(last_bill)) or 0
                start_idx = max(found_idx - BUFFER_LINES, 0)
                if found_idx > 0:
                    print(f"Found last_bill={last_bill} at line {found_idx}; parsing from {start_idx} (buffer={BUFFER_LINES}).")
                else:
                    print(f"Did NOT find last_bill={last_bill} in tail; parsing entire provided content.")
                    start_idx = 0
            else:
                start_idx = 0
                print("No existing rows today for this tenant/location; parsing provided content from start.")
            # Parse
            parse_path = local_path or (Path(tempfile.gettempdir()) / filename)
            parsed_rows = parse_file(
                parse_path,
                wash_type_rules=wash_type_rules,
                start_index=start_idx,
                preloaded_lines=lines,
                location_label=location_label,
                lane_no=lane_no,
            )

            # add tenant + location ids
            for r in parsed_rows:
                r["tenant_id"] = tenant_id
                r["location_id"] = location_id

            all_rows.extend(parsed_rows)
            # Track offset update for this lane/file (even if parsed_rows is empty, we may advance offset)
            offsets_pending.append({
                "tenant_id": tenant_id,
                "location_id": location_id,
                "lane_no": int(lane_no),
                "source_file": filename,
                "s3_key": key,
                "s3_etag": (s3_meta.get("ETag") if s3_meta else None),
                "last_modified": (s3_meta.get("LastModified") if s3_meta else None),
                "last_size": int(object_size or 0),
            })

                # De-dup by logical key (tenant_id, location_id, bill, wash_date)
        # wash_date is derived from wash_ts_first (same as the generated column in DB).
        dedup: Dict[tuple, Dict[str, Any]] = {}
        for r in all_rows:
            wdate = r.get("wash_ts_first").date() if r.get("wash_ts_first") else None
            key = (r.get("tenant_id"), r.get("location_id"), r.get("bill"), wdate)
            dedup[key] = r
        final_rows = list(dedup.values())

        print(f"Parsed {len(all_rows)} rows → {len(final_rows)} after de-dup (by bill)")

        # Validate FK targets (locations) to avoid hard crash and to expose DB mismatch
        bad_locs = sorted({r.get("location_id") for r in final_rows if r.get("location_id") and not location_exists(conn, r.get("location_id"))})
        if bad_locs:
            print("ERROR: Some location_id values are missing in locations table on this DB. Skipping those rows.")
            print("Missing location_id(s):", ", ".join(bad_locs[:20]) + ("..." if len(bad_locs) > 20 else ""))
            final_rows = [r for r in final_rows if r.get("location_id") and location_exists(conn, r.get("location_id"))]
            print(f"Remaining rows after FK filter: {len(final_rows)}")
        inserted = batch_upsert(conn, final_rows)
        print(f"✅ Upserted {inserted} rows into pos")

        # Persist last byte offsets per lane/file (per tenant/location)
        for off in offsets_pending:
            lane = int(off["lane_no"])
            loc_id = off["location_id"]
            tid = off["tenant_id"]
            last_bill_lane = get_last_bill_for_today_lane(conn, tid, loc_id, lane)
            save_ingest_offset(
                conn,
                tid,
                loc_id,
                lane,
                off["source_file"],
                last_byte_offset=int(off.get("last_size") or 0),
                last_bill=last_bill_lane,
                s3_key=off.get("s3_key"),
                s3_etag=off.get("s3_etag"),
                last_modified=off.get("last_modified"),
                last_size=int(off.get("last_size") or 0),
            )

    finally:
        conn.close()

    # NOTE: S3 objects are not deleted (append-only daily files; offsets handle incremental ingestion)



if __name__ == "__main__":
    main()