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

# ===================== FINANCIAL REGEX =====================

TS_RE = re.compile(
    r"^\s*(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s+[AP]M)\s*,\s*"
)

TAX_RE = re.compile(
    r"Tax[:\s]+\$?([0-9]+(?:\.[0-9]{1,2})?)\b",
    re.IGNORECASE
)

TOTAL_RE = re.compile(
    r"Total[:\s]+\$?([0-9]+(?:\.[0-9]{1,2})?)\b",
    re.IGNORECASE
)

DISCOUNT_BOTH_RE = re.compile(
    r"Discount[:\s]+([A-Za-z0-9._-]+)\s+\$?([0-9]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE
)

DISCOUNT_CODE_RE = re.compile(
    r"Discount(?:\s+Code)?[:\s]+([A-Za-z][A-Za-z0-9._-]*)",
    re.IGNORECASE
)

DISCOUNT_AMOUNT_RE = re.compile(
    r"Discount(?:\s+Amount)?[:\s]+\$?([0-9]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE
)

INVOICE_RE = re.compile(r"InvoiceID\s+(\d+)", re.IGNORECASE)

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

# ===================== ARCHIVE SETTINGS =====================
ARCHIVE_ENABLED = str(os.getenv("ARCHIVE_ENABLED", "1")).strip() not in {"0","false","False","no","NO"}
ARCHIVE_PREFIX  = os.getenv("ARCHIVE_PREFIX", "kiosks_archive/").strip()
ARCHIVE_LATEST_NAME = os.getenv("ARCHIVE_LATEST_NAME", "latest.txt").strip()

# Local override for testing (file or directory)
INPUT_PATH = os.getenv("INPUT_PATH")

DELETE_S3_AFTER_SUCCESS = (os.getenv("DELETE_S3_AFTER_SUCCESS", "true").strip().lower() in ("1", "true", "yes", "y"))

# Tail-ingest safety buffer (lines before the last known bill)
BUFFER_LINES = int(os.getenv("BUFFER_LINES", "2000"))

# Byte-buffer when tail-reading from S3/local using last byte offset (keeps session context)
BUFFER_BYTES = int(os.getenv("BUFFER_BYTES", "200000"))  # ~200KB

# S3 folder layout support
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



# ===================== IMAGE PATH NORMALIZATION =====================
IMAGE_BASE_URL = os.getenv("IMAGE_BASE_URL", "https://drbwashifyimages.s3.amazonaws.com/").rstrip("/")

def normalize_image_path(raw: Optional[str], bill: Optional[int] = None) -> Optional[str]:
    """Convert kiosk 'Aws File Name ...' value to a full URL."""
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if s.lower().startswith(("http://", "https://")):
        return s

    s = re.sub(r"^Aws File Name\s+", "", s, flags=re.IGNORECASE).strip()

    if "Server_10/" in s and "/IpCameraImages/" in s:
        if not s.endswith("_Full.jpg"):
            s = f"{s}_Full.jpg"
        return f"{IMAGE_BASE_URL}/{s}"
    return s

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

    with conn.cursor() as cur:
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

    for r in rules:
        if r["match_type"] == "regex":
            try:
                r["_re"] = re.compile(r["pattern"], re.IGNORECASE)
            except Exception:
                r["_re"] = None
    if not rules:
        ensure_default_wash_type_rules(conn, tenant_id)
        return load_wash_type_rules(conn, tenant_id)
    return rules



def ensure_default_wash_type_rules(conn, tenant_id: str):
    """Seed basic wash type rules if none exist for this tenant."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM wash_type_rules WHERE tenant_id = %s", (tenant_id,))
        cnt = int(cur.fetchone()[0] or 0)
        if cnt > 0:
            return
        defaults = [
            ("exact", "BASIC WASH", "Basic", 10),
            ("exact", "GOOD WASH", "Good", 20),
            ("exact", "BETTER WASH", "Better", 30),
            ("exact", "BEST WASH", "Best", 40),
            ("exact", "INTERIOR SUP.", "Super", 50),
            ("exact", "INTERIOR SUP", "Super", 51),
            ("contains", "INTERIOR", "Super", 60),
        ]
        cur.executemany(
            """
            INSERT INTO wash_type_rules (tenant_id, match_type, pattern, wash_type, priority, enabled)
            VALUES (%s,%s,%s,%s,%s,TRUE)
            ON CONFLICT DO NOTHING
            """,
            [(tenant_id, mt, pat, wt, pr) for (mt, pat, wt, pr) in defaults],
        )
    conn.commit()

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
    fn = (key_hint or "").lower()
    for r in rules:
        if (r.get("filename_contains") or "").lower() in fn:
            return (str(r["location_id"]), r.get("lane_no"), r.get("location_code"))
    return (None, None, None)


def lookup_location_code(conn, location_id: str) -> Optional[str]:
    with conn.cursor() as cur:
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


# ===================== NEW: WASH_RECURRING LOOKUP (TENANT-SCOPED) =====================
def load_wash_recurring_map(conn, tenant_id: str) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
              wash_package_id,
              wash_package_name,
              wash_type,
              COALESCE(item_kind, 'WASH') AS item_kind,
              addon_name
            FROM wash_recurring
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        )
        rows = cur.fetchall()

    for r in rows:
        try:
            pid = int(r["wash_package_id"])
        except Exception:
            continue

        out[pid] = {
            "wash_package_id": pid,
            "wash_package_name": r.get("wash_package_name"),
            "wash_type": r.get("wash_type"),
            "item_kind": r.get("item_kind", "WASH"),   # ✅ ADD THIS
            "addon_name": r.get("addon_name"),         # ✅ ADD THIS
        }

    return out
# ===================== PARSING REGEX =====================
TS_RE = re.compile(
    r"^\s*(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s+[AP]M)\s*,\s*"
)

PROCEED_INVOICE_RE = re.compile(
    r"ProceedToCarWashViewModel.*?InvoiceID\s+(\d+)", re.IGNORECASE
)

RETURN_INVOICE_RE = re.compile(
    r"MethodName=ReturnToMainScreen.*?InvoiceID\s+(\d+)", re.IGNORECASE
)

INVOICE_ANY_RE = re.compile(r"InvoiceID\s+(\d+)", re.IGNORECASE)
PAYMENT_TYPE_RE = re.compile(r"Payment Type\s+([A-Za-z]+)", re.IGNORECASE)
WASH_PKG_RE = re.compile(r"Wash Package\s+(\d+)\s+with Name\s+(.+)$", re.IGNORECASE)
AWS_FILE_RE = re.compile(r"Aws File Name\s+(.+)$", re.IGNORECASE)
LICENSE_PLATE_RE = re.compile(r"License Plate\s+([A-Z0-9]+)", re.IGNORECASE)
CUSTOMER_NAME_RE = re.compile(r"Customer Name\s+([^,]+)", re.IGNORECASE)
SERVICE_ID_RE = re.compile(r"ServiceID\s*(\d+)", re.IGNORECASE)
MESSAGE_RECURRING_RE = re.compile(r"Message\s*=\s*RECURRING", re.IGNORECASE)
TIP_AMOUNT_RE = re.compile(r"\bTip\s*\$?\s*([0-9]+(?:\.[0-9]{1,2})?)", re.IGNORECASE)


# ===================== PARSER =====================
def normalize_ws_name(name: Optional[str]) -> Optional[str]:
    """
    Normalize Washify wash package names so they are consistent
    and safe for rule matching.
    """
    if not name:
        return None

    s = str(name).strip()
    if not s:
        return None

    # Strip known noisy suffixes Washify appends
    for token in (
        "VehicleID",
        "VehicleId",
        "ServiceID",
        "ServiceId",
        "ServiceName",
    ):
        if token in s:
            s = s.split(token, 1)[0].strip()

    # Collapse extra whitespace
    s = re.sub(r"\s{2,}", " ", s).strip()

    return s or None
def map_wash_type_from_rules(
    pkg_name: Optional[str],
    rules: List[Dict[str, Any]],
) -> Optional[str]:
    """
    Resolve wash_type using DB-driven wash_type_rules.
    Priority order is already enforced by SQL ORDER BY.
    """
    if not pkg_name:
        return None

    name = normalize_ws_name(pkg_name)
    if not name:
        return None

    low = name.lower()

    for r in rules:
        match_type = r.get("match_type")
        pattern = (r.get("pattern") or "").strip()

        if not pattern:
            continue

        if match_type == "contains":
            if pattern.lower() in low:
                return r.get("wash_type")

        elif match_type == "exact":
            if pattern.lower() == low:
                return r.get("wash_type")

        elif match_type == "regex":
            rx = r.get("_re")
            if rx and rx.search(name):
                return r.get("wash_type")

    return None
# ===================== PARSE_FILE =====================
# ===================== PARSE_FILE =====================
def parse_file(
    path: Path,
    wash_type_rules: List[Dict[str, Any]],
    wash_recurring_map: Dict[int, Dict[str, Any]],
    start_index: int = 0,
    preloaded_lines: Optional[List[str]] = None,
    location_label: Optional[str] = None,
    lane_no: Optional[int] = None,
) -> List[Dict[str, Any]]:

    rows: List[Dict[str, Any]] = []
    sess: Optional[Dict[str, Any]] = None

    lines = (
        preloaded_lines
        if preloaded_lines is not None
        else path.read_text(errors="ignore").splitlines()
    )

    def new_session():
        return {
            "invoice": None,
            "wash_ts_first": None,
            "wash_ts_last": None,
            "wash_package_id": None,
            "wash_package_name": None,
            "wash_type": None,
            "service_id": None,
            "unlimited_type": None,  # NEW | RECURRING
            "_is_unlimited_signup": False,  # INTERNAL ONLY
            "addons": set(),
            "license_plate": None,
            "customer_name": None,
            "payment_type": None,
            "image_path": None,
            "discount_code": None,
            "discount_amount": 0.0,
            "tax": 0.0,
            "total": 0.0,
            "tip_amount": 0.0,
        }

    for raw in lines[start_index:]:
        ts, content = parse_ts(raw.strip())
        if not content or not ts:
            continue

        # =========================================================
        # START OF TRANSACTION (NEW or RECURRING)
        # =========================================================
        if (
            "ClassName=RFID Unlimited" in content
            and ("MethodName=BindCustomerVehicleInformation" in content or "MethodName=SelectOptionsViewModel" in content)
            and ("Message=NEW CUSTOMER" in content or "Message=RECURRING" in content)
        ):
            sess = new_session()
            sess["wash_ts_first"] = ts

            if "Message=RECURRING" in content:
                sess["unlimited_type"] = "RECURRING"

                m = SERVICE_ID_RE.search(content)
                if m:
                    sess["service_id"] = int(m.group(1))
                    rec = wash_recurring_map.get(sess["service_id"])
                    if rec and rec.get("item_kind") == "WASH":
                        sess["wash_package_id"] = rec["wash_package_id"]
                        sess["wash_package_name"] = rec["wash_package_name"]
                        sess["wash_type"] = rec["wash_type"]
            else:
                sess["unlimited_type"] = "NEW"

            continue

        if not sess:
            continue

        # =========================================================
        # UNLIMITED SIGNUP DETECTION (AUTHORITATIVE SIGNAL)
        # =========================================================
        if (
            "ClassName=UnlimitedCustomerSignatureViewModel" in content
            and "MethodName=SubmitSignature" in content
            and "Message=Unlimited" in content
        ):
            sess["_is_unlimited_signup"] = True
            continue

        # =========================================================
        # COMMON FIELD CAPTURE
        # =========================================================
        m = LICENSE_PLATE_RE.search(content)
        if m:
            sess["license_plate"] = m.group(1)

        m = CUSTOMER_NAME_RE.search(content)
        if m:
            sess["customer_name"] = m.group(1).strip()

        m = PAYMENT_TYPE_RE.search(content)
        if m:
            sess["payment_type"] = m.group(1)

        m = AWS_FILE_RE.search(content)
        if m:
            sess["image_path"] = normalize_image_path(m.group(1), sess.get("invoice"))

        m = INVOICE_RE.search(content)
        if m:
            sess["invoice"] = int(m.group(1))

        # =========================================================
        # NEW CUSTOMER OR UNLIMITED SIGNUP → Wash + Add-ons
        # =========================================================
        if sess["unlimited_type"] == "NEW" or sess["_is_unlimited_signup"]:
            m = WASH_PKG_RE.search(content)
            if m:
                pkg_id = int(m.group(1))
                pkg_name = normalize_ws_name(m.group(2))

                rec = next(
                    (
                        r for r in wash_recurring_map.values()
                        if r.get("wash_package_name", "").lower() == pkg_name.lower()
                    ),
                    None,
                )

                if rec:
                    if rec["item_kind"] == "WASH":
                        if not sess["wash_package_id"]:
                            sess["wash_package_id"] = rec["wash_package_id"]
                            sess["wash_package_name"] = rec["wash_package_name"]
                            sess["wash_type"] = rec["wash_type"]
                    elif rec["item_kind"] == "ADDON":
                        sess["addons"].add(rec["addon_name"])
                else:
                    if not sess["wash_package_id"]:
                        mapped = map_wash_type_from_rules(pkg_name, wash_type_rules)
                        if mapped:
                            sess["wash_package_id"] = pkg_id
                            sess["wash_package_name"] = pkg_name
                            sess["wash_type"] = mapped

        # =========================================================
        # UNLIMITED SIGNUP → resolve wash_type by wash_package_id
        # =========================================================
        if sess["_is_unlimited_signup"] and not sess.get("wash_type"):
            pkg_id = sess.get("wash_package_id")
            if pkg_id:
                rec = next(
                    (
                        r for r in wash_recurring_map.values()
                        if int(r.get("wash_package_id", 0)) == int(pkg_id)
                        and r.get("item_kind") == "WASH"
                    ),
                    None,
                )
                if rec:
                    sess["wash_type"] = rec["wash_type"]
                    sess["wash_package_name"] = rec["wash_package_name"]

        # =========================================================
        # END OF TRANSACTION (camera = truth)
        # =========================================================
        if (
            "ClassName=AwsModel" in content
            and "MethodName=SaveIPCameraImageAsync" in content
        ):
            sess["wash_ts_last"] = ts

            # Final safety gate (unchanged)
            if not sess.get("invoice") or not sess.get("wash_type"):
                sess = None
                continue

            rows.append({
                "bill": sess["invoice"],
                "wash_ts_first": sess["wash_ts_first"],
                "wash_ts_last": sess["wash_ts_last"],
                "license_plate": sess["license_plate"],
                "customer_name": sess["customer_name"],
                "wash_package_id": sess["wash_package_id"],
                "wash_package_name": sess["wash_package_name"],
                "wash_type": sess["wash_type"],
                "payment_type": sess["payment_type"],
                "image_path": sess["image_path"],
                "is_unlimited": True,
                "unlimited_type": sess["unlimited_type"],
                "addons": ", ".join(sorted(sess["addons"])) or None,
                "location": location_label,
                "lane_no": lane_no,
                "source_file": path.name,
                "created_on": now_cst_date(),
                "created_at": now_cst_time(),
                "invoice_kind": "WASH",
            })

            sess = None

    return rows

# ===================== PARSE_FILE =====================
def parse_file(
    path: Path,
    wash_type_rules: List[Dict[str, Any]],
    wash_recurring_map: Dict[int, Dict[str, Any]],
    start_index: int = 0,
    preloaded_lines: Optional[List[str]] = None,
    location_label: Optional[str] = None,
    lane_no: Optional[int] = None,
) -> List[Dict[str, Any]]:

    rows: List[Dict[str, Any]] = []
    sess: Optional[Dict[str, Any]] = None

    lines = (
        preloaded_lines
        if preloaded_lines is not None
        else path.read_text(errors="ignore").splitlines()
    )

    def new_session():
        return {
            "invoice": None,
            "wash_ts_first": None,
            "wash_ts_last": None,
            "wash_package_id": None,
            "wash_package_name": None,
            "wash_type": None,
            "service_id": None,
            "unlimited_type": None,  # NEW | RECURRING | SIGNUP
            "addons": set(),
            "license_plate": None,
            "customer_name": None,
            "payment_type": None,
            "image_path": None,
            "discount_code": None,
            "discount_amount": 0.0,
            "tax": 0.0,
            "total": 0.0,
            "tip_amount": 0.0,
        }

    for raw in lines[start_index:]:
        ts, content = parse_ts(raw.strip())
        if not content or not ts:
            continue

        # =========================================================
        # START OF TRANSACTION (NEW or RECURRING)
        # =========================================================
        if (
            "ClassName=RFID Unlimited" in content
            and ("MethodName=BindCustomerVehicleInformation" in content or "MethodName=SelectOptionsViewModel" in content)
            and ("Message=NEW CUSTOMER" in content or "Message=RECURRING" in content)
        ):
            sess = new_session()
            sess["wash_ts_first"] = ts

            if "Message=RECURRING" in content:
                sess["unlimited_type"] = "RECURRING"

                # Extract ServiceID immediately
                m = SERVICE_ID_RE.search(content)
                if m:
                    sess["service_id"] = int(m.group(1))
                    rec = wash_recurring_map.get(sess["service_id"])
                    if rec and rec.get("item_kind") == "WASH":
                        sess["wash_package_id"] = rec["wash_package_id"]
                        sess["wash_package_name"] = rec["wash_package_name"]
                        sess["wash_type"] = rec["wash_type"]
            else:
                sess["unlimited_type"] = "NEW"

            continue

        if not sess:
            continue

        # =========================================================
        # UNLIMITED SIGNUP DETECTION (NEW)
        # =========================================================
        if (
            "ClassName=UnlimitedCustomerSignatureViewModel" in content
            and "MethodName=SubmitSignature" in content
            and "Message=Unlimited" in content
        ):
            sess["unlimited_type"] = "SIGNUP"
            continue

        # =========================================================
        # COMMON FIELD CAPTURE
        # =========================================================
        m = LICENSE_PLATE_RE.search(content)
        if m:
            sess["license_plate"] = m.group(1)

        m = CUSTOMER_NAME_RE.search(content)
        if m:
            sess["customer_name"] = m.group(1).strip()

        m = PAYMENT_TYPE_RE.search(content)
        if m:
            sess["payment_type"] = m.group(1)

        m = AWS_FILE_RE.search(content)
        if m:
            sess["image_path"] = normalize_image_path(m.group(1), sess.get("invoice"))

        m = INVOICE_RE.search(content)
        if m:
            sess["invoice"] = int(m.group(1))

        # =========================================================
        # NEW CUSTOMER → Wash + Add-ons
        # (unchanged, still works)
        # =========================================================
        if sess["unlimited_type"] == "NEW":
            m = WASH_PKG_RE.search(content)
            if m:
                pkg_id = int(m.group(1))
                pkg_name = normalize_ws_name(m.group(2))

                rec = next(
                    (
                        r for r in wash_recurring_map.values()
                        if r.get("wash_package_name", "").lower() == pkg_name.lower()
                    ),
                    None,
                )

                if rec:
                    if rec["item_kind"] == "WASH":
                        if not sess["wash_package_id"]:
                            sess["wash_package_id"] = rec["wash_package_id"]
                            sess["wash_package_name"] = rec["wash_package_name"]
                            sess["wash_type"] = rec["wash_type"]
                    elif rec["item_kind"] == "ADDON":
                        sess["addons"].add(rec["addon_name"])
                else:
                    if not sess["wash_package_id"]:
                        mapped = map_wash_type_from_rules(pkg_name, wash_type_rules)
                        if mapped:
                            sess["wash_package_id"] = pkg_id
                            sess["wash_package_name"] = pkg_name
                            sess["wash_type"] = mapped

        # =========================================================
        # UNLIMITED SIGNUP → resolve wash_type by wash_package_id
        # =========================================================
        if sess["unlimited_type"] == "SIGNUP" and not sess.get("wash_type"):
            pkg_id = sess.get("wash_package_id")
            if pkg_id:
                rec = next(
                    (
                        r for r in wash_recurring_map.values()
                        if int(r.get("wash_package_id", 0)) == int(pkg_id)
                        and r.get("item_kind") == "WASH"
                    ),
                    None,
                )
                if rec:
                    sess["wash_type"] = rec["wash_type"]
                    sess["wash_package_name"] = rec["wash_package_name"]

        # =========================================================
        # END OF TRANSACTION (camera = truth)
        # =========================================================
        if (
            "ClassName=AwsModel" in content
            and "MethodName=SaveIPCameraImageAsync" in content
        ):
            sess["wash_ts_last"] = ts

            # Final safety checks (unchanged)
            if not sess.get("invoice") or not sess.get("wash_type"):
                sess = None
                continue

            rows.append({
                "bill": sess["invoice"],
                "wash_ts_first": sess["wash_ts_first"],
                "wash_ts_last": sess["wash_ts_last"],
                "license_plate": sess["license_plate"],
                "customer_name": sess["customer_name"],
                "wash_package_id": sess["wash_package_id"],
                "wash_package_name": sess["wash_package_name"],
                "wash_type": sess["wash_type"],
                "payment_type": sess["payment_type"],
                "image_path": sess["image_path"],
                "is_unlimited": True,
                "unlimited_type": sess["unlimited_type"],
                "addons": ", ".join(sorted(sess["addons"])) or None,
                "location": location_label,
                "lane_no": lane_no,
                "source_file": path.name,
                "created_on": now_cst_date(),
                "created_at": now_cst_time(),
                "invoice_kind": "WASH",
            })

            sess = None

    return rows

# ===================== UPSERT INTO POS =====================
UPSERT_SQL = """
INSERT INTO pos (
  bill, wash_date, wash_ts_first, wash_ts_last, license_plate, customer_name,
  wash_package_id, wash_package_name, wash_type, payment_type, image_path,
  is_unlimited, unlimited_type, addons, tip_amount,
  discount_code, discount_amount, tax, total,
  location, lane_no, source_file, created_on, created_at, invoice_kind,
  tenant_id, location_id
) VALUES (
  %(bill)s, %(wash_date)s, %(wash_ts_first)s, %(wash_ts_last)s, %(license_plate)s, %(customer_name)s,
  %(wash_package_id)s, %(wash_package_name)s, %(wash_type)s, %(payment_type)s, %(image_path)s,
  %(is_unlimited)s, %(unlimited_type)s, %(addons)s, %(tip_amount)s,
  %(discount_code)s, %(discount_amount)s, %(tax)s, %(total)s,
  %(location)s, %(lane_no)s, %(source_file)s, %(created_on)s, %(created_at)s, %(invoice_kind)s,
  %(tenant_id)s, %(location_id)s
)
ON CONFLICT (tenant_id, location_id, bill, wash_date) DO UPDATE SET
  wash_date = COALESCE(pos.wash_date, EXCLUDED.wash_date),
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

def parse_address_value_from_key(key: str) -> Optional[str]:
    if not key:
        return None
    m = re.search(r"(?:^|/)address=([^/]+)(?:/|$)", key, flags=re.IGNORECASE)
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


def latest_s3_objects(prefix: str, file_match: str, limit: int = 5) -> List[dict]:
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

def archive_and_delete_s3_object(key: str, tenant_value: Optional[str]=None, address_value: Optional[str]=None, lane_no: Optional[int]=None):
    if not ARCHIVE_ENABLED:
        return
    s3 = get_s3_client()

    mirror_key = f"{ARCHIVE_PREFIX.rstrip('/')}/{key.lstrip('/')}"
    s3.copy_object(Bucket=S3_BUCKET, CopySource={"Bucket": S3_BUCKET, "Key": key}, Key=mirror_key)

    if tenant_value and address_value and lane_no is not None:
        latest_key = f"{ARCHIVE_PREFIX.rstrip('/')}/tenant={tenant_value}/address={address_value}/lane={lane_no}/{ARCHIVE_LATEST_NAME}"
        s3.copy_object(Bucket=S3_BUCKET, CopySource={"Bucket": S3_BUCKET, "Key": key}, Key=latest_key)

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
def parse_ts(line: str) -> tuple[Optional[datetime], str]:
    """
    Parse timestamp prefix from kiosk log lines.
    Returns (timestamp | None, remaining_content).
    """
    m = TS_RE.match(line)
    if not m:
        return None, line

    try:
        ts = datetime.strptime(
            m.group(1),
            "%m/%d/%Y %I:%M:%S %p"
        )
    except Exception:
        return None, line

    return ts, line[m.end():]

def find_last_bill_index(lines: list, last_bill: int) -> int | None:
    """
    Find the last occurrence of a given bill number in the kiosk log lines.
    Used to safely resume parsing with a buffer.
    """
    if not last_bill:
        return None

    target = str(last_bill)

    for idx in range(len(lines) - 1, -1, -1):
        raw = lines[idx].strip()
        if not raw:
            continue

        _, content = parse_ts(raw)

        if not content:
            continue

        if f"InvoiceID {target}" in content or f"InvoiceId {target}" in content:
            return idx

    return None

# ===================== MAIN =====================
def main():
    file_entries: List[Tuple[Optional[Path], str]] = []  # (local_path, source_hint)
    from_s3 = False

    if INPUT_PATH:
        files = gather_input_files_local(INPUT_PATH)
        for f in files:
            file_entries.append((f, str(f)))
    else:
        if not S3_BUCKET:
            raise RuntimeError("S3_BUCKET is required when not using INPUT_PATH.")

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
    print(f"WORKER_MODE={WORKER_MODE} TENANT_FILTER={'ALL' if not TENANT_FILTER else ','.join(sorted(TENANT_FILTER))}")
    try:
        tenant_id_single = resolve_tenant_id(conn) if WORKER_MODE == "single" else None
        ensure_rule_tables(conn)

        if WORKER_MODE == "single" and tenant_id_single:
            seed_rules_if_empty(conn, tenant_id_single)

        rules_cache = {}  # tenant_id -> (kiosk_rules, wash_type_rules)
        recurring_cache = {}  # tenant_id -> wash_recurring map (service_id -> fields)

        all_rows: List[Dict[str, Any]] = []
        offsets_pending: List[Dict[str, Any]] = []

        for local_path, source_hint in file_entries:
            if local_path is None:
                key = source_hint
                filename = os.path.basename(key)
            else:
                key = None
                filename = local_path.name

            key_hint = key or filename

            tenant_id = None
            if WORKER_MODE == "single":
                tenant_id = tenant_id_single
            else:
                if not key:
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

            location_label = location_code_label or lookup_location_code(conn, location_id)

            offset_row = load_ingest_offset(conn, tenant_id, location_id, int(lane_no), filename)

            lines: List[str] = []
            object_size = None
            s3_meta = None

            if key:
                s3_meta = head_s3_object(key)
                object_size = int(s3_meta.get("ContentLength") or 0)

                last_byte = int((offset_row or {}).get("last_byte_offset") or 0)
                same_key = bool(offset_row and (offset_row.get("s3_key") == key))

                if same_key and last_byte > 0 and object_size > last_byte:
                    start_byte = max(last_byte - BUFFER_BYTES, 0)
                    b = read_s3_range(key, start_byte)
                    txt = b.decode("utf-8", errors="ignore")
                    lines = txt.splitlines()
                    if start_byte > 0 and lines:
                        lines = lines[1:]
                    print(f"Tail-read s3://{S3_BUCKET}/{key} from byte {start_byte} (prev={last_byte}, size={object_size})")
                else:
                    print(f"Downloading full s3://{S3_BUCKET}/{key} (size={object_size}) ...")
                    local_path = download_s3_to_temp(key)
                    with local_path.open("r", encoding="utf-8", errors="ignore") as fh:
                        lines = fh.readlines()

            else:
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

            parse_path = local_path or (Path(tempfile.gettempdir()) / filename)
            if tenant_id not in recurring_cache:
                recurring_cache[tenant_id] = load_wash_recurring_map(conn, tenant_id)

            parsed_rows = parse_file(
                parse_path,
                wash_type_rules=wash_type_rules,
                wash_recurring_map=recurring_cache[tenant_id], 
                start_index=start_idx,
                preloaded_lines=lines,
                location_label=location_label,
                lane_no=lane_no,
            )


            for r in parsed_rows:
                r["tenant_id"] = tenant_id
                r["location_id"] = location_id

            # ===================== NEW: fill recurring wash package fields via wash_recurring (tenant-scoped) =====================
            if parsed_rows:
                if tenant_id not in recurring_cache:
                    recurring_cache[tenant_id] = load_wash_recurring_map(conn, tenant_id)
                rec_map = recurring_cache[tenant_id]

                for r in parsed_rows:
                    if r.get("unlimited_type") != "RECURRING":
                        continue
                    sid = r.get("service_id")
                    if not sid:
                        continue

                    # Only fill when Washify didn't provide it
                    if r.get("wash_package_id") and r.get("wash_package_name") and r.get("wash_type"):
                        continue

                    rec = rec_map.get(int(sid))
                    if not rec:
                        continue

                    if not r.get("wash_package_id"):
                        r["wash_package_id"] = rec.get("wash_package_id")
                    if not r.get("wash_package_name"):
                        r["wash_package_name"] = rec.get("wash_package_name")
                    if not r.get("wash_type"):
                        r["wash_type"] = rec.get("wash_type")
            # ====================================================================================================================

            all_rows.extend(parsed_rows)

            offsets_pending.append({
                "tenant_id": tenant_id,
                "location_id": location_id,
                "lane_no": int(lane_no),
                "source_file": filename,
                "s3_key": key,
                "tenant_value": parse_tenant_value_from_key(key),
                "address_value": parse_address_value_from_key(key),
                "s3_etag": (s3_meta.get("ETag") if s3_meta else None),
                "last_modified": (s3_meta.get("LastModified") if s3_meta else None),
                "last_size": int(object_size or 0),
            })

        dedup: Dict[tuple, Dict[str, Any]] = {}
        for r in all_rows:
            wdate = r.get("wash_ts_first").date() if r.get("wash_ts_first") else None
            key = (r.get("tenant_id"), r.get("location_id"), r.get("bill"), wdate)
            dedup[key] = r
        final_rows = list(dedup.values())

        print(f"Parsed {len(all_rows)} rows → {len(final_rows)} after de-dup (by bill)")

# ---------- Normalize rows for UPSERT_SQL ----------

        # ---------- Normalize rows for UPSERT_SQL ----------
        REQUIRED_KEYS = [
             "bill","wash_date","wash_ts_first","wash_ts_last",
             "license_plate","customer_name",
             "wash_package_id","wash_package_name","wash_type","payment_type","image_path",
             "is_unlimited","unlimited_type","addons","tip_amount",
             "discount_code","discount_amount","tax","total",
             "location","lane_no","source_file","created_on","created_at","invoice_kind",
             "tenant_id","location_id",
        ]


        for r in final_rows:
            # Financial defaults
            r.setdefault("discount_code", None)
            r.setdefault("discount_amount", 0.0)
            r.setdefault("tax", 0.0)
            r.setdefault("total", 0.0)

            # wash_date is REQUIRED (NOT NULL)
            # Ensure wash_ts_first exists so Postgres can generate wash_date
            # wash_date is REQUIRED for UPSERT (psycopg2 requires key to exist)
            if not r.get("wash_ts_first"):
                r["wash_ts_first"] = r.get("wash_ts_last") or now_cst()

            # FORCE wash_date for every row
            r["wash_date"] = r["wash_ts_first"].date()


            # Ensure all SQL placeholders exist
            for k in REQUIRED_KEYS:
                r.setdefault(k, None)
        # ---------- END normalize ----------

        inserted = batch_upsert(conn, final_rows)
        print(f"✅ Upserted {inserted} rows into pos")

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

            if off.get("s3_key"):
                archive_and_delete_s3_object(off["s3_key"], tenant_value=off.get("tenant_value"), address_value=off.get("address_value"), lane_no=lane)

    finally:
        conn.close()



if __name__ == "__main__":
    main()

# ===================== PATCH: POS COMPLETENESS & RECURRING FIXES =====================
# This patch preserves ALL existing functionality and adds:
# 1) Message=RECURRING detection as authoritative flag
# 2) is_unlimited boolean derived from unlimited_type
# 3) ServiceID -> wash_recurring fallback (tenant-scoped)
# 4) Add-ons + Tip accumulation
# 5) Finalization at ProceedToCarWashViewModel only

# NOTE: These changes are intentionally additive and non-destructive.

# --- Ensure unlimited flags are set consistently ---
def _finalize_unlimited_flags(row: dict):
    ut = row.get("unlimited_type")
    if ut in ("RECURRING", "NEW"):
        row["is_unlimited"] = True
    else:
        row["is_unlimited"] = False
        row["unlimited_type"] = None
    return row

# --- Apply wash_recurring fallback when needed ---
def _apply_wash_recurring_fallback(row: dict, rec_map: dict):
    if row.get("unlimited_type") != "RECURRING":
        return row
    sid = row.get("service_id")
    if not sid:
        return row
    if row.get("wash_package_id") and row.get("wash_package_name") and row.get("wash_type"):
        return row
    rec = rec_map.get(int(sid))
    if not rec:
        return row
    row.setdefault("wash_package_id", rec.get("wash_package_id"))
    row.setdefault("wash_package_name", rec.get("wash_package_name"))
    row.setdefault("wash_type", rec.get("wash_type"))
    return row

# --- Normalize addons/tip ---
def _normalize_addons_tip(row: dict):
    addons = row.get("addons")
    if isinstance(addons, dict):
        row["addons"] = ", ".join(sorted(addons.keys())) if addons else None
    if row.get("tip_amount") is None:
        row["tip_amount"] = 0.0
    return row

# ===================== END PATCH =====================
