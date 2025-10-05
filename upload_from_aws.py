# upload_from_aws.py
import os
import re
import sys
import tempfile
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Dict, Any, Optional

import psycopg2
import psycopg2.extras

# AWS
import boto3
from botocore.config import Config

# timezone
import pytz

# Optional .env support
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ===================== CONFIG =====================
AWS_REGION  = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET   = os.getenv("S3_BUCKET", "safari-franklin-data")
S3_PREFIX   = os.getenv("S3_PREFIX", "kiosks/")
FILE_MATCH  = os.getenv("FILE_MATCH", "Transaction")

INPUT_PATH = os.getenv("INPUT_PATH")  # optional local override

def get_conn():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432"),
        sslmode=os.getenv("DB_SSLMODE", "require"),
    )

# ===================== MAPPINGS =====================
WASH_TYPE_MAP = {
    "INTERIOR SUP": "Super",
    "BEST WASH": "Best",
    "BETTER WASH": "Better",
    "GOOD WASH": "Good",
    "BASIC WASH": "Basic",
}
ALLOWED_WASH_TYPES = {"Basic", "Good", "Better", "Best", "Super"}

# ===================== REGEX =====================
TS_RE = re.compile(r"^\s*(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s+[AP]M)\s*,\s*")

INVOICE_INLINE_PAY_RE = re.compile(r"InvoiceID\s+(\d+)\s+Payment Type\s+([A-Za-z]+)", re.IGNORECASE)
PROCEED_INVOICE_RE    = re.compile(r"ProceedToCarWashViewModel.*?InvoiceID\s+(\d+)", re.IGNORECASE)
DO_TXN_RE             = re.compile(r"DoTransactionAfterDispatcher\s+(\d+)", re.IGNORECASE)
INVOICE_ANY_RE        = re.compile(r"InvoiceID\s+(\d+)", re.IGNORECASE)
INVOICE_FROM_AWS_RE   = re.compile(r"InvoiceId\s+(\d+)", re.IGNORECASE)

WASH_PKG_RE = re.compile(r"Wash Package\s+(\d+)\s+with Name\s+(.+)$", re.IGNORECASE)
PAYMENT_TYPE_RE = re.compile(r"Payment Type\s+([A-Za-z]+)", re.IGNORECASE)
AWS_FILE_RE = re.compile(r"Aws File Name\s+(.+)$", re.IGNORECASE)
LICENSE_PLATE_RE = re.compile(r"(?:License Plate|LICENSE PLATE)\s+([A-Z0-9]+)", re.IGNORECASE)
CUSTOMER_NAME_RE = re.compile(r"Customer Name\s+([^,]+)", re.IGNORECASE)
UNLIMITED_NEW_RE = re.compile(r"NEW CUSTOMER\s*->", re.IGNORECASE)
UNLIMITED_RECUR_RE = re.compile(r"RECURRING\s*->", re.IGNORECASE)

TIP_HEAD_RE = re.compile(r"^\s*TIP\b", re.IGNORECASE)
TIP_AMOUNT_RE = re.compile(r"\bTip\s*\$?\s*([0-9]+(?:\.[0-9]{1,2})?)\b", re.IGNORECASE)

DISCOUNT_BOTH_RE   = re.compile(r"Discount[:\s]+([A-Za-z0-9._-]+)\s+\$?([0-9]+(?:\.[0-9]{1,2})?)", re.IGNORECASE)
DISCOUNT_CODE_RE   = re.compile(r"Discount(?:\s+Code)?[:\s]+([A-Za-z][A-Za-z0-9._-]*)", re.IGNORECASE)
DISCOUNT_AMOUNT_RE = re.compile(r"Discount(?:\s+Amount)?[:\s]+\$?([0-9]+(?:\.[0-9]{1,2})?)", re.IGNORECASE)

TAX_RE   = re.compile(r"Tax[:\s]+\$?([0-9]+(?:\.[0-9]{1,2})?)\b", re.IGNORECASE)
TOTAL_RE = re.compile(r"Total[:\s]+\$?([0-9]+(?:\.[0-9]{1,2})?)\b", re.IGNORECASE)

# ===================== HELPERS =====================
def now_cst_date():
    return datetime.now(pytz.timezone("US/Central")).date()

def now_cst_time():
    return datetime.now(pytz.timezone("US/Central")).time()

def parse_ts(line: str) -> Tuple[Optional[datetime], str]:
    m = TS_RE.match(line)
    if not m:
        return None, line
    ts = datetime.strptime(m.group(1), "%m/%d/%Y %I:%M:%S %p")
    return ts, line[m.end():]

def infer_location_from_filename(p: Path) -> str:
    m = re.search(r"safariexpresswash_(.+?)_\d+_Transaction", p.name, flags=re.IGNORECASE)
    return m.group(1) if m else ""

def map_wash_type(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    up = name.upper()
    mapped = None
    for key, val in WASH_TYPE_MAP.items():
        if key in up:
            mapped = val
            break
    return mapped if mapped in ALLOWED_WASH_TYPES else None

def is_tip_text(txt: str) -> bool:
    return bool(TIP_HEAD_RE.search(txt or ""))

def tip_amount_from_text(txt: str) -> float:
    m = TIP_AMOUNT_RE.search(txt or "")
    if not m:
        return 0.0
    try:
        return float(m.group(1))
    except Exception:
        return 0.0

def safe_float(s: Optional[str]) -> Optional[float]:
    try:
        return float(s) if s is not None else None
    except Exception:
        return None

# ===================== PARSER =====================
def parse_file(path: Path) -> List[Dict[str, Any]]:
    location = infer_location_from_filename(path)
    sessions = []
    sess = None
    session_counter = 0

    def new_session(ts: Optional[datetime]):
        return {
            "invoice": None,
            "first_ts": ts,
            "last_ts": ts,
            "customer_name": None,
            "license_plate": None,
            "wash_package_id": None,
            "wash_package_name": None,
            "payment_type": None,
            "payment_type_ts": None,
            "image_path": None,
            "is_unlimited": False,
            "unlimited_type": None,
            "unlimited_ts": None,
            "addon_map": {},
            "addons": [],
            "tip_amount": 0.0,
            "tip_ts": None,
            "discount_code": None,
            "discount_amount": None,
            "tax": None,
            "total": None,
        }

    def end_session(ts: Optional[datetime]):
        nonlocal sess, session_counter
        if not sess:
            return
        if ts and (not sess["last_ts"] or ts > sess["last_ts"]):
            sess["last_ts"] = ts
        sessions.append({**sess, "session_index": session_counter})
        sess = None
        session_counter += 1

    def set_unlimited(flag_type: str, ts: Optional[datetime]):
        if (not sess["unlimited_ts"]) or (ts and ts >= sess["unlimited_ts"]) or (flag_type == "RECURRING"):
            sess["is_unlimited"] = True
            if (flag_type == "RECURRING") or (sess["unlimited_type"] is None):
                sess["unlimited_type"] = flag_type
            sess["unlimited_ts"] = ts or sess["unlimited_ts"]

    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
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

            # Invoice detection
            for regex in [INVOICE_INLINE_PAY_RE, DO_TXN_RE, PROCEED_INVOICE_RE,
                          INVOICE_ANY_RE, INVOICE_FROM_AWS_RE]:
                m = regex.search(content)
                if m and not sess["invoice"]:
                    inv = m.group(1)
                    if inv != "0":
                        sess["invoice"] = inv

            # Unlimited flags
            if UNLIMITED_NEW_RE.search(content):
                set_unlimited("NEW", ts)
            if UNLIMITED_RECUR_RE.search(content):
                set_unlimited("RECURRING", ts)

            # Customer & Plate
            m = CUSTOMER_NAME_RE.search(content)
            if m and not sess["customer_name"]:
                name = re.sub(r"\s{2,}", " ", m.group(1).strip())
                sess["customer_name"] = name

            m = LICENSE_PLATE_RE.search(content)
            if m and not sess["license_plate"]:
                sess["license_plate"] = m.group(1).strip().upper()

            # Wash package
            if "ServiceControlViewModel" in content and "SelectServiceBlock" in content:
                m = WASH_PKG_RE.search(content)
                if m:
                    pkg_id, pkg_name = m.group(1).strip(), m.group(2).strip().rstrip(".")
                    if not is_tip_text(pkg_name):
                        sess["wash_package_id"] = pkg_id
                        sess["wash_package_name"] = pkg_name

            # Add-ons
            if "SelectOptionalServiceBlock" in content:
                m = WASH_PKG_RE.search(content)
                if m:
                    add_pkg_id, add_name = m.group(1).strip(), m.group(2).strip().rstrip(".")
                    if add_name:
                        if add_pkg_id != sess["wash_package_id"] and add_name != (sess["wash_package_name"] or ""):
                            sess["addon_map"][add_pkg_id] = {"name": add_name, "ts": ts}
                            amt = tip_amount_from_text(add_name)
                            if amt > 0:
                                if (sess["tip_ts"] is None) or (ts and ts >= sess["tip_ts"]):
                                    sess["tip_amount"] = amt
                                    sess["tip_ts"] = ts

            # Payment
            if "SaveTransactions" in content and "SaveTransaction" in content:
                m = PAYMENT_TYPE_RE.search(content)
                if m:
                    ptype = m.group(1).strip()
                    if not sess["payment_type_ts"] or (ts and ts >= sess["payment_type_ts"]):
                        sess["payment_type"] = ptype
                        sess["payment_type_ts"] = ts

            # Image path
            m = AWS_FILE_RE.search(content)
            if m and not sess["image_path"]:
                sess["image_path"] = m.group(1).strip()

            # Discount / Tax / Total
            for regex, attr in [(DISCOUNT_BOTH_RE, "discount_amount"),
                                (DISCOUNT_AMOUNT_RE, "discount_amount"),
                                (TAX_RE, "tax"),
                                (TOTAL_RE, "total")]:
                m = regex.search(content)
                if m:
                    if attr == "discount_amount":
                        sess["discount_amount"] = safe_float(m.groups()[-1])
                    elif attr == "tax":
                        sess["tax"] = safe_float(m.group(1))
                    elif attr == "total":
                        sess["total"] = safe_float(m.group(1))

            # End markers
            if ("ProceedToCarWashViewModel" in content and "ReturnToMainScreen" in content) or \
               ("TransactionMethods" in content and "ResetTransaction" in content):
                end_session(ts)

    # Build rows
    rows: List[Dict[str, Any]] = []
    for s in sessions:
        if not s["invoice"] or s["invoice"] == "0":
            continue

        clean_addons = []
        for info in sorted(s["addon_map"].values(), key=lambda x: (x["ts"] or datetime.min)):
            clean_addons.append(info["name"])
        addons_text = "; ".join(clean_addons) if clean_addons else None

        kind = "NORMAL"
        if s["is_unlimited"]:
            if s.get("session_index", 0) == 0:
                kind = "SIGNUP"
            else:
                kind = "WASH"

        rows.append({
            "bill": int(s["invoice"]),
            "wash_ts_first": s["first_ts"],
            "wash_ts_last": s["last_ts"],
            "license_plate": s["license_plate"],
            "customer_name": s["customer_name"],
            "wash_package_id": int(s["wash_package_id"]) if s["wash_package_id"] else None,
            "wash_package_name": s["wash_package_name"],
            "wash_type": map_wash_type(s["wash_package_name"]),
            "payment_type": s["payment_type"],
            "image_path": s["image_path"],
            "is_unlimited": s["is_unlimited"],
            "unlimited_type": s["unlimited_type"],
            "addons": addons_text,
            "tip_amount": float(s["tip_amount"] or 0.0),
            "discount_code": s["discount_code"],
            "discount_amount": s["discount_amount"],
            "tax": s["tax"],
            "total": s["total"],
            "location": location,
            "source_file": path.name,
            "created_on": now_cst_date(),
            "created_at": now_cst_time(),
            "invoice_kind": kind,
        })
    return rows

# ===================== DDL & UPSERT =====================
DDL_SQL = """
CREATE TABLE IF NOT EXISTS washify (
  bill               BIGINT PRIMARY KEY,
  wash_ts_first      TIMESTAMP,
  wash_ts_last       TIMESTAMP,
  wash_date          DATE GENERATED ALWAYS AS (CAST(wash_ts_first AS DATE)) STORED,
  license_plate      TEXT,
  customer_name      TEXT,
  wash_package_id    INTEGER,
  wash_package_name  TEXT,
  wash_type          TEXT CHECK (wash_type IN ('Basic','Good','Better','Best','Super') OR wash_type IS NULL),
  payment_type       TEXT,
  image_path         TEXT,
  is_unlimited       BOOLEAN,
  unlimited_type     TEXT CHECK (unlimited_type IN ('NEW','RECURRING') OR unlimited_type IS NULL),
  addons             TEXT,
  tip_amount         NUMERIC(8,2) DEFAULT 0.00,
  discount_code      TEXT,
  discount_amount    NUMERIC(8,2),
  tax                NUMERIC(8,2),
  total              NUMERIC(8,2),
  location           TEXT,
  source_file        TEXT,
  created_on         DATE,
  created_at         TIME,
  invoice_kind       TEXT CHECK (invoice_kind IN ('NORMAL','SIGNUP','WASH')) DEFAULT 'NORMAL'
);
CREATE INDEX IF NOT EXISTS washify_idx_ts_first      ON washify (wash_ts_first);
CREATE INDEX IF NOT EXISTS washify_idx_ts_last       ON washify (wash_ts_last);
CREATE INDEX IF NOT EXISTS washify_idx_plate_upper   ON washify ((upper(license_plate)));
CREATE INDEX IF NOT EXISTS washify_idx_location      ON washify (location);
CREATE INDEX IF NOT EXISTS washify_idx_wash_date     ON washify (wash_date);
CREATE INDEX IF NOT EXISTS washify_idx_discount_code ON washify (discount_code);
"""

UPSERT_SQL = """
INSERT INTO washify (
  bill, wash_ts_first, wash_ts_last, license_plate, customer_name,
  wash_package_id, wash_package_name, wash_type, payment_type, image_path,
  is_unlimited, unlimited_type, addons, tip_amount,
  discount_code, discount_amount, tax, total,
  location, source_file, created_on, created_at, invoice_kind
) VALUES (
  %(bill)s, %(wash_ts_first)s, %(wash_ts_last)s, %(license_plate)s, %(customer_name)s,
  %(wash_package_id)s, %(wash_package_name)s, %(wash_type)s, %(payment_type)s, %(image_path)s,
  %(is_unlimited)s, %(unlimited_type)s, %(addons)s, %(tip_amount)s,
  %(discount_code)s, %(discount_amount)s, %(tax)s, %(total)s,
  %(location)s, %(source_file)s, %(created_on)s, %(created_at)s, %(invoice_kind)s
)
ON CONFLICT (bill) DO UPDATE SET
  wash_ts_first      = EXCLUDED.wash_ts_first,
  wash_ts_last       = EXCLUDED.wash_ts_last,
  license_plate      = EXCLUDED.license_plate,
  customer_name      = EXCLUDED.customer_name,
  wash_package_id    = EXCLUDED.wash_package_id,
  wash_package_name  = EXCLUDED.wash_package_name,
  wash_type          = EXCLUDED.wash_type,
  payment_type       = EXCLUDED.payment_type,
  image_path         = EXCLUDED.image_path,
  is_unlimited       = EXCLUDED.is_unlimited,
  unlimited_type     = EXCLUDED.unlimited_type,
  addons             = EXCLUDED.addons,
  tip_amount         = EXCLUDED.tip_amount,
  discount_code      = EXCLUDED.discount_code,
  discount_amount    = EXCLUDED.discount_amount,
  tax                = EXCLUDED.tax,
  total              = EXCLUDED.total,
  location           = EXCLUDED.location,
  source_file        = EXCLUDED.source_file,
  created_on         = EXCLUDED.created_on,
  created_at         = EXCLUDED.created_at,
  invoice_kind       = EXCLUDED.invoice_kind;
"""

def create_table_if_needed(conn):
    with conn.cursor() as cur:
        cur.execute(DDL_SQL)
    conn.commit()

def batch_upsert(conn, rows: List[Dict[str, Any]], batch_size: int = 500):
    if not rows:
        return 0
    total = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i+batch_size]
            psycopg2.extras.execute_batch(cur, UPSERT_SQL, chunk, page_size=len(chunk))
            total += len(chunk)
    conn.commit()
    return total

# ===================== S3 HELPERS =====================
s3 = boto3.client("s3", region_name=AWS_REGION, config=Config(signature_version="s3v4"))

def latest_s3_object(prefix: str, file_match: str) -> Optional[dict]:
    newest = None
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if file_match in os.path.basename(key):
                if newest is None or obj["LastModified"] > newest["LastModified"]:
                    newest = obj
    return newest

def download_s3_to_temp(key: str) -> Path:
    basename = os.path.basename(key)
    local_path = Path(tempfile.gettempdir()) / basename
    s3.download_file(S3_BUCKET, key, str(local_path))
    return local_path

def delete_s3_object(key: str):
    s3.delete_object(Bucket=S3_BUCKET, Key=key)

# ===================== RUN =====================
def gather_input_files_local(input_path: str) -> List[Path]:
    p = Path(input_path)
    if p.is_file():
        return [p]
    if p.is_dir():
        return sorted(p.glob("*.txt"))
    raise FileNotFoundError(f"Input path not found: {input_path}")

def main():
    from_s3 = False
    s3_key = None
    files: List[Path] = []

    if INPUT_PATH:
        files = gather_input_files_local(INPUT_PATH)
    else:
        obj = latest_s3_object(S3_PREFIX, FILE_MATCH)
        if not obj:
            print("No Transaction files in S3.")
            return
        s3_key = obj["Key"]
        print(f"Downloading s3://{S3_BUCKET}/{s3_key} ...")
        local_path = download_s3_to_temp(s3_key)
        files = [local_path]
        from_s3 = True

    all_rows: List[Dict[str, Any]] = []
    for f in files:
        all_rows.extend(parse_file(f))

    dedup: Dict[Tuple[int, str], Dict[str, Any]] = {}
    for r in all_rows:
        key = (r["bill"], r["source_file"])
        dedup[key] = r
    final_rows = list(dedup.values())

    print(f"Parsed {len(all_rows)} raw rows → {len(final_rows)} after dedup")

    conn = get_conn()
    try:
        create_table_if_needed(conn)
        inserted = batch_upsert(conn, final_rows)
        print(f"✅ Upserted {inserted} rows into washify")
    finally:
        conn.close()

    if from_s3 and s3_key:
        delete_s3_object(s3_key)
        print(f"🗑️ Deleted s3://{S3_BUCKET}/{s3_key}")

if __name__ == "__main__":
    main()
