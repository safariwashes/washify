# upload_from_aws.py  (FINAL REVISED VERSION WITH FILENAME-BASED LOCATION/LANE)

import os
import re
import tempfile
from pathlib import Path
from datetime import datetime
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
AWS_REGION  = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET   = os.getenv("S3_BUCKET", "safari-franklin-data")
S3_PREFIX   = os.getenv("S3_PREFIX", "kiosks/")  # e.g. kiosks/, etc.
FILE_MATCH  = os.getenv("FILE_MATCH", "Transaction")

# Local override for testing (file or directory)
INPUT_PATH = os.getenv("INPUT_PATH")


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

INVOICE_INLINE_PAY_RE = re.compile(
    r"InvoiceID\s+(\d+)\s+Payment Type\s+([A-Za-z]+)", re.IGNORECASE
)
PROCEED_INVOICE_RE = re.compile(
    r"ProceedToCarWashViewModel.*?InvoiceID\s+(\d+)", re.IGNORECASE
)
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

TIP_HEAD_RE   = re.compile(r"^\s*TIP\b", re.IGNORECASE)
TIP_AMOUNT_RE = re.compile(r"\bTip\s*\$?\s*([0-9]+(?:\.[0-9]{1,2})?)\b", re.IGNORECASE)

DISCOUNT_BOTH_RE   = re.compile(r"Discount[:\s]+([A-Za-z0-9._-]+)\s+\$?([0-9]+(?:\.[0-9]{1,2})?)", re.IGNORECASE)
DISCOUNT_CODE_RE   = re.compile(r"Discount(?:\s+Code)?[:\s]+([A-Za-z][A-Za-z0-9._-]*)", re.IGNORECASE)
DISCOUNT_AMOUNT_RE = re.compile(r"Discount(?:\s+Amount)?[:\s]+\$?([0-9]+(?:\.[0-9]{1,2})?)", re.IGNORECASE)

TAX_RE   = re.compile(r"Tax[:\s]+\$?([0-9]+(?:\.[0-9]{1,2})?)\b", re.IGNORECASE)
TOTAL_RE = re.compile(r"Total[:\s]+\$?([0-9]+(?:\.[0-9]{1,2})?)\b", re.IGNORECASE)

# ===================== TIME HELPERS =====================
def now_cst_date():
    return datetime.now(ZoneInfo("America/Chicago")).date()

def now_cst_time():
    return datetime.now(ZoneInfo("America/Chicago")).time()

def parse_ts(line: str) -> Tuple[Optional[datetime], str]:
    """
    Extracts the leading timestamp (if present) and returns (datetime, rest_of_line).
    """
    m = TS_RE.match(line)
    if not m:
        return None, line
    ts = datetime.strptime(m.group(1), "%m/%d/%Y %I:%M:%S %p")
    return ts, line[m.end():]

def map_wash_type(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    up = name.upper()
    for key, val in WASH_TYPE_MAP.items():
        if key in up:
            return val
    return None

# ===================== LOCATION / LANE DETECTION (FILENAME-BASED) =====================
def infer_site_and_lane_from_filename(filename: str) -> Tuple[Optional[str], Optional[int]]:
    """
    Determine site ('FRA' or 'NSH') and lane_no based on the filename itself.

    Franklin:
      safariexpresswash_1004 Center Point Pl_1_TransactionMM-DD-YY.txt  → FRA, lane 1
      safariexpresswash_1004 Center Point Pl_2_TransactionMM-DD-YY.txt  → FRA, lane 2

    Nashville:
      safariexpresswash_306 White Bridge Pike_1_TransactionMM-DD-YY.txt → NSH, lane 1
      safariexpresswash_306 White Bridge Pike_2_TransactionMM-DD-YY.txt → NSH, lane 2
    """
    fn = filename.lower()

    site = None
    if "1004 center point pl" in fn:
        site = "FRA"
    elif "306 white bridge pike" in fn:
        site = "NSH"

    lane_no = None
    m = re.search(r"_(\d+)_transaction", fn)
    if m:
        try:
            lane_no = int(m.group(1))
        except Exception:
            lane_no = None

    return site, lane_no

# ===================== REVERSE SEARCH =====================
def find_start_index_for_lines(lines: List[str], last_bill: Optional[int]) -> int:
    """
    Search backwards in the lines to find the last occurrence of last_bill,
    then return the index to start parsing from. If not found, return 0.
    """
    if last_bill is None:
        return 0

    target = str(last_bill)
    for idx in range(len(lines) - 1, -1, -1):
        raw = lines[idx].strip()
        if not raw:
            continue
        _, content = parse_ts(raw)
        for regex in INVOICE_SEARCH_RES:
            m = regex.search(content)
            if m and m.group(1) == target:
                return idx
    return 0

# ===================== PARSER =====================
def parse_file(
    path: Path,
    start_index: int = 0,
    preloaded_lines: Optional[List[str]] = None,
    site_code: Optional[str] = None,
    lane_no: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Parse a single Transaction log file into washify rows.
    site_code: 'FRA' or 'NSH' (location for DB).
    lane_no: integer lane number (1,2,...).
    start_index: line index to start parsing from (for reverse-search optimization).
    """

    sessions = []
    sess = None

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

            "addon_map": {},  # pkg_id -> {name, ts}
            "addons": [],

            "tip_amount": 0.0,
            "tip_ts": None,

            "discount_code": None,
            "discount_amount": None,
            "tax": None,
            "total": None,

            "saw_unlimited_signature": False,
            "saw_creditcard_unlimited": False,
            "saw_unlimited_pkg_name": False,
        }

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

    # Iterate lines from start_index
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

        # ----- Invoice detection -----
        for regex in INVOICE_SEARCH_RES:
            m = regex.search(content)
            if m and not sess["invoice"]:
                inv = m.group(1)
                if inv != "0":
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
            sess["customer_name"] = re.sub(r"\s{2,}", " ", m.group(1).strip())

        # ----- Plate -----
        m = LICENSE_PLATE_RE.search(content)
        if m and not sess["license_plate"]:
            sess["license_plate"] = m.group(1).strip().upper()

        # ----- Main Wash Package -----
        if "ServiceControlViewModel" in content and "SelectServiceBlock" in content:
            m = WASH_PKG_RE.search(content)
            if m:
                pkg_id = m.group(1).strip()
                pkg_name = m.group(2).strip().rstrip(".")
                sess["wash_package_id"] = pkg_id
                sess["wash_package_name"] = pkg_name
                if "UNLIMITED" in pkg_name.upper():
                    sess["saw_unlimited_pkg_name"] = True

        # ----- Add-ons -----
        if "SelectOptionalServiceBlock" in content:
            m = WASH_PKG_RE.search(content)
            if m:
                add_pkg_id = m.group(1).strip()
                add_name = m.group(2).strip().rstrip(".")
                if add_name:
                    sess["addon_map"][add_pkg_id] = {"name": add_name, "ts": ts}

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

        # ----- End-of-transaction markers -----
        if ("ProceedToCarWashViewModel" in content and "ReturnToMainScreen" in content) or \
           ("TransactionMethods" in content and "ResetTransaction" in content):
            end_session(ts)

    # ----- Convert sessions → DB rows -----
    rows: List[Dict[str, Any]] = []
    for s in sessions:
        if not s["invoice"] or s["invoice"] == "0":
            continue

        # Add-ons in time order
        sorted_addons = sorted(
            s["addon_map"].values(),
            key=lambda x: (x["ts"] or datetime.min)
        )
        addons_text = "; ".join([a["name"] for a in sorted_addons]) if sorted_addons else None

        # Unlimited classification
        strong_signup = s["saw_unlimited_signature"] or s["saw_creditcard_unlimited"]
        strong_wash   = s["saw_unlimited_pkg_name"]

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
            "is_unlimited": is_unl,
            "unlimited_type": s["unlimited_type"] if is_unl else None,
            "addons": addons_text,
            "tip_amount": float(s["tip_amount"] or 0),
            "discount_code": s["discount_code"],
            "discount_amount": s["discount_amount"],
            "tax": s["tax"],
            "total": s["total"],
            "location": site_code,        # 'FRA' or 'NSH'
            "lane_no": lane_no,           # 1, 2, ...
            "source_file": path.name,
            "created_on": now_cst_date(),
            "created_at": now_cst_time(),
            "invoice_kind": invoice_kind,
        })

    return rows

# ===================== DDL =====================
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
  unlimited_type     TEXT CHECK (unlimited_type IN ('NEW','RECURRING') OR washify.unlimited_type IS NULL),
  addons             TEXT,
  tip_amount         NUMERIC(8,2) DEFAULT 0.00,
  discount_code      TEXT,
  discount_amount    NUMERIC(8,2),
  tax                NUMERIC(8,2),
  total              NUMERIC(8,2),
  location           TEXT,
  lane_no            INTEGER,
  source_file        TEXT,
  created_on         DATE,
  created_at         TIME,
  invoice_kind       TEXT CHECK (invoice_kind IN ('NORMAL','SIGNUP','WASH')) DEFAULT 'NORMAL'
);
CREATE INDEX IF NOT EXISTS washify_idx_ts_first      ON washify (wash_ts_first);
CREATE INDEX IF NOT EXISTS washify_idx_ts_last       ON washify (wash_ts_last);
CREATE INDEX IF NOT EXISTS washify_idx_location      ON washify (location);
CREATE INDEX IF NOT EXISTS washify_idx_lane_no       ON washify (lane_no);
CREATE INDEX IF NOT EXISTS washify_idx_wash_date     ON washify (wash_date);
"""

UPSERT_SQL = """
INSERT INTO washify (
  bill, wash_ts_first, wash_ts_last, license_plate, customer_name,
  wash_package_id, wash_package_name, wash_type, payment_type, image_path,
  is_unlimited, unlimited_type, addons, tip_amount,
  discount_code, discount_amount, tax, total,
  location, lane_no, source_file, created_on, created_at, invoice_kind
) VALUES (
  %(bill)s, %(wash_ts_first)s, %(wash_ts_last)s, %(license_plate)s, %(customer_name)s,
  %(wash_package_id)s, %(wash_package_name)s, %(wash_type)s, %(payment_type)s, %(image_path)s,
  %(is_unlimited)s, %(unlimited_type)s, %(addons)s, %(tip_amount)s,
  %(discount_code)s, %(discount_amount)s, %(tax)s, %(total)s,
  %(location)s, %(lane_no)s, %(source_file)s, %(created_on)s, %(created_at)s, %(invoice_kind)s
)
ON CONFLICT (bill) DO NOTHING;
"""

def create_table_if_needed(conn):
    with conn.cursor() as cur:
        cur.execute(DDL_SQL)
    conn.commit()

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

# ===================== LAST BILL (BY LOCATION & TODAY) =====================
def get_last_bill_for_today_by_location(conn, site_code: Optional[str]) -> Optional[int]:
    """
    Find today's latest bill for a given site ('FRA' or 'NSH').
    If site_code is None, returns None (no filtering).
    """
    if not site_code:
        return None

    today = now_cst_date()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT bill
            FROM washify
            WHERE created_on = %s
              AND location = %s
            ORDER BY wash_ts_last DESC
            LIMIT 1
            """,
            (today, site_code),
        )
        row = cur.fetchone()
        return row[0] if row else None

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

# ===================== INPUT GATHERING =====================
def gather_input_files_local(input_path: str) -> List[Path]:
    p = Path(input_path)
    if p.is_file():
        return [p]
    if p.is_dir():
        return sorted(p.glob("*.txt"))
    raise FileNotFoundError(f"Input path not found: {input_path}")

# ===================== MAIN =====================
def main():
    from_s3 = False
    s3_key = None
    file_entries: List[Tuple[Path, str]] = []  # (local_path, source_hint)

    if INPUT_PATH:
        # Local testing mode
        files = gather_input_files_local(INPUT_PATH)
        for f in files:
            file_entries.append((f, str(f)))  # hint = full local path
    else:
        # S3 mode - pick latest object under prefix
        obj = latest_s3_object(S3_PREFIX, FILE_MATCH)
        if not obj:
            print("No Transaction files in S3.")
            return
        s3_key = obj["Key"]
        print(f"Downloading s3://{S3_BUCKET}/{s3_key} ...")
        local_path = download_s3_to_temp(s3_key)
        file_entries.append((local_path, s3_key))  # hint = S3 key
        from_s3 = True

    if not file_entries:
        print("No input files found.")
        return

    conn = get_conn()
    try:
        create_table_if_needed(conn)

        all_rows: List[Dict[str, Any]] = []

        for local_path, source_hint in file_entries:
            # Determine site & lane from the FILENAME (works for local + S3 temp)
            site_code, lane_no = infer_site_and_lane_from_filename(local_path.name)
            print(f"File: {local_path.name} → site={site_code}, lane={lane_no}")

            # Get last_bill for this site & today
            last_bill = get_last_bill_for_today_by_location(conn, site_code)
            if last_bill is not None:
                print(f"Today's last bill for site {site_code}: {last_bill}")
            else:
                print(f"No existing rows today for site {site_code}; parsing full file.")

            # Load lines once
            with local_path.open("r", encoding="utf-8", errors="ignore") as fh:
                lines = fh.readlines()

            # Compute starting index via reverse search
            start_idx = find_start_index_for_lines(lines, last_bill)
            if last_bill is not None:
                if start_idx > 0:
                    print(f"Found last_bill={last_bill} in {local_path.name} at line {start_idx}, parsing forward from there.")
                else:
                    print(f"Did NOT find last_bill={last_bill} in {local_path.name}, parsing entire file.")
            else:
                print(f"Parsing entire file {local_path.name} (no last_bill).")

            parsed_rows = parse_file(
                local_path,
                start_index=start_idx,
                preloaded_lines=lines,
                site_code=site_code,
                lane_no=lane_no,
            )
            all_rows.extend(parsed_rows)

        # De-dup: (bill, source_file)
        dedup: Dict[Tuple[int, str], Dict[str, Any]] = {}
        for r in all_rows:
            key = (r["bill"], r["source_file"])
            dedup[key] = r
        final_rows = list(dedup.values())
        print(f"Parsed {len(all_rows)} rows → {len(final_rows)} after de-dup")

        inserted = batch_upsert(conn, final_rows)
        print(f"✅ Upserted {inserted} rows into washify")

    finally:
        conn.close()

    # Delete S3 object only after successful DB write
    if from_s3 and s3_key:
        delete_s3_object(s3_key)
        print(f"🗑️ Deleted s3://{S3_BUCKET}/{s3_key}")

if __name__ == "__main__":
    main()
