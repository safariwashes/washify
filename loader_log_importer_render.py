import os
import re
import sys
import time
import logging
import boto3
import psycopg2
from datetime import datetime

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("loader-log-importer")

# =========================================================
# CONFIG
# =========================================================
S3_BUCKET = os.getenv("S3_BUCKET", "safari-franklin-data")
S3_PREFIX = "loader/"
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
SOURCE_NAME = "LoaderSequentialIngest"

DB_PARAMS = dict(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT", "5432"),
)

s3 = boto3.client("s3", region_name=AWS_REGION)

# =========================================================
# DB CONNECTION WITH RETRY
# =========================================================
def get_conn_with_retry(retries=6, delay=5):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return psycopg2.connect(**DB_PARAMS)
        except psycopg2.OperationalError as e:
            msg = str(e).lower()
            last_err = e
            transient = (
                "recovery mode" in msg
                or "not yet accepting connections" in msg
                or "terminating connection" in msg
                or "connection refused" in msg
            )
            if transient and attempt < retries:
                log.warning(f"DB unavailable ({attempt}/{retries}), retrying...")
                time.sleep(delay)
                continue
            raise
    raise last_err

# =========================================================
# HELPERS
# =========================================================
def parse_s3_context(key):
    m = re.search(r"tenant=([^/]+)/location=([^/]+)/", key)
    if not m:
        raise ValueError(f"Invalid S3 key (missing tenant/location): {key}")
    return m.group(1), m.group(2)

def parse_ts(ts):
    try:
        return datetime.strptime(ts.strip(), "%m/%d/%Y %I:%M:%S %p")
    except Exception:
        return None

def resolve_tenant_id(cur, tenant_slug):
    cur.execute(
        "SELECT tenant_id FROM tenants WHERE tenant_slug = %s",
        (tenant_slug.lower(),),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Tenant not found for slug={tenant_slug}")
    return row[0]

def resolve_location(cur, tenant_id, location_code):
    cur.execute(
        """
        SELECT location_id, location_code
        FROM locations
        WHERE tenant_id = %s
          AND location_code = %s
        """,
        (tenant_id, location_code),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(
            f"Location not found for tenant_id={tenant_id}, location_code={location_code}"
        )
    return row[0], row[1]

def safe_execute(cur, sql, params):
    cur.execute("SAVEPOINT line_sp")
    try:
        cur.execute(sql, params)
        cur.execute("RELEASE SAVEPOINT line_sp")
        return True
    except Exception as e:
        cur.execute("ROLLBACK TO SAVEPOINT line_sp")
        log.warning(f"Line skipped due to DB error: {e}")
        return False

# =========================================================
# CHECKPOINT (PERFORMANCE)
# =========================================================
def get_checkpoint(cur, tenant_id, location_code, file_type):
    cur.execute(
        """
        SELECT last_log_ts
        FROM loader_file_checkpoint
        WHERE tenant_id = %s
          AND location_code = %s
          AND file_type = %s
        """,
        (tenant_id, location_code, file_type),
    )
    row = cur.fetchone()
    return row[0] if row else None

def save_checkpoint(cur, tenant_id, location_code, file_type, ts):
    cur.execute(
        """
        INSERT INTO loader_file_checkpoint
            (tenant_id, location_code, file_type, last_log_ts)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (tenant_id, location_code, file_type)
        DO UPDATE SET
            last_log_ts = EXCLUDED.last_log_ts,
            updated_at = now()
        """,
        (tenant_id, location_code, file_type, ts),
    )

def write_heartbeat(cur, tenant_id, location_id):
    cur.execute(
        "INSERT INTO heartbeat (source, tenant_id, location_id) VALUES (%s,%s,%s)",
        (SOURCE_NAME, tenant_id, location_id),
    )

# =========================================================
# CONTROLLER LOG PROCESSOR
# =========================================================
def process_controller_log(cur, tenant_id, location_id, location_code, key, lines):
    last_ts = get_checkpoint(cur, tenant_id, location_code, "CONTROLLER")
    max_ts = last_ts

    for line in lines:
        if "Invoice Id" not in line:
            continue

        try:
            ts_raw, rest = line.split(",", 1)
            log_ts = parse_ts(ts_raw)
            if not log_ts or (last_ts and log_ts < last_ts):
                continue

            m = re.search(r"Invoice Id (\d+)", rest)
            if not m:
                continue

            invoice = int(m.group(1))

            # BILL (from POS → controller)
            if (
                "Class=CommonFunctions" in rest
                and "SendControllerCommandUsingCode" in rest
            ):
                safe_execute(
                    cur,
                    """
                    INSERT INTO loader_controller_log
                    (tenant_id, location_id, location_code,
                     log_ts, bill, pos_receipt, source_file)
                    VALUES (%s,%s,%s,%s,%s,NULL,%s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        tenant_id,
                        location_id,
                        location_code,
                        log_ts,
                        invoice,
                        key,
                    ),
                )

            # POS RECEIPT (controller → hardware)
            elif (
                "Class=RTCController" in rest
                and "CallRTCControllerByCode" in rest
            ):
                safe_execute(
                    cur,
                    """
                    UPDATE loader_controller_log
                     SET pos_receipt = %s
                     WHERE tenant_id = %s
                     AND location_id = %s
                     AND source_file = %s
                     AND bill IS NOT NULL
                     AND ABS(EXTRACT(EPOCH FROM (log_ts - %s))) <= 2
                     ORDER BY log_ts DESC
                     LIMIT 1
                    """,
                    (
                        invoice,          # POS receipt
                        tenant_id,
                        location_id,
                        key,
                        log_ts,
                    ),
                )

            if not max_ts or log_ts > max_ts:
                max_ts = log_ts

        except Exception as e:
            log.warning(f"ControllerLog skipped line: {e}")

    if max_ts and max_ts != last_ts:
        save_checkpoint(cur, tenant_id, location_code, "CONTROLLER", max_ts)

# =========================================================
# TRANSACTION LOG PROCESSOR
# =========================================================
def process_transaction_log(cur, tenant_id, location_id, location_code, key, lines):
    last_ts = get_checkpoint(cur, tenant_id, location_code, "TRANSACTION")
    max_ts = last_ts

    for line in lines:
        if not line.strip():
            continue

        try:
            ts_raw, rest = line.split(",", 1)
            log_ts = parse_ts(ts_raw)

            # ✅ allow same timestamp, skip only older
            if not log_ts or (last_ts and log_ts < last_ts):
                continue

            # Invoice Id is OPTIONAL in TransactionLog
            inv = re.search(r"Invoice\s*Id\s*(\d+)|InvoiceId\s*(\d+)", rest, re.IGNORECASE)
            bill = int(inv.group(1) or inv.group(2)) if inv else None

            safe_execute(
                cur,
                """
                INSERT INTO loader_transaction_log
                (
                    tenant_id,
                    location_id,
                    location_code,
                    log_ts,
                    bill,
                    raw_line,
                    source_file
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                (
                    tenant_id,
                    location_id,
                    location_code,
                    log_ts,
                    bill,                 -- may be NULL (this is OK)
                    rest.strip(),
                    key,
                ),
            )

            if not max_ts or log_ts > max_ts:
                max_ts = log_ts

        except Exception as e:
            log.warning(f"TransactionLog skipped line: {e}")

    if max_ts and max_ts != last_ts:
        save_checkpoint(cur, tenant_id, location_code, "TRANSACTION", max_ts)
# =========================================================
# MAIN
# =========================================================
def main():
    log.info("🚀 Loader log importer started")

    conn = get_conn_with_retry()
    cur = conn.cursor()

    try:
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)

        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if not key.lower().endswith(".txt"):
                continue

            k = key.lower()
            if "controllerlog" not in k and "transactionlog" not in k:
                continue

            try:
                tenant_slug, location_code = parse_s3_context(key)
                tenant_id = resolve_tenant_id(cur, tenant_slug)
                location_id, location_code = resolve_location(
                    cur, tenant_id, location_code
                )

                body = (
                    s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"]
                    .read()
                    .decode("utf-8", errors="ignore")
                )
                lines = body.splitlines()

                if "controllerlog" in k:
                    process_controller_log(
                        cur, tenant_id, location_id, location_code, key, lines
                    )
                else:
                    process_transaction_log(
                        cur, tenant_id, location_id, location_code, key, lines
                    )

                write_heartbeat(cur, tenant_id, location_id)
                conn.commit()

            except Exception as e:
                conn.rollback()
                log.error(f"❌ Failed processing {key}: {e}")

    finally:
        cur.close()
        conn.close()
        log.info("✅ Loader log importer completed")

if __name__ == "__main__":
    try:
        main()
    except psycopg2.OperationalError:
        log.error("DB unavailable — exiting")
        sys.exit(2)
