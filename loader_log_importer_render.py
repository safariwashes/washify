import os
import re
import sys
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
log = logging.getLogger("loader-sequential")

# =========================================================
# CONFIG
# =========================================================
S3_BUCKET = "safari-franklin-data"
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
# DB
# =========================================================
def get_conn():
    return psycopg2.connect(**DB_PARAMS)

# =========================================================
# HELPERS
# =========================================================
def parse_s3_context(key: str):
    """
    Expected S3 layout:
      loader/tenant=<tenant_uuid>/location=<CODE>/...
    """
    m = re.search(r"tenant=([^/]+)/location=([^/]+)/", key)
    if not m:
        raise ValueError(f"Invalid S3 key (missing tenant/location): {key}")
    return m.group(1), m.group(2)


def parse_ts(ts: str):
    try:
        return datetime.strptime(ts.strip(), "%m/%d/%Y %I:%M:%S %p")
    except Exception:
        return None


def resolve_location_id(cur, tenant_id, location_code):
    cur.execute(
        """
        SELECT location_id
          FROM tenant_location
         WHERE tenant_id = %s
           AND location = %s
        """,
        (tenant_id, location_code),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(
            f"Location not found for tenant={tenant_id}, location={location_code}"
        )
    return row[0]


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
        DO UPDATE
           SET last_log_ts = EXCLUDED.last_log_ts,
               updated_at = now()
        """,
        (tenant_id, location_code, file_type, ts),
    )


def write_heartbeat(cur, tenant_id, location_id):
    cur.execute(
        """
        INSERT INTO heartbeat (source, tenant_id, location_id)
        VALUES (%s, %s, %s)
        """,
        (SOURCE_NAME, tenant_id, location_id),
    )

# =========================================================
# PARSERS
# =========================================================
def process_controller_log(cur, tenant_id, location_code, key, lines):
    last_ts = get_checkpoint(cur, tenant_id, location_code, "CONTROLLER")
    max_ts = last_ts

    for line in lines:
        if "Invoice Id" not in line:
            continue

        try:
            ts_raw, rest = line.split(",", 1)
            log_ts = parse_ts(ts_raw)
            if not log_ts:
                continue

            if last_ts and log_ts <= last_ts:
                continue

            m = re.search(r"Invoice Id (\d+)", rest)
            if not m:
                continue

            invoice_id = int(m.group(1))

            if "RTC True" in rest:
                event_type = "RTC_TRUE"
            elif "CallRTCControllerByCode" in rest:
                event_type = "CALL_CONTROLLER"
            else:
                continue

            cur.execute(
                """
                INSERT INTO loader_controller_log
                    (tenant_id, location_code, log_ts,
                     event_type, invoice_id, source_file)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (
                    tenant_id,
                    location_code,
                    log_ts,
                    event_type,
                    invoice_id,
                    key,
                ),
            )

            if not max_ts or log_ts > max_ts:
                max_ts = log_ts

        except Exception as e:
            log.warning(f"ControllerLog skipped line: {e}")

    if max_ts and max_ts != last_ts:
        save_checkpoint(cur, tenant_id, location_code, "CONTROLLER", max_ts)


def process_transaction_log(cur, tenant_id, location_code, key, lines):
    last_ts = get_checkpoint(cur, tenant_id, location_code, "TRANSACTION")
    max_ts = last_ts

    for line in lines:
        if not line.strip():
            continue

        try:
            ts_raw, rest = line.split(",", 1)
            log_ts = parse_ts(ts_raw)
            if not log_ts:
                continue

            if last_ts and log_ts <= last_ts:
                continue

            cls = re.search(r"Class=([^,]+)", rest)
            mth = re.search(r"Method=([^,]+)", rest)
            msg = re.search(r"Message=(.*)", rest)
            inv = re.search(r"InvoiceId (\d+)", rest)

            cur.execute(
                """
                INSERT INTO loader_transaction_log
                    (tenant_id, location_code, log_ts,
                     class_name, method_name, message,
                     invoice_id, source_file)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    tenant_id,
                    location_code,
                    log_ts,
                    cls.group(1) if cls else None,
                    mth.group(1) if mth else None,
                    msg.group(1) if msg else None,
                    int(inv.group(1)) if inv else None,
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
    log.info("🚀 Loader sequential ingestion started")

    conn = get_conn()
    cur = conn.cursor()

    try:
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)

        for obj in resp.get("Contents", []):
            key = obj["Key"]

            if not key.lower().endswith(".txt"):
                continue

            try:
                tenant_id, location_code = parse_s3_context(key)
                location_id = resolve_location_id(cur, tenant_id, location_code)

                body = (
                    s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"]
                    .read()
                    .decode("utf-8", errors="ignore")
                )
                lines = body.splitlines()

                if "controllerlog" in key.lower():
                    log.info(f"📄 ControllerLog → {key}")
                    process_controller_log(
                        cur, tenant_id, location_code, key, lines
                    )

                elif "transactionlog" in key.lower():
                    log.info(f"📄 TransactionLog → {key}")
                    process_transaction_log(
                        cur, tenant_id, location_code, key, lines
                    )

                write_heartbeat(cur, tenant_id, location_id)
                conn.commit()

            except Exception as e:
                conn.rollback()
                log.error(f"❌ Failed processing {key}: {e}")

    finally:
        cur.close()
        conn.close()
        log.info("✅ Loader sequential ingestion completed")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.critical(f"🔥 Fatal error: {e}")
        sys.exit(1)
