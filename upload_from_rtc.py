import os
import sys
import re
import html as htmlmod
import boto3
import psycopg2
from datetime import datetime
from botocore.exceptions import ClientError

# ------------------------------------------------------------
# Safari RTC (Laguna) Log Importer for Render
# ------------------------------------------------------------
# Triggered by Flask /rtc route
# - Downloads xmlInterfaceLog0.html from S3
# - Parses wash IDs, washPkgNum, direction, timestamp
# - Inserts into PostgreSQL table: rtc_log
# - If parsing yields 0 entries, uploads to rtc/unparsed/ for review
# ------------------------------------------------------------

# ---------- CONFIG ----------
S3_BUCKET = sys.argv[1] if len(sys.argv) > 1 else os.getenv("RTC_S3_BUCKET", "safari-franklin-data")
S3_KEY    = sys.argv[2] if len(sys.argv) > 2 else None

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

s3 = boto3.client("s3")

# ---------- DB CONNECT ----------
def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

def parse_rtc_log(content):
    """
    Parser for Laguna xmlInterfaceLog0.html (RTC).
    Handles both compact and spaced timestamp formats,
    including plain numeric lines (no XML tags).
    Extracts wash_id, washPkgNum, timestamp, IP, direction.
    """

    import re
    from datetime import datetime
    import html as htmlmod

    # --- Unescape and normalize ---
    content = htmlmod.unescape(content)
    content = content.replace(" ", "")
    content = content.replace("\xa0", " ")
    content = re.sub(r"<[^>]+>", " ", content)
    content = re.sub(r"[\u2010-\u2015\u2212\uFE58\uFE63\uFF0D]", "-", content)
    content = re.sub(r"[：]", ":", content)
    content = re.sub(r"\s+", " ", content).strip()

    # --- Repair compact timestamp like Nov092025-13:09:01 ---
    content = re.sub(
        r"([A-Z][a-z]{2})0?(\d{1,2})(\d{4})-?(\d{2}:\d{2}:\d{2})",
        r"\1 \2 \3 - \4",
        content,
    )

    # --- Split into lines ---
    lines = re.split(r"(?=[A-Z][a-z]{2}\s+\d{1,2}\s+\d{4}\s*-)", content)
    lines = [l.strip() for l in lines if l.strip()]

    print("🧩 Sample lines after cleaning:")
    for l in lines[:5]:
        print(l[:200])

    entries = []

    # --- Pattern for RTC entries (no XML tags) ---
    # Example:
    # Nov 9 2025 - 13:09:01:192.168.1.116:send-> 26645116
    # Nov 9 2025 - 13:09:01:192.168.1.116:recv-> 26645116 3
    ts_pattern = re.compile(
        r"([A-Z][a-z]{2}\s+\d{1,2}\s+\d{4})\s*-\s*(\d{2}:\d{2}:\d{2})\s*:\s*([\d\.]+)\s*:\s*(send|recv)->\s*(\d+)(?:\s+(\d+))?",
        re.IGNORECASE,
    )

    for line in lines:
        m = ts_pattern.search(line)
        if not m:
            continue

        try:
            ts = datetime.strptime(
                f"{m.group(1)} {m.group(2)}", "%b %d %Y %H:%M:%S"
            ).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            ts = None

        wash_id = m.group(5)
        pkg_num = int(m.group(6)) if m.group(6) else None

        entries.append({
            "wash_id": wash_id,
            "washpkgnum": pkg_num,
            "wash_ts": ts,
            "source_ip": m.group(3),
            "direction": m.group(4),
            "raw_xml": line[:200]
        })

    print(f"🔍 Debug: Matched {len(entries)} entries out of {len(lines)} lines")
    return entries

# ---------- S3 DOWNLOAD ----------
def download_from_s3(bucket, key):
    tmp_path = f"/tmp/{os.path.basename(key)}"
    try:
        s3.download_file(bucket, key, tmp_path)
        print(f"✅ Downloaded {key} from {bucket}")
        return tmp_path
    except ClientError as e:
        print(f"❌ S3 download failed: {e}")
        return None

# ---------- RE-UPLOAD RAW FILE ----------
def upload_unparsed_file(local_path, key):
    """Re-uploads file only once to rtc/unparsed/, avoids recursion."""
    try:
        if "rtc/unparsed/" in key:
            print("⏭️ Skipping re-upload (already in unparsed/).")
            return
        new_key = key.replace("rtc/", "rtc/unparsed/", 1)
        s3.upload_file(local_path, S3_BUCKET, new_key)
        print(f"⚠️ Uploaded unparsed file → s3://{S3_BUCKET}/{new_key}")
    except Exception as e:
        print(f"❌ Failed to upload unparsed file: {e}")

# ---------- INSERT INTO DB ----------
def insert_entries(entries):
    if not entries:
        print("⚠️ No valid entries parsed.")
        return 0
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        insert_sql = """
            INSERT INTO rtc_log (
                wash_id, washpkgnum, wash_ts,
                source_ip, direction, raw_xml
            )
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        for e in entries:
            cur.execute(
                insert_sql,
                (
                    e["wash_id"],
                    e["washpkgnum"],
                    e["wash_ts"],
                    e["source_ip"],
                    e["direction"],
                    e["raw_xml"],
                ),
            )
        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ Inserted {len(entries)} RTC records into database.")
        return len(entries)
    except Exception as e:
        print(f"❌ Database insert failed: {e}")
        return 0

# ---------- MAIN ----------
# ---------- MAIN ----------
def main():
    if not S3_KEY:
        print("❌ Missing S3 key argument.")
        return

    print(f"🚀 Processing RTC file: s3://{S3_BUCKET}/{S3_KEY}")
    local_path = download_from_s3(S3_BUCKET, S3_KEY)
    if not local_path:
        return

    # --- Read file with auto-detected encoding (UTF-8 vs UTF-16) ---
    with open(local_path, "rb") as f:
        raw = f.read()

    if b"\x00" in raw[:200]:
        content = raw.decode("utf-16", errors="ignore")
        print("📜 Detected UTF-16 encoding")
    else:
        content = raw.decode("utf-8", errors="ignore")

    # --- Parse all entries ---
    entries = parse_rtc_log(content)
    print(f"🧾 Parsed {len(entries)} RTC entries")

    # --- Keep only 'recv' direction entries ---
    recv_entries = [e for e in entries if e.get("direction") == "recv"]
    print(f"🎯 Filtered to {len(recv_entries)} recv entries")

    if not recv_entries:
        print("⚠️ No recv entries found, skipping insert.")
        upload_unparsed_file(local_path, S3_KEY)
        cleanup_file(local_path)
        return

    # --- Open DB connection once ---
    conn = get_db_connection()
    cur = conn.cursor()

    inserted_count = 0
    consecutive_exists = 0  # stop after 2 consecutive existing entries

    for e in recv_entries:
        # Stop if 2 consecutive already exist
        if consecutive_exists >= 2:
            print("🛑 Found 2 consecutive existing entries → stopping early.")
            break

        wash_id = e["wash_id"]
        wash_ts = e["wash_ts"]

        # Check if wash_id already exists
        cur.execute("SELECT 1 FROM rtc_log WHERE wash_id = %s LIMIT 1;", (wash_id,))
        exists = cur.fetchone()

        if exists:
            consecutive_exists += 1
            continue
        else:
            consecutive_exists = 0

        try:
            cur.execute("""
                INSERT INTO rtc_log (wash_id, washpkgnum, wash_ts, source_ip, direction, raw_xml, created_on, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_DATE, CURRENT_TIME);
            """, (
                e["wash_id"],
                e["washpkgnum"],
                e["wash_ts"],
                e["source_ip"],
                e["direction"],
                e["raw_xml"]
            ))
            inserted_count += 1
        except Exception as ex:
            print(f"❌ Skipped wash_id={wash_id} due to insert error: {ex}")

    conn.commit()
    cur.close()
    conn.close()

    if inserted_count:
        print(f"✅ Inserted {inserted_count} new RTC records into database.")
    else:
        print("⚠️ No new RTC entries inserted.")

    # --- Delete file from S3 after success ---
    try:
        s3.delete_object(Bucket=S3_BUCKET, Key=S3_KEY)
        print(f"🧹 Deleted processed file from S3: {S3_KEY}")
    except Exception as e:
        print(f"⚠️ Failed to delete S3 file: {e}")

    # --- Cleanup local temp file ---
    try:
        os.remove(local_path)
        print(f"🧹 Cleaned up temp file {local_path}")
    except Exception:
        pass


def cleanup_file(path):
    """Helper for safe cleanup"""
    try:
        os.remove(path)
        print(f"🧹 Cleaned up temp file {path}")
    except Exception:
        pass


if __name__ == "__main__":
    main()

