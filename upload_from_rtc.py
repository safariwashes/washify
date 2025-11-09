import os
import sys
import re
import html
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
    Cleans broken HTML and extracts (timestamp, IP, direction, XML body).
    """
    import re, html
    from datetime import datetime

    # --- Clean broken HTML but keep structure ---
    content = html.unescape(content)
    content = content.replace(" ", "")
    content = re.sub(r"<[^>]+>", " ", content)  # remove <...> tags
    content = content.replace("\xa0", " ")  # remove non-breaking spaces

    # --- Normalize all dash variants to ASCII '-' ---
    content = re.sub(r"[\u2010-\u2015\u2212\uFE58\uFE63\uFF0D]", "-", content)

    # --- Force-fix compact timestamp pattern like 'Nov092025-13:09:01' ---
    # This covers any junk between month/day/year/time.
    content = re.sub(
        r"([A-Z][a-z]{2})\s*0?(\d{1,2})[^\dA-Za-z]+(\d{4})[^\dA-Za-z-]+(\d{2}:\d{2}:\d{2})",
        r"\1 \2 \3 - \4",
        content,
    )

    # Handle simple compact 'Nov092025' (no time yet)
    content = re.sub(r"([A-Z][a-z]{2})\s*0?(\d{1,2})[^\dA-Za-z]+(\d{4})", r"\1 \2 \3", content)

    # --- Normalize punctuation and whitespace ---
    content = re.sub(r"[：]", ":", content)
    content = re.sub(r"\s+", " ", content).strip()

    # --- Split into log-like lines ---
    lines = re.split(r"(?=[A-Z][a-z]{2}\s+\d{1,2}\s+\d{4}\s*-)", content)
    lines = [l.strip() for l in lines if l.strip()]

    print("🧩 Sample lines after cleaning:")
    for l in lines[:5]:
        print(l[:200])

    entries = []

    # --- Match flexible RTC log format ---
    ts_pattern = re.compile(
        r"([A-Z][a-z]{2}\s+\d{1,2}\s+\d{4})\s*-?\s*(\d{2}:\d{2}:\d{2})\s*:\s*([\d\.]+)\s*:\s*(send|recv)\s*->\s*(.*)",
        re.IGNORECASE,
    )

    for line in lines:
        m = ts_pattern.search(line)
        if not m:
            continue

        body = m.group(5)
        wash_id = re.search(r"<id>(\d+)</id>", body)
        pkg = re.search(r"<washPkgNum>(\d+)</washPkgNum>", body)

        if wash_id:
            try:
                ts = datetime.strptime(
                    f"{m.group(1)} {m.group(2)}", "%b %d %Y %H:%M:%S"
                ).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                ts = None

            entries.append(
                {
                    "wash_id": wash_id.group(1),
                    "washpkgnum": int(pkg.group(1)) if pkg else None,
                    "wash_ts": ts,
                    "source_ip": m.group(3),
                    "direction": m.group(4),
                    "raw_xml": body[:500],
                }
            )

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
        # If file already resides under rtc/unparsed/, do nothing
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
                    e["raw_xml"]
                )
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
def main():
    if not S3_KEY:
        print("❌ Missing S3 key argument.")
        return

    print(f"🚀 Processing RTC file: s3://{S3_BUCKET}/{S3_KEY}")

    local_path = download_from_s3(S3_BUCKET, S3_KEY)
    if not local_path:
        return

    with open(local_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    entries = parse_rtc_log(content)
    print(f"🧾 Parsed {len(entries)} RTC entries")

    inserted = insert_entries(entries)

    # If parser failed → upload to /rtc/unparsed/
    if inserted == 0:
        upload_unparsed_file(local_path, S3_KEY)

    # Cleanup
    try:
        os.remove(local_path)
        print(f"🧹 Cleaned up temp file {local_path}")
    except Exception:
        pass

if __name__ == "__main__":
    main()
