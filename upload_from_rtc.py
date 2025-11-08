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

# ---------- PARSER ----------
def parse_rtc_log(content):
    """
    Parses Laguna XML log (xmlInterfaceLog0.html).
    Handles HTML tags, encoded XML (&lt;&gt;), and mixed send/recv lines.
    """
    entries = []

    # Decode entities like &lt; and &gt;
    content = html.unescape(content)

    # Remove HTML tags
    content = re.sub(r"<[^>]+>", "", content)

    # Split into lines
    lines = [line.strip() for line in content.splitlines() if line.strip()]

    for line in lines:
        m = re.match(
            r"(?P<ts>[A-Z][a-z]{2}\s+\d{2}\s+\d{4})\s*-\s*(?P<hms>\d{2}:\d{2}:\d{2})\s*:\s*(?P<ip>[\d\.]+)\s*:\s*(?P<dir>send|recv).*?<id>(?P<wash_id>\d+)</id>.*?(?:<washPkgNum>(?P<washpkg>\d+)</washPkgNum>)?",
            line
        )
        if not m:
            continue

        try:
            wash_ts = datetime.strptime(f"{m.group('ts')} {m.group('hms')}", "%b %d %Y %H:%M:%S")
            entries.append({
                "wash_id": m.group("wash_id"),
                "washpkgnum": int(m.group("washpkg")) if m.group("washpkg") else None,
                "wash_ts": wash_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "source_ip": m.group("ip"),
                "direction": m.group("dir"),
                "raw_xml": line[:500]  # store truncated line
            })
        except Exception as e:
            print(f"⚠️ Parse error: {e}")
            continue

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
    try:
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
