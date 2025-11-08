import os
import sys
import re
import json
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
# ------------------------------------------------------------

# ---------- CONFIG ----------
S3_BUCKET = sys.argv[1] if len(sys.argv) > 1 else os.getenv("RTC_S3_BUCKET", "safari-franklin-data")
S3_KEY    = sys.argv[2] if len(sys.argv) > 2 else None

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

# ---------- AWS + DB Clients ----------
s3 = boto3.client("s3")

def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

# ---------- Parse HTML Log ----------
import html

def parse_rtc_log(content):
    """
    Parse XML-like HTML lines from Laguna's xmlInterfaceLog0.html.
    Handles HTML tags and encoded entities.
    """
    entries = []
    content = html.unescape(content)  # Decode &lt; and &gt;
    # Remove any <p>, <code>, etc.
    content = re.sub(r"<[^>]+>", "", content)

    pattern = re.compile(
        r"(?P<ts>[A-Z][a-z]{2}\s+\d{2}\s+\d{4}\s*-\s*\d{2}:\d{2}:\d{2})\s*:\s*(?P<ip>[\d\.]+)\s*:\s*(?P<dir>send|recv)\s*->.*?<id>(?P<wash_id>\d+)</id>.*?(?:<washPkgNum>(?P<washpkg>\d+)</washPkgNum>)?",
        re.DOTALL
    )

    for match in pattern.finditer(content):
        try:
            wash_ts = datetime.strptime(match.group("ts"), "%b %d %Y - %H:%M:%S")
            wash_id = match.group("wash_id")
            pkg = match.group("washpkg")
            pkg = int(pkg) if pkg else None

            entries.append({
                "wash_id": wash_id,
                "washpkgnum": pkg,
                "wash_ts": wash_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "source_ip": match.group("ip"),
                "direction": match.group("dir"),
                "raw_xml": match.group(0)[:1000]
            })
        except Exception as e:
            print(f"⚠️ Parse error: {e}")
            continue

    return entries


# ---------- Download from S3 ----------
def download_from_s3(bucket, key):
    tmp_path = f"/tmp/{os.path.basename(key)}"
    try:
        s3.download_file(bucket, key, tmp_path)
        print(f"✅ Downloaded {key} from {bucket}")
        return tmp_path
    except ClientError as e:
        print(f"❌ S3 download failed: {e}")
        return None


# ---------- Insert into DB ----------
def insert_entries(entries):
    if not entries:
        print("⚠️ No valid entries parsed.")
        return

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

    except Exception as e:
        print(f"❌ Database insert failed: {e}")


# ---------- MAIN ----------
def main():
    if not S3_KEY:
        print("❌ Missing S3 key argument.")
        return

    print(f"🚀 Processing RTC file: s3://{S3_BUCKET}/{S3_KEY}")

    # Step 1: Download file
    local_path = download_from_s3(S3_BUCKET, S3_KEY)
    if not local_path:
        return

    # Step 2: Parse content
    with open(local_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()
    entries = parse_rtc_log(content)
    print(f"🧾 Parsed {len(entries)} RTC entries")

    # Step 3: Insert into DB
    insert_entries(entries)

    # Step 4: Cleanup
    try:
        os.remove(local_path)
        print(f"🧹 Cleaned up temp file {local_path}")
    except Exception:
        pass


if __name__ == "__main__":
    main()
