import os
import boto3
import psycopg2
import re
from datetime import date, timedelta
import sys

# ---------- Environment Variables ----------
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD") or os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = "safari-franklin-data"

# ---------- AWS & DB Setup ----------
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def connect_db():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

# ------------------------------------------------------------
#   Time normalization helper (fixes 01:27:48 / 1:27:48 AM)
# ------------------------------------------------------------
def normalize_time(t):
    t = t.strip()
    t = t.replace("AM", "").replace("PM", "").strip()

    parts = t.split(":")
    if len(parts) != 3:
        return None

    hh, mm, ss = parts

    # Add zero padding
    try:
        hh = f"{int(hh):02d}"
        mm = f"{int(mm):02d}"
        ss = f"{int(ss):02d}"
        return f"{hh}:{mm}:{ss}"
    except:
        return None

# ------------------------------------------------------------
#   NEW: fetch last processed bill (for tail-seek)
# ------------------------------------------------------------
def get_last_processed_bill(cursor):
    cursor.execute("""
        SELECT bill, log_dt, log_time 
          FROM loader_log
      ORDER BY log_dt DESC, log_time DESC
         LIMIT 1;
    """)
    result = cursor.fetchone()
    if result:
        bill, log_dt, log_time = result
        print(f"🧭 Last processed bill: {bill} at {log_dt} {log_time}")
        return bill
    else:
        print("🧭 No previous bills, processing full file.")
        return None


# ------------------------------------------------------------
#   Process folder (optimized)
# ------------------------------------------------------------
def process_folder(conn, cursor, folder):
    prefix = f"loader1/{folder}/"
    print(f"🔍 Checking folder: {prefix}")
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)

    if "Contents" not in response:
        print(f"No files in {prefix}")
        return

    last_bill = get_last_processed_bill(cursor)

    for obj in response["Contents"]:
        key = obj["Key"]
        if not key.lower().endswith(".txt"):
            continue

        print(f"📄 Detected file: {key}")

        # Load file
        body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read().decode("utf-8", errors="ignore")
        lines = [l.strip() for l in body.splitlines() if l.strip()]

        # -------------------------------------
        # SEEK to location of last processed bill
        # -------------------------------------
        start_index = 0
        if last_bill:
            pattern = f"Invoice Id {last_bill}"
            for idx in range(len(lines) - 1, -1, -1):
                if pattern in lines[idx]:
                    start_index = idx - (idx % 4)
                    print(f"⏩ Starting at block index {start_index} (after last bill {last_bill})")
                    break
            else:
                print("⚠️ Last bill not found in file, processing full file.")

        inserted_count = 0
        i = start_index

        # -------------------------------------
        # BLOCK-BY-BLOCK PROCESSING
        # -------------------------------------
        while i < len(lines):
            try:
                line1 = lines[i]
                line2 = lines[i + 1]
                line4 = lines[i + 3]

                # Extract timestamp
                ts_match = re.match(r"^([^,]+)", line1)
                timestamp_raw = ts_match.group(1).strip() if ts_match else ""

                parts = timestamp_raw.split(" ", 1)
                if len(parts) != 2:
                    raise Exception(f"Bad timestamp: {timestamp_raw}")

                date_part, time_raw = parts
                time_norm = normalize_time(time_raw)

                if not time_norm:
                    raise Exception(f"Unparseable time: {time_raw}")

                # FULL TIMESTAMP FOR DB (fixes all Postgres errors)
                prep_end_ts = f"{date_part} {time_norm}"

                # Extract bills
                bill = int(re.search(r"Invoice Id (\d+)", line2).group(1))
                washify_rec = int(re.search(r"Invoice Id (\d+)", line4).group(1))

                # Check duplicate
                cursor.execute("SELECT 1 FROM loader_log WHERE bill = %s", (bill,))
                exists = cursor.fetchone()

                # Insert into loader_log
                if not exists:
                    cursor.execute("""
                        INSERT INTO loader_log (bill, washify_rec, log_dt, log_time)
                        VALUES (%s, %s, %s, %s)
                    """, (bill, washify_rec, date_part, time_norm))
                    conn.commit()
                    inserted_count += 1
                    print(f"🆕 Inserted bill={bill}")
                else:
                    print(f"↻ Bill {bill} already exists")

                # Update SUPER
                cursor.execute("""
                    UPDATE super
                       SET status = 3,
                           prep_end = %s,
                           status_desc = 'Wash'
                     WHERE bill = %s
                       AND created_on = %s
                       AND location = 'FRA'
                       AND (status IS NULL OR status < 3)
                """, (prep_end_ts, bill, date_part))
                if cursor.rowcount > 0:
                    print(f"🧾 SUPER updated for bill={bill}")
                conn.commit()

                # Update TUNNEL
                cursor.execute("""
                    UPDATE tunnel
                       SET load = TRUE,
                           load_time = %s
                     WHERE bill = %s
                       AND created_on = %s
                       AND location = 'FRA'
                """, (prep_end_ts, bill, date_part))
                if cursor.rowcount > 0:
                    print(f"🚗 TUNNEL updated for bill={bill}")
                conn.commit()

            except Exception as e:
                print(f"❌ Error parsing block {i}: {e}")
                conn.rollback()

            i += 4

        print(f"✅ File processed: {key}, {inserted_count} new records.\n")

        # Delete file after successful processing
        try:
            s3.delete_object(Bucket=S3_BUCKET, Key=key)
            print(f"🧹 Deleted S3 file: {key}")
        except Exception as e:
            print(f"⚠️ Failed to delete file {key}: {e}")


# ------------------------------------------------------------
#   MAIN RUNNER
# ------------------------------------------------------------
def process_files():
    conn = connect_db()
    cursor = conn.cursor()

    today_folder = date.today().strftime("%Y-%m-%d")
    yesterday_folder = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    for folder in [today_folder, yesterday_folder]:
        process_folder(conn, cursor, folder)

    # Heartbeat
    try:
        cursor.execute("""
            INSERT INTO heartbeat (source, created_on, created_at)
            VALUES (%s, CURRENT_DATE, CURRENT_TIME)
        """, ("Loader2Safari",))
        conn.commit()
        print("💓 Heartbeat logged: Loader2Safari")
    except Exception as e:
        print(f"⚠️ Heartbeat logging failed: {e}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    print("🚀 Loader2Safari single-run mode started...")
    try:
        process_files()
    except Exception as e:
        print(f"⚠️ Unexpected error: {e}")
