import os
import boto3
import psycopg2
import re
from datetime import date, timedelta, datetime
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


# ============================================================
#  NEW: Fetch last processed bill (tail optimization)
# ============================================================
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


# ============================================================
# Normalizes timestamps like:
#   "3:27:19" → "03:27:19"
#   "9:07:04" → "09:07:04"
# ============================================================
def normalize_time(t):
    try:
        return datetime.strptime(t, "%H:%M:%S").strftime("%H:%M:%S")
    except:
        # If missing leading zero
        try:
            return datetime.strptime(t, "%-H:%M:%S").strftime("%H:%M:%S")
        except:
            return None


# ============================================================
# Process a folder (optimized)
# ============================================================
def process_folder(conn, cursor, folder):
    prefix = f"loader1/{folder}/"
    print(f"🔍 Checking folder: {prefix}")

    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    if "Contents" not in response:
        print(f"No files in {prefix}")
        return

    # Get last processed bill
    last_bill = get_last_processed_bill(cursor)

    for obj in response["Contents"]:
        key = obj["Key"]
        if not key.lower().endswith(".txt"):
            continue

        print(f"📄 Detected file: {key}")

        # Read S3 file
        body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read().decode(
            "utf-8", errors="ignore"
        )
        lines = [line.strip() for line in body.splitlines() if line.strip()]

        # ===============================
        #  Tail seek to last processed bill
        # ===============================
        start_index = 0
        if last_bill:
            last_pattern = f"Invoice Id {last_bill}"
            for idx in range(len(lines) - 1, -1, -1):
                if last_pattern in lines[idx]:
                    start_index = idx - (idx % 4)
                    print(f"⏩ Starting at block {start_index}")
                    break
            else:
                print("⚠️ Last bill not found, processing full file.")

        inserted_count = 0
        i = start_index
        full_success = True

        # ===============================
        # Process blocks safely
        # ===============================
        while i < len(lines):
            try:
                if i + 3 >= len(lines):
                    print(f"⚠️ Incomplete block at index {i}, stopping.")
                    break

                line1 = lines[i]
                line2 = lines[i+1]
                line4 = lines[i+3]

                # Extract timestamp
                ts_match = re.match(r"^([^,]+)", line1)
                timestamp_raw = ts_match.group(1).strip() if ts_match else ""

                parts = timestamp_raw.split(" ", 1)
                if len(parts) != 2:
                    raise Exception(f"Bad timestamp: {timestamp_raw}")

                date_part, time_raw = parts
                time_raw = time_raw.replace("AM", "").replace("PM", "").strip()
                time_part = normalize_time(time_raw)

                if not time_part:
                    raise Exception(f"Unparseable time: {time_raw}")

                # Extract bill numbers
                bill = int(re.search(r"Invoice Id (\d+)", line2).group(1))
                washify_rec = int(re.search(r"Invoice Id (\d+)", line4).group(1))

                # Check existence in loader_log
                cursor.execute("SELECT 1 FROM loader_log WHERE bill = %s", (bill,))
                exists = cursor.fetchone()

                if not exists:
                    cursor.execute("""
                        INSERT INTO loader_log (bill, washify_rec, log_dt, log_time)
                        VALUES (%s, %s, %s, %s)
                    """, (bill, washify_rec, date_part, time_part))
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
                """, (time_part, bill, date_part))
                conn.commit()

                # Update TUNNEL
                cursor.execute("""
                    UPDATE tunnel
                       SET load = TRUE,
                           load_time = %s
                     WHERE bill = %s
                       AND created_on = %s
                       AND location = 'FRA'
                """, (time_part, bill, date_part))
                conn.commit()

            except Exception as e:
                print(f"❌ Error parsing block {i}: {e}")
                conn.rollback()
                full_success = False

            i += 4

        print(f"✅ File processed: {key}, {inserted_count} new records.\n")

        # ===============================
        # Delete file only if all good
        # ===============================
        if full_success:
            try:
                s3.delete_object(Bucket=S3_BUCKET, Key=key)
                print(f"🧹 Deleted S3 file: {key}")
            except Exception as e:
                print(f"⚠️ Could not delete file: {e}")
        else:
            print("⚠️ File NOT deleted due to processing errors.")


# ============================================================
#   Main runner
# ============================================================
def process_files():
    conn = connect_db()
    cursor = conn.cursor()

    today = date.today().strftime("%Y-%m-%d")
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    for folder in [today, yesterday]:
        process_folder(conn, cursor, folder)

    # Heartbeat
    try:
        cursor.execute("""
            INSERT INTO heartbeat (source, created_on, created_at)
            VALUES (%s, CURRENT_DATE, CURRENT_TIME)
        """, ("Loader2Safari",))
        conn.commit()
        print("💓 Heartbeat logged.")
    except Exception as e:
        print(f"⚠️ Heartbeat failed: {e}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    print("🚀 Loader2Safari single-run mode started...")
    try:
        process_files()
    except Exception as e:
        print(f"⚠️ Unexpected error: {e}")
