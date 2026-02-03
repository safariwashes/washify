from flask import Flask, request, jsonify
import subprocess
import os
import datetime
import threading
import hashlib

import psycopg2

# ------------------------------------------------------------
# Safari Washify + Loader + RTC Parser Web Service (Render)
# ------------------------------------------------------------
# - Triggered by AWS Lambda when new files are uploaded to S3
# - Routes:
#     /trigger → Washify kiosk files
#     /loader  → Loader (tunnel controller) logs
#     /rtc     → RTC (Laguna) XML interface logs
# - Provides /healthz endpoint for uptime checks
# ------------------------------------------------------------

app = Flask(__name__)

# Shared secret (must match Lambda's RENDER_SECRET)
SECRET = os.getenv("RENDER_SECRET", "Washify123!")

# Database (for advisory locks)
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# Paths to scripts
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WASHIFY_SCRIPT = os.path.join(BASE_DIR, "upload_from_aws.py")
LOADER_SCRIPT  = os.path.join(BASE_DIR, "loader_log_importer_render.py")
RTC_SCRIPT     = os.path.join(BASE_DIR, "upload_from_rtc.py")


# ------------------------------------------------------------
# Advisory Lock Helpers (Postgres)
# ------------------------------------------------------------
def _to_signed_bigint(n: int) -> int:
    """Convert unsigned 64-bit integer to signed BIGINT range."""
    if n >= 2**63:
        n -= 2**64
    return n


def advisory_lock_id(prefix: str, bucket: str, key: str) -> int:
    """
    Create a stable Postgres advisory lock id (signed BIGINT).

    We lock per:
      prefix + bucket + tenant + address + lane

    This allows different lanes/addresses to run in parallel,
    but prevents duplicate runs for the same lane (kiosk) or same tenant/address (loader/rtc).
    """
    parts = (key or "").split("/")
    tenant = next((p.split("=", 1)[1] for p in parts if p.startswith("tenant=")), "unknown")
    address = next((p.split("=", 1)[1] for p in parts if p.startswith("address=")), "unknown")
    lane = next((p.split("=", 1)[1] for p in parts if p.startswith("lane=")), "unknown")

    lock_name = f"{prefix}:{bucket}:{tenant}:{address}:{lane}".lower()
    digest = hashlib.sha256(lock_name.encode("utf-8")).digest()
    # Use first 8 bytes as 64-bit integer
    lock_u64 = int.from_bytes(digest[:8], byteorder="big", signed=False)
    return _to_signed_bigint(lock_u64)


def run_script_with_pg_lock(lock_id: int, cmd: list):
    """
    Acquire advisory lock and run command while holding DB connection open.
    Closing the connection releases the lock automatically.
    """
    conn = None
    try:
        if not DATABASE_URL:
            # Fallback: run without lock (not recommended, but avoids total failure)
            print("[LOCK] DATABASE_URL not set; running without advisory lock.")
            subprocess.run(cmd, check=False)
            return

        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
            got = cur.fetchone()[0]

        if not got:
            print(f"[LOCK] Busy (lock_id={lock_id}). Skipping launch: {cmd}")
            return

        print(f"[LOCK] Acquired (lock_id={lock_id}). Running: {cmd}")
        subprocess.run(cmd, check=False)
        print(f"[LOCK] Finished: {cmd}")

    except Exception as e:
        print(f"[LOCK] Error: {e}")

    finally:
        try:
            if conn is not None:
                conn.close()
                print(f"[LOCK] Released (lock_id={lock_id})")
        except Exception:
            pass


# ------------------------------------------------------------
# Routes
# ------------------------------------------------------------
@app.route("/")
def index():
    """Basic info endpoint."""
    return (
        "<h3>Safari Parser Service</h3>"
        "<p>POST /trigger → process Washify kiosk files</p>"
        "<p>POST /loader → process Loader (tunnel controller) files</p>"
        "<p>POST /rtc → process RTC (Laguna) XML logs</p>"
        "<p>GET /healthz → uptime ping</p>"
    )


@app.route("/healthz")
def healthz():
    """Render & Lambda warm-up check."""
    now = datetime.datetime.now()
    return jsonify({"status": "ok", "time": now.isoformat()})


def _auth_or_403():
    if request.headers.get("X-Webhook-Secret") != SECRET:
        return jsonify({"error": "unauthorized"}), 403
    return None


# ------------------------------------------------------------
# Washify Trigger
# ------------------------------------------------------------
@app.route("/trigger", methods=["POST"])
def trigger():
    """Triggered by Lambda for Washify kiosk files."""
    try:
        auth_resp = _auth_or_403()
        if auth_resp:
            return auth_resp

        data = request.get_json(force=True)
        bucket, key = data.get("bucket"), data.get("key")

        if not bucket or not key:
            return jsonify({"error": "missing bucket/key"}), 400

        now = datetime.datetime.now()
        print(f"[{now}] /trigger received for s3://{bucket}/{key}")

        if os.path.exists(WASHIFY_SCRIPT):
            print(f"[DEBUG] Launching Washify script (locked): {WASHIFY_SCRIPT}")

            lock_id = advisory_lock_id("washify", bucket, key)
            cmd = ["python", WASHIFY_SCRIPT, bucket, key]
            threading.Thread(
                target=run_script_with_pg_lock,
                args=(lock_id, cmd),
                daemon=True
            ).start()

            return jsonify({"status": "started_or_skipped_by_lock", "file": key, "time": now.isoformat()}), 200

        else:
            print(f"[ERROR] Washify script not found at {WASHIFY_SCRIPT}")
            return jsonify({"error": "script not found", "script": WASHIFY_SCRIPT}), 500

    except Exception as e:
        print(f"[ERROR] /trigger exception: {e}")
        return jsonify({"error": str(e)}), 500


# ------------------------------------------------------------
# Loader Trigger
# ------------------------------------------------------------
@app.route("/loader", methods=["POST"])
def loader():
    """Triggered by Lambda for Loader log files."""
    try:
        auth_resp = _auth_or_403()
        if auth_resp:
            return auth_resp

        data = request.get_json(force=True)
        bucket, key = data.get("bucket"), data.get("key")

        if not bucket or not key:
            return jsonify({"error": "missing bucket/key"}), 400

        now = datetime.datetime.now()
        print(f"[{now}] /loader received for s3://{bucket}/{key}")

        if os.path.exists(LOADER_SCRIPT):
            print(f"[DEBUG] Launching Loader script (locked): {LOADER_SCRIPT}")

            lock_id = advisory_lock_id("loader", bucket, key)
            cmd = ["python", LOADER_SCRIPT, bucket, key]
            threading.Thread(
                target=run_script_with_pg_lock,
                args=(lock_id, cmd),
                daemon=True
            ).start()

            return jsonify({"status": "started_or_skipped_by_lock", "file": key, "time": now.isoformat()}), 200

        else:
            print(f"[ERROR] Loader script not found at {LOADER_SCRIPT}")
            return jsonify({"error": "script not found", "script": LOADER_SCRIPT}), 500

    except Exception as e:
        print(f"[ERROR] /loader exception: {e}")
        return jsonify({"error": str(e)}), 500


# ------------------------------------------------------------
# RTC Trigger (Laguna XML Interface Logs)
# ------------------------------------------------------------
@app.route("/rtc", methods=["POST"])
def rtc():
    """Triggered by Lambda for RTC (Laguna) XML interface logs."""
    try:
        auth_resp = _auth_or_403()
        if auth_resp:
            return auth_resp

        data = request.get_json(force=True)
        bucket, key = data.get("bucket"), data.get("key")

        if not bucket or not key:
            return jsonify({"error": "missing bucket/key"}), 400

        now = datetime.datetime.now()
        print(f"[{now}] /rtc received for s3://{bucket}/{key}")

        if os.path.exists(RTC_SCRIPT):
            print(f"[DEBUG] Launching RTC script (locked): {RTC_SCRIPT}")

            lock_id = advisory_lock_id("rtc", bucket, key)
            cmd = ["python", RTC_SCRIPT, bucket, key]
            threading.Thread(
                target=run_script_with_pg_lock,
                args=(lock_id, cmd),
                daemon=True
            ).start()

            return jsonify({"status": "started_or_skipped_by_lock", "file": key, "time": now.isoformat()}), 200

        else:
            print(f"[ERROR] RTC script not found at {RTC_SCRIPT}")
            return jsonify({"error": "script not found", "script": RTC_SCRIPT}), 500

    except Exception as e:
        print(f"[ERROR] /rtc exception: {e}")
        return jsonify({"error": str(e)}), 500


# ------------------------------------------------------------
# Render Entrypoint
# ------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"🚀 Safari Parser Service started on port {port}")
    app.run(host="0.0.0.0", port=port)
