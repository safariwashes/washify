from flask import Flask, request, jsonify
import subprocess
import os
import datetime

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

# Paths to scripts
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WASHIFY_SCRIPT = os.path.join(BASE_DIR, "upload_from_aws.py")
LOADER_SCRIPT  = os.path.join(BASE_DIR, "loader_log_importer_render.py")
RTC_SCRIPT     = os.path.join(BASE_DIR, "upload_from_rtc.py")


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


# ------------------------------------------------------------
# Washify Trigger
# ------------------------------------------------------------
@app.route("/trigger", methods=["POST"])
def trigger():
    """Triggered by Lambda for Washify kiosk files."""
    try:
        if request.headers.get("X-Webhook-Secret") != SECRET:
            return jsonify({"error": "unauthorized"}), 403

        data = request.get_json(force=True)
        bucket, key = data.get("bucket"), data.get("key")

        if not bucket or not key:
            return jsonify({"error": "missing bucket/key"}), 400

        now = datetime.datetime.now()
        print(f"[{now}] /trigger received for s3://{bucket}/{key}")

        if os.path.exists(WASHIFY_SCRIPT):
            print(f"[DEBUG] Launching Washify script: {WASHIFY_SCRIPT}")
            subprocess.Popen(["python", WASHIFY_SCRIPT, bucket, key])
        else:
            print(f"[ERROR] Washify script not found at {WASHIFY_SCRIPT}")

        return jsonify({"status": "started", "file": key, "time": now.isoformat()}), 200

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
        if request.headers.get("X-Webhook-Secret") != SECRET:
            return jsonify({"error": "unauthorized"}), 403

        data = request.get_json(force=True)
        bucket, key = data.get("bucket"), data.get("key")

        if not bucket or not key:
            return jsonify({"error": "missing bucket/key"}), 400

        now = datetime.datetime.now()
        print(f"[{now}] /loader received for s3://{bucket}/{key}")

        if os.path.exists(LOADER_SCRIPT):
            print(f"[DEBUG] Launching Loader script: {LOADER_SCRIPT}")
            subprocess.Popen(["python", LOADER_SCRIPT, bucket, key])
        else:
            print(f"[ERROR] Loader script not found at {LOADER_SCRIPT}")

        return jsonify({"status": "started", "file": key, "time": now.isoformat()}), 200

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
        if request.headers.get("X-Webhook-Secret") != SECRET:
            return jsonify({"error": "unauthorized"}), 403

        data = request.get_json(force=True)
        bucket, key = data.get("bucket"), data.get("key")

        if not bucket or not key:
            return jsonify({"error": "missing bucket/key"}), 400

        now = datetime.datetime.now()
        print(f"[{now}] /rtc received for s3://{bucket}/{key}")

        if os.path.exists(RTC_SCRIPT):
            print(f"[DEBUG] Launching RTC script: {RTC_SCRIPT}")
            subprocess.Popen(["python", RTC_SCRIPT, bucket, key])
        else:
            print(f"[ERROR] RTC script not found at {RTC_SCRIPT}")

        return jsonify({"status": "started", "file": key, "time": now.isoformat()}), 200

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
