from flask import Flask, request, jsonify
import subprocess
import os
import datetime

# ------------------------------------------------------------
# Safari Washify + Loader Parser Web Service (Render)
# ------------------------------------------------------------
# - Triggered by AWS Lambda whenever a new file is uploaded to S3
# - Runs:
#     upload_from_aws.py  → for Washify transactions
#     loader_log_importer_render.py → for Loader files
# - Provides /healthz endpoint for Render uptime checks
# ------------------------------------------------------------

app = Flask(__name__)

# Shared secret (must match Lambda's RENDER_SECRET)
SECRET = os.getenv("RENDER_SECRET", "Washify123!")

# Paths to scripts (absolute paths for safety on Render)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WASHIFY_SCRIPT = os.path.join(BASE_DIR, "upload_from_aws.py")
LOADER_SCRIPT = os.path.join(BASE_DIR, "loader_log_importer_render.py")


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
        "<p>GET /healthz → Render uptime & Lambda warm-up</p>"
    )


@app.route("/healthz")
def healthz():
    """Health check endpoint."""
    now = datetime.datetime.now()
    return jsonify({"status": "ok", "time": now.isoformat()})


# ------------------------------------------------------------
# Washify Trigger
# ------------------------------------------------------------
@app.route("/trigger", methods=["POST"])
def trigger():
    """Triggered by Lambda when a new Washify kiosk file is uploaded."""
    try:
        if request.headers.get("X-Webhook-Secret") != SECRET:
            return jsonify({"error": "unauthorized"}), 403

        data = request.get_json(force=True)
        bucket = data.get("bucket")
        key = data.get("key")

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
    """Triggered by Lambda when a new Loader file is uploaded."""
    try:
        if request.headers.get("X-Webhook-Secret") != SECRET:
            return jsonify({"error": "unauthorized"}), 403

        data = request.get_json(force=True)
        bucket = data.get("bucket")
        key = data.get("key")

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
# Render Entrypoint
# ------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"🚀 Safari Parser Service started on port {port}")
    app.run(host="0.0.0.0", port=port)
