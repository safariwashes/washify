from flask import Flask, request, jsonify
import subprocess
import os
import datetime

# ------------------------------------------------------------
# Safari Washify + Loader Parser Web Service
# ------------------------------------------------------------
# - Triggered by AWS Lambda whenever new S3 file appears
# - Runs upload_from_aws.py (Washify) or process_loader.py (Loader)
# - /healthz used by Lambda warm-up and Render uptime checks
# ------------------------------------------------------------

app = Flask(__name__)

# Shared secret (must match Lambda's RENDER_SECRET)
SECRET = os.getenv("RENDER_SECRET", "Washify123!")

# Path to your existing scripts
WASHIFY_SCRIPT = "upload_from_aws.py"
LOADER_SCRIPT = "process_loader.py"   # create later if not already


# ------------------------------------------------------------
# Routes
# ------------------------------------------------------------
@app.route("/")
def index():
    """Basic info endpoint."""
    return (
        "<h3>Safari Parser Service</h3>"
        "<p>Use POST /trigger for Washify files or POST /loader for loader files.</p>"
        "<p>Use GET /healthz to check health.</p>"
    )


@app.route("/healthz")
def healthz():
    """Health check endpoint for Render and Lambda pre-warm."""
    now = datetime.datetime.now()
    return jsonify({"status": "ok", "time": now.isoformat()})


# ------------------------------------------------------------
# Washify transaction trigger
# ------------------------------------------------------------
@app.route("/trigger", methods=["POST"])
def trigger():
    """Triggered by Lambda when a new Washify file arrives."""
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

        # Launch parser asynchronously
        subprocess.Popen(["python", WASHIFY_SCRIPT, bucket, key])

        return jsonify({"status": "started", "file": key, "time": now.isoformat()}), 200

    except Exception as e:
        print(f"[ERROR] /trigger exception: {e}")
        return jsonify({"error": str(e)}), 500


# ------------------------------------------------------------
# Loader file trigger
# ------------------------------------------------------------
@app.route("/loader", methods=["POST"])
def loader():
    """Triggered by Lambda when a new loader file arrives."""
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

        # Run your loader processor script (replace with real one)
        if os.path.exists(LOADER_SCRIPT):
            subprocess.Popen(["python", LOADER_SCRIPT, bucket, key])
        else:
            print(f"[WARN] {LOADER_SCRIPT} not found — skipping loader processing")

        return jsonify({"status": "started", "file": key, "time": now.isoformat()}), 200

    except Exception as e:
        print(f"[ERROR] /loader exception: {e}")
        return jsonify({"error": str(e)}), 500


# ------------------------------------------------------------
# Render entrypoint
# ------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
