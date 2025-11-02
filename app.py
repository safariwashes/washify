from flask import Flask, request, jsonify
import subprocess
import os
import datetime

# ------------------------------------------------------------
#  Safari Washify Parser Web Service
#  - Receives POSTs from AWS Lambda when new log files hit S3
#  - Immediately runs upload_from_aws.py (your existing parser)
#  - Provides /healthz for Render uptime checks
# ------------------------------------------------------------

app = Flask(__name__)

# Shared secret between Lambda and Render
SECRET = os.getenv("RENDER_SECRET", "Washify123!")

@app.route("/")
def index():
    """Simple info page."""
    return (
        "<h3>Safari Washify Parser</h3>"
        "<p>Use /trigger (POST) to start parsing or /healthz to check health.</p>"
    )

@app.route("/healthz")
def healthz():
    """Used by Render to confirm service is alive."""
    now = datetime.datetime.now()
    return jsonify({"status": "ok", "time": now.isoformat()})

@app.route("/trigger", methods=["POST"])
def trigger():
    """
    Called by AWS Lambda when a new file is uploaded to S3.
    Expects JSON: { "bucket": "bucket-name", "key": "kiosks/TransactionXX.txt" }
    Header: X-Webhook-Secret: <RENDER_SECRET>
    """
    try:
        # --- Security check ---
        if request.headers.get("X-Webhook-Secret") != SECRET:
            return jsonify({"error": "unauthorized"}), 403

        data = request.get_json(force=True)
        bucket = data.get("bucket")
        key = data.get("key")

        if not bucket or not key:
            return jsonify({"error": "missing bucket/key"}), 400

        print(f"[TRIGGER] Received request for s3://{bucket}/{key}")

        # --- Launch the existing parser script asynchronously ---
        # This is equivalent to what the cron job used to run.
        subprocess.Popen(["python", "upload_from_aws.py", bucket, key])

        return jsonify({"status": "started", "file": key}), 200

    except Exception as e:
        print(f"[ERROR] /trigger exception: {e}")
        return jsonify({"error": str(e)}), 500


# ------------------------------------------------------------
#  Render entrypoint
# ------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
