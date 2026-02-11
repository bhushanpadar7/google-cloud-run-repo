from flask import Flask, request, jsonify
import re

app = Flask(__name__)

# Regex patterns
ERR_DISABLE_PATTERN = re.compile(
    r"%PM-4-ERR_DISABLE:\s*(\w+)\s*error detected on\s*(\S+)",
    re.IGNORECASE
)

RECOVER_PATTERN = re.compile(
    r"%PM-4-ERR_RECOVER:\s*Attempting to recover from\s*(\w+)\s*err-disable state on\s*(\S+)",
    re.IGNORECASE
)

def preprocess_text(raw_log):
    """
    Basic text preprocessing
    """
    text = raw_log.lower()
    text = text.strip()
    return text

def extract_fields(preprocessed_text):
    """
    Extract reason and interface
    """
    err_match = ERR_DISABLE_PATTERN.search(preprocessed_text)
    rec_match = RECOVER_PATTERN.search(preprocessed_text)

    if err_match:
        return {
            "event_type": "ERR_DISABLE",
            "reason": err_match.group(1),
            "interface": err_match.group(2)
        }

    if rec_match:
        return {
            "event_type": "ERR_RECOVER",
            "reason": rec_match.group(1),
            "interface": rec_match.group(2)
        }

    return None

@app.route("/process", methods=["POST"])
from google.cloud import storage

storage_client = storage.Client()

@app.route("/process", methods=["POST"])
def process():
    data = request.json

    bucket_name = data.get("bucket")
    file_name = data.get("file")

    if not bucket_name or not file_name:
        return jsonify({"error": "bucket and file required"}), 400

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_text()

    results = []

    for line in content.splitlines():
        cleaned = preprocess_text(line)
        extracted = extract_fields(cleaned)

        if extracted:
            results.append({
                "raw_log": line,
                "cleaned_log": cleaned,
                "event_type": extracted["event_type"],
                "reason": extracted["reason"],
                "interface": extracted["interface"]
            })

    return jsonify({
        "processed_records": len(results),
        "data": results
    })


@app.route("/", methods=["GET"])
def health():
    return "Preprocessor running"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)

