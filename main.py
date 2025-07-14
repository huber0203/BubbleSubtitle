from flask import Flask, request, jsonify, make_response
import os
from utils import process_video_task

app = Flask(__name__)

@app.after_request
def apply_cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return response

@app.route('/', methods=['POST', 'OPTIONS'])
def handle_request():
    if request.method == "OPTIONS":
        return make_response('', 200)

    data = request.get_json()

    video_url = data.get("video_url")
    user_id = data.get("user_id")
    task_id = data.get("task_id")
    language = data.get("whisper_language", "auto")
    max_size_mb = data.get("max_segment_mb", 24)
    webhook_url = data.get("n8n_webhook")

    if not all([video_url, user_id, task_id, webhook_url]):
        return jsonify({"error": "Missing required fields"}), 400

    try:
        process_video_task(video_url, user_id, task_id, language, max_size_mb, webhook_url)
        return jsonify({"status": "processing_started"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
