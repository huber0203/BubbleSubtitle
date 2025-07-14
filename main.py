from flask import Flask, request, jsonify
import os
from utils import process_video_task

app = Flask(__name__)

@app.route('/', methods=['POST'])
def handle_request():
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 8080)))
