from flask import Flask, request, jsonify, make_response
import os
from utils import process_video_task

app = Flask(__name__)

# ➕ CORS headers
@app.after_request
def apply_cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return response

# ✅ POST 接收 webhook 任務請求
@app.route('/', methods=['POST', 'OPTIONS'])
def handle_request():
    if request.method == "OPTIONS":
        return make_response('', 200)

    try:
        data = request.get_json()

        video_url = data.get("video_url")
        user_id = data.get("user_id", "anonymous")
        task_id = data.get("task_id")
        language = data.get("whisper_language", "auto")
        max_segment_mb = data.get("max_segment_mb", 24)
        webhook_url = data.get("n8n_webhook")

        # ⛔ 檢查必要欄位
        if not all([video_url, task_id, webhook_url]):
            return jsonify({
                "error": "Missing one or more required fields: video_url, task_id, webhook_url"
            }), 400

        # ✅ 處理任務
        process_video_task(
            video_url=video_url,
            user_id=user_id,
            task_id=task_id,
            whisper_language=language,
            max_segment_mb=max_segment_mb,
            webhook_url=webhook_url
        )

        return jsonify({"status": "processing_started"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 🔊 啟動 Flask 並綁定到 Cloud Run 的埠口
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
