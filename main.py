from flask import Flask, request, jsonify, make_response
import os
import traceback
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

    print("✅ 收到 POST 請求")

    try:
        data = request.get_json(force=True, silent=True)
        if not data:
            print("❌ 無法解析 JSON")
            return jsonify({"error": "Invalid JSON payload"}), 400

        video_url = data.get("video_url")
        user_id = data.get("user_id", "anonymous")
        task_id = data.get("task_id")
        language = data.get("whisper_language", "auto")
        max_segment_mb = data.get("max_segment_mb", 24)
        webhook_url = data.get("n8n_webhook")
        prompt = data.get("prompt", "")

        if not all([video_url, task_id, webhook_url]):
            print("❌ 缺少必要欄位")
            return jsonify({
                "error": "Missing one or more required fields: video_url, task_id, webhook_url"
            }), 400

        print(f"🚀 啟動任務處理: {task_id}")
        process_video_task(
            video_url=video_url,
            user_id=user_id,
            task_id=task_id,
            whisper_language=language,
            max_segment_mb=max_segment_mb,
            webhook_url=webhook_url,
            prompt=prompt
        )

        print("✅ 任務開始執行")
        return jsonify({"status": "processing_started"}), 200

    except Exception as e:
        print(f"🔥 發生例外錯誤: {e}")
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    print(f"🚀 啟動 Flask 伺服器於 0.0.0.0:{port}")
    app.run(host='0.0.0.0', port=port)
