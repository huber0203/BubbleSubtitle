from flask import Flask, request, jsonify, make_response
import os
from utils import process_video_task

app = Flask(__name__)

# ➕ 全域 CORS 設定
@app.after_request
def apply_cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return response

# ✅ 主路由處理
@app.route('/', methods=['POST', 'OPTIONS'])
def handle_request():
    if request.method == "OPTIONS":
        print("🟡 OPTIONS 預檢請求收到")
        return make_response('', 200)

    print("✅ 收到 POST 請求")

    try:
        # 嘗試解析 JSON
        data = request.get_json(force=True, silent=True)
        print("📦 傳入的資料:", data)

        if not data:
            print("❌ 無法解析 JSON，請檢查 Content-Type 與 body 格式")
            return jsonify({"error": "Invalid JSON payload"}), 400

        # 提取欄位
        video_url = data.get("video_url")
        user_id = data.get("user_id", "anonymous")
        task_id = data.get("task_id")
        language = data.get("whisper_language", "auto")
        max_segment_mb = data.get("max_segment_mb", 24)
        webhook_url = data.get("n8n_webhook")
        prompt = data.get("prompt", "")  # 👈 新增支援 prompt

        # 檢查必要欄位
        if not all([video_url, task_id, webhook_url]):
            print("❌ 缺少必要欄位:", {
                "video_url": video_url,
                "task_id": task_id,
                "webhook_url": webhook_url
            })
            return jsonify({
                "error": "Missing one or more required fields: video_url, task_id, webhook_url"
            }), 400

        print("🚀 啟動任務處理:", {
            "video_url": video_url,
            "user_id": user_id,
            "task_id": task_id,
            "language": language,
            "max_segment_mb": max_segment_mb,
            "webhook_url": webhook_url,
            "prompt": prompt
        })

        # 呼叫任務處理邏輯
        process_video_task(
            video_url=video_url,
            user_id=user_id,
            task_id=task_id,
            whisper_language=language,
            max_segment_mb=max_segment_mb,
            webhook_url=webhook_url,
            prompt=prompt  # 👈 傳進 utils
        )

        print("✅ 任務開始執行")
        return jsonify({"status": "processing_started"}), 200

    except Exception as e:
        print("🔥 發生例外錯誤:", str(e))
        return jsonify({"error": str(e)}), 500

# 📡 啟動 Flask Server
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    print(f"🚀 啟動 Flask 伺服器於 0.0.0.0:{port}")
    app.run(host='0.0.0.0', port=port)
