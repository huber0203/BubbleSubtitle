import os
import tempfile
import requests
import shutil
from urllib.parse import urlparse
from pydub import AudioSegment
from google.cloud import storage
import ffmpeg
import openai

def process_video_task(
    video_url,
    user_id,
    task_id,
    whisper_language="auto",
    max_segment_mb=24,
    webhook_url=None,
    prompt=""
):
    print(f"📥 開始處理影片任務 {task_id}")
    print(f"🌐 影片來源：{video_url}")
    print(f"👤 使用者：{user_id}")
    print(f"🌍 語言：{whisper_language}")
    print(f"📦 Chunk 上限：{max_segment_mb} MB")
    print(f"🔔 Webhook：{webhook_url}")
    print(f"📝 提示詞：{prompt}")

    # 建立暫存資料夾
    temp_dir = tempfile.mkdtemp()
    print(f"📁 建立暫存資料夾：{temp_dir}")
    
    try:
        # 下載影片
        video_path = os.path.join(temp_dir, "input_video")
        print("⬇️ 正在下載影片...")
        with requests.get(video_url, stream=True) as r:
            r.raise_for_status()
            with open(video_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print("✅ 影片下載完成")

        # 轉換為 mp3
        audio_path = os.path.join(temp_dir, "audio.mp3")
        print("🎧 開始轉換音訊...")
        ffmpeg.input(video_path).output(audio_path, ar=16000, ac=1, ab="32k").run(quiet=True, overwrite_output=True)
        print(f"✅ 音訊轉換完成： {audio_path}")

        # 載入 mp3
        print("📀 載入音檔...")
        audio = AudioSegment.from_mp3(audio_path)

        # 根據大小切割音檔
        max_bytes = max_segment_mb * 1024 * 1024
        segment_ms = len(audio)
        bytes_per_ms = len(audio.raw_data) / segment_ms
        max_ms_per_chunk = int(max_bytes / bytes_per_ms)

        chunks = []
        print(f"🧩 將音檔分為 {max(1, segment_ms // max_ms_per_chunk)} 段，每段最大 {max_segment_mb} MB")
        for i, start in enumerate(range(0, segment_ms, max_ms_per_chunk)):
            end = min(start + max_ms_per_chunk, segment_ms)
            chunk = audio[start:end]
            chunk_filename = f"chunk_{i}.mp3"
            chunk_path = os.path.join(temp_dir, chunk_filename)
            chunk.export(chunk_path, format="mp3")
            chunks.append(chunk_path)
            print(f"📤 產生 {chunk_filename}（{round(os.path.getsize(chunk_path) / 1024 / 1024, 2)} MB）")

        # 上傳至 GCS
        print("☁️ 開始上傳至 GCS...")
        gcs_path = _get_gcs_path(video_url)
        client = storage.Client()
        bucket_name = _extract_bucket(video_url)
        bucket = client.bucket(bucket_name)

        gcs_chunk_urls = []
        for chunk_path in chunks:
            chunk_name = os.path.basename(chunk_path)
            blob_path = f"{gcs_path}/chunks/{task_id}_{chunk_name}"
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(chunk_path)
            blob.make_public()
            public_url = blob.public_url
            gcs_chunk_urls.append(public_url)
            print(f"✅ 上傳 {chunk_name} 至 GCS：{public_url}")

        # 呼叫 OpenAI Whisper 轉錄
        print("🧠 呼叫 OpenAI Whisper 進行轉錄")
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise Exception("❌ 請設定 OPENAI_API_KEY 環境變數")
        openai.api_key = api_key

        transcripts = []
        for idx, chunk_path in enumerate(chunks):
            print(f"🧠 處理 chunk_{idx} ...")
            with open(chunk_path, "rb") as audio_file:
                transcript = openai.Audio.transcribe(
                    model="whisper-1",
                    file=audio_file,
                    language=None if whisper_language == "auto" else whisper_language,
                    prompt=prompt if prompt else None
                )
                transcripts.append({
                    "chunk_index": idx,
                    "text": transcript["text"]
                })
                print(f"✅ chunk_{idx} 轉錄完成")

        # 發送 webhook
        if webhook_url:
            print("📬 發送 Webhook 回傳字幕結果...")
            response = requests.post(webhook_url, json={
                "task_id": task_id,
                "transcripts": transcripts
            })
            print(f"✅ Webhook 已送出，狀態碼 {response.status_code}")

    except Exception as e:
        print("🔥 任務處理失敗：", str(e))
        raise

    finally:
        # 清除暫存
        print(f"🧹 清除暫存資料夾：{temp_dir}")
        shutil.rmtree(temp_dir, ignore_errors=True)
        print("✅ 任務開始執行")

# 🔧 工具函式
def _get_gcs_path(url):
    parts = urlparse(url)
    return "/".join(parts.path.strip("/").split("/")[:-1])

def _extract_bucket(url):
    parts = urlparse(url)
    if "storage.googleapis.com" in parts.netloc:
        return parts.path.strip("/").split("/")[0]
    raise Exception("❌ 無法從 URL 解析 bucket 名稱")
