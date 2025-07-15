import os
import requests
import tempfile
import shutil
import math
import uuid
from pydub import AudioSegment
import ffmpeg
from google.cloud import storage

def process_video_task(video_url, user_id, task_id, whisper_language="auto", max_segment_mb=24, webhook_url=None, prompt=""):
    print(f"📥 開始處理影片任務 {task_id}")
    print(f"🌐 影片來源：{video_url}")
    print(f"👤 使用者：{user_id}")
    print(f"🌍 語言：{whisper_language}")
    print(f"📦 Chunk 上限：{max_segment_mb} MB")
    print(f"🔔 Webhook：{webhook_url}")
    print(f"📝 提示詞：{prompt}")

    # 建立暫存資料夾
    tmpdir = tempfile.mkdtemp()
    print(f"📁 建立暫存資料夾：{tmpdir}")
    
    try:
        input_path = os.path.join(tmpdir, "input_video")
        output_audio_path = os.path.join(tmpdir, "audio.mp3")

        # 下載影片
        print("⬇️ 正在下載影片...")
        with requests.get(video_url, stream=True) as r:
            r.raise_for_status()
            with open(input_path, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        print("✅ 影片下載完成")

        # 使用 ffmpeg 轉檔成 MP3
        print("🎧 開始轉換音訊...")
        ffmpeg.input(input_path).output(
            output_audio_path,
            ar=16000, ac=1, ab='32k', format='mp3'
        ).run(overwrite_output=True)
        print("✅ 音訊轉換完成：", output_audio_path)

        # 載入音檔
        print("📀 載入音檔...")
        audio = AudioSegment.from_mp3(output_audio_path)

        # 計算切割數量
        segment_size_bytes = max_segment_mb * 1024 * 1024
        total_bytes = len(audio.raw_data)
        total_chunks = math.ceil(total_bytes / segment_size_bytes)
        print(f"🧩 將音檔分為 {total_chunks} 段，每段最大 {max_segment_mb} MB")

        # 分割音檔
        chunk_paths = []
        for i in range(total_chunks):
            start = i * len(audio) // total_chunks
            end = (i + 1) * len(audio) // total_chunks
            chunk = audio[start:end]
            chunk_path = os.path.join(tmpdir, f"chunk_{i}.mp3")
            chunk.export(chunk_path, format="mp3")
            chunk_paths.append(chunk_path)
            print(f"📤 產生 chunk_{i}.mp3（{round(os.path.getsize(chunk_path)/1024/1024, 2)} MB）")

        # 初始化 GCS
        print("☁️ 開始上傳至 GCS...")
        client = storage.Client()
        bucket_name = video_url.split("/")[3]
        prefix = "/".join(video_url.split("/")[4:-1])
        bucket = client.bucket(bucket_name)

        gcs_urls = []
        for i, path in enumerate(chunk_paths):
            dest_blob_name = f"{prefix}/chunks/{task_id}_chunk_{i}.mp3"
            blob = bucket.blob(dest_blob_name)
            blob.upload_from_filename(path)
            blob.make_public()
            gcs_urls.append(blob.public_url)
            print(f"✅ 上傳 chunk_{i}.mp3 至 GCS：{blob.public_url}")

        # 傳送至 Whisper（模擬）
        print("🧠 模擬 Whisper 處理中...（這部分需你接 OpenAI API）")
        transcript = "\n".join([f"[Chunk {i}] 模擬轉錄內容" for i in range(total_chunks)])

        # Webhook 回傳
        if webhook_url:
            print("📬 發送 Webhook 回傳...")
            resp = requests.post(webhook_url, json={
                "task_id": task_id,
                "user_id": user_id,
                "transcript": transcript,
                "chunks": gcs_urls,
                "prompt": prompt
            })
            print(f"✅ Webhook 已送出，狀態碼 {resp.status_code}")
        else:
            print("⚠️ 未提供 Webhook URL，略過通知")

    except Exception as e:
        print("🔥 任務處理失敗：", str(e))
        raise e

    finally:
        # 清理暫存資料
        shutil.rmtree(tmpdir)
        print(f"🧹 清除暫存資料夾：{tmpdir}")
