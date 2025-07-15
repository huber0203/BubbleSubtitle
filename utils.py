import os
import tempfile
import math
import requests
import ffmpeg
from pydub import AudioSegment
from urllib.parse import urlparse
from google.cloud import storage

# 環境變數中設定 bucket 名稱
BUCKET_NAME = os.getenv("BUCKET_NAME", "bubblebucket-a1q5lb")


def process_video_task(video_url, user_id, task_id, whisper_language, max_segment_mb, webhook_url, prompt=""):
    print("📥 開始處理影片任務...")

    # 解析影片 GCS 路徑
    parsed_url = urlparse(video_url)
    object_path = parsed_url.path.lstrip('/')  # 去掉前導斜線
    object_dir = os.path.dirname(object_path)
    base_name = os.path.splitext(os.path.basename(object_path))[0]

    print(f"🧾 目標路徑：{object_dir}")
    print(f"🎞️ 檔名前綴：{base_name}")

    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = os.path.join(tmpdir, "input_video")
        audio_path = os.path.join(tmpdir, "audio.mp3")

        # 下載影片檔案
        print("⏬ 下載影片中...")
        download_file(video_url, input_path)

        # 轉音訊 MP3（32kbps, 16kHz）
        print("🎧 轉換音訊為 MP3...")
        convert_to_mp3(input_path, audio_path)

        # 切割音訊檔案
        print("✂️ 切割 MP3 音訊...")
        segments = split_audio(audio_path, max_segment_mb)

        # 上傳切片並回報 webhook
        print("☁️ 上傳切片並發送 webhook...")
        upload_segments_and_notify(segments, object_dir, user_id, task_id, whisper_language, webhook_url, prompt)


def download_file(url, output_path):
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)


def convert_to_mp3(input_path, output_path):
    (
        ffmpeg
        .input(input_path)
        .output(output_path, ar=16000, ac=1, audio_bitrate='32k', format='mp3')
        .run(overwrite_output=True)
    )


def split_audio(mp3_path, max_mb):
    audio = AudioSegment.from_mp3(mp3_path)
    segment_size_bytes = max_mb * 1024 * 1024
    segment_duration_ms = segment_size_bytes / (32 * 1024 / 8) * 1000  # 32kbps => 4KB/s

    print(f"📏 每段約 {segment_duration_ms / 1000:.2f} 秒")

    segments = []
    for i, start in enumerate(range(0, len(audio), int(segment_duration_ms))):
        end = min(start + int(segment_duration_ms), len(audio))
        segment = audio[start:end]

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=f"_part{i+1}.mp3")
        segment.export(tmp.name, format="mp3")
        segments.append((i + 1, tmp.name))

    return segments


def upload_segments_and_notify(segments, gcs_prefix, user_id, task_id, language, webhook_url, prompt):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    for idx, segment_path in segments:
        gcs_filename = f"{gcs_prefix}/audio_part_{idx}.mp3"
        blob = bucket.blob(gcs_filename)

        print(f"⏫ 上傳 {gcs_filename}...")
        blob.upload_from_filename(segment_path)
        blob.make_public()

        public_url = blob.public_url
        print(f"📡 傳送 webhook：{public_url}")

        payload = {
            "audio_url": public_url,
            "user_id": user_id,
            "task_id": task_id,
            "part": idx,
            "whisper_language": language,
            "prompt": prompt
        }

        try:
            response = requests.post(webhook_url, json=payload, timeout=30)
            print(f"✅ webhook 回應：{response.status_code}")
        except Exception as e:
            print(f"❌ webhook 發送失敗：{e}")
