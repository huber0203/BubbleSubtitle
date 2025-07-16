import os
import tempfile
import requests
import logging
from urllib.parse import urlparse
from google.cloud import storage
import ffmpeg
from openai import OpenAI
from datetime import timedelta, datetime
import re
import math

# ✅ utils.py 版本
UTILS_VERSION = "v1.3.5"

# ⚙️ 設定 logging
logging.basicConfig(level=logging.INFO)

# ✅ 初始化 OpenAI client（新版 API）
client = OpenAI()

def convert_stream_to_mp3_segments(video_url, output_dir, segment_seconds=300):
    logging.info("🎧 開始直接串流影片並分割音訊...")
    output_template = os.path.join(output_dir, "chunk_%03d.mp3")
    (
        ffmpeg
        .input(video_url)
        .output(output_template, f='segment', segment_time=segment_seconds,
                ac=1, ar=16000, ab='32k')
        .run(overwrite_output=True, quiet=True)
    )
    logging.info("✅ 音訊串流轉換與分段完成")

def upload_to_gcs(bucket_name, destination_blob_name, source_file_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path, content_type="application/x-subrip" if source_file_path.endswith(".srt") else None)
    blob.make_public()
    return blob.public_url

def transcribe_audio(file_path, language, prompt):
    logging.info(f"🧠 上傳至 Whisper 分析中...：{file_path}")
    with open(file_path, "rb") as f:
        result = client.audio.transcriptions.create(
            model="whisper-1",
            file=f,
            language=language,
            response_format="srt",
            prompt=prompt if prompt else None
        )
        return result, None

def get_video_info(url):
    try:
        head = requests.head(url, allow_redirects=True)
        size_bytes = int(head.headers.get("content-length", 0))
        ext = os.path.splitext(urlparse(url).path)[-1].lstrip(".")
        return size_bytes, ext
    except:
        return 0, ""

def get_audio_duration(filepath):
    try:
        probe = ffmpeg.probe(filepath)
        return float(probe["format"]["duration"])
    except Exception:
        return 0

def process_video_task(video_url, user_id, task_id, whisper_language, max_segment_mb, webhook_url, prompt):
    logging.info(f"📥 開始處理影片任務 {task_id}")
    logging.info(f"🌐 影片來源：{video_url}")
    logging.info(f"👤 使用者：{user_id}")
    logging.info(f"🌍 語言：{whisper_language}")
    logging.info(f"📦 Chunk 上限：{max_segment_mb} MB")
    logging.info(f"🔔 Webhook：{webhook_url}")
    logging.info(f"📝 提示詞：{prompt}")
    logging.info(f"🧪 程式版本：{UTILS_VERSION}")

    status = "成功"
    srt_url = ""
    output_srt = ""
    video_duration = 0
    original_file_size_mb = 0
    original_file_format = ""
    compressed_audio_size_mb = 0

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            logging.info(f"📁 建立暫存資料夾：{tmpdir}")

            convert_stream_to_mp3_segments(video_url, tmpdir, 300)

            audio_files = sorted([f for f in os.listdir(tmpdir) if f.startswith("chunk_") and f.endswith(".mp3")])
            total_chunks = len(audio_files)

            compressed_audio_size_mb = round(sum(os.path.getsize(os.path.join(tmpdir, f)) for f in audio_files) / 1024 / 1024, 2)

            original_file_size_bytes, original_file_format = get_video_info(video_url)
            original_file_size_mb = round(original_file_size_bytes / 1024 / 1024, 2)

            bucket_name = "bubblebucket-a1q5lb"
            path_parts = urlparse(video_url).path.lstrip("/").split("/")
            object_path = "/".join(path_parts[1:-1])

            base_time = 0
            for idx, filename in enumerate(audio_files):
                chunk_path = os.path.join(tmpdir, filename)
                actual_size = round(os.path.getsize(chunk_path)/1024/1024, 2)
                logging.info(f"📦 處理進度 {idx+1}/{total_chunks}：{filename}（目標大小：{max_segment_mb} MB，實際大小：{actual_size} MB）")

                gcs_path = f"{object_path}/chunks/{task_id}_{filename}"
                gcs_url = upload_to_gcs(bucket_name, gcs_path, chunk_path)
                logging.info(f"✅ 上傳 {filename} 至 GCS：{gcs_url}")

                try:
                    srt_text, _ = transcribe_audio(chunk_path, whisper_language, prompt)
                    updated_srt = shift_srt_timestamps(srt_text, base_time)
                    output_srt += updated_srt + "\n"
                    chunk_duration = get_audio_duration(chunk_path)
                    base_time += chunk_duration
                    video_duration += chunk_duration
                except Exception as e:
                    status = f"失敗: Whisper 分析失敗 - {str(e)}"
                    logging.error(status)

            final_srt_path = os.path.join(tmpdir, "first.srt")
            with open(final_srt_path, "w", encoding="utf-8-sig") as f:
                f.write(output_srt.strip())

            try:
                srt_gcs_path = f"{object_path}/srt/first.srt"
                srt_url = upload_to_gcs(bucket_name, srt_gcs_path, final_srt_path)
                logging.info(f"📄 SRT 已上傳至 GCS：{srt_url}")
            except Exception as e:
                status = f"失敗: 上傳 SRT 失敗 - {str(e)}"
                logging.error(status)
    except Exception as e:
        status = f"失敗: 任務處理錯誤 - {str(e)}"
        logging.error(status)

    logging.info("📬 發送 Webhook 回傳...")
    try:
        response = requests.post(webhook_url, json={
            "任務狀態": status,
            "user_id": user_id,
            "task_id": task_id,
            "video_url": video_url,
            "whisper_language": whisper_language,
            "srt_url": srt_url,
            "video_duration": round(video_duration, 2),
            "original_file_size_mb": original_file_size_mb,
            "original_file_format": original_file_format,
            "compressed_audio_size_mb": compressed_audio_size_mb
        })
        logging.info(f"✅ Webhook 已送出，狀態碼 {response.status_code}")
    except Exception as e:
        logging.error(f"❌ Webhook 發送失敗：{e}")

def shift_srt_timestamps(srt_text, base_seconds):
    def parse_time(s):
        return datetime.strptime(s, "%H:%M:%S,%f")

    def format_time(t):
        return t.strftime("%H:%M:%S,%f")[:-3]

    updated_lines = []
    for line in srt_text.splitlines():
        match = re.match(r"(\d{2}:\d{2}:\d{2},\d{3}) --> (\d{2}:\d{2}:\d{2},\d{3})", line)
        if match:
            start, end = match.groups()
            start_dt = parse_time(start) + timedelta(seconds=base_seconds)
            end_dt = parse_time(end) + timedelta(seconds=base_seconds)
            updated_lines.append(f"{format_time(start_dt)} --> {format_time(end_dt)}")
        else:
            updated_lines.append(line)
    return "\n".join(updated_lines)
