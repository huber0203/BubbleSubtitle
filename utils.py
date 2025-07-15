import os
import tempfile
import requests
import logging
from urllib.parse import urlparse
from pydub import AudioSegment
from google.cloud import storage
import ffmpeg
from openai import OpenAI
from datetime import timedelta, datetime
import re

# ⚙️ 設定 logging
logging.basicConfig(level=logging.INFO)

# ✅ 初始化 OpenAI client（新版 API）
client = OpenAI()

VERSION = "v1.0.3"

def download_video(video_url, download_path):
    logging.info("⬇️ 正在下載影片...")
    with open(download_path, "wb") as f:
        response = requests.get(video_url)
        f.write(response.content)
    logging.info("✅ 影片下載完成")

def convert_to_mp3(input_path, output_path):
    logging.info("🎧 開始轉換音訊...")
    (
        ffmpeg
        .input(input_path)
        .output(output_path, ac=1, ar=16000, ab='32k')
        .run(overwrite_output=True, quiet=True)
    )
    logging.info(f"✅ 音訊轉換完成： {output_path}")

def split_audio(audio_path, max_mb):
    logging.info("📀 載入音檔...")
    audio = AudioSegment.from_file(audio_path)
    max_bytes = max_mb * 1024 * 1024

    chunks = []
    start_ms = 0
    while start_ms < len(audio):
        end_ms = len(audio)
        chunk = audio[start_ms:end_ms]

        while len(chunk.raw_data) > max_bytes and end_ms - start_ms > 5000:
            end_ms -= 5000
            chunk = audio[start_ms:end_ms]

        chunks.append(chunk)
        start_ms = end_ms

    logging.info(f"🧩 將音檔分為 {len(chunks)} 段，每段最大 {max_mb} MB")
    return chunks

def upload_to_gcs(bucket_name, destination_blob_name, source_file_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    blob.make_public()
    return blob.public_url

def transcribe_audio(file_path, language, prompt):
    logging.info(f"🧠 上傳至 Whisper 分析中...：{file_path}")
    with open(file_path, "rb") as f:
        transcript = client.audio.transcriptions.create(
            model="whisper-1",
            file=f,
            language=language,
            response_format="srt",
            prompt=prompt if prompt else None
        )
        return transcript, transcript.response.json().get("usage")

def process_video_task(video_url, user_id, task_id, whisper_language, max_segment_mb, webhook_url, prompt):
    logging.info(f"📥 開始處理影片任務 {task_id}")
    logging.info(f"🌐 影片來源：{video_url}")
    logging.info(f"👤 使用者：{user_id}")
    logging.info(f"🌍 語言：{whisper_language}")
    logging.info(f"📦 Chunk 上限：{max_segment_mb} MB")
    logging.info(f"🔔 Webhook：{webhook_url}")
    logging.info(f"📝 提示詞：{prompt}")
    logging.info(f"🧪 程式版本：{VERSION}")

    status = "成功"
    usage_total = {
        "type": "tokens",
        "input_tokens": 0,
        "input_token_details": {"text_tokens": 0, "audio_tokens": 0},
        "output_tokens": 0,
        "total_tokens": 0
    }
    srt_url = ""
    output_srt = ""

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            logging.info(f"📁 建立暫存資料夾：{tmpdir}")

            input_path = os.path.join(tmpdir, "input_video")
            audio_path = os.path.join(tmpdir, "audio.mp3")

            download_video(video_url, input_path)
            convert_to_mp3(input_path, audio_path)
            chunks = split_audio(audio_path, max_segment_mb)

            bucket_name = "bubblebucket-a1q5lb"
            path_parts = urlparse(video_url).path.lstrip("/").split("/")
            object_path = "/".join(path_parts[1:-1])

            base_time = 0
            for i, chunk in enumerate(chunks):
                chunk_filename = f"chunk_{i}.mp3"
                chunk_path = os.path.join(tmpdir, chunk_filename)
                chunk.export(chunk_path, format="mp3")

                logging.info(f"📤 產生 {chunk_filename}（{round(os.path.getsize(chunk_path)/1024/1024, 2)} MB）")

                gcs_path = f"{object_path}/chunks/{task_id}_{chunk_filename}"
                gcs_url = upload_to_gcs(bucket_name, gcs_path, chunk_path)
                logging.info(f"✅ 上傳 {chunk_filename} 至 GCS：{gcs_url}")

                try:
                    transcript, usage = transcribe_audio(chunk_path, whisper_language, prompt)
                    updated_srt = shift_srt_timestamps(transcript, base_time)
                    output_srt += updated_srt + "\n"
                    base_time += chunk.duration_seconds

                    if usage:
                        usage_total["input_tokens"] += usage.get("input_tokens", 0)
                        usage_total["output_tokens"] += usage.get("output_tokens", 0)
                        usage_total["total_tokens"] += usage.get("total_tokens", 0)
                        audio_tokens = usage.get("audio_tokens", 0)
                        usage_total["input_token_details"]["audio_tokens"] += audio_tokens
                except Exception as e:
                    status = f"失敗: Whisper 分析失敗 - {str(e)}"
                    logging.error(status)

            final_srt_path = os.path.join(tmpdir, "first.srt")
            with open(final_srt_path, "w", encoding="utf-8") as f:
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
            "usage": usage_total,
            "version": VERSION
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
