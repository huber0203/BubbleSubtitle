import os
import tempfile
import shutil
import logging
import requests
from datetime import timedelta
from google.cloud import storage
from openai import OpenAI
import subprocess

# 初始化 OpenAI client
client = OpenAI()

# 初始化 logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VERSION = "v1.5.2"
BUCKET_NAME = "bubblebucket-a1q5lb"
CHUNK_FOLDER = "chunks"
SRT_FOLDER = "srt"

CHUNK_SIZE_MB = 24
CHUNK_SIZE_BYTES = CHUNK_SIZE_MB * 1024 * 1024

def process_video_task(video_url, user_id, task_id, whisper_language, max_segment_mb, webhook_url, prompt):
    logger.info(f"📥 開始處理影片任務 {task_id}")
    logger.info(f"🌐 影片來源：{video_url}")
    logger.info(f"👤 使用者：{user_id}")
    logger.info(f"🌍 語言：{whisper_language}")
    logger.info(f"📦 Chunk 上限：{max_segment_mb} MB")
    logger.info(f"🔔 Webhook：{webhook_url}")
    logger.info(f"📝 提示詞：{prompt}")
    logger.info(f"🧪 程式版本：{VERSION}")

    temp_dir = tempfile.mkdtemp()
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        head_resp = requests.head(video_url, allow_redirects=True, headers=headers)
        total_size = int(head_resp.headers.get("Content-Length", 0))
        total_mb = round(total_size / 1024 / 1024, 2)
        logger.info(f"📏 影片大小（原始）：{total_mb} MB")

        chunk_dir = os.path.join(temp_dir, "chunks")
        os.makedirs(chunk_dir, exist_ok=True)

        num_chunks = (total_size + CHUNK_SIZE_BYTES - 1) // CHUNK_SIZE_BYTES
        logger.info(f"✅ 預計切分為 {num_chunks} 段，每段 {CHUNK_SIZE_MB} MB")

        for i in range(num_chunks):
            start_byte = i * CHUNK_SIZE_BYTES
            end_byte = min(start_byte + CHUNK_SIZE_BYTES - 1, total_size - 1)
            headers["Range"] = f"bytes={start_byte}-{end_byte}"

            chunk_video_path = os.path.join(temp_dir, f"chunk_{i:03d}.mp4")
            logger.info(f"📦 正在下載 chunk {i+1}/{num_chunks}：{headers['Range']}")

            with requests.get(video_url, headers=headers, stream=True) as r:
                with open(chunk_video_path, "wb") as f:
                    shutil.copyfileobj(r.raw, f)

            size_mb = round(os.path.getsize(chunk_video_path)/1024/1024, 2)
            logger.info(f"✅ Chunk {i+1} 下載完成：{size_mb} MB")

            chunk_audio_path = os.path.join(temp_dir, f"chunk_{i:03d}.mp3")
            cmd = [
                "ffmpeg", "-y", "-fflags", "+discardcorrupt", "-i", chunk_video_path,
                "-vn", "-acodec", "libmp3lame",
                "-ar", "44100", "-b:a", "32k",
                chunk_audio_path
            ]
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if result.returncode != 0:
                logger.error(f"ffmpeg 轉檔失敗：{result.stderr}")
                raise RuntimeError(f"ffmpeg error: {result.stderr}")

            logger.info(f"🎵 chunk_{i:03d}.mp3 轉檔完成")

        logger.info("✅ 音訊轉換與分段全部完成")

        final_srt = []
        offset_ms = 0
        audio_chunks = sorted([f for f in os.listdir(temp_dir) if f.endswith(".mp3")])

        for i, chunk_name in enumerate(audio_chunks):
            chunk_path = os.path.join(temp_dir, chunk_name)
            logger.info(f"📦 處理進度 {i+1}/{len(audio_chunks)}：{chunk_name}（大小：{round(os.path.getsize(chunk_path)/1024/1024, 2)} MB）")

            upload_url = upload_to_gcs(chunk_path, f"{user_id}/{task_id}/{CHUNK_FOLDER}/{chunk_name}")
            logger.info(f"✅ 上傳 {chunk_name} 至 GCS：{upload_url}")

            with open(chunk_path, "rb") as f:
                transcript = client.audio.transcriptions.create(
                    model="whisper-1",
                    file=f,
                    response_format="verbose_json",
                    language=whisper_language,
                    prompt=prompt or None,
                )

            for segment in transcript.segments:
                start = str(timedelta(seconds=segment.start + offset_ms / 1000))[:-3].replace('.', ',')
                end = str(timedelta(seconds=segment.end + offset_ms / 1000))[:-3].replace('.', ',')
                final_srt.append(f"{len(final_srt)+1}\n{start} --> {end}\n{segment.text.strip()}\n")

            offset_ms += int(transcript.segments[-1].end * 1000)

        srt_path = os.path.join(temp_dir, "first.srt")
        with open(srt_path, "w", encoding="utf-8") as f:
            f.write("\n".join(final_srt))

        srt_url = upload_to_gcs(srt_path, f"{user_id}/{task_id}/{SRT_FOLDER}/first.srt")
        logger.info(f"📄 SRT 已上傳至 GCS：{srt_url}")

        payload = {
            "任務狀態": "成功",
            "user_id": user_id,
            "task_id": task_id,
            "video_url": video_url,
            "whisper_language": whisper_language,
            "srt_url": srt_url,
            "影片原始大小MB": total_mb,
            "音訊壓縮大小MB": "N/A（ffmpeg handled）",
            "原始格式": video_url.split(".")[-1],
            "程式版本": VERSION,
        }

        logger.info("📬 發送 Webhook 回傳...")
        requests.post(webhook_url, json=payload, timeout=10)
        logger.info("✅ Webhook 已送出")

    except Exception as e:
        logger.error(f"🔥 任務處理錯誤 - {e}")
        payload = {
            "任務狀態": f"失敗: 任務處理錯誤 - {str(e)}",
            "user_id": user_id,
            "task_id": task_id,
            "video_url": video_url,
            "whisper_language": whisper_language,
            "srt_url": "",
            "程式版本": VERSION,
        }
        try:
            logger.info("📬 發送 Webhook 回傳...")
            requests.post(webhook_url, json=payload, timeout=10)
            logger.info("✅ Webhook 已送出")
        except:
            pass
    finally:
        logger.info(f"🧹 清除暫存資料夾：{temp_dir}")
        shutil.rmtree(temp_dir, ignore_errors=True)

def upload_to_gcs(file_path, blob_path):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)
    content_type = "application/x-subrip" if file_path.endswith(".srt") else "audio/mpeg"
    blob.upload_from_filename(file_path, content_type=content_type)
    return f"https://storage.googleapis.com/{BUCKET_NAME}/{blob_path}"
