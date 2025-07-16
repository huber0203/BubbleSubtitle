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

VERSION = "v1.3.8"
BUCKET_NAME = "bubblebucket-a1q5lb"
CHUNK_FOLDER = "chunks"
SRT_FOLDER = "srt"


def process_video_task(video_url, user_id, task_id, whisper_language, max_segment_mb, webhook_url, prompt):
    logger.info(f"\U0001F4E5 開始處理影片任務 {task_id}")
    logger.info(f"\U0001F310 影片來源：{video_url}")
    logger.info(f"\U0001F464 使用者：{user_id}")
    logger.info(f"\U0001F30D 語言：{whisper_language}")
    logger.info(f"\U0001F4E6 Chunk 上限：{max_segment_mb} MB")
    logger.info(f"\U0001F514 Webhook：{webhook_url}")
    logger.info(f"\U0001F4DD 提示詞：{prompt}")
    logger.info(f"\U0001F9EA 程式版本：{VERSION}")

    temp_dir = tempfile.mkdtemp()
    try:
        video_path = os.path.join(temp_dir, "video.mp4")

        logger.info("\U0001F3A7 開始直接串流影片並分割音訊...")
        headers = {"User-Agent": "Mozilla/5.0"}
        head_resp = requests.head(video_url, allow_redirects=True, headers=headers)
        total_size = int(head_resp.headers.get("Content-Length", 0))
        total_mb = round(total_size / 1024 / 1024, 2)
        logger.info(f"\U0001F4CF 影片大小（原始）：{total_mb} MB")

        with requests.get(video_url, stream=True, headers=headers) as r:
            with open(video_path, 'wb') as f:
                shutil.copyfileobj(r.raw, f)

        logger.info("\u2705 影片下載完成")

        # 使用 ffmpeg 分段音訊
        chunk_dir = os.path.join(temp_dir, "chunks")
        os.makedirs(chunk_dir, exist_ok=True)

        bytes_per_second = 32000  # 約 32kbps mp3
        seconds_per_chunk = (max_segment_mb * 1024 * 1024) // bytes_per_second
        logger.info(f"⏱ 每段音訊長度估算為 {seconds_per_chunk} 秒")

        chunk_pattern = os.path.join(chunk_dir, "chunk_%03d.mp3")
        cmd = [
            "ffmpeg", "-i", video_path,
            "-f", "segment",
            "-segment_time", str(seconds_per_chunk),
            "-c:a", "libmp3lame",
            "-ar", "44100",
            "-b:a", "32k",
            chunk_pattern
        ]
        subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)

        logger.info("\u2705 音訊串流轉換與分段完成")

        chunks = sorted([f for f in os.listdir(chunk_dir) if f.endswith(".mp3")])

        final_srt = []
        offset_ms = 0
        for i, chunk_name in enumerate(chunks):
            chunk_path = os.path.join(chunk_dir, chunk_name)
            logger.info(f"\U0001F4E4 處理進度 {i+1}/{len(chunks)}：{chunk_name}（大小：{round(os.path.getsize(chunk_path)/1024/1024, 2)} MB）")
            upload_url = upload_to_gcs(chunk_path, f"{user_id}/{task_id}/{CHUNK_FOLDER}/{chunk_name}")
            logger.info(f"\u2705 上傳 {chunk_name} 至 GCS：{upload_url}")

            with open(chunk_path, "rb") as f:
                transcript = client.audio.transcriptions.create(
                    model="whisper-1",
                    file=f,
                    response_format="verbose_json",
                    language=whisper_language,
                    prompt=prompt or None,
                )

            for segment in transcript.segments:
                start = str(timedelta(seconds=segment["start"] + offset_ms / 1000))[:-3].replace('.', ',')
                end = str(timedelta(seconds=segment["end"] + offset_ms / 1000))[:-3].replace('.', ',')
                final_srt.append(f"{len(final_srt)+1}\n{start} --> {end}\n{segment['text'].strip()}\n")

            offset_ms += int(transcript.segments[-1]["end"] * 1000)

        srt_path = os.path.join(temp_dir, "first.srt")
        with open(srt_path, "w", encoding="utf-8") as f:
            f.write("\n".join(final_srt))

        srt_url = upload_to_gcs(srt_path, f"{user_id}/{task_id}/{SRT_FOLDER}/first.srt")
        logger.info(f"\U0001F4C4 SRT 已上傳至 GCS：{srt_url}")

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

        logger.info("\U0001F4EC 發送 Webhook 回傳...")
        requests.post(webhook_url, json=payload, timeout=10)
        logger.info("\u2705 Webhook 已送出")

    except Exception as e:
        logger.error(f"\U0001F525 任務處理錯誤 - {e}")
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
            logger.info("\U0001F4EC 發送 Webhook 回傳...")
            requests.post(webhook_url, json=payload, timeout=10)
            logger.info("\u2705 Webhook 已送出")
        except:
            pass
    finally:
        logger.info(f"\U0001F9F9 清除暫存資料夾：{temp_dir}")
        shutil.rmtree(temp_dir, ignore_errors=True)


def upload_to_gcs(file_path, blob_path):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)
    content_type = "application/x-subrip" if file_path.endswith(".srt") else "audio/mpeg"
    blob.upload_from_filename(file_path, content_type=content_type)
    return f"https://storage.googleapis.com/{BUCKET_NAME}/{blob_path}"
