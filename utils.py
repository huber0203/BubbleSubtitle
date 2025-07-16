import os
import tempfile
import shutil
import logging
import requests
from datetime import timedelta
from pydub import AudioSegment
from google.cloud import storage
from openai import OpenAI

# 初始化 OpenAI client
client = OpenAI()

# 初始化 logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VERSION = "v1.3.7"
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
        audio_path = os.path.join(temp_dir, "audio.mp3")

        logger.info("\U0001F3A7 開始直接串流影片並分割音訊...")
        headers = {"User-Agent": "Mozilla/5.0"}
        head_resp = requests.head(video_url, allow_redirects=True, headers=headers)
        total_size = int(head_resp.headers.get("Content-Length", 0))
        total_mb = round(total_size / 1024 / 1024, 2)
        logger.info(f"\U0001F4CF 影片大小（原始）：{total_mb} MB")

        with requests.get(video_url, stream=True, headers=headers) as r:
            with open(os.path.join(temp_dir, "video.mp4"), 'wb') as f:
                shutil.copyfileobj(r.raw, f)

        logger.info("\u2705 影片下載完成")
        logger.info("\U0001F3A7 轉換音訊中...")
        audio = AudioSegment.from_file(os.path.join(temp_dir, "video.mp4"))
        audio.export(audio_path, format="mp3")

        compressed_size = round(os.path.getsize(audio_path) / 1024 / 1024, 2)
        logger.info(f"\U0001F4CA 音訊壓縮後大小：{compressed_size} MB")

        audio = AudioSegment.from_mp3(audio_path)
        max_bytes = max_segment_mb * 1024 * 1024

        chunks = []
        start_ms = 0
        i = 0
        while start_ms < len(audio):
            end_ms = len(audio)
            for j in range(start_ms + 10000, len(audio), 1000):
                if len(audio[start_ms:j].raw_data) > max_bytes:
                    end_ms = j - 1000
                    break
            chunk = audio[start_ms:end_ms]
            chunk_name = f"chunk_{i:03}.mp3"
            chunk_path = os.path.join(temp_dir, chunk_name)
            chunk.export(chunk_path, format="mp3")
            chunks.append((chunk_path, chunk_name, start_ms))
            logger.info(f"\U0001F4E6 處理進度 {i+1}/{len(audio)//(end_ms-start_ms)}：{chunk_name}（目標大小：{max_segment_mb} MB，實際大小：{round(os.path.getsize(chunk_path)/1024/1024,2)} MB）")
            start_ms = end_ms
            i += 1

        logger.info("\u2705 音訊串流轉換與分段完成")

        final_srt = []
        full_usage = {
            "type": "tokens",
            "input_tokens": 0,
            "input_token_details": {"text_tokens": 0, "audio_tokens": 0},
            "output_tokens": 0,
            "total_tokens": 0,
        }

        for idx, (chunk_path, chunk_name, offset_ms) in enumerate(chunks):
            logger.info(f"\U0001F4E4 發現並處理 {chunk_name}（{round(os.path.getsize(chunk_path)/1024/1024,2)} MB）")
            upload_url = upload_to_gcs(chunk_path, f"{user_id}/{task_id}/{CHUNK_FOLDER}/{chunk_name}")
            logger.info(f"\u2705 上傳 {chunk_name} 至 GCS：{upload_url}")

            logger.info(f"\U0001F9E0 上傳至 Whisper 分析中...：{chunk_path}")
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
            "音訊壓縮大小MB": compressed_size,
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
