import os
import tempfile
import requests
import logging
from pydub import AudioSegment
from google.cloud import storage
from datetime import timedelta
from urllib.parse import urlparse
import openai

openai.api_key = os.environ.get("OPENAI_API_KEY")

# 設定 logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_video_task(video_url, user_id, task_id, whisper_language, max_segment_mb, webhook_url, prompt):
    logger.info(f"\U0001F4E5 開始處理影片任務 {task_id}")
    logger.info(f"\U0001F310 影片來源：{video_url}")
    logger.info(f"\U0001F464 使用者：{user_id}")
    logger.info(f"\U0001F30D 語言：{whisper_language}")
    logger.info(f"\U0001F4E6 Chunk 上限：{max_segment_mb} MB")
    logger.info(f"\U0001F514 Webhook：{webhook_url}")
    logger.info(f"\U0001F4DD 提示詞：{prompt}")

    tmp_dir = tempfile.mkdtemp()
    logger.info(f"\U0001F4C1 建立暫存資料夾：{tmp_dir}")

    try:
        video_path = os.path.join(tmp_dir, "input_video")
        audio_path = os.path.join(tmp_dir, "audio.mp3")

        # 下載影片
        logger.info("⬇️ 正在下載影片...")
        with open(video_path, "wb") as f:
            f.write(requests.get(video_url).content)
        logger.info("✅ 影片下載完成")

        # 轉音訊
        logger.info("\U0001F3A7 開始轉換音訊...")
        os.system(f"ffmpeg -i {video_path} -vn -ar 16000 -ac 1 -b:a 32k -f mp3 {audio_path} -y")
        logger.info(f"✅ 音訊轉換完成： {audio_path}")

        # 載入音訊並切片
        logger.info("\U0001F4C0 載入音檔...")
        audio = AudioSegment.from_mp3(audio_path)
        max_bytes = max_segment_mb * 1024 * 1024
        chunks = []

        logger.info(f"\U0001F9E9 將音檔分為多段，每段最大 {max_segment_mb} MB")
        start_ms = 0
        accumulated_srt = ""
        total_duration_ms = len(audio)

        while start_ms < total_duration_ms:
            end_ms = total_duration_ms
            step_ms = 1000 * 5

            while end_ms - start_ms > step_ms:
                chunk = audio[start_ms:end_ms]
                if len(chunk.raw_data) <= max_bytes:
                    break
                end_ms -= step_ms

            chunk = audio[start_ms:end_ms]
            chunks.append((start_ms, chunk))
            logger.info(f"\U0001F4E4 產生 chunk：{start_ms // 1000}s - {end_ms // 1000}s, 大小 {round(len(chunk.raw_data)/1024/1024, 2)} MB")
            start_ms = end_ms

        # 上傳到 GCS
        client = storage.Client()
        bucket_name = urlparse(video_url).path.split('/')[1]
        object_path = '/'.join(urlparse(video_url).path.split('/')[2:-1])
        bucket = client.bucket(bucket_name)

        for idx, (start_ms, chunk) in enumerate(chunks):
            chunk_filename = f"{task_id}_chunk_{idx}.mp3"
            chunk_path = os.path.join(tmp_dir, chunk_filename)
            chunk.export(chunk_path, format="mp3")

            blob_path = f"{object_path}/chunks/{chunk_filename}"
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(chunk_path, content_type='audio/mpeg')

            logger.info(f"☁️ 上傳 {chunk_filename} 至 GCS：https://storage.googleapis.com/{bucket_name}/{blob_path}")

            # 調用 Whisper API
            with open(chunk_path, "rb") as audio_file:
                transcript = openai.Audio.transcribe(
                    model="whisper-1",
                    file=audio_file,
                    language=whisper_language,
                    prompt=prompt,
                    response_format="srt"
                )

            # 調整時間碼
            base_offset = timedelta(milliseconds=start_ms)
            adjusted_srt = adjust_srt_timestamps(transcript, base_offset)
            accumulated_srt += adjusted_srt + "\n"

        # 傳 webhook 結果
        logger.info("\U0001F4E8 發送 Webhook 回傳...")
        requests.post(webhook_url, json={
            "task_id": task_id,
            "user_id": user_id,
            "srt": accumulated_srt
        })
        logger.info("✅ Webhook 已送出")

    except Exception as e:
        logger.error(f"🔥 任務處理失敗：{str(e)}")
        raise
    finally:
        logger.info(f"\U0001F9F9 清除暫存資料夾：{tmp_dir}")
        os.system(f"rm -rf {tmp_dir}")

def adjust_srt_timestamps(srt_text, offset):
    def shift_timecode(tc):
        h, m, s_ms = tc.split(":")
        s, ms = s_ms.split(",")
        delta = timedelta(hours=int(h), minutes=int(m), seconds=int(s), milliseconds=int(ms))
        new_time = delta + offset
        return f"{str(new_time)[:-3].replace('.', ',')}"

    adjusted_lines = []
    for line in srt_text.splitlines():
        if "-->" in line:
            start, end = line.split(" --> ")
            adjusted_lines.append(f"{shift_timecode(start)} --> {shift_timecode(end)}")
        else:
            adjusted_lines.append(line)
    return "\n".join(adjusted_lines)
