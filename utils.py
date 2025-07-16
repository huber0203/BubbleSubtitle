import os
import tempfile
import shutil
import logging
import requests
from datetime import timedelta
from google.cloud import storage
from openai import OpenAI
import subprocess
import io

# 初始化 OpenAI client
client = OpenAI()

# 初始化 logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VERSION = "v1.6.0"
BUCKET_NAME = "bubblebucket-a1q5lb"
CHUNK_FOLDER = "chunks"
SRT_FOLDER = "srt"

# 配置參數
VIDEO_CHUNK_SIZE_MB = 50  # 影片分段大小
VIDEO_CHUNK_SIZE_BYTES = VIDEO_CHUNK_SIZE_MB * 1024 * 1024
AUDIO_BATCH_SIZE_MB = 24  # 音檔累積到這個大小就送 Whisper
AUDIO_BATCH_SIZE_BYTES = AUDIO_BATCH_SIZE_MB * 1024 * 1024

def process_video_task_streaming(video_url, user_id, task_id, whisper_language, max_segment_mb, webhook_url, prompt):
    logger.info(f"📥 開始串流處理影片任務 {task_id}")
    logger.info(f"🌐 影片來源：{video_url}")
    logger.info(f"👤 使用者：{user_id}")
    logger.info(f"🌍 語言：{whisper_language}")
    logger.info(f"📦 影片分段大小：{VIDEO_CHUNK_SIZE_MB} MB")
    logger.info(f"🎵 音檔批次大小：{AUDIO_BATCH_SIZE_MB} MB")
    logger.info(f"🔔 Webhook：{webhook_url}")
    logger.info(f"📝 提示詞：{prompt}")
    logger.info(f"🧪 程式版本：{VERSION}")

    temp_dir = tempfile.mkdtemp()
    try:
        # 1. 取得影片總大小
        headers = {"User-Agent": "Mozilla/5.0"}
        head_resp = requests.head(video_url, allow_redirects=True, headers=headers)
        total_size = int(head_resp.headers.get("Content-Length", 0))
        total_mb = round(total_size / 1024 / 1024, 2)
        logger.info(f"📏 影片大小：{total_mb} MB")

        # 2. 計算影片分段數量
        num_video_chunks = (total_size + VIDEO_CHUNK_SIZE_BYTES - 1) // VIDEO_CHUNK_SIZE_BYTES
        logger.info(f"📦 預計分割為 {num_video_chunks} 個影片段")

        # 3. 音檔累積變數
        accumulated_audio = io.BytesIO()
        accumulated_size = 0
        audio_batch_count = 0
        total_duration_offset = 0.0  # 累計時間偏移
        final_srt_parts = []

        # 4. 逐段處理影片
        for chunk_idx in range(num_video_chunks):
            start_byte = chunk_idx * VIDEO_CHUNK_SIZE_BYTES
            end_byte = min(start_byte + VIDEO_CHUNK_SIZE_BYTES - 1, total_size - 1)
            
            logger.info(f"📦 處理影片段 {chunk_idx + 1}/{num_video_chunks}")
            
            # 4.1 下載影片段
            video_chunk_path = os.path.join(temp_dir, f"video_chunk_{chunk_idx:03d}.mp4")
            if not download_video_chunk(video_url, start_byte, end_byte, video_chunk_path):
                logger.error(f"❌ 影片段 {chunk_idx} 下載失敗")
                continue
                
            # 4.2 轉換為音檔
            audio_chunk_path = os.path.join(temp_dir, f"audio_chunk_{chunk_idx:03d}.mp3")
            chunk_duration = convert_to_audio(video_chunk_path, audio_chunk_path)
            if chunk_duration is None:
                logger.error(f"❌ 音檔轉換失敗：chunk {chunk_idx}")
                continue
                
            # 4.3 讀取音檔內容
            with open(audio_chunk_path, 'rb') as f:
                audio_data = f.read()
            
            audio_size = len(audio_data)
            logger.info(f"🎵 音檔段 {chunk_idx}: {round(audio_size/1024/1024, 2)} MB, 時長: {chunk_duration:.2f}s")
            
            # 4.4 累積音檔
            accumulated_audio.write(audio_data)
            accumulated_size += audio_size
            
            # 4.5 檢查是否需要送 Whisper
            is_last_chunk = (chunk_idx == num_video_chunks - 1)
            should_process = (accumulated_size >= AUDIO_BATCH_SIZE_BYTES) or is_last_chunk
            
            if should_process and accumulated_size > 0:
                audio_batch_count += 1
                batch_size_mb = round(accumulated_size / 1024 / 1024, 2)
                logger.info(f"🚀 準備送 Whisper 批次 {audio_batch_count}，大小：{batch_size_mb} MB")
                
                # 4.6 處理音檔批次
                srt_part, batch_duration = process_audio_batch(
                    accumulated_audio, 
                    audio_batch_count, 
                    total_duration_offset,
                    whisper_language,
                    prompt,
                    temp_dir,
                    user_id,
                    task_id
                )
                
                if srt_part:
                    final_srt_parts.extend(srt_part)
                    total_duration_offset += batch_duration
                    logger.info(f"✅ 批次 {audio_batch_count} 完成，累計時長：{total_duration_offset:.2f}s")
                
                # 4.7 重置累積器
                accumulated_audio.close()
                accumulated_audio = io.BytesIO()
                accumulated_size = 0
            
            # 4.8 清除暫存檔案
            os.remove(video_chunk_path)
            os.remove(audio_chunk_path)

        # 5. 生成最終 SRT
        if final_srt_parts:
            srt_path = os.path.join(temp_dir, "final.srt")
            with open(srt_path, "w", encoding="utf-8") as f:
                for i, srt_entry in enumerate(final_srt_parts):
                    f.write(f"{i + 1}\n{srt_entry}\n")

            srt_url = upload_to_gcs(srt_path, f"{user_id}/{task_id}/{SRT_FOLDER}/final.srt")
            logger.info(f"📄 SRT 已上傳：{srt_url}")

            # 6. 發送成功回應
            payload = {
                "任務狀態": "成功",
                "user_id": user_id,
                "task_id": task_id,
                "video_url": video_url,
                "whisper_language": whisper_language,
                "srt_url": srt_url,
                "影片原始大小MB": total_mb,
                "影片分段數": num_video_chunks,
                "音檔批次數": audio_batch_count,
                "總時長秒": total_duration_offset,
                "程式版本": VERSION,
            }

            requests.post(webhook_url, json=payload, timeout=10)
            logger.info("✅ 任務完成")
        else:
            raise Exception("沒有成功處理任何音檔批次")

    except Exception as e:
        logger.error(f"🔥 任務處理錯誤 - {e}")
        payload = {
            "任務狀態": f"失敗: {str(e)}",
            "user_id": user_id,
            "task_id": task_id,
            "video_url": video_url,
            "whisper_language": whisper_language,
            "srt_url": "",
            "程式版本": VERSION,
        }
        try:
            requests.post(webhook_url, json=payload, timeout=10)
        except:
            pass
    finally:
        logger.info(f"🧹 清除暫存資料夾：{temp_dir}")
        shutil.rmtree(temp_dir, ignore_errors=True)

def download_video_chunk(video_url, start_byte, end_byte, output_path, max_retries=3):
    """下載單個影片段"""
    headers = {"User-Agent": "Mozilla/5.0"}
    
    for attempt in range(max_retries):
        try:
            headers["Range"] = f"bytes={start_byte}-{end_byte}"
            logger.info(f"📥 下載範圍：{headers['Range']}")
            
            with requests.get(video_url, headers=headers, stream=True) as r:
                r.raise_for_status()
                with open(output_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            size_mb = round(os.path.getsize(output_path) / 1024 / 1024, 2)
            logger.info(f"✅ 影片段下載完成：{size_mb} MB")
            return True
            
        except Exception as e:
            logger.warning(f"⚠️ 下載失敗，嘗試 {attempt + 1}/{max_retries}: {e}")
    
    return False

def convert_to_audio(video_path, audio_path):
    """轉換影片為音檔，返回時長"""
    try:
        # 使用容錯性更高的設定
        cmd = [
            "ffmpeg", "-y", 
            "-fflags", "+discardcorrupt+igndts",
            "-i", video_path,
            "-vn", "-acodec", "libmp3lame",
            "-ar", "44100", "-b:a", "32k",
            "-f", "mp3",
            audio_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"FFmpeg 轉檔失敗：{result.stderr}")
            return None
        
        # 取得音檔時長
        cmd = ["ffprobe", "-v", "quiet", "-show_entries", "format=duration", 
               "-of", "csv=p=0", audio_path]
        result = subprocess.run(cmd, capture_output=True, text=True)
        duration = float(result.stdout.strip()) if result.stdout.strip() else 0.0
        
        return duration
        
    except Exception as e:
        logger.error(f"音檔轉換錯誤：{e}")
        return None

def process_audio_batch(accumulated_audio, batch_count, time_offset, whisper_language, prompt, temp_dir, user_id, task_id):
    """處理累積的音檔批次"""
    try:
        # 保存累積的音檔
        batch_audio_path = os.path.join(temp_dir, f"audio_batch_{batch_count:03d}.mp3")
        with open(batch_audio_path, 'wb') as f:
            f.write(accumulated_audio.getvalue())
        
        # 上傳到 GCS
        upload_url = upload_to_gcs(batch_audio_path, f"{user_id}/{task_id}/{CHUNK_FOLDER}/audio_batch_{batch_count:03d}.mp3")
        logger.info(f"✅ 音檔批次上傳：{upload_url}")
        
        # 送 Whisper 轉錄
        with open(batch_audio_path, "rb") as f:
            transcript = client.audio.transcriptions.create(
                model="whisper-1",
                file=f,
                response_format="verbose_json",
                language=whisper_language,
                prompt=prompt or None,
            )
        
        # 處理轉錄結果
        srt_entries = []
        batch_duration = 0.0
        
        for segment in transcript.segments:
            start_time = segment.start + time_offset
            end_time = segment.end + time_offset
            
            start_str = str(timedelta(seconds=start_time))[:-3].replace('.', ',')
            end_str = str(timedelta(seconds=end_time))[:-3].replace('.', ',')
            
            srt_entry = f"{start_str} --> {end_str}\n{segment.text.strip()}"
            srt_entries.append(srt_entry)
            
            batch_duration = max(batch_duration, segment.end)
        
        logger.info(f"📝 批次 {batch_count} 轉錄完成，{len(srt_entries)} 個片段")
        return srt_entries, batch_duration
        
    except Exception as e:
        logger.error(f"音檔批次處理失敗：{e}")
        return [], 0.0

def upload_to_gcs(file_path, blob_path):
    """上傳檔案到 GCS"""
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(blob_path)
        
        content_type = "application/x-subrip" if file_path.endswith(".srt") else "audio/mpeg"
        blob.upload_from_filename(file_path, content_type=content_type)
        
        return f"https://storage.googleapis.com/{BUCKET_NAME}/{blob_path}"
    except Exception as e:
        logger.error(f"GCS 上傳失敗：{e}")
        raise

# 主要入口點
def process_video_task(video_url, user_id, task_id, whisper_language, max_segment_mb, webhook_url, prompt):
    """主要處理函數"""
    return process_video_task_streaming(video_url, user_id, task_id, whisper_language, max_segment_mb, webhook_url, prompt)
