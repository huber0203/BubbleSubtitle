import os
import tempfile
import shutil
import logging
import requests
from datetime import timedelta
from google.cloud import storage
from google.cloud.video import transcoder_v1
from openai import OpenAI
import subprocess
import io
import time

# 初始化 OpenAI client
client = OpenAI()

# 初始化 Google Cloud clients
storage_client = storage.Client()
transcoder_client = transcoder_v1.TranscoderServiceClient()

# 初始化 logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VERSION = "v1.6.6"
BUCKET_NAME = "bubblebucket-a1q5lb"
CHUNK_FOLDER = "chunks"
SRT_FOLDER = "srt"
TRANSCODER_FOLDER = "transcoder"

# 配置參數
AUDIO_BATCH_SIZE_MB = 24  # 音檔累積到這個大小就送 Whisper
AUDIO_BATCH_SIZE_BYTES = AUDIO_BATCH_SIZE_MB * 1024 * 1024

# Google Cloud 配置
PROJECT_ID = "bubble-dropzone-2-pgxrk7"  # 正確的 project ID
LOCATION = "us-central1"  # 美國中部，與 US multi-region bucket 配合

def convert_http_url_to_gcs_uri(http_url):
    """將 HTTP URL 轉換為 GCS URI"""
    try:
        # 移除 https://storage.googleapis.com/ 前綴
        if http_url.startswith("https://storage.googleapis.com/"):
            gcs_path = http_url.replace("https://storage.googleapis.com/", "")
            return f"gs://{gcs_path}"
        else:
            # 如果不是 GCS HTTP URL，拋出錯誤
            raise ValueError(f"URL 不是有效的 GCS HTTP URL: {http_url}")
    except Exception as e:
        logger.error(f"❌ URL 轉換失敗：{e}")
        return None
    """建立 Transcoder 任務來轉換影片為 MP3"""
    try:
        logger.info(f"🎬 建立 Transcoder 任務：{job_id}")
        
        # 配置音檔輸出
        audio_stream = transcoder_v1.AudioStream(
            codec="mp3",
            bitrate_bps=128000,  # 128kbps
            sample_rate_hertz=44100,
            channel_count=2
        )
        
        # 配置 MuxStream (只要音檔)
        mux_stream = transcoder_v1.MuxStream(
            key="audio_only",
            container="mp3",
            elementary_streams=["audio_stream"]
        )
        
        # 配置 Job
        job = transcoder_v1.Job(
            input_uri=input_uri,
            output_uri=output_uri,
            config=transcoder_v1.JobConfig(
                elementary_streams=[
                    transcoder_v1.ElementaryStream(
                        key="audio_stream",
                        audio_stream=audio_stream
                    )
                ],
                mux_streams=[mux_stream]
            )
        )
        
        # 建立任務請求
        parent = f"projects/{PROJECT_ID}/locations/{LOCATION}"
        request = transcoder_v1.CreateJobRequest(
            parent=parent,
            job=job
        )
        
        # 建立任務
        created_job = transcoder_client.create_job(request=request)
        logger.info(f"✅ Transcoder 任務建立成功：{created_job.name}")
        
        return created_job
        
    except Exception as e:
        logger.error(f"❌ 建立 Transcoder 任務失敗：{e}")
        return None

def create_transcoder_job(input_uri, output_uri, job_id):
    """建立 Transcoder 任務來轉換影片為 MP3"""
    try:
        logger.info(f"🎬 建立 Transcoder 任務：{job_id}")
        
        # 配置音檔輸出
        audio_stream = transcoder_v1.AudioStream(
            codec="mp3",
            bitrate_bps=128000,  # 128kbps
            sample_rate_hertz=44100,
            channel_count=2
        )
        
        # 配置 MuxStream (只要音檔)
        mux_stream = transcoder_v1.MuxStream(
            key="audio_only",
            container="mp3",
            elementary_streams=["audio_stream"]
        )
        
        # 配置 Job
        job = transcoder_v1.Job(
            input_uri=input_uri,
            output_uri=output_uri,
            config=transcoder_v1.JobConfig(
                elementary_streams=[
                    transcoder_v1.ElementaryStream(
                        key="audio_stream",
                        audio_stream=audio_stream
                    )
                ],
                mux_streams=[mux_stream]
            )
        )
        
        # 建立任務請求
        parent = f"projects/{PROJECT_ID}/locations/{LOCATION}"
        request = transcoder_v1.CreateJobRequest(
            parent=parent,
            job=job
        )
        
        # 建立任務
        created_job = transcoder_client.create_job(request=request)
        logger.info(f"✅ Transcoder 任務建立成功：{created_job.name}")
        
        return created_job
        
    except Exception as e:
        logger.error(f"❌ 建立 Transcoder 任務失敗：{e}")
        return None

def wait_for_transcoder_job(job_name, timeout_minutes=30):
    """等待 Transcoder 任務完成"""
    try:
        logger.info(f"⏳ 等待 Transcoder 任務完成：{job_name}")
        
        timeout_seconds = timeout_minutes * 60
        start_time = time.time()
        
        while time.time() - start_time < timeout_seconds:
            job = transcoder_client.get_job(name=job_name)
            
            logger.info(f"📊 任務狀態：{job.state}")
            
            if job.state == transcoder_v1.Job.State.SUCCEEDED:
                logger.info("✅ Transcoder 任務完成")
                return True
            elif job.state == transcoder_v1.Job.State.FAILED:
                logger.error(f"❌ Transcoder 任務失敗：{job.failure_reason}")
                return False
            
            time.sleep(30)  # 每 30 秒檢查一次
        
        logger.error(f"⏰ Transcoder 任務超時 ({timeout_minutes} 分鐘)")
        return False
        
    except Exception as e:
        logger.error(f"❌ 等待 Transcoder 任務失敗：{e}")
        return False

def download_audio_from_gcs(gcs_uri, local_path):
    """從 GCS 下載音檔"""
    try:
        # 解析 GCS URI
        if not gcs_uri.startswith("gs://"):
            raise ValueError(f"Invalid GCS URI: {gcs_uri}")
        
        uri_parts = gcs_uri[5:].split("/", 1)
        bucket_name = uri_parts[0]
        blob_name = uri_parts[1]
        
        logger.info(f"📥 從 GCS 下載音檔：{gcs_uri}")
        
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        blob.download_to_filename(local_path)
        
        size_mb = round(os.path.getsize(local_path) / 1024 / 1024, 2)
        logger.info(f"✅ 音檔下載完成：{size_mb} MB")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 下載音檔失敗：{e}")
        return False

def split_audio_file(audio_path, chunk_size_mb=24):
    """分割音檔為多個小檔案"""
    try:
        logger.info(f"🔪 分割音檔：{audio_path}")
        
        # 取得音檔時長
        cmd = ["ffprobe", "-v", "quiet", "-show_entries", "format=duration", 
               "-of", "csv=p=0", audio_path]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise RuntimeError(f"無法取得音檔時長：{result.stderr}")
        
        total_duration = float(result.stdout.strip())
        file_size_mb = os.path.getsize(audio_path) / 1024 / 1024
        
        logger.info(f"📊 音檔總時長：{total_duration:.2f}s，大小：{file_size_mb:.2f}MB")
        
        # 如果檔案小於限制，直接返回
        if file_size_mb <= chunk_size_mb:
            logger.info("📦 音檔大小符合限制，不需分割")
            return [audio_path]
        
        # 計算分割點
        chunk_duration = (total_duration * chunk_size_mb) / file_size_mb
        num_chunks = int(total_duration / chunk_duration) + 1
        
        logger.info(f"🔪 將分割為 {num_chunks} 段，每段約 {chunk_duration:.2f}s")
        
        chunks = []
        base_path = os.path.splitext(audio_path)[0]
        
        for i in range(num_chunks):
            start_time = i * chunk_duration
            end_time = min((i + 1) * chunk_duration, total_duration)
            
            if start_time >= total_duration:
                break
                
            chunk_path = f"{base_path}_chunk_{i:03d}.mp3"
            
            cmd = [
                "ffmpeg", "-y",
                "-i", audio_path,
                "-ss", str(start_time),
                "-t", str(end_time - start_time),
                "-c", "copy",
                chunk_path
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"❌ 分割失敗：{result.stderr}")
                continue
                
            chunks.append(chunk_path)
            chunk_size = round(os.path.getsize(chunk_path) / 1024 / 1024, 2)
            logger.info(f"✅ 分割完成：{os.path.basename(chunk_path)} ({chunk_size} MB)")
        
        return chunks
        
    except Exception as e:
        logger.error(f"❌ 分割音檔失敗：{e}")
        return []

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
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(blob_path)
        
        content_type = "application/x-subrip" if file_path.endswith(".srt") else "audio/mpeg"
        blob.upload_from_filename(file_path, content_type=content_type)
        
        return f"https://storage.googleapis.com/{BUCKET_NAME}/{blob_path}"
    except Exception as e:
        logger.error(f"GCS 上傳失敗：{e}")
        raise

def process_video_task_with_transcoder(video_url, user_id, task_id, whisper_language, max_segment_mb, webhook_url, prompt):
    logger.info(f"📥 開始使用 Transcoder 處理影片任務 {task_id}")
    logger.info(f"🌐 影片來源：{video_url}")
    logger.info(f"👤 使用者：{user_id}")
    logger.info(f"🌍 語言：{whisper_language}")
    logger.info(f"🎵 音檔批次大小：{AUDIO_BATCH_SIZE_MB} MB")
    logger.info(f"🔔 Webhook：{webhook_url}")
    logger.info(f"📝 提示詞：{prompt}")
    logger.info(f"🧪 程式版本：{VERSION}")

    temp_dir = tempfile.mkdtemp()
    try:
        # 1. 確認影片可以訪問並轉換為 GCS URI
        logger.info("🔍 檢查影片 URL...")
        headers = {"User-Agent": "Mozilla/5.0"}
        head_resp = requests.head(video_url, allow_redirects=True, headers=headers)
        total_size = int(head_resp.headers.get("Content-Length", 0))
        total_mb = round(total_size / 1024 / 1024, 2)
        logger.info(f"📏 影片大小：{total_mb} MB")

        # 轉換 HTTP URL 為 GCS URI
        input_gcs_uri = convert_http_url_to_gcs_uri(video_url)
        if not input_gcs_uri:
            raise RuntimeError(f"無法轉換影片 URL 為 GCS URI: {video_url}")
        
        logger.info(f"🔄 轉換後的 GCS URI：{input_gcs_uri}")

        # 2. 建立 Transcoder 任務
        job_id = f"audio-extract-{user_id}-{task_id}"
        output_gcs_uri = f"gs://{BUCKET_NAME}/{user_id}/{task_id}/{TRANSCODER_FOLDER}/audio.mp3"
        
        transcoder_job = create_transcoder_job(input_gcs_uri, output_gcs_uri, job_id)
        if not transcoder_job:
            raise RuntimeError("建立 Transcoder 任務失敗")

        # 3. 等待 Transcoder 完成
        job_name = transcoder_job.name
        if not wait_for_transcoder_job(job_name):
            raise RuntimeError("Transcoder 任務失敗或超時")

        # 4. 下載轉換後的音檔
        audio_path = os.path.join(temp_dir, "full_audio.mp3")
        if not download_audio_from_gcs(output_gcs_uri, audio_path):
            raise RuntimeError("下載音檔失敗")

        # 5. 分割音檔（如果需要）
        audio_chunks = split_audio_file(audio_path, AUDIO_BATCH_SIZE_MB)
        if not audio_chunks:
            raise RuntimeError("分割音檔失敗")

        # 6. 處理音檔批次
        final_srt_parts = []
        total_duration_offset = 0.0
        
        for batch_idx, chunk_path in enumerate(audio_chunks):
            batch_count = batch_idx + 1
            logger.info(f"🚀 處理音檔批次 {batch_count}/{len(audio_chunks)}")
            
            # 6.1 上傳音檔到 GCS
            chunk_name = f"audio_batch_{batch_count:03d}.mp3"
            upload_url = upload_to_gcs(chunk_path, f"{user_id}/{task_id}/{CHUNK_FOLDER}/{chunk_name}")
            logger.info(f"✅ 音檔批次上傳：{upload_url}")
            
            # 6.2 送 Whisper 轉錄
            with open(chunk_path, "rb") as f:
                transcript = client.audio.transcriptions.create(
                    model="whisper-1",
                    file=f,
                    response_format="verbose_json",
                    language=whisper_language,
                    prompt=prompt or None,
                )
            
            # 6.3 處理轉錄結果
            srt_entries = []
            batch_duration = 0.0
            
            for segment in transcript.segments:
                start_time = segment.start + total_duration_offset
                end_time = segment.end + total_duration_offset
                
                start_str = str(timedelta(seconds=start_time))[:-3].replace('.', ',')
                end_str = str(timedelta(seconds=end_time))[:-3].replace('.', ',')
                
                srt_entry = f"{start_str} --> {end_str}\n{segment.text.strip()}"
                srt_entries.append(srt_entry)
                
                batch_duration = max(batch_duration, segment.end)
            
            final_srt_parts.extend(srt_entries)
            total_duration_offset += batch_duration
            
            logger.info(f"📝 批次 {batch_count} 轉錄完成，{len(srt_entries)} 個片段")

        # 7. 生成最終 SRT
        if final_srt_parts:
            srt_path = os.path.join(temp_dir, "final.srt")
            with open(srt_path, "w", encoding="utf-8") as f:
                for i, srt_entry in enumerate(final_srt_parts):
                    f.write(f"{i + 1}\n{srt_entry}\n")

            srt_url = upload_to_gcs(srt_path, f"{user_id}/{task_id}/{SRT_FOLDER}/final.srt")
            logger.info(f"📄 SRT 已上傳：{srt_url}")

            # 8. 發送成功回應
            payload = {
                "任務狀態": "成功",
                "user_id": user_id,
                "task_id": task_id,
                "video_url": video_url,
                "whisper_language": whisper_language,
                "srt_url": srt_url,
                "影片原始大小MB": total_mb,
                "音檔批次數": len(audio_chunks),
                "總時長秒": total_duration_offset,
                "轉換方式": "Google Transcoder API",
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

# 主要入口點
def process_video_task(video_url, user_id, task_id, whisper_language, max_segment_mb, webhook_url, prompt):
    """主要處理函數 - 使用 Google Transcoder API"""
    return process_video_task_with_transcoder(video_url, user_id, task_id, whisper_language, max_segment_mb, webhook_url, prompt)
