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
import time

# 初始化客戶端
client = OpenAI()
storage_client = storage.Client()
transcoder_client = transcoder_v1.TranscoderServiceClient()

# 初始化日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- 常數設定 ---
VERSION = "v1.6.14" # 版本號更新
BUCKET_NAME = "bubblebucket-a1q5lb"
PROJECT_ID = "bubble-dropzone-2-pgxrk7"
LOCATION = "us-central1"
AUDIO_BATCH_SIZE_MB = 24

def format_srt_time(total_seconds):
    """將秒數精確格式化為 HH:MM:SS,mmm 的 SRT 標準時間格式"""
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    milliseconds = int((seconds - int(seconds)) * 1000)
    return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d},{milliseconds:03d}"

def get_audio_duration(file_path):
    """使用 ffprobe 取得音檔的精確時長 (秒)"""
    try:
        cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", file_path]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return float(result.stdout.strip())
    except (subprocess.CalledProcessError, ValueError) as e:
        logger.error(f"❌ 無法取得音檔時長 {os.path.basename(file_path)}: {e}")
        return 0.0

def extract_base_path_from_url(video_url):
    if video_url.startswith("https://storage.googleapis.com/"):
        gcs_path = video_url.replace("https://storage.googleapis.com/", "")
        return "/".join(gcs_path.split("/")[:-1])
    raise ValueError(f"URL 不是有效的 GCS HTTP URL: {video_url}")

def convert_http_url_to_gcs_uri(http_url):
    if http_url.startswith("https://storage.googleapis.com/"):
        gcs_path = http_url.replace("https://storage.googleapis.com/", "")
        return f"gs://{gcs_path}"
    raise ValueError(f"URL 不是有效的 GCS HTTP URL: {http_url}")

def create_transcoder_job(input_uri, output_folder_uri, job_id):
    logger.info(f"🎬 建立 Transcoder 任務：{job_id}")
    audio_stream = transcoder_v1.AudioStream(codec="mp3", bitrate_bps=128000, sample_rate_hertz=44100, channel_count=2)
    mux_stream = transcoder_v1.MuxStream(key="audio_only", container="mp3", elementary_streams=["audio_stream"])
    job = transcoder_v1.Job(
        input_uri=input_uri,
        output_uri=output_folder_uri,
        config=transcoder_v1.JobConfig(elementary_streams=[transcoder_v1.ElementaryStream(key="audio_stream", audio_stream=audio_stream)], mux_streams=[mux_stream])
    )
    parent = f"projects/{PROJECT_ID}/locations/{LOCATION}"
    request = transcoder_v1.CreateJobRequest(parent=parent, job=job)
    return transcoder_client.create_job(request=request)

def wait_for_transcoder_job(job_name, timeout_minutes=30):
    """等待 Transcoder 任務完成"""
    logger.info(f"⏳ 等待 Transcoder 任務完成：{job_name}")
    start_time = time.time()
    while time.time() - start_time < timeout_minutes * 60:
        job = transcoder_client.get_job(name=job_name)
        
        # 狀態對應：1=PENDING, 2=RUNNING, 3=SUCCEEDED, 4=FAILED
        state_names = {1: "PENDING", 2: "RUNNING", 3: "SUCCEEDED", 4: "FAILED"}
        state_name = state_names.get(job.state, f"UNKNOWN({job.state})")
        logger.info(f"📊 任務狀態：{state_name}")

        # --- 修正：使用數字來判斷狀態 ---
        if job.state == 3: # SUCCEEDED
            logger.info("✅ Transcoder 任務完成")
            return True
        if job.state == 4: # FAILED
            logger.error(f"❌ Transcoder 任務失敗: {job.error}")
            return False
            
        time.sleep(30)
    logger.error("⏰ Transcoder 任務超時")
    return False

def download_audio_from_gcs(gcs_uri, local_path):
    logger.info(f"📥 從 GCS 下載音檔：{gcs_uri}")
    bucket_name, blob_name = gcs_uri[5:].split("/", 1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(local_path)
    logger.info(f"✅ 音檔下載完成")

def split_audio_file(audio_path, chunk_size_mb):
    logger.info(f"🔪 分割音檔：{audio_path}")
    total_duration = get_audio_duration(audio_path)
    if total_duration == 0.0:
        raise RuntimeError(f"無法取得音檔時長：{audio_path}")
    file_size_mb = os.path.getsize(audio_path) / 1024 / 1024
    if file_size_mb <= chunk_size_mb:
        return [audio_path]
    
    chunk_duration = (total_duration * chunk_size_mb) / file_size_mb
    num_chunks = int(total_duration / chunk_duration) + 1
    logger.info(f"🔪 將分割為 {num_chunks} 段，每段約 {chunk_duration:.2f}s")
    
    chunks = []
    base_path = os.path.splitext(audio_path)[0]
    for i in range(num_chunks):
        start_time = i * chunk_duration
        if start_time >= total_duration:
            break
        chunk_path = f"{base_path}_chunk_{i:03d}.mp3"
        cmd = ["ffmpeg", "-y", "-ss", str(start_time), "-i", audio_path, "-t", str(chunk_duration), "-c", "copy", chunk_path]
        subprocess.run(cmd, check=True, capture_output=True)
        chunks.append(chunk_path)
    return chunks

def upload_to_gcs(file_path, blob_path):
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)
    content_type = "application/x-subrip" if file_path.endswith(".srt") else "audio/mpeg"
    blob.upload_from_filename(file_path, content_type=content_type)
    return blob.public_url

def process_video_task(video_url, user_id, task_id, whisper_language, max_segment_mb, webhook_url, prompt):
    logger.info(f"📥 開始處理任務 {task_id} (版本: {VERSION})")
    temp_dir = tempfile.mkdtemp()
    try:
        input_gcs_uri = convert_http_url_to_gcs_uri(video_url)
        base_path = extract_base_path_from_url(video_url)
        
        job_id = f"audio-extract-{user_id}-{task_id}"
        output_gcs_folder = f"gs://{base_path}/transcoder/"
        transcoder_job = create_transcoder_job(input_gcs_uri, output_gcs_folder, job_id)
        
        if not wait_for_transcoder_job(transcoder_job.name):
            raise RuntimeError("Transcoder 任務失敗或超時")
            
        output_gcs_uri = f"gs://{base_path}/transcoder/audio_only.mp3"
        audio_path = os.path.join(temp_dir, "full_audio.mp3")
        download_audio_from_gcs(output_gcs_uri, audio_path)
        
        audio_chunks = split_audio_file(audio_path, max_segment_mb)
        
        final_srt_parts = []
        total_duration_offset = 0.0
        for i, chunk_path in enumerate(audio_chunks):
            logger.info(f"🚀 處理音檔批次 {i+1}/{len(audio_chunks)}")
            with open(chunk_path, "rb") as f:
                transcript = client.audio.transcriptions.create(model="whisper-1", file=f, response_format="verbose_json", language=whisper_language, prompt=prompt or None)
            
            for segment in transcript.segments:
                start_time = segment['start'] + total_duration_offset
                end_time = segment['end'] + total_duration_offset
                
                start_str = format_srt_time(start_time)
                end_str = format_srt_time(end_time)
                
                text = segment['text'].strip()
                final_srt_parts.append((start_str, end_str, text))
            
            chunk_duration = get_audio_duration(chunk_path)
            total_duration_offset += chunk_duration
            logger.info(f"📝 批次 {i+1} 完成。累計 offset: {total_duration_offset:.2f}s")

        if not final_srt_parts:
            raise Exception("沒有產生任何轉錄內容")

        srt_path = os.path.join(temp_dir, "final.srt")
        with open(srt_path, "w", encoding="utf-8") as f:
            for i, (start, end, text) in enumerate(final_srt_parts):
                f.write(f"{i + 1}\n")
                f.write(f"{start}  {end}\n")
                f.write(f"{text}\n\n")

        srt_blob_path = f"{base_path}/srt/final.srt"
        srt_url = upload_to_gcs(srt_path, srt_blob_path)
        
        payload = {"任務狀態": "成功", "srt_url": srt_url, "task_id": task_id, "user_id": user_id}
        requests.post(webhook_url, json=payload, timeout=10)
        logger.info(f"✅ 任務 {task_id} 完成")

    except Exception as e:
        logger.error(f"🔥 任務 {task_id} 處理錯誤: {e}", exc_info=True)
        payload = {"任務狀態": f"失敗: {str(e)}", "task_id": task_id, "user_id": user_id}
        requests.post(webhook_url, json=payload, timeout=10)
    finally:
        shutil.rmtree(temp_dir)
