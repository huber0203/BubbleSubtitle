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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

VERSION = "v1.7.0" # 優化後的版本
BUCKET_NAME = "bubblebucket-a1q5lb"
CHUNK_FOLDER = "chunks"
SRT_FOLDER = "srt"
TRANSCODER_FOLDER = "transcoder"
AUDIO_BATCH_SIZE_MB = 24

# Google Cloud 配置
PROJECT_ID = "bubble-dropzone-2-pgxrk7"
LOCATION = "us-central1"

### --- 優化開始 --- ###

def main_handler(request_data: dict or list):
    """
    主要處理入口，接收原始請求資料，進行驗證和分派。
    這個函式更有彈性，能處理物件或陣列形式的輸入。
    """
    try:
        # 1. 彈性處理輸入資料，相容 n8n 的 [{...}] 和標準的 {...} 格式
        if isinstance(request_data, list) and request_data:
            data = request_data[0]
            logger.info("偵測到輸入為陣列格式，已自動選取第一個物件。")
        elif isinstance(request_data, dict):
            data = request_data
            logger.info("接收到標準物件格式輸入。")
        else:
            raise ValueError("輸入資料格式不正確，必須是物件或非空陣列。")
        
        logger.info(f"接收到的任務資料: {data}")

        # 2. 驗證必要參數是否存在
        required_keys = [
            "video_url", "task_id", "user_id", "user_email", "user_name",
            "user_headpic", "user_lastname", "whisper_language", "prompt",
            "max_segment_mb", "n8n_webhook"
        ]
        
        # 將 n8n_webhook 重新命名為 webhook_url 以符合內部函式使用
        if "n8n_webhook" in data:
            data["webhook_url"] = data["n8n_webhook"]

        for key in required_keys:
            if key not in data and key != "n8n_webhook": # webhook_url 是其替代品
                 raise ValueError(f"缺少必要參數: '{key}'")

        # 3. 使用關鍵字參數解包，清晰地呼叫核心邏輯
        process_video_task_with_transcoder(**data)

    except (ValueError, KeyError) as e:
        logger.error(f"❌ 任務前置檢查失敗: {e}", exc_info=True)
        # 可以在此處增加發送到 Webhook 的失敗通知
        # (但通常在此階段失敗，可能連 webhook_url 都拿不到)
        return {"status": "error", "message": str(e)}, 400
    except Exception as e:
        logger.error(f"❌ 發生未預期的嚴重錯誤: {e}", exc_info=True)
        return {"status": "error", "message": "內部伺服器錯誤"}, 500

    return {"status": "ok", "message": "任務已成功接收並開始處理"}, 202

### --- 優化結束 --- ###


def process_video_task_with_transcoder(
    video_url, user_id, task_id, whisper_language, max_segment_mb, 
    webhook_url, prompt, user_email, user_name, user_headpic, user_lastname, **kwargs):
    
    # **kwargs 會接收任何額外傳入的參數，避免函式出錯
    
    logger.info(f"📥 開始使用 Transcoder 處理影片任務 {task_id}")
    logger.info(f"🌐 影片來源：{video_url}")
    logger.info(f"👤 使用者：{user_id} ({user_email})")
    logger.info(f"🌍 語言：{whisper_language}")
    logger.info(f"🔔 Webhook：{webhook_url}")
    logger.info(f"📝 提示詞：{prompt}")
    logger.info(f"🧪 程式版本：{VERSION}")

    temp_dir = tempfile.mkdtemp()
    try:
        # (這裡之後的所有程式碼都與您原本的相同，因此省略以保持簡潔)
        # ... 
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

        # 提取基礎路徑用於組織檔案結構
        base_path = extract_base_path_from_url(video_url)
        if not base_path:
            raise RuntimeError(f"無法提取基礎路徑: {video_url}")
        
        logger.info(f"📁 基礎路徑：{base_path}")

        # 2. 建立 Transcoder 任務
        job_id = f"audio-extract-{user_id}-{task_id}"
        output_gcs_folder = f"gs://{base_path}/{TRANSCODER_FOLDER}/"
        
        transcoder_job = create_transcoder_job(input_gcs_uri, output_gcs_folder, job_id)
        if not transcoder_job:
            raise RuntimeError("建立 Transcoder 任務失敗")

        # 3. 等待 Transcoder 完成
        job_name = transcoder_job.name
        if not wait_for_transcoder_job(job_name):
            raise RuntimeError("Transcoder 任務失敗或超時")

        # 4. 下載轉換後的音檔
        output_gcs_uri = f"gs://{base_path}/{TRANSCODER_FOLDER}/audio_only.mp3"
        audio_path = os.path.join(temp_dir, "full_audio.mp3")
        if not download_audio_from_gcs(output_gcs_uri, audio_path):
            raise RuntimeError("下載音檔失敗")

        # 5. 分割音檔
        audio_chunks = split_audio_file(audio_path, max_segment_mb)
        if not audio_chunks:
            raise RuntimeError("分割音檔失敗")

        # 6. 處理音檔批次
        final_srt_parts = []
        total_duration_offset = 0.0
        
        for batch_idx, chunk_path in enumerate(audio_chunks):
            batch_count = batch_idx + 1
            logger.info(f"🚀 處理音檔批次 {batch_count}/{len(audio_chunks)}")
            
            chunk_name = f"audio_batch_{batch_count:03d}.mp3"
            chunk_blob_path = f"{base_path}/{CHUNK_FOLDER}/{chunk_name}"
            chunk_blob_path_clean = chunk_blob_path.replace(f"{BUCKET_NAME}/", "")
            upload_url = upload_to_gcs(chunk_path, chunk_blob_path_clean)
            logger.info(f"✅ 音檔批次上傳：{upload_url}")
            
            with open(chunk_path, "rb") as f:
                transcript = client.audio.transcriptions.create(
                    model="whisper-1",
                    file=f,
                    response_format="verbose_json",
                    language=whisper_language,
                    prompt=prompt or None,
                )
            
            srt_entries = []
            batch_duration = 0.0
            
            if transcript.segments:
                for segment in transcript.segments:
                    start_time = segment['start'] + total_duration_offset
                    end_time = segment['end'] + total_duration_offset
                    start_td = timedelta(seconds=start_time)
                    end_td = timedelta(seconds=end_time)
                    start_str = f"{int(start_td.total_seconds()) // 3600:02}:{int(start_td.total_seconds()) % 3600 // 60:02}:{int(start_td.total_seconds()) % 60:02},{start_td.microseconds // 1000:03}"
                    end_str = f"{int(end_td.total_seconds()) // 3600:02}:{int(end_td.total_seconds()) % 3600 // 60:02}:{int(end_td.total_seconds()) % 60:02},{end_td.microseconds // 1000:03}"
                    srt_entry = f"{start_str} --> {end_str}\n{segment['text'].strip()}"
                    srt_entries.append(srt_entry)
                    batch_duration = max(batch_duration, segment['end'])
            
            final_srt_parts.extend(srt_entries)
            total_duration_offset += batch_duration
            logger.info(f"📝 批次 {batch_count} 轉錄完成，{len(srt_entries)} 個片段")

        # 7. 生成最終 SRT
        if final_srt_parts:
            srt_path = os.path.join(temp_dir, "final.srt")
            with open(srt_path, "w", encoding="utf-8") as f:
                for i, srt_entry in enumerate(final_srt_parts):
                    f.write(f"{i + 1}\n{srt_entry}\n\n")

            srt_blob_path = f"{base_path}/{SRT_FOLDER}/final.srt"
            srt_blob_path_clean = srt_blob_path.replace(f"{BUCKET_NAME}/", "")
            srt_url = upload_to_gcs(srt_path, srt_blob_path_clean)
            logger.info(f"📄 SRT 已上傳：{srt_url}")

            payload = {
                "任務狀態": "成功", "user_id": user_id, "task_id": task_id, "video_url": video_url,
                "whisper_language": whisper_language, "srt_url": srt_url, "影片原始大小MB": total_mb,
                "音檔批次數": len(audio_chunks), "總時長秒": total_duration_offset,
                "轉換方式": "Google Transcoder API", "程式版本": VERSION,
                "user_email": user_email, "user_name": user_name, "user_headpic": user_headpic,
                "user_lastname": user_lastname,
            }
            requests.post(webhook_url, json=payload, timeout=10)
            logger.info("✅ 任務完成")
        else:
            raise Exception("沒有成功處理任何音檔批次")

    except Exception as e:
        logger.error(f"🔥 任務處理錯誤 - {e}", exc_info=True)
        payload = {
            "任務狀態": f"失敗: {str(e)}", "user_id": user_id, "task_id": task_id,
            "video_url": video_url, "whisper_language": whisper_language, "srt_url": "",
            "程式版本": VERSION, "user_email": user_email, "user_name": user_name,
            "user_headpic": user_headpic, "user_lastname": user_lastname,
        }
        try:
            requests.post(webhook_url, json=payload, timeout=10)
        except Exception as webhook_e:
            logger.error(f"🔥 發送失敗通知到 Webhook 時也發生錯誤: {webhook_e}")
    finally:
        logger.info(f"🧹 清除暫存資料夾：{temp_dir}")
        shutil.rmtree(temp_dir, ignore_errors=True)

# (其餘輔助函式 extract_base_path_from_url, convert_http_url_to_gcs_uri 等保持不變)
# ...
def extract_base_path_from_url(video_url):
    """從影片 URL 提取基礎路徑"""
    try:
        if video_url.startswith("https://storage.googleapis.com/"):
            gcs_path = video_url.replace("https://storage.googleapis.com/", "")
            base_path = "/".join(gcs_path.split("/")[:-1])
            return base_path
        else:
            raise ValueError(f"URL 不是有效的 GCS HTTP URL: {video_url}")
    except Exception as e:
        logger.error(f"❌ 提取基礎路徑失敗：{e}")
        return None

def convert_http_url_to_gcs_uri(http_url):
    """將 HTTP URL 轉換為 GCS URI"""
    try:
        if http_url.startswith("https://storage.googleapis.com/"):
            gcs_path = http_url.replace("https://storage.googleapis.com/", "")
            return f"gs://{gcs_path}"
        else:
            raise ValueError(f"URL 不是有效的 GCS HTTP URL: {http_url}")
    except Exception as e:
        logger.error(f"❌ URL 轉換失敗：{e}")
        return None

def create_transcoder_job(input_uri, output_folder_uri, job_id):
    """建立 Transcoder 任務來轉換影片為 MP3"""
    try:
        logger.info(f"🎬 建立 Transcoder 任務：{job_id}")
        logger.info(f"📥 輸入：{input_uri}")
        logger.info(f"📤 輸出目錄：{output_folder_uri}")
        
        audio_stream = transcoder_v1.AudioStream(
            codec="mp3", bitrate_bps=128000, sample_rate_hertz=44100, channel_count=2
        )
        mux_stream = transcoder_v1.MuxStream(
            key="audio_only", container="mp3", elementary_streams=["audio_stream"]
        )
        job = transcoder_v1.Job(
            input_uri=input_uri, output_uri=output_folder_uri,
            config=transcoder_v1.JobConfig(
                elementary_streams=[
                    transcoder_v1.ElementaryStream(key="audio_stream", audio_stream=audio_stream)
                ],
                mux_streams=[mux_stream]
            )
        )
        parent = f"projects/{PROJECT_ID}/locations/{LOCATION}"
        request = transcoder_v1.CreateJobRequest(parent=parent, job=job)
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
            state_names = {1: "PENDING", 2: "RUNNING", 3: "SUCCEEDED", 4: "FAILED"}
            state_name = state_names.get(job.state, f"UNKNOWN({job.state})")
            logger.info(f"📊 任務狀態：{state_name}")
            if job.state == 3:
                logger.info("✅ Transcoder 任務完成")
                return True
            elif job.state == 4:
                logger.error(f"❌ Transcoder 任務失敗")
                if hasattr(job, 'error') and job.error:
                    logger.error(f"錯誤詳情：{job.error}")
                return False
            time.sleep(30)
        logger.error(f"⏰ Transcoder 任務超時 ({timeout_minutes} 分鐘)")
        return False
    except Exception as e:
        logger.error(f"❌ 等待 Transcoder 任務失敗：{e}")
        return False

def download_audio_from_gcs(gcs_uri, local_path):
    """從 GCS 下載音檔"""
    try:
        if not gcs_uri.startswith("gs://"):
            raise ValueError(f"Invalid GCS URI: {gcs_uri}")
        uri_parts = gcs_uri[5:].split("/", 1)
        bucket = storage_client.bucket(uri_parts[0])
        blob = bucket.blob(uri_parts[1])
        logger.info(f"📥 從 GCS 下載音檔：{gcs_uri}")
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
        cmd = ["ffprobe", "-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0", audio_path]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"無法取得音檔時長：{result.stderr}")
        total_duration = float(result.stdout.strip())
        file_size_mb = os.path.getsize(audio_path) / 1024 / 1024
        logger.info(f"📊 音檔總時長：{total_duration:.2f}s，大小：{file_size_mb:.2f}MB")
        if file_size_mb <= chunk_size_mb:
            logger.info("📦 音檔大小符合限制，不需分割")
            return [audio_path]
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
            cmd = ["ffmpeg", "-y", "-i", audio_path, "-ss", str(start_time), "-t", str(end_time - start_time), "-c", "copy", chunk_path]
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
