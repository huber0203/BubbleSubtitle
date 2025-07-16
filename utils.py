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

        # 2. 先下載 MP4 metadata (moov atom)
        metadata_path = os.path.join(temp_dir, "metadata.mp4")
        if not download_mp4_metadata(video_url, total_size, metadata_path):
            raise RuntimeError("無法下載 MP4 metadata")
        
        # 3. 計算影片分段數量
        num_video_chunks = (total_size + VIDEO_CHUNK_SIZE_BYTES - 1) // VIDEO_CHUNK_SIZE_BYTES
        logger.info(f"📦 預計分割為 {num_video_chunks} 個影片段")

        # 4. 音檔累積變數
        accumulated_audio = io.BytesIO()
        accumulated_size = 0
        audio_batch_count = 0
        total_duration_offset = 0.0  # 累計時間偏移
        final_srt_parts = []

        # 5. 逐段處理影片
        for chunk_idx in range(num_video_chunks):
            start_byte = chunk_idx * VIDEO_CHUNK_SIZE_BYTES
            end_byte = min(start_byte + VIDEO_CHUNK_SIZE_BYTES - 1, total_size - 1)
            
            logger.info(f"📦 處理影片段 {chunk_idx + 1}/{num_video_chunks}")
            
            # 5.1 下載影片段
            video_chunk_path = os.path.join(temp_dir, f"video_chunk_{chunk_idx:03d}.mp4")
            if not download_video_chunk(video_url, start_byte, end_byte, video_chunk_path):
                error_msg = f"影片段 {chunk_idx} 下載失敗"
                logger.error(f"❌ {error_msg}")
                raise RuntimeError(error_msg)
                
            # 5.2 組合 chunk + metadata 創建完整 MP4
            complete_video_path = os.path.join(temp_dir, f"complete_video_{chunk_idx:03d}.mp4")
            if not combine_chunk_with_metadata(video_chunk_path, metadata_path, complete_video_path):
                error_msg = f"影片段 {chunk_idx} metadata 組合失敗"
                logger.error(f"❌ {error_msg}")
                raise RuntimeError(error_msg)
                
            # 5.3 轉換為音檔
            audio_chunk_path = os.path.join(temp_dir, f"audio_chunk_{chunk_idx:03d}.mp3")
            chunk_duration = convert_to_audio(complete_video_path, audio_chunk_path)
            if chunk_duration is None:
                error_msg = f"音檔轉換失敗：chunk {chunk_idx} - 可能是影片格式問題或分段破壞了檔案結構"
                logger.error(f"❌ {error_msg}")
                raise RuntimeError(error_msg)
                
            # 5.4 讀取音檔內容
            with open(audio_chunk_path, 'rb') as f:
                audio_data = f.read()
            
            audio_size = len(audio_data)
            logger.info(f"🎵 音檔段 {chunk_idx}: {round(audio_size/1024/1024, 2)} MB, 時長: {chunk_duration:.2f}s")
            
            # 5.5 累積音檔
            accumulated_audio.write(audio_data)
            accumulated_size += audio_size
            
            # 5.6 檢查是否需要送 Whisper
            is_last_chunk = (chunk_idx == num_video_chunks - 1)
            should_process = (accumulated_size >= AUDIO_BATCH_SIZE_BYTES) or is_last_chunk
            
            if should_process and accumulated_size > 0:
                audio_batch_count += 1
                batch_size_mb = round(accumulated_size / 1024 / 1024, 2)
                logger.info(f"🚀 準備送 Whisper 批次 {audio_batch_count}，大小：{batch_size_mb} MB")
                
                # 5.7 處理音檔批次
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
                
                # 5.8 重置累積器
                accumulated_audio.close()
                accumulated_audio = io.BytesIO()
                accumulated_size = 0
            
            # 5.9 清除暫存檔案
            os.remove(video_chunk_path)
            os.remove(complete_video_path)
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

def download_mp4_metadata(video_url, total_size, metadata_path, metadata_size_mb=5):
    """下載 MP4 檔案的 metadata (從結尾開始，失敗則嘗試開頭)"""
    try:
        # 方法1：下載檔案末尾的 metadata（通常在最後幾 MB）
        metadata_bytes = metadata_size_mb * 1024 * 1024
        start_byte = max(0, total_size - metadata_bytes)
        end_byte = total_size - 1
        
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Range": f"bytes={start_byte}-{end_byte}"
        }
        
        logger.info(f"📥 嘗試下載檔案末尾 metadata：{headers['Range']}")
        
        with requests.get(video_url, headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(metadata_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        size_mb = round(os.path.getsize(metadata_path) / 1024 / 1024, 2)
        logger.info(f"✅ 末尾 metadata 下載完成：{size_mb} MB")
        
        # 驗證是否包含 moov atom
        with open(metadata_path, 'rb') as f:
            content = f.read()
            if b'moov' in content:
                logger.info("✅ 在檔案末尾找到 moov atom")
                return True
        
        logger.warning("⚠️ 檔案末尾未找到 moov atom，嘗試檔案開頭")
        
        # 方法2：下載檔案開頭的 metadata
        start_byte = 0
        end_byte = min(metadata_bytes - 1, total_size - 1)
        
        headers["Range"] = f"bytes={start_byte}-{end_byte}"
        logger.info(f"📥 嘗試下載檔案開頭 metadata：{headers['Range']}")
        
        with requests.get(video_url, headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(metadata_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        size_mb = round(os.path.getsize(metadata_path) / 1024 / 1024, 2)
        logger.info(f"✅ 開頭 metadata 下載完成：{size_mb} MB")
        
        # 驗證是否包含 moov atom
        with open(metadata_path, 'rb') as f:
            content = f.read()
            if b'moov' in content:
                logger.info("✅ 在檔案開頭找到 moov atom")
                return True
        
        logger.warning("⚠️ 檔案開頭也未找到 moov atom，嘗試開頭+結尾組合")
        
        # 方法3：同時下載開頭和結尾
        return download_combined_metadata(video_url, total_size, metadata_path, metadata_size_mb)
        
    except Exception as e:
        logger.error(f"Metadata 下載失敗：{e}")
        return False

def download_combined_metadata(video_url, total_size, metadata_path, metadata_size_mb):
    """下載開頭+結尾的組合 metadata"""
    try:
        metadata_bytes = metadata_size_mb * 1024 * 1024
        headers = {"User-Agent": "Mozilla/5.0"}
        
        logger.info(f"📥 下載開頭+結尾組合 metadata")
        
        with open(metadata_path, "wb") as output:
            # 下載開頭部分
            headers["Range"] = f"bytes=0-{metadata_bytes - 1}"
            logger.info(f"📥 下載開頭：{headers['Range']}")
            
            with requests.get(video_url, headers=headers, stream=True) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=8192):
                    output.write(chunk)
            
            # 下載結尾部分
            start_byte = max(metadata_bytes, total_size - metadata_bytes)
            end_byte = total_size - 1
            
            if start_byte < end_byte:  # 確保不重複下載
                headers["Range"] = f"bytes={start_byte}-{end_byte}"
                logger.info(f"📥 下載結尾：{headers['Range']}")
                
                with requests.get(video_url, headers=headers, stream=True) as r:
                    r.raise_for_status()
                    for chunk in r.iter_content(chunk_size=8192):
                        output.write(chunk)
        
        size_mb = round(os.path.getsize(metadata_path) / 1024 / 1024, 2)
        logger.info(f"✅ 組合 metadata 下載完成：{size_mb} MB")
        
        # 驗證是否包含 moov atom
        with open(metadata_path, 'rb') as f:
            content = f.read()
            if b'moov' in content:
                logger.info("✅ 在組合 metadata 中找到 moov atom")
                return True
        
        logger.error("❌ 所有方法都無法找到 moov atom")
        return False
        
    except Exception as e:
        logger.error(f"組合 metadata 下載失敗：{e}")
        return False

def combine_chunk_with_metadata(chunk_path, metadata_path, output_path):
    """將影片段與 metadata 組合成完整的 MP4"""
    try:
        # 方法1：簡單合併 - 將 chunk 和 metadata 合併
        with open(output_path, 'wb') as output:
            # 先寫入 chunk 內容
            with open(chunk_path, 'rb') as chunk_file:
                output.write(chunk_file.read())
            
            # 再寫入 metadata
            with open(metadata_path, 'rb') as meta_file:
                output.write(meta_file.read())
        
        # 驗證合併後的檔案
        if verify_mp4_structure(output_path):
            logger.info("✅ 簡單合併成功")
            return True
        
        logger.warning("⚠️ 簡單合併失敗，嘗試 FFmpeg 修復")
        
        # 方法2：使用 FFmpeg 修復
        temp_path = output_path.replace('.mp4', '_temp.mp4')
        os.rename(output_path, temp_path)
        
        cmd = [
            "ffmpeg", "-y",
            "-fflags", "+discardcorrupt+igndts",
            "-i", temp_path,
            "-c", "copy",
            "-movflags", "faststart",
            "-f", "mp4",
            output_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        os.remove(temp_path)
        
        if result.returncode == 0 and verify_mp4_structure(output_path):
            logger.info("✅ FFmpeg 修復成功")
            return True
        
        logger.error("❌ 所有組合方法都失敗")
        return False
        
    except Exception as e:
        logger.error(f"組合失敗：{e}")
        return False

def verify_mp4_structure(file_path):
    """驗證 MP4 檔案結構是否正確"""
    try:
        cmd = ["ffprobe", "-v", "quiet", "-show_entries", "format=duration", file_path]
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode == 0 and result.stdout.strip()
    except:
        return False
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
        # 方法1：嘗試直接轉換（適用於完整的影片段）
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
        if result.returncode == 0:
            # 成功，取得時長
            duration = get_audio_duration(audio_path)
            if duration > 0:
                return duration
        
        logger.warning(f"⚠️ 直接轉換失敗，嘗試修復檔案結構...")
        
        # 方法2：先用 ffmpeg 修復檔案結構
        temp_fixed_path = video_path.replace('.mp4', '_fixed.mp4')
        cmd = [
            "ffmpeg", "-y",
            "-fflags", "+discardcorrupt+igndts",
            "-i", video_path,
            "-c", "copy",
            "-movflags", "faststart",
            "-f", "mp4",
            temp_fixed_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            # 修復成功，再轉音檔
            cmd = [
                "ffmpeg", "-y", 
                "-i", temp_fixed_path,
                "-vn", "-acodec", "libmp3lame",
                "-ar", "44100", "-b:a", "32k",
                "-f", "mp3",
                audio_path
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                duration = get_audio_duration(audio_path)
                if duration > 0:
                    os.remove(temp_fixed_path)
                    return duration
            
            os.remove(temp_fixed_path)
        
        logger.warning(f"⚠️ 修復失敗，嘗試原始音檔提取...")
        
        # 方法3：嘗試直接提取音檔流（忽略容器格式）
        cmd = [
            "ffmpeg", "-y",
            "-fflags", "+discardcorrupt+igndts+ignidx",
            "-analyzeduration", "10000000",
            "-probesize", "10000000", 
            "-i", video_path,
            "-vn", "-acodec", "libmp3lame",
            "-ar", "44100", "-b:a", "32k",
            "-avoid_negative_ts", "make_zero",
            "-f", "mp3",
            audio_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            duration = get_audio_duration(audio_path)
            if duration > 0:
                return duration
        
        # 所有方法都失敗
        logger.error(f"FFmpeg 所有轉檔方法都失敗")
        logger.error(f"最後錯誤：{result.stderr}")
        return None
        
    except Exception as e:
        logger.error(f"音檔轉換錯誤：{e}")
        return None

def get_audio_duration(audio_path):
    """取得音檔時長"""
    try:
        cmd = ["ffprobe", "-v", "quiet", "-show_entries", "format=duration", 
               "-of", "csv=p=0", audio_path]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip():
            return float(result.stdout.strip())
    except:
        pass
    return 0.0

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
