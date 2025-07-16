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

VERSION = "v1.6.11"
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

# 主要入口點
def process_video_task(video_url, user_id, task_id, whisper_language, max_segment_mb, webhook_url, prompt, user_email=None, user_name=None, user_lastname=None, user_headpic=None):
    """主要處理函數 - 使用 Google Transcoder API"""
    result = process_video_task_with_transcoder(video_url, user_id, task_id, whisper_language, max_segment_mb, webhook_url, prompt, user_email)

    # 合併額外欄位進 webhook payload（模擬結果中回傳）
    if isinstance(result, dict):
        extra_fields = {
            "user_email": user_email,
            "user_name": user_name,
            "user_lastname": user_lastname,
            "user_headpic": user_headpic,
        }
        result.update({k: v for k, v in extra_fields.items() if v is not None})
    return result
