import os
import requests
import subprocess
import tempfile
import shutil
from typing import List

def download_file(url, output_path):
    print(f"⬇️ 下載檔案：{url}")
    response = requests.get(url, stream=True)
    with open(output_path, 'wb') as f:
        shutil.copyfileobj(response.raw, f)
    print(f"✅ 下載完成：{output_path}")

def extract_audio(video_path, audio_path):
    print("🎧 擷取音訊 (32kbps / 16kHz / mono)...")
    command = [
        'ffmpeg', '-i', video_path,
        '-vn', '-ar', '16000', '-ac', '1',
        '-b:a', '32k',
        audio_path
    ]
    subprocess.run(command, check=True)
    print("✅ 擷取完成：", audio_path)

def split_audio_by_ffmpeg(audio_path: str, max_size_mb: int) -> List[str]:
    print(f"✂️ 使用 ffmpeg 切割音訊，每段最多 {max_size_mb}MB...")
    output_dir = tempfile.mkdtemp()
    segment_pattern = os.path.join(output_dir, "part_%03d.mp3")
    
    command = [
        'ffmpeg', '-i', audio_path,
        '-f', 'segment',
        '-segment_size', str(max_size_mb * 1024),  # KB
        '-c', 'copy',
        segment_pattern
    ]
    subprocess.run(command, check=True)
    
    segments = sorted([
        os.path.join(output_dir, f) 
        for f in os.listdir(output_dir)
        if f.endswith(".mp3")
    ])
    
    print(f"✅ 共分割出 {len(segments)} 段")
    return segments

def transcribe_with_whisper(audio_path, whisper_language, prompt):
    print(f"🧠 發送給 Whisper：{audio_path}")
    
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("❌ 請設定 OPENAI_API_KEY 環境變數")
        raise Exception("缺少 OpenAI 金鑰，無法呼叫 Whisper API")
    
    with open(audio_path, 'rb') as f:
        files = {'file': f}
        data = {
            'model': 'whisper-1',
            'language': whisper_language,
        }
        if prompt:
            data['prompt'] = prompt

        headers = {
            'Authorization': f"Bearer {api_key}"
        }

        response = requests.post(
            "https://api.openai.com/v1/audio/transcriptions",
            headers=headers,
            files=files,
            data=data
        )

    if response.status_code != 200:
        print("❌ Whisper 失敗：", response.text)
        return ""

    text = response.json().get("text", "")
    print(f"✅ Whisper 成功：{text[:30]}...")
    return text

def post_to_webhook(webhook_url, payload):
    print("📬 回傳結果到 n8n webhook...")
    response = requests.post(webhook_url, json=payload)
    print("✅ 回傳狀態：", response.status_code)
    if response.status_code != 200:
        print("❌ 回傳錯誤：", response.text)

def process_video_task(
    video_url: str,
    user_id: str,
    task_id: str,
    whisper_language: str,
    max_segment_mb: int,
    webhook_url: str,
    prompt: str
):
    print("⚙️ 任務開始")
    try:
        temp_dir = tempfile.mkdtemp()
        video_path = os.path.join(temp_dir, "input.mp4")
        audio_path = os.path.join(temp_dir, "audio.mp3")

        download_file(video_url, video_path)
        extract_audio(video_path, audio_path)
        segments = split_audio_by_ffmpeg(audio_path, max_segment_mb)

        full_transcript = ""
        for idx, segment_path in enumerate(segments):
            print(f"🔍 分段 {idx+1}/{len(segments)}")
            transcript = transcribe_with_whisper(segment_path, whisper_language, prompt)
            full_transcript += transcript + "\n"

        post_to_webhook(webhook_url, {
            "user_id": user_id,
            "task_id": task_id,
            "transcript": full_transcript.strip()
        })

        print("✅ 任務完成")

    except Exception as e:
        print("🔥 任務錯誤：", str(e))
        post_to_webhook(webhook_url, {
            "user_id": user_id,
            "task_id": task_id,
            "error": str(e)
        })

    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print("🧹 清理暫存資料夾")
