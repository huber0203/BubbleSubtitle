import os
import requests
import subprocess
import tempfile
import shutil
from pydub import AudioSegment

def download_file(url, output_path):
    print(f"⬇️ 下載檔案：{url}")
    response = requests.get(url, stream=True)
    with open(output_path, 'wb') as f:
        shutil.copyfileobj(response.raw, f)
    print(f"✅ 下載完成：{output_path}")

def extract_audio(video_path, audio_path):
    print("🎧 擷取音訊...")
    command = ['ffmpeg', '-i', video_path, '-vn', '-acodec', 'libmp3lame', '-ar', '16000', audio_path]
    subprocess.run(command, check=True)
    print("✅ 擷取完成：", audio_path)

def split_audio_by_size(audio_path, max_size_mb):
    print(f"✂️ 分割音訊為每段不超過 {max_size_mb}MB")

    audio = AudioSegment.from_mp3(audio_path)
    segment_paths = []

    temp_dir = tempfile.mkdtemp()
    max_bytes = max_size_mb * 1024 * 1024
    current = 0
    part = 1

    while current < len(audio):
        end = current + 60 * 1000  # 初始分 1 分鐘
        while end < len(audio):
            chunk = audio[current:end]
            size = len(chunk.raw_data)
            if size >= max_bytes:
                break
            end += 10 * 1000  # 每次往後加 10 秒

        chunk = audio[current:end]
        output_path = os.path.join(temp_dir, f"part{part}.mp3")
        chunk.export(output_path, format="mp3")
        segment_paths.append(output_path)
        print(f"🧩 第 {part} 段完成，長度 {len(chunk) / 1000:.1f}s，儲存：{output_path}")

        current = end
        part += 1

    return segment_paths

def transcribe_with_whisper(audio_path, whisper_language, prompt):
    print(f"🧠 發送給 Whisper：{audio_path}")
    with open(audio_path, 'rb') as f:
        files = {'file': f}
        data = {
            'model': 'whisper-1',
            'language': whisper_language,
        }
        if prompt:
            data['prompt'] = prompt

        headers = {
            'Authorization': f"Bearer {os.getenv('OPENAI_API_KEY')}"
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

        # Step 1: 下載影片
        download_file(video_url, video_path)

        # Step 2: 擷取音訊
        extract_audio(video_path, audio_path)

        # Step 3: 分割音訊
        segments = split_audio_by_size(audio_path, max_segment_mb)

        # Step 4: Whisper 分段辨識
        full_transcript = ""
        for segment_path in segments:
            transcript = transcribe_with_whisper(segment_path, whisper_language, prompt)
            full_transcript += transcript + "\n"

        # Step 5: 回傳 n8n webhook
        payload = {
            "user_id": user_id,
            "task_id": task_id,
            "transcript": full_transcript.strip()
        }
        post_to_webhook(webhook_url, payload)

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
