import os
import requests
import subprocess
import tempfile
import uuid

def download_file(url, output_path):
    r = requests.get(url, stream=True)
    with open(output_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

def convert_to_mp3(input_path, output_path):
    cmd = ["ffmpeg", "-i", input_path, "-vn", "-acodec", "libmp3lame", output_path]
    subprocess.run(cmd, check=True)

def split_mp3_by_size(input_path, segment_size_mb):
    segment_size_bytes = segment_size_mb * 1024 * 1024
    temp_dir = tempfile.mkdtemp()
    cmd = [
        "ffmpeg", "-i", input_path, "-f", "segment",
        "-segment_maxsize", str(segment_size_bytes),
        "-c", "copy", os.path.join(temp_dir, "part_%03d.mp3")
    ]
    subprocess.run(cmd, check=True)
    return [os.path.join(temp_dir, f) for f in os.listdir(temp_dir)]

def send_to_whisper(mp3_path, language):
    with open(mp3_path, 'rb') as f:
        response = requests.post(
            "https://api.openai.com/v1/audio/transcriptions",
            headers={"Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}"},
            data={"model": "whisper-1", "language": language},
            files={"file": f}
        )
    return response.json()

def post_to_webhook(webhook_url, payload):
    requests.post(webhook_url, json=payload)

def process_video_task(video_url, user_id, task_id, language, max_segment_mb, webhook_url):
    with tempfile.TemporaryDirectory() as tmpdir:
        video_path = os.path.join(tmpdir, "video.mp4")
        mp3_path = os.path.join(tmpdir, "audio.mp3")

        download_file(video_url, video_path)
        convert_to_mp3(video_path, mp3_path)

        segments = split_mp3_by_size(mp3_path, max_segment_mb)

        results = []
        for i, segment in enumerate(segments):
            result = send_to_whisper(segment, language)
            results.append({
                "segment": i,
                "text": result.get("text", "")
            })

        post_to_webhook(webhook_url, {
            "user_id": user_id,
            "task_id": task_id,
            "transcription": results
        })
