FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN apt-get update && \
    apt-get install -y ffmpeg && \
    pip install --no-cache-dir -r requirements.txt

ENV PORT=8080

CMD ["python", "main.py"]
