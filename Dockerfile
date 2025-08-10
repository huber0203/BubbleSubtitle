FROM python:3.10-slim

# 設定工作目錄
WORKDIR /app

# 複製當前目錄的所有檔案到容器中
COPY ./scripts/ /app/

# 安裝 ffmpeg 及 Python 套件
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 明確告訴 Cloud Run 對外開放 port 8080
EXPOSE 8080

# 設定環境變數（非必要，Cloud Run 會自動注入 PORT）
ENV PORT=8080

# 啟動 Flask 應用
CMD ["python", "main.py"]
