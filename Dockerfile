# 使用官方 Python 映像檔作為基礎
FROM python:3.10-slim

# 設定工作目錄
WORKDIR /app

# 複製 requirements.txt 並安裝套件
# 這一步可以利用 Docker 的快取機制，如果 requirements.txt 沒有變動，就不會重新安裝
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 安裝 ffmpeg
RUN apt-get update && apt-get install -y ffmpeg && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 複製應用程式的所有程式碼
COPY . .

# 開放容器的 8080 port
EXPOSE 8080

# 設定環境變數，Cloud Run 會自動使用這個 port
ENV PORT=8080

# 啟動 Flask 應用程式
CMD ["python", "main.py"]
