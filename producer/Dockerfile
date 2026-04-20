FROM python:3.11-slim

# 設定工作目錄
WORKDIR /app

# 先複製 requirements.txt（利用 Docker layer cache：
# 如果 requirements.txt 沒變，這層不會重新 build，加快後續開發速度）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 再複製應用程式碼
COPY . .

# 啟動 Producer 主程式
ENV PYTHONUNBUFFERED=1

CMD ["python", "producer.py"]
