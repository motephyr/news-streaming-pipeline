FROM apache/airflow:2.8.1

# 切換到 root 安裝系統套件
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 切回 airflow user（安全最佳實踐：不以 root 執行應用程式）
USER airflow

# 安裝 Python 套件
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Airflow 官方 image 已設定好 entrypoint，不需要 CMD
# 啟動指令（webserver 或 scheduler）由 docker-compose.yml 的 command 欄位指定
