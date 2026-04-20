"""
producer.py — Kafka Producer 主程式

流程：
  1. 連線到 Kafka（含重試，因容器啟動時 Kafka 可能還沒 ready）
  2. 每隔 FETCH_INTERVAL_SECONDS 秒從 NewsAPI 抓新聞
  3. 替每篇文章加上 fetched_at 時間戳（Consumer 可追蹤延遲）
  4. 將文章序列化為 JSON 推入 Kafka topic
  5. flush() 確保訊息真正送出後再 sleep
"""

import json
import os
import time

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from news_fetcher import NewsApiFetcher

load_dotenv()


def create_producer(max_retries: int = 10, retry_delay: int = 10) -> KafkaProducer:
    """
    建立 Kafka Producer，含重試邏輯。

    為何需要重試？
    docker compose up 時所有容器同時啟動，但 Kafka broker
    需要數秒到數十秒才能 ready。若 Producer 啟動太快會連線失敗。
    加上重試讓 Producer 等待 Kafka ready，而不是直接崩潰。
    """
    bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                # value_serializer：自動將 Python dict 轉為 UTF-8 JSON bytes
                # 這樣 send() 時直接傳 dict 即可，不需手動 json.dumps
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                # acks="all"：等待所有 replica 確認收到才算成功
                # 學習專案只有一個 broker，效果等同 acks=1，但習慣寫 "all" 更嚴謹
                acks="all",
                # 傳送失敗時自動重試 3 次
                retries=3,
            )
            print(f"[Producer] Connected to Kafka at {bootstrap_servers}")
            return producer

        except NoBrokersAvailable:
            print(f"[Producer] Attempt {attempt}/{max_retries}: Kafka not ready yet. "
                  f"Retrying in {retry_delay}s...")
            time.sleep(retry_delay)

    raise RuntimeError(
        f"[Producer] Could not connect to Kafka after {max_retries} attempts. "
        "Check that Kafka container is running."
    )


def main():
    topic = os.environ.get("NEWS_TOPIC", "raw-news")
    interval = int(os.environ.get("FETCH_INTERVAL_SECONDS", 300))

    producer = create_producer()
    fetcher = NewsApiFetcher()

    print(f"[Producer] Starting. Fetching every {interval}s → topic: '{topic}'")

    while True:
        articles = fetcher.fetch(country="us", page_size=20)

        sent_count = 0
        for article in articles:
            # 加上 fetched_at：Consumer 可計算「API 發布時間」到「寫入 Kafka 時間」的延遲
            article["fetched_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

            # send() 是非同步的，訊息先進 buffer
            producer.send(topic, value=article)
            sent_count += 1

        # flush() 等待 buffer 內所有訊息送出，確保資料不遺失
        producer.flush()
        print(f"[Producer] Sent {sent_count} messages to topic '{topic}'. "
              f"Sleeping {interval}s...")

        time.sleep(interval)


if __name__ == "__main__":
    main()
