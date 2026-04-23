"""
consumer.py — Kafka Consumer 主程式

流程：
  1. 連線 Kafka，訂閱 raw-news topic
  2. 持續讀取訊息（blocking loop）
  3. 每筆訊息經過三層資料品質處理：
     Layer 1: validate_article()  → 拒絕無效資料
     Layer 2: clean_article()     → 清洗、正規化、產生去重 ID
     Layer 3: DB ON CONFLICT      → 最終去重保障
  4. 將清洗後資料寫入本機 PostgreSQL

Consumer Group（group_id）的作用：
  Kafka 用 group_id 追蹤「這個消費者群組讀到哪裡了」（committed offset）
  如果 Consumer 容器重啟，會從上次記錄的位置繼續讀，而不是從頭開始。
  搭配 ON CONFLICT DO NOTHING，即使有少量重複消費也不會造成重複資料。
"""

import json
import os
import time

from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from data_cleaner import clean_article, validate_article
from db_writer import get_connection, insert_article

load_dotenv()


def create_consumer(max_retries: int = 10, retry_delay: int = 10) -> KafkaConsumer:
    """建立 Kafka Consumer，含重試邏輯（原因同 Producer）。"""
    topic = os.environ.get("NEWS_TOPIC", "raw-news")
    bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                # value_deserializer：自動將 bytes → JSON → Python dict
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                # group_id：讓 Kafka 記錄這個 consumer 的讀取進度
                group_id="news-consumer-group",
                # auto_offset_reset：如果這個 group 是第一次啟動（沒有 committed offset）
                # "earliest" = 從最舊的訊息開始讀（不遺漏歷史資料）
                # "latest"   = 從現在起開始讀（忽略舊訊息）
                auto_offset_reset="earliest",
            )
            print(f"[Consumer] Connected to Kafka. Subscribed to topic: '{topic}'")
            return consumer

        except NoBrokersAvailable:
            print(f"[Consumer] Attempt {attempt}/{max_retries}: Kafka not ready. "
                  f"Retrying in {retry_delay}s...")
            time.sleep(retry_delay)

    raise RuntimeError("[Consumer] Could not connect to Kafka after max retries.")


def main():
    consumer = create_consumer()

    # 建立 DB 連線（連到本機 PostgreSQL via host.docker.internal）
    conn = get_connection()
    print("[Consumer] Connected to PostgreSQL.")

    inserted_count = 0
    duplicate_count = 0
    error_count = 0

    print("[Consumer] Listening for messages...")

    # KafkaConsumer 是一個 iterable，每次 iteration 都會 blocking 等待下一筆訊息
    for message in consumer:
        raw_article = message.value

        # ── Layer 1: 驗證 ──────────────────────────────────────────────────────
        is_valid, reason = validate_article(raw_article)
        if not is_valid:
            print(f"[Consumer] SKIP (invalid): {reason} | title={raw_article.get('title', '')[:40]}")
            error_count += 1
            continue

        # ── Layer 2: 清洗與正規化 ──────────────────────────────────────────────
        cleaned = clean_article(raw_article)

        # ── Layer 3: 寫入 DB（含 ON CONFLICT 去重）────────────────────────────
        try:
            inserted = insert_article(conn, cleaned)

            if inserted:
                inserted_count += 1
                print(f"[Consumer] INSERTED: {cleaned['title'][:60]}")
            else:
                duplicate_count += 1
                # 只印前 16 字的 article_id（避免 log 太長）
                print(f"[Consumer] DUPLICATE: id={cleaned['article_id'][:16]}...")

        except Exception as e:
            print(f"[Consumer] DB ERROR: {e}")
            # DB 連線可能已斷線，嘗試重新連線
            try:
                conn.close()
            except Exception:
                pass
            try:
                conn = get_connection()
                print("[Consumer] DB reconnected.")
            except Exception as reconnect_err:
                print(f"[Consumer] DB reconnect failed: {reconnect_err}")
            error_count += 1

        # 每處理 10 筆印一次統計摘要
        total_processed = inserted_count + duplicate_count + error_count
        if total_processed > 0 and total_processed % 10 == 0:
            print(
                f"[Consumer] ── Stats: inserted={inserted_count}, "
                f"duplicates={duplicate_count}, errors={error_count} ──"
            )


if __name__ == "__main__":
    main()
