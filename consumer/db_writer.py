"""
db_writer.py — PostgreSQL 寫入邏輯

職責：
- 建立/回傳資料庫連線
- 將清洗後的文章 insert 進 news_articles table
- 處理去重（ON CONFLICT DO NOTHING）

為何獨立成模組？
- consumer.py 只需關注「消費 Kafka、協調流程」
- 資料庫相關邏輯集中在此，換 DB 或調整 SQL 只需改這一個檔案
"""

import os

import psycopg2
from psycopg2.extras import Json


def get_connection():
    """
    建立並回傳 PostgreSQL 連線。

    連線參數從環境變數讀取，透過 .env 管理：
    - POSTGRES_HOST：容器內使用 host.docker.internal（指向宿主機）
    - POSTGRES_PORT：5432（本機 PostgreSQL 預設 port）
    - POSTGRES_DB / USER / PASSWORD：對應本機 DB 設定
    """
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def insert_article(conn, article: dict) -> bool:
    """
    將文章寫入 news_articles table。

    Layer 3 去重機制：
    ON CONFLICT (article_id) DO NOTHING
    → 若 article_id（SHA-256 of url）已存在，靜默略過
    → 即使 Consumer 因 crash 重啟並重新讀取 Kafka offset，也不會產生重複資料
    → 這是「exactly-once semantics」在學習專案中的簡化實作

    Args:
        conn:    psycopg2 connection 物件
        article: 已通過 clean_article() 的 dict

    Returns:
        True  → 成功寫入（新文章）
        False → 跳過（重複文章，article_id 已存在）
    """
    sql = """
        INSERT INTO news_articles
            (article_id, title, description, url, source_name, author,
             published_at, raw_content)
        VALUES
            (%(article_id)s, %(title)s, %(description)s, %(url)s,
             %(source_name)s, %(author)s, %(published_at)s, %(raw_content)s)
        ON CONFLICT (article_id) DO NOTHING
    """

    # psycopg2 的 JSONB 欄位需要明確用 Json() 包裝，
    # 否則會以字串形式傳入，導致 DB 型別錯誤
    article_data = {**article, "raw_content": Json(article["raw_content"])}

    with conn.cursor() as cur:
        cur.execute(sql, article_data)
        # rowcount == 0：ON CONFLICT 觸發，代表是重複文章
        # rowcount == 1：成功寫入新文章
        inserted = cur.rowcount > 0

    conn.commit()
    return inserted
