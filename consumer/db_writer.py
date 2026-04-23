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
import json

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

_ENGINE: Engine | None = None


def _build_db_url() -> str:
    """組出 SQLAlchemy 使用的 PostgreSQL 連線字串。"""
    host = os.environ["POSTGRES_HOST"]
    port = int(os.environ.get("POSTGRES_PORT", 5432))
    dbname = os.environ["POSTGRES_DB"]
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"


def get_engine() -> Engine:
    """建立 SQLAlchemy Engine。"""
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(_build_db_url(), pool_pre_ping=True)
    return _ENGINE


def get_connection() -> Connection:
    """
    建立並回傳 PostgreSQL 連線。

    連線參數從環境變數讀取，透過 .env 管理：
    - POSTGRES_HOST：容器內使用 host.docker.internal（指向宿主機）
    - POSTGRES_PORT：5432（本機 PostgreSQL 預設 port）
    - POSTGRES_DB / USER / PASSWORD：對應本機 DB 設定
    """
    return get_engine().connect()


def insert_article(conn: Connection, article: dict) -> bool:
    """
    將文章寫入 news_articles table。

    Layer 3 去重機制：
    ON CONFLICT (article_id) DO NOTHING
    → 若 article_id（SHA-256 of url）已存在，靜默略過
    → 即使 Consumer 因 crash 重啟並重新讀取 Kafka offset，也不會產生重複資料
    → 這是「exactly-once semantics」在學習專案中的簡化實作

    Args:
        conn:    SQLAlchemy connection 物件
        article: 已通過 clean_article() 的 dict

    Returns:
        True  → 成功寫入（新文章）
        False → 跳過（重複文章，article_id 已存在）
    """
    sql = text("""
        INSERT INTO news_articles
            (article_id, title, description, url, source_name, author,
             published_at, raw_content)
        VALUES
            (:article_id, :title, :description, :url,
             :source_name, :author, :published_at, CAST(:raw_content AS JSONB))
        ON CONFLICT (article_id) DO NOTHING
    """)

    article_data = {**article, "raw_content": json.dumps(article["raw_content"], ensure_ascii=False)}

    result = conn.execute(sql, article_data)
    # rowcount == 0：ON CONFLICT 觸發，代表是重複文章
    # rowcount == 1：成功寫入新文章
    inserted = result.rowcount > 0
    conn.commit()
    return inserted
