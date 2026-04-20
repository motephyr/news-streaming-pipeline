-- ─────────────────────────────────────────────────────────────────────────────
-- init.sql — PostgreSQL 初始化腳本
-- 這個檔案會在 Postgres 容器第一次啟動時自動執行（透過 Docker volume mount）
-- ─────────────────────────────────────────────────────────────────────────────

-- ── Table 1：新聞文章（主要資料表）────────────────────────────────────────────
-- 設計原則：append-only（只新增，不更新），保留 raw_content 供除錯與稽核
CREATE TABLE IF NOT EXISTS news_articles (
    id           SERIAL PRIMARY KEY,

    -- article_id = SHA-256(url)，作為去重主鍵
    -- 為何用 SHA-256？長度固定（64 chars）、相同 URL 永遠產生相同 hash
    -- Consumer 在應用層去重 + DB UNIQUE 約束雙重保障
    article_id   VARCHAR(64) UNIQUE NOT NULL,

    title        TEXT NOT NULL,
    description  TEXT,
    url          TEXT NOT NULL,
    source_name  VARCHAR(255),
    author       VARCHAR(255),
    published_at TIMESTAMPTZ,             -- 儲存為 UTC，方便跨時區查詢

    -- 保留原始 JSON，當資料格式改變時可重新解析，不需重新抓取
    raw_content  JSONB,

    created_at   TIMESTAMPTZ DEFAULT NOW()  -- 寫入 DB 的時間（非文章發布時間）
);

-- ── Table 2：Pipeline 統計（Airflow DAG 每日寫入）────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_stats (
    id              SERIAL PRIMARY KEY,
    report_date     DATE NOT NULL,
    total_articles  INTEGER DEFAULT 0,    -- 當日新增文章數
    duplicate_count INTEGER DEFAULT 0,   -- 被去重略過的文章數
    error_count     INTEGER DEFAULT 0,   -- 驗證失敗數
    -- 各來源文章數，例如：{"BBC News": 12, "CNN": 8}
    sources         JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ── Indexes ───────────────────────────────────────────────────────────────────
-- article_id 用於去重查詢（ON CONFLICT 時使用）
CREATE INDEX IF NOT EXISTS idx_articles_article_id
    ON news_articles(article_id);

-- published_at 用於時間範圍查詢（Airflow DAG 查今日文章）
CREATE INDEX IF NOT EXISTS idx_articles_published_at
    ON news_articles(published_at DESC);

-- created_at 用於 Pipeline 健康檢查（查最近 2 小時有無新資料）
CREATE INDEX IF NOT EXISTS idx_articles_created_at
    ON news_articles(created_at DESC);

-- ── Seed Check（可選）────────────────────────────────────────────────────────
-- 確認建表成功，會印在 Postgres 啟動 log 裡
DO $$
BEGIN
    RAISE NOTICE 'Database initialized: news_articles and pipeline_stats tables ready.';
END $$;
