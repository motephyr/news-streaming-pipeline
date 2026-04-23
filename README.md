# News Real-time Streaming Pipeline

在處理多來源即時資料的需求時，研究並實作的串流 Pipeline。

從 NewsAPI 抓取新聞作為驗證資料源，透過 Apache Kafka 解耦生產與消費端，經三層資料品質防護後寫入 PostgreSQL，並以 Apache Airflow 排程監控 Pipeline 健康狀態與每日統計。

---

## 架構圖

```
NewsAPI
    │
    ▼
┌──────────────┐
│  Producer    │  Python — 每 5 分鐘抓取新聞，推入 Kafka topic
└──────┬───────┘
       │  Kafka topic: raw-news
       ▼
┌──────────────┐
│  Consumer    │  Python — 三層資料品質防護 → 寫入 PostgreSQL
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  PostgreSQL  │  本機運行 — 儲存結構化新聞資料與統計
└──────────────┘
       ▲
       │
┌──────────────┐
│   Airflow    │  Docker — 排程健康檢查 + 每日批次報表
└──────────────┘
```

---

## 技術棧

| 技術 | 版本 | 用途 |
|------|------|------|
| Apache Kafka | 7.4.0 (Confluent) | 即時訊息佇列 |
| Apache Airflow | 2.8.1 | Pipeline 排程與監控 |
| PostgreSQL | 15 | 結構化資料儲存 |
| Streamlit | 1.35.0 | 報表視覺化 Dashboard |
| Python | 3.11 | Producer / Consumer / Dashboard 邏輯 |
| Docker Compose | — | 容器化部署 |
| kafka-python | 2.0.2 | Python Kafka client |
| SQLAlchemy | 2.0.30 | DB 存取層（Consumer / Airflow / Dashboard） |
| psycopg2-binary | 2.9.9 | PostgreSQL driver（SQLAlchemy 的 DBAPI） |

---

## 前置需求

- **Docker Desktop**（已安裝並運行）
- **NewsAPI 金鑰**（免費申請：[newsapi.org/register](https://newsapi.org/register)）

---

## 快速啟動

### Step 1：設定環境變數

```bash
cp .env.example .env
# 編輯 .env，填入：
# NEWSAPI_KEY=your_key_here
# POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DB
```

### Step 2：首次啟動

```bash
./scripts/setup.sh
```

腳本預設使用 `docker-compose.full.yml`（內含 PostgreSQL），會自動初始化 Airflow、建立管理員帳號並啟動所有服務。

若要改用其他 compose 檔，可覆寫 `COMPOSE_FILE`：

```bash
COMPOSE_FILE=docker-compose.yml ./scripts/setup.sh
```

### Step 3：服務入口

| 服務 | 網址 | 說明 |
|------|------|------|
| Airflow UI | http://localhost:8080 | DAG 管理與監控 |
| Dashboard | http://localhost:8501 | 報表視覺化 |

---

## Docker Compose 版本說明

本專案提供兩個 Compose 檔案：

| 檔案 | PostgreSQL | 適用情境 |
|------|-----------|---------|
| `docker-compose.yml` | 外部（host.docker.internal） | 已有獨立 PostgreSQL 容器或本機 DB |
| `docker-compose.full.yml` | 內含（自動建立容器） | 一鍵啟動、完全自給自足 |

```bash
# 使用外部 PostgreSQL
docker compose up -d

# 使用內建 PostgreSQL（一鍵啟動）
docker compose -f docker-compose.full.yml up -d
```

---

## 專案結構

```
news-streaming-pipeline/
├── docker-compose.yml          # 使用外部 PostgreSQL
├── docker-compose.full.yml     # 含內建 PostgreSQL（自給自足）
├── .env.example                # 環境變數範本
├── .gitignore
├── README.md
│
├── producer/
│   ├── news_fetcher.py         # 封裝 NewsAPI 呼叫邏輯
│   ├── producer.py             # 主程式：抓新聞 → 推 Kafka
│   ├── Dockerfile
│   └── requirements.txt
│
├── consumer/
│   ├── data_cleaner.py         # 資料品質核心：驗證、清洗、去重
│   ├── db_writer.py            # PostgreSQL 寫入邏輯
│   ├── consumer.py             # 主程式：讀 Kafka → 清洗 → 寫入
│   ├── Dockerfile
│   └── requirements.txt
│
├── airflow/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
│       ├── pipeline_health_dag.py   # 每小時健康檢查
│       └── daily_report_dag.py      # 每日批次統計
│
├── dashboard/
│   ├── app.py                  # Streamlit 報表頁面
│   ├── Dockerfile
│   └── requirements.txt
│
├── database/
│   └── init.sql                # 建表 SQL
│
└── scripts/
    ├── setup.sh                # 首次啟動腳本
    └── test_pipeline.sh        # 端對端測試腳本
```

---

## 元件說明

### Kafka Producer（`producer/`）

每隔 `FETCH_INTERVAL_SECONDS`（預設 300 秒）呼叫一次 NewsAPI，將每篇文章加上 `fetched_at` 時間戳後，序列化為 JSON 推入 `raw-news` topic。

包含重試邏輯（最多 10 次），應對 Docker 容器啟動時 Kafka 尚未 ready 的情況。

### Kafka Consumer（`consumer/`）

訂閱 `raw-news` topic，持續消費訊息。每筆訊息經過三層品質防護：

**Layer 1 — 驗證（`validate_article`）**
拒絕沒有 URL、沒有標題、或 NewsAPI `[Removed]` 佔位符的文章。

**Layer 2 — 清洗（`clean_article`）**
清除多餘空白、統一時間格式（UTC）、以 SHA-256(url) 產生 `article_id`。

**Layer 3 — DB 約束**
`INSERT ... ON CONFLICT (article_id) DO NOTHING`。即使 Consumer 因 crash 重啟重新讀取 Kafka offset，也不會產生重複資料（冪等寫入）。

### Airflow DAGs（`airflow/dags/`）

**`pipeline_health_dag`**（每小時）
查詢過去 2 小時內是否有新文章寫入。若無，Task 失敗並在 UI 標紅，方便值班人員快速發現問題。

**`daily_statistics_report`**（每日 UTC 00:00）
計算當日文章總數與各來源分布，寫入 `pipeline_stats` table。使用 `context["ds"]`（Airflow execution date），並透過 `ON CONFLICT (report_date) DO UPDATE` 確保冪等性——重跑同一天的 DAG 只會覆蓋同一列。

### 資料庫設計（`database/init.sql`）

| Table | 說明 |
|-------|------|
| `news_articles` | 主要資料表，含 `article_id UNIQUE` 去重約束與 `raw_content JSONB` 保留原始資料 |
| `pipeline_stats` | Airflow 每日寫入的統計摘要 |

---

## 一篇文章的完整旅程

```
1. NewsAPI 回傳原始 JSON（含 title, url, publishedAt, source...）
2. Producer 加上 fetched_at，以 JSON 格式推入 Kafka topic "raw-news"
3. Consumer 從 Kafka 讀取訊息
4. validate_article()：沒有 url 或 title → 拒絕；[Removed] → 拒絕
5. clean_article()：
   - 產生 article_id = SHA-256("https://bbc.com/...")
   - 清除標題多餘空白
   - 解析 "2024-01-15T10:30:00Z" → datetime(2024, 1, 15, 10, 30, tzinfo=UTC)
6. insert_article()：
   - 若 article_id 已存在 → ON CONFLICT DO NOTHING（略過）
   - 若為新文章 → 寫入，rowcount=1
7. 每日 UTC 00:00，Airflow DAG 統計當天寫入筆數與來源分布
```

---

## 常見問題

**Q: Producer log 顯示 "Kafka not ready. Retrying..."**
A: 正常現象，Kafka 需要約 15-30 秒啟動。Producer 會自動重試，等待即可。

**Q: Consumer log 顯示 DB 連線錯誤**
A: 確認本機 PostgreSQL 正在運行（`pg_isready`），以及 `.env` 中的連線資訊正確。

**Q: 我想用 `docker-compose.yml`（外部 PostgreSQL）而不是 full 版**
A: `setup.sh` 預設跑 full 版。若要外部 DB，請使用：
`COMPOSE_FILE=docker-compose.yml ./scripts/setup.sh`

**Q: Airflow DAG 顯示 "No module named psycopg2"**
A: 重新 build Airflow image：`docker compose build airflow-webserver`

**Q: NewsAPI 回傳 "rateLimited" 錯誤**
A: 免費方案每天 100 次請求。將 `FETCH_INTERVAL_SECONDS` 調高至 900（15 分鐘）。

---

## 未來擴充方向

- **多資料來源**：加入 RSS feed（BBC、Reuters）補充 NewsAPI 的免費限制
- **Schema Registry**：使用 Confluent Schema Registry 管理 Kafka 訊息格式
- **告警通知**：Airflow 健康檢查失敗時，透過 SlackWebhookOperator 發送通知
- **S3 Archive**：Consumer 同時將 raw JSON 備份至 S3，建立 data lake
- **資料品質報告**：用 Great Expectations 取代自訂 `validate_article()`
