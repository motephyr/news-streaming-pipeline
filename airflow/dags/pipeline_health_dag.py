"""
pipeline_health_dag.py — Pipeline 健康檢查 DAG

排程：每小時執行一次（@hourly）

檢查邏輯：
  查詢過去 2 小時內是否有新文章寫入 news_articles table。
  若沒有 → raise ValueError，Task 狀態變為 Failed（在 Airflow UI 標紅）
  若有   → 印出統計，Task 成功

為何是 2 小時而不是 1 小時？
  給 Producer 一點餘裕：NewsAPI free tier 限制 100 req/day，
  若 FETCH_INTERVAL_SECONDS=300（5 分鐘），2 小時內理論上有 24 次機會。
  設為 2 小時避免因為短暫網路問題就誤報警。

實際工作中的延伸：
  Task 失敗時可接 EmailOperator 或 SlackWebhookOperator 發送告警通知。
"""

import os
from datetime import datetime, timedelta, timezone

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── DAG 預設參數 ───────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineer",
    "retries": 1,                           # Task 失敗後自動重試一次
    "retry_delay": timedelta(minutes=5),    # 重試間隔 5 分鐘
}


def check_pipeline_health(**context):
    """
    連線 PostgreSQL，查詢最近 2 小時的文章數。

    context 是 Airflow 傳入的執行上下文（execution date、run_id 等），
    使用 **kwargs 接收是 Airflow PythonOperator 的標準做法。
    """
    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

    two_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)

    with conn.cursor() as cur:
        # 查詢最近 2 小時內寫入的文章數
        cur.execute(
            "SELECT COUNT(*) FROM news_articles WHERE created_at > %s",
            (two_hours_ago,),
        )
        recent_count = cur.fetchone()[0]

        # 同時查詢總文章數，方便觀察整體成長
        cur.execute("SELECT COUNT(*) FROM news_articles")
        total_count = cur.fetchone()[0]

    conn.close()

    print(f"[HealthCheck] Articles in last 2h: {recent_count}")
    print(f"[HealthCheck] Total articles in DB: {total_count}")

    # 若最近 2 小時沒有新文章，Task 失敗（在 UI 標紅）
    # raise 的 exception 會被 Airflow 捕捉並標記 Task 為 Failed 狀態
    if recent_count == 0:
        raise ValueError(
            "PIPELINE ALERT: No articles inserted in the last 2 hours! "
            "Check producer and consumer container logs."
        )

    print(f"[HealthCheck] Pipeline is healthy. ✓")


# ── DAG 定義 ───────────────────────────────────────────────────────────────────
with DAG(
    dag_id="pipeline_health_check",
    description="每小時檢查 news_articles 是否持續有新資料寫入",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    default_args=default_args,
    # catchup=False：不補跑過去錯過的時間點
    # （若設為 True，DAG 啟動時會補跑從 start_date 到現在所有應該跑的次數）
    catchup=False,
    tags=["monitoring", "health-check"],
) as dag:

    health_check_task = PythonOperator(
        task_id="check_recent_article_count",
        python_callable=check_pipeline_health,
    )
