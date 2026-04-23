"""
daily_report_dag.py — 每日批次統計 DAG

排程：每天 UTC 00:00 執行（@daily）

功能：
  計算前一天（或當天）的新聞攝取統計：
  - 文章總數
  - 各來源文章數（source breakdown）
  將結果寫入 pipeline_stats table，方便後續查詢或接 dashboard。

冪等性（Idempotency）設計：
  使用 context["ds"]（Airflow 的 execution date，格式 "YYYY-MM-DD"）
  而非 datetime.now()。
  好處：重跑同一天的 DAG，結果相同，不會產生重複 stats 記錄。

實際工作中的延伸：
  stats 資料可接 Grafana、Metabase 等 BI 工具做成 dashboard，
  或用 EmailOperator 每天寄發報表給 stakeholders。
"""

import json
import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine, text

default_args = {
    "owner": "data-engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def generate_daily_report(**context):
    """
    計算指定日期的文章統計並寫入 pipeline_stats。

    context["ds"]：Airflow 傳入的 execution date（"YYYY-MM-DD"）
    使用這個而非 datetime.now() 是為了確保冪等性。
    """
    # Airflow 傳入執行日期，格式 "YYYY-MM-DD"
    report_date = context["ds"]
    print(f"[DailyReport] Generating report for date: {report_date}")

    host = os.environ["POSTGRES_HOST"]
    port = int(os.environ.get("POSTGRES_PORT", 5432))
    dbname = os.environ["POSTGRES_DB"]
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}",
        pool_pre_ping=True,
    )

    with engine.begin() as conn:
        # ── 1. 當日文章總數 ────────────────────────────────────────────────────
        total_articles = conn.execute(
            text("SELECT COUNT(*) FROM news_articles WHERE DATE(created_at) = :report_date"),
            {"report_date": report_date},
        ).scalar_one()

        # ── 2. 各來源文章數 ────────────────────────────────────────────────────
        # 結果範例：{"BBC News": 12, "CNN": 8, "Reuters": 5}
        source_rows = conn.execute(
            text("""
            SELECT source_name, COUNT(*) AS cnt
            FROM news_articles
            WHERE DATE(created_at) = :report_date
            GROUP BY source_name
            ORDER BY cnt DESC
            """),
            {"report_date": report_date},
        ).all()
        sources = {row[0] or "Unknown": row[1] for row in source_rows}

        # ── 3. 寫入統計結果 ────────────────────────────────────────────────────
        # ON CONFLICT (report_date) DO UPDATE：
        # 需要搭配 DB 的 unique index (report_date)。
        # 這樣重跑同一天會覆蓋該日統計，確保冪等。
        conn.execute(
            text("""
            INSERT INTO pipeline_stats (report_date, total_articles, duplicate_count, error_count, sources)
            VALUES (:report_date, :total_articles, :duplicate_count, :error_count, CAST(:sources AS JSONB))
            ON CONFLICT (report_date)
            DO UPDATE SET
                total_articles = EXCLUDED.total_articles,
                duplicate_count = EXCLUDED.duplicate_count,
                error_count = EXCLUDED.error_count,
                sources = EXCLUDED.sources
            """),
            {
                "report_date": report_date,
                "total_articles": total_articles,
                "duplicate_count": 0,
                "error_count": 0,
                "sources": json.dumps(sources, ensure_ascii=False),
            },
        )

    # 印出統計摘要（會顯示在 Airflow Task log 裡）
    print(f"[DailyReport] Date: {report_date}")
    print(f"[DailyReport] Total articles: {total_articles}")
    print(f"[DailyReport] Sources breakdown:")
    for source, count in sources.items():
        print(f"  {source}: {count} articles")
    print(f"[DailyReport] Report written to pipeline_stats table. ✓")


# ── DAG 定義 ───────────────────────────────────────────────────────────────────
with DAG(
    dag_id="daily_statistics_report",
    description="每日統計新聞攝取量並寫入 pipeline_stats table",
    schedule_interval="@daily",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=False,
    tags=["reporting", "statistics"],
) as dag:

    report_task = PythonOperator(
        task_id="generate_daily_report",
        python_callable=generate_daily_report,
    )
