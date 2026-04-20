import os
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="News Pipeline Dashboard", layout="wide")


def get_engine():
    host = os.environ["POSTGRES_HOST"]
    port = os.environ.get("POSTGRES_PORT", 5432)
    db = os.environ["POSTGRES_DB"]
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


@st.cache_data(ttl=60)
def load_pipeline_stats():
    with get_engine().connect() as conn:
        return pd.read_sql("SELECT * FROM pipeline_stats ORDER BY report_date ASC", conn)


@st.cache_data(ttl=60)
def load_source_breakdown():
    with get_engine().connect() as conn:
        return pd.read_sql(
            """
            SELECT source_name, COUNT(*) AS article_count
            FROM news_articles
            GROUP BY source_name
            ORDER BY article_count DESC
            LIMIT 15
            """,
            conn,
        )


@st.cache_data(ttl=60)
def load_total_articles():
    with get_engine().connect() as conn:
        return conn.execute(text("SELECT COUNT(*) FROM news_articles")).scalar()


# ── 頁面標題 ──────────────────────────────────────────────────────────────────
st.title("News Pipeline Dashboard")
st.caption("每 60 秒自動更新｜資料來源：PostgreSQL")

# ── 指標卡片 ──────────────────────────────────────────────────────────────────
stats_df = load_pipeline_stats()
total = load_total_articles()

col1, col2, col3, col4 = st.columns(4)

if not stats_df.empty:
    latest = stats_df.iloc[-1]
    col1.metric("今日文章數", int(latest["total_articles"]))
    col2.metric("今日重複數", int(latest["duplicate_count"]))
    col3.metric("今日錯誤數", int(latest["error_count"]))
else:
    col1.metric("今日文章數", "—")
    col2.metric("今日重複數", "—")
    col3.metric("今日錯誤數", "—")

col4.metric("資料庫累積總數", total)

st.divider()

# ── 每日文章趨勢 ──────────────────────────────────────────────────────────────
st.subheader("每日文章趨勢")
if not stats_df.empty:
    fig1 = px.line(
        stats_df,
        x="report_date",
        y="total_articles",
        markers=True,
        labels={"report_date": "日期", "total_articles": "文章數"},
    )
    st.plotly_chart(fig1, use_container_width=True)
else:
    st.info("尚無統計資料，請先在 Airflow 觸發 daily_statistics_report DAG。")

# ── 重複與錯誤趨勢 ────────────────────────────────────────────────────────────
st.subheader("每日重複與錯誤數")
if not stats_df.empty:
    fig2 = px.bar(
        stats_df,
        x="report_date",
        y=["duplicate_count", "error_count"],
        barmode="group",
        labels={"report_date": "日期", "value": "筆數", "variable": "類型"},
    )
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ── 各來源文章數 ──────────────────────────────────────────────────────────────
st.subheader("各來源文章數（Top 15）")
source_df = load_source_breakdown()
if not source_df.empty:
    fig3 = px.bar(
        source_df,
        x="article_count",
        y="source_name",
        orientation="h",
        labels={"article_count": "文章數", "source_name": "來源"},
    )
    fig3.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig3, use_container_width=True)
else:
    st.info("尚無文章資料。")
