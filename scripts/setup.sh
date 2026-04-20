#!/bin/bash
# setup.sh — 首次啟動完整流程腳本
#
# 使用方式：
#   chmod +x scripts/setup.sh
#   ./scripts/setup.sh

set -e  # 任何指令失敗就立即停止

echo "======================================================"
echo "  News Streaming Pipeline — 首次啟動設定"
echo "======================================================"

# ── Step 1: 確認 .env 存在 ────────────────────────────────────────────────────
if [ ! -f .env ]; then
  echo ""
  echo "[1/5] 建立 .env 設定檔..."
  cp .env.example .env
  echo "  已從 .env.example 複製 .env"
  echo ""
  echo "  ⚠️  請先編輯 .env 填入以下值後重新執行此腳本："
  echo "     - NEWSAPI_KEY（至 https://newsapi.org/register 申請）"
  echo "     - POSTGRES_USER / POSTGRES_PASSWORD（本機 PostgreSQL 帳號）"
  echo "     - POSTGRES_DB（請先在本機 PostgreSQL 建立此 database）"
  echo ""
  exit 1
fi
echo "[1/5] .env 已存在，跳過。"

# ── Step 2: 等待 PostgreSQL 容器健康 ─────────────────────────────────────────
echo ""
echo "[2/5] 啟動 PostgreSQL 容器並等待就緒..."
source .env

docker compose up -d postgres

echo -n "  等待 postgres 健康檢查通過"
until docker compose exec postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" &> /dev/null; do
  echo -n "."
  sleep 2
done
echo ""
echo "  PostgreSQL 已就緒。"

# ── Step 3: 確認資料表已建立 ──────────────────────────────────────────────────
echo ""
echo "[3/5] 確認資料表已建立（由容器自動執行 init.sql）..."
TABLE_COUNT=$(docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
  "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('news_articles','pipeline_stats');")
if [ "$TABLE_COUNT" -eq 2 ]; then
  echo "  news_articles、pipeline_stats 資料表建立完成。"
else
  echo "  ❌ 資料表未建立，請檢查 database/init.sql 是否正確。"
  exit 1
fi

# ── Step 4: 初始化 Airflow DB ─────────────────────────────────────────────────
echo ""
echo "[4/5] 初始化 Airflow metadata..."
docker compose run --rm airflow-webserver airflow db migrate
docker compose run --rm airflow-webserver airflow users create \
  --username "${AIRFLOW_ADMIN_USERNAME:-admin}" \
  --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email "${AIRFLOW_ADMIN_EMAIL:-admin@example.com}"
echo "  Airflow 初始化完成。"

# ── Step 5: 啟動所有服務 ──────────────────────────────────────────────────────
echo ""
echo "[5/5] 啟動所有服務..."
docker compose up -d

echo ""
echo "======================================================"
echo "  啟動完成！"
echo ""
echo "  服務入口："
echo "    Airflow UI: http://localhost:8080"
echo "    帳號: ${AIRFLOW_ADMIN_USERNAME:-admin} / ${AIRFLOW_ADMIN_PASSWORD:-admin}"
echo ""
echo "  查看 logs："
echo "    docker compose logs -f producer"
echo "    docker compose logs -f consumer"
echo ""
echo "  驗證資料："
echo "    psql -U $POSTGRES_USER -d $POSTGRES_DB \\"
echo "      -c 'SELECT COUNT(*), MAX(created_at) FROM news_articles;'"
echo "======================================================"
