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

# ── Step 2: 確認 PostgreSQL 可連線 ───────────────────────────────────────────
echo ""
echo "[2/5] 確認本機 PostgreSQL 連線..."
source .env

# 用 psql 確認連線（需要本機有安裝 psql client）
if command -v psql &> /dev/null; then
  if psql -h localhost -p "${POSTGRES_PORT:-5432}" \
          -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT 1;" &> /dev/null; then
    echo "  PostgreSQL 連線成功。"
  else
    echo "  ❌ 無法連線到 PostgreSQL！請確認："
    echo "     1. 本機 PostgreSQL 正在運行（port ${POSTGRES_PORT:-5432}）"
    echo "     2. database '$POSTGRES_DB' 已建立"
    echo "     3. user '$POSTGRES_USER' 有連線權限"
    echo ""
    echo "  快速建立 DB 的指令："
    echo "     createdb -U postgres $POSTGRES_DB"
    echo "     psql -U postgres -c \"CREATE USER $POSTGRES_USER WITH PASSWORD '$POSTGRES_PASSWORD';\""
    echo "     psql -U postgres -c \"GRANT ALL PRIVILEGES ON DATABASE $POSTGRES_DB TO $POSTGRES_USER;\""
    exit 1
  fi
else
  echo "  psql 未安裝，跳過連線確認（若 DB 連線有問題，Consumer log 會顯示錯誤）。"
fi

# ── Step 3: 初始化資料庫 Table ────────────────────────────────────────────────
echo ""
echo "[3/5] 初始化資料庫 tables..."
if command -v psql &> /dev/null; then
  psql -h localhost -p "${POSTGRES_PORT:-5432}" \
       -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
       -f database/init.sql
  echo "  Tables 建立完成。"
else
  echo "  psql 未安裝，請手動執行："
  echo "     psql -U $POSTGRES_USER -d $POSTGRES_DB -f database/init.sql"
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
