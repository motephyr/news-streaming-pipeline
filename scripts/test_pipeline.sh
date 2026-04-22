#!/bin/bash
# test_pipeline.sh — 端對端 Pipeline 測試腳本
#
# 用途：確認整個 Pipeline 正常運作
# 使用方式：
#   chmod +x scripts/test_pipeline.sh
#   ./scripts/test_pipeline.sh

set -e
source .env

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.full.yml}"
DC=(docker compose -f "$COMPOSE_FILE")

echo "======================================================"
echo "  Pipeline 端對端測試"
echo "======================================================"
echo "  使用 compose 檔案：$COMPOSE_FILE"

# ── Test 1: 確認所有容器運行中 ──────────────────────────────────────────────
echo ""
echo "[Test 1] 確認容器狀態..."
"${DC[@]}" ps

# ── Test 2: 查看 Producer 最新 log ──────────────────────────────────────────
echo ""
echo "[Test 2] Producer 最新 log（最後 10 行）："
"${DC[@]}" logs --tail=10 producer

# ── Test 3: 查看 Consumer 最新 log ──────────────────────────────────────────
echo ""
echo "[Test 3] Consumer 最新 log（最後 10 行）："
"${DC[@]}" logs --tail=10 consumer

# ── Test 4: 確認 PostgreSQL 有資料 ──────────────────────────────────────────
echo ""
echo "[Test 4] PostgreSQL 資料筆數："
if "${DC[@]}" config --services | grep -qx "postgres"; then
  "${DC[@]}" exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
     -c "SELECT COUNT(*) AS total_articles,
                MIN(created_at) AS first_inserted,
                MAX(created_at) AS last_inserted
         FROM news_articles;"

  echo ""
  echo "[Test 4b] 各來源文章數（Top 10）："
  "${DC[@]}" exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
     -c "SELECT source_name, COUNT(*) AS count
         FROM news_articles
         GROUP BY source_name
         ORDER BY count DESC
         LIMIT 10;"
else
  echo "  ⚠️  目前 compose 不含 postgres 服務，略過容器內 DB 測試。"
fi

# ── Test 5: 確認 Kafka topic 存在 ────────────────────────────────────────────
echo ""
echo "[Test 5] Kafka topic 列表："
"${DC[@]}" exec kafka \
  kafka-topics --bootstrap-server localhost:9092 --list

echo ""
echo "======================================================"
echo "  測試完成！若 [Test 4] 顯示 count > 0，Pipeline 運作正常。"
echo "======================================================"
