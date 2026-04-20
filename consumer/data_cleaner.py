"""
data_cleaner.py — 資料清洗、驗證、去重邏輯

這是整個 Pipeline 的資料品質核心。
設計為純函式（無副作用），所有邏輯可以獨立測試，不需要啟動 Kafka 或 PostgreSQL。

三層資料品質防護架構：
  Layer 1 - validate_article()：驗證必填欄位，拒絕無效資料
  Layer 2 - clean_article()：  正規化欄位格式，產生去重 ID
  Layer 3 - DB UNIQUE 約束：   最終保障，insert ON CONFLICT DO NOTHING（在 db_writer.py）
"""

import hashlib
import re
from datetime import datetime, timezone


def generate_article_id(url: str) -> str:
    """
    用 SHA-256 雜湊 URL，產生固定長度（64 字）的唯一識別碼。

    為何用 SHA-256 而不是 URL 本身？
    - URL 長度不固定，可能超過 DB 欄位限制
    - SHA-256 長度固定（64 hex chars），適合做 UNIQUE index
    - 相同 URL 永遠產生相同 hash（deterministic）→ 去重可靠
    - 即使 Consumer 因 crash 重啟，相同文章不會重複寫入

    面試說明重點：這是 idempotent（冪等）寫入的核心機制。
    """
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def clean_text(text: str | None) -> str | None:
    """
    清除文字欄位中的多餘空白與控制字元。

    處理場景：
    - NewsAPI 有時回傳前後有空格的標題
    - 內容中有連續多個空格或換行
    """
    if text is None:
        return None
    # re.sub 將連續空白（包含換行、tab）全部換成單一空格
    cleaned = re.sub(r"\s+", " ", text.strip())
    # 清完如果是空字串，回傳 None 而非空字串（DB 統一用 NULL 表示「沒有值」）
    return cleaned if cleaned else None


def parse_published_at(date_str: str | None) -> datetime | None:
    """
    解析 NewsAPI 的 ISO 8601 時間字串為 Python datetime 物件。

    NewsAPI 格式：'2024-01-15T10:30:00Z'
    Python 的 fromisoformat 不直接支援 'Z' 後綴（Python 3.10 以下）
    因此先將 'Z' 替換為 '+00:00' 再解析。

    回傳 timezone-aware datetime（UTC），方便跨時區比較。
    """
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        # 格式不符預期，回傳 None 而非拋例外
        print(f"[DataCleaner] Could not parse date: '{date_str}'")
        return None


def validate_article(article: dict) -> tuple[bool, str]:
    """
    Layer 1：資料驗證閘門。

    在清洗和寫入 DB 之前先做基本驗證，
    拒絕不符合最低要求的文章，避免垃圾資料進入 Pipeline。

    Args:
        article: 原始 NewsAPI 文章 dict

    Returns:
        (True, "ok")       → 通過驗證
        (False, "reason")  → 驗證失敗，附上原因
    """
    # 必填欄位：沒有 URL 就無法去重，沒有標題資料無意義
    if not article.get("url"):
        return False, "missing url"

    if not article.get("title"):
        return False, "missing title"

    # NewsAPI 對已下架或被法律要求移除的文章，title 會是 "[Removed]"
    # 這類文章內容完全空白，寫進 DB 沒有價值
    if article.get("title") == "[Removed]":
        return False, "removed article (NewsAPI '[Removed]' placeholder)"

    return True, "ok"


def clean_article(raw: dict) -> dict:
    """
    Layer 2：將原始 NewsAPI dict 正規化為乾淨的結構化資料。

    轉換邏輯：
    - 產生 article_id（SHA-256 of url）
    - 清除文字欄位空白
    - 解析時間字串為 datetime
    - 提取巢狀欄位（source.name）
    - 保留完整原始 JSON（raw_content）

    Args:
        raw: 已通過 validate_article() 的原始文章 dict

    Returns:
        dict，可直接傳給 db_writer.insert_article()
    """
    return {
        "article_id":   generate_article_id(raw["url"]),
        "title":        clean_text(raw.get("title")),
        "description":  clean_text(raw.get("description")),
        "url":          raw["url"],
        "source_name":  raw.get("source", {}).get("name"),
        "author":       clean_text(raw.get("author")),
        "published_at": parse_published_at(raw.get("publishedAt")),
        # raw_content 保留完整原始資料（包含 urlToImage、content 等未明確對應的欄位）
        # 用途：除錯、稽核、未來擴充欄位時不需重新抓 API
        "raw_content":  raw,
    }


# ── 本機快速測試 ──────────────────────────────────────────────────────────────
# 執行方式：python data_cleaner.py
if __name__ == "__main__":
    # 模擬一筆 NewsAPI 回傳的文章
    sample = {
        "source": {"id": "bbc-news", "name": "BBC News"},
        "author": "  BBC Sport  ",
        "title": "England   win the Ashes",
        "description": "England beat Australia in dramatic fashion...",
        "url": "https://www.bbc.com/sport/cricket/12345678",
        "urlToImage": "https://ichef.bbci.co.uk/news/example.jpg",
        "publishedAt": "2024-07-15T10:30:00Z",
        "content": "Full article text (truncated)...",
    }

    print("=== 驗證測試 ===")
    is_valid, reason = validate_article(sample)
    print(f"valid={is_valid}, reason={reason}")

    print("\n=== 清洗測試 ===")
    cleaned = clean_article(sample)
    for k, v in cleaned.items():
        if k != "raw_content":
            print(f"  {k}: {v!r}")

    print("\n=== article_id（SHA-256）===")
    print(f"  {cleaned['article_id']}")

    print("\n=== 驗證失敗測試 ===")
    bad_article = {"title": "[Removed]", "url": ""}
    print(validate_article(bad_article))
