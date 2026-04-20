"""
news_fetcher.py — 封裝 NewsAPI 呼叫邏輯

為何獨立成一個模組？
- 單一職責原則：producer.py 只負責「推 Kafka」，這裡只負責「抓新聞」
- 易於獨立測試：可以不啟動 Kafka 直接測試這個模組
- 易於替換：未來換成 RSS 或其他 API，只需改這個檔案

NewsAPI 免費方案限制：
- 每天 100 次請求
- 文章內容截斷為 200 字
- 資料延遲 24 小時（非 developer/business 方案）
- 申請網址：https://newsapi.org/register
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()


class NewsApiFetcher:
    """從 NewsAPI 抓取新聞頭條。"""

    BASE_URL = "https://newsapi.org/v2/top-headlines"

    def __init__(self):
        self.api_key = os.environ["NEWSAPI_KEY"]

    def fetch(self, country: str = "us", page_size: int = 20) -> list[dict]:
        """
        抓取指定國家的新聞頭條。

        Args:
            country:   國家代碼，預設 "us"（美國）
            page_size: 每次抓取幾篇，最多 100（免費方案建議 20-50）

        Returns:
            list[dict]：每個 dict 是一篇文章，格式如下：
            {
              "source": {"id": "bbc-news", "name": "BBC News"},
              "author": "John Smith",
              "title": "...",
              "description": "...",
              "url": "https://...",
              "urlToImage": "https://...",
              "publishedAt": "2024-01-15T10:30:00Z",
              "content": "...(截斷至 200 字)..."
            }
            發生錯誤時回傳空 list，不拋例外（避免 Producer 整個崩潰）
        """
        params = {
            "country": country,
            "pageSize": page_size,
            "apiKey": self.api_key,
        }

        try:
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            response.raise_for_status()  # 4xx/5xx → 拋 HTTPError

            data = response.json()

            # NewsAPI 回傳 status: "ok" 或 "error"
            if data.get("status") != "ok":
                print(f"[NewsApiFetcher] API error: {data.get('message', 'unknown')}")
                return []

            articles = data.get("articles", [])
            print(f"[NewsApiFetcher] Fetched {len(articles)} articles from NewsAPI")
            return articles

        except requests.exceptions.Timeout:
            print("[NewsApiFetcher] Request timed out after 10s")
            return []
        except requests.exceptions.HTTPError as e:
            print(f"[NewsApiFetcher] HTTP error: {e.response.status_code} — {e}")
            return []
        except requests.exceptions.RequestException as e:
            print(f"[NewsApiFetcher] Network error: {e}")
            return []
        except Exception as e:
            print(f"[NewsApiFetcher] Unexpected error: {e}")
            return []


# ── 本機快速測試（不需 Docker）────────────────────────────────────────────────
# 執行方式：python news_fetcher.py
if __name__ == "__main__":
    fetcher = NewsApiFetcher()
    articles = fetcher.fetch(country="us", page_size=5)
    print(f"\n=== 取得 {len(articles)} 篇文章 ===")
    for i, article in enumerate(articles, 1):
        print(f"\n[{i}] {article.get('title', 'No title')}")
        print(f"    來源: {article.get('source', {}).get('name', 'Unknown')}")
        print(f"    發布: {article.get('publishedAt', 'Unknown')}")
        print(f"    URL:  {article.get('url', 'No URL')}")
