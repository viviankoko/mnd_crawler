import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path
import re
import pandas as pd
import time

BASE_URL = "https://www.mnd.gov.tw"
LIST_BASE = f"{BASE_URL}/news/plaactlist"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X)"
}

BASE_DIR = Path(__file__).parent

# 路徑（都在 repo 根目錄）
MANUAL_CSV = BASE_DIR / "manual_gap.csv"
LATEST_CSV = BASE_DIR / "pla_daily_latest.csv"
FINAL_CSV = BASE_DIR / "pla_daily_clean_full.csv"


# ------------------------------------------------------------
# 工具：Retry 包裝（列表頁、文章頁共用）
# ------------------------------------------------------------
def safe_get(url: str, max_retries: int = 3, timeout: int = 20):
    """以 retry 方式抓取頁面，失敗會回傳 None（不讓程式 crash）"""
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            r.encoding = r.apparent_encoding
            return r.text
        except Exception as e:
            print(f"⚠️ 第 {attempt} 次抓取失敗：{url} - {e}")
            if attempt == max_retries:
                print(f"❌ 放棄抓取（最終失敗）：{url}")
                return None
            time.sleep(2)


# ------------------------------------------------------------
# 列表頁（有 retry、防 timeout、防 503、不讓 workflow 崩）
# ------------------------------------------------------------
def build_list_url(page: int) -> str:
    return LIST_BASE if page == 1 else f"{LIST_BASE}/{page}"


def crawl_list_page(page: int):
    url = build_list_url(page)
    print(f"\n🔍 抓列表頁：{url}")

    html = safe_get(url)
    if html is None:
        print(f"⚠️ 列表頁失敗，視為無資料 → 停止抓取後續頁面")
        return []  # 讓 crawl_all_pages() 停止

    soup = BeautifulSoup(html, "html.parser")
    rows = []

    for a in soup.find_all("a"):
        text = a.get_text(strip=True)
        if "中共解放軍臺海周邊海、空域動態" not in text:
            continue

        m = re.search(r"\d{3}\.\d{2}\.\d{2}", text)
        if not m:
            continue

        roc_date = m.group(0)
        href = a.get("href")
        if not href:
            continue

        article_url = urljoin(BASE_URL, href)

        rows.append(
            {
                "roc_date": roc_date,
                "url": article_url,
            }
        )

    print(f"📌 本頁抓到 {len(rows)} 筆")
    return rows


# ------------------------------------------------------------
# 文章頁擷取（有 retry、防噪音）
# ------------------------------------------------------------
def extract_maincontent_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    main_div = soup.select_one("div.maincontent")
    if main_div is None:
        return ""

    parts = list(main_div.stripped_strings)
    return " ".join(parts)


def crawl_article_text(url: str) -> str:
    print(f"➡️ 抓文章頁：{url}")

    html = safe_get(url)
    if html is None:
        return ""

    return extract_maincontent_text(html)


# ------------------------------------------------------------
# 日期排序工具（民國年）
# ------------------------------------------------------------
def roc_to_sort_key(s: str):
    try:
        y, m, d = s.split("/")
        return int(y), int(m), int(d)
    except Exception:
        return (0, 0, 0)


# ------------------------------------------------------------
# 主流程：抓所有頁面
# ------------------------------------------------------------
def crawl_all_pages(max_pages: int = 200) -> pd.DataFrame:
    data_rows = []

    for page in range(1, max_pages + 1):
        entries = crawl_list_page(page)

        if not entries:
            print(f"⚪ 第 {page} 頁無資料，結束抓取。")
            break

        for entry in entries:
            text = crawl_article_text(entry["url"])
            date_str = entry["roc_date"].replace(".", "/")

            data_rows.append(
                {
                    "日期": date_str,
                    "內容": text,
                }
            )

    return pd.DataFrame(data_rows)


# ------------------------------------------------------------
# 合併 manual_gap.csv
# ------------------------------------------------------------
def merge_with_manual(df_new: pd.DataFrame) -> pd.DataFrame:
    if MANUAL_CSV.exists():
        print(f"📥 讀取手動補齊檔案：{MANUAL_CSV}")
        df_manual = pd.read_csv(MANUAL_CSV, header=None, names=["日期", "內容"])
    else:
        print("⚠️ 未找到 manual_gap.csv")
        df_manual = pd.DataFrame(columns=["日期", "內容"])

    df_manual = df_manual.drop_duplicates(subset=["日期"], keep="first")
    df_new = df_new.drop_duplicates(subset=["日期"], keep="first")

    df_all = pd.concat([df_manual, df_new], ignore_index=True)
    df_all = df_all.drop_duplicates(subset=["日期"], keep="first")
    df_all = df_all.sort_values(by="日期", key=lambda col: col.map(roc_to_sort_key))

    return df_all


# ------------------------------------------------------------
# main()
# ------------------------------------------------------------
def main():
    print("🚀 開始爬取國防部區域動態…")

    df_new = crawl_all_pages()
    print(f"\n✅ 本次共爬到 {len(df_new)} 筆資料")

    if len(df_new) > 0:
        df_new.to_csv(LATEST_CSV, index=False, header=False, encoding="utf-8-sig")
        print(f"📝 已寫入最新爬取資料：{LATEST_CSV}")

    df_final = merge_with_manual(df_new)
    df_final.to_csv(FINAL_CSV, index=False, header=False, encoding="utf-8-sig")

    print(f"🏁 已寫入最終完整資料：{FINAL_CSV}")
    print(f"📊 最終筆數：{len(df_final)}")


if __name__ == "__main__":
    main()
