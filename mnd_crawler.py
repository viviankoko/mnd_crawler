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

MANUAL_CSV = BASE_DIR / "manual_gap.csv"
LATEST_CSV = BASE_DIR / "pla_daily_latest.csv"
FINAL_CSV = BASE_DIR / "pla_daily_clean_full.csv"

# 參考舊版 ASP.NET 爬蟲的關鍵字列表，略微擴充
TITLE_KEYWORDS = [
    "中共解放軍臺海周邊海、空域動態",
    "中共解放軍軍機",
    "中共解放軍進入我西南空域活動情況",
    "踰越海峽中線及進入我西南空域活動情況",
    "逾越海峽中線及進入我西南空域活動情況",
    "我西南空域空情動態",
    "臺海周邊空域空情動態",
    "偵獲共機、艦在臺海周邊活動情形",
    "臺海周邊空域情勢動態新聞稿",
    "臺海周邊空域情勢動態",
]


# ------------------------------------------------------------
# HTTP with retry
# ------------------------------------------------------------
def safe_get(url: str, max_retries: int = 3, timeout: int = 20) -> str | None:
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            r.encoding = r.apparent_encoding
            return r.text
        except Exception as e:
            print(f"⚠️ 第 {attempt} 次抓取失敗：{url} - {e}")
            if attempt == max_retries:
                print(f"❌ 放棄抓取：{url}")
                return None
            time.sleep(2)


# ------------------------------------------------------------
# 日期工具
# ------------------------------------------------------------
def normalize_date_str(s: str) -> str:
    """
    統一成民國年：YYY/MM/DD
    - 民國：114/12/3 -> 114/12/03
    - 西元：2025/2/3 -> 114/02/03
    其他奇怪格式就原樣丟回去
    """
    s = str(s).strip()
    if not s:
        return s

    m_roc = re.match(r"^(\d{3})/(\d{1,2})/(\d{1,2})$", s)
    if m_roc:
        y = int(m_roc.group(1))
        m = int(m_roc.group(2))
        d = int(m_roc.group(3))
        return f"{y:03d}/{m:02d}/{d:02d}"

    m_ad = re.match(r"^(\d{4})/(\d{1,2})/(\d{1,2})$", s)
    if m_ad:
        y_ad = int(m_ad.group(1))
        m = int(m_ad.group(2))
        d = int(m_ad.group(3))
        y_roc = y_ad - 1911
        return f"{y_roc:03d}/{m:02d}/{d:02d}"

    return s


def roc_to_sort_key(s: str):
    try:
        y, m, d = s.split("/")
        return int(y), int(m), int(d)
    except Exception:
        return (0, 0, 0)


# ------------------------------------------------------------
# 列表頁
# ------------------------------------------------------------
def build_list_url(page: int) -> str:
    return LIST_BASE if page == 1 else f"{LIST_BASE}/{page}"


def crawl_list_page(page: int):
    """
    回傳 None：整頁 timeout / 503 之類
    回傳 []：有抓到頁面，但沒有任何符合關鍵字的項目
    回傳 list[{roc_date, url}]
    """
    url = build_list_url(page)
    print(f"\n🔍 抓列表頁：{url}")

    html = safe_get(url)
    if html is None:
        return None

    soup = BeautifulSoup(html, "html.parser")
    rows = []

    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)

        # 標題中有任一 keyword 才抓
        if not any(kw in text for kw in TITLE_KEYWORDS):
            continue

        # 例如：111.11.08 11月8日臺海周邊空域情勢動態新聞稿 點閱次數…
        m = re.search(r"\d{3}\.\d{2}\.\d{2}", text)
        if not m:
            continue

        roc_date = m.group(0)  # 111.11.08
        href = a["href"]
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
# 內文
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
# 主流程：爬到 list/189
# ------------------------------------------------------------
def crawl_all_pages(max_pages: int = 189) -> pd.DataFrame:
    data_rows = []

    for page in range(1, max_pages + 1):
        entries = crawl_list_page(page)

        # 整頁爆掉 → 略過繼續
        if entries is None:
            print(f"⚪ 第 {page} 頁抓取失敗，略過。")
            continue

        # 有頁面但剛好沒有符合 keyword 的條目 → 也繼續
        if len(entries) == 0:
            print(f"⚪ 第 {page} 頁沒有符合關鍵字的公告。")
            continue

        for entry in entries:
            text = crawl_article_text(entry["url"])
            date_str = entry["roc_date"].replace(".", "/")
            date_str = normalize_date_str(date_str)

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
        df_raw = pd.read_csv(MANUAL_CSV, dtype=str)

        if {"日期", "內容"}.issubset(df_raw.columns):
            df_manual = df_raw[["日期", "內容"]].copy()
        else:
            df_manual = pd.read_csv(
                MANUAL_CSV, header=None, names=["日期", "內容"], dtype=str
            )
    else:
        print("⚠️ 找不到 manual_gap.csv，只使用本次爬到的資料。")
        df_manual = pd.DataFrame(columns=["日期", "內容"])

    if not df_manual.empty:
        df_manual["日期"] = df_manual["日期"].astype(str).map(normalize_date_str)
    if not df_new.empty:
        df_new["日期"] = df_new["日期"].astype(str).map(normalize_date_str)

    df_manual = df_manual.drop_duplicates(subset=["日期"], keep="first")
    df_new = df_new.drop_duplicates(subset=["日期"], keep="first")

    df_all = pd.concat([df_manual, df_new], ignore_index=True)
    df_all = df_all.drop_duplicates(subset=["日期"], keep="first")
    df_all = df_all.sort_values(by="日期", key=lambda col: col.map(roc_to_sort_key))

    return df_all


# ------------------------------------------------------------
# main
# ------------------------------------------------------------
def main():
    print("🚀 開始爬取國防部區域動態…")

    df_new = crawl_all_pages()
    print(f"\n✅ 本次共爬到 {len(df_new)} 筆資料")

    if not df_new.empty:
        # 這次新爬到的原始資料（有欄位名稱）
        df_new.to_csv(LATEST_CSV, index=False, encoding="utf-8-sig")
        print(f"📝 已寫入最新爬取資料：{LATEST_CSV}")

    df_final = merge_with_manual(df_new)
    df_final.to_csv(FINAL_CSV, index=False, encoding="utf-8-sig")

    print(f"🏁 已寫入最終完整資料：{FINAL_CSV}")
    print(f"📊 最終筆數：{len(df_final)}")


if __name__ == "__main__":
    main()
