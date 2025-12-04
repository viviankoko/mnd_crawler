# -*- coding: utf-8 -*-
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

# 檔案路徑（都放在 repo 根目錄）
MANUAL_CSV = BASE_DIR / "manual_gap.csv"          # 手動補的缺口
LATEST_CSV = BASE_DIR / "pla_daily_latest.csv"    # 這次爬到的最新 raw 資料（無欄位名稱）
FINAL_CSV = BASE_DIR / "pla_daily_clean_full.csv" # 合併後最終檔案（有「日期,內容」欄位）

# 關鍵字：沿用你 ASPX 舊版爬蟲的篩選標準
KEYWORDS = [
    "中共解放軍臺海周邊海、空域動態",
    "中共解放軍軍機",
    "中共解放軍進入我西南空域活動情況",
    "踰越海峽中線及進入我西南空域活動情況",
    "逾越海峽中線及進入我西南空域活動情況",
    "我西南空域空情動態",
    "臺海周邊空域空情動態",
    "偵獲共機、艦在臺海周邊活動情形",
]

# 共用 Session（效能好一點）
SESSION = requests.Session()


# ------------------------------------------------------------
# 工具：帶 retry 的 GET（列表頁、內頁共用）
# ------------------------------------------------------------
def safe_get(url: str, max_retries: int = 5, timeout: int = 40, sleep_base: float = 2.0):
    """
    帶重試機制的 GET：
    - 失敗時會最多重試 max_retries 次
    - 最後仍失敗就回傳 None（呼叫端自己決定怎麼處理）
    """
    for attempt in range(1, max_retries + 1):
        try:
            r = SESSION.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            r.encoding = r.apparent_encoding
            return r.text
        except Exception as e:
            print(f"⚠️ 第 {attempt} 次抓取失敗：{url} - {e}")
            if attempt == max_retries:
                print(f"❌ 放棄抓取：{url}")
                return None
            # 遞增等待時間（2 秒、4 秒、6 秒…）
            time.sleep(sleep_base * attempt)


# ------------------------------------------------------------
# 列表頁
# ------------------------------------------------------------
def build_list_url(page: int) -> str:
    """page=1: /plaactlist, page>=2: /plaactlist/2"""
    return LIST_BASE if page == 1 else f"{LIST_BASE}/{page}"


def crawl_list_page(page: int):
    """
    抓某一頁列表，只留我們關心的關鍵字標題。
    回傳：
        - list[dict]，每個元素：{roc_date, url}
        - 若整頁讀取失敗：回傳 None（給上層判斷「略過這一頁」）
    """
    url = build_list_url(page)
    print(f"\n🔍 抓列表頁：{url}")

    html = safe_get(url, max_retries=5, timeout=40)
    if html is None:
        # 明確標記這一頁失敗（與「正常但剛好沒有資料」區分）
        print(f"⚪ 第 {page} 頁抓取失敗，略過。")
        return None

    soup = BeautifulSoup(html, "html.parser")
    rows = []

    # 目前網站列表的日期通常直接寫在 a 文字前面，例如：
    # 111.11.08 11月8日臺海周邊空域空情動態新聞稿
    for a in soup.find_all("a", href=True):
        title = a.get_text(strip=True)
        if not any(kw in title for kw in KEYWORDS):
            continue

        # 抓 ROC 日期 111.11.08
        m = re.search(r"\d{3}\.\d{2}\.\d{2}", title)
        if not m:
            # 有些舊文可能沒帶這種格式，直接略過
            continue
        roc_date = m.group(0)

        href = a.get("href")
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
# 文章頁擷取
# ------------------------------------------------------------
def extract_maincontent_text(html: str) -> str:
    """
    只取 <div class="maincontent"> 裡面的文字，
    串成一行：
    「國防部今（8）日表示，迄1700時止，偵獲共機…」
    """
    soup = BeautifulSoup(html, "html.parser")
    main_div = soup.select_one("div.maincontent")
    if main_div is None:
        return ""

    parts = list(main_div.stripped_strings)
    return " ".join(parts)


def crawl_article_text(url: str) -> str:
    print(f"➡️ 抓文章頁：{url}")
    html = safe_get(url, max_retries=3, timeout=30)
    if html is None:
        # 內頁真的抓不到就留空字串，但不讓整個流程掛掉
        return ""
    return extract_maincontent_text(html)


# ------------------------------------------------------------
# 日期排序（民國年）
# ------------------------------------------------------------
def roc_to_sort_key(s: str):
    """
    把 '114/12/03' 轉成排序用 tuple (114, 12, 3)
    """
    try:
        y, m, d = s.split("/")
        return int(y), int(m), int(d)
    except Exception:
        return (0, 0, 0)


# ------------------------------------------------------------
# 主流程：抓所有頁面
# ------------------------------------------------------------
def crawl_all_pages(max_pages: int = 200):
    """
    從第 1 頁一路抓到 max_pages。
    特點：
      - 某一頁整頁 timeout → 記錄在 skipped_pages，繼續下一頁
      - 若連續 3 頁是「正常但沒有任何符合關鍵字的資料」才停止
    回傳：
      df_new: DataFrame(欄位：日期, 內容，日期為民國年格式 114/12/03)
      skipped_pages: list[int] 被略過的頁碼
    """
    data_rows = []
    skipped_pages = []
    empty_streak = 0

    for page in range(1, max_pages + 1):
        entries = crawl_list_page(page)

        # 整頁抓取失敗：略過
        if entries is None:
            skipped_pages.append(page)
            continue

        # 正常但沒有符合關鍵字的資料
        if not entries:
            empty_streak += 1
            print(f"🔚 第 {page} 頁無符合關鍵字的資料（連續 {empty_streak} 頁）")
            # 這裡採「連續 3 頁空」就停止，避免某一頁剛好沒有資料就提早結束
            if empty_streak >= 3:
                print("📴 連續 3 頁無資料，停止後續抓取。")
                break
            else:
                continue

        # 有資料 → reset 空頁計數
        empty_streak = 0

        for entry in entries:
            text = crawl_article_text(entry["url"])
            date_str = entry["roc_date"].replace(".", "/")  # 111.11.08 -> 111/11/08

            data_rows.append(
                {
                    "日期": date_str,
                    "內容": text,
                }
            )

        # 避免太兇猛被當機器人，頁與頁間稍微睡一下
        time.sleep(1.0)

    df = pd.DataFrame(data_rows)
    return df, skipped_pages


# ------------------------------------------------------------
# 合併 manual_gap.csv
# ------------------------------------------------------------
def merge_with_manual(df_new: pd.DataFrame) -> pd.DataFrame:
    """
    把這次爬到的 df_new 跟 manual_gap.csv 合併。
    manual_gap.csv 每列格式：
        114/11/29,中共解放軍臺海周邊海、空域動態 一、日期：…
    （沒有欄位名稱）
    """
    if MANUAL_CSV.exists():
        print(f"📥 讀取手動補齊檔案：{MANUAL_CSV}")
        df_manual = pd.read_csv(MANUAL_CSV, header=None, names=["日期", "內容"])
    else:
        print("⚠️ 未找到 manual_gap.csv，僅使用本次爬到的資料。")
        df_manual = pd.DataFrame(columns=["日期", "內容"])

    # 個別去重
    df_manual = df_manual.drop_duplicates(subset=["日期"], keep="first")
    df_new = df_new.drop_duplicates(subset=["日期"], keep="first")

    # manual 在前，新爬在後
    df_all = pd.concat([df_manual, df_new], ignore_index=True)

    # 再以「日期」去重，保留第一次（優先 manual）
    df_all = df_all.drop_duplicates(subset=["日期"], keep="first")

    # 依民國年月日排序
    df_all = df_all.sort_values(by="日期", key=lambda col: col.map(roc_to_sort_key))

    return df_all


# ------------------------------------------------------------
# main()
# ------------------------------------------------------------
def main():
    print("🚀 開始爬取國防部區域動態…")

    df_new, skipped_pages = crawl_all_pages(max_pages=200)
    print(f"\n✅ 本次共爬到 {len(df_new)} 筆資料")

    if skipped_pages:
        print(f"⚠️ 有被略過的列表頁（完整 timeout）：{skipped_pages}")

    # 最新一輪 raw 資料（維持無欄位名稱）
    if len(df_new) > 0:
        df_new.to_csv(LATEST_CSV, index=False, header=False, encoding="utf-8-sig")
        print(f"📝 已寫入最新爬取資料（無欄位名稱）：{LATEST_CSV}")
    else:
        print("⚠️ 本次沒有爬到任何新資料，LATEST 檔不會覆蓋。")

    # 合併 manual_gap + 本次新資料 → 最終完整資料（有欄位名稱）
    df_final = merge_with_manual(df_new)
    df_final.to_csv(FINAL_CSV, index=False, header=True, encoding="utf-8-sig")

    print(f"🏁 已寫入最終完整資料（含標題列）：{FINAL_CSV}")
    print(f"📊 最終資料筆數：{len(df_final)}")


if __name__ == "__main__":
    main()
