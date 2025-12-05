#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
mnd_crawler.py  —  國防部「區域動態」爬蟲

功能：
1. full 模式（python mnd_crawler.py full）
   - 從 /news/plaactlist 開始一路往下爬，直到某頁沒有符合關鍵字的連結為止
   - 把所有符合關鍵字的文章日期＋全文抓下來
   - 與 manual_gap.csv 合併後輸出成 mnd_pla.csv

2. daily 模式（python mnd_crawler.py）
   - 只抓第 1 頁最上面一筆（假設是最新公告）
   - append 到既有的 mnd_pla.csv，再與 manual_gap.csv 合併覆寫 mnd_pla.csv

兩個模式都會：
- 保留「日期, 內容」兩欄
- 以「日期」去重、排序
- 若 manual_gap.csv 存在，會一起合併

注意：
- manual_gap.csv 可以有標題列「日期,內容」，也可以沒有。
"""

import sys
import time
import re
from pathlib import Path
from typing import List, Dict

import requests
from bs4 import BeautifulSoup
import pandas as pd

# ------------------------------------------------------------
# 基本設定
# ------------------------------------------------------------
BASE_URL = "https://www.mnd.gov.tw"
LIST_BASE = f"{BASE_URL}/news/plaactlist"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X)"}

BASE_DIR = Path(__file__).parent
OUTPUT_CSV = BASE_DIR / "mnd_pla.csv"
MANUAL_GAP = BASE_DIR / "manual_gap.csv"

# 和你舊版 ASPX 爬蟲一樣的標題關鍵字（確保各種版本都抓得到）
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


# ------------------------------------------------------------
# 工具：GET with retry
# ------------------------------------------------------------
def safe_get(url: str, retries: int = 3, timeout: int = 20) -> str | None:
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            r.encoding = r.apparent_encoding
            return r.text
        except Exception as e:
            print(f"⚠️ 第 {attempt} 次失敗：{url} - {e}")
            if attempt < retries:
                time.sleep(2)
    print(f"❌ 放棄抓取：{url}")
    return None


# ------------------------------------------------------------
# 列表頁：page=1 => /plaactlist，其餘 /plaactlist/2 ...
# ------------------------------------------------------------
def build_list_url(page: int) -> str:
    return LIST_BASE if page == 1 else f"{LIST_BASE}/{page}"


def crawl_list_page(page: int) -> List[Dict]:
    """
    抓某一頁列表，回傳：
      [{"roc_date": "114.12.03", "url": "https://..."}, ...]
    只保留標題含 KEYWORDS 任一字串的項目。
    """
    url = build_list_url(page)
    print(f"\n🔍 抓列表頁：{url}")

    html = safe_get(url)
    if html is None:
        print("⚠️ 列表頁抓取失敗，視為沒有資料。")
        return []

    soup = BeautifulSoup(html, "html.parser")
    rows: List[Dict] = []

    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        if not text:
            continue

        # 標題必須包含任一關鍵字
        if not any(kw in text for kw in KEYWORDS):
            continue

        # 列表上會有類似「114.12.03」的日期
        m = re.search(r"\d{3}\.\d{2}\.\d{2}", text)
        if not m:
            continue
        roc_date = m.group(0)

        href = a["href"]
        article_url = requests.compat.urljoin(BASE_URL, href)

        rows.append({"roc_date": roc_date, "url": article_url})

    print(f"📌 本頁抓到 {len(rows)} 筆（符合關鍵字）")
    return rows


# ------------------------------------------------------------
# 內頁：只抓 <div class="maincontent"> 的文字
# ------------------------------------------------------------
def extract_maincontent_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    main = soup.select_one("div.maincontent")
    if not main:
        return ""

    parts = list(main.stripped_strings)
    text = " ".join(parts)
    return text


def crawl_article(url: str) -> str:
    print(f"➡️ 抓文章頁：{url}")
    html = safe_get(url)
    if html is None:
        return ""
    return extract_maincontent_text(html)


# ------------------------------------------------------------
# 民國日期排序 key
# ------------------------------------------------------------
def roc_sort_key(s: str):
    try:
        y, m, d = s.split("/")
        return int(y), int(m), int(d)
    except Exception:
        return (0, 0, 0)


# ------------------------------------------------------------
# 合併補丁 manual_gap.csv
# ------------------------------------------------------------
def load_manual_gap() -> pd.DataFrame:
    if not MANUAL_GAP.exists():
        print("ℹ️ 找不到 manual_gap.csv，略過補丁。")
        return pd.DataFrame(columns=["日期", "內容"])

    print(f"📥 讀取補丁檔：{MANUAL_GAP}")
    gap = pd.read_csv(MANUAL_GAP, encoding="utf-8-sig")

    # 允許有標題列或沒有標題列
    if "日期" not in gap.columns or "內容" not in gap.columns:
        # 只拿前兩欄，改名成 日期 / 內容
        cols = list(gap.columns)
        if len(cols) < 2:
            raise ValueError("manual_gap.csv 至少需要兩欄（日期, 內容）")
        gap = gap.iloc[:, :2]
        gap.columns = ["日期", "內容"]
    else:
        gap = gap[["日期", "內容"]]

    return gap


def apply_manual_gap(df: pd.DataFrame) -> pd.DataFrame:
    gap = load_manual_gap()
    if not gap.empty:
        df = pd.concat([df, gap], ignore_index=True)

    if "日期" not in df.columns:
        raise ValueError("資料表缺少『日期』欄位，無法排序。")

    df = df.drop_duplicates(subset=["日期"], keep="last")
    df = df.sort_values("日期", key=lambda col: col.map(roc_sort_key))
    df = df.reset_index(drop=True)
    return df


# ------------------------------------------------------------
# 模式一：full — 從第 1 頁一路往下爬到「沒有資料」為止
# ------------------------------------------------------------
def run_full():
    print("🚀 [FULL] 全量模式開始")
    all_rows: List[Dict] = []
    page = 1

    while True:
        entries = crawl_list_page(page)
        if not entries:
            print(f"⚪ 第 {page} 頁沒有資料（或抓失敗），停止。")
            break

        for e in entries:
            date_str = e["roc_date"].replace(".", "/")
            content = crawl_article(e["url"])
            all_rows.append({"日期": date_str, "內容": content})
            time.sleep(0.3)

        page += 1
        time.sleep(1.0)

    df = pd.DataFrame(all_rows)
    df = apply_manual_gap(df)

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ 全量完成，共 {len(df)} 筆，已寫入 {OUTPUT_CSV.name}")


# ------------------------------------------------------------
# 模式二：daily — 只抓第 1 頁的最新一筆
# ------------------------------------------------------------
def run_daily():
    print("📅 [DAILY] 每日模式開始（只抓第 1 頁最上面一筆）")

    entries = crawl_list_page(1)
    if not entries:
        print("⚠️ 第 1 頁沒有符合關鍵字的資料。")
        return

    newest = entries[0]
    date_str = newest["roc_date"].replace(".", "/")
    content = crawl_article(newest["url"])

    df_new = pd.DataFrame([{"日期": date_str, "內容": content}])

    if OUTPUT_CSV.exists():
        df_old = pd.read_csv(OUTPUT_CSV, encoding="utf-8-sig")
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new

    df = apply_manual_gap(df)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ 每日更新完成，目前共 {len(df)} 筆，已寫入 {OUTPUT_CSV.name}")


# ------------------------------------------------------------
# main
# ------------------------------------------------------------
if __name__ == "__main__":
    # python mnd_crawler.py full  => 全量
    # python mnd_crawler.py       => 每日
    if len(sys.argv) > 1 and sys.argv[1].lower() == "full":
        run_full()
    else:
        run_daily()
