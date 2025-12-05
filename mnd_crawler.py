#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
mnd_crawler.py

功能：
- full：把目前網站上所有「區域動態」的共機/海域公告抓下來 → mnd_pla.csv
- daily：每天只抓最新一筆，append 到 mnd_pla.csv
- manual_gap.csv：補不了的日期用這個補，最後會一起 merge 進 mnd_pla.csv

特別處理：
- 列表頁用 https://www.mnd.gov.tw/news/plaactlist (+ /2, /3, ...)
- 內頁只抓 <div class="maincontent">
- 補丁檔 manual_gap.csv 要有兩欄：日期, 內容（含標題列）
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
# 常數設定
# ------------------------------------------------------------
BASE_URL = "https://www.mnd.gov.tw"
LIST_BASE = f"{BASE_URL}/news/plaactlist"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X)"}

BASE_DIR = Path(__file__).parent
OUTPUT_CSV = BASE_DIR / "mnd_pla.csv"
MANUAL_GAP = BASE_DIR / "manual_gap.csv"

# 你原本 ASPX 版本的關鍵字（確定抓得到所有版本的共機公告）
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
# GET with retry
# ------------------------------------------------------------
def safe_get(url: str, retries: int = 3, timeout: int = 20) -> str | None:
    for i in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            r.encoding = r.apparent_encoding
            return r.text
        except Exception as e:
            print(f"⚠️ 第 {i} 次失敗：{url} - {e}")
            time.sleep(1)
    print(f"❌ 最終失敗：{url}")
    return None


# ------------------------------------------------------------
# 列表頁
# ------------------------------------------------------------
def build_list_url(page: int) -> str:
    # page=1: /plaactlist, page>=2: /plaactlist/2
    return LIST_BASE if page == 1 else f"{LIST_BASE}/{page}"


def crawl_list_page(page: int) -> List[Dict]:
    """
    抓某一頁列表，只留你指定關鍵字的公告
    回傳 list[dict]: {"roc_date": "114.12.01", "url": "..."}
    """
    url = build_list_url(page)
    print(f"\n🔍 抓列表頁：{url}")

    html = safe_get(url)
    if html is None:
        print("⚠️ 列表頁抓取失敗，視為無資料")
        return []

    soup = BeautifulSoup(html, "html.parser")
    rows: List[Dict] = []

    for a in soup.find_all("a", href=True):
        title = a.get_text(strip=True)
        if not any(kw in title for kw in KEYWORDS):
            continue

        # 例如：114.12.01中共解放軍臺海周邊海、空域動態點閱次數：413 次
        m = re.search(r"\d{3}\.\d{2}\.\d{2}", title)
        if not m:
            continue
        roc_date = m.group(0)

        article_url = requests.compat.urljoin(BASE_URL, a["href"])
        rows.append({"roc_date": roc_date, "url": article_url})

    print(f"📌 本頁抓到 {len(rows)} 筆")
    return rows


# ------------------------------------------------------------
# 內頁：只抓 maincontent
# ------------------------------------------------------------
def extract_maincontent_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    main = soup.select_one("div.maincontent")
    if not main:
        return ""

    text = " ".join(main.stripped_strings)
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
def apply_manual_gap(df: pd.DataFrame) -> pd.DataFrame:
    """
    manual_gap.csv 結構：
    日期,內容
    109/01/01,....
    """
    if MANUAL_GAP.exists():
        print(f"📥 合併補丁：{MANUAL_GAP}")
        gap = pd.read_csv(MANUAL_GAP, encoding="utf-8-sig")
        df = pd.concat([df, gap], ignore_index=True)
    else:
        print("ℹ️ 找不到 manual_gap.csv，略過補丁合併")

    df = df.drop_duplicates(subset=["日期"], keep="last")
    df = df.sort_values("日期", key=lambda col: col.map(roc_sort_key))
    return df.reset_index(drop=True)


# ------------------------------------------------------------
# full：抓到「沒有新文章」就自動停
# ------------------------------------------------------------
def run_full():
    print("🚀 [FULL] 全量模式開始")

    all_rows: List[Dict] = []
    seen_urls: set[str] = set()
    page = 1
    consecutive_no_new = 0  # 連續幾頁「沒有新文章」

    while True:
        entries = crawl_list_page(page)
        if not entries:
            print("⚪ 此頁完全沒有符合關鍵字的文章 → 視為尾端，停止。")
            break

        # 只留下沒看過的 url
        new_entries = [e for e in entries if e["url"] not in seen_urls]

        if not new_entries:
            consecutive_no_new += 1
            print(f"⚪ 第 {page} 頁沒有新文章（連續 {consecutive_no_new} 頁）。")

            # 國防部在超過最後一頁時會重複回傳同一頁
            # → 連續兩頁都沒有新網址，就視為已經刷到最後一頁
            if consecutive_no_new >= 2:
                print("🔚 連續兩頁都沒有新網址，判定已到最後一頁，停止往後抓。")
                break
        else:
            consecutive_no_new = 0

        for e in new_entries:
            seen_urls.add(e["url"])
            content = crawl_article(e["url"])
            date_str = e["roc_date"].replace(".", "/")
            all_rows.append({"日期": date_str, "內容": content})
            time.sleep(0.3)

        print(f"📊 累積筆數：{len(all_rows)}")
        page += 1
        time.sleep(0.5)

        # 安全保險：防禦性上限，避免意外 infinite loop
        if page > 1000:
            print("⚠️ 頁數超過 1000，強制停止（應該不會發生）。")
            break

    df = pd.DataFrame(all_rows)
    df = apply_manual_gap(df)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print(f"✅ 全量完成，輸出 {OUTPUT_CSV}，共 {len(df)} 筆")


# ------------------------------------------------------------
# daily：每天只抓最新一筆
# ------------------------------------------------------------
def run_daily():
    print("📅 [DAILY] 每日模式開始（只抓最新一筆）")

    entries = crawl_list_page(1)
    if not entries:
        print("⚠️ 第 1 頁抓不到資料，今日略過。")
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

    print(f"✅ 每日更新完成，現在 {OUTPUT_CSV} 共 {len(df)} 筆")


# ------------------------------------------------------------
# main
# ------------------------------------------------------------
if __name__ == "__main__":
    # python mnd_crawler.py full  → 全量
    # python mnd_crawler.py       → 每日模式
    if len(sys.argv) > 1 and sys.argv[1] == "full":
        run_full()
    else:
        run_daily()
