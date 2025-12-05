#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
mnd_crawler.py

功能：
- python mnd_crawler.py full
    → 全量重建（從第 1 頁爬到無資料為止）＋合併 manual_gap.csv
- python mnd_crawler.py
    → 每日更新（抓第 1 頁新日期）＋合併 manual_gap.csv
"""

import sys
import time
import re
from pathlib import Path
from typing import List, Dict, Tuple

import requests
from bs4 import BeautifulSoup
import pandas as pd


# ---------------- 基本設定 ----------------
BASE_URL = "https://www.mnd.gov.tw"
LIST_BASE = f"{BASE_URL}/news/plaactlist"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X)"
}

BASE_DIR = Path(__file__).parent
OUTPUT_CSV = BASE_DIR / "mnd_pla.csv"
MANUAL_GAP = BASE_DIR / "manual_gap.csv"

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


# ---------------- 工具 ----------------

def safe_get(url: str, timeout: int = 20) -> str | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        r.encoding = "utf-8"
        return r.text
    except Exception as e:
        print(f"⚠️ 抓取失敗：{url} - {e}")
        return None


def build_list_url(page: int) -> str:
    return LIST_BASE if page == 1 else f"{LIST_BASE}/{page}"


# ---------------- 列表頁 ----------------

def crawl_list_page(page: int) -> List[Dict]:
    url = build_list_url(page)
    print(f"\n🔍 抓列表頁：{url}")

    html = safe_get(url)
    if html is None:
        return []

    soup = BeautifulSoup(html, "html.parser")
    rows = []

    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        if not any(kw in text for kw in KEYWORDS):
            continue

        m = re.search(r"\d{3}\.\d{2}\.\d{2}", text)
        if not m:
            parent = a.parent.get_text(strip=True)
            m = re.search(r"\d{3}\.\d{2}\.\d{2}", parent)
            if not m:
                continue

        roc_date = m.group(0)
        url2 = requests.compat.urljoin(BASE_URL, a["href"])
        rows.append({"roc_date": roc_date, "url": url2})

    print(f"📌 第 {page} 頁抓到 {len(rows)} 筆")
    return rows


# ---------------- 內文 ----------------

def extract_maincontent_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    main = soup.select_one("div.maincontent")
    if not main:
        return ""
    return " ".join(main.stripped_strings)


def crawl_article_text(url: str) -> str:
    print(f"➡️ 抓文章頁：{url}")
    html = safe_get(url)
    return extract_maincontent_text(html) if html else ""


# ---------------- 日期排序 ----------------

def date_sort_key(s: str) -> Tuple[int, int, int]:
    try:
        y, m, d = s.split("/")
        y = int(y)
        if len(y.__str__()) == 3:
            y += 1911
        return (y, int(m), int(d))
    except:
        return (0, 0, 0)


# ---------------- 補丁併入（補丁優先） ----------------

def merge_with_manual(df_core: pd.DataFrame) -> pd.DataFrame:
    df = df_core.copy()
    df["日期"] = df["日期"].astype(str).str.strip()

    if MANUAL_GAP.exists():
        gap = pd.read_csv(MANUAL_GAP, encoding="utf-8-sig")
        gap["日期"] = gap["日期"].astype(str).str.strip()
        df = pd.concat([df, gap[["日期", "內容"]]], ignore_index=True)
        print(f"📥 補丁筆數：{len(gap)}")
    else:
        print("ℹ️ manual_gap.csv 不存在（略過補丁）")

    df = df.drop_duplicates(subset=["日期"], keep="last")
    df = df.sort_values("日期", key=lambda col: col.map(date_sort_key))
    df = df.reset_index(drop=True)
    return df


# ---------------- FULL：無上限往下爬 ----------------

def run_full():
    print("🚀 [FULL] 全量開始（直到無資料頁）")

    rows = []
    page = 1

    while True:
        entries = crawl_list_page(page)
        if not entries:
            print(f"⚪ 第 {page} 頁已無資料，停止 full")
            break

        for e in entries:
            date_str = e["roc_date"].replace(".", "/")
            content = crawl_article_text(e["url"])
            rows.append({"日期": date_str, "內容": content})
            time.sleep(0.15)

        page += 1
        time.sleep(0.2)

    df_core = pd.DataFrame(rows)
    print(f"📌 FULL 爬到 {len(df_core)} 筆")

    df_final = merge_with_manual(df_core)
    df_final.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print(f"🏁 FULL 完成 → {OUTPUT_CSV}（{len(df_final)} 筆）")


# ---------------- DAILY：抓第 1 頁新資料 ----------------

def run_daily():
    print("📅 [DAILY] 每日更新")

    if not OUTPUT_CSV.exists():
        print("⚠️ 主檔不存在 → 改跑 FULL")
        run_full()
        return

    df_old = pd.read_csv(OUTPUT_CSV, encoding="utf-8-sig")
    df_old["日期"] = df_old["日期"].astype(str).str.strip()
    old_dates = set(df_old["日期"])

    entries = crawl_list_page(1)
    if not entries:
        print("⚠️ 第 1 頁無資料")
        return

    new_rows = []
    for e in entries:
        date_str = e["roc_date"].replace(".", "/")
        if date_str in old_dates:
            continue

        content = crawl_article_text(e["url"])
        new_rows.append({"日期": date_str, "內容": content})
        time.sleep(0.15)

    if not new_rows:
        print("✅ 無新日期")
        return

    df_core = pd.concat([df_old, pd.DataFrame(new_rows)], ignore_index=True)
    df_final = merge_with_manual(df_core)
    df_final.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print(f"🏁 DAILY 完成（{len(df_final)} 筆）")


# ---------------- main ----------------

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].lower() == "full":
        run_full()
    else:
        run_daily()
