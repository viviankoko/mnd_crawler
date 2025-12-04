#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
mnd_crawler.py — 最終穩定版（依照你的指定翻頁格式）
---------------------------------------------------
列表頁 URL 格式完全依你要求：

p=1: https://www.mnd.gov.tw/news/plaactlist/
p=2: https://www.mnd.gov.tw/news/plaactlist/2
p=3: https://www.mnd.gov.tw/news/plaactlist/3
...

同時修正 href 解析與內頁完整 URL 組法，避免出現 www.mnd.gov.twnews。
"""

import os
import time
import re
from typing import List, Dict
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import pandas as pd

BASE = "https://www.mnd.gov.tw"
LIST_URL = BASE + "/news/plaactlist/"

DATA_PATH = "mnd_pla.csv"
GAP_PATH = "manual_gap.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

# -----------------------------------------------------
# 抓取工具
# -----------------------------------------------------

def fetch(url: str, session=None) -> str:
    s = session or requests.Session()
    r = s.get(url, headers=HEADERS, timeout=20)
    r.encoding = "utf-8"
    return r.text


def parse_list_page(html: str) -> List[Dict]:
    """解析列表頁：抓出 date / title / url"""
    soup = BeautifulSoup(html, "lxml")
    records: List[Dict] = []

    # 抓所有含有 plaact/ 的連結（可能是 news/plaact/... 或 /news/plaact/...）
    for a in soup.select("a[href*='plaact/']"):
        href = a.get("href") or ""

        # ★★★ 正確 URL 組法 — 永遠不會再變成 www.mnd.gov.twnews ★★★
        url = urljoin(BASE + "/", href)

        # 從父層找日期
        row = a.find_parent("tr") or a.find_parent("div")
        date_str = ""
        if row:
            m = re.search(r"\d{3}[./]\d{2}[./]\d{2}", row.get_text())
            if m:
                date_str = m.group(0).replace(".", "/")

        records.append({
            "date": date_str,
            "title": a.get_text(strip=True),
            "url": url,
        })

    return records


def parse_article(html: str) -> Dict[str, str]:
    """解析內頁：抓 content 及內頁日期"""
    soup = BeautifulSoup(html, "lxml")

    main = soup.select_one(".maincontent")
    content_text = main.get_text("\n", strip=True) if main else ""

    date_str = ""
    pageinfo = soup.select_one(".pageinfo")
    if pageinfo:
        spans = pageinfo.select("span")
        if len(spans) >= 2:
            raw = spans[1].get_text(strip=True)
            m = re.search(r"\d{3}[./]\d{2}[./]\d{2}", raw)
            if m:
                date_str = m.group(0).replace(".", "/")

    return {"date": date_str, "content": content_text}


# -----------------------------------------------------
# 爬多頁
# -----------------------------------------------------

def crawl_pages(max_page: int) -> pd.DataFrame:
    all_rows: List[Dict] = []
    session = requests.Session()

    for page in range(1, max_page + 1):

        # ★★★ 照你指定的翻頁格式 ★★★
        if page == 1:
            list_url = LIST_URL      # 必須結尾有 "/"
        else:
            list_url = LIST_URL.rstrip("/") + f"/{page}"

        print(f"🔍 抓列表頁：{list_url}")

        try:
            html = fetch(list_url, session=session)
        except Exception as e:
            print(f"⚠️ 列表頁抓取失敗 {list_url}: {e}")
            continue

        base_records = parse_list_page(html)
        if not base_records:
            print(f"頁 {page} 沒抓到任何 plaact 連結，停止。")
            break

        print(f"頁 {page} 抓到 {len(base_records)} 筆")

        # 抓每則內頁
        for rec in base_records:
            art_url = rec["url"]
            try:
                art_html = fetch(art_url, session=session)
            except Exception as e:
                print(f"⚠️ 內頁抓取失敗 {art_url}: {e}")
                continue

            art = parse_article(art_html)

            all_rows.append({
                "date": art["date"] or rec["date"],
                "title": rec["title"],
                "url": rec["url"],
                "content": art["content"],
            })

            time.sleep(0.3)

    df = pd.DataFrame(all_rows)

    if not df.empty:
        df = df.drop_duplicates(subset=["url"], keep="last").reset_index(drop=True)

    return df


# -----------------------------------------------------
# manual_gap
# -----------------------------------------------------

def load_manual_gap() -> pd.DataFrame:
    if not os.path.exists(GAP_PATH):
        print("🔍 manual_gap.csv 不存在，略過。")
        return pd.DataFrame()

    df = pd.read_csv(GAP_PATH, encoding="utf-8-sig")
    print(f"📥 讀取補丁，共 {len(df)} 筆")
    return df


def merge_with_gap(main_df, gap_df):
    if gap_df.empty:
        return main_df.reset_index(drop=True)

    merged = pd.concat([main_df, gap_df], ignore_index=True)
    merged = merged.drop_duplicates(subset=["url"], keep="last")
    merged = merged.sort_values("date").reset_index(drop=True)
    return merged


# -----------------------------------------------------
# 全量
# -----------------------------------------------------

def build_full_dataset(max_page: int = 200):
    print("🚀 全量重建開始")
    df = crawl_pages(max_page=max_page)
    print(f"🌐 共抓到 {len(df)} 筆")

    gap = load_manual_gap()
    final = merge_with_gap(df, gap)

    final.to_csv(DATA_PATH, index=False, encoding="utf-8-sig")
    print(f"✅ 已輸出 {len(final)} 筆至 {DATA_PATH}")


# -----------------------------------------------------
# daily
# -----------------------------------------------------

def load_existing_data():
    if not os.path.exists(DATA_PATH):
        print("⚠️ 找不到主檔，改跑全量。")
        build_full_dataset()
        return pd.read_csv(DATA_PATH, encoding="utf-8-sig")

    df = pd.read_csv(DATA_PATH, encoding="utf-8-sig")
    print(f"📥 主檔 {len(df)} 筆")
    return df


def daily_update(max_page: int = 3):
    existing = load_existing_data()
    known = set(existing["url"].tolist())

    print("🌐 抓取最近幾頁找新資料")
    recent = crawl_pages(max_page=max_page)

    is_new = ~recent["url"].isin(known)
    new_rows = recent[is_new]
    print(f"🆕 新增 {len(new_rows)} 筆")

    updated = pd.concat([existing, new_rows], ignore_index=True)
    gap = load_manual_gap()
    final = merge_with_gap(updated, gap)

    final.to_csv(DATA_PATH, index=False, encoding="utf-8-sig")
    print(f"✅ 寫入完畢，目前 {len(final)} 筆")


# -----------------------------------------------------
# main
# -----------------------------------------------------

def main():
    mode = os.getenv("MND_MODE", "").lower()

    if mode == "full":
        build_full_dataset()
    else:
        daily_update()


if __name__ == "__main__":
    main()
