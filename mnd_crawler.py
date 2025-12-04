#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
mnd_crawler.py

用途：
1. 第一次跑（全量）：從國防部「區域動態」把目前所有資料爬完，
   然後把 manual_gap.csv 併進去 → 輸出 mnd_pla.csv。
2. 之後每天跑（增量）：只抓最近幾頁，找出「還沒寫進 mnd_pla.csv」的新資料，
   append 進去，再併一次 manual_gap.csv → 覆蓋回 mnd_pla.csv。

使用方式：
- 第一次全量重建：
    在終端機執行：
      MND_MODE=full python mnd_crawler.py
    （Windows 可以用：set MND_MODE=full && python mnd_crawler.py）

- 之後每日排程：
    直接：
      python mnd_crawler.py
    （或在 GitHub Actions 裡不設定 MND_MODE）
"""

import os
import time
import re
from typing import List, Dict

import requests
from bs4 import BeautifulSoup
import pandas as pd

BASE = "https://www.mnd.gov.tw"
LIST_URL = BASE + "/news/plaactlist"

DATA_PATH = "mnd_pla.csv"     # 主資料表
GAP_PATH  = "manual_gap.csv"  # 你的補丁檔

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}


# ---------- 抓取＆解析 ----------

def fetch(url: str, session: requests.Session | None = None) -> str:
    """抓網頁，強制用 UTF-8 解碼。"""
    s = session or requests.Session()
    r = s.get(url, headers=HEADERS, timeout=20)
    r.encoding = "utf-8"
    return r.text


def parse_list_page(html: str) -> List[Dict]:
    """
    解析列表頁，回傳每一筆的 date / title / url。

    重點：
    - selector 用 a[href*="plaact/"]，因為列表裡 href 多半是相對路徑
      例如 "news/plaact/85454" 或 "/news/plaact/85454"。
    """
    soup = BeautifulSoup(html, "lxml")
    records: List[Dict] = []

    for a in soup.select("a[href*='plaact/']"):
        href = a.get("href") or ""
        if not href:
            continue

        # 組成完整網址（支援相對＋絕對）
        if href.startswith("http"):
            url = href
        else:
            url = BASE + href.lstrip("/")

        # 往上找父層，從文字裡抓日期（109.09.17 / 109/09/17）
        row = a.find_parent("tr") or a.find_parent("div")
        date_str = ""
        if row:
            m = re.search(r"\d{3}[./]\d{2}[./]\d{2}", row.get_text())
            if m:
                date_str = m.group(0).replace(".", "/")

        title = a.get_text(strip=True)
        records.append(
            {
                "date": date_str,  # 發布日（列表上的）
                "title": title,
                "url": url,
            }
        )

    return records


def parse_article(html: str) -> Dict[str, str]:
    """
    解析內頁，抓 maincontent 的完整文字，以及 pageinfo 裡的日期。

    回傳：
      {"date": "109/09/17", "content": "...全文..."}
    """
    soup = BeautifulSoup(html, "lxml")

    main = soup.select_one(".maincontent")
    if main:
        content_text = main.get_text("\n", strip=True)
    else:
        content_text = ""

    date_str = ""
    pageinfo = soup.select_one(".pageinfo")
    if pageinfo:
        spans = pageinfo.select("span")
        if len(spans) >= 2:
            raw = spans[1].get_text(strip=True)  # 例如 "109.09.17"
            m = re.search(r"\d{3}[./]\d{2}[./]\d{2}", raw)
            if m:
                date_str = m.group(0).replace(".", "/")

    return {"date": date_str, "content": content_text}


def crawl_pages(max_page: int) -> pd.DataFrame:
    """
    從第 1 頁爬到 max_page。
    - 第 1 頁： https://www.mnd.gov.tw/news/plaactlist
    - 後續頁： https://www.mnd.gov.tw/news/plaactlist/2, /3, ...
    - 每頁解析列表，抓出每則的 url，再去爬內頁。
    - 以 url 去重，避免重複。
    - 若某頁完全抓不到任何 plaact 連結，就當作到尾端直接 break。
    """
    all_rows: List[Dict] = []
    session = requests.Session()

    for page in range(1, max_page + 1):
        if page == 1:
            list_url = LIST_URL
        else:
            list_url = f"{LIST_URL}/{page}"

        print(f"🔍 抓列表頁：{list_url}")
        try:
            html = fetch(list_url, session=session)
        except Exception as e:
            print(f"⚠️ 列表頁抓取失敗 {list_url}: {e}")
            continue

        base_records = parse_list_page(html)
        if not base_records:
            print(f"頁 {page} 沒抓到任何 plaact 連結，視為到尾端，停止。")
            break

        print(f"頁 {page} 抓到 {len(base_records)} 筆")

        for rec in base_records:
            art_url = rec["url"]
            try:
                art_html = fetch(art_url, session=session)
            except Exception as e:
                print(f"  ⚠️ 內頁抓取失敗 {art_url}: {e}")
                continue

            art = parse_article(art_html)
            row = {
                "date": art["date"] or rec["date"],  # 內頁日期優先，沒有就用列表的
                "title": rec["title"],
                "url": rec["url"],
                "content": art["content"],
            }
            all_rows.append(row)

            # 禮貌性 sleep，別把官方網站打爆
            time.sleep(0.3)

    df = pd.DataFrame(all_rows)

    if not df.empty and "url" in df.columns:
        df = df.drop_duplicates(subset=["url"], keep="last").reset_index(drop=True)

    return df


# ---------- manual_gap 合併 ----------

def load_manual_gap() -> pd.DataFrame:
    """讀進 manual_gap.csv，如果沒有就回傳空 DataFrame。"""
    if not os.path.exists(GAP_PATH):
        print("🔍 manual_gap.csv 不存在，略過補丁。")
        return pd.DataFrame()

    print(f"📥 讀取補丁檔：{GAP_PATH}")
    gap_df = pd.read_csv(GAP_PATH, encoding="utf-8-sig")
    print(f"  → {len(gap_df)} 筆補丁資料")
    return gap_df


def merge_with_gap(main_df: pd.DataFrame, gap_df: pd.DataFrame) -> pd.DataFrame:
    """
    把主資料表與 manual_gap 合併。

    規則：
    - 以 url 當唯一 key。
    - manual_gap 放在後面：如果同一個 url 主檔和補丁都有，以補丁版本為準。
    """
    if gap_df.empty:
        return main_df.reset_index(drop=True)

    merged = pd.concat([main_df, gap_df], ignore_index=True)

    if "url" in merged.columns:
        merged = merged.drop_duplicates(subset=["url"], keep="last")
    elif set(["date", "title"]).issubset(merged.columns):
        merged = merged.drop_duplicates(subset=["date", "title"], keep="last")
    else:
        merged = merged.drop_duplicates(keep="last")

    if "date" in merged.columns:
        merged = merged.sort_values("date").reset_index(drop=True)

    return merged


# ---------- 模式 A：全量重建 ----------

def build_full_dataset(max_page: int = 200):
    """
    從第 1 頁一路爬到 max_page（遇到空頁就提前停），
    把目前所有區域動態都抓下來，再與 manual_gap 合併，輸出 mnd_pla.csv。

    ✔ 這一步完全不讀舊 CSV → 可把 109/09/17 之前留下的亂碼整個洗掉。
    """
    print("🚀 開始全量重建（從網站抓到現在所有資料）")
    df = crawl_pages(max_page=max_page)
    print(f"🌐 從網站共抓到 {len(df)} 筆")

    gap_df = load_manual_gap()
    final = merge_with_gap(df, gap_df)

    final.to_csv(DATA_PATH, index=False, encoding="utf-8-sig")
    print(f"✅ 全量重建完成，已輸出 {len(final)} 筆到 {DATA_PATH}")


# ---------- 模式 B：每日增量更新 ----------

def load_existing_data() -> pd.DataFrame:
    """讀入既有的 mnd_pla.csv；若找不到就自動跑一次全量重建。"""
    if not os.path.exists(DATA_PATH):
        print("⚠️ 找不到既有主檔，先跑全量重建。")
        build_full_dataset()
        return pd.read_csv(DATA_PATH, encoding="utf-8-sig")

    print(f"📥 讀取既有主檔：{DATA_PATH}")
    df = pd.read_csv(DATA_PATH, encoding="utf-8-sig")
    print(f"  → {len(df)} 筆")
    return df


def daily_update(max_page: int = 3):
    """
    每日更新：
    1. 讀既有主檔 mnd_pla.csv
    2. 去抓最近幾頁（預設 3 頁）的資料
    3. 只挑出「url 不在主檔」的那些 → 視為新資料
    4. append 進主檔，再與 manual_gap 合併，輸出回 mnd_pla.csv
    """
    existing = load_existing_data()
    known_urls = set(existing.get("url", []))

    print("🌐 抓取最近幾頁（預設 3 頁）找新資料…")
    recent_df = crawl_pages(max_page=max_page)
    if recent_df.empty:
        print("⚠️ 最近頁面沒有抓到任何資料，結束。")
        return

    is_new = ~recent_df["url"].isin(known_urls)
    new_rows = recent_df[is_new]
    print(f"🆕 找到 {len(new_rows)} 筆「主檔裡沒有的」新資料")

    if new_rows.empty:
        print("✅ 沒有新資料，主檔維持不變。")
        return

    updated = pd.concat([existing, new_rows], ignore_index=True)
    gap_df = load_manual_gap()
    final = merge_with_gap(updated, gap_df)

    final.to_csv(DATA_PATH, index=False, encoding="utf-8-sig")
    print(f"✅ 已寫入新資料，現在共有 {len(final)} 筆到 {DATA_PATH}")


# ---------- 入口點 ----------

def main():
    mode = os.getenv("MND_MODE", "").lower()
    if mode == "full":
        # 一次性全量：第一次建檔，或哪天你想重建都可以再跑
        build_full_dataset()
    else:
        # 例行：每天排程跑這個
        daily_update()


if __name__ == "__main__":
    main()
