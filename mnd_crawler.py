#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
import pandas as pd
import time
import sys
from pathlib import Path

BASE_URL = "https://www.mnd.gov.tw"
LIST_BASE = f"{BASE_URL}/news/plaactlist"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X)"}

BASE_DIR = Path(__file__).parent
OUTPUT_CSV = BASE_DIR / "mnd_pla.csv"
MANUAL_GAP = BASE_DIR / "manual_gap.csv"


# ------------------------------------------------------------
# GET with retry
# ------------------------------------------------------------
def safe_get(url: str, retries=3):
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            r.encoding = r.apparent_encoding
            return r.text
        except Exception as e:
            print(f"⚠️ 第 {i+1} 次失敗：{url} - {e}")
            time.sleep(1)
    print(f"❌ 最終失敗：{url}")
    return None


# ------------------------------------------------------------
# 列表頁 URL（保留你的原版規則）
# ------------------------------------------------------------
def build_list_url(page: int):
    return LIST_BASE if page == 1 else f"{LIST_BASE}/{page}"


# ------------------------------------------------------------
# 抓列表頁（沿用你原始條件，只抓特定標題）
# ------------------------------------------------------------
def crawl_list_page(page: int):
    url = build_list_url(page)
    print(f"\n🔍 抓列表頁：{url}")

    html = safe_get(url)
    if html is None:
        return []

    soup = BeautifulSoup(html, "html.parser")
    rows = []

    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)

        # ✔ 保留你指定的文章標題
        if "中共解放軍臺海周邊海、空域動態" not in text:
            continue

        # 抓日期：例如 114.12.03
        m = re.search(r"\d{3}\.\d{2}\.\d{2}", text)
        if not m:
            continue
        roc_date = m.group(0)

        article_url = urljoin(BASE_URL, a["href"])
        rows.append({"roc_date": roc_date, "url": article_url})

    print(f"📌 本頁抓到 {len(rows)} 筆")
    return rows


# ------------------------------------------------------------
# 文章內文：只抓 maincontent、清除亂碼
# ------------------------------------------------------------
def extract_maincontent_text(html: str):
    soup = BeautifulSoup(html, "html.parser")
    main = soup.select_one("div.maincontent")
    if not main:
        return ""

    text = " ".join(main.stripped_strings)

    # ✔ 偵測類似 109/09/17 那種俄文字亂碼（你說補丁是補別的區間也沒關係）
    if re.search(r"[а-яА-ЯёЁ]+", text):
        print("⚠️ 偵測到亂碼 → 交給補丁處理")
        return ""

    return text


def crawl_article(url: str):
    print(f"➡️ 抓文章頁：{url}")
    html = safe_get(url)
    if html is None:
        return ""
    return extract_maincontent_text(html)


# ------------------------------------------------------------
# 民國日期排序
# ------------------------------------------------------------
def roc_sort_key(s: str):
    try:
        y, m, d = s.split("/")
        return int(y), int(m), int(d)
    except:
        return (0, 0, 0)


# ------------------------------------------------------------
# 合併補丁 manual_gap.csv
# ------------------------------------------------------------
def apply_manual_gap(df: pd.DataFrame):
    if MANUAL_GAP.exists():
        print(f"📥 合併補丁：{MANUAL_GAP}")
        # 假設 manual_gap.csv 沒有欄位名稱、兩欄：日期,內容
        gap = pd.read_csv(
            MANUAL_GAP,
            encoding="utf-8-sig",
            header=None,
            names=["日期", "內容"],
        )
        df = pd.concat([df, gap], ignore_index=True)

    # 以「日期」去重，補丁在後面 → 補丁優先
    if "日期" in df.columns:
        df = df.drop_duplicates(subset=["日期"], keep="last")
        df = df.sort_values("日期", key=lambda col: col.map(roc_sort_key))

    return df


# ------------------------------------------------------------
# 全量模式：一次爬所有頁面
# ------------------------------------------------------------
def run_full():
    print("🚀 [FULL] 全量模式開始")
    all_rows = []

    for page in range(1, 300):
        entries = crawl_list_page(page)
        if not entries:
            break

        for e in entries:
            content = crawl_article(e["url"])
            date_str = e["roc_date"].replace(".", "/")
            all_rows.append({"日期": date_str, "內容": content})
            time.sleep(0.3)

    df = pd.DataFrame(all_rows)
    df = apply_manual_gap(df)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print(f"✅ 全量完成，共 {len(df)} 筆")


# ------------------------------------------------------------
# 每日模式：只抓最新一筆（第 1 頁第一筆）
# ------------------------------------------------------------
def run_daily():
    print("📅 [DAILY] 每日模式開始（只抓最新一筆）")

    entries = crawl_list_page(1)
    if not entries:
        print("⚠️ 無資料")
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

    print(f"✅ 已更新，共 {len[df]} 筆")


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
if __name__ == "__main__":
    # python mnd_crawler.py full  → 全量
    # python mnd_crawler.py       → 每日只抓最新
    if len(sys.argv) > 1 and sys.argv[1] == "full":
        run_full()
    else:
        run_daily()
