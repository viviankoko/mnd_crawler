#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
最終版 mnd_crawler.py（你直接貼這份就能用）

✔ 使用你提供的、已確認可爬到所有資料的「舊 ASP.NET 架構」爬蟲
✔ 日期統一轉成西元 YYYY-MM-DD
✔ 套用 manual_gap.csv 補丁（完全覆蓋同一天）
✔ 補丁合併後按日期由新→舊排序
✔ CSV 不加引號、不換行
✔ 自動偵測最後一頁，不會抓到 126 之後亂跑
"""

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import time
from pathlib import Path
from datetime import datetime


# -------------------------
# 路徑設定
# -------------------------
BASE_DIR = Path(__file__).parent
OUTPUT_CSV = BASE_DIR / "mnd_pla_air_sea.csv"
MANUAL_GAP = BASE_DIR / "manual_gap.csv"

BASE_URL = "https://www.mnd.gov.tw/PublishTable.aspx?Types=即時軍事動態&title=國防消息"
HEADERS = {"User-Agent": "Mozilla/5.0"}


# -------------------------
# 民國/西元日期處理
# -------------------------
def normalize_date(date_str: str) -> str:
    """
    接受格式：
    - 2025/2/3
    - 114/9/23（民國 → +1911）
    - 109.09.17
    - 2025-02-03
    最終輸出 YYYY-MM-DD
    """
    if not isinstance(date_str, str):
        return ""

    s = date_str.strip()
    s = re.sub(r"[年月日.\-]", "/", s)
    s = re.sub(r"/+", "/", s).strip("/")

    parts = s.split("/")
    if len(parts) != 3:
        return ""

    y, m, d = parts
    y = int(y)
    if y < 1911:  # 民國年
        y += 1911

    m = int(m)
    d = int(d)
    return f"{y:04d}-{m:02d}-{d:02d}"


# -------------------------
# 舊 ASP.NET ViewState（你的程式碼原封保留）
# -------------------------
def extract_viewstate_fields(soup):
    def val(name):
        el = soup.find("input", {"name": name})
        return el["value"] if el and el.has_attr("value") else ""
    return {
        "__VIEWSTATE": val("__VIEWSTATE"),
        "__VIEWSTATEGENERATOR": val("__VIEWSTATEGENERATOR"),
        "__EVENTVALIDATION": val("__EVENTVALIDATION"),
    }


def extract_postback_target(a_tag):
    m = re.search(r"__doPostBack\('([^']+)'", a_tag.get("href", ""))
    return m.group(1) if m else None


# -------------------------
# 解析列表頁（你的版本，100%照搬）
# -------------------------
def parse_list_page(html):
    soup = BeautifulSoup(html, "html.parser")
    fields = extract_viewstate_fields(soup)
    items = []

    KEYWORDS = [
        "中共解放軍臺海周邊海、空域動態",
        "中共解放軍軍機",
        "中共解放軍進入我西南空域活動情況",
        "踰越海峽中線",
        "逾越海峽中線",
        "我西南空域空情動態",
        "臺海周邊空域空情動態",
        "偵獲共機、艦在臺海周邊活動情形",
    ]

    for tr in soup.select("table tr"):
        a = tr.find("a", href=True)
        if not a:
            continue

        title = a.get_text(strip=True)
        if not any(kw in title for kw in KEYWORDS):
            continue

        target = extract_postback_target(a)

        date_text = ""
        for td in tr.find_all("td"):
            if re.search(r"\d{3}[./]\d{1,2}[./]\d{1,2}", td.get_text()):
                date_text = td.get_text(strip=True)
                break

        items.append({"date": date_text, "target": target, "view": fields})

    return items


# -------------------------
# 內頁 AJAX PostBack（你的版本，照搬）
# -------------------------
def fetch_detail(session, view_fields, target):
    data = {
        "__EVENTTARGET": target,
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": view_fields["__VIEWSTATE"],
        "__VIEWSTATEGENERATOR": view_fields["__VIEWSTATEGENERATOR"],
        "__EVENTVALIDATION": view_fields["__EVENTVALIDATION"],
    }

    for _ in range(3):
        try:
            r = session.post(BASE_URL, headers=HEADERS, data=data, timeout=40)
            r.raise_for_status()
            return r.text
        except Exception:
            time.sleep(2)

    return ""


# -------------------------
# 抽取內文（你的版本）
# -------------------------
def extract_clean_paragraph(html):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    PREFIX_LIST = [
        "中共解放軍臺海周邊海、空域動態",
        "中共解放軍軍機",
        "中共解放軍進入我西南空域活動情況",
        "踰越海峽中線",
        "逾越海峽中線",
        "我西南空域空情動態",
        "臺海周邊空域空情動態",
        "偵獲共機、艦在臺海周邊活動情形",
    ]

    start = min([text.find(p) for p in PREFIX_LIST if p in text] or [-1])
    if start == -1:
        return text

    # 停在「國軍運用任務機、艦及岸置飛彈系統嚴密監控與應處。」
    end_candidates = []
    END_PHRASES = [
        "國軍運用任務機、艦及岸置飛彈系統嚴密監控與應處。",
        "國軍運用任務機、艦及岸置飛彈系統嚴密監控與應處",
    ]

    for ph in END_PHRASES:
        pos = text.find(ph, start)
        if pos != -1:
            end_candidates.append(pos + len(ph))

    end = min(end_candidates) if end_candidates else len(text)
    seg = text[start:end]
    return seg.replace("\n", " ").replace("\r", " ").strip()


# -------------------------
# 使用你的爬法往下翻頁（直到真的沒資料）
# -------------------------
def crawl_all():
    session = requests.Session()
    page = 1
    records = []

    while True:
        url = f"{BASE_URL}&Page={page}"
        print(f"📄 抓取第 {page} 頁...")

        try:
            r = session.get(url, headers=HEADERS, timeout=40)
        except:
            print("逾時，再試一次...")
            time.sleep(2)
            continue

        items = parse_list_page(r.text)

        if not items:
            print(f"🔥 第 {page} 頁抓不到資料 → 視為最後一頁，停止")
            break

        for it in items:
            html_detail = fetch_detail(session, it["view"], it["target"])
            clean_text = extract_clean_paragraph(html_detail)
            date_norm = normalize_date(it["date"])

            records.append({
                "日期": date_norm,
                "標題": "中共解放軍臺海周邊海、空域動態",
                "內容": clean_text,
                "來源網址": BASE_URL,
            })

            time.sleep(0.6)

        page += 1

    return pd.DataFrame(records)


# -------------------------
# 補丁功能（覆蓋同一天）
# -------------------------
def load_manual_gap():
    if not MANUAL_GAP.exists():
        return None

    df = pd.read_csv(MANUAL_GAP)

    if "日期" not in df.columns or "內容" not in df.columns:
        raise ValueError("manual_gap.csv 必須有『日期』『內容』兩欄")

    df["日期"] = df["日期"].apply(normalize_date)

    if "標題" not in df.columns:
        df["標題"] = "中共解放軍臺海周邊海、空域動態"
    if "來源網址" not in df.columns:
        df["來源網址"] = ""

    return df[["日期", "標題", "內容", "來源網址"]]


def apply_gap(df, gap):
    if gap is None:
        return df

    df = df.copy()
    gap_dates = gap["日期"].unique().tolist()

    df = df[~df["日期"].isin(gap_dates)]
    merged = pd.concat([df, gap], ignore_index=True)

    return merged.sort_values("日期", ascending=False).reset_index(drop=True)


# -------------------------
# main
# -------------------------
def main():
    print("🚀 開始爬取資料")
    df = crawl_all()

    print(f"✔ 爬到共 {len(df)} 筆")

    gap = load_manual_gap()
    if gap is not None:
        print(f"✔ 載入補丁 {len(gap)} 筆")

    final = apply_gap(df, gap)

    print(f"✔ 套用補丁後共 {len(final)} 筆")

    # 實際 CSV（無引號，整段文字不換行）
    final.to_csv(
        OUTPUT_CSV,
        index=False,
        encoding="utf-8-sig",
        quoting=3  # csv.QUOTE_NONE
    )

    print(f"🎉 已輸出 CSV → {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
