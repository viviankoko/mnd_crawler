# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import time

BASE_URL = "https://www.mnd.gov.tw/PublishTable.aspx?Types=即時軍事動態&title=國防消息"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# ---------------------------------------------------------
# 解析軍機/軍艦數量（可保留，不影響你目前只產出日期＋全文）
# ---------------------------------------------------------
def extract_metrics(text):
    m_air = re.search(r"(共|計)\s*(\d+)\s*架次", text)
    aircraft_total = int(m_air.group(2)) if m_air else None

    m_adiz = re.search(r"其中\s*(\d+)\s*架次.*?(ADIZ|空域|中線)", text)
    adiz_count = int(m_adiz.group(1)) if m_adiz else None

    m_ship = re.search(r"(共|計)\s*(\d+)\s*艦", text)
    ship_count = int(m_ship.group(2)) if m_ship else None

    return {
        "偵測到的共機總數": aircraft_total,
        "進入ADIZ或跨越中線": adiz_count,
        "共艦活動數量": ship_count,
    }


# ---------------------------------------------------------
# ASP.NET ViewState
# ---------------------------------------------------------
def parse_viewstate_fields(soup):
    def val(name):
        el = soup.find("input", {"name": name})
        return el["value"] if el and el.has_attr("value") else ""
    return {
        "__VIEWSTATE": val("__VIEWSTATE"),
        "__VIEWSTATEGENERATOR": val("__VIEWSTATEGENERATOR"),
        "__EVENTVALIDATION": val("__EVENTVALIDATION"),
    }


def extract_postback_target(a_tag):
    href = a_tag.get("href", "")
    m = re.search(r"__doPostBack\('([^']+)'", href)
    return m.group(1) if m else None


# ---------------------------------------------------------
# 列表頁：抓日期與 postback TARGET
# ---------------------------------------------------------
def parse_list_page(html):
    soup = BeautifulSoup(html, "html.parser")
    fields = parse_viewstate_fields(soup)
    items = []

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

    for tr in soup.select("table tr"):
        a = tr.find("a", href=True)
        if not a:
            continue
        title = a.get_text(strip=True)

        if not any(kw in title for kw in KEYWORDS):
            continue

        target = extract_postback_target(a)

        date_text = None
        for td in tr.find_all("td"):
            if re.search(r"\d{3}/\d{1,2}/\d{1,2}", td.get_text()):
                date_text = td.get_text(strip=True)
                break

        items.append({"date": date_text, "target": target, "view": fields})

    return items


# ---------------------------------------------------------
# 內頁請求（加 retry）
# ---------------------------------------------------------
def fetch_detail(session, view_fields, target, retries=2):
    data = {
        "__EVENTTARGET": target,
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": view_fields["__VIEWSTATE"],
        "__VIEWSTATEGENERATOR": view_fields["__VIEWSTATEGENERATOR"],
        "__EVENTVALIDATION": view_fields["__EVENTVALIDATION"],
    }

    for attempt in range(retries):
        try:
            r = session.post(BASE_URL, headers=HEADERS, data=data, timeout=40)
            r.raise_for_status()
            return r.text
        except requests.exceptions.ReadTimeout:
            print(f"內頁逾時（第 {attempt+1} 次），重試中…")
            time.sleep(2)

    print("內頁讀取失敗，略過此筆資料。")
    return ""


# ---------------------------------------------------------
# 萃取公告全文（不重複標題＋支援所有結尾）
# ---------------------------------------------------------
def extract_clean_paragraph(html):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    PREFIXES = [
        "中共解放軍臺海周邊海、空域動態",
        "中共解放軍軍機",
        "中共解放軍進入我西南空域活動情況",
        "踰越海峽中線及進入我西南空域活動情況",
        "逾越海峽中線及進入我西南空域活動情況",
        "我西南空域空情動態",
        "臺海周邊空域空情動態",
        "偵獲共機、艦在臺海周邊活動情形",
    ]

    # 找最早出現的 prefix
    start = -1
    used_prefix = None
    for p in PREFIXES:
        pos = text.find(p)
        if pos != -1 and (start == -1 or pos < start):
            start = pos
            used_prefix = p

    if start == -1:
        return None

    END_PHRASES = [
        "下載專區",
        "國軍運用任務機、艦及岸置飛彈系統嚴密監控與應處。",
        "國軍運用任務機、艦及岸置飛彈系統嚴密監控與應處",
    ]

    end_candidates = []
    for phrase in END_PHRASES:
        pos = text.find(phrase, start)
        if pos != -1:
            if "應處" in phrase:
                end_candidates.append(pos + len(phrase))
            else:
                end_candidates.append(pos)

    if end_candidates:
        end = min(end_candidates)
    else:
        end = len(text)

    segment = text[start:end]

    # 去掉標題重複
    if used_prefix:
        dup = used_prefix + " " + used_prefix
        if segment.startswith(dup):
            segment = used_prefix + segment[len(dup):]

    return segment.strip()


# ---------------------------------------------------------
# 爬全部資料（正式版本）
# ---------------------------------------------------------
def crawl_all():
    session = requests.Session()
    page = 1
    records = []

    while True:
        url = f"{BASE_URL}&Page={page}"
        print(f"\n抓取第 {page} 頁: {url}")

        # 列表頁 retry
        try:
            r = session.get(url, headers=HEADERS, timeout=40)
        except requests.exceptions.ReadTimeout:
            print(f"第 {page} 頁逾時，再試一次…")
            time.sleep(2)
            continue

        if r.status_code != 200:
            print("無法連線")
            break

        items = parse_list_page(r.text)
        if not items:
            print("已無更多資料。")
            break

        for it in items:
            print(f"➡ 抓取 {it['date']}")

            html_detail = fetch_detail(session, it["view"], it["target"])
            clean_text = extract_clean_paragraph(html_detail)

            records.append({
                "日期": it["date"],
                "通報內容": clean_text,
            })

            time.sleep(0.8)

        page += 1
        time.sleep(1.5)

    return pd.DataFrame(records)


# ---------------------------------------------------------
# Debug：只抓某一天
# ---------------------------------------------------------
def debug_one_day(DEBUG_DATE):
    session = requests.Session()
    page = 1

    while True:
        url = f"{BASE_URL}&Page={page}"
        print(f"查頁 {page} … {url}")

        try:
            r = session.get(url, headers=HEADERS, timeout=40)
        except requests.exceptions.ReadTimeout:
            print(f"第 {page} 頁逾時，再試一次…")
            time.sleep(2)
            continue

        items = parse_list_page(r.text)
        if not items:
            print("找不到這一天。")
            return

        for it in items:
            if it["date"] == DEBUG_DATE:
                print(f"\n🎯 找到日期：{DEBUG_DATE}")
                html_detail = fetch_detail(session, it["view"], it["target"])
                clean_text = extract_clean_paragraph(html_detail)

                print("\n=== HTML detail (前 1200 字) ===")
                print(html_detail[:1200])
                print("\n=== clean_text ===")
                print(clean_text)
                return

        page += 1


# ---------------------------------------------------------
# main
# ---------------------------------------------------------
if __name__ == "__main__":
    df = crawl_all()
    df.to_csv("pla_daily_clean_full.csv", index=False, encoding="utf-8-sig")
    print("\n全部完成！筆數 =", len(df))
