# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import time

BASE_URL = "https://www.mnd.gov.tw/PublishTable.aspx?Types=即時軍事動態&title=國防消息"
HEADERS = {"User-Agent": "Mozilla/5.0"}

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


def fetch_detail(session, view_fields, target):
    data = {
        "__EVENTTARGET": target,
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": view_fields["__VIEWSTATE"],
        "__VIEWSTATEGENERATOR": view_fields["__VIEWSTATEGENERATOR"],
        "__EVENTVALIDATION": view_fields["__EVENTVALIDATION"],
    }
    r = session.post(BASE_URL, headers=HEADERS, data=data, timeout=20)
    r.raise_for_status()
    return r.text


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

    # 🔹 1. 找起點：哪一個標題最早出現
    start = -1
    used_prefix = None
    for p in PREFIXES:
        idx = text.find(p)
        if idx != -1 and (start == -1 or idx < start):
            start = idx
            used_prefix = p

    if start == -1:
        # 這頁根本不是我們要的格式
        return None

    # 🔹 2. 找多種可能的「結尾」
    END_PHRASES = [
        "國軍運用任務機、艦及岸置飛彈系統嚴密監控與應處。",
        "國軍運用任務機、艦及岸置飛彈系統嚴密監控與應處",
        "下載專區",
    ]

    end_candidates = []

    for phrase in END_PHRASES:
        pos = text.find(phrase, start)
        if pos != -1:
            # 「嚴密監控與應處」要切在句子後面，「下載專區」就切在它前面即可
            if "嚴密監控與應處" in phrase:
                end_candidates.append(pos + len(phrase))
            else:
                end_candidates.append(pos)

    if end_candidates:
        end = min(end_candidates)  # 取最早出現的結尾
    else:
        # 萬一真的沒有任何結尾詞，就切到全文末尾，至少不會是 None
        end = len(text)

    segment = text[start:end]

    # 🔹 3. 去掉「標題標題」這種重複開頭
    if used_prefix is not None:
        double = used_prefix + " " + used_prefix
        if segment.startswith(double):
            segment = used_prefix + segment[len(double):]

    return segment.strip()

def crawl_all():
    session = requests.Session()
    page = 1
    records = []

    while True:
        url = f"{BASE_URL}&Page={page}"
        print(f"\n抓取第 {page} 頁: {url}")

        r = session.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            print("無法連線，停止。")
            break

        items = parse_list_page(r.text)
        if not items:
            print("沒有更多資料，結束。")
            break

        for i, it in enumerate(items, 1):
            print(f"({i}/{len(items)}) 抓取 {it['date']}")

            try:
                html_detail = fetch_detail(session, it["view"], it["target"])
                clean_text = extract_clean_paragraph(html_detail)
            except Exception as e:
                print("內頁錯誤:", e)
                clean_text = ""

            records.append({
                "日期": it["date"],
                "通報內容": clean_text,
            })

            time.sleep(0.8)

        page += 1
        time.sleep(1.5)

    return pd.DataFrame(records)


if __name__ == "__main__":
    df = crawl_all()
    df.to_csv("pla_daily_clean_full.csv", index=False, encoding="utf-8-sig")
    print("\n全部完成！共抓取", len(df), "筆資料。")
    print(df.head(5))
