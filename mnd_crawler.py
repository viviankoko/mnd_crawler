import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path
import re
import pandas as pd

BASE_URL = "https://www.mnd.gov.tw"
LIST_BASE = f"{BASE_URL}/news/plaactlist"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X)"
}

BASE_DIR = Path(__file__).parent

# 路徑（都放在 repo 根目錄）
MANUAL_CSV = BASE_DIR / "manual_gap.csv"          # 你手動補的缺口資料
LATEST_CSV = BASE_DIR / "pla_daily_latest.csv"    # 這次爬到的所有資料
FINAL_CSV = BASE_DIR / "pla_daily_clean_full.csv" # 合併後最終檔案


def build_list_url(page: int) -> str:
    """page=1: /plaactlist, page>=2: /plaactlist/2"""
    return LIST_BASE if page == 1 else f"{LIST_BASE}/{page}"


def crawl_list_page(page: int):
    """
    抓某一頁列表，只留「中共解放軍臺海周邊海、空域動態」
    回傳 list[dict]: {roc_date, url}
    roc_date 例如 '114.12.01'
    """
    url = build_list_url(page)
    print(f"\n🔍 抓列表頁：{url}")

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        # 🔥 關鍵：列表頁 503 或其他錯誤時，不要讓整個程式掛掉
        print(f"⚠️ 抓取列表頁失敗：第 {page} 頁 {url} - {e}")
        # 回傳空 list，讓 crawl_all_pages() 把這一頁視為「沒有資料」並停止往後抓
        return []

    soup = BeautifulSoup(r.text, "html.parser")

    rows = []

    for a in soup.find_all("a"):
        text = a.get_text(strip=True)
        if "中共解放軍臺海周邊海、空域動態" not in text:
            continue

        # 例如：114.12.01中共解放軍臺海周邊海、空域動態點閱次數：413 次
        m = re.search(r"\d{3}\.\d{2}\.\d{2}", text)
        if not m:
            continue
        roc_date = m.group(0)

        href = a.get("href")
        if not href:
            continue
        article_url = urljoin(BASE_URL, href)

        rows.append(
            {
                "roc_date": roc_date,
                "url": article_url,
            }
        )

    print(f"📌 本頁抓到 {len(rows)} 筆")
    return rows


def extract_maincontent_text(html: str) -> str:
    """
    只取 <div class="maincontent"> 裡面的文字，
    串成一行：
    「中共解放軍臺海周邊海、空域動態 一、日期：… 二、活動動態：…」
    """
    soup = BeautifulSoup(html, "html.parser")

    main_div = soup.select_one("div.maincontent")
    if main_div is None:
        return ""

    parts = list(main_div.stripped_strings)
    text = " ".join(parts)
    return text


def crawl_article_text(url: str) -> str:
    print(f"➡️ 抓文章頁：{url}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
    except Exception as e:
        print(f"⚠️ 抓取文章失敗：{url} - {e}")
        return ""
    return extract_maincontent_text(r.text)


def roc_to_sort_key(s: str):
    """
    把 '114/12/03' 轉成排序用 tuple (114, 12, 3)
    """
    try:
        y, m, d = s.split("/")
        return int(y), int(m), int(d)
    except Exception:
        return (0, 0, 0)


def crawl_all_pages(max_pages: int = 200) -> pd.DataFrame:
    """
    從第 1 頁一路抓到沒資料或達到 max_pages。
    回傳欄位：日期, 內容
    """
    data_rows = []

    for page in range(1, max_pages + 1):
        entries = crawl_list_page(page)
        if not entries:
            print(f"⚪ 第 {page} 頁沒有資料，停止往後抓。")
            break

        for entry in entries:
            text = crawl_article_text(entry["url"])
            date_str = entry["roc_date"].replace(".", "/")  # 114.12.03 -> 114/12/03
            data_rows.append(
                {
                    "日期": date_str,
                    "內容": text,
                }
            )

    df = pd.DataFrame(data_rows)
    return df


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
        print("⚠️ 找不到 manual_gap.csv，只使用本次爬到的資料。")
        df_manual = pd.DataFrame(columns=["日期", "內容"])

    # 個別去重
    df_manual = df_manual.drop_duplicates(subset=["日期"], keep="first")
    df_new = df_new.drop_duplicates(subset=["日期"], keep="first")

    # 合併：手動補齊在前，新爬資料在後
    df_all = pd.concat([df_manual, df_new], ignore_index=True)

    # 以「日期」去重，保留第一次出現（優先 manual）
    df_all = df_all.drop_duplicates(subset=["日期"], keep="first")

    # 依日期排序（民國年 / 月 / 日）
    df_all = df_all.sort_values(by="日期", key=lambda col: col.map(roc_to_sort_key))

    return df_all


def main():
    print("🚀 開始爬取國防部區域動態…")

    df_new = crawl_all_pages()
    print(f"\n✅ 本次共爬到 {len(df_new)} 筆資料")

    if len(df_new) > 0:
        # 本次爬到的原始資料
        df_new.to_csv(LATEST_CSV, index=False, header=False, encoding="utf-8-sig")
        print(f"📝 已寫入最新爬取資料：{LATEST_CSV}")
    else:
        print("⚠️ 本次沒有爬到任何資料。仍會嘗試用 manual_gap.csv 產出最終檔。")

    # 合併手動補齊
    df_final = merge_with_manual(df_new)

    # 最終輸出：不寫欄位名稱
    df_final.to_csv(FINAL_CSV, index=False, header=False, encoding="utf-8-sig")
    print(f"🏁 已寫入合併後最終檔案：{FINAL_CSV}")
    print(f"📊 最終資料筆數：{len(df_final)}")


if __name__ == "__main__":
    main()
