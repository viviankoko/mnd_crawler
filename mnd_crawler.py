#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
mnd_crawler.py（西元日期＋補丁覆蓋版）

功能：
- full：把目前網站上所有「區域動態」的共機/海域公告抓下來 → mnd_pla.csv
- daily：每天只抓最新一筆，append 到 mnd_pla.csv
- manual_gap.csv：補不了的日期用這個補，最後會一起 merge 進 mnd_pla.csv

調整重點：
1. 列表頁仍用 https://www.mnd.gov.tw/news/plaactlist (+ /2, /3, ...)
2. 內頁仍只抓 <div class="maincontent">
3. 把民國日期（例如 114.02.03）轉成西元 YYYY-MM-DD
4. 多存兩欄：標題、來源網址
5. manual_gap.csv 至少要有：日期, 內容
   - 日期可寫：2025/2/3, 2025-02-03, 114/2/3, 114年2月3日……
   - 會統一轉成西元 YYYY-MM-DD
   - 若缺標題／來源網址會補預設值
6. 補丁規則：
   - 以日期為 key
   - 先刪掉原始資料中同日期的列，再把補丁加進來（補丁覆蓋）
"""

import sys
import time
import re
from pathlib import Path
from typing import List, Dict

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime


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
# 日期工具：全部轉成西元 YYYY-MM-DD
# ------------------------------------------------------------
def normalize_date_to_iso(date_str: str) -> str:
    """
    把輸入字串轉成西元 YYYY-MM-DD

    支援範例：
    - 2025/2/3
    - 2025/02/03
    - 2025-02-03
    - 114/2/3（民國年 → 自動 +1911）
    - 114年2月3日
    - 114.02.03（會先被呼叫者轉成斜線）
    """
    if not isinstance(date_str, str):
        raise ValueError(f"日期不是字串: {date_str!r}")

    s = date_str.strip()
    if not s:
        raise ValueError("日期是空字串")

    # 如果本來就是 YYYY-MM-DD，試著 parse 一下，成功就直接回傳
    try:
        dt = datetime.strptime(s, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass

    # 統一把中文年月日、點、減號都換成斜線
    s_clean = re.sub(r"[年月日.\-]", "/", s)
    s_clean = re.sub(r"/+", "/", s_clean).strip("/")

    parts = s_clean.split("/")
    if len(parts) != 3:
        raise ValueError(f"無法解析日期格式: {date_str!r}（清洗後: {s_clean!r}）")

    y, m, d = parts
    y = y.strip()
    m = m.strip()
    d = d.strip()

    year = int(y)
    # 粗略：小於 1911 視為民國年
    if year < 1911:
        year = year + 1911

    month = int(m)
    day = int(d)

    dt = datetime(year, month, day)
    return dt.strftime("%Y-%m-%d")


def normalize_date_column(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    把 df[col] 全部轉成 YYYY-MM-DD（字串），回傳新 DataFrame
    """
    if col not in df.columns:
        raise KeyError(f"DataFrame 不含欄位 {col!r}")
    df = df.copy()
    df[col] = df[col].astype(str).apply(normalize_date_to_iso)
    return df


# ------------------------------------------------------------
# GET with retry
# ------------------------------------------------------------
def safe_get(url: str, retries: int = 3, timeout: int = 20) -> str | None:
    """
    發 GET，回傳正確解碼後的文字（優先嘗試 utf-8 / big5 / cp950），
    避免少數舊頁面（例如 109.09.17）出現亂碼。
    """
    for i in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()

            raw = r.content  # 先拿 bytes

            # 優先放進可能的編碼
            enc_candidates: list[str] = []

            if r.encoding:
                enc_candidates.append(r.encoding)
            if r.apparent_encoding and r.apparent_encoding not in enc_candidates:
                enc_candidates.append(r.apparent_encoding)

            # 再補常見的
            for e in ("utf-8", "big5", "cp950"):
                if e not in enc_candidates:
                    enc_candidates.append(e)

            text = None
            for enc in enc_candidates:
                try:
                    text = raw.decode(enc)
                    # print(f"[DEBUG] {url} 使用編碼：{enc}")
                    break
                except UnicodeDecodeError:
                    continue

            if text is None:
                # 全部失敗就用 utf-8 + replace 撐住
                text = raw.decode("utf-8", errors="replace")
                # print(f"[DEBUG] {url} 使用編碼：utf-8 (replace)")

            return text

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
    回傳 list[dict]:
        {
            "roc_date": "114.12.01",
            "url": "https://www.mnd.gov.tw/......",
            "title": "114.12.01中共解放軍臺海周邊海、空域動態......"
        }
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
        if not title:
            continue

        # 關鍵字過濾
        if not any(kw in title for kw in KEYWORDS):
            continue

        # 例如：114.12.01中共解放軍臺海周邊海、空域動態點閱次數：413 次
        m = re.search(r"\d{3}\.\d{2}\.\d{2}", title)
        if not m:
            continue
        roc_date = m.group(0)

        article_url = requests.compat.urljoin(BASE_URL, a["href"])
        rows.append({"roc_date": roc_date, "url": article_url, "title": title})

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
# 合併補丁 manual_gap.csv（以西元日期為 key 覆蓋）
# ------------------------------------------------------------
def load_manual_gap() -> pd.DataFrame | None:
    """
    manual_gap.csv 結構（最少）：
    日期,內容
    2025/02/03,國防部今日表示，……
    
    可選欄位：
    - 標題
    - 來源網址

    日期可以是：
    - 2025/2/3, 2025-02-03
    - 114/2/3, 114年2月3日, 114.02.03（會轉成西元）

    回傳的 DataFrame：
    - 日期 已轉為 YYYY-MM-DD
    - 至少含有：日期, 標題, 內容, 來源網址
    """
    if not MANUAL_GAP.exists():
        print("ℹ️ 找不到 manual_gap.csv，略過補丁讀取")
        return None

    print(f"📥 讀取補丁：{MANUAL_GAP}")
    gap = pd.read_csv(MANUAL_GAP, encoding="utf-8-sig")

    if "日期" not in gap.columns or "內容" not in gap.columns:
        raise KeyError("manual_gap.csv 至少要有『日期』『內容』兩個欄位。")

    # 統一日期格式
    gap = normalize_date_column(gap, "日期")

    # 補缺欄位
    if "標題" not in gap.columns:
        gap["標題"] = "中共解放軍臺海周邊海、空域動態"
    if "來源網址" not in gap.columns:
        gap["來源網址"] = ""

    # 欄位順序整理
    gap = gap[["日期", "標題", "內容", "來源網址"]]

    print(f"📥 補丁筆數：{len(gap)}")
    return gap


def apply_manual_gap(base_df: pd.DataFrame) -> pd.DataFrame:
    """
    以「日期」（YYYY-MM-DD）為 key 套用補丁：

    規則：
    1. 若 manual_gap.csv 不存在 → 直接回傳 base_df
    2. 若存在：
       - 讀出 gap_df，日期轉成 YYYY-MM-DD
       - 刪除 base_df 中所有「日期在 gap_df 裡」的列
       - 再把 gap_df 加進來
       - 依日期排序後回傳
    """
    gap_df = load_manual_gap()
    if gap_df is None or gap_df.empty:
        print("ℹ️ 沒有補丁或補丁為空，略過補丁合併")
        # 仍然確保欄位齊全
        base_df = base_df.copy()
        for col in ["日期", "標題", "內容", "來源網址"]:
            if col not in base_df.columns:
                base_df[col] = ""
        base_df = base_df[["日期", "標題", "內容", "來源網址"]]
        # 日期已在外面處理成 YYYY-MM-DD，這裡只做排序
        base_df = base_df.sort_values("日期").reset_index(drop=True)
        return base_df

    base_df = base_df.copy()

    # 確保欄位一致
    for col in ["日期", "標題", "內容", "來源網址"]:
        if col not in base_df.columns:
            base_df[col] = ""

    base_df = base_df[["日期", "標題", "內容", "來源網址"]]
    gap_df = gap_df[["日期", "標題", "內容", "來源網址"]]

    # 以日期為 key 覆蓋：先刪再加
    gap_dates = gap_df["日期"].unique().tolist()
    before_len = len(base_df)
    base_df = base_df[~base_df["日期"].isin(gap_dates)].reset_index(drop=True)
    after_len = len(base_df)
    print(f"🩹 套用補丁：刪除原本同日期資料 {before_len - after_len} 筆")

    merged_df = pd.concat([base_df, gap_df], ignore_index=True)

    # 依日期排序（YYYY-MM-DD 字串可以直接正確排序）
    merged_df = merged_df.sort_values("日期").reset_index(drop=True)
    print(f"🩹 套用補丁後總筆數：{len(merged_df)}")
    return merged_df


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

            # 轉日期：114.12.01 → 114/12/01 → 2025-12-01
            roc_date_slash = e["roc_date"].replace(".", "/")
            iso_date = normalize_date_to_iso(roc_date_slash)

            all_rows.append(
                {
                    "日期": iso_date,
                    "標題": e["title"],
                    "內容": content,
                    "來源網址": e["url"],
                }
            )
            time.sleep(0.3)

        print(f"📊 累積筆數：{len(all_rows)}")
        page += 1
        time.sleep(0.5)

        # 安全保險：防禦性上限，避免意外 infinite loop
        if page > 1000:
            print("⚠️ 頁數超過 1000，強制停止（應該不會發生）。")
            break

    df = pd.DataFrame(all_rows)

    if df.empty:
        print("⚠️ 全量爬完結果為空，請檢查列表 selector 或網站結構。")
        # 建一個空的標準欄位 CSV，至少不會直接炸掉
        df = pd.DataFrame(columns=["日期", "公告內容"])
        df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
        print(f"⚠️ 輸出空 CSV：{OUTPUT_CSV}")
        return

    # 日期已經是 ISO，這裡主要是套補丁
    df = apply_manual_gap(df)

    # 只保留「日期」「內容」，並把「內容」改名成「公告內容」
    output_df = df[["日期", "內容"]].rename(columns={"內容": "公告內容"})

    # 日期由近到遠排序（新到舊）
    output_df = output_df.sort_values("日期", ascending=False).reset_index(drop=True)

    output_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ 全量完成，輸出 {OUTPUT_CSV}，共 {len(output_df)} 筆")

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

    roc_date_slash = newest["roc_date"].replace(".", "/")
    iso_date = normalize_date_to_iso(roc_date_slash)
    content = crawl_article(newest["url"])

    df_new = pd.DataFrame(
        [
            {
                "日期": iso_date,
                "標題": newest["title"],
                "內容": content,
                "來源網址": newest["url"],
            }
        ]
    )

    if OUTPUT_CSV.exists():
        df_old = pd.read_csv(OUTPUT_CSV, encoding="utf-8-sig")

        # 如果舊檔是「日期, 公告內容」的格式，先映射回「內容」
        if "內容" not in df_old.columns and "公告內容" in df_old.columns:
            df_old["內容"] = df_old["公告內容"]

        # 舊檔如果還是舊格式（只有日期＋內容、民國日期），這裡會稍微「幫你升級」：
        if "標題" not in df_old.columns:
            df_old["標題"] = ""
        if "來源網址" not in df_old.columns:
            df_old["來源網址"] = ""
        if df_old["日期"].astype(str).str.contains(r"/").any():
            # 粗略判斷舊檔可能是民國格式（例如 114/02/03）
            try:
                df_old = normalize_date_column(df_old, "日期")
            except Exception as e:
                print(f"⚠️ 舊檔日期轉換失敗：{e}")

        df = pd.concat([df_old, df_new], ignore_index=True)

    else:
        df = df_new

    # 不管有沒有舊檔，都要在這裡套一次補丁
    df = apply_manual_gap(df)

    # 只保留「日期」「內容」，並把「內容」改名成「公告內容」
    output_df = df[["日期", "內容"]].rename(columns={"內容": "公告內容"})

    # 日期由近到遠排序（新到舊）
    output_df = output_df.sort_values("日期", ascending=False).reset_index(drop=True)

    output_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print(f"✅ 每日更新完成，現在 {OUTPUT_CSV} 共 {len(output_df)} 筆")

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
