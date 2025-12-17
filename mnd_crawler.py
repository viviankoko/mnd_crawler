import sys
import time
import re
from pathlib import Path
from typing import List, Dict

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime



BASE_URL = "https://www.mnd.gov.tw"
LIST_BASE = f"{BASE_URL}/news/plaactlist"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X)"}

BASE_DIR = Path(__file__).parent
OUTPUT_CSV = BASE_DIR / "mnd_pla.csv"
MANUAL_GAP = BASE_DIR / "manual_gap.csv"

# 國防部所有公告版本的關鍵字
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

#日期處理
def normalize_date_to_iso(date_str: str) -> str:

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



def safe_get(url: str, retries: int = 3, timeout: int = 20) -> str | None:
   
    for i in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()

            raw = r.content 

            
            enc_candidates: list[str] = []

            if r.encoding:
                enc_candidates.append(r.encoding)
            if r.apparent_encoding and r.apparent_encoding not in enc_candidates:
                enc_candidates.append(r.apparent_encoding)

            
            for e in ("utf-8", "big5", "cp950"):
                if e not in enc_candidates:
                    enc_candidates.append(e)

            text = None
            for enc in enc_candidates:
                try:
                    text = raw.decode(enc)
             
                    break
                except UnicodeDecodeError:
                    continue

            if text is None:
    
                text = raw.decode("utf-8", errors="replace")
     

            return text

        except Exception as e:
            print(f"第 {i} 次失敗：{url} - {e}")
            time.sleep(1)

    print(f"最終失敗：{url}")
    return None


#列表頁

def build_list_url(page: int) -> str:
    # page=1: /plaactlist, page>=2: /plaactlist/2
    return LIST_BASE if page == 1 else f"{LIST_BASE}/{page}"


def crawl_list_page(page: int) -> List[Dict]:
    """
    抓某一頁列表，只留指定關鍵字的公告
    """
    url = build_list_url(page)
    print(f"\n抓列表頁：{url}")

    html = safe_get(url)
    if html is None:
        print("列表頁抓取失敗，視為無資料")
        return []

    soup = BeautifulSoup(html, "html.parser")
    rows: List[Dict] = []

    for a in soup.find_all("a", href=True):
        title = a.get_text(strip=True)
        if not title:
            continue

      
        if not any(kw in title for kw in KEYWORDS):
            continue

        m = re.search(r"\d{3}\.\d{2}\.\d{2}", title)
        if not m:
            continue
        roc_date = m.group(0)

        article_url = requests.compat.urljoin(BASE_URL, a["href"])
        rows.append({"roc_date": roc_date, "url": article_url, "title": title})

    print(f"本頁抓到 {len(rows)} 筆")
    return rows



def clean_content(text: str) -> str:
    """把公告內容裡的換行、全形空白等整理成單行字串。"""
    if not isinstance(text, str):
        return ""

    
    text = text.replace("\r\n", "\n").replace("\r", "\n")

   
    text = text.replace("\n\u3000", "")

    
    text = text.replace("\n", " ")

    
    text = re.sub(r"\s+", " ", text)

    
    text = text.replace("\u3000", "")
    
    return text.strip()

def extract_maincontent_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    main = soup.select_one("div.maincontent")
    if not main:
        return ""

    text = " ".join(main.stripped_strings)

    #統一清掉不必要的換行／空白
    text = clean_content(text)

    return text

def crawl_article(url: str) -> str:
    print(f"抓文章頁：{url}")
    html = safe_get(url)
    if html is None:
        return ""
    return extract_maincontent_text(html)



# 合併manual_gap.csv（以西元日期為 key 覆蓋）
# ------------------------------------------------------------
def load_manual_gap() -> pd.DataFrame | None:
    
    if not MANUAL_GAP.exists():
        print("ℹ️ 找不到 manual_gap.csv，略過補丁讀取")
        return None

    print(f"讀取補丁：{MANUAL_GAP}")
    gap = pd.read_csv(MANUAL_GAP, encoding="utf-8-sig")

    if "日期" not in gap.columns or "內容" not in gap.columns:
        raise KeyError("manual_gap.csv 至少要有日期、內容兩個欄位。")

    # 統一日期格式
    gap = normalize_date_column(gap, "日期")

  
    if "標題" not in gap.columns:
        gap["標題"] = "中共解放軍臺海周邊海、空域動態"
    if "來源網址" not in gap.columns:
        gap["來源網址"] = ""

    #欄位順序整理
    gap = gap[["日期", "標題", "內容", "來源網址"]]

    print(f"補丁筆數：{len(gap)}")
    return gap


def apply_manual_gap(base_df: pd.DataFrame) -> pd.DataFrame:
    
    gap_df = load_manual_gap()
    if gap_df is None or gap_df.empty:
        print("沒有補丁或補丁為空，略過補丁合併")
       
        base_df = base_df.copy()
        for col in ["日期", "標題", "內容", "來源網址"]:
            if col not in base_df.columns:
                base_df[col] = ""
        base_df = base_df[["日期", "標題", "內容", "來源網址"]]
        #排序
        base_df = base_df.sort_values("日期").reset_index(drop=True)
        return base_df

    base_df = base_df.copy()

    
    for col in ["日期", "標題", "內容", "來源網址"]:
        if col not in base_df.columns:
            base_df[col] = ""

    base_df = base_df[["日期", "標題", "內容", "來源網址"]]
    gap_df = gap_df[["日期", "標題", "內容", "來源網址"]]

 
    gap_dates = gap_df["日期"].unique().tolist()
    before_len = len(base_df)
    base_df = base_df[~base_df["日期"].isin(gap_dates)].reset_index(drop=True)
    after_len = len(base_df)
    print(f"套用補丁：刪除原本同日期資料 {before_len - after_len} 筆")

    merged_df = pd.concat([base_df, gap_df], ignore_index=True)

    #依日期排序
    merged_df = merged_df.sort_values("日期").reset_index(drop=True)
    print(f"套用補丁後總筆數：{len(merged_df)}")
    return merged_df


# ------------------------------------------------------------
# full：抓到「沒有新文章」就自動停
# ------------------------------------------------------------
def run_full():
    print("[FULL] 全量模式開始")

    all_rows: List[Dict] = []
    seen_urls: set[str] = set()
    page = 1
    consecutive_no_new = 0  

    while True:
        entries = crawl_list_page(page)
        if not entries:
            print("此頁完全沒有符合關鍵字的文章 視為尾端，停止。")
            break

     
        new_entries = [e for e in entries if e["url"] not in seen_urls]

        if not new_entries:
            consecutive_no_new += 1
            print(f"第 {page} 頁沒有新文章（連續 {consecutive_no_new} 頁）。")

            #國防部在超過最後一頁時會重複回傳同一頁

            if consecutive_no_new >= 2:
                print("🔚 連續兩頁都沒有新網址，判定已到最後一頁，停止往後抓。")
                break
        else:
            consecutive_no_new = 0

        for e in new_entries:
            seen_urls.add(e["url"])
            content = crawl_article(e["url"])

          
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

        print(f"累積筆數：{len(all_rows)}")
        page += 1
        time.sleep(0.5)

   
        if page > 1000:
            print("頁數超過 1000，強制停止（應該不會發生）。")
            break

    df = pd.DataFrame(all_rows)

    if df.empty:
        print("全量爬完結果為空，請檢查列表 selector 或網站結構。")
        # 建一個空的標準欄位 CSV，至少不會直接炸掉
        df = pd.DataFrame(columns=["日期", "公告內容"])
        df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
        print(f"輸出空 CSV：{OUTPUT_CSV}")
        return

   
    df = apply_manual_gap(df)

    
    output_df = df[["日期", "內容"]].rename(columns={"內容": "公告內容"})

    # 日期由近到遠排序
    output_df = output_df.sort_values("日期", ascending=False).reset_index(drop=True)

    output_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"全量完成，輸出 {OUTPUT_CSV}，共 {len(output_df)} 筆")

# ------------------------------------------------------------
# daily：每天只抓最新一筆
# ------------------------------------------------------------
def run_daily():
    print("[DAILY] 每日模式開始（只抓最新一筆）")

    entries = crawl_list_page(1)
    if not entries:
        print("第 1 頁抓不到資料，今日略過。")
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

  
        if "內容" not in df_old.columns and "公告內容" in df_old.columns:
            df_old["內容"] = df_old["公告內容"]

        if "標題" not in df_old.columns:
            df_old["標題"] = ""
        if "來源網址" not in df_old.columns:
            df_old["來源網址"] = ""
        if df_old["日期"].astype(str).str.contains(r"/").any():
         
            try:
                df_old = normalize_date_column(df_old, "日期")
            except Exception as e:
                print(f"舊檔日期轉換失敗：{e}")

        df = pd.concat([df_old, df_new], ignore_index=True)

    else:
        df = df_new


    df = apply_manual_gap(df)

  
    output_df = df[["日期", "內容"]].rename(columns={"內容": "公告內容"})


    output_df = output_df.sort_values("日期", ascending=False).reset_index(drop=True)

    output_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print(f"每日更新完成，現在 {OUTPUT_CSV} 共 {len(output_df)} 筆")

# ------------------------------------------------------------
# main
# ------------------------------------------------------------
if __name__ == "__main__":
    # python mnd_crawler.py full  全量
    # python mnd_crawler.py       每日模式
    if len(sys.argv) > 1 and sys.argv[1] == "full":
        run_full()
    else:
        run_daily()
