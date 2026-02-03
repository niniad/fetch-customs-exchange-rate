#!/usr/bin/env python3
"""
税関の為替レートCSVファイルを2023年1月1日以降から全てダウンロードするスクリプト
URL形式: https://www.customs.go.jp/tetsuzuki/kawase/csv/YYMMDD-YYMMDD.csv
"""

import requests
from datetime import datetime, timedelta
import os
import time

# --- SETTINGS ---
BASE_URL = "https://www.customs.go.jp/tetsuzuki/kawase/csv"
OUTPUT_DIR = "/home/user/webapp/downloaded_csv"
START_DATE = datetime(2023, 1, 1)  # 2023年1月1日から
END_DATE = datetime.now()  # 現在まで

# User-Agentを設定（アクセス拒否対策）
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}


def get_sunday(date):
    """指定日付の週の日曜日を取得"""
    days_since_sunday = date.weekday() + 1
    if days_since_sunday == 7:  # 日曜日の場合
        days_since_sunday = 0
    return date - timedelta(days=days_since_sunday)


def get_saturday(date):
    """指定日付の週の土曜日を取得"""
    days_until_saturday = 5 - date.weekday()
    if days_until_saturday < 0:
        days_until_saturday += 7
    return date + timedelta(days=days_until_saturday)


def generate_weekly_ranges(start_date, end_date):
    """
    週ごとの日付範囲を生成（日曜日〜土曜日）
    税関の為替レートは週単位で公示される
    """
    ranges = []
    current = get_sunday(start_date)
    
    while current <= end_date:
        week_start = current
        week_end = current + timedelta(days=6)  # 土曜日
        
        # 開始日のフォーマット: YYMMDD
        start_str = week_start.strftime("%y%m%d")
        end_str = week_end.strftime("%y%m%d")
        
        ranges.append((start_str, end_str, week_start, week_end))
        current += timedelta(days=7)
    
    return ranges


def download_csv(start_str, end_str):
    """CSVファイルをダウンロード"""
    filename = f"{start_str}-{end_str}.csv"
    url = f"{BASE_URL}/{filename}"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        
        if response.status_code == 200:
            return response.content, filename
        elif response.status_code == 404:
            return None, filename
        else:
            print(f"  Unexpected status {response.status_code} for {url}")
            return None, filename
            
    except requests.RequestException as e:
        print(f"  Request error for {url}: {e}")
        return None, filename


def main():
    # 出力ディレクトリ作成
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print(f"=== 税関為替レートCSVダウンローダー ===")
    print(f"期間: {START_DATE.strftime('%Y-%m-%d')} から {END_DATE.strftime('%Y-%m-%d')}")
    print(f"保存先: {OUTPUT_DIR}")
    print()
    
    # 週ごとの範囲を生成
    weekly_ranges = generate_weekly_ranges(START_DATE, END_DATE)
    print(f"対象週数: {len(weekly_ranges)}週")
    print()
    
    downloaded_count = 0
    skipped_count = 0
    failed_count = 0
    downloaded_files = []
    
    for i, (start_str, end_str, week_start, week_end) in enumerate(weekly_ranges, 1):
        filename = f"{start_str}-{end_str}.csv"
        filepath = os.path.join(OUTPUT_DIR, filename)
        
        # 既にダウンロード済みの場合はスキップ
        if os.path.exists(filepath):
            print(f"[{i}/{len(weekly_ranges)}] {filename} - Already exists, skipping")
            skipped_count += 1
            downloaded_files.append(filepath)
            continue
        
        print(f"[{i}/{len(weekly_ranges)}] Downloading {filename}...", end=" ")
        
        content, _ = download_csv(start_str, end_str)
        
        if content:
            with open(filepath, "wb") as f:
                f.write(content)
            print(f"OK ({len(content)} bytes)")
            downloaded_count += 1
            downloaded_files.append(filepath)
        else:
            print("Not found (404)")
            failed_count += 1
        
        # サーバー負荷軽減のため少し待機
        time.sleep(0.3)
    
    print()
    print("=== ダウンロード完了 ===")
    print(f"成功: {downloaded_count} ファイル")
    print(f"スキップ（既存）: {skipped_count} ファイル")
    print(f"失敗（404等）: {failed_count} ファイル")
    print(f"合計取得可能: {downloaded_count + skipped_count} ファイル")
    print()
    
    # ダウンロードしたファイル一覧を保存
    list_file = os.path.join(OUTPUT_DIR, "downloaded_files.txt")
    with open(list_file, "w") as f:
        for filepath in sorted(downloaded_files):
            f.write(os.path.basename(filepath) + "\n")
    print(f"ファイルリスト保存: {list_file}")
    
    return downloaded_files


if __name__ == "__main__":
    main()
