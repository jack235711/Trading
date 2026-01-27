
import os
import requests
import concurrent.futures
from pathlib import Path
from datetime import datetime, timedelta
from tqdm import tqdm
import time

# --- Settings ---
BASE_DIR = Path("../data")
MAX_WORKERS = 30 # 高速化のためのスレッド数
START_YEAR = 2020 # 探索開始年
END_YEAR = 2025 # 探索終了年
URL_TEMPLATE = "https://datafeed.dukascopy.com/datafeed/{pair}/{year}/{month:02d}/{day:02d}/{hour:02d}h_ticks.bi5"

def get_existing_pairs():
    """dataディレクトリにある既存の通貨ペアを取得"""
    if not BASE_DIR.exists():
        return []
    return [d.name for d in BASE_DIR.iterdir() if d.is_dir()]

def download_file(args):
    pair, year, month, day, hour = args
    # DukascopyのURLは月が0-indexed (0=1月)
    url = URL_TEMPLATE.format(pair=pair, year=year, month=month, day=day, hour=hour)
    
    # 保存パス: data/PAIR/YEAR/MONTH/DAY/HHh_ticks.bi5
    # 注意: 保存ディレクトリ名もURLに合わせて0-indexedに統一
    month_str = f"{month:02d}"
    day_str = f"{day:02d}"
    save_dir = BASE_DIR / pair / str(year) / month_str / day_str
    file_path = save_dir / f"{hour:02d}h_ticks.bi5"

    # 既に存在し、かつサイズが0でない場合はスキップ
    if file_path.exists() and file_path.stat().st_size > 0:
        return 'skipped'

    try:
        # ネットワークエラー等に備えてリトライ
        for attempt in range(3):
            try:
                response = requests.get(url, timeout=15)
                if response.status_code == 200:
                    if len(response.content) > 0:
                        save_dir.mkdir(parents=True, exist_ok=True)
                        with open(file_path, "wb") as f:
                            f.write(response.content)
                        return 'downloaded'
                    else:
                        return 'empty_data'
                elif response.status_code == 404:
                    return 'not_found'
                elif response.status_code == 503:
                    time.sleep(1) # Rate limit or overload
                    continue
                else:
                    return f'error_{response.status_code}'
            except requests.exceptions.RequestException:
                time.sleep(1)
                continue
        return 'failed'
    except Exception as e:
        return f'exception_{str(e)}'

def main():
    pairs = get_existing_pairs()
    if not pairs:
        print("No pairs found in data directory.")
        return

    print(f"Target Pairs: {', '.join(pairs)}")
    print(f"Range: {START_YEAR} - {END_YEAR}")
    
    all_tasks = []
    
    # タスクの生成 (高速探索)
    print("Generating task list...")
    for pair in pairs:
        current_date = datetime(START_YEAR, 1, 1)
        # 本日の日付まで、または設定した終了年末まで
        end_limit = min(datetime.now(), datetime(END_YEAR, 12, 31))
        
        while current_date <= end_limit:
            # 土日はデータがないのでスキップ (Dukascopy仕様)
            # weekday() 5=土曜, 6=日曜
            if current_date.weekday() < 5:
                year = current_date.year
                month = current_date.month - 1 # 0-indexed
                day = current_date.day
                
                # 1時間の各ファイルをタスクに追加
                for hour in range(24):
                    all_tasks.append((pair, year, month, day, hour))
            
            current_date += timedelta(days=1)

    total_tasks = len(all_tasks)
    print(f"Total tasks: {total_tasks}")
    
    results = {
        'downloaded': 0,
        'skipped': 0,
        'not_found': 0,
        'empty_data': 0,
        'failed': 0
    }

    # ThreadPoolExecutorによる高速実行
    print(f"Starting execution with {MAX_WORKERS} workers...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # プログレスバーを表示しながら実行
        futures = {executor.submit(download_file, task): task for task in all_tasks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_tasks, desc="Repairing data", unit="file"):
            res = future.result()
            if res in results:
                results[res] += 1
            elif res.startswith('error_') or res.startswith('exception_'):
                results['failed'] += 1
            else:
                # Other unexpected results
                results['failed'] += 1

    print("\n--- Repair Results ---")
    print(f"Downloaded: {results['downloaded']}")
    print(f"Skipped (Already exists): {results['skipped']}")
    print(f"Not Found (Expected for early dates/holidays): {results['not_found']}")
    print(f"Empty Data: {results['empty_data']}")
    print(f"Failed (Errors/Exceptions): {results['failed']}")

if __name__ == "__main__":
    main()
