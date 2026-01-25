import requests
import time
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor


def download_single_hour(pair: str, year: int, month: int, day: int, hour: int, save_dir: Path):
    """
    1時間分のbi5ファイルをダウンロードする補助関数
    """
    url = f"https://datafeed.dukascopy.com/datafeed/{pair}/{year}/{month:02d}/{day:02d}/{hour:02d}h_ticks.bi5"
    filename = save_dir / f"{hour:02d}h_ticks.bi5"
    
    # 既存ファイルがある場合はスキップ
    if filename.exists() and filename.stat().st_size > 0:
        return True, "skip"
    
    max_retries = 5
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            
            # 404 Not Found はデータがないのでリトライしない
            if response.status_code == 404:
                return False, "no data (404)"

            # 5xxエラーなどはリトライ
            if response.status_code >= 500:
                response.raise_for_status() # 例外を発生させてcatchブロックへ

            response.raise_for_status()
            
            # ファイルに保存
            content = response.content
            if len(content) > 0:
                with open(filename, 'wb') as f:
                    f.write(content)
                return True, f"complete ({len(content)} bytes)"
            else:
                # 200 OK だが空データの場合もリトライ対象にするか、あるいはデータなしとみなすか
                # Dukascopyの場合、休日は空ファイルが返ることもあるが、サイズ0なら意味ないのでリトライせずno data扱いとする
                return False, "no data (0 bytes)"
        
        except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
            # 404以外は原則リトライ
            last_error = e
            if attempt < max_retries - 1:
                # 指数バックオフ + ジッター (固定だと競合しやすいため)
                sleep_time = (2 ** attempt) + (0.1 * attempt) 
                time.sleep(sleep_time)
                continue
            
    return False, f"error - max retries exceeded: {last_error}"


def download_bi5_files(pair: str, date: str, max_workers: int = 2):
    """
    指定した通貨ペアと日付のbi5ファイルを並列ダウンロード
    
    Args:
        pair: 通貨ペア（例: "EURUSD"）
        date: 日付（例: "2020-01-01"）
        max_workers: 並列数 (デフォルト2: 複数ペア同時実行時の負荷軽減のため)
    """
    # 日付をパース
    dt = datetime.strptime(date, "%Y-%m-%d")
    year = dt.year
    month = dt.month - 1  # Dukascopyは0-indexed
    day = dt.day
    
    # 保存先ディレクトリを作成
    save_dir = Path(f"../data/{pair}/{year}/{month:02d}/{day:02d}")
    save_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"ダウンロード開始 (並列): {pair} / {date}")
    
    success_count = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(download_single_hour, pair, year, month, day, hour, save_dir): hour 
            for hour in range(24)
        }
        
        for future in futures:
            hour = futures[future]
            success, msg = future.result()
            if success:
                success_count += 1
            print(f"  {hour:02d}h: {msg}")
    
    print(f"ダウンロード完了: {pair}/{date} ({success_count}/24 ファイル)")
    return success_count


def download_date_range(pair: str, start_date: str, end_date: str):
    """
    指定した日付範囲のbi5ファイルをダウンロード
    
    Args:
        pair: 通貨ペア（例: "EURUSD"）
        start_date: 開始日（例: "2025-01-01"）
        end_date: 終了日（例: "2025-12-31"）
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    current_dt = start_dt
    total_days = 0
    total_files = 0
    
    print(f"=" * 60)
    print(f"日付範囲ダウンロード: {pair}")
    print(f"期間: {start_date} 〜 {end_date}")
    print(f"=" * 60)
    
    while current_dt <= end_dt:
        date_str = current_dt.strftime("%Y-%m-%d")
        files = download_bi5_files(pair, date_str)
        total_days += 1
        total_files += files
        current_dt += timedelta(days=1)
    
    print(f"\n" + "=" * 60)
    print(f"全ダウンロード完了！")
    print(f"総日数: {total_days} 日")
    print(f"総ファイル数: {total_files} ファイル")
    print(f"=" * 60)


if __name__ == "__main__":
    # 2025年1年間の主要通貨ペアデータを並列にダウンロード開始
    pairs = ["EURUSD", "USDJPY", "GBPUSD", "AUDUSD"]
    
    with ThreadPoolExecutor(max_workers=len(pairs)) as executor:
        for pair in pairs:
            executor.submit(download_date_range, pair, "2025-01-01", "2025-12-31")
