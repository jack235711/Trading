"""
Dukascopyから指定した日付のbi5ファイルをダウンロードするスクリプト
"""
import requests
from pathlib import Path
from datetime import datetime, timedelta


def download_bi5_files(pair: str, date: str):
    """
    指定した通貨ペアと日付のbi5ファイルをダウンロード
    
    Args:
        pair: 通貨ペア（例: "EURUSD"）
        date: 日付（例: "2020-01-01"）
    """
    # 日付をパース
    dt = datetime.strptime(date, "%Y-%m-%d")
    year = dt.year
    month = dt.month - 1  # Dukascopyは0-indexed
    day = dt.day
    
    # 保存先ディレクトリを作成
    save_dir = Path(f"../data/{pair}/{year}/{month:02d}/{day:02d}")
    save_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"ダウンロード開始: {pair} / {date}")
    
    # 0時〜23時の24ファイルをダウンロード
    success_count = 0
    for hour in range(24):
        url = f"https://datafeed.dukascopy.com/datafeed/{pair}/{year}/{month:02d}/{day:02d}/{hour:02d}h_ticks.bi5"
        filename = save_dir / f"{hour:02d}h_ticks.bi5"
        
        # 既存ファイルがある場合はスキップ
        if filename.exists():
            print(f"  {hour:02d}h: スキップ（既存）")
            success_count += 1
            continue
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # ファイルに保存
            with open(filename, 'wb') as f:
                f.write(response.content)
            
            if len(response.content) > 0:
                print(f"  {hour:02d}h: ダウンロード完了 ({len(response.content)} bytes)")
                success_count += 1
            else:
                print(f"  {hour:02d}h: データなし")
        
        except requests.exceptions.RequestException as e:
            print(f"  {hour:02d}h: エラー - {e}")
    
    print(f"ダウンロード完了: {save_dir} ({success_count}/24 ファイル)")
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
    # 2025年1年間のEURUSDデータをダウンロード
    download_date_range("EURUSD", "2025-01-01", "2025-12-31")
