"""
bi5ファイルをParquet形式に一括変換するスクリプト

使用方法:
    python convert_to_parquet.py --symbol EURUSD
    python convert_to_parquet.py --all  # 全ペアを変換
"""
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from bi5_reader import load_day_data
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import sys

# 設定
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = (SCRIPT_DIR / "../data").resolve()
PARQUET_DIR = (SCRIPT_DIR / "../parquet_data").resolve()


def get_all_pairs():
    """dataディレクトリ配下のサブディレクトリをスキャンして通貨ペアリストを取得"""
    if not DATA_DIR.exists():
        return []
    return [d.name for d in DATA_DIR.iterdir() if d.is_dir() and not d.name.startswith('.')]


def convert_day_to_parquet(args):
    """1日分のbi5データをParquetに変換"""
    pair, date_str = args
    
    try:
        # bi5から1分足OHLCを読み込み
        ohlc = load_day_data(pair, date_str)
        
        if ohlc.empty:
            return None, f"{pair}/{date_str}: No data"
        
        # Parquet保存先を作成
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        year = dt.year
        month = dt.month - 1  # Dukascopy format
        day = dt.day
        
        output_dir = PARQUET_DIR / pair / str(year) / f"{month:02d}"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / f"{day:02d}.parquet"
        
        # Parquetに保存（snappy圧縮）
        ohlc.to_parquet(
            output_file,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        
        return output_file, f"{pair}/{date_str}: {len(ohlc)} records"
        
    except FileNotFoundError:
        return None, f"{pair}/{date_str}: bi5 not found"
    except Exception as e:
        return None, f"{pair}/{date_str}: Error - {str(e)}"


def find_all_bi5_dates(pair):
    """指定ペアの全bi5ファイルの日付リストを取得"""
    pair_dir = DATA_DIR / pair
    if not pair_dir.exists():
        return []
    
    dates = []
    for year_dir in sorted(pair_dir.iterdir()):
        if not year_dir.is_dir():
            continue
        year = int(year_dir.name)
        
        for month_dir in sorted(year_dir.iterdir()):
            if not month_dir.is_dir():
                continue
            month = int(month_dir.name) + 1  # Convert from 0-indexed
            
            for day_dir in sorted(month_dir.iterdir()):
                if not day_dir.is_dir():
                    continue
                day = int(day_dir.name)
                
                # Check if any bi5 files exist
                if list(day_dir.glob("*.bi5")):
                    date_str = f"{year}-{month:02d}-{day:02d}"
                    dates.append(date_str)
    
    return dates


def convert_pair(pair, max_workers=4):
    """指定ペアの全データをParquetに変換"""
    print(f"\n{'='*60}")
    print(f"Converting {pair}...")
    print(f"{'='*60}")
    
    # 全日付を取得
    dates = find_all_bi5_dates(pair)
    
    if not dates:
        print(f"No bi5 data found for {pair}")
        return
    
    print(f"Found {len(dates)} days of data")
    print(f"Date range: {dates[0]} to {dates[-1]}")
    
    # タスクリストを作成
    tasks = [(pair, date) for date in dates]
    
    # 並列変換
    success_count = 0
    error_count = 0
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(convert_day_to_parquet, task): task for task in tasks}
        
        with tqdm(total=len(tasks), desc=pair, unit="day") as pbar:
            for future in as_completed(futures):
                output_file, msg = future.result()
                
                if output_file:
                    success_count += 1
                else:
                    error_count += 1
                
                pbar.update(1)
                pbar.set_postfix({"Success": success_count, "Errors": error_count})
    
    print(f"\nCompleted {pair}:")
    print(f"  ✓ Successfully converted: {success_count} days")
    print(f"  ✗ Errors/Missing: {error_count} days")


def main():
    parser = argparse.ArgumentParser(description="Convert bi5 files to Parquet format")
    parser.add_argument("--symbol", type=str, help="Currency pair to convert (e.g., EURUSD)")
    parser.add_argument("--all", action="store_true", help="Convert all currency pairs")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers (default: 4)")
    
    args = parser.parse_args()
    
    if not args.symbol and not args.all:
        parser.print_help()
        sys.exit(1)
    
    # Parquetディレクトリを作成
    PARQUET_DIR.mkdir(exist_ok=True)
    
    print(f"Parquet Conversion Tool")
    print(f"Data source: {DATA_DIR.absolute()}")
    print(f"Output directory: {PARQUET_DIR.absolute()}")
    print(f"Parallel workers: {args.workers}")
    
    if args.all:
        pairs = get_all_pairs()
        for pair in pairs:
            convert_pair(pair, max_workers=args.workers)
    else:
        convert_pair(args.symbol, max_workers=args.workers)
    
    print(f"\n{'='*60}")
    print("Conversion complete!")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
