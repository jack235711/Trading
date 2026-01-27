import pandas as pd
from pathlib import Path
import shutil
import concurrent.futures
import time
import argparse

from functools import partial

# 設定
SCRIPT_DIR = Path(__file__).parent
PARQUET_DIR = (SCRIPT_DIR / "../parquet_data").resolve()


def get_all_pairs():
    """parquet_dataディレクトリ配下のサブディレクトリをスキャンして通貨ペアリストを取得"""
    if not PARQUET_DIR.exists():
        return []
    return [d.name for d in PARQUET_DIR.iterdir() if d.is_dir() and not d.name.startswith('.')]


def aggregate_month(pair, year, month, cleanup=False):
    """
    指定された年月の日次Parquetファイルを結合して月次ファイルを作成する
    """
    month_dir = PARQUET_DIR / pair / str(year) / f"{month:02d}"
    output_file = PARQUET_DIR / pair / str(year) / f"{month:02d}.parquet"

    # 既に月次ファイルが存在する場合
    if output_file.exists():
        # クリーンアップが要求されており、かつ月次ディレクトリがまだ残っている場合は削除を実行
        if cleanup and month_dir.exists():
            try:
                shutil.rmtree(month_dir)
                # print(f"Cleaned up {month_dir}")
                return True # Cleaned up only
            except Exception as e:
                print(f"Error deleting {month_dir}: {e}")
        return False

    if not month_dir.exists():
        return False

    day_files = sorted(list(month_dir.glob("*.parquet")))
    if not day_files:
        if cleanup:
            shutil.rmtree(month_dir)
        return False

    dfs = []
    for f in day_files:
        try:
            df = pd.read_parquet(f)
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {f}: {e}")

    if not dfs:
        return False

    try:
        # 月次データの結合
        monthly_df = pd.concat(dfs)
        monthly_df = monthly_df.sort_index()
        
        # 月次ファイルとして保存
        monthly_df.to_parquet(output_file, compression='zstd')
        
        # 元の日次ディレクトリを削除
        if cleanup:
            shutil.rmtree(month_dir)
            
        return True
    
    except Exception as e:
        print(f"Error aggregating {pair} {year}-{month:02d}: {e}")
        try:
            if output_file.exists():
                output_file.unlink()
        except:
            pass
        return False


def process_pair(cleanup, pair):
    """
    指定された通貨ペアの全期間を処理
    Args:
        cleanup (bool): 処理後に日次ディレクトリを削除するかどうか
        pair (str): 通貨ペア名
    """
    pair_dir = PARQUET_DIR / pair
    if not pair_dir.exists():
        print(f"Directory not found: {pair_dir}")
        return

    years = sorted([d.name for d in pair_dir.iterdir() if d.is_dir() and d.name.isdigit()])
    
    count = 0
    cleaned = 0
    for year in years:
        year_dir = pair_dir / year
        months = sorted([d.name for d in year_dir.iterdir() if d.is_dir() and d.name.isdigit()])
        
        for month_str in months:
            month = int(month_str)
            if aggregate_month(pair, year, month, cleanup):
                count += 1
    
    print(f"Processed {pair}: Aggregated/Cleaned {count} months")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Aggregate daily parquet files into monthly files")
    parser.add_argument("--cleanup", action="store_true", help="Delete daily folders after aggregation")
    args = parser.parse_args()

    pairs = get_all_pairs()
    print(f"Found pairs: {pairs}")
    print(f"Cleanup mode: {args.cleanup}")

    start_time = time.time()

    # process_pairにcleanup引数を部分適用
    process_func = partial(process_pair, args.cleanup)

    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        executor.map(process_func, pairs)

    print(f"Operation complete in {time.time() - start_time:.2f} seconds")
