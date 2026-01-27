import os
import sys
import shutil
from pathlib import Path
import argparse
import time
import requests
from datetime import datetime, timedelta, timezone
import concurrent.futures
from tqdm import tqdm
import pandas as pd
import subprocess

# Add current dir to path to import sibling modules
sys.path.append(str(Path(__file__).parent))

# Import logic from other modules if possible, but for stability I might implement wrapper calls
# or reimplement simple logic.
# fast_downloader3 has good download logic.

# Settings
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = (SCRIPT_DIR / "../data").resolve()
PARQUET_DIR = (SCRIPT_DIR / "../parquet_data").resolve()

# Dukascopy API Template
URL_TEMPLATE = "https://datafeed.dukascopy.com/datafeed/{pair}/{year}/{month:02d}/{day:02d}/{hour:02d}h_ticks.bi5"

def get_existing_pairs():
    if not PARQUET_DIR.exists():
        return []
    return [d.name for d in PARQUET_DIR.iterdir() if d.is_dir() and not d.name.startswith('.')]

def scan_missing_months(pair):
    """
    Scans the parquet directory for the given pair and finds missing months
    between the start and end of the existing data.
    """
    pair_dir = PARQUET_DIR / pair
    if not pair_dir.exists():
        print(f"No data for {pair}")
        return []

    years = sorted([int(d.name) for d in pair_dir.iterdir() if d.is_dir() and d.name.isdigit()])
    if not years:
        return []

    min_year = min(years)
    max_year = max(years)
    
    missing = []
    
    # Check all months between min and max
    # Note: Dukascopy data usually starts from distinct points, but we will assume contiguous expectation
    # between min and max year found.
    
    for year in range(min_year, max_year + 1):
        year_dir = pair_dir / str(year)
        
        # Define expected months
        # For start year, check existing? Or just check all 00-11?
        # Let's check all 00-11 for simplicity within the range
        
        expected_months = range(12)
        
        # If year_dir doesn't exist at all, all months are missing
        if not year_dir.exists():
            for m in expected_months:
                missing.append((year, m))
            continue
            
        # Check for .parquet files
        existing_months = []
        for f in year_dir.glob("*.parquet"):
            try:
                # Expecting '00.parquet', '11.parquet'
                m_curr = int(f.stem)
                existing_months.append(m_curr)
            except:
                pass
        
        for m in expected_months:
            # Skip future months if year is current year
            now = datetime.now(timezone.utc)
            if year == now.year and m > (now.month - 1): # current month might be incomplete? or allow previous month?
                continue
            
            if m not in existing_months:
                missing.append((year, m))
                
    return missing

def download_month_bi5(pair, year, month):
    """
    Download all bi5 files for a specific month.
    """
    # month is 0-indexed here
    
    # Calculate days in month
    # Using datetime to find next month then subtract day
    if month == 11:
        next_month_date = datetime(year + 1, 1, 1)
    else:
        next_month_date = datetime(year, month + 2, 1) # month+1 is actual month, so month+2 is next
    
    # Start date = year, month+1, 1
    current_date = datetime(year, month + 1, 1)
    
    tasks = []
    
    # Prepare tasks
    while current_date < next_month_date:
        # Dukascopy month is 0-indexed in URL?
        # fast_downloader3: month = date.month - 1
        # so for Jan (1), month is 0. 
        # My 'month' arg is already 0-indexed.
        
        day = current_date.day
        
        save_dir = DATA_DIR / pair / str(year) / f"{month:02d}" / f"{day:02d}"
        
        for h in range(24):
            # Check if parquet already exists? No, we know it's missing.
            # Check if bi5 exists?
            filename = save_dir / f"{h:02d}h_ticks.bi5"
            if not filename.exists() or filename.stat().st_size == 0:
                tasks.append((pair, current_date, h, save_dir))
        
        current_date += timedelta(days=1)
        
    if not tasks:
        return True # All files exist or no tasks
        
    print(f"Downloading {len(tasks)} files for {pair} {year}-{month:02d}...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(do_download, t): t for t in tasks}
        for future in concurrent.futures.as_completed(futures):
            # Just consume
            pass
            
    return True

def do_download(args):
    pair, date, hour, save_dir = args
    year = date.year
    month = date.month - 1 # 0-indexed for URL
    day = date.day
    
    url = URL_TEMPLATE.format(pair=pair, year=year, month=month, day=day, hour=hour)
    filename = save_dir / f"{hour:02d}h_ticks.bi5"
    
    # Retry loop
    for _ in range(3):
        try:
            res = requests.get(url, timeout=10)
            if res.status_code == 200:
                filename.parent.mkdir(parents=True, exist_ok=True)
                with open(filename, "wb") as f:
                    f.write(res.content)
                return True
            elif res.status_code == 404:
                return False # Not found
        except:
            time.sleep(1)
    return False

def process_missing_month(pair, year, month):
    print(f"Repairing {pair} {year} Month {month}...")
    
    # 1. Download
    download_month_bi5(pair, year, month)
    
    # 2. Convert to Daily Parquet
    # Reuse convert_to_parquet logic?
    # It's better to run it via subprocess or import logic.
    # Invoking convert_to_parquet.py for specific dates is hard because it accepts --symbol or --all.
    # It doesn't accept date range.
    # But I can use bi5_reader to load DF then save to parquet manually here.
    
    from bi5_reader import load_day_data
    
    month_start = datetime(year, month + 1, 1)
    if month == 11:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 2, 1)
        
    current = month_start
    dfs = []
    
    while current < next_month:
        date_str = current.strftime("%Y-%m-%d")
        try:
            # bi5_reader will read from ../data which we just downloaded to
            df = load_day_data(pair, date_str)
            if not df.empty:
                dfs.append(df)
        except Exception as e:
            # print(f"Failed to load {date_str}: {e}")
            pass
        current += timedelta(days=1)
        
    if not dfs:
        print(f"No data found/downloaded for {pair} {year}-{month:02d}")
        # Clean up bi5 logic here if needed
        return
        
    # 3. Aggregate
    monthly_df = pd.concat(dfs)
    monthly_df = monthly_df.sort_index()
    
    # Save
    output_dir = PARQUET_DIR / pair / str(year)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{month:02d}.parquet"
    
    monthly_df.to_parquet(output_file, compression='zstd')
    print(f"Fixed: {output_file}")
    
    # 4. Cleanup bi5
    # Delete ../data/{pair}/{year}/{month:02d}
    data_month_dir = DATA_DIR / pair / str(year) / f"{month:02d}"
    if data_month_dir.exists():
        shutil.rmtree(data_month_dir)

def main():
    pairs = get_existing_pairs()
    print(f"Scanning pairs: {pairs}")
    
    for pair in pairs:
        missing = scan_missing_months(pair)
        if not missing:
            print(f"{pair}: No missing months found in range.")
            continue
            
        print(f"{pair}: Found {len(missing)} missing months.")
        for year, month in missing:
            process_missing_month(pair, year, month)

if __name__ == "__main__":
    main()
