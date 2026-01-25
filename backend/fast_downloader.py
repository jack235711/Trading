
import requests
import os
from pathlib import Path
from datetime import datetime, timedelta
import concurrent.futures
import time
from tqdm import tqdm

# Settings
PAIRS = ["EURUSD", "USDJPY", "GBPUSD", "EURJPY", "EURGBP"]
START_YEAR = 2000
END_YEAR = 2025
BASE_DIR = Path("../data")
MAX_WORKERS = 30  # Increased threads for speed

# Dukascopy API Template
URL_TEMPLATE = "https://datafeed.dukascopy.com/datafeed/{pair}/{year}/{month:02d}/{day:02d}/{hour:02d}h_ticks.bi5"

def download_file(args):
    pair, date, hour, save_dir = args
    year = date.year
    month = date.month - 1 # 0-indexed for URL
    day = date.day
    
    url = URL_TEMPLATE.format(pair=pair, year=year, month=month, day=day, hour=hour)
    filename = save_dir / f"{hour:02d}h_ticks.bi5"
    
    if filename.exists() and filename.stat().st_size > 0:
        return 'skipped'
    
    try:
        for attempt in range(3):
            try:
                res = requests.get(url, timeout=10)
                if res.status_code == 200:
                    filename.parent.mkdir(parents=True, exist_ok=True)
                    with open(filename, "wb") as f:
                        f.write(res.content)
                    return 'downloaded'
                elif res.status_code == 404:
                    return 'not_found' 
                elif res.status_code == 503:
                    time.sleep(1 * (attempt+1))
                    continue
                else:
                    return f'error_{res.status_code}'
            except requests.exceptions.RequestException:
                time.sleep(1)
                continue
        return 'failed'
    except Exception as e:
        return f'exception_{e}'

def process_year_block(year):
    print(f"\n=== Processing Year {year} for ALL PAIRS ===")
    
    all_tasks = []
    
    for pair in PAIRS:
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 12, 31)
        current = start_date
        
        while current <= end_date:
            month = current.month - 1
            day = current.day
            save_dir = BASE_DIR / pair / str(year) / f"{month:02d}" / f"{day:02d}"
            
            for h in range(24):
                all_tasks.append((pair, current, h, save_dir))
            
            current += timedelta(days=1)

    print(f"Total files for {year}: {len(all_tasks)}")
    
    results = {'downloaded': 0, 'skipped': 0, 'not_found': 0, 'failed': 0}
    
    # Process all pairs for this year in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_file, t): t for t in all_tasks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(all_tasks), desc=f"Year {year}", unit="file"):
            res = future.result()
            if res in results:
                results[res] += 1
            else:
                results['failed'] += 1
                
    print(f"Finished Year {year}: {results}")

def main():
    # Iterate years descending (Newest first)
    for year in range(END_YEAR, START_YEAR - 1, -1):
        process_year_block(year)

if __name__ == "__main__":
    if not BASE_DIR.exists():
        BASE_DIR.mkdir()
    main()
