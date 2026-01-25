
import os
import requests
from pathlib import Path
from datetime import datetime, timedelta
import time

def check_and_redownload(pair="EURUSD", start_year=2025, end_year=2025):
    base_dir = Path(f"../data/{pair}")
    
    print(f"Checking data for {pair} ({start_year}-{end_year})...")
    
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    current_date = start_date
    today = datetime.now()
    
    missing_count = 0
    redownload_count = 0
    
    while current_date <= end_date and current_date <= today:
        year = current_date.year
        month = current_date.month - 1
        day = current_date.day
        
        day_dir = base_dir / str(year) / f"{month:02d}" / f"{day:02d}"
        
        # Check 0-23h files
        for hour in range(24):
            file_path = day_dir / f"{hour:02d}h_ticks.bi5"
            
            needs_download = False
            
            if not file_path.exists():
                # print(f"Missing: {file_path}")
                needs_download = True
            elif file_path.stat().st_size == 0:
                print(f"Empty file (0 bytes): {file_path}")
                file_path.unlink() # Delete empty file
                needs_download = True
                
            if needs_download:
                missing_count += 1
                # Try simple download
                url = f"https://datafeed.dukascopy.com/datafeed/{pair}/{year}/{month:02d}/{day:02d}/{hour:02d}h_ticks.bi5"
                try:
                    # 100ms delay to avoid rate limit
                    time.sleep(0.1) 
                    
                    res = requests.get(url, timeout=10)
                    if res.status_code == 200:
                        day_dir.mkdir(parents=True, exist_ok=True)
                        with open(file_path, 'wb') as f:
                            f.write(res.content)
                        if len(res.content) > 0:
                            print(f"Downloaded: {file_path} ({len(res.content)} bytes)")
                            redownload_count += 1
                    elif res.status_code == 404:
                        pass # Valid but no data
                    else:
                        print(f"Failed {url}: {res.status_code}")
                except Exception as e:
                    print(f"Error downloading {url}: {e}")
        
        current_date += timedelta(days=1)
        if current_date.day == 1:
            print(f"Processed through {current_date.strftime('%Y-%m-%d')}...")

    print(f"Check complete.")
    print(f"Missing/Empty files found: {missing_count}")
    print(f"Successfully redownloaded: {redownload_count}")

if __name__ == "__main__":
    check_and_redownload()
