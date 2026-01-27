
import os
import requests
import struct
import lzma
import pandas as pd
import concurrent.futures
from pathlib import Path
from datetime import datetime, timedelta, timezone
from tqdm import tqdm
import time

# Options
PAIRS = ["EURUSD", "USDJPY", "GBPUSD", "EURJPY", "EURGBP"]
START_YEAR = 2000
END_YEAR = 2025
PARQUET_DIR = Path("../parquet_data")
MAX_WORKERS = 6 # Golden ratio: 4-8
BASE_URL = "https://datafeed.dukascopy.com/datafeed"

def parse_bi5(compressed_data, base_timestamp_ms):
    if not compressed_data:
        return []
    try:
        decompressed_data = lzma.decompress(compressed_data)
    except:
        return []

    ticks = []
    offset = 0
    # struct format: >iiiff (Big-endian: int, int, int, float, float)
    # 20 bytes per record
    record_size = 20
    num_records = len(decompressed_data) // record_size

    for _ in range(num_records):
        time_delta, ask, bid, ask_vol, bid_vol = struct.unpack('>iiiff', decompressed_data[offset:offset+20])
        timestamp = base_timestamp_ms + time_delta
        # Price scaling
        price = bid / 100000.0
        ticks.append((timestamp, price))
        offset += 20
        
    return ticks

def process_day(args):
    pair, date_str, session = args
    
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.year
    month = dt.month - 1
    day = dt.day
    
    # Target Parquet File
    save_path = PARQUET_DIR / pair / str(year) / f"{month:02d}" / f"{day:02d}.parquet"
    if save_path.exists():
        return 'skipped'

    all_ticks = []
    
    # Download 24h
    for hour in range(24):
        url = f"{BASE_URL}/{pair}/{year}/{month:02d}/{day:02d}/{hour:02d}h_ticks.bi5"
        
        try:
            resp = session.get(url, timeout=5)
            if resp.status_code == 200:
                # Calculate base timestamp for this hour
                base_dt = datetime(year, month + 1, day, hour, 0, 0, tzinfo=timezone.utc)
                base_ts = int(base_dt.timestamp() * 1000)
                
                ticks = parse_bi5(resp.content, base_ts)
                all_ticks.extend(ticks)
            elif resp.status_code == 404:
                pass # No data
            else:
                pass # potentially retry?
        except Exception as e:
            # print(f"Err {url}: {e}")
            pass
            
    if not all_ticks:
        return 'empty'
        
    # Convert to DataFrame
    df = pd.DataFrame(all_ticks, columns=['timestamp', 'price'])
    
    # Optimize conversion
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df.set_index('datetime', inplace=True)
    
    # Resample to 1min OHLC
    ohlc = df['price'].resample('1min').ohlc()
    ohlc = ohlc.dropna().reset_index()
    ohlc.rename(columns={'datetime': 'time'}, inplace=True)
    
    if ohlc.empty:
        return 'empty'
        
    # Save to Parquet
    save_path.parent.mkdir(parents=True, exist_ok=True)
    ohlc.to_parquet(save_path, engine='pyarrow', compression='snappy')
    
    return 'done'

def main():
    if not PARQUET_DIR.exists():
        PARQUET_DIR.mkdir()

    # Create task queue (Recent first)
    tasks = []
    print("Generating task list...")
    
    # Priority: Year Descending, Pair
    for year in range(END_YEAR, START_YEAR - 1, -1):
        for pair in PAIRS:
            start_date = datetime(year, 1, 1)
            end_date = datetime(year, 12, 31)
            current = start_date
            while current <= end_date:
                # Limit to today
                if current > datetime.now():
                    break
                tasks.append((pair, current.strftime("%Y-%m-%d")))
                current += timedelta(days=1)

    print(f"Total Daily Tasks: {len(tasks)}")
    
    # Use a session per thread? 
    # Actually, we can pass a session created inside the thread initializer, 
    # but concurrent.futures doesn't support initializers easily for ThreadPoolExecutor in the args way easily.
    # Simpler: Create session inside the worker or pass one? 
    # Passing session across threads is not thread-safe for requests.Session object usually? 
    # Documentation says Session is not thread-safe.
    # So we should create a session local to the thread.
    # We will use a custom worker function that creates a session.
    
    # However, for simplicity with Executor, I'll instantiate a session per batch or just standard requests? 
    # User said "Keep-alive". Session provides that. 
    # Let's use `thread_local` storage for sessions.
    
    pass

# Thread Local Session
import threading
thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
        # Optimize headers
        thread_local.session.headers.update({
            'User-Agent': 'Mozilla/5.0',
            'Connection': 'keep-alive'
        })
    return thread_local.session

def worker_wrapper(args):
    pair, date_str = args
    session = get_session()
    return process_day((pair, date_str, session))

if __name__ == "__main__":
    if not PARQUET_DIR.exists():
        PARQUET_DIR.mkdir()
        
    tasks = []
    # 2025 -> 2000
    for year in range(END_YEAR, START_YEAR - 1, -1):
        for pair in PAIRS:
            start = datetime(year, 1, 1)
            end = datetime(year, 12, 31)
            curr = start
            while curr <= end:
                if curr > datetime.now(): break
                tasks.append((pair, curr.strftime("%Y-%m-%d")))
                curr += timedelta(days=1)
                
    print(f"Starting Smart Pipeline: {len(tasks)} days to process.")
    print(f"Max Workers: {MAX_WORKERS}")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        list(tqdm(executor.map(worker_wrapper, tasks), total=len(tasks), unit="day"))

