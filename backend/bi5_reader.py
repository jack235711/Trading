"""
bi5ファイルを読み込み、1分足OHLCに変換するモジュール
Parquetファイルが存在する場合はそちらを優先的に読み込む
"""
import lzma
import struct
from pathlib import Path
from datetime import datetime, timezone, timedelta
import pandas as pd

# Parquetデータディレクトリ
PARQUET_DIR = Path("../parquet_data")


def read_bi5_file(filepath: Path, base_timestamp_ms: int):
    """
    bi5ファイルを読み込み、ティックデータをDataFrameに変換
    
    Args:
        filepath: bi5ファイルのパス
        base_timestamp_ms: 基準タイムスタンプ（ミリ秒）
    
    Returns:
        pandas.DataFrame: ティックデータ（columns: timestamp, price）
    """
    # LZMA解凍
    with open(filepath, 'rb') as f:
        compressed_data = f.read()
    
    # 空ファイルの場合は空のDataFrameを返す
    if len(compressed_data) == 0:
        return pd.DataFrame(columns=['timestamp', 'price'])
    
    decompressed_data = lzma.decompress(compressed_data)
    
    # ティックデータをパース
    ticks = []
    offset = 0
    current_time_ms = base_timestamp_ms
    
    # 1レコード = 20バイト
    record_size = 20
    num_records = len(decompressed_data) // record_size
    
    for i in range(num_records):
        # 20バイトを読み込み
        record = decompressed_data[offset:offset + record_size]
        
        # int32, int32, int32, float, float (Total 20 bytes)
        # TimeDelta, Ask, Bid, AskVolume, BidVolume
        time_delta_ms, ask, bid, ask_vol, bid_vol = struct.unpack('>iiiff', record)
        
        # タイムスタンプは時開始からの絶対値（ミリ秒）
        timestamp_ms = base_timestamp_ms + time_delta_ms
        
        # bid価格を使用（price = bid / 100000）
        price = bid / 100000.0
        
        ticks.append({
            'timestamp': timestamp_ms,
            'price': price
        })
        
        offset += record_size
    
    return pd.DataFrame(ticks)


def load_day_data(pair: str, date: str):
    """
    指定した日付の全ティックデータを読み込み、1分足OHLCに変換
    
    Args:
        pair: 通貨ペア（例: "EURUSD"）
        date: 日付（例: "2020-01-01"）
    
    Returns:
        pandas.DataFrame: 1分足OHLC（columns: time, open, high, low, close）
    """
    # 日付をパース
    dt = datetime.strptime(date, "%Y-%m-%d")
    year = dt.year
    month = dt.month - 1  # Dukascopyは0-indexed
    day = dt.day
    
    # データディレクトリ
    data_dir = Path(f"../data/{pair}/{year}/{month:02d}/{day:02d}")
    
    if not data_dir.exists():
        raise FileNotFoundError(f"データディレクトリが見つかりません: {data_dir}")
    
    # 全ティックデータを結合
    all_ticks = []
    
    for hour in range(24):
        filepath = data_dir / f"{hour:02d}h_ticks.bi5"
        
        if not filepath.exists():
            continue
        
        # 基準タイムスタンプ（その時間の開始時刻）
        base_dt = datetime(year, month + 1, day, hour, 0, 0, tzinfo=timezone.utc)
        base_timestamp_ms = int(base_dt.timestamp() * 1000)
        
        # bi5ファイルを読み込み
        df = read_bi5_file(filepath, base_timestamp_ms)
        if not df.empty:
            all_ticks.append(df)
    
    if not all_ticks:
        return pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close'])
    
    # 全ティックを結合
    df_all = pd.concat(all_ticks, ignore_index=True)
    
    # タイムスタンプをdatetimeに変換
    df_all['datetime'] = pd.to_datetime(df_all['timestamp'], unit='ms', utc=True)
    df_all.set_index('datetime', inplace=True)
    
    # 1分足OHLCにresample
    ohlc = df_all['price'].resample('1min').ohlc()
    
    # NaNを除去
    ohlc = ohlc.dropna()
    
    # インデックスをリセットしてtime列を作成
    ohlc = ohlc.reset_index()
    ohlc.rename(columns={'datetime': 'time'}, inplace=True)
    
    return ohlc


def load_date_range_data(pair: str, start_date: str, end_date: str):
    """
    指定した日付範囲の全ティックデータを読み込み、1分足OHLCに変換
    
    Args:
        pair: 通貨ペア（例: "EURUSD"）
        start_date: 開始日（例: "2025-01-01"）
        end_date: 終了日（例: "2025-01-31"）
    
    Returns:
        pandas.DataFrame: 1分足OHLC（columns: time, open, high, low, close）
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    all_ohlc = []
    current_dt = start_dt
    
    while current_dt <= end_dt:
        date_str = current_dt.strftime("%Y-%m-%d")
        try:
            ohlc = load_day_data(pair, date_str)
            if not ohlc.empty:
                all_ohlc.append(ohlc)
        except FileNotFoundError:
            pass  # データがない日はスキップ
        
        current_dt += timedelta(days=1)
    
    if not all_ohlc:
        return pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close'])
    
    # 全データを結合
    result = pd.concat(all_ohlc, ignore_index=True)
    return result


def load_day_data_from_parquet(pair: str, date: str):
    """
    Parquetファイルから1日分のOHLCデータを読み込み
    
    Args:
        pair: 通貨ペア（例: "EURUSD"）
        date: 日付（例: "2025-04-01"）
    
    Returns:
        pandas.DataFrame: 1分足OHLC（columns: time, open, high, low, close）
    """
    dt = datetime.strptime(date, "%Y-%m-%d")
    year = dt.year
    month = dt.month - 1  # Dukascopy format
    day = dt.day
    
    parquet_file = PARQUET_DIR / pair / str(year) / f"{month:02d}" / f"{day:02d}.parquet"
    
    if not parquet_file.exists():
        raise FileNotFoundError(f"Parquetファイルが見つかりません: {parquet_file}")
    
    # Parquetから読み込み（高速）
    df = pd.read_parquet(parquet_file, engine='pyarrow')
    return df


def load_date_range_data_from_parquet(pair: str, start_date: str, end_date: str):
    """
    Parquetファイルから日付範囲のOHLCデータを読み込み
    
    Args:
        pair: 通貨ペア（例: "EURUSD"）
        start_date: 開始日（例: "2025-01-01"）
        end_date: 終了日（例: "2025-01-31"）
    
    Returns:
        pandas.DataFrame: 1分足OHLC（columns: time, open, high, low, close）
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    all_ohlc = []
    current_dt = start_dt
    
    while current_dt <= end_dt:
        date_str = current_dt.strftime("%Y-%m-%d")
        try:
            ohlc = load_day_data_from_parquet(pair, date_str)
            if not ohlc.empty:
                all_ohlc.append(ohlc)
        except FileNotFoundError:
            pass  # データがない日はスキップ
        
        current_dt += timedelta(days=1)
    
    if not all_ohlc:
        return pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close'])
    
    # 全データを結合
    result = pd.concat(all_ohlc, ignore_index=True)
    return result


def load_day_data_smart(pair: str, date: str):
    """
    Parquetが存在すればそちらから、なければbi5から読み込み
    
    Args:
        pair: 通貨ペア（例: "EURUSD"）
        date: 日付（例: "2025-04-01"）
    
    Returns:
        pandas.DataFrame: 1分足OHLC
    """
    try:
        return load_day_data_from_parquet(pair, date)
    except FileNotFoundError:
        # Parquetがなければbi5から読み込み
        return load_day_data(pair, date)


def load_date_range_data_smart(pair: str, start_date: str, end_date: str):
    """
    Parquetが存在すればそちらから、なければbi5から読み込み
    
    Args:
        pair: 通貨ペア（例: "EURUSD"）
        start_date: 開始日（例: "2025-01-01"）
        end_date: 終了日（例: "2025-01-31"）
    
    Returns:
        pandas.DataFrame: 1分足OHLC
    """
    try:
        return load_date_range_data_from_parquet(pair, start_date, end_date)
    except FileNotFoundError:
        # Parquetがなければbi5から読み込み
        return load_date_range_data(pair, start_date, end_date)


if __name__ == "__main__":
    # テスト: EURUSD / 2020-01-01 のデータを読み込み
    ohlc = load_day_data("EURUSD", "2020-01-01")
    print(ohlc.head(10))
    print(f"\n総レコード数: {len(ohlc)}")
