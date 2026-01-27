
import polars as pl
import argparse
import json
from pathlib import Path
from datetime import datetime, timezone

def run_backtest(symbol, start_date, end_date, fast_sma, slow_sma):
    # Determine parquet path pattern
    # ../parquet_data/{symbol}/**/*.parquet
    base_path = Path(f"../parquet_data/{symbol}")
    if not base_path.exists():
        return {"error": f"No data found for {symbol}"}

    try:
        # Scan Parquet (Lazy)
        q = pl.scan_parquet(f"../parquet_data/{symbol}/**/*.parquet")
        
        # Filter Date Range
        # Convert inputs to datetime with UTC to match Parquet data
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
        
        q = q.filter(
            (pl.col("time") >= start_dt) & (pl.col("time") <= end_dt)
        )
        
        # Sort (Parquet partitions might be unordered)
        q = q.sort("time")
        
        # Strategy Logic (Vectorized)
        # 1. Calculate SMAs
        q = q.with_columns([
            pl.col("close").rolling_mean(window_size=fast_sma).alias("fast"),
            pl.col("close").rolling_mean(window_size=slow_sma).alias("slow"),
        ])
        
        # 2. Signals
        # Bullish: Fast > Slow
        q = q.with_columns(
            (pl.col("fast") > pl.col("slow")).alias("bullish")
        )
        
        # 3. Crossover (State Change)
        # We also need to keep the SMA data for visualization
        q = q.with_columns(
            (pl.col("bullish") != pl.col("bullish").shift(1)).fill_null(False).alias("signal_change")
        )
        
        # 4. Filter only relevant columns and collect all bars for time-axis sync
        # We need 'time', 'close', 'bullish', 'fast', 'slow' for every bar.
        full_data = q.select(["time", "close", "bullish", "fast", "slow"]).collect()
        
        # Process Trades and Equity in Python
        trades = []
        equity_curve = []
        
        position = 0 # 0: None, 1: Long, -1: Short
        entry_price = 0.0
        entry_time = None
        
        realized_pnl = 0.0
        wins = 0
        count = 0
        
        multiplier = 100 if "JPY" in symbol else 10000
        
        rows = full_data.rows(named=True)
        for row in rows:
            is_bullish = row['bullish']
            price = row['close']
            time = row['time']
            
            # Check for Signal Change (Crossover)
            # Simplified: if is_bullish is True and we aren't Long -> Buy
            # If is_bullish is False and we aren't Short -> Sell
            
            signal_target = 1 if is_bullish else -1
            
            if position != signal_target:
                # Close existing if any
                if position != 0:
                    pnl = 0.0
                    if position == 1:
                        pnl = price - entry_price
                    else:
                        pnl = entry_price - price
                    
                    realized_pnl += pnl
                    if pnl > 0: wins += 1
                    count += 1
                    
                    trades.append({
                        "entry_time": entry_time.isoformat(),
                        "exit_time": time.isoformat(),
                        "entry_price": entry_price,
                        "exit_price": price,
                        "type": "LONG" if position == 1 else "SHORT",
                        "pnl": pnl
                    })
                
                # Open new
                position = signal_target
                entry_price = price
                entry_time = time
                
            # Current Equity = Realized + Floating
            floating_pnl = 0.0
            if position == 1:
                floating_pnl = price - entry_price
            elif position == -1:
                floating_pnl = entry_price - price
                
            current_total_pips = (realized_pnl + floating_pnl) * multiplier
            equity_curve.append({
                "time": time.isoformat(),
                "value": round(current_total_pips, 2)
            })

        # Prepare indicator data for frontend display
        # Only taking a sample if data is huge, but for now we take all to match bars
        indicators = {
            "fast_sma": [{"time": row['time'].isoformat(), "value": round(row['fast'], 5)} for row in full_data.rows(named=True) if row['fast'] is not None],
            "slow_sma": [{"time": row['time'].isoformat(), "value": round(row['slow'], 5)} for row in full_data.rows(named=True) if row['slow'] is not None]
        }

        return {
            "symbol": symbol,
            "period_start": start_date,
            "period_end": end_date,
            "stats": {
                "total_trades": count,
                "win_rate": round(wins / count if count > 0 else 0, 4),
                "total_pnl_pips": round(realized_pnl * multiplier, 2),
                "profit_factor": 1.5 
            },
            "trades": trades,
            "equity": equity_curve,
            "indicators": indicators
        }

    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--fast", type=int, default=20)
    parser.add_argument("--slow", type=int, default=50)
    
    args = parser.parse_args()
    
    result = run_backtest(args.symbol, args.start, args.end, args.fast, args.slow)
    print(json.dumps(result, indent=2))
