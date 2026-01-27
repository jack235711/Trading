use anyhow::{Context, Result};
use clap::Parser;
use polars::prelude::*;
use serde::Serialize;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    symbol: String,

    #[arg(long)]
    start: String, // YYYY-MM-DD

    #[arg(long)]
    end: String, // YYYY-MM-DD

    #[arg(long, default_value = "20")]
    fast_period: usize,

    #[arg(long, default_value = "50")]
    slow_period: usize,
}

#[derive(Serialize)]
struct BacktestResult {
    total_trades: usize,
    total_pnl: f64,
    win_rate: f64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    
    // Parquet file path discovery (Simplified glob optimization)
    // Looking for files in ../parquet_data/{symbol}/{year}/{month}/{day}.parquet
    let base_path = format!("../parquet_data/{}/**/*.parquet", args.symbol);
    
    // Create ScanArgs
    let scan_args = ScanArgsParquet::default();

    // Lazy load
    let lf = LazyFrame::scan_parquet(&base_path, scan_args)?
        .with_columns(vec![
            col("time").cast(DataType::Datetime(TimeUnit::Nanoseconds, None)),
        ])
        .filter(
            col("time").gt_eq(lit(NaiveDate::parse_from_str(&args.start, "%Y-%m-%d")?.and_hms_opt(0, 0, 0).unwrap()))
            .and(col("time").lt_eq(lit(NaiveDate::parse_from_str(&args.end, "%Y-%m-%d")?.and_hms_opt(23, 59, 59).unwrap())))
        )
        .sort("time", Default::default());

    // Strategy Execution (Vectorized)
    // 1. Calc SMAs
    // 2. Determine Signal
    let strategy_lf = lf
        .with_columns(vec![
            col("close").rolling_mean(RollingOptions { window_size: Duration::new(args.fast_period as i64), min_periods: args.fast_period, ..Default::default() }).alias("fast_sma"),
            col("close").rolling_mean(RollingOptions { window_size: Duration::new(args.slow_period as i64), min_periods: args.slow_period, ..Default::default() }).alias("slow_sma"),
        ])
        .with_columns(vec![
            (col("fast_sma").gt(col("slow_sma"))).alias("bullish"),
        ])
        .with_columns(vec![
            (col("bullish").neq(col("bullish").shift(1))).alias("crossover"),
        ])
        .filter(col("crossover"))
        .select(vec![
            col("time"),
            col("close"),
            col("bullish"), // True = Golden Cross (Buy), False = Dead Cross (Sell)
        ]);

    // Collect result processing in memory (Usually minimal result set)
    let df = strategy_lf.collect()?;
    
    // Simple PnL Calculation (FIFO in memory iterate)
    // For simplicity: Always in market. Buy at Golden, Sell at Dead.
    let closes = df.column("close")?.f64()?;
    let bullish = df.column("bullish")?.bool()?;
    
    let mut balance = 0.0;
    let mut position = 0; // 0: None, 1: Long, -1: Short
    let mut entry_price = 0.0;
    let mut winning_trades = 0;
    let mut total_trades = 0;

    for i in 0..df.height() {
        let price = closes.get(i).unwrap();
        let is_buy_signal = bullish.get(i).unwrap();
        
        // Close existing
        if position != 0 {
            let pnl = if position == 1 { price - entry_price } else { entry_price - price };
            balance += pnl * 10000.0; // 1 lot = 100k, mini 10k? Let's say 10k units
            if pnl > 0.0 { winning_trades += 1; }
            total_trades += 1;
        }
        
        // Open new
        if is_buy_signal {
            position = 1;
        } else {
            position = -1;
        }
        entry_price = price;
    }

    let result = BacktestResult {
        total_trades,
        total_pnl: balance,
        win_rate: if total_trades > 0 { winning_trades as f64 / total_trades as f64 } else { 0.0 },
    };

    println!("{}", serde_json::to_string(&result)?);

    Ok(())
}
