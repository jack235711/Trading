from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path
from bi5_reader import load_day_data_smart, load_date_range_data_smart, DATA_DIR, PARQUET_DIR
import sys
sys.path.append("../engine")
from engine import run_backtest
from pydantic import BaseModel
from typing import Optional, List
from functools import lru_cache
import json
import os

app = FastAPI()

# Cache for OHLC data - stores up to 1000 unique requests
@lru_cache(maxsize=1000)
def get_cached_ohlc(symbol: str, start_date: str, end_date: str):
    """Cached version of OHLC data loading (Parquet-first with bi5 fallback)"""
    try:
        if start_date == end_date:
            ohlc = load_day_data_smart(symbol, start_date)
        else:
            ohlc = load_date_range_data_smart(symbol, start_date, end_date)
        
        result = []
        for _, row in ohlc.iterrows():
            result.append({
                "time": row['time'].strftime("%Y-%m-%dT%H:%M:%SZ"),
                "open": float(row['open']),
                "high": float(row['high']),
                "low": float(row['low']),
                "close": float(row['close'])
            })
        return result
    except FileNotFoundError:
        return []
    except Exception as e:
        print(f"Error: {e}")
        return []

@app.get("/symbols")
def get_symbols():
    """利用可能な通貨ペアのリストを取得"""
    symbols = set()
    
    # 既存のデータディレクトリをスキャン
    if DATA_DIR.exists():
        symbols.update([d.name for d in DATA_DIR.iterdir() if d.is_dir() and not d.name.startswith('.')])
        
    # Parquetディレクトリをスキャン
    if PARQUET_DIR.exists():
        symbols.update([d.name for d in PARQUET_DIR.iterdir() if d.is_dir() and not d.name.startswith('.')])
        
    return sorted(list(symbols))

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/ohlc")
def get_ohlc(
    start_date: Optional[str] = Query(None, description="開始日 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="終了日 (YYYY-MM-DD)"),
    symbol: str = Query("EURUSD", description="通貨ペア")
):
    if start_date is None:
        start_date = "2020-01-01"
    if end_date is None:
        end_date = start_date
    
    # Use cached version for instant response
    return get_cached_ohlc(symbol, start_date, end_date)

class LabRequest(BaseModel):
    symbol: str
    start: str
    end: str
    fast: int
    slow: int

@app.post("/lab/run")
def run_lab_strategy(req: LabRequest):
    # Execute the Polars engine
    # In a real "AI" scenario, we would parse natural language here.
    # For now, we use the explicitly extracted params.
    result = run_backtest(req.symbol, req.start, req.end, req.fast, req.slow)
    return result

# --- Serve Frontend ---
app.mount("/static", StaticFiles(directory="../frontend"), name="static")

@app.get("/")
async def read_index():
    return FileResponse("../frontend/index.html")

# Also serve files direct for simplicity if needed
@app.get("/{filename}")
async def get_frontend_file(filename: str):
    path = Path(f"../frontend/{filename}")
    if path.exists(): return FileResponse(path)
    return {"error": "Not Found"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
