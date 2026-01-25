"""
FastAPIでOHLCデータをJSON形式で提供するAPIサーバー
"""
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from bi5_reader import load_day_data, load_date_range_data
from typing import Optional

app = FastAPI()

# CORS設定（フロントエンドからのアクセスを許可）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 本番環境では適切に制限すること
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/ohlc")
def get_ohlc(
    start_date: Optional[str] = Query(None, description="開始日 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="終了日 (YYYY-MM-DD)")
):
    """
    1分足OHLCデータをJSON形式で返す
    """
    # デフォルト値設定
    if start_date is None:
        start_date = "2020-01-01"
    
    if end_date is None:
        end_date = start_date
    
    try:
        # データを読み込み
        if start_date == end_date:
            ohlc = load_day_data("EURUSD", start_date)
        else:
            ohlc = load_date_range_data("EURUSD", start_date, end_date)
        
        # JSON形式に変換
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
        # データがない場合は単に空リストを返す（CORSエラーを防ぐため正常レスポンスとする）
        return []
    except Exception as e:
        print(f"Error processing request: {e}")
        return []


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
