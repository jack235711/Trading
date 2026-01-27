import pandas as pd
from pathlib import Path

def main():
    # Pick a file based on search results
    file_path = Path("../parquet_data/EURUSD/2025/11/03.parquet")
    if not file_path.exists():
        # Try another one if 03.parquet doesn't exist
        parquet_dir = Path("../parquet_data")
        files = list(parquet_dir.glob("**/*.parquet"))
        if not files:
            print("No parquet files found.")
            return
        file_path = files[0]
        print(f"File not found: ../parquet_data/EURUSD/2025/11/03.parquet. Using {file_path} instead.\n")

    df = pd.read_parquet(file_path)
    head_100 = df.head(100)
    print(f"--- Showing first 100 rows of {file_path} ---")
    
    chunk_size = 10
    for i in range(0, 100, chunk_size):
        print(head_100.iloc[i:i+chunk_size].to_string(header=(i==0)))
        print("-" * 50)

if __name__ == "__main__":
    main()
