import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
import os

# Configuration
SCRIPT_DIR = Path(__file__).parent.absolute()
DATA_DIR = SCRIPT_DIR / "data"
OHLCV_DIR = DATA_DIR / "ohlcv"

def migrate():
    print(f"Starting migration from CSV to Parquet in {DATA_DIR}...")
    OHLCV_DIR.mkdir(exist_ok=True)
    
    # Find all CSV files
    csv_files = list(DATA_DIR.glob("*.csv"))
    if not csv_files:
        print("No CSV files found for migration.")
        return

    for csv_path in csv_files:
        try:
            # Parse symbol and interval from filename (e.g., VN30F1M_1m.csv)
            parts = csv_path.stem.split('_')
            if len(parts) < 2:
                continue
            
            symbol = parts[0]
            # We only support 1m migration to the standard storage for now
            if parts[1] != "1m":
                print(f"Skipping {csv_path.name} (only 1m is migrated to daily partitions)")
                continue

            print(f"Migrating {csv_path.name}...")
            df = pd.read_csv(csv_path)
            
            # Ensure timestamp column is datetime
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            elif 'datetime' in df.columns:
                df['timestamp'] = pd.to_datetime(df['datetime'])
            else:
                print(f"  - Error: No timestamp/datetime column found in {csv_path.name}")
                continue

            # Drop old column if it was 'datetime'
            if 'datetime' in df.columns and 'timestamp' in df.columns:
                df = df.drop(columns=['datetime'])

            # Group by date
            df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d')
            
            symbol_dir = OHLCV_DIR / symbol.replace("!", "F")
            symbol_dir.mkdir(parents=True, exist_ok=True)

            for date_str, group in df.groupby('date'):
                file_path = symbol_dir / f"{date_str}.parquet"
                
                final_df = group.drop(columns=['date']).sort_values("timestamp")
                
                # If parquet exists, merge to avoid data loss
                if file_path.exists():
                    existing_df = pd.read_parquet(file_path)
                    final_df = pd.concat([existing_df, final_df]).drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
                
                final_df.to_parquet(file_path, compression="snappy")
            
            print(f"  - Successfully migrated {symbol} to daily Parquet partitions.")
            
            # Optional: rename original file to .bak or delete
            # os.rename(csv_path, csv_path.with_suffix(".csv.bak"))

        except Exception as e:
            print(f"  - Error migrating {csv_path.name}: {e}")

if __name__ == "__main__":
    migrate()
