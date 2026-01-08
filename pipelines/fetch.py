import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from pipelines.kafka_producer import send_to_kafka
# ================= CONFIG =================

url = "https://api.binance.com/api/v3/klines"
workers = 4

# ================= FETCH =================

def fetch_daily(symbol, execution_date):
    start_dt = datetime.strptime(execution_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1)

    response = requests.get(
        url,
        params={
            "symbol": symbol,
            "interval": "1h",
            "startTime": int(start_dt.timestamp() * 1000),
            "endTime": int(end_dt.timestamp() * 1000),
            "limit": 100
        },
        timeout=(5, 20)
    )

    response.raise_for_status()
    return response.json()

# ================= TRANSFORM =================

def transform(raw):
    cols = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "num_trades",
        "taker_buy_base", "taker_buy_quote", "_ignore"
    ]

    df = pd.DataFrame(raw, columns=cols)

    float_cols = [
        "open", "high", "low", "close", "volume",
        "quote_volume", "taker_buy_base", "taker_buy_quote"
    ]

    df[float_cols] = df[float_cols].astype(float)
    df["num_trades"] = df["num_trades"].astype(int)

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")

    return df

# ================= VALIDATIONS =================

def validate(df: pd.DataFrame):
    if df.empty:
        raise ValueError("Validation failed: DataFrame is empty")

    required_cols = [
        "open_time", "open", "high", "low", "close",
        "volume", "close_time", "num_trades"
    ]

    missing = set(required_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    if df[required_cols].isnull().any().any():
        raise ValueError("Null values detected")

    if df.duplicated(subset=["open_time"]).any():
        raise ValueError("Duplicate open_time detected")

    if (df["high"] < df["low"]).any():
        raise ValueError("Invalid price range detected")

    print("Data validation passed.")

# ================= One_day_task ==========

def process_one_day(execution_date, symbol, interval, table, engine):
    try:
        raw = fetch_daily(symbol, execution_date)
        if not raw:
            return
            
        df = transform(raw)
        validate(df)
        send_to_kafka(topic="binance", data=df)

        print(f" Completed {execution_date}")

        time.sleep(0.2)  # extra Binance safety

    except Exception as e:
        print(f" Failed {execution_date}: {e}")

# ================= MAIN =================

def generate_dates(start_date, end_date):
    current_date = start_date
    while current_date < end_date:
        yield current_date.strftime("%Y-%m-%d")
        current_date += timedelta(days=1)

if __name__ == "__main__":

    # Multiple symbols
    SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]   


    #  Backfill last 3 months ONLY
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=90)

    dates = list(generate_dates(start_date, end_date))

    tasks = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for symbol in SYMBOLS:
            for date in dates:
                tasks.append(
                    executor.submit(process_one_day, date, symbol)
                )

        for future in as_completed(tasks):
            future.result()

    print("Parallel backfill completed for all symbols.")