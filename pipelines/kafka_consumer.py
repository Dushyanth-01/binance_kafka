import os
import json
import logging
from datetime import datetime
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("binance-consumer")

# ================= CONFIG =================
TOPIC = "binance"
BOOTSTRAP_SERVERS = "localhost:9093"   # correct for host
GROUP_ID = "binance-group"
TABLE = "binance_klines"

SYMBOL = os.getenv("SYMBOL", "BTCUSDT")
INTERVAL = os.getenv("INTERVAL", "1h")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))
DB_URI = os.getenv(
    "DB_URI",
    "postgresql+psycopg2://postgres:2001@localhost:5432/binance_klines"
)

REQUIRED_FIELDS = {
    "symbol", "interval", "open_time",
    "open", "high", "low", "close", "volume"
}

# ================= DB SETUP =================
engine = create_engine(DB_URI)

def ensure_table(engine, table):
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                symbol TEXT,
                interval TEXT,
                open_time TIMESTAMP,
                open DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                close DOUBLE PRECISION,
                volume DOUBLE PRECISION,
                close_time TIMESTAMP,
                quote_volume DOUBLE PRECISION,
                num_trades INTEGER,
                taker_buy_base DOUBLE PRECISION,
                taker_buy_quote DOUBLE PRECISION,
                PRIMARY KEY (symbol, interval, open_time)
            );
        """))

UPSERT_SQL = text(f"""
    INSERT INTO {TABLE} (
        symbol, interval, open_time, open, high, low, close,
        volume, close_time, quote_volume, num_trades,
        taker_buy_base, taker_buy_quote
    )
    VALUES (
        :symbol, :interval, :open_time, :open, :high, :low, :close,
        :volume, :close_time, :quote_volume, :num_trades,
        :taker_buy_base, :taker_buy_quote
    )
    ON CONFLICT (symbol, interval, open_time)
    DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        quote_volume = EXCLUDED.quote_volume,
        num_trades = EXCLUDED.num_trades,
        close_time = EXCLUDED.close_time,
        taker_buy_base = EXCLUDED.taker_buy_base,
        taker_buy_quote = EXCLUDED.taker_buy_quote;
""")

def upsert_batch(records):
    with engine.begin() as conn:
        conn.execute(UPSERT_SQL, records)

# ================= CONSUMER =================
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# ================= MAIN =================
if __name__ == "__main__":
    logger.info("Starting Kafka Consumer")
    ensure_table(engine, TABLE)

    buffer = []

    try:
        for message in consumer:
            record = message.value

            # ---- enrich missing fields ----
            record["symbol"] = SYMBOL
            record["interval"] = INTERVAL

            # ---- timestamp conversion ----
            record["open_time"] = datetime.fromisoformat(record["open_time"])
            record["close_time"] = datetime.fromisoformat(record["close_time"])

            # ---- schema validation ----
            if not REQUIRED_FIELDS.issubset(record):
                logger.error(f"Invalid record skipped: {record}")
                continue

            buffer.append(record)

            # ---- batch processing ----
            if len(buffer) >= BATCH_SIZE:
                upsert_batch(buffer)
                consumer.commit()
                logger.info(f"Inserted {len(buffer)} records into Postgres")
                buffer.clear()

    except KeyboardInterrupt:
        logger.info("Shutdown requested")

    except Exception:
        logger.exception("Unexpected consumer error")

    finally:
        if buffer:
            upsert_batch(buffer)
            consumer.commit()
            logger.info(f"Flushed remaining {len(buffer)} records")

        consumer.close()
        logger.info("Consumer closed cleanly") 