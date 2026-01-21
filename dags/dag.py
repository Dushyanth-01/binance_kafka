from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def fetch_transform_load(execution_date: str):
    from pipelines.fetch import fetch_daily, transform, validate
    from pipelines.kafka_producer import send_to_kafka
    
    
    symbol = "BTCUSDT"
    interval = "1d"
    table = "binance_klines"

    print(f"Processing execution_date: {execution_date}")


    raw = fetch_daily(symbol, execution_date)

    if not raw:
        print("No data fetched for this execution date")
        return

    df = transform(raw)
    validate(df)
    send_to_kafka(topic="binance", data= df)

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="binance_klines_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 */2 * * *",
    catchup=True,
    tags=["binance", "crypto"],
) as dag:

    fetch_transform_load_task = PythonOperator(
        task_id="fetch_transform_load",
        python_callable=fetch_transform_load,
        op_kwargs={
            "execution_date": "{{ ds }}"
        },
    )