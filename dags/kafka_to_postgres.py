from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import psycopg2
from kafka import KafkaConsumer
from pendulum import timezone
import pandas as pd
from sqlalchemy import create_engine

local_tz = timezone("Asia/Manila")


# def consume_and_store():
#     consumer = KafkaConsumer(
#         'sales-topic',
#         bootstrap_servers='kafka:9092',
#         auto_offset_reset='earliest',
#         enable_auto_commit=True,
#         value_deserializer=lambda m: json.loads(m.decode('utf-8'))
#     )
#     conn = psycopg2.connect(
#         dbname='airflow',
#         user='airflow',
#         password='airflow',
#         host='postgres'
#     )
#     cur = conn.cursor()
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS sales (
#             id SERIAL PRIMARY KEY,
#             product TEXT,
#             price NUMERIC,
#             timestamp TIMESTAMP
#         )
#     """)
#     conn.commit()    
#     for msg in consumer.poll(timeout_ms=5000).values():
#         for record in msg:
#             sale = record.value
#             print("Inserting:", sale)
#             cur.execute(
#                 "INSERT INTO sales (product, price, timestamp) VALUES (%s, %s, to_timestamp(%s))",
#                 (sale['product'], sale['price'], sale['timestamp'])
#             )
#     conn.commit()
#     cur.close()
#     conn.close()
#     consumer.close()

def consume_and_store():
    consumer = KafkaConsumer(
        'sales-topic',
        bootstrap_servers='kafka:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")

    records = []
    for msg in consumer.poll(timeout_ms=5000).values():
        for record in msg:
            sale = record.value
            records.append(sale)

    if records:
        df = pd.DataFrame(records)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df.to_sql('sales', engine, if_exists='append', index=False)



default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='kafka_to_postgres',
    default_args=default_args,
    start_date=datetime(2023, 1, 1, tzinfo=local_tz),
    schedule_interval='*/1 * * * *',  # every minute
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='consume_kafka_messages',
        python_callable=consume_and_store,
    )
