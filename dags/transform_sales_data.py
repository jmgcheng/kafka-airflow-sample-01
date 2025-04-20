from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from pendulum import timezone
import pandas as pd
from sqlalchemy import create_engine

# def transform_sales_data():
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
#     # Create summary table if it doesn't exist
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS sales_summary (
#             date DATE PRIMARY KEY,
#             total_sales NUMERIC
#         )
#     """)
#     # Aggregate sales from raw data
#     cur.execute("""
#         INSERT INTO sales_summary (date, total_sales)
#         SELECT 
#             DATE(timestamp) AS date,
#             SUM(price) AS total_sales
#         FROM sales
#         GROUP BY DATE(timestamp)
#         ON CONFLICT (date) DO UPDATE SET total_sales = EXCLUDED.total_sales
#     """)
#     conn.commit()
#     cur.close()
#     conn.close()

def transform_sales_data():
    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")

    # Load raw data
    df = pd.read_sql("SELECT * FROM sales", engine)

    if df.empty:
        print("No data to transform.")
        return

    df['date'] = df['timestamp'].dt.date
    df['net_revenue'] = df['price'] * df['quantity'] * (1 - df['discount'])

    summary_df = (
        df.groupby('date')
        .agg(total_sales=('net_revenue', 'sum'), num_orders=('sale_id', 'count'))
        .reset_index()
    )

    summary_df.to_sql('sales_summary', engine, if_exists='replace', index=False)


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

local_tz = timezone("Asia/Manila")

with DAG(
    dag_id='transform_sales_data',
    default_args=default_args,
    start_date=datetime(2023, 1, 1, tzinfo=local_tz),
    # schedule_interval='@daily',
    schedule_interval='*/1 * * * *',  # every minute
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='transform_raw_sales',
        python_callable=transform_sales_data,
    )
