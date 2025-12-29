from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from scripts.ingest import ingest
from scripts.load import load

with DAG(
    'stock_data_pipeline',
    default_args={
        'owner': 'flavio',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='ELT pipeline that ingests stock data, streams them via kafka, and loads data into PostgreSQL',
    schedule=timedelta(minutes=1),
    start_date=datetime(2025, 12, 27),
    catchup=False,
    tags=['stocks', 'elt'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_stock_data',
        python_callable=ingest
    )

    load_task = PythonOperator(
        task_id='load_stock_data',
        python_callable=load
    )

    ingest_task >> load_task