from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import logging
import sys

sys.path.append('/home/tsabitghazian/airflow_demo/dags/ourplayground/etl-workshop/etl_henry_final')

from etl_bq_bq import BQtoBQQueryPipeline

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1)
}

dag = DAG(
    'bq_to_bq_etl',
    default_args=default_args,
    description='ETL pipeline to move data from one BigQuery table to another',
    schedule_interval=timedelta(days=1),
    catchup=False
)

def extract(**kwargs):
    logging.info("Starting extraction process.")
    etl_pipeline = BQtoBQQueryPipeline(
        source_table='jobs_henry.job_db',
        dest_table='job_dwh_henry.job_db',
        cred_path='/home/tsabitghazian/airflow_demo/dags/ourplayground/etl-workshop/etl_henry_final/bq-etl-task.json'
    )
    df = etl_pipeline.extract()
    kwargs['ti'].xcom_push(key='extracted_data', value=df.to_dict())
    logging.info("Extraction process completed.")

def transform(**kwargs):
    logging.info("Starting transformation process.")
    ti = kwargs['ti']
    df_dict = ti.xcom_pull(key='extracted_data', task_ids='extract_task')
    df = pd.DataFrame(df_dict)
    etl_pipeline = BQtoBQQueryPipeline(
        source_table='jobs_henry.job_db',
        dest_table='job_dwh_henry.job_db',
        cred_path='/home/tsabitghazian/airflow_demo/dags/ourplayground/etl-workshop/etl_henry_final/bq-etl-task.json'
    )
    transformed_df = etl_pipeline.transform(df)
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_df.to_dict())
    logging.info("Transformation process completed.")

def load(**kwargs):
    logging.info("Starting loading process.")
    ti = kwargs['ti']
    transformed_df_dict = ti.xcom_pull(key='transformed_data', task_ids='transform_task')
    transformed_df = pd.DataFrame(transformed_df_dict)
    etl_pipeline = BQtoBQQueryPipeline(
        source_table='jobs_henry.job_db',
        dest_table='job_dwh_henry.job_db',
        cred_path='/home/tsabitghazian/airflow_demo/dags/ourplayground/etl-workshop/etl_henry_final/bq-etl-task.json'
    )
    etl_pipeline.load(transformed_df)
    logging.info("Loading process completed.")

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task