import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from scrapper_impl.scrapper_grab import GrabScrapper
from scrapper_impl.scrapper_tiket import TiketScrapper
import pandas as pd
from etl_pandas_bquery import PandasToBigQueryPipeline

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG(
    'etl_pandas_to_bq',
    default_args=args,
    description="ETL Pipeline to extract data from Company's career page to Google BigQuery",
    schedule_interval='0 2 * * *',
    catchup=False
)

def extract_grab(**kwargs):
    logging.info("Starting the extraction process from Grab.")
    scraper = GrabScrapper(
        url='https://www.grab.careers/en/jobs/?orderby=0&pagesize=20&page=1&team=Engineering',
        title='a',
        location='ul',
        tags='ul'
    )
    df = scraper.extract()
    kwargs['ti'].xcom_push(key='extracted_data_grab', value=df.to_dict())
    logging.info("Extraction process from Grab completed.")


def extract_tiket(**kwargs):
    logging.info("Starting the extraction process from Tiket.")
    scraper = TiketScrapper(
        url='https://careers.tiket.com/jobs',
        title='h5',
        location='ul',
        tags='span'
    )
    df = scraper.scrape()
    kwargs['ti'].xcom_push(key='extracted_data_tiket', value=df.to_dict())
    logging.info("Extraction process from Tiket completed.")

def transform(**kwargs):
    logging.info("Starting the transformation process.")
    ti = kwargs['ti']
    df_grab_dict = ti.xcom_pull(key='extracted_data_grab', task_ids='extract_grab_task')
    df_tiket_dict = ti.xcom_pull(key='extracted_data_tiket', task_ids='extract_tiket_task')
    
    df_grab = pd.DataFrame(df_grab_dict)
    df_tiket = pd.DataFrame(df_tiket_dict)
    
    combined_df = pd.concat([df_grab, df_tiket], ignore_index=True)
    
    pipeline = PandasToBigQueryPipeline(
        dest_table='jobs_henry.job_db', 
        cred_path='bq-etl-task.json'
    )
    
    transformed_df = pipeline.transform(combined_df)
    ti.xcom_push(key='transformed_data', value=transformed_df.to_dict())
    logging.info("Transformation process completed.")

def load(**kwargs):
    logging.info("Starting the loading process.")
    ti = kwargs['ti']
    transformed_df_dict = ti.xcom_pull(key='transformed_data', task_ids='transform_task')
    transformed_df = pd.DataFrame(transformed_df_dict)
    
    pipeline = PandasToBigQueryPipeline(
        dest_table='jobs_henry.job_db', 
        cred_path='bq-etl-task.json'
    )
    
    pipeline.load(transformed_df)
    logging.info("Loading process completed.")

extract_grab_task = PythonOperator(
    task_id='extract_grab_task',
    python_callable=extract_grab,
    provide_context=True,
    dag=dag,
)

extract_tiket_task = PythonOperator(
    task_id='extract_tiket_task',
    python_callable=extract_tiket,
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

[extract_grab_task, extract_tiket_task] >> transform_task >> load_task