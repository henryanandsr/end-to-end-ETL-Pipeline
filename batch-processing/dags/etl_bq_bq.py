from google.oauth2.credentials import Credentials
from base.pipeline import GenericPipelineInterface
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq
import pandas as pd
import logging

logging.basicConfig(
    filename='ingestion2.log',
    filemode='a',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

class BQtoBQQueryPipeline(GenericPipelineInterface):
    def __init__(self, source_table : str,dest_table: str, cred_path: str):
        self.credentials = service_account.Credentials.from_service_account_file(cred_path)
        self.client = bigquery.Client(credentials=self.credentials)
        self.source_table = source_table
        self.dest_table = dest_table
    
    def extract(self) -> pd.DataFrame:
        query = f"SELECT DISTINCT * FROM `{self.source_table}`"
        query_job = self.client.query(query)
        df = query_job.result().to_dataframe()
        print(df)
        return df
    
    def transform(self, source: pd.DataFrame) -> pd.DataFrame:
        df = source.copy()
        return df

    def load(self, transformed_result: pd.DataFrame):
        existing_records_query = f"SELECT * FROM `{self.dest_table}`"
        existing_records_job = self.client.query(existing_records_query)
        existing_records_df = existing_records_job.result().to_dataframe()

        unique_key = 'title'

        new_records_df = transformed_result[~transformed_result[unique_key].isin(existing_records_df[unique_key])]

        pandas_gbq.to_gbq(
            new_records_df,
            destination_table=self.dest_table,
            project_id='partnatech',
            if_exists='append',
            credentials=self.credentials
        )