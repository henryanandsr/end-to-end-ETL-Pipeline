from google.oauth2.credentials import Credentials
from base.pipeline import GenericPipelineInterface
from google.oauth2 import service_account
from google.cloud import bigquery

import pandas as pd
import logging

logging.basicConfig(
    filename='../logs/ingestion.log',
    filemode='a',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

class PandasToBigQueryPipeline(GenericPipelineInterface):
    def __init__(self, dest_table: str, cred_path: str):
        self.credentials = service_account.Credentials.from_service_account_file(cred_path)
        self.client = bigquery.Client(credentials=self.credentials)
        self.dest_table = dest_table
    
    def extract(self) -> pd.DataFrame:
        """
        The extract method implemented in scrapper
        """
        pass
        
    def transform(self, extracted_result: pd.DataFrame) -> pd.DataFrame:
        logging.info("Transforming data")
        transformed_df = extracted_result.dropna()
        logging.info(f"Transformed data, resulting in {len(transformed_df)} records")
        return transformed_df
        
    def load(self, transformed_result: pd.DataFrame):
        logging.info(f"Loading data into BigQuery table {self.dest_table}")
        table_ref = self.client.dataset(self.dest_table.split('.')[0]).table(self.dest_table.split('.')[1])
        job = self.client.load_table_from_dataframe(transformed_result, table_ref)
        job.result()
        logging.info(f"Loaded {job.output_rows} rows into {self.dest_table}")
    
    def run(self, df: pd.DataFrame):
        transformed_df = self.transform(df)
        self.load(transformed_df)