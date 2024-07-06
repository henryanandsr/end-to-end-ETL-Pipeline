# End to End ETL Pipeline

Group 4 - Henry & Ghifari

## Batch Processing
The primary objective of this project is to automate the extraction of job listings from various company job portals using web scraping techniques. The extracted data is then transformed and loaded into Google BigQuery for further analysis and reporting. 

Process:

1. Data Extraction : The first step involves scrapping jobs from various companies job portals. This achieved using BS4 (BeautifulSoup) and Selenium to scrap dynamic website.
2. Data Transformation : Once extracted, data will be transformed into pandas dataframe
3. Data Loading : Transformed data will be load into Google BigQuery, which served as serving layer
4. Second Pipeline : This pipeline will extract data from BigQuery, remove duplicates data then load to another BigQuery dataset
### DAG 1
![batch-processing](/batch-processing/img/DAG1.png)
### DAG 2
![batch-processing](/batch-processing/img/DAG2.png)

## Stream Processing
The primary objective of this project is to automate the real-time extraction of news updates for specific companies using Apache Kafka for stream processing. The streamed data is then transformed and loaded into Google BigQuery

Process:

1. Data Extraction: The first step involves capturing real-time news updates for specific companies using Apache Kafka for stream processing.
2. Data Transformation: Once extracted, the data is transformed into a structured format using Pandas.
3. Data Loading: The transformed data is loaded into Google BigQuery, which serves as the storage layer.
4. Data Consumption: Looker consumes the data from BigQuery for visualization and reporting.

Video explanation for both batch and steam can be accessed through link below:
https://www.youtube.com/watch?v=8UDtQCb3AEY
