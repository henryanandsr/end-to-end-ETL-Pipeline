# End to End ETL Pipeline

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
