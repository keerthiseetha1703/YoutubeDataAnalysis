# YoutubeDataAnalysis
END TO END DATA ENGINEERING PROJECT using Kaggle YouTube Trending Dataset
# YoutubeDataAnalysis
END TO END DATA ENGINEERING PROJECT using Kaggle YouTube Trending Dataset
Goals :
Data Ingestion
Data Lake 
AWS
ETL Design 
Scalability 
Reporting

CODE: 





Copy files in current folder to s3 bucket
aws s3 cp . s3://de-youtubetrending-raw/youtube/raw_statistics_reference_data/ --recursive --exclude "*" --include "*.json"

Copy data files to their own space
aws s3 cp CAvideos.csv s3://de-youtubetrending-raw/youtube/raw_statistics/region=ca/
aws s3 cp DEvideos.csv s3://de-youtubetrending-raw/youtube/raw_statistics/region=de/
aws s3 cp FRvideos.csv s3://de-youtubetrending-raw/youtube/raw_statistics/region=fr/
aws s3 cp GBvideos.csv s3://de-youtubetrending-raw/youtube/raw_statistics/region=gb/
aws s3 cp INvideos.csv s3://de-youtubetrending-raw/youtube/raw_statistics/region=in/
aws s3 cp JPvideos.csv s3://de-youtubetrending-raw/youtube/raw_statistics/region=jp/
aws s3 cp KRvideos.csv s3://de-youtubetrending-raw/youtube/raw_statistics/region=kr/
aws s3 cp MXvideos.csv s3://de-youtubetrending-raw/youtube/raw_statistics/region=mx/
aws s3 cp RUvideos.csv s3://de-youtubetrending-raw/youtube/raw_statistics/region=ru/
aws s3 cp USvideos.csv s3://de-youtubetrending-raw/youtube/raw_statistics/region=us/


Lambda function

import awswrangler as wr
import pandas as pd
import urllib.parse
import os

os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
os_input_write_data_operation = os.environ['write_data_operation']


def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8’)—file to be read
    try:

        # Creating DF from content
        df_raw = wr.s3.read_json('s3://{}/{}'.format(bucket, key))

        # Extract required columns:
        df_step_1 = pd.json_normalize(df_raw['items’])—normalizes nested json to flattened json

        # Write to S3
        wr_response = wr.s3.to_parquet(
            df=df_step_1,
            path=os_input_s3_cleansed_layer,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_table_name,
            mode=os_input_write_data_operation
        )

        return wr_response
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

Glue-csv to parquet
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

predicate_pushdown = "region in ('ca','gb','us')"

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "de_youtube_raw", table_name = "raw_statistics", transformation_ctx = "datasource0", push_down_predicate = predicate_pushdown)



datasink1 = datasource0.toDF().coalesce(1)
df_final_output = DynamicFrame.fromDF(datasink1, glueContext, "df_final_output")
datasink4 = glueContext.write_dynamic_frame.from_options(frame = df_final_output, connection_type = "s3", connection_options = {"path": "s3://de-youtube-trending-cleaned/youtube/raw_statistics/", "partitionKeys": ["region"]}, format = "parquet", transformation_ctx = "datasink4")

job.commit()
