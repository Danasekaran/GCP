#--------Import required modules and packages------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import sum
from pyspark.sql.functions import *
from google.cloud import bigquery
from datetime import datetime

# Create Spark session
spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-ingest-gcs-to-bigquery') \
  .getOrCreate()

# Define temporary GCS bucket for Dataproc to write it's process data
bucket='abc-ltd-datalake/tmp/dataproc'
spark.conf.set('temporaryGcsBucket', bucket)

###################### fetch ingestion file details from  job_detail table in bq###############################3

bq_client1 = bigquery.Client(project='abc-services-limited')
job_detail_query = """select * from `abc-services-limited.ds_metadata_info.tbl_job_exec_detail_new`_new where src_file_name not in (
select src_file_name  from `abc-services-limited.ds_metadata_info.tbl_job_exec_detail_new` where process_step=2) order by src_file_date asc"""
get_job_detail = bq_client1.query(job_detail_query)
data = get_job_detail.result()
rows = list(data)
##################################################################################################################3


lst = list(rows)
if not lst:
    print("No files found to process")
    exit(1)
else:  
    for i in lst:
      d = dict(i)
      print(d['src_file_path'])
      print(d)

      df=spark.read.option("header",True).csv('gs://abc-ltd-datalake/'+d['src_file_path']+d['src_file_name'])

      req_df=df.select(col('SalesOrderLineKey'),col('ResellerKey'),col('CustomerKey'),col('ProductKey'), \
                       col('OrderDateKey'),col('DueDateKey'),col('ShipDateKey'),col('SalesTerritoryKey'), \
                        col('OrderQuantity'),col('UnitPrice'),col('ExtendedAmount'),col('UnitPriceDiscountPct'), \
                          col('ProductStandardCost'),col('TotalProductCost'),col('SalesAmount'),col('LastModifiedOn'),current_timestamp().alias("Ingestion_Date"), \
                            lit(d['src_file_name']).alias("File_Name"))

      # Writing the data to BigQuery
      req_df.write.format('bigquery') \
        .option('table', 'ds_stg.tbl_sales') \
        .option('createDisposition','CREATE_IF_NEEDED') \
        .mode("append").save()

      row_count = req_df.count()
      print(f"DataFrame row count : {row_count}")

########################## Update the ingestion log into tbl_job_exec_detail table ########################################################3
      status_query="""INSERT INTO abc-services-limited.ds_metadata_info.tbl_job_exec_detail_new
      (rec_count,pipeline_step,PIPELINE_NAME,src_file_date,src_file_path,src_file_name,ingested_date,is_ingested,Process_Step) 
      VALUES ("""+str(row_count)+""",'GCS to STG - Step 2/4','1-Ingest-sales-gcs-to-bq_stg',"""+"'"+str(d['src_file_date'])+ \
        "'"+','+"'"+d['src_file_path']+"'"+','+"'"+d['src_file_name']+"'"+','+"""CURRENT_DATETIME,True,2)"""

      print(status_query)
      bq_client1 = bigquery.Client()
      query_job1 = bq_client1.query(status_query)
      query_job1.result()  #Waits for the query to finish
###################################################################################################################################

spark.stop()