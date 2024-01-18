from google.cloud import bigquery

# Create a new Google BigQuery client using Google Cloud Platform project
bq_client = bigquery.Client()
job_config = bigquery.QueryJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND,)
job_config.destination = f"{bq_client.project}.ds_raw.tbl_sales"

###################### fetch ingestion file details from  job_detail table in bq###############################3
#
# process one file data at a time. So we can maintain log status on each file basis
# but, we can ingest all the files data at a time also.
#

bq_client1 = bigquery.Client(project='abc-services-limited')
job_detail_query = """select * from `abc-services-limited.ds_metadata_info.tbl_job_exec_detail_new` 
where process_step=2 and src_file_name not in (
select src_file_name  from `abc-services-limited.ds_metadata_info.tbl_job_exec_detail_new` where process_step=3) order by src_file_date asc"""
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
       query = """SELECT 
            SalesOrderLineKey,ResellerKey,CustomerKey,ProductKey,OrderDateKey,DueDateKey,
            ShipDateKey,SalesTerritoryKey,OrderQuantity,UnitPrice,ExtendedAmount,UnitPriceDiscountPct,
            ProductStandardCost,TotalProductCost,SalesAmount,LastModifiedOn,current_timestamp() as Ingestion_Date,File_Name
            FROM ds_stg.tbl_sales where file_name=""" + "'"+d['src_file_name']+"'"
       print(query)
       query_job = bq_client.query(query, job_config=job_config)
       data=query_job.result()  # Waits for the query to finish
       print('total no of record moved :' + str(query_job._query_results.total_rows))
       row_count=query_job._query_results.total_rows

########################## Update the ingestion log into tbl_job_exec_detail table ########################################################3

       bq_client1 = bigquery.Client(project='abc-services-limited')
       status_query="""INSERT INTO abc-services-limited.ds_metadata_info.tbl_job_exec_detail_new
        (rec_count,pipeline_step,PIPELINE_NAME,src_file_date,src_file_path,src_file_name,ingested_date,is_ingested,Process_Step) 
        VALUES ("""+str(row_count)+""",'STG to RAW - Step 3/4','2-move-sales-bq_stg-to-bq_raw',"""+"'"+str(d['src_file_date'])+ \
            "'"+','+"'"+d['src_file_path']+"'"+','+"'"+d['src_file_name']+"'"+','+"""CURRENT_DATETIME,True,3)"""

       print(status_query)
       query_job1 = bq_client1.query(status_query)
       query_job1.result()  #Waits for the query to finish

###################################################################################################################################