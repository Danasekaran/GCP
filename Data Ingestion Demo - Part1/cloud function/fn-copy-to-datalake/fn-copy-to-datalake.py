import functions_framework
from google.cloud import pubsub_v1
from google.cloud import storage
import pandas as pd
import datetime
from datetime import datetime
import google.cloud.datastore as datastore
from google.cloud import bigquery

def file_sanity_check(bucket_name,file_name):
    storage_client = storage.Client()
    # client = datastore.Client()
    # query = client.query(kind="file-sanity-metadata")
    # query = query.add_filter("src_file_name", "=", file_name)
    
    # l = query.fetch()

    ###################### fetch ingestion file details from  job_detail table in bq###############################

    bq_client1 = bigquery.Client(project='abc-services-limited')
    file_metadata_query = """select * from `abc-services-limited.ds_metadata_info.file_metadata_info` where upper(src_file_name)=""" + "'"+file_name.upper() +"'"
    get_job_detail = bq_client1.query(file_metadata_query)
    data = get_job_detail.result()
    rows = list(data)
    ##################################################################################################################

    l = list(rows)
    print(l)
    print(file_name)
    print(file_metadata_query)
    if not l:
        print("No result is returned")
    else:  
        d = dict(l[0])
        expected_list=d['exp_columns']
        print(d['exp_columns'])
        
    bucket = storage_client.bucket(bucket_name)

    df = pd.read_csv("gs://"+bucket_name+"/"+file_name)
    heading=df.columns

    # expected_list=['Postcode']
    recevived_list=df.columns.tolist()
    print(expected_list)
    print(recevived_list)
    print(','.join(recevived_list))
    
    if (str(expected_list).upper().strip()==str(','.join(recevived_list)).upper().strip()):
        return True
    else:
        return False



# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def copy_to_datalake(cloud_event):
    data = cloud_event.data
    event_id = cloud_event["id"]
    event_type = cloud_event["type"]
    bucket_name = data["bucket"]
    file_name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]


    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket_name}")
    print(f"File: {file_name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")
    print(data)
    if file_sanity_check(bucket_name,file_name)==True:

        #copy file to datalake and delete copied file
        storage_client = storage.Client()
        source_bucket = storage_client.bucket(bucket_name)
        source_blob = source_bucket.blob(file_name)
        updated=updated[0:19]
        datetime_obj=datetime.strptime(updated, '%Y-%m-%dT%H:%M:%S')
        storage_client_trg = storage.Client()
        destination_bucket = storage_client_trg.bucket('abc-ltd-datalake')

        # copy to new destination
        new_file_name1=file_name.split('.')
        new_file_name=new_file_name1[0]
        new_file_type=new_file_name1[1]

        trg_bucket_folder=str(datetime_obj.year)+str(datetime_obj.month)+str(datetime_obj.day)
        
        new_file_name_with_timestamp=new_file_name+'-'+str(datetime_obj.year)+str(datetime_obj.month)+str(datetime_obj.day)+str(datetime_obj.hour)+str(datetime_obj.minute)+str(datetime_obj.second)+'.'+new_file_type
        #new_blob = source_bucket.copy_blob(source_blob, destination_bucket, str(datetime_obj.year)+'/'+str(datetime_obj.month)+'/'+str(datetime_obj.day)+'/'+new_file_name2)
        new_blob = source_bucket.copy_blob(source_blob, destination_bucket, new_file_name+'/'+trg_bucket_folder+'/'+new_file_name_with_timestamp)

        # Insert row in datastore job_exec_detail table for further use
        # obj_datastore = datastore.Client()
        
        # incomplete_key = obj_datastore.key("job_exec_detail")
        # task = datastore.Entity(key=incomplete_key)
        # task.update(
        #     {
        #         "file_date": updated,
        #         "src_file_name":file_name,
        #         "trg_file_name": new_file_name_with_timestamp,
        #         "file_path": new_file_name+'/'+trg_bucket_folder+'/',
        #         "is_ingested_to_stg": False,
        #         "is_ingested_to_raw": False,
        #         "is_ingested_to_curated": False,
        #     }
        # )
        # obj_datastore.put(task)

        a= new_file_name+'/'+trg_bucket_folder+'/'

##############################log file copy status into tbl_job_exec_detail_new table for further use.##################################
        status_query="""INSERT INTO abc-services-limited.ds_metadata_info.tbl_job_exec_detail_new
        (pipeline_step,PIPELINE_NAME,src_file_date,src_file_path,src_file_name,ingested_date,is_ingested,Process_Step) 
        VALUES ('Move file to Lake - Step 1/4','fn-copy-to-datalake',"""+"'"+updated+"'"+','+"'"+ a+"'"+','+"'"+new_file_name_with_timestamp+"',"+"""CURRENT_DATETIME,True,1)"""

        print(status_query)
        bq_client1 = bigquery.Client()
        query_job1 = bq_client1.query(status_query)
        query_job1.result()  #Waits for the query to finish
#################################################################################################################################
        source_blob.delete()
        
        # construct mail and call pubsub
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path('abc-services-limited', 'topic-trigger-notification')

        data_str = "Dear Data Owner,<br>Greetings...<br>Thank you for sending feed for us.<br><br>"
        data_str = data_str + "Here is file sanity status,<br>"
        data_str = data_str + "<b>Status: <font color='green'>Success</b></font><br>" 
        data_str = data_str + "Source file <b>'"+file_name +"'</b> is valid to ingest the data.<br>"
        data_str = data_str + "This file will be ingested in next schedule.<br><b>"
        data_str = data_str + "<b>Thank you,<br>Data Ingestion Team<br>ABC Services Limited</b>"

        data = data_str.encode("utf-8")
        future = publisher.publish( topic_path, data, origin="Ingest-customer-dim-data", username="Dana" )

    else:

        #copy file to datalake and delete copied file
        storage_client = storage.Client()
        source_bucket = storage_client.bucket(bucket_name)
        source_blob = source_bucket.blob(file_name)
        updated=updated[0:19]
        datetime_obj=datetime.strptime(updated, '%Y-%m-%dT%H:%M:%S')
        storage_client_trg = storage.Client()
        destination_bucket = storage_client_trg.bucket('src-sanity-failed-files')

        # copy to new destination
        new_file_name1=file_name.split('.')
        new_file_name=new_file_name1[0]
        new_file_type=new_file_name1[1]

        new_file_name2=new_file_name+'-'+str(datetime_obj.year)+str(datetime_obj.month)+str(datetime_obj.day)+str(datetime_obj.hour)+str(datetime_obj.minute)+str(datetime_obj.second)+'.'+new_file_type
        new_blob = source_bucket.copy_blob(source_blob, destination_bucket, new_file_name+'/'+new_file_name2)
        source_blob.delete()


        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path('abc-services-limited', 'topic-trigger-notification') 
        data_str = "Dear Data Owner,<br>Greetings...<br>Thank you for sending feed for us.<br><br>"
        data_str = data_str + "Here is file sanity status,<br>"
        data_str = data_str + "<b>Status: <font color='red'>Failure</b></font><br>" 
        data_str = data_str + "Source file <b>'"+file_name +"'</b> you sent us is not matching with the expected columns.<br>"
        data_str = data_str + "Please send us valid source file to ingest the data.<br><b>"
        data_str = data_str + "<b>Thank you,<br>Data Ingestion Team<br>ABC Services Limited</b>"
        data = data_str.encode("utf-8")
        future = publisher.publish( topic_path, data, origin="Ingest-customer-dim-data", username="Dana" )
