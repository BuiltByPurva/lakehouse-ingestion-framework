import json
import psycopg2
from unzip_code import download, unzip_and_upload, upload, archive, folder_name
from exceltocsv import convert
from connection import postgres_conn
import os
 
# sqs= boto3.client('sqs')
# url= os.getenv("url")
SNS_TOPIC_ARN=os.getenv("SNS_TOPIC_ARN")
AWS_REGION_DEFAULT=os.getenv("AWS_REGION_DEFAULT")
SECRET_KEY_NAME= os.getenv("SECRET_KEY_NAME")
 
 
def update_state(conn, id, state, execution_id):
    with conn.cursor() as cur:
        q="""update public.intrn_purva_file_reg set state=%s, last_updated=now(), job_id=%s where id=%s"""
        cur.execute(q,(state, execution_id, id))
        conn.commit()
def get_db_table(conn, file):
    with conn.cursor() as cur:
        q="""select table_id, database_nm, table_nm, scd_type from public.intrn_purva_entity where %s ~ file_pattern"""
        cur.execute(q,(file,))
        res=cur.fetchone()
        table_id=res[0]
        database_nm=res[1]
        table_nm=res[2]
        scd_type=res[3]
        return table_id, database_nm, table_nm, scd_type
 
def delete_state(conn, id):
    with conn.cursor() as cur:
        q="""delete from public.intrn_purva_file_reg where id =%s"""
        cur.execute(q,(id,))
        conn.commit()
def lambda_handler(event, context):
    # TODO implement
    print(event)
    # job_id=context.aws_request_id
   
    rds_username, rds_password, rds_host, rds_port, rds_dbname=postgres_conn(AWS_REGION_DEFAULT, SECRET_KEY_NAME)
    try:
        conn = psycopg2.connect(host=rds_host, user=rds_username, password=rds_password, dbname=rds_dbname, port=rds_port)
        print("connected")
    except Exception as e:
        print(e)
        raise e
    execution_id = event["execution_id"]
    data = event["input"]
    id=data[0]
    bucket=data[1]
    path=data[2]
    file=data[3]
    size=data[4]
    time=data[5]
    preproc=data[6]
    name=data[7]
    subfolder=data[8]
    print(id, " ", bucket, " ", path, " ", file)
    key="Not asssigned"
    table_id = None
    database_nm = None
    table_nm = None
    scd_type = None
    if preproc=="unzip":
        try:
            download(bucket, path, SNS_TOPIC_ARN)
            archive(bucket, path, file, size, name, subfolder)
            key=unzip_and_upload(bucket, path, SNS_TOPIC_ARN, name, subfolder)
            update_state(conn, id, "Ready", execution_id)
            table_id, database_nm, table_nm, scd_type = get_db_table(conn, file)
            print("Done unzippinggggg")
            # delete_state(conn, id)
           
        except Exception as e:
            print(e)
            update_state(conn, id, "Failed", execution_id)
            raise e
           
           
    elif preproc=="na":
        try:
            key=upload(bucket, path, file, size, SNS_TOPIC_ARN, name, subfolder)
            archive(bucket, path, file, size, name, subfolder)
            update_state(conn, id, "Ready", execution_id)
            table_id, database_nm, table_nm, scd_type = get_db_table(conn, file)
            print("Done csv process")
            # delete_state(conn, id)
        except Exception as e:
            print(e)
            update_state(conn, id, "Failed", execution_id)
            raise e
           
 
    elif preproc=="exceltocsv":
        try:
            key=convert(bucket, path, name, subfolder)
            archive(bucket, path, file, size, name, subfolder)
            update_state(conn, id, "Ready", execution_id)
            table_id, database_nm, table_nm, scd_type = get_db_table(conn, file)
            print("Done xl to csv")
            # delete_state(conn, id)
        except Exception as e:
            print(e)
            update_state(conn, id, "Failed", execution_id)
            raise e
       
           
    return {
        'statusCode': 200,
        'body': json.dumps('File processing done'),
        'bucket': bucket,
        'key': key,
        'name': name,
        'table_id': table_id,
        'database_nm': database_nm,
        'table_nm': table_nm,
        'scd_type':scd_type,
        'id':id
    }
 