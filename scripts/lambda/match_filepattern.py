import json
import os
import boto3
import psycopg2
from unzip_code import download, unzip_and_upload, upload, archive, folder_name
from connection import postgres_conn
 
 
sqs= boto3.client('sqs')
url= os.getenv("url")
# SNS_TOPIC_ARN=os.getenv("SNS_TOPIC_ARN")
AWS_REGION_DEFAULT=os.getenv("AWS_REGION_DEFAULT")
SECRET_KEY_NAME= os.getenv("SECRET_KEY_NAME")
def update_state(conn, id, state, job_id):
    with conn.cursor() as cur:
        q="""update public.intrn_purva_file_reg set state=%s, last_updated=now(), job_id=%s where id=%s"""
        cur.execute(q,(state,job_id, id))
        conn.commit()
 
def delete_state(conn, id):
    with conn.cursor() as cur:
        q="""delete from public.intrn_purva_file_reg where id =%s"""
        cur.execute(q,(id,))
        conn.commit()
def lambda_handler(event, context):
    print(event)
    print(context)
    job_id=context.aws_request_id
    entries=[]
    rds_username, rds_password, rds_host, rds_port, rds_dbname=postgres_conn(AWS_REGION_DEFAULT, SECRET_KEY_NAME)
    # rds_username="wrong"
    # print(rds_username)
    flag=True
    try:
        conn = psycopg2.connect(host=rds_host, user=rds_username, password=rds_password, dbname=rds_dbname, port=rds_port)
        print("connected")
    except Exception as e:
        print(e)
        raise e
   
    try:
        with conn.cursor() as cur:
            q="""select * from public.intrn_purva_file_reg where state='Registered' limit 100 for update skip locked"""
            cur.execute(q)
            data=cur.fetchall()
            if data==[]:
                types=event.get('type')
                print("type: ", types)
                if types=="jobstart":
                    print("No files")
                    print(data)
                    return {"entries":[], "message":"No entries to process in DB. Thus, Job ended", "type":"emptyentries", "subject":"End of Job"}
                else:
                    print("Not started from startjob process")
                    return {"entries":[], "message":"Incorrect input passed from StartJob Process", "type":"wronginput", "subject":"Job not initiated correctly"}
            for files in data:
                id=files[0]
                bucket=files[2]
                path=files[4]
                file=files[5]
                size=files[6]
                time=files[8].strftime("%Y%m%d%H%M%S")
                name, subfolder= folder_name(file, time)
                update_state(conn, id, "Running", job_id)
                if name=="" or name==[]:
                    name="dates"
                q="""select file_pattern, preproc, database_nm, table_nm from public.intrn_purva_entity where %s ~ file_pattern"""
                cur.execute(q,(file,))
                regex_pattern=cur.fetchone()
                if regex_pattern==[] or regex_pattern==None:
                    print("No pattern matched")
                    archive(bucket, path, file, size, name, subfolder)
                    update_state(conn, id, "Discarded", job_id)
                    delete_state(conn,id)
                    # return {"status":"Succeed", "Message":"No regex pattern matched"}
                else:
                    print(regex_pattern, "--------------------------------")
                    regex=regex_pattern[0]
                    preproc=regex_pattern[1]
                    database_nm=regex_pattern[2]
                    table_nm=regex_pattern[3]
                    print("matched")  
                    entries.append((id, bucket, path, file, size, time, preproc, name, subfolder))
                   
            conn.commit()
            conn.close()
            print("Entries: ", entries)
            return {"entries":entries, "type":"hasentries", "database_nm":database_nm, "table_nm":table_nm}
    except Exception as e:
        print(e)
        raise e
 
       