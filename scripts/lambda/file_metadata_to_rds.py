import json
import os
import boto3
import psycopg2
from psycopg2.extras import execute_values
from urllib.parse import unquote_plus
 
sqs= boto3.client('sqs')
url= os.getenv("QUEUE_URL")
step_client = boto3.client('stepfunctions')
STEP_ARN=os.getenv("STEP_ARN")
 
def trigger_stepfun(step_client, conn):
    with conn.cursor() as cur:
        q="""select id from public.intrn_purva_file_reg where state = 'Running'"""
        cur.execute(q)
        res=cur.fetchall()
        if res:
            print("Res:",res)
            print("Running SFN")
        else:
            q="""select id from public.intrn_purva_file_reg where state = 'Registered'"""
            cur.execute(q)
            res=cur.fetchall()
            print("Response for registered:",res)
            if res==[]:
                print("No entries to registered no need sfn")
            else:
                sfn=step_client.start_execution(
                    stateMachineArn=STEP_ARN,
                    input=json.dumps({
                        "subject": "Start of Job",
                        "message": "Processing started successfully"
                    })
                )
                print("Started SFN")
   
   
 
def lambda_handler(event, context):
    job_id = context.aws_request_id
    print("JobID ", job_id)
    session = boto3.session.Session()
    rds_client = session.client(
        service_name="secretsmanager", region_name=os.getenv("AWS_REGION_DEFAULT")
    )
    secret = rds_client.get_secret_value(SecretId=os.getenv("SECRET_KEY_NAME"))
    secret_dict = json.loads(secret["SecretString"])
    # print(secret_dict)
    rds_username = secret_dict["username"]
    rds_password = secret_dict["password"]
    rds_host = secret_dict["host"]
    rds_port = secret_dict["port"]
    rds_dbname = secret_dict["dbname"]
    flag=True
    try:
        conn = psycopg2.connect(host=rds_host, user=rds_username, password=rds_password, dbname=rds_dbname, port=rds_port)
        print("connected")
    except Exception as e:
        print(e)
    while True:
        response = sqs.receive_message(
            QueueUrl=url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=0
        )
 
        if 'Messages' in response:
            try:
                row = []
                rh = []
                for msg in response['Messages']:
                    data = json.loads(msg['Body'])
                    print(msg['Body'])
                    print(type(msg['Body']))
                    if 'Records' in data:
                        for record in data['Records']:
                            bucket = record['s3']['bucket']['name']
                            key_path = record['s3']['object']['key']
                            key_path= unquote_plus(key_path)
                            file = key_path.split('/')[-1]
                            file_ext=file.split('.')[-1]
                            file_size = record['s3']['object']['size']
                            owner_acc = record['userIdentity']['principalId'].split(':')[-1]
                            region = record['awsRegion']
                            created = record['eventTime']
 
                            rec=(owner_acc, bucket, region, key_path, file, file_size, file_ext, created, "Registered", job_id)
                            row.append(rec)
                    rh.append(msg['ReceiptHandle'])
                    print("Row: ", row)
                    print("rh: ",rh)
 
            except Exception as e:
                print(e)
                flag=False
            with conn.cursor() as cur:
                if flag:
                    query=""" insert into public.intrn_purva_file_reg (owner_acc, bucket, region, key_path, file, file_size, file_ext, created, state, job_id) values %s"""
                    if row:
                        execute_values(cur, query, row)
                    conn.commit()
                    for i in rh:
                        sqs.delete_message(QueueUrl=url, ReceiptHandle=i)  
        else:
            print("No messages")
            break
    trigger_stepfun(step_client, conn)
 
    conn.close()
    return {'statusCode': 200, 'body': 'Success'}