import sys
import json
import boto3
import psycopg2
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, lit
# from pyspark.sql.types import StructType, StructField
 
 
 
 
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'AWS_REGION_DEFAULT', 'SECRET_KEY_NAME', 's3_key', 'bucket', 'name', 'table_id', 'table_nm', 'database_nm', 'scd_type', 'id'
])
 
 
AWS_REGION_DEFAULT = args['AWS_REGION_DEFAULT']
SECRET_KEY_NAME = args['SECRET_KEY_NAME']
s3_key = args['s3_key']
bucket = args['bucket']
name = args['name']
table_id = args['table_id']
table_id = int(table_id)
table_nm = args['table_nm']
database_nm =args['database_nm']
scd_type = args['scd_type']
id = int(args['id'])
path = "s3://"+bucket+"/"+s3_key
print(path)
 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
 
 
 
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
 
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{bucket}/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
 
 
type_mapping = {
    "string": StringType(),
    "long": LongType(),
    "integer": IntegerType(),
    "double": DoubleType(),
    "date": DateType(),
    "timestamp": TimestampType()
}
try:
    print(psycopg2.__version__)
    session = boto3.session.Session()
 
    client = session.client(
        service_name="secretsmanager",
        region_name=AWS_REGION_DEFAULT
    )
 
    secret = client.get_secret_value(
        SecretId=SECRET_KEY_NAME
    )
 
    secret_dict = json.loads(secret['SecretString'])
 
    conn = psycopg2.connect(
        host=secret_dict["host"],
        user=secret_dict["username"],
        password=secret_dict["password"],
        dbname=secret_dict["dbname"],
        port=secret_dict["port"]
    )
    # spark.sql("DROP TABLE glue_catalog.dev_kna_iceberg_intern_purva.cast")
    # spark.sql("DROP TABLE glue_catalog.dev_kna_iceberg_intern_purva.movies")
    # spark.sql("DROP TABLE glue_catalog.dev_kna_iceberg_intern_purva.reviews")
    # spark.sql("DROP TABLE glue_catalog.dev_kna_iceberg_intern_purva.crew")
    # spark.sql("DROP TABLE glue_catalog.dev_kna_iceberg_intern_purva.genres")
    # print(s3_key, bucket, database_nm, table_id, table_nm, name)
    with conn.cursor() as cur:
        q="""SELECT col_nm, data_type, nullable, primary_key_ind, scd_col_ind
                FROM public.intrn_purva_attribute
                WHERE table_id = %s
                ORDER BY ordinal_pos"""
        cur.execute(q, (table_id,))
        rows = cur.fetchall()
       
        q="""SELECT header_row, delim_cd,  file_encd
            FROM public.intrn_purva_entity
            WHERE table_id = %s """
        cur.execute(q, (table_id,))
        entity_info = cur.fetchone()
       
        fields = []
        pk_cols = []
        non_pk = []
        scd_cols = []
        for r in rows:
            col = r[0]
            dtype = r[1].lower()
            spark_type = type_mapping.get(dtype, StringType())
            nullable = r[2]
            fields.append(
                StructField(col, spark_type, nullable)
            )
           
            pk=r[3]
            if pk==True:
                pk_cols.append(r[0])
            else:
                non_pk.append(r[0])
               
            scd=r[4]
            if scd==True:
                scd_cols.append(r[0])
       
        schema = StructType(fields)
        print("Primary key: ", pk_cols)
        print("Non_pk: ", non_pk)
        print("scd col:", scd_cols)
       
        header_row=entity_info[0]
        delim_cd = entity_info[1]
        file_encd= entity_info[2]
       
        # print(schema, "schema")
        # print("header", header_row)
        # print("delim_cd", delim_cd)
        # print("encd", file_encd)
       
        df = spark.read \
                .option("header", header_row) \
                .option("delimiter", delim_cd) \
                .option("quote", '"') \
                .option("escape", '"') \
                .option("multiLine", True) \
                .option("encoding", file_encd) \
                .schema(schema) \
                .csv(path)
       
        print("--------------------------------")
       
        # df = df.dropDuplicates(pk_cols)
        df.show(5)
        if scd_type == "2":
            print("Type 2")
            df = df.withColumn("effective_date", current_timestamp()) \
                  .withColumn("end_date", lit(None).cast("timestamp")) \
                  .withColumn("is_current", lit(True)   )
        df.createOrReplaceTempView("staging")
        table = f"glue_catalog.{database_nm}.{table_nm}"
        if not spark.catalog.tableExists(table):
            df.writeTo(table) \
              .tableProperty("format-version", "2") \
              .create()
            print("table created")
       
        pk_condition = " AND ".join([f"t.{c}=s.{c}" for c in pk_cols])
        print("pk_condition: ",pk_condition)
        if scd_type == "1":
            print("Type 1")
            merge_sql = f"""
            MERGE INTO {table} t
            USING staging s
            ON {pk_condition}
           
            WHEN MATCHED THEN
             UPDATE SET *
           
            WHEN NOT MATCHED THEN
             INSERT *
            """
            spark.sql(merge_sql)
           
        elif scd_type == "2":
            print("Type 2")
            df.show(10)
            # change_cond = " OR ".join([f"t.{c} <> s.{c}" for c in non_pk])
            # change_cond = " OR ".join([f"t.{c} <> s.{c}" for c in scd_cols])
            change_cond = " OR ".join([f"t.{c} <> s.{c} OR (t.{c} IS NULL AND s.{c} IS NOT NULL) OR (t.{c} IS NOT NULL AND s.{c} IS NULL)" for c in scd_cols])
            print("change_cond: ",change_cond)
            # merge_sql = f"""
            # MERGE INTO {table} t
            # USING staging s
            # ON {pk_condition} AND t.is_current = true
           
            # WHEN MATCHED AND ({change_cond})
            #  THEN UPDATE SET
            #   t.end_date = current_timestamp(),
            #   t.is_current = false
           
            # WHEN NOT MATCHED
            #  THEN INSERT *
            # """
            # spark.sql(merge_sql)
            expire_sql = f"""
            MERGE INTO {table} t
            USING staging s
            ON {pk_condition} AND t.is_current = true
           
            WHEN MATCHED AND ({change_cond})
            THEN UPDATE SET
            t.end_date = current_timestamp(),
            t.is_current = false
            """
            spark.sql(expire_sql)
           
            insert_sql = f"""
            INSERT INTO {table}
            SELECT
            s.*
            FROM staging s
            LEFT JOIN {table} t
            ON {pk_condition} AND t.is_current = true
            WHERE {" AND ".join([f"t.{c} IS NULL" for c in pk_cols])}
            """
            spark.sql(insert_sql)
        q="""delete from public.intrn_purva_file_reg where id =%s"""
        cur.execute(q,(id,))
        conn.commit()
        print("Data written to Iceberg successfully")
   
 
except Exception as e:
    print("Error")
    print("Error: ", e)
    raise e
 
 
 
 
job.commit()