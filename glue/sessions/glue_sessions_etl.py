import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, trim, lower, upper, to_timestamp

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

INPUT_PATH = "s3://ruben-aws-snowflake/raw/sessions/sessions.csv"
OUTPUT_PATH = "s3://ruben-aws-snowflake/processed/sessions/"

df = spark.read.option("header", "true").csv(INPUT_PATH)

df_clean = (
    df
    .filter(col("session_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .dropDuplicates(["session_id"])
    .withColumn("session_id", col("session_id").cast("string"))
    .withColumn("customer_id", col("customer_id").cast("string"))
    .withColumn("start_time", to_timestamp(col("start_time"), "yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("device", lower(trim(col("device"))))
    .withColumn("source", lower(trim(col("source"))))
    .withColumn("country", upper(trim(col("country"))))
)

df_clean = df_clean.filter(col("start_time").isNotNull())

df_clean.write.mode("overwrite").parquet(OUTPUT_PATH)

job.commit()