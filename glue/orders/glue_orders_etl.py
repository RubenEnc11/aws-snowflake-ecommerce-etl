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

INPUT_PATH = "s3://ruben-aws-snowflake/raw/orders/orders.csv"
OUTPUT_PATH = "s3://ruben-aws-snowflake/processed/orders/"

df = spark.read.option("header", "true").csv(INPUT_PATH)

df_clean = (
    df
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .dropDuplicates(["order_id"])
    .withColumn("order_id", col("order_id").cast("string"))
    .withColumn("customer_id", col("customer_id").cast("string"))
    # ISO 8601 like 2025-01-31T23:07:42
    .withColumn("order_time", to_timestamp(col("order_time"), "yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("payment_method", lower(trim(col("payment_method"))))
    .withColumn("discount_pct", col("discount_pct").cast("int"))
    .withColumn("subtotal_usd", col("subtotal_usd").cast("double"))
    .withColumn("total_usd", col("total_usd").cast("double"))
    .withColumn("country", upper(trim(col("country"))))
    .withColumn("device", lower(trim(col("device"))))
    .withColumn("source", lower(trim(col("source"))))
)

# Basic sanity checks
df_clean = df_clean.filter(
    (col("subtotal_usd") >= 0) &
    (col("total_usd") >= 0) &
    (col("discount_pct") >= 0) & (col("discount_pct") <= 100) &
    (col("order_time").isNotNull())
)

df_clean.write.mode("overwrite").parquet(OUTPUT_PATH)

job.commit()