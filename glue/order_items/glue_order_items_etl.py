import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

INPUT_PATH = "s3://ruben-aws-snowflake/raw/order_items/order_items.csv"
OUTPUT_PATH = "s3://ruben-aws-snowflake/processed/order_items/"

df = spark.read.option("header", "true").csv(INPUT_PATH)

df_clean = (
    df
    .filter(col("order_id").isNotNull())
    .filter(col("product_id").isNotNull())
    # PK lógica compuesta (order_id, product_id)
    .dropDuplicates(["order_id", "product_id"])
    .withColumn("order_id", col("order_id").cast("string"))
    .withColumn("product_id", col("product_id").cast("string"))
    .withColumn("unit_price_usd", col("unit_price_usd").cast("double"))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("line_total_usd", col("line_total_usd").cast("double"))
)

# Basic sanity checks
df_clean = df_clean.filter(
    (col("quantity") >= 1) &
    (col("unit_price_usd") >= 0) &
    (col("line_total_usd") >= 0)
)

df_clean.write.mode("overwrite").parquet(OUTPUT_PATH)

job.commit()
