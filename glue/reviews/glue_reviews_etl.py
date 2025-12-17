import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, trim, to_timestamp

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

INPUT_PATH = "s3://ruben-aws-snowflake/raw/reviews/reviews.csv"
OUTPUT_PATH = "s3://ruben-aws-snowflake/processed/reviews/"

df = spark.read.option("header", "true").csv(INPUT_PATH)

df_clean = (
    df
    .filter(col("review_id").isNotNull())
    .filter(col("order_id").isNotNull())
    .filter(col("product_id").isNotNull())
    .filter(col("review_time").isNotNull())
    .dropDuplicates(["review_id"])
    .withColumn("review_id", col("review_id").cast("string"))
    .withColumn("order_id", col("order_id").cast("string"))
    .withColumn("product_id", col("product_id").cast("string"))
    .withColumn("rating", col("rating").cast("int"))
    .withColumn("review_text", trim(col("review_text")))
    .withColumn("review_time", to_timestamp(col("review_time"), "yyyy-MM-dd'T'HH:mm:ss"))
)

# Basic sanity check: rating 1..5
df_clean = df_clean.filter((col("rating") >= 1) & (col("rating") <= 5))

df_clean.write.mode("overwrite").parquet(OUTPUT_PATH)

job.commit()