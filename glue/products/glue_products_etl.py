import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, trim

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---- Paths (raw -> processed)
INPUT_PATH = "s3://ruben-aws-snowflake/raw/products/products.csv"
OUTPUT_PATH = "s3://ruben-aws-snowflake/processed/products/"

# ---- Read raw CSV
df = spark.read.option("header", "true").csv(INPUT_PATH)

# ---- Clean + cast
df_clean = (
    df
    .filter(col("product_id").isNotNull())
    .dropDuplicates(["product_id"])
    .withColumn("product_id", col("product_id").cast("string"))
    .withColumn("category", trim(col("category")))
    .withColumn("name", trim(col("name")))
    .withColumn("price_usd", col("price_usd").cast("double"))
    .withColumn("cost_usd", col("cost_usd").cast("double"))
    .withColumn("margin_usd", col("margin_usd").cast("double"))
)

# Optional: basic non-negative checks (keeps it simple)
df_clean = df_clean.filter(
    (col("price_usd") >= 0) & (col("cost_usd") >= 0) & (col("margin_usd") >= 0)
)

# ---- Write curated Parquet
df_clean.write.mode("overwrite").parquet(OUTPUT_PATH)

job.commit()