import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, trim, upper, to_date

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---- Paths (raw -> processed)
INPUT_PATH = "s3://ruben-aws-snowflake/raw/customers/customers.csv"
OUTPUT_PATH = "s3://ruben-aws-snowflake/processed/customers/"

# ---- Read raw CSV
df = spark.read.option("header", "true").csv(INPUT_PATH)

# ---- Clean + cast
df_clean = (
    df
    .filter(col("customer_id").isNotNull())
    .dropDuplicates(["customer_id"])
    .withColumn("customer_id", col("customer_id").cast("string"))
    .withColumn("name", trim(col("name")))
    .withColumn("email", trim(col("email")))
    .withColumn("country", upper(trim(col("country"))))
    .withColumn("age", col("age").cast("int"))
    .withColumn("signup_date", to_date(col("signup_date"), "M/d/yyyy"))
    .withColumn("marketing_opt_in", col("marketing_opt_in").cast("boolean"))
)

# ---- Write curated Parquet
df_clean.write.mode("overwrite").parquet(OUTPUT_PATH)

job.commit()
