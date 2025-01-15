from pyspark.sql import SparkSession
import sys
import base64

# Validate command-line arguments
if len(sys.argv) < 5:
    raise ValueError("Insufficient arguments provided.")

# GoogleBigQuery JDBC connection parameters
bqSA = sys.argv[1]
project_id = sys.argv[2]
dataset = sys.argv[3]
table = sys.argv[4]

table_id = f"{project_id}.{dataset}.{table}"
encoded_string = base64.b64encode(bqSA.encode('utf-8')).decode('utf-8')

# Initialize Spark Session
spark = SparkSession.builder \
        .appName("GoogleBigQuery JDBC Read") \
        .getOrCreate()

# Read data from GoogleBigQuery using JDBC
df = spark.read \
    .format("bigquery") \
    .option("table", table_id) \
    .option("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .option("credentials", encoded_string) \
    .option("parentProject", project_id) \
    .load()

df.show()
spark.stop()