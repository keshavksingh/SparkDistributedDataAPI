from pyspark.sql import SparkSession
import sys

# Validate command-line arguments
if len(sys.argv) < 8:
    raise ValueError("Insufficient arguments provided.")

# Databricks JDBC connection parameters
sfURL = sys.argv[1]
sfUser = sys.argv[2]
sfPassword = sys.argv[3]
sfDatabase = sys.argv[4]
sfSchema = sys.argv[5]
sfWarehouse = sys.argv[6]
sfTable = sys.argv[7] 

sf_options = {
        "sfURL": sfURL,
        "sfUser": sfUser,
        "sfPassword": sfPassword,
        "sfDatabase": sfDatabase,
        "sfSchema": sfSchema,
        "sfWarehouse": sfWarehouse
    }

spark = SparkSession.builder \
        .appName("Snowflake JDBC Read") \
        .config("spark.jars", "/opt/spark/jars/spark-snowflake_2.12-2.16.0-spark_3.4.jar") \
        .getOrCreate()

# Initialize Spark Session
df = spark.read \
        .format("net.snowflake.spark.snowflake") \
        .options(**sf_options) \
        .option("dbtable", sfTable) \
        .load()

df.show()
spark.stop()

