from pyspark.sql import SparkSession
import sys

# Validate command-line arguments
if len(sys.argv) < 5:
    raise ValueError("Insufficient arguments provided.")

# Databricks JDBC connection parameters
personal_access_token = sys.argv[1]  # Replace with your token
jdbc_url = sys.argv[2] #"jdbc:databricks://adb-432006791996276.16.azuredatabricks.net:443/default"
http_path = sys.argv[3] #"/sql/1.0/warehouses/474c2a93e88c3a00"
query = sys.argv[4] #"(select * FROM adbucwusdev01.adbdevschema.salesorder) as DataSet"

# Initialize Spark Session
spark = SparkSession.builder \
        .appName("Databricks JDBC Read") \
        .getOrCreate()

# Read data from Databricks using JDBC
df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", query) \
    .option("user", "token") \
    .option("password", personal_access_token) \
    .option("driver", "com.databricks.client.jdbc.Driver") \
    .option("ssl", "1") \
    .option("AuthMech", "3") \
    .option("httpPath", http_path) \
    .option("UseNativeQuery", "0") \
    .load()

df.show()
spark.stop()