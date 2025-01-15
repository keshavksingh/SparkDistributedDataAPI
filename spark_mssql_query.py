from pyspark.sql import SparkSession
import sys
import urllib

# Validate command-line arguments
if len(sys.argv) < 5:
    raise ValueError("Insufficient arguments provided.")

# MSSQL JDBC connection parameters
server = sys.argv[1]
database_name = sys.argv[2]
table_name = sys.argv[3]
access_token = sys.argv[4]

# Initialize Spark Session
spark = SparkSession.builder \
        .appName("MSSQL JDBC Read") \
        .getOrCreate()

url = f"jdbc:sqlserver://{server}:1433;database={database_name};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;"

encoded_token = urllib.parse.quote_plus(access_token)

df = spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable", table_name) \
    .option("accessToken", encoded_token) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

#df = spark.read \
#        .format("com.microsoft.sqlserver.jdbc.spark") \
#        .option("url", url) \
#        .option("dbtable", table_name) \
#        .option("Authentication", "ActiveDirectoryManagedIdentity") \
#        .option("encrypt", "true") \
#        .option("hostNameInCertificate", "*.database.windows.net") \
#        .load()

#        .option("accessToken", access_token) \

df.show()
spark.stop()
