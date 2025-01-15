"""
from pyspark.sql import SparkSession
import sys

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("QueryTables") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("fs.azure.account.key.adlssynapseeus01.dfs.core.windows.net", "WexnrEjSijsk52WSFtx8nvojXcJ+Y5Pj4XwD1rAogBszPkFGka8U27xtO+a5OZRrV1OdFKzIZM7V+ASthe/x9g==") \
    .getOrCreate()

# Retrieve table path and format type from arguments
table_path = sys.argv[1]
format_type = sys.argv[2]

# Read and query data based on format
if format_type.lower() == "delta":
    df = spark.read.format("delta").load(table_path)
elif format_type.lower() == "iceberg":
    df = spark.read.format("iceberg").load(table_path)
elif format_type.lower() == "parquet":
    df = spark.read.format("parquet").load(table_path).limit(1)    
else:
    raise ValueError(f"Unsupported format type: {format_type}")

df.show()

spark.stop()

"""

from pyspark.sql import SparkSession
import sys

# Validate command-line arguments
if len(sys.argv) < 6:
    raise ValueError("Insufficient arguments provided.")

# Retrieve arguments
table_path = sys.argv[1]
format_type = sys.argv[2]
token = sys.argv[3]
#expiry = int(sys.argv[4])  # Ensure expiry is an integer
expiry = sys.argv[4]
storageAccount = sys.argv[5]
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("QueryTables") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.jars", "/opt/spark/jars/my-custom-token-provider.jar") \
    .config("fs.azure.account.auth.type", "Custom") \
    .config(f"fs.azure.account.oauth.provider.type.{storageAccount}", "CustomTokenProvider") \
    .config("fs.azure.account.custom.token", token) \
    .config("fs.azure.account.custom.token.expiry", expiry) \
    .getOrCreate()

# Read and query data based on format
if format_type.lower() == "delta":
    df = spark.read.format("delta").load(table_path).limit(4)
elif format_type.lower() == "iceberg":
    df = spark.read.format("iceberg").load(table_path).limit(5)
elif format_type.lower() == "parquet":
    df = spark.read.format("parquet").load(table_path).limit(1)   
else:
    raise ValueError(f"Unsupported format type: {format_type}")

df.show()

spark.stop()
