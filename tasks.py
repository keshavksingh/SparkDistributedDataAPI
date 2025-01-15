import os
import subprocess
import logging
from celery import Celery

redis_host = os.getenv('REDIS_HOST', 'localhost')
redis_password = os.getenv('REDIS_PASSWORD', '')
celery_broker = os.getenv('CELERY_BROKER', f'redis://:{redis_password}@{redis_host}:6379/0')
celery_backend = os.getenv('CELERY_BACKEND', f'redis://:{redis_password}@{redis_host}:6379/0')

# Configure Celery
celery_app = Celery(
    'tasks',
    broker=celery_broker,
    backend=celery_backend
)

#celery_app.conf.update(
#    worker_concurrency=100,  # Maximum of 100 concurrent tasks
#    task_acks_late=True,     # Ensure tasks are acknowledged only after completion
#    worker_prefetch_multiplier=1  # Fetch only one task at a time per worker
#)


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@celery_app.task(bind=True)
def run_spark_query(self, datalakepath: str, format_type: str, token: str, expires_on: int, storageAccount: str):
    try:
        logger.info(f"Starting Spark query on table: {datalakepath} with format: {format_type}")

        # Construct the spark-submit command
        cmd = [
            "/opt/spark/bin/spark-submit",
            #"--packages", "io.delta:delta-core_2.12:2.4.0,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.1",
            #"--jars", "/app/jars/delta-core_2.12-2.4.0.jar,/app/jars/iceberg-spark-runtime-3.4_2.12-1.7.1.jar",
            "--conf", "spark.jars.ivy=/tmp/ivy2",
            "--conf", "spark.executor.extraJavaOptions=-Dother.option=some_value",
            "/app/spark_query.py",
            datalakepath,
            format_type,
            token,
            str(expires_on),
            storageAccount
        ]

        # Execute the command
        result = subprocess.run(cmd, check=True, text=True, capture_output=True)
        logger.info(f"Query completed successfully on datalakepath: {datalakepath}")

        # Return result
        return {"status": "success", "output": result.stdout}
    except subprocess.CalledProcessError as e:
        error_message = f"Error executing Spark query: {e.stderr}"
        logger.error(error_message)
        raise Exception(error_message)
    except Exception as e:
        logger.exception("Unexpected error occurred during Spark query.")
        raise Exception(str(e))

@celery_app.task(bind=True)
def run_spark_databricks_query(self, personal_access_token: str, jdbc_url: str, http_path: str, query: str):
    try:
        logger.info(f"Starting Spark query For Databricks!")

        # Construct the spark-submit command
        cmd = [
            "/opt/spark/bin/spark-submit",
            #"--packages", "com.databricks:databricks-jdbc:2.6.38",
            "--jars", "/app/jars/databricks-jdbc-2.6.40.jar",
            "--conf", "spark.jars.ivy=/tmp/ivy2",
            "--conf", "spark.executor.extraJavaOptions=-Dother.option=some_value",
            "--conf", "spark.logLevel=DEBUG",
            "/app/spark_databricks_query.py",
            personal_access_token,
            jdbc_url,
            http_path,
            query
        ]

        # Execute the command
        result = subprocess.run(cmd, check=True, text=True, capture_output=True)
        logger.info(f"Query completed successfully on Databricks {query}")

        # Return result
        return {"status": "success", "output": result.stdout}
    except subprocess.CalledProcessError as e:
        error_message = f"Error executing Spark query: {e.stderr}"
        logger.error(error_message)
        raise Exception(error_message)
    except Exception as e:
        logger.exception("Unexpected error occurred during Spark query.")
        raise Exception(str(e))

@celery_app.task(bind=True)
def run_spark_snowflake_query(self, sfURL: str, sfUser: str, sfPassword: str, sfDatabase: str, sfSchema: str, sfWarehouse: str, sfTable: str):
    try:
        logger.info(f"Starting Spark query For Snowflake!")
        # Construct the spark-submit command
        cmd = [
            "/opt/spark/bin/spark-submit",
            #"--packages", "net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4",
            "--jars", "/app/jars/spark-snowflake_2.12-2.16.0-spark_3.4.jar,/app/jars/snowflake-jdbc-3.21.0.jar",
            "--conf", "spark.jars.ivy=/tmp/ivy2",
            "--conf", "spark.executor.extraJavaOptions=-Dother.option=some_value",
            "--conf", "spark.logLevel=DEBUG",
            "/app/spark_snowflake_query.py",
            sfURL,
            sfUser,
            sfPassword,
            sfDatabase,
            sfSchema,
            sfWarehouse,
            sfTable
        ]

        # Execute the command
        result = subprocess.run(cmd, check=True, text=True, capture_output=True)
        logger.info(f"Query completed successfully on Snowflake {sfTable}")
        # Return result
        return {"status": "success", "output": result.stdout}
    except subprocess.CalledProcessError as e:
        error_message = f"Error executing Spark query: {e.stderr}"
        logger.error(error_message)
        raise Exception(error_message)
    except Exception as e:
        logger.exception("Unexpected error occurred during Spark query.")
        raise Exception(str(e))

@celery_app.task(bind=True)
def run_spark_googlebigquery_query(self, bqSA: str, project_id: str, dataset: str, table: str):
    try:
        logger.info(f"Starting Spark query For GoogleBigQuery!")

        # Construct the spark-submit command
        cmd = [
            "/opt/spark/bin/spark-submit",
            #"--packages", "com.google.cloud.spark:spark-3.4-bigquery-lib:0.41.1",
            "--jars", "/app/jars/spark-3.4-bigquery-0.41.1.jar",
            "--conf", "spark.jars.ivy=/tmp/ivy2",
            "--conf", "spark.executor.extraJavaOptions=-Dother.option=some_value",
            "--conf", "spark.logLevel=DEBUG",
            "/app/spark_googlebigquery_query.py",
            bqSA,
            project_id,
            dataset,
            table
        ]

        # Execute the command
        result = subprocess.run(cmd, check=True, text=True, capture_output=True)
        logger.info(f"Query completed successfully on GoogleBigQuery {table}")
        # Return result
        return {"status": "success", "output": result.stdout}
    except subprocess.CalledProcessError as e:
        error_message = f"Error executing Spark query: {e.stderr}"
        logger.error(error_message)
        raise Exception(error_message)
    except Exception as e:
        logger.exception("Unexpected error occurred during Spark query.")
        raise Exception(str(e))

@celery_app.task(bind=True)
def run_spark_mssql_query(self, server: str, database_name: str, table_name: str, access_token: str):
    try:
        logger.info(f"Starting Spark query For MSSQL Server {server} Database {database_name} Table {table_name}")

        # Construct the spark-submit command
        cmd = [
            "/opt/spark/bin/spark-submit",
            #"--packages", "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11",
            "--jars", "/app/jars/mssql-jdbc-12.8.1.jre11.jar",
            "--conf", "spark.jars.ivy=/tmp/ivy2",
            "--conf", "spark.executor.extraJavaOptions=-Dother.option=some_value",
            "--conf", "spark.logLevel=DEBUG",
            "/app/spark_mssql_query.py",
            server,
            database_name,
            table_name,
            access_token
        ]

        # Execute the command
        result = subprocess.run(cmd, check=True, text=True, capture_output=True)
        logger.info(f"Query completed successfully on MSSQL Server {server} Database {database_name} Table {table_name}")
        # Return result
        return {"status": "success", "output": result.stdout}
    except subprocess.CalledProcessError as e:
        error_message = f"Error executing Spark query: {e.stderr}"
        logger.error(error_message)
        raise Exception(error_message)
    except Exception as e:
        logger.exception("Unexpected error occurred during Spark query.")
        raise Exception(str(e))