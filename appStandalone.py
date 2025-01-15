import subprocess
import logging
from fastapi import FastAPI, HTTPException

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.get("/")
def root():
    return {"message": "Welcome to Spark Query API"}

@app.post("/run-query")
def run_query(datalakepath: str, format_type: str = "delta"):
    # Validate inputs
    if not datalakepath:
        raise HTTPException(status_code=400, detail="Delta table path must be provided.")
    if format_type not in ["delta", "iceberg", "parquet"]:
        raise HTTPException(status_code=400, detail="Unsupported format type. Use 'delta' or 'iceberg' or 'parquet'.")

    try:
        logger.info(f"Starting Spark query on table: {datalakepath} with format: {format_type}")

        # Construct the spark-submit command
        cmd = [
            "/opt/spark/bin/spark-submit",  # Update this path if needed
            "--packages", "io.delta:delta-core_2.12:2.4.0,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.1",
            "--conf", "spark.jars.ivy=/tmp/ivy2",  # Set Ivy cache location
            "--conf", "spark.executor.extraJavaOptions=-Dother.option=some_value",  # Fix extraJavaOptions usage
            "/app/spark_query.py",          # Your Spark script path
            datalakepath,
            format_type
        ]

        # Execute the command
        result = subprocess.run(cmd, check=True, text=True, capture_output=True)

        logger.info(f"Query completed successfully on datalakepath: {datalakepath}")
        return {"status": "success", "output": result.stdout}
    except subprocess.CalledProcessError as e:
        error_message = f"Error executing Spark query: {e.stderr}"
        logger.error(error_message)
        raise HTTPException(status_code=500, detail=error_message)
    except Exception as e:
        logger.exception("Unexpected error occurred during Spark query.")
        raise HTTPException(status_code=500, detail=str(e))
