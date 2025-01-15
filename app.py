import logging
from fastapi import FastAPI, HTTPException
from celery.result import AsyncResult
from tasks import run_spark_query, celery_app, run_spark_databricks_query, run_spark_snowflake_query, run_spark_googlebigquery_query, run_spark_mssql_query
# Import both the task and the Celery app instance

# Create FastAPI app
app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.post("/submit-query")
def submit_query(datalakepath: str, format_type: str, token: str, expires_on: int, storageAccount: str):
    # Validate inputs
    if not datalakepath:
        raise HTTPException(status_code=400, detail="Delta table path must be provided.")
    if format_type not in ["delta", "iceberg", "parquet"]:
        raise HTTPException(status_code=400, detail="Unsupported format type. Use 'delta', 'iceberg', or 'parquet'.")

    # Submit the task to Celery
    task = run_spark_query.delay(datalakepath, format_type, token, expires_on, storageAccount)
    logger.info(f"Task submitted with task_id: {task.id}")
    return {"task_id": task.id}

@app.post("/submit-databricks-query")
def submit_databricks_query(personal_access_token: str, jdbc_url: str, http_path: str, query: str):
    # Submit the task to Celery
    task = run_spark_databricks_query.delay(personal_access_token, jdbc_url, http_path, query)
    logger.info(f"Task submitted with task_id: {task.id}")
    return {"task_id": task.id}

@app.post("/submit-snowflake-query")
def submit_snowflake_query(sfURL: str, sfUser: str, sfPassword: str, sfDatabase: str, sfSchema: str, sfWarehouse: str, sfTable: str):
    # Submit the task to Celery
    task = run_spark_snowflake_query.delay(sfURL, sfUser, sfPassword, sfDatabase, sfSchema, sfWarehouse, sfTable)
    logger.info(f"Task submitted with task_id: {task.id}")
    return {"task_id": task.id}

@app.post("/submit-googlebigquery-query")
def submit_googlebigquery_query(bqSA: str, project_id: str, dataset: str, table: str):
    # Submit the task to Celery
    task = run_spark_googlebigquery_query.delay(bqSA, project_id, dataset, table)
    logger.info(f"Task submitted with task_id: {task.id}")
    return {"task_id": task.id}

@app.post("/submit-mssql-query")
def submit_mssql_query(server: str, database_name: str, table_name: str, access_token: str):
    # Submit the task to Celery
    task = run_spark_mssql_query.delay(server, database_name, table_name, access_token)
    logger.info(f"Task submitted with task_id: {task.id}")
    return {"task_id": task.id}

@app.get("/get-status/{task_id}")
def get_status(task_id: str):
    # Use the Celery app instance to check task status
    task_result = AsyncResult(task_id, app=celery_app)
    
    logger.info(f"Checking status of task with id: {task_id}")

    if task_result.status == "PENDING":
        return {"task_id": task_id, "status": "PENDING"}
    elif task_result.status == "STARTED":
        return {"task_id": task_id, "status": "IN PROGRESS"}
    elif task_result.status == "SUCCESS":
        result = task_result.result
        logger.info(f"Status SUCCESS. Task {task_id} completed successfully. Result: {result}")
        
        return {
            "task_id": task_id,
            "status": "SUCCESS",
            "result": str(result)
        }
        
    elif task_result.status == "FAILURE":
        logger.error(f"Task {task_id} failed. Error: {task_result.result}")
        return {"task_id": task_id, "status": "FAILED", "error": str(task_result.result)}
    else:
        logger.info(f"Task {task_id} is in state: {task_result.status}")
        return {"task_id": task_id, "status": task_result.status}
