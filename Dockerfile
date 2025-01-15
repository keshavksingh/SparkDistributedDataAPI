# Base image: Apache Spark with Python support
FROM apache/spark:3.4.4-scala2.12-java11-python3-r-ubuntu

# Set working directory
WORKDIR /app

# Switch to root to install Python libraries
USER root

# Install necessary Python libraries
RUN pip install --no-cache-dir fastapi uvicorn celery redis

# Install Hadoop dependencies for ADLS Gen2
RUN apt-get update && apt-get install -y openjdk-11-jdk wget \
    && wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar -P /opt/spark/jars/ \
    && wget https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar -P /opt/spark/jars/ \
    && wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure-datalake/3.3.4/hadoop-azure-datalake-3.3.4.jar -P /opt/spark/jars/ \
    && wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.7.1/iceberg-spark-runtime-3.4_2.12-1.7.1.jar -P /opt/spark/jars/ \
    && wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar -P /opt/spark/jars/ \
    && wget https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar -P /opt/spark/jars/ \
    && apt-get clean

# Fix Ivy cache directory permissions
RUN mkdir -p /tmp/ivy2 && chmod -R 777 /tmp/ivy2

COPY CustomTokenProvider.java /app/

# Compile the Java class and create a JAR file
RUN javac -cp "/opt/spark/jars/hadoop-common-3.3.4.jar:/opt/spark/jars/hadoop-azure-3.3.4.jar:/opt/spark/jars/hadoop-client-api-3.3.4.jar:/opt/spark/jars/hadoop-client-runtime-3.3.4.jar" CustomTokenProvider.java && \
    jar cf my-custom-token-provider.jar CustomTokenProvider.class && \
    mv my-custom-token-provider.jar /opt/spark/jars/


# Clean up unnecessary files
RUN rm CustomTokenProvider.java CustomTokenProvider.class

# Add Spark configuration
COPY spark-defaults.conf /opt/spark/conf/

# Switch back to the 'spark' user
#USER spark

# Copy application code
COPY . /app/
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
#COPY app.py /app/
#COPY spark_query.py /app/

# Switch back to the 'spark' user
USER spark

# Expose ports for FastAPI
EXPOSE 8000

# Start FastAPI
#CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
# Copy the entrypoint script


# Start the entrypoint script
CMD ["/app/entrypoint.sh"]
