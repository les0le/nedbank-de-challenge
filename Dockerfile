FROM nedbank-de-challenge/base:1.0

ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="$SPARK_HOME/bin:$PATH"
ENV PYSPARK_PYTHON=python3
ENV SPARK_LOCAL_IP=127.0.0.1
ENV HOSTNAME=localhost
ENV PYSPARK_SUBMIT_ARGS="\
--conf spark.jars.ivy= \
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
pyspark-shell"

# Install any additional Python dependencies you need beyond the base image.

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create jars directory
RUN mkdir -p /opt/spark/jars

# Download ALL required jars at build time
RUN curl -L -o /opt/spark/jars/delta-spark_2.12-3.1.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \
    curl -L -o /opt/spark/jars/delta-storage-3.1.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar && \
    curl -L -o /opt/spark/jars/antlr4-runtime-4.9.3.jar \
        https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar

# Leave requirements.txt empty if the base packages are sufficient.
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline code and configuration into the image.
# Do NOT copy data files or output directories — these are injected at runtime
# via Docker volume mounts by the scoring system.
COPY pipeline/ /app/pipeline/
COPY config/ /app/config/

# Entry point — must run the complete pipeline end-to-end without interactive input.
# The scoring system uses this CMD directly; do not require TTY or stdin.
CMD ["python", "pipeline/run_all.py"]
