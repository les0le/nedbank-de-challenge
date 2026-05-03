"""
Bronze layer: Ingest raw source data into Delta Parquet tables.

Input paths (read-only mounts — do not write here):
  /data/input/accounts.csv
  /data/input/transactions.jsonl
  /data/input/customers.csv

Output paths (your pipeline must create these directories):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Requirements:
  - Preserve source data as-is; do not transform at this layer.
  - Add an `ingestion_timestamp` column (TIMESTAMP) recording when each
    record entered the Bronze layer. Use a consistent timestamp for the
    entire ingestion run (not per-row).
  - Write each table as a Delta Parquet table (not plain Parquet).
  - Read paths from config/pipeline_config.yaml — do not hardcode paths.
  - All paths are absolute inside the container (e.g. /data/input/accounts.csv).

Spark configuration tip:
  Run Spark in local[2] mode to stay within the 2-vCPU resource constraint.
  Configure Delta Lake using the builder pattern shown in the base image docs.
"""
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import yaml

def load_config():
    config_path = os.environ.get("PIPELINE_CONFIG", "/data/config/pipeline_config.yaml")
    try:
        with open(config_path) as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def initialise_spark(app_name:str, delta_lake_support: str):
    spark = (
        SparkSession.builder\
        .appName(app_name)\
        .master(delta_lake_support)\
        .config("spark.jars", ",".join(["/opt/spark/jars/delta-spark_2.12-3.1.0.jar",
          "/opt/spark/jars/delta-storage-3.1.0.jar",
          "/opt/spark/jars/antlr4-runtime-4.9.3.jar"]))\
        .getOrCreate()
    )
    return spark

def ingest_file(spark, configs: dict, file:str, file_format: str, ingestion_time):
    input_path = configs["input"]
    output_path_bronze = configs["output"]["bronze_path"]
    input_file_path = input_path[file +"_path"]
        
    if file_format == "csv":
        df = spark.read.format("csv").option("header","true").load(input_file_path)
    
    if file_format == "json":
        df = spark.read.format("json").load(input_file_path)
    
    df = df.withColumn("ingestion_timestamp", lit(ingestion_time))    
    df.write.format("delta").mode("overwrite").save(f"{output_path_bronze}/{file}/")



def run_ingestion():
    #   1. Load pipeline_config.yaml to get input/output paths.
    #print("Loading Configs")
    configs = load_config()
    
    #   2. Initialise a SparkSession with Delta Lake support (local[2]).
    #delta_lake_support = configs["spark"]["master"]
    #appName = configs["spark"]["app_name"]
    spark = initialise_spark("Nedbank-DE-Challenge", "local[2]")
    
    ingestion_time = datetime.now()
    #   3. Read accounts.csv → append ingestion_timestamp → write to bronze/accounts/.
    #print("Ingest Accounts")
    ingest_file(spark, configs, "accounts","csv", ingestion_time)

    #   4. Read transactions.jsonl → append ingestion_timestamp → write to bronze/transactions/.
    #print("Ingest Transactions")
    ingest_file(spark, configs, "transactions", "json", ingestion_time)

    #   5. Read customers.csv → append ingestion_timestamp → write to bronze/customers/.
    #print("Ingest Customers")
    ingest_file(spark, configs, "customers","csv", ingestion_time)
    
    spark.stop()

if __name__ == "__main__":
  print(f"Ingestion Start: {datetime.now()}")
  run_ingestion()
  print(f"Ingestion Ended: {datetime.now()}")

