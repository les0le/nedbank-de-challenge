"""
Gold layer: Join and aggregate Silver tables into the scored output schema.

Input paths (Silver layer output — read these, do not modify):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Output paths (your pipeline must create these directories):
  /data/output/gold/fact_transactions/     — 14 fields (see output_schema_spec.md §2)
  /data/output/gold/dim_accounts/          — 11 fields (see output_schema_spec.md §3)
  /data/output/gold/dim_customers/         — 9 fields  (see output_schema_spec.md §4)

Requirements:
  - Generate surrogate keys (_sk fields) that are unique, non-null, and stable
    across pipeline re-runs on the same input data. Use row_number() with a
    stable ORDER BY on the natural key, or sha2(natural_key, 256) cast to BIGINT.
  - Resolve all foreign key relationships:
      fact_transactions.account_sk  → dim_accounts.account_sk
      fact_transactions.customer_sk → dim_customers.customer_sk
      dim_accounts.customer_id      → dim_customers.customer_id
  - Rename accounts.customer_ref → dim_accounts.customer_id at this layer.
  - Derive dim_customers.age_band from dob (do not copy dob directly).
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.

See output_schema_spec.md for the complete field-by-field specification.
"""
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit, floor,  datediff, when, row_number, current_date, sha2
from pyspark.sql.window import Window 
import yaml

# ***** TO DO: Put this method in a different file and import method for all python scripts
def load_config():
    config_path = os.environ.get("PIPELINE_CONFIG", "/data/config/pipeline_config.yaml")
    try:
        with open(config_path) as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# ***** TO DO: Put this method in a different file and import method for all python scripts
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

def build_dim_customers(df):
    #window_customer = Window.orderBy("customer_id") # Performance issues
    
    df = df.withColumn("customer_sk", sha2(col("customer_id"), 256))\
        .withColumn("age", floor(datediff(current_date(),col("dob")) / 365.25))\
        .withColumn("age_band", when (col("age") >= 65, lit("65+"))\
        .when (col("age") >= 56, lit("56-65"))\
        .when (col("age") >= 46, lit("46-55"))\
        .when (col("age") >= 36, lit("36-45"))\
        .when (col("age") >= 26, lit("26-35"))\
        .when (col("age") >= 18, lit("18-25"))\
        .otherwise(lit(None)))\
        .drop(col("age"))
        
    df = df.select(
        col("customer_sk"),
        col("customer_id"),
        col("gender"),
        col("province"),
        col("income_band"),
        col("segment"),
        col("risk_score"),
        col("kyc_status"),
        col("age_band")
        )
    
    return df
    
def build_dim_accounts(df):
    #window_account = Window.orderBy("account_id") # Performance Issues
    
    df = df.withColumn("account_sk", sha2(col("account_id"), 256))\
        .select(col("account_sk"),
            col("account_id"),
            col("customer_ref").alias("customer_id"),
            col("account_type"),
            col("account_status"),
            col("open_date"),
            col("product_tier"),
            col("digital_channel"),
            col("credit_limit"),
            col("current_balance"),
            col("last_activity_date"))
    
    return df

def build_fact_transactions(silver_transactions, dim_customers, dim_accounts):
        
    #Get foreign keys
    df = silver_transactions.alias("txn")\
        .join(dim_accounts.alias("acc"), col("txn.account_id") == col("acc.account_id"), "left")\
        .join(dim_customers.alias("cust"), col("acc.customer_id") == col("cust.customer_id"), "left")\
        .withColumn("transaction_sk",sha2(col("transaction_id"), 256))\
        .select(col("transaction_sk"),
            col("txn.transaction_id"),
            col("acc.account_sk"),
            col("cust.customer_sk"),
            col("txn.transaction_date"),
            col("txn.transaction_timestamp"),
            col("txn.transaction_type"),
            col("txn.merchant_category"),
            col("txn.amount"),
            col("txn.currency"),
            col("txn.channel"),
            col("txn.province"),
            col("txn.dq_flag"),
            col("txn.ingestion_timestamp"))
    
    return df
    
def run_provisioning():
    #   1. Load pipeline_config.yaml to get input/output paths.
    configs = load_config()                         
    #   2. Initialise (or reuse) SparkSession.
    delta_lake_support = configs["spark"]["master"]
    appName = configs["spark"]["app_name"]
    spark = initialise_spark(appName, delta_lake_support)
    
    #   3. Read Silver tables.
    silver_folder = configs["output"]["silver_path"]
    
    silver_accounts = spark.read.format("delta").load(f"{silver_folder}/accounts/")
    silver_transactions = spark.read.format("delta").load(f"{silver_folder}/transactions/")
    silver_customers = spark.read.format("delta").load(f"{silver_folder}/customers/")
    #   4. Build dim_customers with surrogate keys and derived age_band.
    dim_customers = build_dim_customers(silver_customers)
    
    #   5. Build dim_accounts with surrogate keys; rename customer_ref → customer_id.
    dim_accounts = build_dim_accounts(silver_accounts)
    #   6. Build fact_transactions, resolving account_sk and customer_sk via joins.
    fact_transactions = build_fact_transactions(silver_transactions, dim_customers, dim_accounts)
    
    #dq_rules 
    
    #   7. Write all three Gold tables as Delta Parquet.
    gold_folder = configs["output"]["gold_path"]
    dim_accounts.write.format("delta").mode("overwrite").save(f"{gold_folder}/dim_accounts/")
    fact_transactions.write.format("delta").mode("overwrite").save(f"{gold_folder}/fact_transactions/")
    dim_customers.write.format("delta").mode("overwrite").save(f"{gold_folder}/dim_customers/")

print(f"Provisioning Start: {datetime.now()}")
run_provisioning()
print(f"Provisioning Ended: {datetime.now()}")

