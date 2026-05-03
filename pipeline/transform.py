"""
Silver layer: Clean and conform Bronze tables into validated Silver Delta tables.

Input paths (Bronze layer output — read these, do not modify):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Output paths (your pipeline must create these directories):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Requirements:
  - Deduplicate records within each table on natural keys
    (account_id, transaction_id, customer_id respectively).
  - Standardise data types (e.g. parse date strings to DATE, cast amounts to
    DECIMAL(18,2), normalise currency variants to "ZAR").
  - Apply DQ flagging to transactions:
      - Set dq_flag = NULL for clean records.
      - Set dq_flag to the appropriate issue code for flagged records.
      - Valid codes: ORPHANED_ACCOUNT, DUPLICATE_DEDUPED, TYPE_MISMATCH,
        DATE_FORMAT, CURRENCY_VARIANT, NULL_REQUIRED.
  - Load DQ rules from config/dq_rules.yaml rather than hardcoding.
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.

See output_schema_spec.md §6 for the full list of DQ flag values and their
definitions.
"""
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,to_date, to_timestamp, concat, coalesce, when, row_number, concat
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window 
import yaml

from data_quality import dq_rules_config, dq_null_checks, dq_type_checks, dq_domain_checks, dq_currency_normalisation, dq_date_format_checks, dq_referential_integrity

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
    
def transform_accounts(bronze_accounts):
    """
        Transform Accounts:
        Input: Accounts bronze Data
        
        Returns a dataframe
    """
    window_accounts = Window.partitionBy("account_id").orderBy(col("ingestion_timestamp").desc())
    accounts_deduped = bronze_accounts.withColumn("row_id", row_number().over(window_accounts)).filter(col("row_id") ==1).drop("row_id")
    accounts_standardised = accounts_deduped.select(
        col("account_id"),
        col("customer_ref"),
        col("account_type"),
        col("account_status"),
        to_date(col("open_date"), 'yyyy-mm-dd').alias("open_date"),
        col("product_tier"),
        col("digital_channel"),
        col("credit_limit").cast(DecimalType(18,2)),
        col("current_balance").cast(DecimalType(18,2)),
        to_date(col("last_activity_date"), 'yyyy-mm-dd').alias("last_activity_date"),
        col("ingestion_timestamp"),
    )
    return accounts_standardised
    
def transform_transactions(bronze_transactions):
    """
        Transform Transaction Data:
        Input: transactions bronze Data
        
        Returns a dataframe
    """
    window_transactions = Window.partitionBy("transaction_id").orderBy(col("ingestion_timestamp").desc())
    transactions_deduped = bronze_transactions.withColumn("row_id", row_number().over(window_transactions)).filter(col("row_id") ==1).drop("row_id")
    
    transactions_standardised = transactions_deduped.select(
        col("transaction_id"),
        col("account_id"),
        col("transaction_date").cast("date").alias("transaction_date"),
        col("transaction_time"),
        to_timestamp(concat(col("transaction_date"), lit(" "), col("transaction_time")),"yyyy-MM-dd HH:mm:ss").alias("transaction_timestamp"),
        col("transaction_type"),
        col("merchant_category"),
        col("amount").cast(DecimalType(18,2)),
        lit("ZAR").alias("currency"),
        col("channel"),
        col("location.province").alias("province"),
        col("location.city").alias("city"),
        col("location.coordinates").alias("location_coordinates"),
        col("metadata.device_id").alias("device_id"),
        col("metadata.session_id").alias("session_id"),
        col("metadata.retry_flag").alias("retry_flag"),
        col("ingestion_timestamp"),
    )
    return transactions_standardised

def transform_customers(bronze_customers):
    """
        Transform Customers:
        Input: Customer Bronze Data
        
        Returns a dataframe
    """
    window_customers = Window.partitionBy("customer_id").orderBy(col("ingestion_timestamp").desc())
    customers_deduped = bronze_customers.withColumn("row_id", row_number().over(window_customers)).filter(col("row_id") ==1).drop("row_id")
    customers_standardised = customers_deduped.select(
        col("customer_id"),
        col("id_number"),
        col("first_name"),
        col("last_name"),
        to_date(col("dob"), 'yyyy-mm-dd').alias("dob"),
        col("gender"),
        col("province"),
        col("income_band"),
        col("segment"),
        col("risk_score").cast("int"),
        col("kyc_status"),
        col("product_flags"),
        col("ingestion_timestamp"),
    )
    return customers_standardised

def run_transformation():
    #   1. Load pipeline_config.yaml to get input/output paths.
    configs = load_config()
    #   2. Initialise (or reuse) SparkSession.
    delta_lake_support = configs["spark"]["master"]
    appName = configs["spark"]["app_name"]
    spark = initialise_spark(appName, delta_lake_support)
    
    #   3. Read each Bronze table.
    bronze_folder = configs["output"]["bronze_path"]
    silver_folder = configs["output"]["silver_path"]
    
    bronze_accounts = spark.read.format("delta").load(f"{bronze_folder}/accounts/")
    bronze_transactions = spark.read.format("delta").load(f"{bronze_folder}/transactions/")
    bronze_customers = spark.read.format("delta").load(f"{bronze_folder}/customers/")
    
    #   4. Deduplicate, type-cast, and standardise each table.
    df_accounts = transform_accounts(bronze_accounts)
    df_transactions = transform_transactions(bronze_transactions)
    df_customers = transform_customers(bronze_customers)
    
    #df = bronze_transactions.limit(10000)
    #df.write.format("json").save("/data/input/trans.json")
    
    #df = bronze_accounts.limit(1000)
    #df.write.mode("overwrite").format("csv").option("header", "true").save("/data/input/account.csv")
    
    #df = bronze_customers.limit(1000)
    #df.write.mode("overwrite").format("csv").option("header", "true").save("/data/input/cust.csv")
    
    #   5. Apply DQ flagging to the transactions table.
    rules = dq_rules_config()
    df_transactions = dq_null_checks(df_transactions, rules, "fact_transactions")
    df_transactions = dq_type_checks(df_transactions, rules, "fact_transactions")
    df_transactions = dq_domain_checks(df_transactions, rules, "fact_transactions")
    df_transactions = dq_currency_normalisation(df_transactions, rules, "fact_transactions")
    df_transactions = dq_date_format_checks(df_transactions, rules, "fact_transactions")

    df_list = [("fact_transactions", df_transactions), ("dim_accounts", df_accounts), ("dim_customers", df_customers)]
    df_transactions = dq_referential_integrity(df_list, rules, "fact_transactions")
    
    print("dq_rules_applied")
    
    #   6. Write cleaned tables to silver/.
    df_accounts.write.format("delta").mode("overwrite").save(f"{silver_folder}/accounts/")
    df_transactions.write.format("delta").mode("overwrite").save(f"{silver_folder}/transactions/")
    df_customers.write.format("delta").mode("overwrite").save(f"{silver_folder}/customers/")

if __name__ == "__main__":
    print(f"Transformation Start: {datetime.now()}")
    run_transformation()
    print(f"Transformation Ended: {datetime.now()}")
